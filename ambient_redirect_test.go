// Copyright © 2026 Meroxa, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package s3_test

import (
	"context"
	"encoding/pem"
	"io"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/hostpolicy"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-protocol/pconnector"
	s3 "github.com/conduitio/conduit-connector-s3"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
)

// isolateAWSEnv clears every ambient AWS source (and proxies) so the test sees
// only what it sets. Mirrors config/credentials_test.go.
func isolateAWSEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{
		"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_ACCESS_KEY", "AWS_SECRET_KEY",
		"AWS_PROFILE", "AWS_DEFAULT_PROFILE", "AWS_ROLE_ARN", "AWS_WEB_IDENTITY_TOKEN_FILE",
		"AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "AWS_CONTAINER_CREDENTIALS_FULL_URI",
		"AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL_S3", "AWS_ENDPOINT_URL_STS", "AWS_CA_BUNDLE",
		"AWS_EC2_METADATA_SERVICE_ENDPOINT", "AWS_MAX_ATTEMPTS", "AWS_RETRY_MODE", "AWS_REGION", "AWS_DEFAULT_REGION",
		"HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy",
	} {
		t.Setenv(k, "")
	}
	dir := t.TempDir()
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(dir, "no-credentials"))
	t.Setenv("AWS_CONFIG_FILE", filepath.Join(dir, "no-config"))
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
}

// chanStream is a minimal in-memory pconnector.DestinationRunStream, standing
// in for the engine's (the SDK adapter is what the test exercises).
type chanStream struct {
	reqs  chan pconnector.DestinationRunRequest
	resps chan pconnector.DestinationRunResponse
}

func newChanStream() *chanStream {
	return &chanStream{reqs: make(chan pconnector.DestinationRunRequest), resps: make(chan pconnector.DestinationRunResponse, 16)}
}

func (s *chanStream) Client() pconnector.DestinationRunStreamClient { return chanClient{s} }
func (s *chanStream) Server() pconnector.DestinationRunStreamServer { return chanServer{s} }

type (
	chanClient struct{ s *chanStream }
	chanServer struct{ s *chanStream }
)

func (c chanClient) Send(r pconnector.DestinationRunRequest) error { c.s.reqs <- r; return nil }
func (c chanClient) Recv() (pconnector.DestinationRunResponse, error) {
	select {
	case r := <-c.s.resps:
		return r, nil
	case <-time.After(20 * time.Second):
		return pconnector.DestinationRunResponse{}, io.ErrNoProgress
	}
}
func (c chanServer) Send(r pconnector.DestinationRunResponse) error { c.s.resps <- r; return nil }
func (c chanServer) Recv() (pconnector.DestinationRunRequest, error) {
	r, ok := <-c.s.reqs
	if !ok {
		return pconnector.DestinationRunRequest{}, io.EOF
	}
	return r, nil
}

type redirectRig struct {
	firstURL    string
	firstHits   atomic.Int32
	secondHits  atomic.Int32
	secondToken atomic.Value
}

// newRedirectRig starts server A on 127.0.0.1, which answers every request
// with a 307 to server B on [::1] (a different host; both names are covered by
// the httptest certificate, whose CA the SDK trusts through AWS_CA_BUNDLE).
func newRedirectRig(t *testing.T) *redirectRig {
	t.Helper()
	rig := &redirectRig{}
	rig.secondToken.Store("")

	ln6, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "[::1]:0")
	if err != nil {
		t.Skipf("IPv6 loopback not available: %v", err)
	}
	b := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rig.secondHits.Add(1)
		rig.secondToken.Store(r.Header.Get("X-Amz-Security-Token"))
		w.WriteHeader(http.StatusOK)
	}))
	_ = b.Listener.Close()
	b.Listener = ln6
	b.StartTLS()
	t.Cleanup(b.Close)

	a := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rig.firstHits.Add(1)
		http.Redirect(w, r, b.URL+r.URL.Path, http.StatusTemporaryRedirect)
	}))
	t.Cleanup(a.Close)
	rig.firstURL = a.URL

	ca := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: a.Certificate().Raw}), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AWS_CA_BUNDLE", ca)
	t.Setenv("AWS_MAX_ATTEMPTS", "10") // a refused redirect must still be one request
	return rig
}

// writeOneRecord drives the destination through sdk.NewDestinationPlugin, the
// wrapper the engine uses for built-in connectors, and returns the ack error.
func writeOneRecord(ctx context.Context, t *testing.T, cfg map[string]string) string {
	t.Helper()
	is := is.New(t)
	spec := s3.Connector.NewSpecification()
	dst := sdk.NewDestinationPlugin(s3.Connector.NewDestination(), pconnector.PluginConfig{}, spec.DestinationParams)
	_, err := dst.Configure(ctx, pconnector.DestinationConfigureRequest{Config: cfg})
	is.NoErr(err)
	_, err = dst.Open(ctx, pconnector.DestinationOpenRequest{})
	is.NoErr(err)
	stream := newChanStream()
	runDone := make(chan error, 1)
	go func() { runDone <- dst.Run(ctx, stream) }() // the SDK's Run blocks until the stream ends
	defer func() {
		close(stream.reqs)
		select {
		case <-runDone:
		case <-time.After(10 * time.Second):
			t.Error("destination Run did not stop")
		}
	}()
	is.NoErr(stream.Client().Send(pconnector.DestinationRunRequest{Records: []opencdc.Record{{
		Position: opencdc.Position("p1"), Operation: opencdc.OperationCreate,
		Payload: opencdc.Change{After: opencdc.RawData("hello")},
	}}}))
	resp, err := stream.Client().Recv()
	is.NoErr(err)
	is.Equal(len(resp.Acks), 1)
	return resp.Acks[0].Error
}

// openSource drives the source through sdk.NewSourcePlugin, the wrapper the
// engine uses for built-in connectors. Open calls HeadBucket, a bodyless
// request: the SDK follows a 307 for those (it does not for PutObject, see
// TestAmbientCredentials_PutObjectRedirectNotFollowed).
func openSource(ctx context.Context, t *testing.T, cfg map[string]string) error {
	t.Helper()
	spec := s3.Connector.NewSpecification()
	cfg = maps.Clone(cfg)
	delete(cfg, "format") // destination-only parameter
	src := sdk.NewSourcePlugin(s3.Connector.NewSource(), pconnector.PluginConfig{}, spec.SourceParams)
	if _, err := src.Configure(ctx, pconnector.SourceConfigureRequest{Config: cfg}); err != nil {
		return err
	}
	_, err := src.Open(ctx, pconnector.SourceOpenRequest{})
	_, _ = src.Teardown(ctx, pconnector.SourceTeardownRequest{})
	return err
}

func ambientCtx(t *testing.T, endpoint string) context.Context {
	t.Helper()
	u, _ := url.Parse(endpoint)
	ep, err := hostpolicy.NewEndpoint(u.Hostname(), u.Port())
	if err != nil {
		t.Fatal(err)
	}
	return hostpolicy.WithPolicy(context.Background(), hostpolicy.Policy{
		AmbientCredentials: true,
		AllowResources:     []string{"allowed-bucket"},
		AllowEndpoints:     []hostpolicy.Endpoint{ep},
	})
}

func ambientChainEnv(t *testing.T) {
	t.Helper()
	isolateAWSEnv(t)
	t.Setenv("AWS_ACCESS_KEY_ID", "AKIDAMBIENT")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "ambient-secret")
	t.Setenv("AWS_SESSION_TOKEN", "ambient-session-token")
}

func redirectCfg(endpoint string, extra map[string]string) map[string]string {
	c := map[string]string{
		"aws.region": "us-east-1", "aws.bucket": "allowed-bucket", "aws.endpoint": endpoint,
		"aws.pathStyle": "true", "format": "json",
	}
	for k, v := range extra {
		c[k] = v
	}
	return c
}

func TestAmbientCredentials_RedirectDoesNotLeakToSecondHost(t *testing.T) {
	t.Run("control: net/http forwards the session token across hosts", func(t *testing.T) {
		is := is.New(t)
		isolateAWSEnv(t)
		rig := newRedirectRig(t)
		pool := httptest.NewTLSServer(http.NotFoundHandler()) // same certificate, for its client's trust pool
		defer pool.Close()
		req, _ := http.NewRequestWithContext(context.Background(), http.MethodHead, rig.firstURL+"/allowed-bucket", nil)
		req.Header.Set("X-Amz-Security-Token", "session-token")
		resp, err := pool.Client().Do(req)
		is.NoErr(err)
		_ = resp.Body.Close()
		is.Equal(rig.secondHits.Load(), int32(1))
		is.Equal(rig.secondToken.Load(), "session-token") // the leak the guard exists to stop
	})

	t.Run("control: the connector follows the redirect when it is not guarded (static keys)", func(t *testing.T) {
		is := is.New(t)
		isolateAWSEnv(t)
		rig := newRedirectRig(t)
		_ = openSource(context.Background(), t, redirectCfg(rig.firstURL,
			map[string]string{"aws.accessKeyId": "AKIDSTATIC", "aws.secretAccessKey": "static-secret"}))
		is.True(rig.secondHits.Load() >= 1) // the rig observes a followed redirect through the connector
	})

	t.Run("ambient credentials: refused once, nothing reaches the second host", func(t *testing.T) {
		is := is.New(t)
		ambientChainEnv(t)
		rig := newRedirectRig(t)
		err := openSource(ambientCtx(t, rig.firstURL), t, redirectCfg(rig.firstURL, nil))
		he, ok := hostpolicy.AsError(err)
		is.True(ok)
		is.Equal(he.Code, hostpolicy.CodeRedirectRefused)
		is.Equal(rig.secondHits.Load(), int32(0))
		is.Equal(rig.firstHits.Load(), int32(1)) // not retried, although AWS_MAX_ATTEMPTS=10
	})
}

// TestAmbientCredentials_PutObjectRedirectNotFollowed pins an SDK behavior the
// design left unverified: a 307 on PutObject is returned as an error, not
// followed and not retried, so the write path never reaches a second host,
// with or without the guard. The record is not acked as written.
func TestAmbientCredentials_PutObjectRedirectNotFollowed(t *testing.T) {
	is := is.New(t)
	ambientChainEnv(t)
	rig := newRedirectRig(t)
	// aws.serverSideEncryption is set so Open does not probe the bucket's
	// default encryption (GetBucketEncryption); that would be a second request
	// to the first server and would hide the PutObject behavior under test.
	ackErr := writeOneRecord(ambientCtx(t, rig.firstURL), t, redirectCfg(rig.firstURL,
		map[string]string{"aws.serverSideEncryption": "AES256"}))
	is.True(ackErr != "")
	is.Equal(rig.secondHits.Load(), int32(0))
	is.Equal(rig.firstHits.Load(), int32(1))
}

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

package config

import (
	"bytes"
	"context"
	"encoding/pem"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/conduitio/conduit-commons/dialer"
	"github.com/conduitio/conduit-commons/hostpolicy"
	"github.com/matryer/is"
)

var errDialRefused = errors.New("dial refused by test host")

// countingServer is an HTTP server that counts accepted TCP connections, so a
// test can prove that no connection was made.
type countingServer struct {
	*httptest.Server
	accepts atomic.Int64
}

type countingListener struct {
	net.Listener
	n *atomic.Int64
}

func (l countingListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err == nil {
		l.n.Add(1)
	}
	return c, err
}

func newCountingServer(t *testing.T, h http.Handler) *countingServer {
	t.Helper()
	return startCountingServer(t, h, false)
}

// newTLSCountingServer is newCountingServer over https. The ambient path only
// talks https; the test trusts the server through AWS_CA_BUNDLE, which also
// shows a custom CA bundle keeps working next to a host dialer.
func newTLSCountingServer(t *testing.T, h http.Handler) *countingServer {
	t.Helper()
	return startCountingServer(t, h, true)
}

func startCountingServer(t *testing.T, h http.Handler, useTLS bool) *countingServer {
	t.Helper()
	cs := &countingServer{}
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	cs.Server = httptest.NewUnstartedServer(h)
	cs.Listener = countingListener{Listener: ln, n: &cs.accepts}
	if useTLS {
		cs.StartTLS()
	} else {
		cs.Start()
	}
	t.Cleanup(cs.Close)
	return cs
}

func (c *countingServer) addr() string {
	return strings.TrimPrefix(strings.TrimPrefix(c.URL, "https://"), "http://")
}

// trust makes the SDK trust the server certificate; call it after isolateAWSEnv.
func (c *countingServer) trust(t *testing.T) {
	t.Helper()
	ca := filepath.Join(t.TempDir(), "ca.pem")
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: c.Certificate().Raw})
	if err := os.WriteFile(ca, pemBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AWS_CA_BUNDLE", ca)
}

// recordingDialer records every address it is asked for. It refuses when
// refuse is set, otherwise it connects with a plain net.Dialer.
type recordingDialer struct {
	refuse bool
	mu     sync.Mutex
	addrs  []string
}

func (d *recordingDialer) dial(ctx context.Context, network, address string) (net.Conn, error) {
	d.mu.Lock()
	d.addrs = append(d.addrs, address)
	d.mu.Unlock()
	if d.refuse {
		return nil, errDialRefused
	}
	return (&net.Dialer{}).DialContext(ctx, network, address)
}

func (d *recordingDialer) calls() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.addrs...)
}

func (d *recordingDialer) ctx(parent context.Context) context.Context {
	return dialer.WithDialContext(parent, d.dial)
}

// closeAfterResponse makes the server end every connection after one response,
// so the next request has to dial again.
func closeAfterResponse(h http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Connection", "close")
		h(w, r)
	})
}

func s3OK(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/xml")
	fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?><ListBucketResult><Name>`+allowedBucket+`</Name><IsTruncated>false</IsTruncated></ListBucketResult>`)
}

// s3Client builds the client the source and the destination use, either with
// static credentials or on the ambient credentials path (the engine policy
// allows it). The server is https and trusted through AWS_CA_BUNDLE.
func s3Client(ctx context.Context, t *testing.T, mode string, srv *countingServer) *s3.Client {
	t.Helper()
	var (
		c   *s3.Client
		err error
	)
	switch mode {
	case "static":
		isolateAWSEnv(t)
		srv.trust(t)
		p := params(srv.URL)
		p.AccessKeyID, p.SecretAccessKey = "AKIDSTATIC", "static-secret"
		c, _, err = NewS3Client(ctx, p, PurposeOpen)
	default:
		ambientEnv(t)
		srv.trust(t)
		ctx = hostpolicy.WithPolicy(ctx, enabledPolicy(t, nil, srv.URL))
		c, _, err = NewS3Client(ctx, params(srv.URL), PurposeOpen)
	}
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func noRetry(o *s3.Options) { o.Retryer = aws.NopRetryer{} }

func TestHostDialer_S3Client(t *testing.T) {
	for _, mode := range []string{"static", "ambient"} {
		t.Run(mode+"/refused", func(t *testing.T) {
			is := is.New(t)
			srv := newTLSCountingServer(t, http.HandlerFunc(s3OK))
			d := &recordingDialer{refuse: true}
			client := s3Client(d.ctx(context.Background()), t, mode, srv)

			// Source-side and destination-side operations.
			_, err := client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: aws.String(allowedBucket)}, noRetry)
			is.True(errors.Is(err, errDialRefused))
			_, err = client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: aws.String(allowedBucket), Key: aws.String("k"), Body: bytes.NewReader([]byte("v")),
			}, noRetry)
			is.True(errors.Is(err, errDialRefused))

			for _, a := range d.calls() {
				is.Equal(a, srv.addr())
			}
			is.True(len(d.calls()) >= 2)
			is.Equal(srv.accepts.Load(), int64(0)) // no real connection was made
		})

		t.Run(mode+"/dials_again_after_reconnect", func(t *testing.T) {
			is := is.New(t)
			srv := newTLSCountingServer(t, closeAfterResponse(s3OK))
			d := &recordingDialer{}
			client := s3Client(d.ctx(context.Background()), t, mode, srv)

			for range 2 {
				_, err := client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: aws.String(allowedBucket)}, noRetry)
				is.NoErr(err)
			}
			is.Equal(d.calls(), []string{srv.addr(), srv.addr()})
			is.Equal(srv.accepts.Load(), int64(2))
		})

		t.Run(mode+"/captured_at_build_time", func(t *testing.T) {
			is := is.New(t)
			srv := newTLSCountingServer(t, closeAfterResponse(s3OK))
			d := &recordingDialer{}
			ctx, cancel := context.WithCancel(d.ctx(context.Background()))
			client := s3Client(ctx, t, mode, srv)
			cancel() // the build context is gone; the dialer is still used

			// A context without the dialer must not change what the client uses.
			_, err := client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: aws.String(allowedBucket)}, noRetry)
			is.NoErr(err)
			is.Equal(d.calls(), []string{srv.addr()})
		})

		t.Run(mode+"/no_dialer_unchanged", func(t *testing.T) {
			is := is.New(t)
			srv := newTLSCountingServer(t, http.HandlerFunc(s3OK))
			client := s3Client(context.Background(), t, mode, srv)
			_, err := client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: aws.String(allowedBucket)}, noRetry)
			is.NoErr(err)
			is.Equal(srv.accepts.Load(), int64(1))
		})
	}
}

// TestHostDialer_AmbientKeepsRedirectGuard shows the checks that do not depend
// on the dialer are still active when the host supplies one.
func TestHostDialer_AmbientKeepsRedirectGuard(t *testing.T) {
	is := is.New(t)
	other := newTLSCountingServer(t, http.HandlerFunc(s3OK))
	srv := newTLSCountingServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, other.URL+"/x", http.StatusTemporaryRedirect)
	}))
	d := &recordingDialer{}
	ambientEnv(t)
	srv.trust(t)
	ctx := hostpolicy.WithPolicy(d.ctx(context.Background()), enabledPolicy(t, nil, srv.URL))
	client, _, err := NewS3Client(ctx, params(srv.URL), PurposeOpen)
	is.NoErr(err)
	_, err = client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: aws.String(allowedBucket)}, noRetry)
	is.True(err != nil)
	is.Equal(other.accepts.Load(), int64(0))
}

// retrieve loads the default chain with ctx and fetches credentials once.
func retrieve(ctx context.Context) (aws.Credentials, aws.CredentialsProvider, error) {
	cfg, err := LoadAWSConfig(ctx, "us-east-1", "", "", "")
	if err != nil {
		return aws.Credentials{}, nil, err
	}
	creds, err := cfg.Credentials.Retrieve(ctx)
	return creds, cfg.Credentials, err
}

func containerCredsHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Connection", "close")
	fmt.Fprintf(w, `{"AccessKeyId":"AKIDCONTAINER","SecretAccessKey":"s","Token":"t","Expiration":%q}`,
		time.Now().Add(time.Hour).UTC().Format(time.RFC3339))
}

func TestHostDialer_CredentialProviders(t *testing.T) {
	t.Run("container endpoint", func(t *testing.T) {
		t.Run("refused", func(t *testing.T) {
			is := is.New(t)
			srv := newCountingServer(t, http.HandlerFunc(containerCredsHandler))
			isolateAWSEnv(t)
			t.Setenv("AWS_CONTAINER_CREDENTIALS_FULL_URI", srv.URL+"/creds")
			d := &recordingDialer{refuse: true}
			_, _, err := retrieve(d.ctx(context.Background()))
			is.True(errors.Is(err, errDialRefused))
			is.Equal(d.calls()[0], srv.addr())
			is.Equal(srv.accepts.Load(), int64(0))
		})
		t.Run("dials again after reconnect", func(t *testing.T) {
			is := is.New(t)
			srv := newCountingServer(t, http.HandlerFunc(containerCredsHandler))
			isolateAWSEnv(t)
			t.Setenv("AWS_CONTAINER_CREDENTIALS_FULL_URI", srv.URL+"/creds")
			d := &recordingDialer{}
			creds, provider, err := retrieve(d.ctx(context.Background()))
			is.NoErr(err)
			is.Equal(creds.AccessKeyID, "AKIDCONTAINER")
			provider.(*aws.CredentialsCache).Invalidate()
			_, err = provider.Retrieve(context.Background())
			is.NoErr(err)
			is.Equal(d.calls(), []string{srv.addr(), srv.addr()})
		})
		t.Run("no dialer unchanged", func(t *testing.T) {
			is := is.New(t)
			srv := newCountingServer(t, http.HandlerFunc(containerCredsHandler))
			isolateAWSEnv(t)
			t.Setenv("AWS_CONTAINER_CREDENTIALS_FULL_URI", srv.URL+"/creds")
			creds, _, err := retrieve(context.Background())
			is.NoErr(err)
			is.Equal(creds.AccessKeyID, "AKIDCONTAINER")
			is.Equal(srv.accepts.Load(), int64(1))
		})
	})

	t.Run("instance metadata", func(t *testing.T) {
		h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Connection", "close")
			switch {
			case r.Method == http.MethodPut:
				_, _ = w.Write([]byte("token"))
			case strings.HasSuffix(r.URL.Path, "/security-credentials/"):
				_, _ = w.Write([]byte("role"))
			default:
				fmt.Fprintf(w, `{"Code":"Success","AccessKeyId":"AKIDIMDS","SecretAccessKey":"s","Token":"t","Expiration":%q}`,
					time.Now().Add(time.Hour).UTC().Format(time.RFC3339))
			}
		})
		t.Run("refused", func(t *testing.T) {
			is := is.New(t)
			srv := newCountingServer(t, h)
			isolateAWSEnv(t)
			t.Setenv("AWS_EC2_METADATA_DISABLED", "false")
			t.Setenv("AWS_EC2_METADATA_SERVICE_ENDPOINT", srv.URL)
			d := &recordingDialer{refuse: true}
			_, _, err := retrieve(d.ctx(context.Background()))
			is.True(err != nil)
			is.True(len(d.calls()) >= 1)
			is.Equal(d.calls()[0], srv.addr())
			is.Equal(srv.accepts.Load(), int64(0))
		})
		t.Run("dials", func(t *testing.T) {
			is := is.New(t)
			srv := newCountingServer(t, h)
			isolateAWSEnv(t)
			t.Setenv("AWS_EC2_METADATA_DISABLED", "false")
			t.Setenv("AWS_EC2_METADATA_SERVICE_ENDPOINT", srv.URL)
			d := &recordingDialer{}
			creds, provider, err := retrieve(d.ctx(context.Background()))
			is.NoErr(err)
			is.Equal(creds.AccessKeyID, "AKIDIMDS")
			n := len(d.calls())
			provider.(*aws.CredentialsCache).Invalidate()
			_, err = provider.Retrieve(context.Background())
			is.NoErr(err)
			is.True(len(d.calls()) > n) // the second fetch dialed again
			for _, a := range d.calls() {
				is.Equal(a, srv.addr())
			}
		})
	})

	t.Run("web identity through STS", func(t *testing.T) {
		stsOK := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Connection", "close")
			w.Header().Set("Content-Type", "text/xml")
			fmt.Fprintf(w, `<AssumeRoleWithWebIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/"><AssumeRoleWithWebIdentityResult><Credentials><AccessKeyId>AKIDSTS</AccessKeyId><SecretAccessKey>s</SecretAccessKey><SessionToken>t</SessionToken><Expiration>%s</Expiration></Credentials></AssumeRoleWithWebIdentityResult></AssumeRoleWithWebIdentityResponse>`,
				time.Now().Add(time.Hour).UTC().Format(time.RFC3339))
		})
		setup := func(t *testing.T, srv *countingServer) {
			isolateAWSEnv(t)
			token := filepath.Join(t.TempDir(), "token")
			if err := os.WriteFile(token, []byte("jwt"), 0o600); err != nil {
				t.Fatal(err)
			}
			t.Setenv("AWS_WEB_IDENTITY_TOKEN_FILE", token)
			t.Setenv("AWS_ROLE_ARN", "arn:aws:iam::123456789012:role/test")
			t.Setenv("AWS_ENDPOINT_URL_STS", srv.URL)
			t.Setenv("AWS_MAX_ATTEMPTS", "1")
		}
		t.Run("refused", func(t *testing.T) {
			is := is.New(t)
			srv := newCountingServer(t, stsOK)
			setup(t, srv)
			d := &recordingDialer{refuse: true}
			_, _, err := retrieve(d.ctx(context.Background()))
			is.True(errors.Is(err, errDialRefused))
			is.Equal(d.calls(), []string{srv.addr()})
			is.Equal(srv.accepts.Load(), int64(0))
		})
		t.Run("defaults mode does not bypass the dialer", func(t *testing.T) {
			is := is.New(t)
			srv := newCountingServer(t, stsOK)
			setup(t, srv)
			t.Setenv("AWS_DEFAULTS_MODE", "standard")
			d := &recordingDialer{refuse: true}
			_, _, err := retrieve(d.ctx(context.Background()))
			is.True(errors.Is(err, errDialRefused))
			is.Equal(d.calls(), []string{srv.addr()})
			is.Equal(srv.accepts.Load(), int64(0))
		})
		t.Run("dials again after reconnect", func(t *testing.T) {
			is := is.New(t)
			srv := newCountingServer(t, stsOK)
			setup(t, srv)
			d := &recordingDialer{}
			creds, provider, err := retrieve(d.ctx(context.Background()))
			is.NoErr(err)
			is.Equal(creds.AccessKeyID, "AKIDSTS")
			provider.(*aws.CredentialsCache).Invalidate()
			_, err = provider.Retrieve(context.Background())
			is.NoErr(err)
			is.Equal(d.calls(), []string{srv.addr(), srv.addr()})
		})
	})
}

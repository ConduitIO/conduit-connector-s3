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
	"bufio"
	"bytes"
	"context"
	"encoding/pem"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	smithyendpoints "github.com/aws/smithy-go/endpoints"
	"github.com/conduitio/conduit-commons/hostpolicy"
	"github.com/matryer/is"
	"github.com/rs/zerolog"
)

const allowedBucket = "allowed-bucket"

// recorder collects the decisions a connector reports to the engine hook.
type recorder struct {
	mu sync.Mutex
	ds []hostpolicy.Decision
}

func (r *recorder) observe(_ context.Context, d hostpolicy.Decision) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ds = append(r.ds, d)
}

func (r *recorder) all() []hostpolicy.Decision {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]hostpolicy.Decision(nil), r.ds...)
}

func mustEndpoint(t *testing.T, raw string) hostpolicy.Endpoint {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	e, err := hostpolicy.NewEndpoint(u.Hostname(), u.Port())
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func enabledPolicy(t *testing.T, rec *recorder, endpoints ...string) hostpolicy.Policy {
	t.Helper()
	p := hostpolicy.Policy{AmbientCredentials: true, AllowResources: []string{allowedBucket}}
	for _, e := range endpoints {
		p.AllowEndpoints = append(p.AllowEndpoints, mustEndpoint(t, e))
	}
	if rec != nil {
		p.Observe = rec.observe
	}
	return p
}

// ambientEnv isolates the environment and provides credentials through the
// default chain's environment provider, so a test exercises the chain without
// AWS and without the developer's own credentials.
func ambientEnv(t *testing.T) {
	t.Helper()
	isolateAWSEnv(t)
	t.Setenv("AWS_ACCESS_KEY_ID", "AKIDAMBIENT")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "ambient-secret")
	t.Setenv("AWS_SESSION_TOKEN", "ambient-session-token")
}

// brokenChain makes loading the default chain fail, so a test can prove the
// chain was never loaded: LoadDefaultConfig errors on a missing profile.
func brokenChain(t *testing.T) {
	t.Helper()
	isolateAWSEnv(t)
	t.Setenv("AWS_PROFILE", "profile-that-does-not-exist")
	if _, err := LoadAWSConfig(context.Background(), "us-east-1", "", "", ""); err == nil {
		t.Fatal("precondition: loading the default chain should fail in this environment")
	}
}

func params(endpoint string) ClientParams {
	return ClientParams{Region: "us-east-1", Bucket: allowedBucket, Endpoint: endpoint, PathStyle: true}
}

func assertCode(t *testing.T, err error, code hostpolicy.Code, path string) {
	t.Helper()
	_ = wantCode(t, err, code, path)
}

func wantCode(t *testing.T, err error, code hostpolicy.Code, path string) *hostpolicy.Error {
	t.Helper()
	he, ok := hostpolicy.AsError(err)
	if !ok {
		t.Fatalf("expected %s, got %v", code, err)
	}
	if he.Code != code {
		t.Fatalf("expected %s, got %s (%v)", code, he.Code, err)
	}
	if he.ConfigPath != path {
		t.Fatalf("expected config path %q, got %q (%v)", path, he.ConfigPath, err)
	}
	return he
}

func TestNewS3Client_DisabledNeverLoadsTheChain(t *testing.T) {
	testCases := []struct {
		name string
		ctx  func(*recorder) context.Context
	}{
		{"no policy on the context (standalone plugin, older engine)", func(*recorder) context.Context { return context.Background() }},
		{"operator left it off", func(r *recorder) context.Context {
			return hostpolicy.WithPolicy(context.Background(), hostpolicy.Policy{AllowResources: []string{allowedBucket}, Observe: r.observe})
		}},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			brokenChain(t)
			rec := &recorder{}
			for _, purpose := range []Purpose{PurposeValidate, PurposeOpen} {
				_, auth, err := NewS3Client(tc.ctx(rec), params(""), purpose)
				// The disabled refusal, not the profile error: the chain was not loaded.
				assertCode(t, err, hostpolicy.CodeAmbientCredentialsDisabled, ConfigKeyAWSAccessKeyID)
				is.True(auth.Ambient())
			}
			for _, d := range rec.all() {
				is.Equal(d.Outcome, hostpolicy.OutcomeDeniedDisabled)
			}
		})
	}
}

func TestNewS3Client_OneKeyNeverFallsThrough(t *testing.T) {
	for _, pol := range []bool{false, true} {
		for _, p := range []ClientParams{
			{Region: "us-east-1", Bucket: allowedBucket, AccessKeyID: "id"},
			{Region: "us-east-1", Bucket: allowedBucket, SecretAccessKey: "secret"},
		} {
			t.Run(fmt.Sprintf("policy=%v id=%q", pol, p.AccessKeyID), func(t *testing.T) {
				brokenChain(t)
				ctx := context.Background()
				if pol {
					ctx = hostpolicy.WithPolicy(ctx, enabledPolicy(t, nil))
				}
				_, _, err := NewS3Client(ctx, p, PurposeValidate)
				want := ConfigKeyAWSSecretAccessKey
				if p.AccessKeyID == "" {
					want = ConfigKeyAWSAccessKeyID
				}
				assertCode(t, err, hostpolicy.CodeCredentialsIncomplete, want)
			})
		}
	}
}

// TestNewS3Client_StaticKeysUnaffected: static keys work with any endpoint
// and bucket, and the policy is never consulted.
func TestNewS3Client_StaticKeysUnaffected(t *testing.T) {
	isolateAWSEnv(t)
	rec := &recorder{}
	ctx := hostpolicy.WithPolicy(context.Background(), hostpolicy.Policy{Observe: rec.observe})
	for _, ep := range []string{"", "http://localhost:9000", "https://x.execute-api.us-east-1.amazonaws.com"} {
		t.Run(ep, func(t *testing.T) {
			is := is.New(t)
			p := ClientParams{Region: "us-east-1", Bucket: "any-bucket", Endpoint: ep, AccessKeyID: "id", SecretAccessKey: "secret"}
			c, auth, err := NewS3Client(ctx, p, PurposeOpen)
			is.NoErr(err)
			is.True(!auth.Ambient())
			is.True(c != nil)
		})
	}
	is.New(t).Equal(len(rec.all()), 0)
}

func TestNewS3Client_AmbientRefusals(t *testing.T) {
	testCases := []struct {
		name     string
		p        ClientParams
		allow    []string
		code     hostpolicy.Code
		path     string
		regionOK bool
	}{
		{name: "bucket not on the allowlist", p: ClientParams{Region: "us-east-1", Bucket: "someone-elses"}, code: hostpolicy.CodeResourceNotAllowed, path: ConfigKeyAWSBucket},
		{name: "access point ARN", p: ClientParams{Region: "us-east-1", Bucket: "arn:aws:s3:us-east-1:123456789012:accesspoint/ap"}, code: hostpolicy.CodeResourceNotAllowed, path: ConfigKeyAWSBucket},
		{name: "object lambda alias", p: ClientParams{Region: "us-east-1", Bucket: "x--ol-s3"}, code: hostpolicy.CodeResourceNotAllowed, path: ConfigKeyAWSBucket},
		{name: "outposts alias", p: ClientParams{Region: "us-east-1", Bucket: "x--op-s3"}, code: hostpolicy.CodeResourceNotAllowed, path: ConfigKeyAWSBucket},
		{name: "access point alias", p: ClientParams{Region: "us-east-1", Bucket: "x-s3alias"}, code: hostpolicy.CodeResourceNotAllowed, path: ConfigKeyAWSBucket},
		{name: "directory bucket", p: ClientParams{Region: "us-east-1", Bucket: "name--usw2-az1--x-s3"}, code: hostpolicy.CodeResourceNotAllowed, path: ConfigKeyAWSBucket},
		// The design's mandatory regressions: tenant-controlled AWS-suffix hosts.
		{name: "API Gateway host", p: params("https://x.execute-api.us-east-1.amazonaws.com"), code: hostpolicy.CodeEndpointNotAllowed, path: ConfigKeyAWSEndpoint},
		{name: "ELB host", p: params("https://my-lb-1234567890.us-east-1.elb.amazonaws.com"), code: hostpolicy.CodeEndpointNotAllowed, path: ConfigKeyAWSEndpoint},
		{name: "EC2 public DNS", p: params("https://ec2-203-0-113-25.compute-1.amazonaws.com"), code: hostpolicy.CodeEndpointNotAllowed, path: ConfigKeyAWSEndpoint},
		{name: "http endpoint, host listed", p: params("http://minio.internal:9000"), allow: []string{"https://minio.internal:9000"}, code: hostpolicy.CodeEndpointNotAllowed, path: ConfigKeyAWSEndpoint},
		{name: "listed host, other port", p: params("https://minio.internal:9001"), allow: []string{"https://minio.internal:9000"}, code: hostpolicy.CodeEndpointNotAllowed, path: ConfigKeyAWSEndpoint},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ambientEnv(t)
			rec := &recorder{}
			ctx := hostpolicy.WithPolicy(context.Background(), enabledPolicy(t, rec, tc.allow...))
			_, _, err := NewS3Client(ctx, tc.p, PurposeValidate)
			assertCode(t, err, tc.code, tc.path)
			ds := rec.all()
			if len(ds) != 1 || ds[0].Outcome != tc.code.Outcome() || ds[0].Err.ConfigPath != tc.path {
				t.Fatalf("expected one %s decision at %s, got %+v", tc.code.Outcome(), tc.path, ds)
			}
		})
	}

	for _, region := range []string{"us-east-1.evil.example", "US-EAST-1", "us east 1", ""} {
		t.Run("region "+region, func(t *testing.T) {
			ambientEnv(t)
			ctx := hostpolicy.WithPolicy(context.Background(), enabledPolicy(t, nil))
			_, _, err := NewS3Client(ctx, ClientParams{Region: region, Bucket: allowedBucket}, PurposeValidate)
			is.New(t).True(err != nil && strings.Contains(err.Error(), ConfigKeyAWSRegion))
		})
	}
}

func TestNewS3Client_AmbientAccepted(t *testing.T) {
	is := is.New(t)
	ambientEnv(t)
	rec := &recorder{}
	ctx := hostpolicy.WithPolicy(context.Background(), enabledPolicy(t, rec, "https://minio.internal:9000"))

	// Validate reports nothing on success.
	_, auth, err := NewS3Client(ctx, params("https://MinIO.internal:9000"), PurposeValidate)
	is.NoErr(err)
	is.True(auth.Ambient())
	is.Equal(len(rec.all()), 0)

	// Open reports use once, with the endpoint host.
	_, _, err = NewS3Client(ctx, params("https://minio.internal:9000"), PurposeOpen)
	is.NoErr(err)
	_, _, err = NewS3Client(ctx, params(""), PurposeOpen)
	is.NoErr(err)
	ds := rec.all()
	is.Equal(len(ds), 2)
	is.Equal(ds[0], hostpolicy.Used("us-east-1", "minio.internal:9000"))
	is.Equal(ds[1], hostpolicy.Used("us-east-1", "")) // SDK regional endpoint, not checked
}

// TestNewS3Client_SDKEndpointSources: every endpoint source the SDK honors is
// checked on the resolved result, not only aws.endpoint.
func TestNewS3Client_SDKEndpointSources(t *testing.T) {
	const host = "https://unlisted.example:9443"
	testCases := []struct {
		name  string
		setup func(t *testing.T)
	}{
		{"AWS_ENDPOINT_URL_S3", func(t *testing.T) { t.Setenv("AWS_ENDPOINT_URL_S3", host) }},
		{"AWS_ENDPOINT_URL (global set, service unset)", func(t *testing.T) { t.Setenv("AWS_ENDPOINT_URL", host) }},
		{"profile endpoint_url", func(t *testing.T) {
			writeProfile(t, "[profile conduit]\nendpoint_url = "+host+"\n")
		}},
		{"profile services section", func(t *testing.T) {
			writeProfile(t, "[profile conduit]\nservices = conduit-services\n\n[services conduit-services]\ns3 =\n  endpoint_url = "+host+"\n")
		}},
	}
	for _, tc := range testCases {
		t.Run(tc.name+"/refused when unlisted", func(t *testing.T) {
			is := is.New(t)
			ambientEnv(t)
			tc.setup(t)
			ctx := hostpolicy.WithPolicy(context.Background(), enabledPolicy(t, nil))
			_, _, err := NewS3Client(ctx, params(""), PurposeValidate)
			he := wantCode(t, err, hostpolicy.CodeEndpointNotAllowed, "")
			is.True(strings.Contains(he.Message, sdkConfiguredEndpoint))
		})
		t.Run(tc.name+"/accepted when listed", func(t *testing.T) {
			ambientEnv(t)
			tc.setup(t)
			ctx := hostpolicy.WithPolicy(context.Background(), enabledPolicy(t, nil, host))
			_, _, err := NewS3Client(ctx, params(""), PurposeValidate)
			is.New(t).NoErr(err)
		})
	}

	t.Run("aws.endpoint wins over the environment and is named", func(t *testing.T) {
		ambientEnv(t)
		t.Setenv("AWS_ENDPOINT_URL_S3", "https://minio.internal:9000")
		ctx := hostpolicy.WithPolicy(context.Background(), enabledPolicy(t, nil, "https://minio.internal:9000"))
		_, _, err := NewS3Client(ctx, params(host), PurposeValidate)
		assertCode(t, err, hostpolicy.CodeEndpointNotAllowed, ConfigKeyAWSEndpoint)
	})
}

func writeProfile(t *testing.T, content string) {
	t.Helper()
	f := filepath.Join(t.TempDir(), "config")
	if err := os.WriteFile(f, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AWS_CONFIG_FILE", f)
	t.Setenv("AWS_PROFILE", "conduit")
}

// TestNewS3Client_RequestTimeEndpointCheck: an endpoint swapped after
// Configure (here a per-operation override) is refused by the resolver
// wrapper before any connection is made.
func TestNewS3Client_RequestTimeEndpointCheck(t *testing.T) {
	is := is.New(t)
	ambientEnv(t)
	rec := &recorder{}
	ctx := hostpolicy.WithPolicy(context.Background(), enabledPolicy(t, rec, "https://minio.internal:9000"))
	c, _, err := NewS3Client(ctx, params("https://minio.internal:9000"), PurposeValidate)
	is.NoErr(err)

	opCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	_, err = c.HeadBucket(opCtx, &s3.HeadBucketInput{Bucket: aws.String(allowedBucket)}, func(o *s3.Options) {
		o.BaseEndpoint = aws.String("https://x.execute-api.us-east-1.amazonaws.com")
	})
	assertCode(t, err, hostpolicy.CodeEndpointNotAllowed, "")
	is.Equal(len(rec.all()), 1)
	is.Equal(rec.all()[0].Outcome, hostpolicy.OutcomeDeniedEndpoint)
}

// TestNewS3Client_LinkLocalRefusedEvenWhenListed: a metadata or link-local
// aws.endpoint is refused at Configure, even if a policy entry for it was
// built by hand (NewEndpoint refuses to build one).
func TestNewS3Client_LinkLocalRefusedEvenWhenListed(t *testing.T) {
	for _, ep := range []string{
		"https://169.254.169.254",
		"https://[fd00:ec2::254]",
		"https://[fd00:ec2::23]",
		"https://[::ffff:169.254.169.254]",
		"https://[fe80::1]",
	} {
		t.Run(ep, func(t *testing.T) {
			ambientEnv(t)
			u, _ := url.Parse(ep)
			pol := enabledPolicy(t, nil)
			pol.AllowEndpoints = []hostpolicy.Endpoint{{Host: strings.Trim(u.Hostname(), "[]"), Port: "443"}}
			_, _, err := NewS3Client(hostpolicy.WithPolicy(context.Background(), pol), params(ep), PurposeValidate)
			assertCode(t, err, hostpolicy.CodeEndpointLinkLocal, ConfigKeyAWSEndpoint)
		})
	}
}

// staticResolver sends every request to one URL, bypassing the checking
// resolver, so a test can reach the transport layer with a metadata address.
type staticResolver struct{ u url.URL }

func (r staticResolver) ResolveEndpoint(context.Context, s3.EndpointParameters) (smithyendpoints.Endpoint, error) {
	return smithyendpoints.Endpoint{URI: r.u}, nil
}

func toMetadata(o *s3.Options) {
	o.EndpointResolverV2 = staticResolver{u: url.URL{Scheme: "https", Host: "169.254.169.254"}}
	o.RetryMaxAttempts = 10
}

// TestNewS3Client_TransportRefusesLinkLocal: the S3 client's own transport
// (not only the endpoint check) refuses to dial a metadata address, and the
// refusal is not retried.
func TestNewS3Client_TransportRefusesLinkLocal(t *testing.T) {
	is := is.New(t)
	ambientEnv(t)
	ctx := hostpolicy.WithPolicy(context.Background(), enabledPolicy(t, nil, "https://minio.internal:9000"))
	c, _, err := NewS3Client(ctx, params("https://minio.internal:9000"), PurposeValidate)
	is.NoErr(err)
	opCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	start := time.Now()
	_, err = c.HeadBucket(opCtx, &s3.HeadBucketInput{Bucket: aws.String(allowedBucket)}, toMetadata)
	he, ok := hostpolicy.AsError(err)
	is.True(ok)
	is.Equal(he.Code, hostpolicy.CodeEndpointLinkLocal)
	is.True(time.Since(start) < 2*time.Second) // ten attempts would take seconds of backoff
}

// tlsStub starts an https server on loopback that answers like S3 and returns
// its URL; AWS_CA_BUNDLE makes the SDK trust it, which also proves the guarded
// client keeps the SDK's CA bundle.
func tlsStub(t *testing.T, h http.Handler) *httptest.Server {
	t.Helper()
	srv := httptest.NewTLSServer(h)
	t.Cleanup(srv.Close)
	ca := filepath.Join(t.TempDir(), "ca.pem")
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
	if err := os.WriteFile(ca, pemBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AWS_CA_BUNDLE", ca)
	return srv
}

// TestNewS3Client_PrivateEndpointNotRefused: the guard scope is link-local and
// metadata addresses only. A listed loopback/private endpoint is reached.
func TestNewS3Client_PrivateEndpointNotRefused(t *testing.T) {
	is := is.New(t)
	ambientEnv(t)
	var hits atomic.Int32
	srv := tlsStub(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		is.Equal(r.Header.Get("X-Amz-Security-Token"), "ambient-session-token") // chain credentials used
		w.WriteHeader(http.StatusOK)
	}))
	ctx := hostpolicy.WithPolicy(context.Background(), enabledPolicy(t, nil, srv.URL))
	c, _, err := NewS3Client(ctx, params(srv.URL), PurposeOpen)
	is.NoErr(err)
	_, err = c.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(allowedBucket)})
	is.NoErr(err)
	is.Equal(hits.Load(), int32(1))
}

// TestNewS3Client_Proxy runs in a child process: net/http reads the proxy
// variables once per process, so setting them in this process would leak into
// every later test.
func TestNewS3Client_Proxy(t *testing.T) {
	if os.Getenv("S3_PROXY_CHILD") != "" {
		runProxyChild(t, os.Getenv("S3_PROXY_CHILD"))
		return
	}
	for _, mode := range []string{"refused", "allowed-void"} {
		t.Run(mode, func(t *testing.T) {
			is := is.New(t)
			var connects []string
			var mu sync.Mutex
			ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
			is.NoErr(err)
			defer ln.Close()
			go func() {
				for {
					c, err := ln.Accept()
					if err != nil {
						return
					}
					go func(c net.Conn) {
						defer c.Close()
						line, _ := bufio.NewReader(c).ReadString('\n')
						mu.Lock()
						connects = append(connects, strings.TrimSpace(line))
						mu.Unlock()
						_, _ = c.Write([]byte("HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n"))
					}(c)
				}
			}()

			cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestNewS3Client_Proxy$", "-test.count=1")
			cmd.Env = []string{
				"S3_PROXY_CHILD=" + mode,
				"HTTPS_PROXY=http://" + ln.Addr().String(),
				"HOME=" + t.TempDir(),
				"PATH=" + os.Getenv("PATH"),
			}
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("child failed: %v\n%s", err, out)
			}
			mu.Lock()
			defer mu.Unlock()
			switch mode {
			case "refused":
				is.Equal(len(connects), 0) // nothing reached the proxy
			case "allowed-void":
				// Under allow-proxy the dial guard sees only the proxy: a request
				// to the metadata address goes out through it. Documented void.
				is.True(len(connects) >= 1)
				is.True(strings.HasPrefix(connects[0], "CONNECT 169.254.169.254:443"))
			}
		})
	}
}

func runProxyChild(t *testing.T, mode string) {
	is := is.New(t)
	proxy := os.Getenv("HTTPS_PROXY")
	ambientEnv(t)
	t.Setenv("HTTPS_PROXY", proxy) // keep the parent's value after isolation
	pol := enabledPolicy(t, nil, "https://minio.internal:9000")
	switch mode {
	case "refused":
		_, _, err := NewS3Client(hostpolicy.WithPolicy(context.Background(), pol), params("https://minio.internal:9000"), PurposeValidate)
		assertCode(t, err, hostpolicy.CodeProxyNotAllowed, ConfigKeyAWSEndpoint)
	case "allowed-void":
		pol.AllowProxy = true
		ctx := hostpolicy.WithPolicy(context.Background(), pol)
		c, _, err := NewS3Client(ctx, params("https://minio.internal:9000"), PurposeValidate)
		is.NoErr(err)
		opCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		_, err = c.HeadBucket(opCtx, &s3.HeadBucketInput{Bucket: aws.String(allowedBucket)},
			toMetadata, func(o *s3.Options) { o.RetryMaxAttempts = 1 })
		is.True(err != nil) // the stub proxy answers 502
	}
}

func TestScrubText(t *testing.T) {
	testCases := []struct{ in, want string }{
		{
			"User: arn:aws:sts::123456789012:assumed-role/ConduitRole/i-0abc is not authorized to perform: s3:PutObject",
			"User: arn:<redacted> is not authorized to perform: s3:PutObject",
		},
		{
			"AccessDenied: arn:aws-us-gov:iam::210987654321:role/x; request id: 123456789012345",
			"AccessDenied: arn:<redacted>; request id: 123456789012345",
		},
		{"account 123456789012 has no access", "account <redacted> has no access"},
		{`account_id="123456789012"`, `account_id="<redacted>"`},
		{"Account ID: 123456789012", "Account ID: <redacted>"},
		// 12-digit numbers outside ARN/account contexts are left alone.
		{"RequestID: 123456789012, wrote 123456789012 bytes", "RequestID: 123456789012, wrote 123456789012 bytes"},
	}
	for _, tc := range testCases {
		if got := ScrubText(tc.in); got != tc.want {
			t.Errorf("ScrubText(%q)\n got %q\nwant %q", tc.in, got, tc.want)
		}
	}
}

func TestAuthScrub(t *testing.T) {
	is := is.New(t)
	var buf bytes.Buffer
	ctx := zerolog.New(&buf).WithContext(context.Background())
	orig := fmt.Errorf("operation error S3: PutObject: %w",
		errors.New("AccessDenied: User: arn:aws:sts::123456789012:assumed-role/R/s is not authorized"))

	// Static credentials: the identity is the author's own, nothing changes.
	is.Equal(Auth{}.Scrub(ctx, orig), orig)
	is.Equal(buf.Len(), 0)

	got := Auth{ambient: true}.Scrub(ctx, orig)
	is.True(!strings.Contains(got.Error(), "123456789012"))
	is.True(errors.Is(got, orig))                           // classification still works
	is.True(strings.Contains(buf.String(), "123456789012")) // full text in the operator's log
	is.Equal(Auth{ambient: true}.Scrub(ctx, nil), nil)
}

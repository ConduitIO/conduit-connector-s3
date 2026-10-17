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
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/aws-sdk-go-v2/service/sts/types"
	"github.com/conduitio/conduit-commons/hostpolicy"
	"github.com/matryer/is"
)

// These tests run the ambient-credentials path end to end against MinIO over
// TLS, with no AWS account: MinIO's STS mints real temporary credentials, and
// a mocked EC2 metadata service (the instance-role path) or a mocked STS
// AssumeRoleWithWebIdentity endpoint (the IRSA path) hands them to the AWS SDK
// default chain. Run with `make test-integration-ambient`.
//
// Environment: MINIO_TLS_ENDPOINT (https://127.0.0.1:<port>), MINIO_TLS_CA
// (PEM of the server certificate), MINIO_ROOT_USER, MINIO_ROOT_PASSWORD.

const ambientBucket = "conduit-s3-ambient-test"

type minioEnv struct {
	endpoint, ca, user, password string
}

func requireMinIOTLS(t *testing.T) minioEnv {
	t.Helper()
	e := minioEnv{
		endpoint: os.Getenv("MINIO_TLS_ENDPOINT"),
		ca:       os.Getenv("MINIO_TLS_CA"),
		user:     os.Getenv("MINIO_ROOT_USER"),
		password: os.Getenv("MINIO_ROOT_PASSWORD"),
	}
	if e.endpoint == "" || e.ca == "" || e.user == "" || e.password == "" {
		t.Skip("MINIO_TLS_ENDPOINT, MINIO_TLS_CA, MINIO_ROOT_USER and MINIO_ROOT_PASSWORD are required; run make test-integration-ambient")
	}
	return e
}

// rootConfig is the operator's own (static) access, used to set up and check.
func (e minioEnv) rootConfig(t *testing.T) aws.Config {
	t.Helper()
	cfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(e.user, e.password, "")),
	)
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func (e minioEnv) rootS3(t *testing.T) *awss3.Client {
	t.Helper()
	return awss3.NewFromConfig(e.rootConfig(t), func(o *awss3.Options) {
		o.BaseEndpoint = aws.String(e.endpoint)
		o.UsePathStyle = true
	})
}

// mintTemporaryCredentials gets real session credentials from MinIO's STS, so
// the requests the connector signs with them carry X-Amz-Security-Token and
// are accepted by the server only if the token is valid.
func (e minioEnv) mintTemporaryCredentials(t *testing.T) types.Credentials {
	t.Helper()
	client := sts.NewFromConfig(e.rootConfig(t), func(o *sts.Options) { o.BaseEndpoint = aws.String(e.endpoint) })
	out, err := client.AssumeRole(context.Background(), &sts.AssumeRoleInput{
		RoleArn:         aws.String("arn:minio:iam:::role/conduit-ambient-test"),
		RoleSessionName: aws.String("conduit-ambient-test"),
		DurationSeconds: aws.Int32(900),
	})
	if err != nil {
		t.Fatalf("MinIO AssumeRole: %v", err)
	}
	return *out.Credentials
}

func setupAmbientMinIO(t *testing.T) (minioEnv, types.Credentials) {
	t.Helper()
	e := requireMinIOTLS(t)
	isolateAWSEnv(t)
	t.Setenv("AWS_CA_BUNDLE", e.ca)
	_, err := e.rootS3(t).CreateBucket(context.Background(), &awss3.CreateBucketInput{Bucket: aws.String(ambientBucket)})
	if err != nil && !strings.Contains(err.Error(), "BucketAlreadyOwnedByYou") {
		t.Fatalf("create bucket: %v", err)
	}
	return e, e.mintTemporaryCredentials(t)
}

func (e minioEnv) policyCtx(t *testing.T) context.Context {
	t.Helper()
	return ambientCtxFor(t, e.endpoint, ambientBucket)
}

func ambientCtxFor(t *testing.T, endpoint, bucket string) context.Context {
	t.Helper()
	ctx := ambientCtx(t, endpoint)
	p := hostpolicy.FromContext(ctx)
	p.AllowResources = []string{bucket}
	return hostpolicy.WithPolicy(context.Background(), p)
}

func (e minioEnv) cfg(prefix string) map[string]string {
	return map[string]string{
		"aws.region": "us-east-1", "aws.bucket": ambientBucket, "aws.endpoint": e.endpoint,
		"aws.pathStyle": "true", "format": "json", "prefix": prefix,
	}
}

func (e minioEnv) objectCount(t *testing.T, prefix string) int {
	t.Helper()
	out, err := e.rootS3(t).ListObjectsV2(context.Background(), &awss3.ListObjectsV2Input{
		Bucket: aws.String(ambientBucket), Prefix: aws.String(prefix),
	})
	if err != nil {
		t.Fatal(err)
	}
	return len(out.Contents)
}

// imdsStub is a minimal IMDSv2 serving one instance-role credential.
func imdsStub(t *testing.T, c types.Credentials) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var credHits atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /latest/api/token", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("X-Aws-Ec2-Metadata-Token-Ttl-Seconds", "21600")
		_, _ = w.Write([]byte("imds-session-token"))
	})
	mux.HandleFunc("GET /latest/meta-data/iam/security-credentials/", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Aws-Ec2-Metadata-Token") != "imds-session-token" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		_, _ = w.Write([]byte("conduit-role"))
	})
	mux.HandleFunc("GET /latest/meta-data/iam/security-credentials/conduit-role", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Aws-Ec2-Metadata-Token") != "imds-session-token" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		credHits.Add(1)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"Code": "Success", "Type": "AWS-HMAC",
			"AccessKeyId": *c.AccessKeyId, "SecretAccessKey": *c.SecretAccessKey, "Token": *c.SessionToken,
			"Expiration":  c.Expiration.UTC().Format(time.RFC3339),
			"LastUpdated": time.Now().UTC().Format(time.RFC3339),
		})
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, &credHits
}

// TestMinIOAmbient_InstanceRole: the EC2 instance-role provider (via a mocked
// IMDS) supplies session credentials; the destination writes and the source
// opens through the SDK plugin wrappers under the operator policy.
func TestMinIOAmbient_InstanceRole(t *testing.T) {
	is := is.New(t)
	e, creds := setupAmbientMinIO(t)
	imds, credHits := imdsStub(t, creds)
	t.Setenv("AWS_EC2_METADATA_DISABLED", "false")
	t.Setenv("AWS_EC2_METADATA_SERVICE_ENDPOINT", imds.URL)

	t.Run("refused without the operator policy, and the role is never fetched", func(t *testing.T) {
		err := openSource(context.Background(), t, e.cfg("refused/"))
		he, ok := hostpolicy.AsError(err)
		is.True(ok)
		is.Equal(he.Code, hostpolicy.CodeAmbientCredentialsDisabled)
		is.Equal(credHits.Load(), int32(0))
	})

	t.Run("bucket not on the allowlist is refused before any request", func(t *testing.T) {
		err := openSource(ambientCtxFor(t, e.endpoint, "some-other-bucket"), t, e.cfg("refused/"))
		he, ok := hostpolicy.AsError(err)
		is.True(ok)
		is.Equal(he.Code, hostpolicy.CodeResourceNotAllowed)
		is.Equal(credHits.Load(), int32(0))
	})

	t.Run("allowed: write and read with the instance role", func(t *testing.T) {
		prefix := fmt.Sprintf("imds-%d/", time.Now().UnixNano())
		ackErr := writeOneRecord(e.policyCtx(t), t, e.cfg(prefix))
		is.Equal(ackErr, "")
		is.Equal(e.objectCount(t, prefix), 1)
		is.NoErr(openSource(e.policyCtx(t), t, e.cfg(prefix)))
		is.True(credHits.Load() >= 1) // credentials came from the (mocked) instance role
	})
}

// TestMinIOAmbient_WebIdentity: the web-identity provider (the IRSA path)
// exchanges a token file for session credentials at a mocked STS endpoint.
// AWS_ENDPOINT_URL_STS is outside the policy by design (it is operator
// environment), so it needs no allowlist entry.
func TestMinIOAmbient_WebIdentity(t *testing.T) {
	is := is.New(t)
	e, creds := setupAmbientMinIO(t)

	var gotToken, gotRole atomic.Value
	gotToken.Store("")
	gotRole.Store("")
	stsStub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		if r.Form.Get("Action") != "AssumeRoleWithWebIdentity" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		gotToken.Store(r.Form.Get("WebIdentityToken"))
		gotRole.Store(r.Form.Get("RoleArn"))
		w.Header().Set("Content-Type", "text/xml")
		_, _ = fmt.Fprintf(w, `<AssumeRoleWithWebIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
<AssumeRoleWithWebIdentityResult>
<Credentials><AccessKeyId>%s</AccessKeyId><SecretAccessKey>%s</SecretAccessKey><SessionToken>%s</SessionToken><Expiration>%s</Expiration></Credentials>
<AssumedRoleUser><Arn>arn:aws:sts::123456789012:assumed-role/conduit/s</Arn><AssumedRoleId>AROAEXAMPLE:s</AssumedRoleId></AssumedRoleUser>
</AssumeRoleWithWebIdentityResult>
<ResponseMetadata><RequestId>1</RequestId></ResponseMetadata>
</AssumeRoleWithWebIdentityResponse>`, *creds.AccessKeyId, *creds.SecretAccessKey, *creds.SessionToken,
			creds.Expiration.UTC().Format(time.RFC3339))
	}))
	defer stsStub.Close()

	tokenFile := filepath.Join(t.TempDir(), "token")
	is.NoErr(os.WriteFile(tokenFile, []byte("fake-oidc-jwt"), 0o600))
	t.Setenv("AWS_WEB_IDENTITY_TOKEN_FILE", tokenFile)
	t.Setenv("AWS_ROLE_ARN", "arn:aws:iam::123456789012:role/conduit")
	t.Setenv("AWS_ROLE_SESSION_NAME", "conduit-test")
	t.Setenv("AWS_ENDPOINT_URL_STS", stsStub.URL)

	prefix := fmt.Sprintf("webid-%d/", time.Now().UnixNano())
	ackErr := writeOneRecord(e.policyCtx(t), t, e.cfg(prefix))
	is.Equal(ackErr, "")
	is.Equal(e.objectCount(t, prefix), 1)
	is.Equal(gotToken.Load(), "fake-oidc-jwt")
	is.Equal(gotRole.Load(), "arn:aws:iam::123456789012:role/conduit")
}

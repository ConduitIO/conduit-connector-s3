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

package destination_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/conduitio/conduit-commons/opencdc"
	s3Conn "github.com/conduitio/conduit-connector-s3"
	"github.com/conduitio/conduit-connector-s3/config"
	"github.com/conduitio/conduit-connector-s3/destination"
	sdk "github.com/conduitio/conduit-connector-sdk"

	cconfig "github.com/conduitio/conduit-commons/config"
)

const (
	headerSSE         = "X-Amz-Server-Side-Encryption"
	headerSSEKMSKeyID = "X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id"
)

// putCapture is a fake S3 endpoint that accepts every PutObject and records
// the encryption headers of the last one.
type putCapture struct {
	mu      sync.Mutex
	puts    int
	headers http.Header
}

func (c *putCapture) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	_, _ = io.Copy(io.Discard, r.Body)
	if r.Method == http.MethodPut {
		c.mu.Lock()
		c.puts++
		c.headers = r.Header.Clone()
		c.mu.Unlock()
	}
	w.Header().Set("ETag", `"d41d8cd98f00b204e9800998ecf8427e"`)
	w.WriteHeader(http.StatusOK)
}

func (c *putCapture) last() (int, http.Header) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.puts, c.headers
}

// writeOneRecord runs the production destination (config parsing, Open,
// Write) against the fake endpoint with the extra settings and returns the
// headers of the PutObject request it sent.
func writeOneRecord(t *testing.T, extra map[string]string) http.Header {
	t.Helper()
	ctx := context.Background()
	capture := &putCapture{}
	srv := httptest.NewServer(capture)
	t.Cleanup(srv.Close)
	t.Setenv("AWS_ENDPOINT_URL", "") // only aws.endpoint may point the client anywhere

	cfg := map[string]string{
		config.ConfigKeyAWSAccessKeyID:       "fake-access-key",
		config.ConfigKeyAWSSecretAccessKey:   "fake-secret-key",
		config.ConfigKeyAWSRegion:            "us-east-1",
		config.ConfigKeyAWSBucket:            "sse-test-bucket",
		config.ConfigKeyAWSEndpoint:          srv.URL,
		config.ConfigKeyAWSPathStyle:         "true",
		destination.ConfigKeyFormat:          "json",
		"sdk.schema.extract.key.enabled":     "false",
		"sdk.schema.extract.payload.enabled": "false",
	}
	for k, v := range extra {
		cfg[k] = v
	}

	underTest := destination.NewDestination()
	if err := sdk.Util.ParseConfig(ctx, cfg, underTest.Config(), s3Conn.Connector.NewSpecification().DestinationParams); err != nil {
		t.Fatalf("parse config: %v", err)
	}
	if err := underTest.Open(ctx); err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = underTest.Teardown(ctx) })

	n, err := underTest.Write(ctx, []opencdc.Record{{
		Operation: opencdc.OperationCreate,
		Position:  opencdc.Position("p1"),
		Key:       opencdc.RawData("k1"),
		Payload:   opencdc.Change{After: opencdc.RawData("v1")},
	}})
	if err != nil || n != 1 {
		t.Fatalf("write: n=%d err=%v", n, err)
	}
	puts, headers := capture.last()
	if puts != 1 {
		t.Fatalf("expected 1 PutObject, got %d", puts)
	}
	return headers
}

// TestDestination_SSEDefaultSendsNoHeader is the regression test for #1047:
// the destination sent ServerSideEncryption=AES256 on every PutObject, and an
// explicit header overrides the bucket's default encryption, so a bucket whose
// default is SSE-KMS silently got SSE-S3 objects. With no encryption setting
// the request must carry no encryption header, so the bucket default applies.
func TestDestination_SSEDefaultSendsNoHeader(t *testing.T) {
	headers := writeOneRecord(t, nil)
	if v := headers.Get(headerSSE); v != "" {
		t.Fatalf("%s = %q, want no header so the bucket default encryption applies", headerSSE, v)
	}
	if v := headers.Get(headerSSEKMSKeyID); v != "" {
		t.Fatalf("%s = %q, want no header", headerSSEKMSKeyID, v)
	}
}

// TestDestination_SSEConfigured checks that a configured mode, and a KMS key
// with a KMS mode, reach the PutObject request.
func TestDestination_SSEConfigured(t *testing.T) {
	const keyARN = "arn:aws:kms:us-east-1:111122223333:key/1234abcd-12ab-34cd-56ef-1234567890ab"
	tests := []struct {
		name    string
		extra   map[string]string
		wantSSE string
		wantKey string
	}{
		{"AES256", map[string]string{destination.ConfigKeyServerSideEncryption: "AES256"}, "AES256", ""},
		{"kms bucket key", map[string]string{destination.ConfigKeyServerSideEncryption: "aws:kms"}, "aws:kms", ""},
		{"kms with key", map[string]string{
			destination.ConfigKeyServerSideEncryption: "aws:kms",
			destination.ConfigKeySSEKMSKeyID:          keyARN,
		}, "aws:kms", keyARN},
		{"dsse with key", map[string]string{
			destination.ConfigKeyServerSideEncryption: "aws:kms:dsse",
			destination.ConfigKeySSEKMSKeyID:          keyARN,
		}, "aws:kms:dsse", keyARN},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			headers := writeOneRecord(t, tt.extra)
			if got := headers.Get(headerSSE); got != tt.wantSSE {
				t.Errorf("%s = %q, want %q", headerSSE, got, tt.wantSSE)
			}
			if got := headers.Get(headerSSEKMSKeyID); got != tt.wantKey {
				t.Errorf("%s = %q, want %q", headerSSEKMSKeyID, got, tt.wantKey)
			}
		})
	}
}

// TestDestination_SSEConfigRejected checks that an unknown mode, or a KMS key
// with no KMS mode, fails at configuration time instead of writing objects
// with weaker encryption than asked for.
func TestDestination_SSEConfigRejected(t *testing.T) {
	tests := []struct {
		name  string
		extra map[string]string
		want  string
	}{
		{"unknown mode", map[string]string{destination.ConfigKeyServerSideEncryption: "aes256"}, destination.ConfigKeyServerSideEncryption},
		{"key without mode", map[string]string{destination.ConfigKeySSEKMSKeyID: "alias/my-key"}, destination.ConfigKeySSEKMSKeyID},
		{"key with AES256", map[string]string{
			destination.ConfigKeyServerSideEncryption: "AES256",
			destination.ConfigKeySSEKMSKeyID:          "alias/my-key",
		}, destination.ConfigKeySSEKMSKeyID},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := map[string]string{
				config.ConfigKeyAWSAccessKeyID:     "fake-access-key",
				config.ConfigKeyAWSSecretAccessKey: "fake-secret-key",
				config.ConfigKeyAWSRegion:          "us-east-1",
				config.ConfigKeyAWSBucket:          "sse-test-bucket",
				destination.ConfigKeyFormat:        "json",
			}
			for k, v := range tt.extra {
				cfg[k] = v
			}
			underTest := destination.NewDestination()
			err := sdk.Util.ParseConfig(context.Background(), cfg, underTest.Config(), s3Conn.Connector.NewSpecification().DestinationParams)
			if err == nil {
				t.Fatal("expected a configuration error")
			}
			if !errors.Is(err, cconfig.ErrInvalidParameterValue) {
				t.Errorf("error %v does not wrap ErrInvalidParameterValue", err)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("error %q does not name %s", err, tt.want)
			}
		})
	}
}

// A KMS failure on PutObject (unknown key, no permission) must name the
// setting, so the error points at the fix.
func TestDestination_KMSPutErrorNamesTheKey(t *testing.T) {
	ctx := context.Background()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		if r.Method == http.MethodPut {
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = io.WriteString(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>KMS.NotFoundException</Code><Message>Invalid keyId</Message></Error>`)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	t.Setenv("AWS_ENDPOINT_URL", "")

	cfg := map[string]string{
		config.ConfigKeyAWSAccessKeyID:            "fake-access-key",
		config.ConfigKeyAWSSecretAccessKey:        "fake-secret-key",
		config.ConfigKeyAWSRegion:                 "us-east-1",
		config.ConfigKeyAWSBucket:                 "sse-test-bucket",
		config.ConfigKeyAWSEndpoint:               srv.URL,
		config.ConfigKeyAWSPathStyle:              "true",
		destination.ConfigKeyFormat:               "json",
		destination.ConfigKeyServerSideEncryption: "aws:kms",
		destination.ConfigKeySSEKMSKeyID:          "no-such-key",
		"sdk.schema.extract.key.enabled":          "false",
		"sdk.schema.extract.payload.enabled":      "false",
	}
	d := destination.NewDestination()
	if err := sdk.Util.ParseConfig(ctx, cfg, d.Config(), s3Conn.Connector.NewSpecification().DestinationParams); err != nil {
		t.Fatal(err)
	}
	if err := d.Open(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Teardown(ctx) })
	_, err := d.Write(ctx, []opencdc.Record{{Operation: opencdc.OperationCreate, Position: opencdc.Position("p1"), Payload: opencdc.Change{After: opencdc.RawData("v")}}})
	if err == nil {
		t.Fatal("want an error")
	}
	if !strings.Contains(err.Error(), destination.ConfigKeySSEKMSKeyID) || !strings.Contains(err.Error(), "KMS.NotFoundException") {
		t.Fatalf("error should name %s and keep the cause: %v", destination.ConfigKeySSEKMSKeyID, err)
	}
}

// Configuration values are trimmed by the SDK's parser, so a key id pasted with
// surrounding whitespace reaches S3 trimmed. Config.Validate additionally
// rejects whitespace for callers that fill the struct directly.
func TestDestination_KMSKeyIDWhitespace(t *testing.T) {
	headers := writeOneRecord(t, map[string]string{
		destination.ConfigKeyServerSideEncryption: "aws:kms",
		destination.ConfigKeySSEKMSKeyID:          "  alias/my-key\t",
	})
	if got := headers.Get(headerSSEKMSKeyID); got != "alias/my-key" {
		t.Fatalf("%s = %q, want the trimmed key", headerSSEKMSKeyID, got)
	}

	c := destination.Config{ServerSideEncryption: "aws:kms", SSEKMSKeyID: " alias/my-key"}
	err := c.Validate(context.Background())
	if err == nil || !strings.Contains(err.Error(), destination.ConfigKeySSEKMSKeyID) {
		t.Fatalf("Validate should reject a key id with whitespace and name the setting, got %v", err)
	}
}

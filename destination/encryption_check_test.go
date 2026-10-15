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
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	s3Conn "github.com/conduitio/conduit-connector-s3"
	"github.com/conduitio/conduit-connector-s3/config"
	"github.com/conduitio/conduit-connector-s3/destination"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/rs/zerolog"
)

// encryptionStore answers GetBucketEncryption with a fixed response and counts
// the calls; everything else gets a 200.
type encryptionStore struct {
	status int
	body   string
	calls  atomic.Int32
}

func (s *encryptionStore) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	_, _ = io.Copy(io.Discard, r.Body)
	if r.Method == http.MethodGet && r.URL.Query().Has("encryption") {
		s.calls.Add(1)
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(s.status)
		_, _ = io.WriteString(w, s.body)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// openWithLog opens the production destination against store and returns
// what it logged at debug level and above.
func openWithLog(t *testing.T, store *encryptionStore, extra map[string]string) string {
	t.Helper()
	srv := httptest.NewServer(store)
	t.Cleanup(srv.Close)
	t.Setenv("AWS_ENDPOINT_URL", "")
	var buf bytes.Buffer
	logger := zerolog.New(&buf).Level(zerolog.DebugLevel)
	ctx := logger.WithContext(context.Background())

	cfg := map[string]string{
		config.ConfigKeyAWSAccessKeyID:       "fake-access-key",
		config.ConfigKeyAWSSecretAccessKey:   "fake-secret-key",
		config.ConfigKeyAWSRegion:            "us-east-1",
		config.ConfigKeyAWSBucket:            "enc-check-bucket",
		config.ConfigKeyAWSEndpoint:          srv.URL,
		config.ConfigKeyAWSPathStyle:         "true",
		destination.ConfigKeyFormat:          "json",
		"sdk.schema.extract.key.enabled":     "false",
		"sdk.schema.extract.payload.enabled": "false",
	}
	for k, v := range extra {
		cfg[k] = v
	}
	d := destination.NewDestination()
	if err := sdk.Util.ParseConfig(ctx, cfg, d.Config(), s3Conn.Connector.NewSpecification().DestinationParams); err != nil {
		t.Fatalf("parse config: %v", err)
	}
	if err := d.Open(ctx); err != nil {
		t.Fatalf("open must not fail on the encryption check: %v", err)
	}
	t.Cleanup(func() { _ = d.Teardown(ctx) })
	return buf.String()
}

const (
	noEncryptionConfig = `<?xml version="1.0" encoding="UTF-8"?><Error><Code>ServerSideEncryptionConfigurationNotFoundError</Code><Message>The server side encryption configuration was not found</Message></Error>`
	accessDenied       = `<?xml version="1.0" encoding="UTF-8"?><Error><Code>AccessDenied</Code><Message>Access Denied</Message></Error>`
	kmsDefault         = `<?xml version="1.0" encoding="UTF-8"?><ServerSideEncryptionConfiguration><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>aws:kms</SSEAlgorithm><KMSMasterKeyID>my-key</KMSMasterKeyID></ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>`
)

// With no encryption configured the destination sends no header (#1047), so a
// bucket without default encryption stores objects unencrypted. Open warns,
// and never refuses to start.
func TestDestination_WarnsWhenBucketIsUnencrypted(t *testing.T) {
	tests := []struct {
		name      string
		store     *encryptionStore
		extra     map[string]string
		wantWarn  bool
		wantCalls int32
	}{
		{"no bucket default", &encryptionStore{status: 404, body: noEncryptionConfig}, nil, true, 1},
		{"bucket default aws:kms", &encryptionStore{status: 200, body: kmsDefault}, nil, false, 1},
		{"access denied", &encryptionStore{status: 403, body: accessDenied}, nil, false, 1},
		{"store without the API", &encryptionStore{status: 501, body: ""}, nil, false, 1},
		{"encryption configured: no check", &encryptionStore{status: 404, body: noEncryptionConfig}, map[string]string{destination.ConfigKeyServerSideEncryption: "AES256"}, false, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			logs := openWithLog(t, tc.store, tc.extra)
			gotWarn := strings.Contains(logs, "s3.destination.bucket_unencrypted")
			if gotWarn != tc.wantWarn {
				t.Fatalf("warned=%v, want %v; logs:\n%s", gotWarn, tc.wantWarn, logs)
			}
			if gotWarn && (!strings.Contains(logs, `"level":"warn"`) || !strings.Contains(logs, destination.ConfigKeyServerSideEncryption)) {
				t.Fatalf("the warning must be at warn level and name %s; logs:\n%s", destination.ConfigKeyServerSideEncryption, logs)
			}
			if got := tc.store.calls.Load(); got != tc.wantCalls {
				t.Fatalf("GetBucketEncryption called %d times, want %d", got, tc.wantCalls)
			}
		})
	}
}

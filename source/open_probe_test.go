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

package source_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	s3Conn "github.com/conduitio/conduit-connector-s3"
	"github.com/conduitio/conduit-connector-s3/config"
	"github.com/conduitio/conduit-connector-s3/source"
	"github.com/conduitio/conduit-connector-s3/source/iterator"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
	"github.com/rs/zerolog"
)

// probeStore answers HeadBucket and the two listings Open probes; versions
// and uploads return the configured status and S3 error code.
type probeStore struct {
	versionsStatus int
	versionsCode   string
	uploadsStatus  int
	uploadsCode    string
}

func (p *probeStore) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	_, _ = io.Copy(io.Discard, r.Body)
	w.Header().Set("Content-Type", "application/xml")
	q := r.URL.Query()
	fail := func(status int, code string) {
		w.WriteHeader(status)
		_, _ = io.WriteString(w, `<?xml version="1.0"?><Error><Code>`+code+`</Code><Message>injected</Message></Error>`)
	}
	switch {
	case r.Method == http.MethodHead:
		w.WriteHeader(http.StatusOK)
	case q.Has("versions"):
		if p.versionsStatus != 0 {
			fail(p.versionsStatus, p.versionsCode)
			return
		}
		_, _ = io.WriteString(w, `<?xml version="1.0"?><ListVersionsResult><IsTruncated>false</IsTruncated></ListVersionsResult>`)
	case q.Has("uploads"):
		if p.uploadsStatus != 0 {
			fail(p.uploadsStatus, p.uploadsCode)
			return
		}
		_, _ = io.WriteString(w, `<?xml version="1.0"?><ListMultipartUploadsResult><Bucket>b</Bucket><IsTruncated>false</IsTruncated></ListMultipartUploadsResult>`)
	default:
		w.WriteHeader(http.StatusOK)
	}
}

func openAgainst(t *testing.T, store http.Handler, extra map[string]string) (string, error) {
	t.Helper()
	srv := httptest.NewServer(store)
	t.Cleanup(srv.Close)
	t.Setenv("AWS_ENDPOINT_URL", "")
	var logs bytes.Buffer
	l := zerolog.New(&logs)
	ctx := l.WithContext(context.Background())
	cfg := map[string]string{
		config.ConfigKeyAWSAccessKeyID:       "fake",
		config.ConfigKeyAWSSecretAccessKey:   "fake",
		config.ConfigKeyAWSRegion:            "us-east-1",
		config.ConfigKeyAWSBucket:            "probe-bucket",
		config.ConfigKeyAWSEndpoint:          srv.URL,
		config.ConfigKeyAWSPathStyle:         "true",
		"sdk.schema.extract.key.enabled":     "false",
		"sdk.schema.extract.payload.enabled": "false",
	}
	for k, v := range extra {
		cfg[k] = v
	}
	s := &source.Source{}
	if err := sdk.Util.ParseConfig(ctx, cfg, s.Config(), s3Conn.Connector.NewSpecification().SourceParams); err != nil {
		t.Fatalf("parse config: %v", err)
	}
	err := s.Open(ctx, nil)
	t.Cleanup(func() { _ = s.Teardown(ctx) })
	return logs.String(), err
}

func TestOpen_Probes(t *testing.T) {
	t.Run("defaults pass", func(t *testing.T) {
		is := is.New(t)
		_, err := openAgainst(t, &probeStore{}, nil)
		is.NoErr(err)
	})
	t.Run("versions listing not implemented: unsupported store", func(t *testing.T) {
		is := is.New(t)
		_, err := openAgainst(t, &probeStore{versionsStatus: 501, versionsCode: "NotImplemented"}, nil)
		is.True(errors.Is(err, source.ErrStoreUnsupported))
	})
	t.Run("versions listing denied names the permission", func(t *testing.T) {
		is := is.New(t)
		_, err := openAgainst(t, &probeStore{versionsStatus: 403, versionsCode: "AccessDenied"}, nil)
		is.True(err != nil && strings.Contains(err.Error(), "s3:ListBucketVersions"))
	})
	t.Run("uploads listing denied fails Open with the code and both fixes", func(t *testing.T) {
		is := is.New(t)
		_, err := openAgainst(t, &probeStore{uploadsStatus: 403, uploadsCode: "AccessDenied"}, nil)
		is.True(errors.Is(err, iterator.ErrMultipartListingDenied))
		is.True(strings.Contains(err.Error(), "s3:ListBucketMultipartUploads"))
		is.True(strings.Contains(err.Error(), source.ConfigKeyMultipartHold))
	})
	t.Run("opt-out skips the uploads probe and warns", func(t *testing.T) {
		is := is.New(t)
		logs, err := openAgainst(t, &probeStore{uploadsStatus: 403, uploadsCode: "AccessDenied"}, map[string]string{source.ConfigKeyMultipartHold: "false"})
		is.NoErr(err)
		is.True(strings.Contains(logs, "s3.source.multipart_hold_disabled"))
	})
	t.Run("S3 Express directory bucket is rejected", func(t *testing.T) {
		is := is.New(t)
		_, err := openAgainst(t, &probeStore{}, map[string]string{config.ConfigKeyAWSBucket: "data--usw2-az1--x-s3"})
		is.True(errors.Is(err, source.ErrStoreUnsupported))
	})
	t.Run("short lookback warns", func(t *testing.T) {
		is := is.New(t)
		for _, lb := range []string{"0s", "10s"} {
			logs, err := openAgainst(t, &probeStore{}, map[string]string{source.ConfigKeyLookbackPeriod: lb})
			is.NoErr(err)
			is.True(strings.Contains(logs, "s3.source.lookback_short"))
		}
		logs, err := openAgainst(t, &probeStore{}, map[string]string{source.ConfigKeyLookbackPeriod: "30s"})
		is.NoErr(err)
		is.True(!strings.Contains(logs, "s3.source.lookback_short"))
	})
}

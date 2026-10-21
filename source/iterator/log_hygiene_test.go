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

package iterator

import (
	"bytes"
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-s3/source/position"
	"github.com/matryer/is"
	"github.com/rs/zerolog"
)

// An S3 object key is the record key and is also embedded in the position, so
// neither may be written to the log.
const secretKey = "customers/SECRET-acme/invoice-4711.csv"

func debugLogger() (*zerolog.Logger, *bytes.Buffer) {
	var buf bytes.Buffer
	l := zerolog.New(&buf).Level(zerolog.TraceLevel)
	return &l, &buf
}

func TestSnapshot_LogsDoNotContainObjectKeys(t *testing.T) {
	is := is.New(t)
	st := newSnapStore(tNow)
	st.put(secretKey+"-same", tNow.Add(-time.Hour), `"e-s"`)
	st.put(secretKey+"-replaced", tNow.Add(-time.Hour), `"e-r"`)
	st.getOverride[secretKey+"-same"] = func(o snapObject) *http.Response {
		o.modified = tNow.Add(-time.Minute) // rewritten after the listing
		return objectWith(secretKey+"-same", o)
	}
	st.getOverride[secretKey+"-replaced"] = func(snapObject) *http.Response {
		return xmlResp(412, `<?xml version="1.0"?><Error><Code>PreconditionFailed</Code><Message>no</Message></Error>`)
	}
	cfg := snapCfg()
	cfg.Now = func() time.Time { return tNow }
	logger, buf := debugLogger()
	drainSnap(t, NewSnapshotIterator(cfg, st.client(), nil, logger))

	out := buf.String()
	is.True(strings.Contains(out, "object changed or was deleted"))
	is.True(strings.Contains(out, "object was overwritten"))
	is.True(strings.Contains(out, `"key_hash"`))
	is.True(!strings.Contains(out, "SECRET-acme"))
}

func TestCDC_GoneVersionLogDoesNotContainObjectKey(t *testing.T) {
	is := is.New(t)
	st := &fakeStore{handler: func(*http.Request) *http.Response {
		return xmlResp(404, `<?xml version="1.0"?><Error><Code>NoSuchVersion</Code><Message>gone</Message></Error>`)
	}}
	logger, buf := debugLogger()
	cfg := CDCConfig{Bucket: "b", Lookback: time.Minute, PollingPeriod: time.Hour, Now: func() time.Time { return tNow }}
	w := newCDCIterator(cfg, st.client(), position.CDCV2{}, logger)

	ok, err := w.deliver(context.Background(),
		candidate{key: secretKey, versionID: "v-SECRET-1", operation: opencdc.OperationCreate},
		func(opencdc.Record) error { return nil }, func() {})
	is.NoErr(err)
	is.True(!ok)

	out := buf.String()
	is.True(strings.Contains(out, "version changed or was deleted"))
	is.True(strings.Contains(out, `"key_hash"`))
	is.True(!strings.Contains(out, "SECRET"))
}

func TestCombined_UpgradeWarningsDoNotContainThePosition(t *testing.T) {
	cfg := CombinedConfig{Bucket: "b", PollingPeriod: time.Hour, Lookback: 5 * time.Minute, MultipartHold: true, MaxTrackedUploads: 8, MaxPendingUploadAge: 24 * time.Hour, MaxEarlyObjects: 16, MaxSettledBytesPerPoll: 1 << 20}
	for _, tc := range []struct{ name, pos string }{
		{"v1 snapshot", secretKey + "_s1792000000"},
		{"v1 cdc", secretKey + "_c1792000000"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			var buf bytes.Buffer
			ctx := zerolog.New(&buf).WithContext(context.Background())
			st := newSnapStore(time.Now())
			c, err := NewCombinedIterator(ctx, cfg, st.client(), opencdc.Position(tc.pos))
			is.NoErr(err)
			t.Cleanup(c.Stop)

			out := buf.String()
			is.True(strings.Contains(out, "s3.source.position_upgraded"))
			is.True(strings.Contains(out, `"position_hash"`))
			is.True(!strings.Contains(out, "SECRET"))
		})
	}
}

// Errors reach the DLQ, the API and CLI output, so a failed fetch names the
// object by key hash only.
func TestCDC_FetchErrorDoesNotContainObjectKey(t *testing.T) {
	is := is.New(t)
	st := &fakeStore{handler: func(*http.Request) *http.Response {
		return errResp(500)
	}}
	cfg := CDCConfig{Bucket: "b", Lookback: time.Minute, PollingPeriod: time.Hour, Now: func() time.Time { return tNow }}
	w := newCDCIterator(cfg, st.client(), position.CDCV2{}, nil)

	_, err := w.deliver(context.Background(),
		candidate{key: secretKey, versionID: "v1", operation: opencdc.OperationCreate},
		func(opencdc.Record) error { return nil }, func() {})
	is.True(err != nil)
	is.True(strings.Contains(err.Error(), "could not fetch S3 object (key hash "))
	is.True(!strings.Contains(err.Error(), "SECRET"))
}

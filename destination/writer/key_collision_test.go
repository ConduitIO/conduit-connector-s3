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

package writer

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-s3/destination/format"
)

// fakeBucket counts PutObjects per key. With If-None-Match: * it refuses to
// overwrite an existing key with 412, as S3 and MinIO do.
type fakeBucket struct {
	mu        sync.Mutex
	puts      map[string]int
	refused   int
	condition int // PutObjects that carried If-None-Match: *
}

func newFakeBucket() *fakeBucket { return &fakeBucket{puts: map[string]int{}} }

func (b *fakeBucket) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	_, _ = io.Copy(io.Discard, r.Body)
	if r.Method != http.MethodPut {
		w.WriteHeader(http.StatusOK)
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if r.Header.Get("If-None-Match") == "*" {
		b.condition++
		if b.puts[r.URL.Path] > 0 {
			b.refused++
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusPreconditionFailed)
			_, _ = io.WriteString(w, `<?xml version="1.0"?><Error><Code>PreconditionFailed</Code><Message>At least one of the pre-conditions you specified did not hold</Message></Error>`)
			return
		}
	}
	b.puts[r.URL.Path]++
	w.Header().Set("ETag", `"e"`)
	w.WriteHeader(http.StatusOK)
}

func (b *fakeBucket) stats() (puts, keys, overwritten int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, n := range b.puts {
		puts += n
		overwritten += n - 1
	}
	return puts, len(b.puts), overwritten
}

func newTestWriter(t *testing.T, endpoint string) *S3 {
	t.Helper()
	w, err := NewS3(context.Background(), &S3Config{
		AccessKeyID: "x", SecretAccessKey: "y", Region: "us-east-1",
		Bucket: "b", KeyPrefix: "p", Endpoint: endpoint, PathStyle: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func oneRecordBatch() *Batch {
	enc, err := format.NewEncoder(format.JSON, "", "", format.CompressionDefault)
	if err != nil {
		panic(err) // test helper: JSON with no compression always constructs
	}
	return &Batch{Format: enc, Records: []opencdc.Record{{Position: opencdc.Position("1"), Payload: opencdc.Change{After: opencdc.RawData("x")}}}}
}

// TestS3_TwoWritersNeverShareAKey is the regression test for s3#1064: keys
// were <prefix>/<UnixNano>.<ext>, so two writers on one bucket and prefix
// (two pipelines, two Conduit instances) that wrote in the same clock tick got
// the same key, and the second PutObject replaced an object whose records were
// already acked. Every PutObject must create a distinct object.
func TestS3_TwoWritersNeverShareAKey(t *testing.T) {
	bucket := newFakeBucket()
	srv := httptest.NewServer(bucket)
	t.Cleanup(srv.Close)
	ws := []*S3{newTestWriter(t, srv.URL), newTestWriter(t, srv.URL)}

	const n = 2000
	var wg sync.WaitGroup
	for _, w := range ws {
		wg.Add(1)
		go func(w *S3) {
			defer wg.Done()
			for i := 0; i < n; i++ {
				if err := w.Write(context.Background(), oneRecordBatch()); err != nil {
					t.Error(err)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	puts, keys, overwritten := bucket.stats()
	t.Logf("%d PutObjects, %d distinct keys, %d overwritten", puts, keys, overwritten)
	if puts != len(ws)*n || keys != puts || overwritten != 0 {
		t.Fatalf("every PutObject must create its own object: %d PutObjects, %d distinct keys, %d overwritten", puts, keys, overwritten)
	}
}

// frozen pins a writer's clock, the worst case of a coarse clock or one that
// stepped back.
func frozen(w *S3) { at := time.Unix(1_800_000_000, 0); w.now = func() time.Time { return at } }

// Keys stay distinct when the clock does not move: across writers by the
// writer id, within one writer by the counter.
func TestS3_KeysAreUniqueWithAFrozenClock(t *testing.T) {
	bucket := newFakeBucket()
	srv := httptest.NewServer(bucket)
	t.Cleanup(srv.Close)
	a, b := newTestWriter(t, srv.URL), newTestWriter(t, srv.URL)
	frozen(a)
	frozen(b)
	for i := 0; i < 50; i++ {
		for _, w := range []*S3{a, b} {
			if err := w.Write(context.Background(), oneRecordBatch()); err != nil {
				t.Fatal(err)
			}
		}
	}
	if puts, keys, over := bucket.stats(); keys != puts || over != 0 {
		t.Fatalf("%d PutObjects, %d distinct keys, %d overwritten", puts, keys, over)
	}
}

func TestS3_KeyFormat(t *testing.T) {
	bucket := newFakeBucket()
	srv := httptest.NewServer(bucket)
	t.Cleanup(srv.Close)
	w := newTestWriter(t, srv.URL)
	b := oneRecordBatch()
	b.Dir = "orders/partition=0"
	if err := w.Write(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	re := regexp.MustCompile(`^p/orders/partition=0/[0-9]{19}-[0-9a-f]{16}-1\.json$`)
	if len(w.FilesWritten) != 1 || !re.MatchString(w.FilesWritten[0]) {
		t.Fatalf("key %v does not match <prefix>/<dir>/<UnixNano>-<writer id>-<counter>.<ext>", w.FilesWritten)
	}
}

// With conditional writes, a key that exists anyway (forced here: two
// writers with the same id and a frozen clock) is refused by the store and the
// write fails loudly with ErrKeyExists: nothing is overwritten and the batch
// is not acknowledged.
func TestS3_ConditionalWriteRefusesAnExistingKey(t *testing.T) {
	bucket := newFakeBucket()
	srv := httptest.NewServer(bucket)
	t.Cleanup(srv.Close)
	a, b := newTestWriter(t, srv.URL), newTestWriter(t, srv.URL)
	a.ConditionalWrites, b.ConditionalWrites = true, true
	b.id = a.id
	frozen(a)
	frozen(b)
	if err := a.Write(context.Background(), oneRecordBatch()); err != nil {
		t.Fatal(err)
	}
	err := b.Write(context.Background(), oneRecordBatch())
	if !errors.Is(err, ErrKeyExists) {
		t.Fatalf("want ErrKeyExists, got %v", err)
	}
	if !strings.Contains(err.Error(), "not acknowledged") {
		t.Fatalf("error should say the records are not acknowledged: %v", err)
	}
	if _, _, over := bucket.stats(); over != 0 || bucket.refused != 1 || bucket.condition != 2 {
		t.Fatalf("overwritten=%d refused=%d conditional=%d", over, bucket.refused, bucket.condition)
	}
}

// answering is a store that answers every PutObject with status and body.
func answering(t *testing.T, status int, body string) *S3 {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		if body != "" {
			w.Header().Set("Content-Type", "application/xml")
		}
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(srv.Close)
	w := newTestWriter(t, srv.URL)
	w.ConditionalWrites = true
	return w
}

func s3Error(code, msg string) string {
	return `<?xml version="1.0"?><Error><Code>` + code + `</Code><Message>` + msg + `</Message></Error>`
}

// Every answer to a conditional PutObject maps to the code that says what
// happened; none of them acknowledges the batch (Position stays unset).
func TestS3_ConditionalWriteErrors(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
		want   error  // nil: an ordinary error, not about the condition
		text   string // part of the message
	}{
		{"412 with body", 412, s3Error("PreconditionFailed", "At least one of the pre-conditions you specified did not hold"), ErrKeyExists, "retried this writer's own PutObject"},
		{"412 with empty body", 412, "", ErrKeyExists, "duplicate under a new key"},
		{"409 conflict", 409, s3Error("ConditionalRequestConflict", "A conflicting conditional operation is currently in progress"), ErrConditionalConflict, "transient"},
		{"501 not implemented", 501, s3Error("NotImplemented", "A header you provided implies functionality that is not implemented"), ErrConditionalWritesUnsupported, "conditionalWrites"},
		{"400 naming If-None-Match", 400, s3Error("InvalidArgument", "Unsupported header If-None-Match"), ErrConditionalWritesUnsupported, "conditionalWrites"},
		{"400 naming conditional writes", 400, s3Error("InvalidRequest", "Conditional writes are not supported"), ErrConditionalWritesUnsupported, "conditionalWrites"},
		{"405 naming If-None-Match", 405, s3Error("MethodNotAllowed", "If-None-Match not allowed"), ErrConditionalWritesUnsupported, "conditionalWrites"},
		{"400 about something else", 400, s3Error("InvalidArgument", "Invalid canned ACL"), nil, "InvalidArgument"},
		{"403 access denied", 403, s3Error("AccessDenied", "Access Denied"), nil, "AccessDenied"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := answering(t, tc.status, tc.body)
			err := w.Write(context.Background(), oneRecordBatch())
			if err == nil {
				t.Fatal("want an error")
			}
			for _, code := range []error{ErrKeyExists, ErrConditionalConflict, ErrConditionalWritesUnsupported} {
				if errors.Is(err, code) != (code == tc.want) {
					t.Fatalf("errors.Is(%v) = %v, want %v; err: %v", code, errors.Is(err, code), code == tc.want, err)
				}
			}
			if !strings.Contains(err.Error(), tc.text) {
				t.Fatalf("error should mention %q: %v", tc.text, err)
			}
			if w.Position != nil || len(w.FilesWritten) != 0 {
				t.Fatalf("a failed write must not record a position or a file: %v %v", w.Position, w.FilesWritten)
			}
			// The object key is derived from record data (partition values), so
			// errors name it by hash only.
			if tc.want == ErrKeyExists || tc.want == ErrConditionalConflict {
				if !strings.Contains(err.Error(), "key hash ") || strings.Contains(err.Error(), ".json") {
					t.Fatalf("error should name the object by key hash, not by key: %v", err)
				}
			}
		})
	}
}

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
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
)

// These tests drive the iterators against a scripted HTTP store, so every
// failure shape can be produced deterministically: a failing page, a failing
// or truncated object, a broken continuation, a Date header that is not used.

type roundTripFunc func(*http.Request) *http.Response

func (f roundTripFunc) Do(r *http.Request) (*http.Response, error) { return f(r), nil }

// fakeStore records the requests it serves.
type fakeStore struct {
	mu       sync.Mutex
	requests []string
	handler  func(r *http.Request) *http.Response
	// uploads answers ListMultipartUploads; nil means no uploads in progress.
	uploads func(r *http.Request) *http.Response
}

func isUploads(r *http.Request) bool { return r.URL.Query().Has("uploads") }

const noUploads = `<?xml version="1.0"?><ListMultipartUploadsResult><Bucket>b</Bucket><IsTruncated>false</IsTruncated></ListMultipartUploadsResult>`

func (f *fakeStore) client() *s3.Client {
	return s3.New(s3.Options{
		Region:       "us-east-1",
		BaseEndpoint: aws.String("http://fake.test"),
		UsePathStyle: true,
		Credentials:  credentials.NewStaticCredentialsProvider("a", "b", ""),
		Retryer:      aws.NopRetryer{},
		HTTPClient: roundTripFunc(func(r *http.Request) *http.Response {
			f.mu.Lock()
			f.requests = append(f.requests, r.Method+" "+r.URL.Path+"?"+r.URL.RawQuery)
			f.mu.Unlock()
			var resp *http.Response
			switch {
			case isUploads(r) && f.uploads != nil:
				resp = f.uploads(r)
			case isUploads(r):
				resp = xmlResp(200, noUploads)
			default:
				resp = f.handler(r)
			}
			resp.Request = r
			if resp.Header == nil {
				resp.Header = http.Header{}
			}
			// a real store always sends Date; a test that wants it missing or
			// wrong sets it explicitly (even to "")
			if _, ok := resp.Header["Date"]; !ok {
				resp.Header.Set("Date", time.Now().UTC().Format(http.TimeFormat))
			}
			return resp
		}),
	})
}

func (f *fakeStore) count(substr string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, r := range f.requests {
		if strings.Contains(r, substr) {
			n++
		}
	}
	return n
}

func xmlResp(status int, body string, hdr ...string) *http.Response {
	h := http.Header{"Content-Type": {"application/xml"}}
	for i := 0; i+1 < len(hdr); i += 2 {
		h.Set(hdr[i], hdr[i+1])
	}
	return &http.Response{StatusCode: status, Header: h, Body: io.NopCloser(strings.NewReader(body))}
}

func errResp(status int) *http.Response {
	return xmlResp(status, `<?xml version="1.0"?><Error><Code>AccessDenied</Code><Message>injected</Message></Error>`)
}

const lm = "2026-01-02T03:04:05.000Z"

func listV2(truncated bool, token string, keys ...string) string {
	var b strings.Builder
	fmt.Fprintf(&b, `<?xml version="1.0"?><ListBucketResult><Name>b</Name><IsTruncated>%v</IsTruncated>`, truncated)
	if token != "" {
		fmt.Fprintf(&b, `<NextContinuationToken>%s</NextContinuationToken>`, token)
	}
	for _, k := range keys {
		fmt.Fprintf(&b, `<Contents><Key>%s</Key><LastModified>%s</LastModified><ETag>"e"</ETag><Size>1</Size></Contents>`, k, lm)
	}
	b.WriteString(`</ListBucketResult>`)
	return b.String()
}

func objectResp(key string) *http.Response {
	return &http.Response{
		StatusCode: 200,
		Header: http.Header{
			"Content-Type":   {"text/plain"},
			"Last-Modified":  {"Fri, 02 Jan 2026 03:04:05 GMT"},
			"Etag":           {`"e"`},
			"Content-Length": {fmt.Sprint(len(key))},
		},
		Body: io.NopCloser(strings.NewReader(key)),
	}
}

type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, errors.New("connection reset") }

func isList(r *http.Request) bool { return r.URL.Query().Get("list-type") == "2" }
func isGet(r *http.Request) bool {
	return r.Method == http.MethodGet && !isList(r) && !r.URL.Query().Has("versions") && !isUploads(r)
}
func keyOf(r *http.Request) string { return strings.TrimPrefix(r.URL.Path, "/b/") }
func token(r *http.Request) string { return r.URL.Query().Get("continuation-token") }
func newSnap(t *testing.T, c *s3.Client) *SnapshotIterator {
	t.Helper()
	return NewSnapshotIterator(SnapshotConfig{
		Bucket: "b", Lookback: 5 * time.Minute, MultipartHold: true,
		MaxTrackedUploads: DefaultMaxTrackedUploads, MaxPendingUploadAge: DefaultMaxPendingUploadAge,
	}, c, nil, nil)
}

// twoPages serves keys a,b on page 1 and c on page 2; hooks can override a
// request (return nil to fall through).
func twoPages(hook func(r *http.Request) *http.Response) func(r *http.Request) *http.Response {
	return func(r *http.Request) *http.Response {
		if hook != nil {
			if resp := hook(r); resp != nil {
				return resp
			}
		}
		switch {
		case isList(r) && token(r) == "":
			return xmlResp(200, listV2(true, "t1", "a", "b"))
		case isList(r):
			return xmlResp(200, listV2(false, "", "c"))
		default:
			return objectResp(keyOf(r))
		}
	}
}

// assertSticky checks that after a failure every further call keeps failing:
// no record, no ErrBackoffRetry (the end of the snapshot), no skipping ahead.
func assertSticky(t *testing.T, it *SnapshotIterator) {
	t.Helper()
	for i := 0; i < 3; i++ {
		if !it.HasNext(context.Background()) {
			t.Fatal("HasNext reported the end of the snapshot after a failure")
		}
		rec, err := it.Next(context.Background())
		if err == nil {
			t.Fatalf("Next returned record %q after a failure instead of the error", rec.Key.Bytes())
		}
		if errors.Is(err, sdk.ErrBackoffRetry) {
			t.Fatalf("Next reported the end of the snapshot after a failure: %v", err)
		}
	}
}

// failSnapshotPage2 fails every request for page 2.
func failSnapshotPage2() func(r *http.Request) *http.Response {
	return func(r *http.Request) *http.Response {
		if isList(r) && token(r) == "t1" {
			return errResp(403)
		}
		return nil
	}
}

func TestSnapshotIterator_ListErrorOnLaterPageIsSticky(t *testing.T) {
	is := is.New(t)
	st := &fakeStore{handler: twoPages(failSnapshotPage2())} //nolint:bodyclose // scripted responses, closed by the SDK
	it := newSnap(t, st.client())
	ctx := context.Background()

	for _, want := range []string{"a", "b"} {
		rec, err := it.Next(ctx)
		is.NoErr(err)
		is.Equal(string(rec.Key.Bytes()), want)
	}
	_, err := it.Next(ctx) // needs page 2
	is.True(err != nil)
	assertSticky(t, it)
	is.Equal(st.count("continuation-token=t1"), 1) // the one failed request: not retried behind the caller's back
	is.Equal(st.count("GET /b/c"), 0)              // and c is never read
}

// The uploads listing runs before the first page; its failure is as sticky as
// any other and no record is delivered without a cut.
func TestSnapshotIterator_UploadsListingFailureIsSticky(t *testing.T) {
	is := is.New(t)
	st := &fakeStore{handler: twoPages(nil), uploads: func(*http.Request) *http.Response { return errResp(500) }} //nolint:bodyclose // scripted responses, closed by the SDK
	it := newSnap(t, st.client())
	assertSticky(t, it)
	is.Equal(st.count("GET /b/a"), 0)
}

func TestSnapshotIterator_FirstPageErrorIsNotAnEmptyBucket(t *testing.T) {
	st := &fakeStore{handler: func(_ *http.Request) *http.Response { return errResp(403) }}
	it := newSnap(t, st.client())
	assertSticky(t, it)
}

func TestSnapshotIterator_GetObjectErrorIsStickyAndDoesNotAdvance(t *testing.T) {
	is := is.New(t)
	st := &fakeStore{handler: twoPages(func(r *http.Request) *http.Response { //nolint:bodyclose // scripted responses, closed by the SDK
		if isGet(r) && keyOf(r) == "b" {
			return errResp(500)
		}
		return nil
	})}
	it := newSnap(t, st.client())
	ctx := context.Background()

	rec, err := it.Next(ctx)
	is.NoErr(err)
	is.Equal(string(rec.Key.Bytes()), "a")
	_, err = it.Next(ctx)
	is.True(err != nil)
	assertSticky(t, it)
	is.Equal(it.index, 1) // still at b, not past it
	is.Equal(st.count("GET /b/c"), 0)
}

func TestSnapshotIterator_BodyReadErrorDoesNotAdvancePosition(t *testing.T) {
	is := is.New(t)
	st := &fakeStore{handler: twoPages(func(r *http.Request) *http.Response { //nolint:bodyclose // scripted responses, closed by the SDK
		if isGet(r) && keyOf(r) == "a" {
			resp := objectResp("a")
			resp.Body = io.NopCloser(errReader{})
			return resp
		}
		return nil
	})}
	it := newSnap(t, st.client())
	_, err := it.Next(context.Background())
	is.True(err != nil)
	is.Equal(it.index, 0) // not advanced by a record that was never delivered
	assertSticky(t, it)
}

func TestSnapshotIterator_BrokenContinuationIsNotTheEnd(t *testing.T) {
	for name, handler := range map[string]func(r *http.Request) *http.Response{
		"truncated without token": func(r *http.Request) *http.Response {
			if isList(r) {
				return xmlResp(200, listV2(true, "", "a"))
			}
			return objectResp(keyOf(r))
		},
		"repeated token": func(r *http.Request) *http.Response {
			if isList(r) {
				return xmlResp(200, listV2(true, "t1", "a"))
			}
			return objectResp(keyOf(r))
		},
	} {
		t.Run(name, func(t *testing.T) {
			is := is.New(t)
			it := newSnap(t, (&fakeStore{handler: handler}).client())
			ctx := context.Background()
			var err error
			for i := 0; i < 4 && err == nil; i++ {
				_, err = it.Next(ctx)
			}
			is.True(err != nil)
			is.True(!errors.Is(err, sdk.ErrBackoffRetry))
			assertSticky(t, it)
		})
	}
}

func TestCombinedIterator_ListErrorNeverHandsOverToCDC(t *testing.T) {
	is := is.New(t)
	st := &fakeStore{handler: twoPages(failSnapshotPage2())} //nolint:bodyclose // scripted responses, closed by the SDK
	ctx := context.Background()
	c, err := NewCombinedIterator(ctx, CombinedConfig{Bucket: "b", PollingPeriod: time.Hour, Lookback: time.Minute, MultipartHold: true, MaxTrackedUploads: 4, MaxPendingUploadAge: time.Hour}, st.client(), nil)
	is.NoErr(err)

	var keys []string
	var last error
	for i := 0; i < 6; i++ {
		is.True(c.HasNext(ctx)) // never "no more": that would switch to CDC
		rec, err := c.Next(ctx)
		if err != nil {
			last = err
			continue
		}
		keys = append(keys, string(rec.Key.Bytes()))
	}
	is.Equal(keys, []string{"a", "b"})
	is.True(last != nil)
	is.True(c.cdcIterator == nil) // no CDC iterator was started: nothing ends up after a gap
	is.True(c.snapshotIterator != nil)
}

func TestSnapshotIterator_ObjectDeletedAfterListingIsSkipped(t *testing.T) {
	is := is.New(t)
	st := &fakeStore{handler: twoPages(func(r *http.Request) *http.Response { //nolint:bodyclose // scripted responses, closed by the SDK
		if isGet(r) && keyOf(r) == "b" {
			return xmlResp(404, `<?xml version="1.0"?><Error><Code>NoSuchKey</Code><Message>gone</Message></Error>`)
		}
		return nil
	})}
	it := newSnap(t, st.client())
	ctx := context.Background()

	var got []string
	for it.HasNext(ctx) {
		rec, err := it.Next(ctx)
		if errors.Is(err, sdk.ErrBackoffRetry) {
			break
		}
		is.NoErr(err)
		got = append(got, string(rec.Key.Bytes()))
	}
	is.Equal(got, []string{"a", "c"}) // b was deleted: skipped, the snapshot goes on
}

// stepper drives a CDC iterator one poll at a time with an explicit host clock,
// so the tests do not depend on timers.

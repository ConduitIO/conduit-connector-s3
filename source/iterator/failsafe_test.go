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
	"github.com/conduitio/conduit-connector-s3/source/position"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
)

// These tests drive the iterators against a scripted HTTP store, so every
// failure shape can be produced deterministically: a failing page, a failing
// or truncated object, a broken continuation, a lying Date header.

type roundTripFunc func(*http.Request) *http.Response

func (f roundTripFunc) Do(r *http.Request) (*http.Response, error) { return f(r), nil }

// fakeStore records the requests it serves.
type fakeStore struct {
	mu       sync.Mutex
	requests []string
	handler  func(r *http.Request) *http.Response
}

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
			resp := f.handler(r)
			resp.Request = r
			if resp.Header == nil {
				resp.Header = http.Header{}
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
	return r.Method == http.MethodGet && !isList(r) && !r.URL.Query().Has("versions")
}
func keyOf(r *http.Request) string { return strings.TrimPrefix(r.URL.Path, "/b/") }
func token(r *http.Request) string { return r.URL.Query().Get("continuation-token") }
func newSnap(t *testing.T, c *s3.Client) *SnapshotIterator {
	t.Helper()
	it, err := NewSnapshotIterator("b", "", c, position.Position{})
	if err != nil {
		t.Fatal(err)
	}
	return it
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

func TestSnapshotIterator_ListErrorOnLaterPageIsSticky(t *testing.T) {
	is := is.New(t)
	st := &fakeStore{handler: twoPages(func(r *http.Request) *http.Response { //nolint:bodyclose // scripted responses, closed by the SDK
		if isList(r) && token(r) == "t1" {
			return errResp(403)
		}
		return nil
	})}
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
	is.Equal(st.count("continuation-token=t1"), 1) // the failed page is not retried behind the caller's back
	is.Equal(st.count("GET /b/c"), 0)              // and c is never read
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
	is.Equal(it.index, 0)
	is.True(it.maxLastModified.IsZero()) // not advanced by a record that was never delivered
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

func TestSnapshotIterator_StartedAtIsNeverBeyondHost(t *testing.T) {
	for name, tc := range map[string]struct {
		date string
		want func(started, host time.Time) bool
	}{
		"far future ignored":         {time.Now().Add(30 * 24 * time.Hour).UTC().Format(http.TimeFormat), func(s, h time.Time) bool { return s.Sub(h) < 5*time.Second && h.Sub(s) < 5*time.Second }},
		"far past ignored":           {time.Now().Add(-30 * 24 * time.Hour).UTC().Format(http.TimeFormat), func(s, h time.Time) bool { return h.Sub(s) < 5*time.Second }},
		"garbage ignored":            {"yesterday-ish", func(s, h time.Time) bool { return h.Sub(s) < 5*time.Second }},
		"missing ignored":            {"", func(s, h time.Time) bool { return h.Sub(s) < 5*time.Second }},
		"near future capped at host": {time.Now().Add(2 * time.Hour).UTC().Format(http.TimeFormat), func(s, h time.Time) bool { return !s.After(h) && h.Sub(s) < 5*time.Second }},
		"store behind host is used":  {time.Now().Add(-time.Hour).UTC().Format(http.TimeFormat), func(s, h time.Time) bool { d := h.Sub(s); return d > 59*time.Minute && d < 61*time.Minute }},
	} {
		t.Run(name, func(t *testing.T) {
			is := is.New(t)
			st := &fakeStore{handler: func(r *http.Request) *http.Response {
				if isList(r) {
					return xmlResp(200, listV2(false, "", "a"), "Date", tc.date)
				}
				return objectResp(keyOf(r))
			}}
			it := newSnap(t, st.client())
			is.True(it.HasNext(context.Background()))
			is.True(tc.want(it.startedAt, time.Now()))
		})
	}
}

func TestCombinedIterator_ListErrorNeverHandsOverToCDC(t *testing.T) {
	is := is.New(t)
	st := &fakeStore{handler: twoPages(func(r *http.Request) *http.Response { //nolint:bodyclose // scripted responses, closed by the SDK
		if isList(r) && token(r) == "t1" {
			return errResp(403)
		}
		return nil
	})}
	ctx := context.Background()
	c, err := NewCombinedIterator(ctx, "b", "", time.Hour, time.Minute, st.client(), position.Position{})
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

func TestCDCIterator_PollFailureDoesNotMoveState(t *testing.T) {
	is := is.New(t)
	versions := func(trunc bool, nextKey, nextVer string, keys ...string) string {
		var b strings.Builder
		fmt.Fprintf(&b, `<?xml version="1.0"?><ListVersionsResult><IsTruncated>%v</IsTruncated>`, trunc)
		if nextKey != "" {
			fmt.Fprintf(&b, `<NextKeyMarker>%s</NextKeyMarker><NextVersionIdMarker>%s</NextVersionIdMarker>`, nextKey, nextVer)
		}
		for _, k := range keys {
			fmt.Fprintf(&b, `<Version><Key>%s</Key><VersionId>null</VersionId><IsLatest>true</IsLatest><LastModified>%s</LastModified><ETag>"e"</ETag></Version>`, k, lm)
		}
		b.WriteString(`</ListVersionsResult>`)
		return b.String()
	}
	wm := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	for name, handler := range map[string]func(r *http.Request) *http.Response{
		"second page fails": func(r *http.Request) *http.Response {
			if r.URL.Query().Get("key-marker") != "" {
				return errResp(403)
			}
			return xmlResp(200, versions(true, "a", "v1", "a"))
		},
		"truncated without marker": func(_ *http.Request) *http.Response {
			return xmlResp(200, versions(true, "", "", "a"))
		},
	} {
		t.Run(name, func(*testing.T) {
			w := &CDCIterator{
				bucket: "b", client: (&fakeStore{handler: handler}).client(),
				watermark: wm, delivered: map[deliveredKey]struct{}{},
			}
			cache, _, err := w.poll(context.Background())
			is.True(err != nil)
			is.Equal(len(cache), 0)   // nothing partial is handed over
			is.Equal(w.watermark, wm) // and the watermark did not move
			is.Equal(len(w.delivered), 0)
		})
	}

	t.Run("iterator dies instead of skipping", func(*testing.T) {
		st := &fakeStore{handler: func(_ *http.Request) *http.Response { return errResp(403) }}
		it, err := NewCDCIterator("b", "", 10*time.Millisecond, time.Minute, st.client(), wm, nil)
		is.NoErr(err)
		defer it.Stop()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for !it.HasNext(ctx) {
			time.Sleep(10 * time.Millisecond)
		}
		_, err = it.Next(ctx)
		is.True(err != nil)
		is.True(!errors.Is(err, context.DeadlineExceeded))
	})
}

func versionsXML(modified string, keys ...string) string {
	var b strings.Builder
	b.WriteString(`<?xml version="1.0"?><ListVersionsResult><IsTruncated>false</IsTruncated>`)
	for _, k := range keys {
		fmt.Fprintf(&b, `<Version><Key>%s</Key><VersionId>null</VersionId><IsLatest>true</IsLatest><LastModified>%s</LastModified><ETag>"e"</ETag></Version>`, k, modified)
	}
	b.WriteString(`</ListVersionsResult>`)
	return b.String()
}

// TestCDCIterator_HostileDateNeverSkips: end to end through the iterator, with
// a store whose Date header lies. The object is modified "now" on the store's
// clock; it must be delivered whatever the header says.
func TestCDCIterator_HostileDateNeverSkips(t *testing.T) {
	for name, date := range map[string]func() string{
		"far future":  func() string { return time.Now().Add(100 * 24 * time.Hour).UTC().Format(http.TimeFormat) },
		"near future": func() string { return time.Now().Add(2 * time.Hour).UTC().Format(http.TimeFormat) },
		"far past":    func() string { return time.Now().Add(-100 * 24 * time.Hour).UTC().Format(http.TimeFormat) },
		"garbage":     func() string { return "next tuesday" },
		"missing":     func() string { return "" },
		// a host clock ahead of the store: the store's clock (Date and
		// LastModified) is 10 minutes behind, with a 5 minute lookback
		"host ahead of store": func() string { return time.Now().Add(-10 * time.Minute).UTC().Format(http.TimeFormat) },
	} {
		t.Run(name, func(t *testing.T) {
			is := is.New(t)
			var mu sync.Mutex
			published := false
			st := &fakeStore{handler: func(r *http.Request) *http.Response {
				if r.URL.Query().Has("versions") {
					mu.Lock()
					defer mu.Unlock()
					storeNow := time.Now().UTC()
					if d, ok := parseStoreDate(date()); ok && name == "host ahead of store" {
						storeNow = d
					}
					if !published {
						return xmlResp(200, versionsXML(lm), "Date", date()) // empty listing
					}
					return xmlResp(200, versionsXML(storeNow.Format("2006-01-02T15:04:05.000Z"), "obj"), "Date", date())
				}
				return objectResp(keyOf(r))
			}}
			from := time.Now() // the start point, as the snapshot hands it over (min of host and store)
			if name == "host ahead of store" {
				from = from.Add(-10 * time.Minute)
			}
			it, err := NewCDCIterator("b", "", 10*time.Millisecond, 5*time.Minute, st.client(), from, nil)
			is.NoErr(err)
			defer it.Stop()

			time.Sleep(100 * time.Millisecond) // a few empty polls first
			mu.Lock()
			published = true
			mu.Unlock()

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			rec, err := it.Next(ctx)
			if err != nil {
				t.Fatalf("object lost with a %s Date header: %v", name, err)
			}
			is.Equal(string(rec.Key.Bytes()), "obj")
		})
	}
}

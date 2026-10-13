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
	"sync/atomic"
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
	return r.Method == http.MethodGet && !isList(r) && !r.URL.Query().Has("versions")
}
func keyOf(r *http.Request) string { return strings.TrimPrefix(r.URL.Path, "/b/") }
func token(r *http.Request) string { return r.URL.Query().Get("continuation-token") }
func newSnap(t *testing.T, c *s3.Client) *SnapshotIterator {
	t.Helper()
	it, err := NewSnapshotIterator("b", "", c, position.Position{}, nil, false)
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

func TestSnapshotIterator_StartedAt(t *testing.T) {
	near := func(a, b time.Time) bool { d := a.Sub(b); return d < 5*time.Second && d > -5*time.Second }
	for name, tc := range map[string]struct {
		date    string
		trust   bool
		wantErr error
		want    func(started, host time.Time) bool
	}{
		"near future capped at host":  {date: time.Now().Add(2 * time.Hour).UTC().Format(http.TimeFormat), want: func(s, h time.Time) bool { return !s.After(h) && near(s, h) }},
		"store behind host is used":   {date: time.Now().Add(-time.Hour).UTC().Format(http.TimeFormat), want: func(s, h time.Time) bool { d := h.Sub(s); return d > 59*time.Minute && d < 61*time.Minute }},
		"far future refused":          {date: time.Now().Add(30 * 24 * time.Hour).UTC().Format(http.TimeFormat), wantErr: ErrStoreClockOutOfRange},
		"far past refused":            {date: time.Now().Add(-30 * 24 * time.Hour).UTC().Format(http.TimeFormat), wantErr: ErrStoreClockOutOfRange},
		"garbage refused":             {date: "yesterday-ish", wantErr: ErrStoreClockUnavailable},
		"missing refused":             {date: "", wantErr: ErrStoreClockUnavailable},
		"missing with trustHostClock": {date: "", trust: true, want: func(s, h time.Time) bool { return near(s, h) }},
	} {
		t.Run(name, func(t *testing.T) {
			is := is.New(t)
			st := &fakeStore{handler: func(r *http.Request) *http.Response {
				if isList(r) {
					return xmlResp(200, listV2(false, "", "a"), "Date", tc.date)
				}
				return objectResp(keyOf(r))
			}}
			it, err := NewSnapshotIterator("b", "", st.client(), position.Position{}, nil, tc.trust)
			is.NoErr(err)
			is.True(it.HasNext(context.Background()))
			if tc.wantErr != nil {
				_, err := it.Next(context.Background())
				is.True(errors.Is(err, tc.wantErr))
				assertSticky(t, it)
				return
			}
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
	c, err := NewCombinedIterator(ctx, "b", "", time.Hour, time.Minute, false, st.client(), position.Position{})
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
		it, err := NewCDCIterator("b", "", 10*time.Millisecond, time.Minute, false, st.client(), wm, nil)
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

// simStore is a store whose clock is host + skew (negative: the store is behind
// the host). It lists the objects published so far with LastModified on its own
// clock, and sends a Date header chosen by dateFn.
type simStore struct {
	fakeStore
	skew time.Duration
	// dateFn returns the Date header for a poll given the store's time. nil: a
	// correct one.
	dateFn func(storeNow time.Time) string

	mu2     sync.Mutex
	objects map[string]time.Time // key -> LastModified (store clock)
	polls   atomic.Int64
}

func newSimStore(skew time.Duration, dateFn func(time.Time) string) *simStore {
	st := &simStore{skew: skew, dateFn: dateFn, objects: map[string]time.Time{}}
	st.handler = func(r *http.Request) *http.Response {
		if !r.URL.Query().Has("versions") {
			return objectResp(keyOf(r))
		}
		st.polls.Add(1)
		now := time.Now().Add(st.skew).UTC()
		date := now.Format(http.TimeFormat)
		if st.dateFn != nil {
			date = st.dateFn(now)
		}
		st.mu2.Lock()
		defer st.mu2.Unlock()
		var b strings.Builder
		b.WriteString(`<?xml version="1.0"?><ListVersionsResult><IsTruncated>false</IsTruncated>`)
		for k, t := range st.objects {
			fmt.Fprintf(&b, `<Version><Key>%s</Key><VersionId>null</VersionId><IsLatest>true</IsLatest><LastModified>%s</LastModified><ETag>"e-%s"</ETag></Version>`,
				k, t.UTC().Format("2006-01-02T15:04:05.000Z"), k)
		}
		b.WriteString(`</ListVersionsResult>`)
		resp := xmlResp(200, b.String())
		resp.Header.Set("Date", date) // possibly "": explicitly missing
		return resp
	}
	return st
}

// put makes an object visible, stamped with the store's current time.
func (s *simStore) put(key string) {
	s.mu2.Lock()
	defer s.mu2.Unlock()
	s.objects[key] = time.Now().Add(s.skew)
}

func (s *simStore) waitPolls(n int64) {
	target := s.polls.Load() + n
	for s.polls.Load() < target {
		time.Sleep(2 * time.Millisecond)
	}
}

func startSim(t *testing.T, st *simStore, from time.Time, trustHost ...bool) *CDCIterator {
	t.Helper()
	it, err := NewCDCIterator("b", "", 5*time.Millisecond, 5*time.Minute, len(trustHost) > 0 && trustHost[0], st.client(), from, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(it.Stop)
	return it
}

func nextKey(t *testing.T, it *CDCIterator, within time.Duration) (string, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), within)
	defer cancel()
	rec, err := it.Next(ctx)
	if err != nil {
		return "", err
	}
	return string(rec.Key.Bytes()), nil
}

func mustDeliver(t *testing.T, it *CDCIterator, key string) {
	t.Helper()
	got, err := nextKey(t, it, 5*time.Second)
	if err != nil {
		t.Fatalf("%s was lost: %v", key, err)
	}
	if got != key {
		t.Fatalf("got %s, want %s", got, key)
	}
}

// A header dropped on a poll while the host clock is 10 minutes AHEAD of the
// store's. The watermark used to jump to host - lookback on the host-only
// reading and never come back, so an object stamped on the store's clock was
// below it and lost.
func TestCDCIterator_DroppedDateHeaderWithHostAhead(t *testing.T) {
	var drop atomic.Bool
	st := newSimStore(-10*time.Minute, func(now time.Time) string {
		if drop.Load() {
			return ""
		}
		return now.Format(http.TimeFormat)
	})
	it := startSim(t, st, time.Now().Add(-10*time.Minute))

	st.waitPolls(5) // valid headers: the offset is learned
	drop.Store(true)
	st.waitPolls(5) // polls without a header advance the watermark (old code)
	st.put("late")
	st.waitPolls(3)
	drop.Store(false)
	mustDeliver(t, it, "late")
}

func TestCDCIterator_IntermittentDateHeader(t *testing.T) {
	var n atomic.Int64
	st := newSimStore(-10*time.Minute, func(now time.Time) string {
		if n.Add(1)%2 == 0 {
			return ""
		}
		return now.Format(http.TimeFormat)
	})
	it := startSim(t, st, time.Now().Add(-10*time.Minute))

	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("obj-%d", i)
		st.waitPolls(3)
		st.put(key)
		mustDeliver(t, it, key)
	}
}

// A store that never sends Date. The host clock must not silently take over:
// the watermark is held (objects are still delivered), then the iterator stops
// with a stable code. With the explicit opt-in the host clock is used.
func TestCDCIterator_StoreThatNeverSendsDate(t *testing.T) {
	t.Run("default: held, delivered, then fails loudly", func(t *testing.T) {
		is := is.New(t)
		// the host is 10 minutes ahead; with the host clock the watermark would
		// jump past objects stamped on the store's clock
		st := newSimStore(-10*time.Minute, func(time.Time) string { return "" })
		from := time.Now().Add(-10 * time.Minute)
		it := startSim(t, st, from)

		st.waitPolls(2)
		st.put("early")
		mustDeliver(t, it, "early")

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := it.Next(ctx)
		is.True(errors.Is(err, ErrStoreClockUnavailable))
		is.Equal(it.watermark, from.Add(-5*time.Minute)) // never advanced on the host clock
	})
	t.Run("trustHostClock: opt-in, host clock in sync", func(t *testing.T) {
		st := newSimStore(0, func(time.Time) string { return "" })
		it := startSim(t, st, time.Now(), true)
		st.waitPolls(maxBadDatePolls + 5)
		st.put("later")
		mustDeliver(t, it, "later")
	})
}

// Alternating good (in-bound) and out-of-range headers must not stay under the
// breaker.
func TestCDCIterator_AlternatingGoodAndBadHeadersTrip(t *testing.T) {
	is := is.New(t)
	var n atomic.Int64
	st := newSimStore(-10*time.Minute, func(now time.Time) string {
		if n.Add(1)%2 == 0 {
			return now.Add(-30 * time.Hour).Format(http.TimeFormat)
		}
		return now.Format(http.TimeFormat)
	})
	it := startSim(t, st, time.Now().Add(-10*time.Minute))
	st.waitPolls(3)
	st.put("obj")
	mustDeliver(t, it, "obj")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := it.Next(ctx)
	is.True(errors.Is(err, ErrStoreClockOutOfRange))
}

// One in-bound header that claims the store's clock equals the host's, while
// the real offset is 10 minutes: it must not push the watermark.
func TestCDCIterator_InBoundOutlierHeaderCannotPushWatermark(t *testing.T) {
	var outlier atomic.Bool
	st := newSimStore(-10*time.Minute, func(now time.Time) string {
		if outlier.CompareAndSwap(true, false) {
			return time.Now().UTC().Format(http.TimeFormat) // claims store == host
		}
		return now.Format(http.TimeFormat)
	})
	it := startSim(t, st, time.Now().Add(-10*time.Minute))
	st.waitPolls(5)
	outlier.Store(true)
	st.waitPolls(3)
	st.put("late") // LastModified is host - 10m
	mustDeliver(t, it, "late")
}

// Heavy churn while the watermark is held: memory stays bounded and nothing is
// lost before the iterator stops loudly.
func TestCDCIterator_HeldWatermarkWithChurnIsBounded(t *testing.T) {
	is := is.New(t)
	old := maxDeliveredEntries
	maxDeliveredEntries = 200
	defer func() { maxDeliveredEntries = old }()

	st := newSimStore(0, func(time.Time) string { return "" }) // no Date: watermark held
	it := startSim(t, st, time.Now())

	delivered := map[string]bool{}
	var stopErr error
	deadline := time.Now().Add(10 * time.Second)
	for i := 0; stopErr == nil && time.Now().Before(deadline); i++ {
		for j := 0; j < 40; j++ {
			st.put(fmt.Sprintf("o-%d-%d", i, j))
		}
		for k := 0; k < 40 && stopErr == nil; k++ {
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			rec, err := it.Next(ctx)
			cancel()
			switch {
			case err == nil:
				delivered[string(rec.Key.Bytes())] = true
			case errors.Is(err, context.DeadlineExceeded):
				k = 40
			default:
				stopErr = err
			}
		}
	}
	is.True(errors.Is(stopErr, ErrDedupeWindowOverflow) || errors.Is(stopErr, ErrStoreClockUnavailable))
	is.True(len(it.delivered) <= maxDeliveredEntries) // bounded
	is.True(len(delivered) > 0)                       // objects were delivered while held
}

func TestCDCIterator_DedupeOverflowFailsBeforeDelivering(t *testing.T) {
	is := is.New(t)
	old := maxDeliveredEntries
	maxDeliveredEntries = 100
	defer func() { maxDeliveredEntries = old }()

	st := newSimStore(0, nil)
	for i := 0; i < 300; i++ {
		st.put(fmt.Sprintf("o-%d", i))
	}
	it := startSim(t, st, time.Now().Add(-time.Hour))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := it.Next(ctx)
	is.True(errors.Is(err, ErrDedupeWindowOverflow))
	is.True(strings.Contains(err.Error(), "s3.source.dedupe_window_overflow"))
}

// The HOST is the liar: its clock is 30h ahead of the store's, and both the
// Date header and LastModified are on the store's clock. No object may be
// skipped, and the iterator must stop loudly instead of guessing.
func TestCDCIterator_HostClockFarAheadFailsLoudly(t *testing.T) {
	is := is.New(t)
	st := newSimStore(-30*time.Hour, nil)
	it := startSim(t, st, time.Now().Add(-30*time.Hour))

	st.waitPolls(2)
	st.put("obj")
	mustDeliver(t, it, "obj") // delivered while the watermark is held

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := it.Next(ctx)
	is.True(errors.Is(err, ErrStoreClockOutOfRange))
	is.True(strings.Contains(err.Error(), "s3.source.store_clock_out_of_range"))

	// and it stays failed
	for i := 0; i < 3; i++ {
		_, err := it.Next(ctx)
		is.True(errors.Is(err, ErrStoreClockOutOfRange))
	}
}

// Next on a stopped iterator must report an error on every call, never an
// empty record (a receive from the closed buffer).
func TestCDCIterator_NextOnDeadIteratorAlwaysErrors(t *testing.T) {
	st := newSimStore(0, nil)
	it := startSim(t, st, time.Now())
	st.waitPolls(2)
	it.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for i := 0; i < 10; i++ {
		rec, err := it.Next(ctx)
		if err == nil {
			t.Fatalf("call %d returned a record (%q) from a dead iterator", i, rec.Key.Bytes())
		}
	}
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

// TestCDCIterator_HostileDateNeverSkips: end to end through the iterator, with
// a store whose Date header lies. The object is modified "now" on the store's
// clock; it must be delivered whatever the header says. A header that is out of
// range holds the watermark (the object is still delivered) and then fails the
// iterator loudly.
func TestCDCIterator_HostileDateNeverSkips(t *testing.T) {
	for name, tc := range map[string]struct {
		date       func(store time.Time) string
		failsAfter bool
	}{
		"far future":  {func(time.Time) string { return time.Now().Add(100 * 24 * time.Hour).UTC().Format(http.TimeFormat) }, true},
		"far past":    {func(time.Time) string { return time.Now().Add(-100 * 24 * time.Hour).UTC().Format(http.TimeFormat) }, true},
		"near future": {func(time.Time) string { return time.Now().Add(2 * time.Hour).UTC().Format(http.TimeFormat) }, false},
		"garbage":     {func(time.Time) string { return "next tuesday" }, false},
		"missing":     {func(time.Time) string { return "" }, false},
	} {
		t.Run(name, func(t *testing.T) {
			st := newSimStore(0, tc.date)
			it := startSim(t, st, time.Now())
			st.waitPolls(2)
			st.put("obj")
			mustDeliver(t, it, "obj")
			if tc.failsAfter {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				_, err := it.Next(ctx)
				if !errors.Is(err, ErrStoreClockOutOfRange) {
					t.Fatalf("want a loud ErrStoreClockOutOfRange, got %v", err)
				}
			}
		})
	}
}

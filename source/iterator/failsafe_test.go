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
// or truncated object, a broken continuation, a Date header that is not used.

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
	it, err := NewSnapshotIterator("b", "", c, position.Position{}, nil)
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
				maxSeen: wm, delivered: map[deliveredKey]struct{}{},
			}
			cache, err := w.poll(context.Background(), wm)
			is.True(err != nil)
			is.Equal(len(cache), 0) // nothing partial is handed over
			is.Equal(w.maxSeen, wm) // and the newest-seen time did not move
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

// simStore is a store whose clock is host + skew (negative: the store is behind
// the host). It lists the objects published so far with LastModified on its own
// clock. The Date header is deliberately useless (it is not used).
type simStore struct {
	fakeStore
	skew time.Duration

	mu2     sync.Mutex
	objects map[string]time.Time // key -> LastModified (store clock)
	hidden  map[string]bool      // keys not (yet) visible in listings
	polls   atomic.Int64
}

func newSimStore(skew time.Duration) *simStore {
	st := &simStore{skew: skew, objects: map[string]time.Time{}, hidden: map[string]bool{}}
	st.handler = func(r *http.Request) *http.Response {
		if !r.URL.Query().Has("versions") {
			return objectResp(keyOf(r))
		}
		st.polls.Add(1)
		st.mu2.Lock()
		defer st.mu2.Unlock()
		var b strings.Builder
		b.WriteString(`<?xml version="1.0"?><ListVersionsResult><IsTruncated>false</IsTruncated>`)
		for k, t := range st.objects {
			if st.hidden[k] {
				continue
			}
			fmt.Fprintf(&b, `<Version><Key>%s</Key><VersionId>null</VersionId><IsLatest>true</IsLatest><LastModified>%s</LastModified><ETag>"e-%s"</ETag></Version>`,
				k, t.UTC().Format("2006-01-02T15:04:05.000Z"), k)
		}
		b.WriteString(`</ListVersionsResult>`)
		return xmlResp(200, b.String(), "Date", "garbage")
	}
	return st
}

// put makes an object visible, stamped with the store's current time.
func (s *simStore) put(key string) {
	s.mu2.Lock()
	defer s.mu2.Unlock()
	s.objects[key] = time.Now().Add(s.skew)
}

// putAt makes an object visible with an explicit LastModified.
func (s *simStore) putAt(key string, t time.Time) {
	s.mu2.Lock()
	defer s.mu2.Unlock()
	s.objects[key] = t
}

func (s *simStore) hide(key string, h bool) {
	s.mu2.Lock()
	defer s.mu2.Unlock()
	s.hidden[key] = h
}

func (s *simStore) waitPolls(n int64) {
	target := s.polls.Load() + n
	for s.polls.Load() < target {
		time.Sleep(2 * time.Millisecond)
	}
}

func startSim(t *testing.T, st *simStore, from time.Time, lookback ...time.Duration) *CDCIterator {
	t.Helper()
	lb := 5 * time.Minute
	if len(lookback) > 0 {
		lb = lookback[0]
	}
	it, err := NewCDCIterator("b", "", 5*time.Millisecond, lb, st.client(), from, nil)
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

// Next on a stopped iterator must report an error on every call, never an
// empty record (a receive from the closed buffer).
func TestCDCIterator_NextOnDeadIteratorAlwaysErrors(t *testing.T) {
	st := newSimStore(0)
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

func collect(t *testing.T, it *CDCIterator, want int, within time.Duration) map[string]int {
	t.Helper()
	got := map[string]int{}
	deadline := time.Now().Add(within)
	for len(got) < want && time.Now().Before(deadline) {
		k, err := nextKey(t, it, time.Until(deadline))
		if err != nil {
			t.Fatalf("after %d of %d objects: %v", len(got), want, err)
		}
		got[k]++
	}
	return got
}

// The watermark uses only the store's timestamps, so a host clock that is
// wrong in either direction, by any amount, cannot lose or hide an object.
func TestCDCIterator_HostClockSkewEitherDirection(t *testing.T) {
	for name, skew := range map[string]time.Duration{
		"host 10m ahead of the store": -10 * time.Minute,
		"host 10m behind the store":   10 * time.Minute,
		"host 30h ahead of the store": -30 * time.Hour,
		"host 30h behind the store":   30 * time.Hour,
	} {
		t.Run(name, func(t *testing.T) {
			st := newSimStore(skew)
			it := startSim(t, st, time.Time{}) // empty bucket
			for i := 0; i < 4; i++ {
				st.waitPolls(3)
				key := fmt.Sprintf("obj-%d", i)
				st.put(key)
				mustDeliver(t, it, key)
			}
		})
	}
}

// A store that stamps one object far in the future (its clock stepped forward
// and back, or a lying endpoint) must not raise the watermark above everything
// written afterwards.
func TestCDCIterator_FutureLastModifiedDoesNotHideLaterObjects(t *testing.T) {
	for name, ahead := range map[string]time.Duration{"1h": time.Hour, "30 days": 30 * 24 * time.Hour} {
		t.Run(name, func(t *testing.T) {
			st := newSimStore(0)
			it := startSim(t, st, time.Time{})
			st.waitPolls(2)
			st.putAt("bogus", time.Now().Add(ahead))
			mustDeliver(t, it, "bogus")
			for i := 0; i < 3; i++ {
				st.waitPolls(3)
				key := fmt.Sprintf("normal-%d", i)
				st.put(key)
				mustDeliver(t, it, key)
			}
		})
	}
}

// Hide-then-release: an object that becomes visible late, with a LastModified
// older than newer ones already delivered, is delivered when the gap is within
// the lookback and lost beyond it (the documented bound).
func TestCDCIterator_LateVisibleObject(t *testing.T) {
	for _, tc := range []struct {
		name     string
		gap      time.Duration
		lookback time.Duration
		want     bool
	}{
		{"within the lookback", 2 * time.Minute, 5 * time.Minute, true},
		{"beyond the lookback", 2 * time.Minute, 30 * time.Second, false},
		{"lookback 0", 2 * time.Minute, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := newSimStore(0)
			it := startSim(t, st, time.Time{}, tc.lookback)
			now := time.Now()
			st.hide("late", true)
			st.putAt("late", now.Add(-tc.gap))
			st.putAt("newer", now)
			mustDeliver(t, it, "newer")
			st.waitPolls(3)
			st.hide("late", false)
			if tc.want {
				mustDeliver(t, it, "late")
				return
			}
			if k, err := nextKey(t, it, 300*time.Millisecond); err == nil {
				t.Fatalf("late was delivered (%s) although it is beyond the lookback", k)
			}
		})
	}
}

// A catch-up over many old objects must deliver all of them even when they do
// not fit the dedupe memory limit: only the objects inside the window around
// the newest one are tracked.
func TestCDCIterator_CatchUpOverOldObjectsIsNotCapped(t *testing.T) {
	is := is.New(t)
	old := maxDeliveredBytes
	maxDeliveredBytes = 100 * 230
	defer func() { maxDeliveredBytes = old }()

	st := newSimStore(0)
	now := time.Now()
	for i := 0; i < 300; i++ {
		st.putAt(fmt.Sprintf("o-%03d", i), now.Add(-time.Duration(300-i)*10*time.Minute)) // 10 minutes apart
	}
	it := startSim(t, st, time.Time{})
	got := collect(t, it, 300, 10*time.Second)
	is.Equal(len(got), 300)
	it.Stop()
	_ = it.tomb.Wait()
	is.True(len(it.delivered) <= 2) // only the newest object's window is tracked
}

// A window that genuinely holds more than the limit stops the iterator loudly
// before delivering a batch it could not deduplicate, and lowering the lookback
// is the way out.
func TestCDCIterator_WindowOverLimitStopsLoudly(t *testing.T) {
	is := is.New(t)
	old := maxDeliveredBytes
	maxDeliveredBytes = 100 * 230
	defer func() { maxDeliveredBytes = old }()

	newStore := func() *simStore {
		st := newSimStore(0)
		now := time.Now()
		for i := 0; i < 300; i++ {
			st.putAt(fmt.Sprintf("o-%03d", i), now.Add(-time.Duration(300-i)*time.Second)) // 1s apart
		}
		return st
	}

	t.Run("lookback 5m: 300 objects inside the window", func(t *testing.T) {
		it := startSim(t, newStore(), time.Time{})
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := it.Next(ctx)
		is.True(errors.Is(err, ErrDedupeWindowOverflow))
		is.True(strings.Contains(err.Error(), "s3.source.dedupe_window_overflow"))
	})
	t.Run("lookback 10s: the window is small, everything is delivered", func(t *testing.T) {
		it := startSim(t, newStore(), time.Time{}, 10*time.Second)
		got := collect(t, it, 300, 10*time.Second)
		is.Equal(len(got), 300)
		it.Stop()
		_ = it.tomb.Wait()
		is.True(it.deliveredBytes <= maxDeliveredBytes)
	})
}

// Memory stays bounded during heavy churn with a small window.
func TestCDCIterator_ChurnKeepsTheSetBounded(t *testing.T) {
	is := is.New(t)
	st := newSimStore(0)
	it := startSim(t, st, time.Time{}, 2*time.Second)
	base := time.Now().Add(-time.Minute)
	total := 0
	for round := 0; round < 20; round++ {
		for j := 0; j < 50; j++ {
			st.putAt(fmt.Sprintf("o-%d-%d", round, j), base.Add(time.Duration(round)*time.Second))
			total++
		}
		time.Sleep(10 * time.Millisecond)
	}
	got := collect(t, it, total, 15*time.Second)
	is.Equal(len(got), total)
	st.waitPolls(5)
	it.Stop()
	_ = it.tomb.Wait()                 // the poll goroutine owns the set; read it after it stopped
	is.True(len(it.delivered) <= 50*5) // the last 2-4 seconds of churn, not all 1000
}

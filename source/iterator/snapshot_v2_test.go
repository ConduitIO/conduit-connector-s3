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
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-s3/source/position"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
)

// snapStore is a fake store for the snapshot: objects with explicit
// LastModified and ETag, in-progress uploads, and a Date header per listing
// that the test controls (the store's clock).
type snapStore struct {
	fakeStore
	mu       sync.Mutex
	objects  map[string]snapObject
	uploads  []snapUpload
	pageSize int
	// listDate and uploadsDate are the Date headers of the listings.
	listDate, uploadsDate time.Time
	// getOverride changes what GetObject returns for a key.
	getOverride map[string]func(o snapObject) *http.Response
	starts      []string          // StartAfter of each ListObjectsV2 request
	ifMatch     map[string]string // If-Match of the last GetObject per key
}

type snapObject struct {
	modified time.Time
	etag     string
}

type snapUpload struct {
	key       string
	initiated time.Time
}

const ms3 = "2006-01-02T15:04:05.000Z"

func newSnapStore(now time.Time) *snapStore {
	st := &snapStore{objects: map[string]snapObject{}, pageSize: 1000, listDate: now, uploadsDate: now, getOverride: map[string]func(snapObject) *http.Response{}, ifMatch: map[string]string{}}
	st.handler = st.serve
	st.fakeStore.uploads = st.serveUploads
	return st
}

func (s *snapStore) put(key string, modified time.Time, etag string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.objects[key] = snapObject{modified: modified, etag: etag}
}

func (s *snapStore) serveUploads(*http.Request) *http.Response {
	s.mu.Lock()
	defer s.mu.Unlock()
	var b strings.Builder
	b.WriteString(`<?xml version="1.0"?><ListMultipartUploadsResult><Bucket>b</Bucket><IsTruncated>false</IsTruncated>`)
	for i, u := range s.uploads {
		fmt.Fprintf(&b, `<Upload><Key>%s</Key><UploadId>u%d</UploadId><Initiated>%s</Initiated></Upload>`, u.key, i, u.initiated.UTC().Format(ms3))
	}
	b.WriteString(`</ListMultipartUploadsResult>`)
	return xmlResp(200, b.String(), "Date", s.uploadsDate.UTC().Format(http.TimeFormat))
}

func (s *snapStore) serve(r *http.Request) *http.Response {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r.URL.Query().Has("versions") {
		// the versions of the objects under a prefix (the store is unversioned:
		// one null version each), as the same-second probe asks for them
		prefix := r.URL.Query().Get("prefix")
		keys := make([]string, 0, len(s.objects))
		for k := range s.objects {
			if strings.HasPrefix(k, prefix) {
				keys = append(keys, k)
			}
		}
		sort.Strings(keys)
		var b strings.Builder
		b.WriteString(`<?xml version="1.0"?><ListVersionsResult><IsTruncated>false</IsTruncated>`)
		for _, k := range keys {
			o := s.objects[k]
			fmt.Fprintf(&b, `<Version><Key>%s</Key><VersionId>null</VersionId><IsLatest>true</IsLatest><LastModified>%s</LastModified><ETag>%s</ETag></Version>`, k, o.modified.UTC().Format(ms3), o.etag)
		}
		b.WriteString(`</ListVersionsResult>`)
		return xmlResp(200, b.String(), "Date", s.listDate.UTC().Format(http.TimeFormat))
	}
	if isList(r) {
		keys := make([]string, 0, len(s.objects))
		for k := range s.objects {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		start := r.URL.Query().Get("start-after")
		if tok := r.URL.Query().Get("continuation-token"); tok != "" {
			start = tok
		}
		s.starts = append(s.starts, r.URL.Query().Get("start-after"))
		var page []string
		for _, k := range keys {
			if k > start {
				page = append(page, k)
			}
		}
		truncated := len(page) > s.pageSize
		if truncated {
			page = page[:s.pageSize]
		}
		var b strings.Builder
		fmt.Fprintf(&b, `<?xml version="1.0"?><ListBucketResult><Name>b</Name><IsTruncated>%v</IsTruncated>`, truncated)
		if truncated {
			fmt.Fprintf(&b, `<NextContinuationToken>%s</NextContinuationToken>`, page[len(page)-1])
		}
		for _, k := range page {
			o := s.objects[k]
			fmt.Fprintf(&b, `<Contents><Key>%s</Key><LastModified>%s</LastModified><ETag>%s</ETag><Size>1</Size></Contents>`, k, o.modified.UTC().Format(ms3), o.etag)
		}
		b.WriteString(`</ListBucketResult>`)
		return xmlResp(200, b.String(), "Date", s.listDate.UTC().Format(http.TimeFormat))
	}
	k := keyOf(r)
	o, ok := s.objects[k]
	if !ok {
		return xmlResp(404, `<?xml version="1.0"?><Error><Code>NoSuchKey</Code><Message>gone</Message></Error>`)
	}
	s.ifMatch[k] = r.Header.Get("If-Match")
	if f := s.getOverride[k]; f != nil {
		return f(o)
	}
	if im := r.Header.Get("If-Match"); im != "" && im != o.etag {
		return xmlResp(412, `<?xml version="1.0"?><Error><Code>PreconditionFailed</Code><Message>no</Message></Error>`)
	}
	return objectWith(k, o)
}

func objectWith(key string, o snapObject) *http.Response {
	return &http.Response{StatusCode: 200, Header: http.Header{
		"Content-Type":   {"text/plain"},
		"Last-Modified":  {o.modified.UTC().Format(http.TimeFormat)},
		"Etag":           {o.etag},
		"Content-Length": {fmt.Sprint(len(key))},
	}, Body: io.NopCloser(strings.NewReader(key))}
}

func snapCfg() SnapshotConfig {
	return SnapshotConfig{Bucket: "b", Lookback: 5 * time.Minute, MultipartHold: true, MaxTrackedUploads: 8, MaxPendingUploadAge: 24 * time.Hour}
}

// drainSnap reads the snapshot to its end and returns keys and positions.
func drainSnap(t *testing.T, it *SnapshotIterator) ([]string, []opencdc.Position) {
	t.Helper()
	ctx := context.Background()
	var keys []string
	var pos []opencdc.Position
	for it.HasNext(ctx) {
		rec, err := it.Next(ctx)
		if errors.Is(err, sdk.ErrBackoffRetry) {
			break
		}
		if err != nil {
			t.Fatalf("next: %v", err)
		}
		if rec.Operation != opencdc.OperationSnapshot {
			t.Fatalf("record %s has operation %v", rec.Key.Bytes(), rec.Operation)
		}
		keys = append(keys, string(rec.Key.Bytes()))
		pos = append(pos, rec.Position)
	}
	return keys, pos
}

var tNow = time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)

// Regression test for s3#1041: a restart from a snapshot position starts the
// snapshot over. It must resume after the acked key, with the same cut.
func TestSnapshot_ResumesAfterTheAckedKey(t *testing.T) {
	is := is.New(t)
	st := newSnapStore(tNow)
	st.pageSize = 2
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		st.put(k, tNow.Add(-time.Hour), `"e-`+k+`"`)
	}
	cfg := snapCfg()
	cfg.Now = func() time.Time { return tNow }
	first := NewSnapshotIterator(cfg, st.client(), nil, nil)
	ctx := context.Background()
	var acked opencdc.Position
	for i := 0; i < 3; i++ {
		rec, err := first.Next(ctx)
		is.NoErr(err)
		acked = rec.Position
	}
	// The store's clock moves on; the resumed snapshot must keep its cut.
	st.listDate = tNow.Add(time.Hour)
	parsed, err := position.ParseV2(acked)
	is.NoErr(err)
	is.True(parsed.Snapshot != nil)
	is.Equal(parsed.Snapshot.LastKey, "c")
	is.Equal(parsed.Snapshot.Cut, tNow.Add(-5*time.Minute))

	resumed := NewSnapshotIterator(cfg, st.client(), parsed.Snapshot, nil)
	keys, _ := drainSnap(t, resumed)
	is.Equal(keys, []string{"d", "e"})
	is.Equal(st.starts[len(st.starts)-1], "c") // the resumed listing starts after the acked key
}

// The snapshot delivers only what was settled at its start; newer objects are
// change capture's. Revision 1's open question R2: this is why acceptance
// tests must pre-settle their snapshot data.
func TestSnapshot_DeliversOnlySettledObjects(t *testing.T) {
	is := is.New(t)
	st := newSnapStore(tNow)
	st.put("old", tNow.Add(-6*time.Minute), `"e1"`)
	st.put("edge", tNow.Add(-5*time.Minute), `"e2"`)
	st.put("recent", tNow.Add(-4*time.Minute), `"e3"`)
	cfg := snapCfg()
	cfg.Now = func() time.Time { return tNow }
	keys, _ := drainSnap(t, NewSnapshotIterator(cfg, st.client(), nil, nil))
	is.Equal(keys, []string{"edge", "old"})
}

// R1 for the snapshot: C0 = min(Tu, Tl) - lookback. An uploads listing whose
// Date is earlier than the objects listing's must set the cut.
func TestSnapshot_CutUsesTheEarlierOfBothListingDates(t *testing.T) {
	is := is.New(t)
	st := newSnapStore(tNow)
	st.uploadsDate = tNow.Add(-time.Minute) // a slow uploads listing started earlier
	st.put("x", tNow.Add(-5*time.Minute-30*time.Second), `"e"`)
	cfg := snapCfg()
	cfg.Now = func() time.Time { return tNow }
	it := NewSnapshotIterator(cfg, st.client(), nil, nil)
	keys, _ := drainSnap(t, it)
	cut, _ := it.Cut()
	is.Equal(cut, tNow.Add(-6*time.Minute))
	is.Equal(len(keys), 0) // x is above the cut from Tu, although below Tl - lookback
}

// The cut is capped at the host clock: a store Date in the future never moves
// it up.
func TestSnapshot_CutIsCappedAtTheHostClock(t *testing.T) {
	is := is.New(t)
	st := newSnapStore(tNow.Add(2 * time.Hour))
	cfg := snapCfg()
	cfg.Now = func() time.Time { return tNow }
	it := NewSnapshotIterator(cfg, st.client(), nil, nil)
	drainSnap(t, it)
	cut, _ := it.Cut()
	is.Equal(cut, tNow.Add(-5*time.Minute))
}

// Old multipart uploads: tracked in R0 while there is room, the oldest
// untracked one holds the cut, uploads older than maxPendingUploadAge are
// released, recent ones do not matter yet. A completed upload's version
// (multipart ETag, stamped at initiation) is left to change capture; a plain
// PUT of the same key in the same second is not.
func TestSnapshot_TracksOldUploadsAndHoldsOnOverflow(t *testing.T) {
	is := is.New(t)
	st := newSnapStore(tNow)
	st.pageSize = 1
	st.uploads = []snapUpload{
		{"stale", tNow.Add(-48 * time.Hour)},
		{"u1", tNow.Add(-3 * time.Hour)},
		{"u2", tNow.Add(-2 * time.Hour)},
		{"u3", tNow.Add(-time.Hour)},
		{"fresh", tNow.Add(-time.Minute)},
	}
	st.put("done-mp", tNow.Add(-3*time.Hour), `"abc-3"`) // a multipart object of an untracked key: delivered
	st.put("z-last", tNow.Add(-3*time.Hour), `"z"`)      // makes the first page truncated
	cfg := snapCfg()
	cfg.MaxTrackedUploads = 2
	cfg.Now = func() time.Time { return tNow }
	it := NewSnapshotIterator(cfg, st.client(), nil, nil)
	ctx := context.Background()
	first, err := it.Next(ctx) // starts the snapshot: first page, cut, R0
	is.NoErr(err)
	// u1 and u2 complete while the snapshot runs
	st.put("u1", tNow.Add(-3*time.Hour), `"abc-2"`)     // the completed version of tracked u1: left to CDC
	st.put("u2", tNow.Add(-2*time.Hour), `"plainetag"`) // a plain PUT of u2's key in u2's second: delivered
	keys := []string{string(first.Key.Bytes())}
	rest, _ := drainSnap(t, it)
	keys = append(keys, rest...)
	cut, tracked := it.Cut()
	is.Equal(len(tracked), 2) // u1, u2
	is.Equal(tracked[0].KeyHash, position.KeyHash("u1"))
	is.Equal(tracked[1].KeyHash, position.KeyHash("u2"))
	is.Equal(cut, tNow.Add(-time.Hour-time.Second)) // u3 did not fit: it holds the cut
	is.Equal(keys, []string{"done-mp", "u2", "z-last"})
}

// An old upload whose key already has a version stamped in the upload's second
// cannot be recognized when it completes (the version would match its entry
// instead): it holds the cut and is not tracked. Two uploads of a key in one
// second are held the same way.
func TestSnapshot_SameSecondUploadsAreHeldNotTracked(t *testing.T) {
	is := is.New(t)
	st := newSnapStore(tNow)
	st.uploads = []snapUpload{
		{"has-version", tNow.Add(-3*time.Hour + 500*time.Millisecond)},
		{"twice", tNow.Add(-2*time.Hour + 100*time.Millisecond)},
		{"twice", tNow.Add(-2*time.Hour + 900*time.Millisecond)},
	}
	st.put("has-version", tNow.Add(-3*time.Hour), `"abc-2"`) // an earlier multipart object of the key, same second
	cfg := snapCfg()
	cfg.Now = func() time.Time { return tNow }
	it := NewSnapshotIterator(cfg, st.client(), nil, nil)
	keys, _ := drainSnap(t, it)
	cut, tracked := it.Cut()
	is.Equal(len(tracked), 0)
	is.Equal(cut, tNow.Add(-3*time.Hour-time.Second))
	is.Equal(len(keys), 0) // the object is above the cut: change capture delivers it

	// without the version, the first upload is tracked and the same-second pair holds
	st2 := newSnapStore(tNow)
	st2.uploads = st.uploads
	it2 := NewSnapshotIterator(cfg, st2.client(), nil, nil)
	drainSnap(t, it2)
	cut2, tracked2 := it2.Cut()
	is.Equal(len(tracked2), 1)
	is.Equal(tracked2[0].KeyHash, position.KeyHash("has-version"))
	is.Equal(cut2, tNow.Add(-2*time.Hour-time.Second))
}

// R8 for the snapshot: the fetched object must be the listed version. A
// same-content overwrite keeps the ETag but not the time; a different ETag
// fails If-Match. Both are skipped (change capture delivers the new version).
func TestSnapshot_FetchMustBeTheListedVersion(t *testing.T) {
	is := is.New(t)
	st := newSnapStore(tNow)
	st.put("a", tNow.Add(-time.Hour), `"e-a"`)
	st.put("same-content", tNow.Add(-time.Hour), `"e-s"`)
	st.put("replaced", tNow.Add(-time.Hour), `"e-r"`)
	st.getOverride["same-content"] = func(o snapObject) *http.Response {
		o.modified = tNow.Add(-time.Minute) // rewritten after the listing, same bytes
		return objectWith("same-content", o)
	}
	st.getOverride["replaced"] = func(snapObject) *http.Response {
		return xmlResp(412, `<?xml version="1.0"?><Error><Code>PreconditionFailed</Code><Message>no</Message></Error>`)
	}
	cfg := snapCfg()
	cfg.Now = func() time.Time { return tNow }
	keys, _ := drainSnap(t, NewSnapshotIterator(cfg, st.client(), nil, nil))
	is.Equal(keys, []string{"a"})
	is.Equal(st.ifMatch["a"], `"e-a"`) // the listed ETag is sent as If-Match
	is.Equal(st.ifMatch["replaced"], `"e-r"`)
}

// R4: a denied uploads listing stops the snapshot with a stable code; there
// is no silent fallback.
func TestSnapshot_DeniedUploadsListingStops(t *testing.T) {
	is := is.New(t)
	st := newSnapStore(tNow)
	st.put("a", tNow.Add(-time.Hour), `"e"`)
	st.fakeStore.uploads = func(*http.Request) *http.Response {
		return xmlResp(403, `<?xml version="1.0"?><Error><Code>AccessDenied</Code><Message>denied</Message></Error>`)
	}
	it := NewSnapshotIterator(snapCfg(), st.client(), nil, nil)
	_, err := it.Next(context.Background())
	is.True(errors.Is(err, ErrMultipartListingDenied))
	is.True(strings.Contains(err.Error(), "s3:ListBucketMultipartUploads"))
	assertSticky(t, it)
}

func TestCombined_PositionKinds(t *testing.T) {
	ctx := context.Background()
	cfg := CombinedConfig{Bucket: "b", PollingPeriod: time.Hour, Lookback: 5 * time.Minute, MultipartHold: true, MaxTrackedUploads: 8, MaxPendingUploadAge: 24 * time.Hour, MaxEarlyObjects: 16, MaxSettledBytesPerPoll: 1 << 20}

	t.Run("v1 snapshot position restarts the snapshot", func(t *testing.T) {
		is := is.New(t)
		st := newSnapStore(time.Now())
		st.put("a", time.Now().Add(-time.Hour), `"e"`)
		st.put("b", time.Now().Add(-time.Hour), `"e"`)
		c, err := NewCombinedIterator(ctx, cfg, st.client(), opencdc.Position("b_s1792000000"))
		is.NoErr(err)
		t.Cleanup(c.Stop)
		is.True(c.snapshotIterator != nil)
		rec, err := c.Next(ctx)
		is.NoErr(err)
		is.Equal(string(rec.Key.Bytes()), "a") // from the beginning, once
		is.True(position.IsV2(rec.Position))   // and from now on in v2
	})
	t.Run("v1 CDC position resumes CDC", func(t *testing.T) {
		is := is.New(t)
		st := newSnapStore(time.Now())
		c, err := NewCombinedIterator(ctx, cfg, st.client(), opencdc.Position("k_c1792000000"))
		is.NoErr(err)
		t.Cleanup(c.Stop)
		is.True(c.cdcIterator != nil && c.snapshotIterator == nil)
	})
	t.Run("v2 CDC position resumes CDC with its state", func(t *testing.T) {
		is := is.New(t)
		state := position.CDCV2{
			S:       position.Tuple{Time: tNow, Key: "k", VersionID: "null"},
			Early:   []position.EarlyEntry{{Modified: tNow.Add(time.Second), Hash: position.Hash{7}}},
			Tracked: []position.TrackedUpload{{KeyHash: position.KeyHash("u"), Initiated: tNow.Add(-time.Hour)}},
		}
		p, err := state.ToRecordPosition()
		is.NoErr(err)
		c, err := NewCombinedIterator(ctx, cfg, newSnapStore(tNow).client(), p)
		is.NoErr(err)
		t.Cleanup(c.Stop)
		is.True(c.cdcIterator != nil && c.snapshotIterator == nil)
		got := c.cdcIterator.State()
		is.Equal(got.S, state.S)
		is.Equal(len(got.Early), 1)
		is.Equal(len(got.Tracked), 1)
	})
	t.Run("corrupt position fails", func(t *testing.T) {
		is := is.New(t)
		_, err := NewCombinedIterator(ctx, cfg, newSnapStore(tNow).client(), opencdc.Position{0x00, 0x02, 's', 1, 2, 3, 4, 5, 0})
		is.True(errors.Is(err, position.ErrInvalid))
	})
	// N5: the position of the last snapshot record is a snapshot position;
	// resuming from it finds the listing exhausted and switches to change
	// capture right away (the SRC-03 boundary case).
	t.Run("resume from the last snapshot record switches to CDC", func(t *testing.T) {
		is := is.New(t)
		st := newSnapStore(time.Now())
		st.put("a", time.Now().Add(-time.Hour), `"e"`)
		c, err := NewCombinedIterator(ctx, cfg, st.client(), nil)
		is.NoErr(err)
		rec, err := c.Next(ctx)
		is.NoErr(err)
		c.Stop()
		parsed, err := position.ParseV2(rec.Position)
		is.NoErr(err)
		is.True(parsed.Snapshot != nil) // never a CDC position on a snapshot record

		c2, err := NewCombinedIterator(ctx, cfg, st.client(), rec.Position)
		is.NoErr(err)
		t.Cleanup(c2.Stop)
		is.True(!c2.HasNext(ctx))
		is.True(c2.cdcIterator != nil && c2.snapshotIterator == nil)
	})
}

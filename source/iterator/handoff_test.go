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

	"github.com/conduitio/conduit-connector-s3/source/position"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
)

// handoffStore serves the same objects to ListObjectsV2 (the snapshot),
// ListObjectVersions (CDC) and GetObject, the way MinIO does: listings carry
// milliseconds, GetObject's Last-Modified header whole seconds.
type handoffStore struct {
	fakeStore
	mu        sync.Mutex
	objects   map[string]handoffObject
	versioned bool
	listV2    int
	// afterListV2 runs after the n-th ListObjectsV2 was answered (1-based);
	// the first one is the snapshot's anchor pass.
	afterListV2 func(n int)
	// getETag overrides the ETag GetObject returns for a key (a stale read or
	// an overwrite between listing and reading).
	getETag map[string]string
}

type handoffObject struct {
	modified time.Time
	etag     string
	version  string
}

func newHandoffStore(versioned bool) *handoffStore {
	st := &handoffStore{objects: map[string]handoffObject{}, versioned: versioned, getETag: map[string]string{}}
	st.handler = st.serve
	return st
}

func (s *handoffStore) put(key string, modified time.Time, etag string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.putLocked(key, modified, etag)
}

func (s *handoffStore) putLocked(key string, modified time.Time, etag string) {
	v := nullVersionID
	if s.versioned {
		v = fmt.Sprintf("v-%s-%d", key, modified.UnixNano())
	}
	s.objects[key] = handoffObject{modified: modified, etag: etag, version: v}
}

func (s *handoffStore) serve(r *http.Request) *http.Response {
	s.mu.Lock()
	keys := make([]string, 0, len(s.objects))
	for k := range s.objects {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	const ms = "2006-01-02T15:04:05.000Z"
	var resp *http.Response
	var hook func()
	switch {
	case isList(r):
		var b strings.Builder
		b.WriteString(`<?xml version="1.0"?><ListBucketResult><Name>b</Name><IsTruncated>false</IsTruncated>`)
		for _, k := range keys {
			o := s.objects[k]
			fmt.Fprintf(&b, `<Contents><Key>%s</Key><LastModified>%s</LastModified><ETag>%s</ETag><Size>1</Size></Contents>`,
				k, o.modified.UTC().Format(ms), o.etag)
		}
		b.WriteString(`</ListBucketResult>`)
		resp = xmlResp(200, b.String())
		s.listV2++
		if s.afterListV2 != nil {
			n, f := s.listV2, s.afterListV2
			hook = func() { f(n) }
		}
	case r.URL.Query().Has("versions"):
		var b strings.Builder
		b.WriteString(`<?xml version="1.0"?><ListVersionsResult><IsTruncated>false</IsTruncated>`)
		for _, k := range keys {
			o := s.objects[k]
			fmt.Fprintf(&b, `<Version><Key>%s</Key><VersionId>%s</VersionId><IsLatest>true</IsLatest><LastModified>%s</LastModified><ETag>%s</ETag></Version>`,
				k, o.version, o.modified.UTC().Format(ms), o.etag)
		}
		b.WriteString(`</ListVersionsResult>`)
		resp = xmlResp(200, b.String())
	default:
		k := keyOf(r)
		o := s.objects[k]
		etag := o.etag
		if e, ok := s.getETag[k]; ok {
			etag = e
		}
		h := http.Header{
			"Content-Type":   {"text/plain"},
			"Last-Modified":  {o.modified.UTC().Format(http.TimeFormat)}, // whole seconds
			"Etag":           {etag},
			"Content-Length": {fmt.Sprint(len(k))},
		}
		if s.versioned {
			h.Set("X-Amz-Version-Id", o.version)
		}
		resp = &http.Response{StatusCode: 200, Header: h, Body: io.NopCloser(strings.NewReader(k))}
	}
	s.mu.Unlock()
	if hook != nil {
		hook()
	}
	return resp
}

// handoff drives a snapshot to its end and builds the CDC iterator the way
// CombinedIterator.switchToCDCIterator does, without starting its goroutines,
// so the test steps it with a fixed clock: no timing involved.
type handoff struct {
	t   *testing.T
	cdc *CDCIterator
}

func runHandoff(t *testing.T, st *handoffStore, lookback time.Duration) (map[string]int, *handoff) {
	t.Helper()
	ctx := context.Background()
	snap, err := NewSnapshotIterator("b", "", lookback, st.client(), position.Position{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]int{}
	for snap.HasNext(ctx) {
		rec, err := snap.Next(ctx)
		if errors.Is(err, sdk.ErrBackoffRetry) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		seen[string(rec.Key.Bytes())]++
	}
	cdc := newCDCIterator("b", "", lookback, st.client(), snap.anchor, nil)
	cdc.seedDelivered(snap.delivered)
	return seen, &handoff{t: t, cdc: cdc}
}

// poll runs one CDC step at host time now and counts what it hands over.
func (h *handoff) poll(now time.Time, seen map[string]int) {
	h.t.Helper()
	err := h.cdc.step(context.Background(), now, func(c []CacheEntry) error {
		for _, e := range c {
			seen[e.key]++
		}
		return nil
	})
	if err != nil {
		h.t.Fatalf("step: %v", err)
	}
}

func (h *handoff) polls(n int, now time.Time, seen map[string]int) {
	h.t.Helper()
	for i := 0; i < n; i++ {
		h.poll(now.Add(time.Duration(i)*time.Second), seen)
	}
}

// TestHandoff_SnapshotIsNotDeliveredAgain is the regression test for #1040:
// CDC starts at (snapshot start - lookback) and used to start with an empty
// dedupe set, so everything the snapshot delivered inside that window came a
// second time, with nothing failing.
func TestHandoff_SnapshotIsNotDeliveredAgain(t *testing.T) {
	for _, versioned := range []bool{false, true} {
		t.Run(fmt.Sprintf("versioned=%v", versioned), func(t *testing.T) {
			is := is.New(t)
			now := time.Now().Truncate(time.Millisecond)
			st := newHandoffStore(versioned)
			st.put("a", now.Add(-2*time.Second+123*time.Millisecond), `"e-a"`)
			st.put("b", now.Add(-time.Second+794*time.Millisecond), `"e-b"`)
			st.put("c", now.Add(-500*time.Millisecond), `"e-c"`)

			seen, h := runHandoff(t, st, 5*time.Minute)
			h.polls(3, now, seen)
			is.Equal(seen, map[string]int{"a": 1, "b": 1, "c": 1})
		})
	}
}

// The seed suppresses only the exact version delivered: an overwrite after
// the snapshot read the object is a change.
func TestHandoff_LaterOverwriteIsDelivered(t *testing.T) {
	is := is.New(t)
	now := time.Now().Truncate(time.Millisecond)
	st := newHandoffStore(false)
	st.put("a", now.Add(-time.Second), `"e-a"`)
	st.put("b", now.Add(-time.Second), `"e-b"`)

	seen, h := runHandoff(t, st, 5*time.Minute)
	is.Equal(seen, map[string]int{"a": 1, "b": 1})
	st.put("b", now.Add(10*time.Millisecond), `"e-b2"`)
	after := map[string]int{}
	h.polls(3, now, after)
	is.Equal(after, map[string]int{"b": 1})
}

// Objects created while the snapshot runs: one under a key the snapshot has
// not reached is delivered by the snapshot (and seeded), one under a key the
// snapshot already passed is not, and CDC delivers it. Each exactly once.
func TestHandoff_ObjectsCreatedDuringTheSnapshot(t *testing.T) {
	is := is.New(t)
	now := time.Now().Truncate(time.Millisecond)
	st := newHandoffStore(false)
	st.put("m", now.Add(-time.Second), `"e-m"`)
	st.afterListV2 = func(n int) {
		if n == 1 { // after the anchor pass, before the snapshot lists its page
			st.put("z-during", now, `"e-z"`)
		}
	}
	seen, h := runHandoff(t, st, 5*time.Minute)
	is.Equal(seen, map[string]int{"m": 1, "z-during": 1})

	st.put("a-during", now.Add(5*time.Millisecond), `"e-a"`) // a key the snapshot passed
	h.polls(3, now, seen)
	is.Equal(seen, map[string]int{"m": 1, "z-during": 1, "a-during": 1})
}

// A seed over budget is not used beyond the budget: those objects are
// delivered again (a duplicate), never skipped.
func TestHandoff_SeedOverBudgetRedeliversInsteadOfSkipping(t *testing.T) {
	is := is.New(t)
	old := maxSeedBytes
	one := deliveredKey{key: "k0", versionID: nullVersionID, etag: `"e-k0"`}.bytes()
	maxSeedBytes = 2 * one // room for two entries
	t.Cleanup(func() { maxSeedBytes = old })

	now := time.Now().Truncate(time.Millisecond)
	st := newHandoffStore(false)
	for i := 0; i < 5; i++ {
		st.put(fmt.Sprintf("k%d", i), now.Add(-time.Duration(5-i)*time.Second), fmt.Sprintf(`"e-k%d"`, i))
	}
	seen, h := runHandoff(t, st, 5*time.Minute)
	is.Equal(len(h.cdc.delivered), 2)
	h.polls(3, now, seen)
	once, twice := 0, 0
	for k, n := range seen {
		switch n {
		case 1:
			once++
		case 2:
			twice++
		default:
			t.Fatalf("%s delivered %d times", k, n)
		}
	}
	is.Equal(once, 2)  // seeded: not delivered again
	is.Equal(twice, 3) // over budget: at-least-once
}

// A GetObject that returns another version than the one listed (an overwrite
// in between, or a stale read) must not seed the listed identity: CDC would
// then suppress a version whose body nobody delivered.
func TestHandoff_ReadOfAnotherVersionIsNotSeeded(t *testing.T) {
	is := is.New(t)
	now := time.Now().Truncate(time.Millisecond)
	st := newHandoffStore(false)
	st.put("a", now.Add(-time.Second), `"e-a-new"`)
	st.getETag["a"] = `"e-a-old"` // the read returns a stale body

	seen, h := runHandoff(t, st, 5*time.Minute)
	is.Equal(len(h.cdc.delivered), 0)
	h.polls(2, now, seen)
	is.Equal(seen["a"], 2) // the listed version comes again from CDC
}

// Seeds below the floor are released even when the polls find nothing new.
func TestHandoff_SeedBelowTheFloorIsPrunedOnEmptyPolls(t *testing.T) {
	is := is.New(t)
	now := time.Now().Truncate(time.Millisecond)
	st := newHandoffStore(false)
	st.put("a", now.Add(-time.Second), `"e-a"`)
	_, h := runHandoff(t, st, 5*time.Minute)
	is.Equal(len(h.cdc.delivered), 1)
	// A raised minimum floor (as after a shrink) puts the seed below the floor.
	h.cdc.minFloor = now.Add(time.Second)
	h.cdc.maxSeen = now.Add(10 * time.Minute)
	h.poll(now.Add(10*time.Minute), map[string]int{})
	is.Equal(len(h.cdc.delivered), 0)
	is.Equal(h.cdc.deliveredBytes, int64(0))
}

// CombinedIterator.switchToCDCIterator must hand the snapshot's seed to the
// CDC iterator it starts. The polling period is an hour, so the check does not
// race with a poll.
func TestCombinedIterator_HandoffSeedsTheCDCIterator(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	now := time.Now().Truncate(time.Millisecond)
	st := newHandoffStore(false)
	st.put("a", now.Add(-2*time.Second), `"e-a"`)
	st.put("b", now.Add(-time.Second), `"e-b"`)

	c, err := NewCombinedIterator(ctx, "b", "", time.Hour, 5*time.Minute, st.client(), position.Position{})
	is.NoErr(err)
	t.Cleanup(c.Stop)
	n := 0
	for c.HasNext(ctx) && c.snapshotIterator != nil {
		_, err := c.Next(ctx)
		is.NoErr(err)
		n++
	}
	is.Equal(n, 2)
	is.True(c.cdcIterator != nil)
	is.Equal(len(c.cdcIterator.delivered), 2)
}

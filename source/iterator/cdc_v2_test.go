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
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-s3/source/position"
	"github.com/matryer/is"
)

const lb = 5 * time.Minute

// start is the CDC state right after an empty snapshot at the model's clock.
func startState(m *awsModel) position.CDCV2 {
	return position.CDCV2{S: position.Tuple{Time: m.clock().Add(-lb).Truncate(time.Millisecond), Inf: true}}
}

func TestCDC_EarlyThenSettled_ExactlyOnceAcrossKills(t *testing.T) {
	is := is.New(t)
	m := newAWSModel(tNow, false)
	d := newDriver(t, m, CDCConfig{}, startState(m))
	m.put("a", 0)
	m.put("b", 0)
	m.advance(time.Second)
	is.Equal(keysOf(d.poll()), []string{"a", "b"}) // early
	d.kill(1)                                      // a acked, b not
	m.advance(time.Second)
	is.Equal(keysOf(d.poll()), []string{"b"}) // only the unacked one comes again
	m.advance(lb + time.Second)
	is.Equal(len(d.poll()), 0) // settling moves S past them without redelivery
	d.kill(len(d.run))         // b acked
	m.advance(time.Second)
	is.Equal(len(d.poll()), 0) // nothing acked comes again
	d.kill(0)                  // nothing acked in this run: resume from the same position
	m.advance(time.Second)
	is.Equal(len(d.poll()), 0)
	requireExactlyOnce(t, d.finish(), m.deletes)
	is.True(d.it.State().S.Inf)
	is.Equal(len(d.it.State().Early), 0) // pruned once settled
}

// A version visible later than its LastModified, but within the lookback, is
// delivered (the reason the lookback exists), once.
func TestCDC_LateVisibleWithinLookback(t *testing.T) {
	is := is.New(t)
	m := newAWSModel(tNow, false)
	d := newDriver(t, m, CDCConfig{}, startState(m))
	m.put("late", 3*time.Minute)
	m.advance(time.Second)
	m.put("now", 0)
	m.advance(time.Second)
	is.Equal(keysOf(d.poll()), []string{"now"})
	for i := 0; i < 4; i++ {
		m.advance(time.Minute)
		d.poll()
	}
	is.Equal(keysOf(d.run), []string{"now", "late"})
	requireExactlyOnce(t, d.finish(), m.deletes)
}

// B2: with a settled budget of one candidate per poll, S advances through
// what was delivered and never jumps over the deferred ones.
func TestCDC_SettledBudgetOfOne(t *testing.T) {
	is := is.New(t)
	m := newAWSModel(tNow, false)
	old := position.CDCV2{S: position.Tuple{Time: tNow.Add(-time.Hour), Inf: true}}
	for i := 0; i < 6; i++ {
		m.mu.Lock()
		m.addVersion(fmt.Sprintf("k%d", i), tNow.Add(-30*time.Minute+time.Duration(i)*time.Second), tNow.Add(-30*time.Minute), false, "")
		m.mu.Unlock()
	}
	d := newDriver(t, m, CDCConfig{MaxSettledBytesPerPoll: 1}, old)
	for i := 0; i < 10; i++ {
		got := d.poll()
		is.True(len(got) <= 1)
		if i == 2 {
			d.kill(len(d.run))
		}
		m.advance(time.Second)
	}
	all := d.finish()
	requireExactlyOnce(t, all, m.deletes)
	is.Equal(keysOf(all), []string{"k0", "k1", "k2", "k3", "k4", "k5"}) // in tuple order, none skipped
}

// An early bound of one: the excess waits until it settles.
func TestCDC_EarlyBoundOfOne(t *testing.T) {
	is := is.New(t)
	m := newAWSModel(tNow, false)
	d := newDriver(t, m, CDCConfig{MaxEarlyObjects: 1}, startState(m))
	for _, k := range []string{"a", "b", "c"} {
		m.put(k, 0)
	}
	m.advance(time.Second)
	is.Equal(len(d.poll()), 1)
	m.advance(time.Second)
	is.Equal(len(d.poll()), 0) // E is full
	m.advance(lb)
	got := d.poll()
	is.Equal(len(got), 2) // settled now
	requireExactlyOnce(t, d.finish(), m.deletes)
}

// SRC-02 regression: the acceptance suite writes 1 + 100 + MaxBatchSize
// (10000) objects to one running source. With the old default early bound
// (2048), the 10000-object burst overflowed E, so the source deferred the
// excess to settle within lookbackPeriod and the suite read them before they
// were delivered. The default bound must deliver the whole burst at once,
// exactly once.
func TestCDC_BurstOfMaxBatchDeliveredAtOnce(t *testing.T) {
	is := is.New(t)
	m := newAWSModel(tNow, false)
	d := newDriver(t, m, CDCConfig{MaxEarlyObjects: DefaultMaxEarlyObjects}, startState(m))

	m.put("snapshot", 0)
	m.advance(time.Second)
	is.Equal(len(d.poll()), 1)
	for i := 0; i < 100; i++ {
		m.put(fmt.Sprintf("create-%03d", i), 0)
	}
	m.advance(time.Second)
	is.Equal(len(d.poll()), 100)

	const burst = 10000
	for i := 0; i < burst; i++ {
		m.put(fmt.Sprintf("burst-%05d", i), 0)
	}
	m.advance(time.Second)
	// The whole burst is delivered in this poll: nothing waits to settle.
	is.Equal(len(d.poll()), burst)

	// While the burst is still within the lookback, no record comes again and
	// nothing else is delivered.
	for i := 0; i < 3; i++ {
		m.advance(time.Second)
		is.Equal(len(d.poll()), 0)
	}
	all := d.finish()
	requireExactlyOnce(t, all, m.deletes)
	is.Equal(len(all), 1+100+burst)
}

// B1: a multipart upload that runs longer than the lookback on an idle bucket
// is stamped with its initiation time when it completes; it is tracked in R
// and delivered once, also across a kill.
func TestCDC_LongMultipartUploadOnIdleBucket(t *testing.T) {
	is := is.New(t)
	m := newAWSModel(tNow, false)
	d := newDriver(t, m, CDCConfig{}, startState(m))
	u := m.startUpload("big")
	for i := 0; i < 60; i++ {
		m.advance(time.Minute)
		d.poll()
		if i == 30 {
			d.kill(0)
		}
	}
	is.Equal(len(d.it.State().Tracked), 1)
	is.True(d.it.State().S.Time.After(u.initiated)) // the cut is not held: R carries it
	m.complete(u)
	m.advance(time.Second)
	got := d.poll()
	is.Equal(keysOf(got), []string{"big"})
	d.kill(1)
	m.advance(time.Second)
	is.Equal(len(d.poll()), 0)
	is.Equal(len(d.it.State().Tracked), 0)
	requireExactlyOnce(t, d.finish(), m.deletes)
}

// R1: T = min(Tu, Tv). An upload initiated during a slow uploads listing,
// under a key the listing already passed, is in neither listing. With T taken
// from the versions listing alone, the cut passed its initiation time, and if
// it completed before the next poll it landed below S, untracked: lost.
//
// The host clock runs ahead of the store's here: with both in sync, the host
// cap (the host clock is read when the poll starts) already keeps T at or
// below the start of the uploads listing, and Tu would not matter.
func TestCDC_UploadInitiatedDuringSlowUploadsListing(t *testing.T) {
	is := is.New(t)
	m := newAWSModel(tNow, false)
	d := newDriver(t, m, CDCConfig{}, startState(m))
	ahead := func() time.Time { return m.clock().Add(10 * time.Minute) }
	d.cfg.Now, d.it.cfg.Now = ahead, ahead
	m.startUpload("b-existing")
	m.startUpload("z-existing")
	m.advance(time.Second)
	m.pageSize = 1
	var late *mUpload
	m.onUploadsPage = func(n int) {
		if n == 1 {
			// a-late starts right after page 1 (under a key page 1 already
			// passed), then the listing stalls for longer than the lookback
			late = m.startUpload("a-late")
			m.advance(lb + time.Minute)
		}
	}
	d.poll()
	m.onUploadsPage = nil
	m.pageSize = 1000
	m.complete(late) // before the next poll's uploads listing
	m.advance(time.Second)
	got := d.poll()
	is.Equal(keysOf(got), []string{"a-late"})
	requireExactlyOnce(t, d.finish(), m.deletes)
}

// B4: a version written while a multi-page versions listing runs, under a key
// already paged past, is above the cut and comes next poll.
func TestCDC_VersionWrittenDuringMultiPageListing(t *testing.T) {
	is := is.New(t)
	m := newAWSModel(tNow, false)
	d := newDriver(t, m, CDCConfig{}, startState(m))
	m.put("m", 0)
	m.put("z", 0)
	m.advance(time.Second)
	m.pageSize = 1
	m.tickPerPage = 2 * time.Minute
	d.poll()
	m.tickPerPage = 0
	m.put("a", 0)
	m.advance(time.Second)
	d.poll()
	m.advance(lb + time.Minute)
	d.poll()
	all := d.finish()
	requireExactlyOnce(t, all, m.deletes)
	seen := map[string]bool{}
	for _, k := range keysOf(all) {
		seen[k] = true
	}
	is.True(seen["a"] && seen["m"] && seen["z"])
}

// R3: more long-running uploads than R holds: the oldest untracked one holds
// the cut (latency, not loss); a completed upload overwritten before the next
// poll collapses into the overwrite; a plain PUT of a tracked key in the same
// second does not consume the entry.
func TestCDC_TrackedUploadsOverflowAndEdges(t *testing.T) {
	is := is.New(t)
	m := newAWSModel(tNow, false)
	d := newDriver(t, m, CDCConfig{MaxTrackedUploads: 1}, startState(m))
	u1 := m.startUpload("u1")
	m.advance(time.Second)
	u2 := m.startUpload("u2")
	for i := 0; i < 10; i++ {
		m.advance(time.Minute)
		d.poll()
	}
	st := d.it.State()
	is.Equal(len(st.Tracked), 1)
	is.True(!st.S.Time.After(u2.initiated)) // held below the untracked u2
	// a plain PUT of u1's key in u1's second: not the tracked object
	m.mu.Lock()
	m.addVersion("u1", u1.initiated, m.now, false, "")
	m.mu.Unlock()
	m.advance(time.Second)
	got := d.poll()
	is.Equal(len(d.it.State().Tracked), 1) // entry kept
	_ = got
	m.complete(u1)
	m.complete(u2)
	m.advance(time.Second)
	d.poll()
	m.advance(lb + time.Minute)
	d.poll()
	all := d.finish()
	requireExactlyOnce(t, all, m.deletes)
	last := map[string]string{}
	for _, r := range all {
		last[string(r.Key.Bytes())] = body(r)
	}
	is.Equal(len(last), 2)
	is.Equal(len(d.it.State().Tracked), 0)

	// completed, then overwritten before the next poll: collapsed
	m2 := newAWSModel(tNow, false)
	d2 := newDriver(t, m2, CDCConfig{}, startState(m2))
	u := m2.startUpload("c")
	m2.advance(lb + time.Minute)
	d2.poll()
	m2.complete(u)
	m2.advance(time.Second)
	m2.put("c", 0)
	m2.advance(time.Second)
	is.Equal(keysOf(d2.poll()), []string{"c"})
	is.Equal(len(d2.it.State().Tracked), 0)
	requireExactlyOnce(t, d2.finish(), m2.deletes)
}

// R4: AccessDenied on the uploads listing while running stops the iterator
// with the stable code; there is no fallback.
func TestCDC_DeniedUploadsListingStops(t *testing.T) {
	is := is.New(t)
	m := newAWSModel(tNow, false)
	d := newDriver(t, m, CDCConfig{}, startState(m))
	m.denyUploads = true
	err := d.it.step(context.Background(), func(opencdc.Record) error { return nil })
	is.True(errors.Is(err, ErrMultipartListingDenied))
}

// B3: early entries carry their time, so a restarted iterator prunes them
// when S passes them, instead of keeping them forever.
func TestCDC_EarlyEntriesArePrunedAfterRestart(t *testing.T) {
	is := is.New(t)
	m := newAWSModel(tNow, false)
	d := newDriver(t, m, CDCConfig{}, startState(m))
	m.put("a", 0)
	m.advance(time.Second)
	d.poll()
	d.kill(1)
	is.Equal(len(d.it.State().Early), 1)
	m.advance(lb + time.Second)
	d.poll()
	is.Equal(len(d.it.State().Early), 0)
	requireExactlyOnce(t, d.finish(), m.deletes)
}

// R8: an overwrite with the same content between listing and fetch keeps the
// ETag but not the time; the stale listing is skipped, and the new version
// comes once.
func TestCDC_SameContentOverwriteBetweenListingAndFetch(t *testing.T) {
	is := is.New(t)
	m := newAWSModel(tNow, false)
	d := newDriver(t, m, CDCConfig{}, startState(m))
	v := m.put("a", 0)
	m.advance(time.Second)
	done := false
	m.getHook = func(key string, cur *mVersion) *mVersion {
		if key == "a" && !done {
			done = true
			nv := m.addVersion("a", m.now, m.now, false, v.etag) // same content, new time
			nv.body = cur.body
			return nv
		}
		return cur
	}
	is.Equal(len(d.poll()), 0) // skipped: Last-Modified differs from the listing
	m.getHook = nil
	m.advance(time.Second)
	is.Equal(keysOf(d.poll()), []string{"a"})
	requireExactlyOnce(t, d.finish(), m.deletes)
}

// Versioned bucket: create, update and delete are classified from the
// listing, and only the latest version per key is delivered.
func TestCDC_VersionedOperations(t *testing.T) {
	is := is.New(t)
	m := newAWSModel(tNow, true)
	d := newDriver(t, m, CDCConfig{}, startState(m))
	m.put("k", 0)
	m.advance(time.Second)
	got := d.poll()
	is.Equal(len(got), 1)
	is.Equal(got[0].Operation, opencdc.OperationCreate)
	m.put("k", 0)
	m.advance(time.Second)
	got = d.poll()
	is.Equal(len(got), 1)
	is.Equal(got[0].Operation, opencdc.OperationUpdate)
	m.del("k")
	m.advance(time.Second)
	got = d.poll()
	is.Equal(len(got), 1)
	is.Equal(got[0].Operation, opencdc.OperationDelete)
	requireExactlyOnce(t, d.finish(), m.deletes)
}

// Upgrade from a v1 CDC position: S = (seconds - lookback, +inf), and an old
// upload that cannot be tracked lowers S below it on the first poll.
func TestCDC_UpgradeFromV1(t *testing.T) {
	is := is.New(t)
	m := newAWSModel(tNow, false)
	m.mu.Lock()
	m.addVersion("recent", tNow.Add(-time.Minute), tNow.Add(-time.Minute), false, "")
	m.mu.Unlock()
	m.advance(-2 * time.Hour)
	held := m.startUpload("held")
	m.advance(2 * time.Hour)
	cfg := CDCConfig{
		Bucket: "b", Lookback: lb, MultipartHold: true, MaxTrackedUploads: 0, MaxPendingUploadAge: 24 * time.Hour,
		MaxEarlyObjects: 16, MaxSettledBytesPerPoll: 1 << 20, Now: m.clock, PollingPeriod: time.Hour,
	}
	w := NewCDCIteratorFromV1(cfg, m.client(), position.Position{Key: "x", Timestamp: tNow.Add(-10 * time.Second), Type: position.TypeCDC}, nil)
	w.Stop()
	w2 := newCDCIterator(cfg, m.client(), w.State(), nil)
	w2.upgradeFloor = true
	var got []opencdc.Record
	is.NoErr(w2.step(context.Background(), func(r opencdc.Record) error { got = append(got, r); return nil }))
	is.Equal(keysOf(got), []string{"recent"}) // the window comes once more
	is.True(w2.State().S.Time.Before(held.initiated))
}

// The randomized property test: writes (some visible late within the
// lookback), overwrites, multipart uploads (some longer than the lookback),
// aborts, and kills at random records. Every acked version is delivered at
// most once, and every key's final state is the last thing delivered for it.
func TestCDC_PropertyExactResume(t *testing.T) {
	seeds := 20
	if v, err := strconv.Atoi(os.Getenv("S3_PROP_SEEDS")); err == nil {
		seeds = v
	} else if testing.Short() {
		seeds = 5
	}
	for seed := 1; seed <= seeds; seed++ {
		t.Run(strconv.Itoa(seed), func(t *testing.T) { runExactProp(t, int64(seed)) })
	}
}

func runExactProp(t *testing.T, seed int64) { //nolint:gocyclo // a randomized schedule reads best as one function
	r := rand.New(rand.NewSource(seed))
	m := newAWSModel(tNow, r.Intn(2) == 0)
	cfg := CDCConfig{
		MaxEarlyObjects:        1 + r.Intn(6),
		MaxTrackedUploads:      1 + r.Intn(3),
		MaxSettledBytesPerPoll: int64(100 + r.Intn(400)),
	}
	d := newDriver(t, m, cfg, startState(m))
	if r.Intn(2) == 0 {
		m.pageSize = 1 + r.Intn(4)
	}
	var uploads []*mUpload
	keys := []string{"a", "b", "c", "d", "e", "f"}
	for step := 0; step < 400; step++ {
		switch op := r.Intn(10); {
		case op < 4:
			delay := time.Duration(0)
			if r.Intn(4) == 0 {
				delay = time.Duration(r.Intn(int(lb/time.Second)-30)) * time.Second // late, within the lookback
			}
			m.put(keys[r.Intn(len(keys))], delay)
		case op == 4 && m.versioned:
			m.del(keys[r.Intn(len(keys))])
		case op == 5:
			uploads = append(uploads, m.startUpload("mp-"+keys[r.Intn(len(keys))]))
		case op == 6 && len(uploads) > 0:
			i := r.Intn(len(uploads))
			if r.Intn(5) == 0 {
				m.abort(uploads[i])
			} else {
				m.complete(uploads[i])
			}
			uploads = append(uploads[:i], uploads[i+1:]...)
		}
		m.advance(time.Duration(1+r.Intn(40)) * time.Second)
		d.poll()
		if r.Intn(15) == 0 {
			d.kill(r.Intn(len(d.run) + 1))
		}
	}
	for _, u := range uploads {
		m.complete(u)
	}
	for i := 0; i < 20; i++ { // let everything settle
		m.advance(lb / 4)
		d.poll()
	}
	all := d.finish()
	requireExactlyOnce(t, all, m.deletes)

	// Every key's final state was delivered, and is the last record for it.
	last := map[string]opencdc.Record{}
	for _, rec := range all {
		last[string(rec.Key.Bytes())] = rec
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for key := range m.keys {
		vs := m.visible(key)
		if len(vs) == 0 {
			continue
		}
		tip := vs[len(vs)-1]
		got, ok := last[key]
		if !ok {
			t.Fatalf("seed %d: key %s never delivered (final version %s)", seed, key, tip.id)
		}
		if tip.deleted {
			if got.Operation != opencdc.OperationDelete {
				t.Fatalf("seed %d: key %s ends deleted, last delivered %v", seed, key, got.Operation)
			}
			continue
		}
		if body(got) != tip.body {
			t.Fatalf("seed %d: key %s: last delivered %q, final %q", seed, key, body(got), tip.body)
		}
	}
}

// R6 memory test (connector side only): 10,000 records in flight, each with a
// full early set, take about N x (80 + |key| + 20 x K) bytes of positions.
func TestCDC_PositionMemoryWithinTheFormula(t *testing.T) {
	is := is.New(t)
	const n, k = 10000, 2048
	state := position.CDCV2{S: position.Tuple{Time: tNow, Key: "dir/key-000000", VersionID: "null"}}
	for i := 0; i < k; i++ {
		state.Early = append(state.Early, position.EarlyEntry{Modified: tNow.Add(time.Duration(i+1) * time.Millisecond), Hash: position.VersionHash(fmt.Sprint(i), "null", "e", tNow)})
	}
	total := 0
	for i := 0; i < n; i++ {
		p, err := state.ToRecordPosition()
		is.NoErr(err)
		total += cap(p)
	}
	formula := n * (80 + len(state.S.Key) + 20*k)
	if total > formula*11/10 {
		t.Fatalf("positions take %d bytes for %d records, formula %d (+10%%)", total, n, formula)
	}
	t.Logf("positions: %d MiB for %d records in flight with |E| = %d (formula %d MiB)", total>>20, n, k, formula>>20)
}

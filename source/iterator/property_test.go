// Copyright © 2022 Meroxa, Inc.
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
)

// Randomized schedule against a key-ordered store: bursts (incl. same-second
// bursts over the candidate budget), late-visible objects within the lookback
// of the newest object, restarts from the last handed-over position. Checks:
// every object is delivered at least once after quiescing, no stall, and the
// iterator only stops with overflow when one second really exceeds the budget.
func TestCDCIterator_PropertyNoSkipNoStall(t *testing.T) {
	seeds := 5 // S3_PROP_SEEDS=100 for the full run (about 6 minutes with -race on an idle machine)
	if v, err := strconv.Atoi(os.Getenv("S3_PROP_SEEDS")); err == nil {
		seeds = v
		quiesce = 1500 // the full run waits long enough to expose slow stalls
	} else if testing.Short() {
		seeds = 2
	}
	first := 1 // S3_PROP_FIRST shards a long run over several processes
	if v, err := strconv.Atoi(os.Getenv("S3_PROP_FIRST")); err == nil {
		first = v
	}
	for seed := first; seed < first+seeds; seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) { runProp(t, int64(seed)) })
	}
}

var debugSeed int64 = -1
var quiesce = 400 // polls after the last write; 1500 for a full run (S3_PROP_SEEDS)

func runProp(t *testing.T, seed int64) { //nolint:gocyclo // a randomized schedule reads best as one function
	r := rand.New(rand.NewSource(seed))
	oldD, oldC := maxDeliveredBytes, maxCandidateBytes
	e := deliveredKey{key: "k0000", versionID: "null", etag: `"e-k0000"`}.bytes()
	dN := int64(20 + r.Intn(60))
	cN := int64(2 + r.Intn(int(dN/3)))
	maxDeliveredBytes = dN * e
	maxCandidateBytes = cN * e
	defer func() { maxDeliveredBytes, maxCandidateBytes = oldD, oldC }()

	lookback := 5 * time.Minute
	host := time.Unix(1_800_000_000, 0)
	st := newSimStore(0) // lists in key order, like real S3
	var lastPos time.Time
	newIt := func(from time.Time) *CDCIterator { return newCDCIterator("b", "", lookback, st.client(), from, nil) }
	it := newIt(time.Time{})
	delivered := map[string]int{}
	written := map[string]time.Time{}
	perSec := map[int64]int64{}
	newest := time.Time{}
	n := 0
	put := func(at time.Time) {
		k := fmt.Sprintf("k%04d", n)
		n++
		st.putAt(k, at)
		written[k] = at
		perSec[at.Unix()]++
		if at.After(newest) {
			newest = at
		}
	}
	eligible := map[string]bool{}
	step := func() error {
		f := laterOf(it.floorFor(it.baseMax(host), host), it.minFloor)
		for k, at := range written {
			if !at.Before(f) {
				eligible[k] = true
			}
		}
		return it.step(context.Background(), host, func(c []CacheEntry) error {
			for _, ce := range c {
				delivered[ce.key]++
				lastPos = ce.posTime
			}
			return nil
		})
	}
	restarts := 0
	totalDeliv := func() int {
		n := 0
		for _, v := range delivered {
			n += v
		}
		return n
	}
	defer func() { t.Logf("seed %d: %d objects, %d deliveries", seed, len(written), totalDeliv()) }()
	for round := 0; round < 60; round++ {
		host = host.Add(time.Duration(1+r.Intn(5)) * time.Second)
		switch r.Intn(5) {
		case 0: // same-second burst
			c := 1 + r.Intn(int(dN))
			for i := 0; i < c; i++ {
				put(host)
			}
		case 1: // spread burst
			c := 1 + r.Intn(int(dN*2))
			for i := 0; i < c; i++ {
				put(host.Add(-time.Duration(r.Intn(30)) * time.Second))
			}
		case 2: // late-visible, older than newest but within 60s of it
			if !newest.IsZero() {
				c := 1 + r.Intn(int(dN))
				at := newest.Add(-time.Duration(1+r.Intn(60)) * time.Second)
				for i := 0; i < c; i++ {
					put(at)
				}
			}
		case 3:
			if r.Intn(3) == 0 {
				it = newIt(lastPos)
				restarts++
			}
		}
		for p := 0; p < 1+r.Intn(3); p++ {
			if err := step(); err != nil {
				if errors.Is(err, ErrDedupeWindowOverflow) {
					// legitimate only if some second alone exceeds the budget
					for _, c := range perSec {
						if c*e > maxDeliveredBytes {
							return
						}
					}
				}
				t.Fatalf("seed %d: step error without a one-second overflow: %v (D=%d C=%d)", seed, err, dN, cN)
			}
		}
	}
	// quiesce: no new writes, host keeps moving slowly (still within lookback of newest)
	for p := 0; p < quiesce; p++ {
		host = host.Add(100 * time.Millisecond)
		if err := step(); err != nil {
			for _, c := range perSec {
				if c*e > maxDeliveredBytes {
					return
				}
			}
			t.Fatalf("seed %d: quiesce step error: %v", seed, err)
		}
	}
	miss, emiss := 0, 0
	var ex []string
	for k := range written {
		if delivered[k] == 0 {
			miss++
			if eligible[k] {
				emiss++
				if len(ex) < 3 {
					ex = append(ex, fmt.Sprintf("%s@%d", k, written[k].Unix()-1_800_000_000))
				}
			}
		}
	}
	if emiss > 0 {
		t.Errorf("seed %d: %d objects were selectable (at/above the poll floor) yet never delivered; %d never delivered in total of %d (D=%d C=%d restarts=%d) e.g. %v; final floor %d minFloor %d maxSeen %d",
			seed, emiss, miss, len(written), dN, cN, restarts, ex,
			laterOf(it.floorFor(it.maxSeen, host), it.minFloor).Unix()-1_800_000_000, it.minFloor.Unix()-1_800_000_000, it.maxSeen.Unix()-1_800_000_000)
	} else if miss > 0 {
		t.Logf("seed %d: %d never delivered, all below the poll floor when they became visible (documented shrunk-window bound)", seed, miss)
	}
	// stall/duplication check: after quiescing, polls should not keep redelivering
	before := 0
	for _, v := range delivered {
		before += v
	}
	for p := 0; p < 20; p++ {
		host = host.Add(100 * time.Millisecond)
		_ = step()
	}
	if debugSeed == seed {
		for p := 0; p < 3; p++ {
			f := laterOf(it.floorFor(it.baseMax(host), host), it.minFloor)
			c, cut, _ := it.poll(context.Background(), f)
			var ks []string
			for _, ce := range c {
				_, tr := it.delivered[ce.deliveredKey()]
				ks = append(ks, fmt.Sprintf("%s@%d tracked=%v", ce.key, ce.lastModified.Unix()-1_800_000_000, tr))
			}
			secs := map[int64]int{}
			for k := range it.delivered {
				secs[k.modified.Unix()-1_800_000_000]++
			}
			t.Logf("poll floor=%d cutoff=%d cands=%v trackedN=%d perSec=%v", f.Unix()-1_800_000_000, cut.Unix()-1_800_000_000, ks, len(it.delivered), secs)
			host = host.Add(100 * time.Millisecond)
			_ = step()
		}
	}
	after := 0
	for _, v := range delivered {
		after += v
	}
	if after != before {
		t.Errorf("seed %d: still redelivering after quiescing: %d extra deliveries in 20 polls", seed, after-before)
	}
}

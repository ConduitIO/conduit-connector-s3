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
	"strings"
	"testing"
	"time"

	"github.com/conduitio/conduit-connector-s3/source/position"
	"github.com/matryer/is"
)

// Invariant 3 regression: the settled budget is in bytes, and candidates differ
// in size. When a large candidate is evicted from the settled heap, a later,
// smaller candidate that sorts above it used to fit the budget, so it was
// delivered and S moved past the evicted one, which was then never delivered.
//
// Keys "a", "b...b" (large) and "c" have the same LastModified. The budget
// holds "a" and "c" but not all three: "b..." is evicted when it is pushed,
// then "c" fits.
func TestCDC_SettledEvictionNeverSkipsARecord(t *testing.T) {
	is := is.New(t)
	m := newAWSModel(tNow, false)
	old := position.CDCV2{S: position.Tuple{Time: tNow.Add(-time.Hour), Inf: true}}
	big := strings.Repeat("b", 100)
	at := tNow.Add(-30 * time.Minute)
	for _, k := range []string{"a", big, "c"} {
		m.mu.Lock()
		m.addVersion(k, at, at, false, "")
		m.mu.Unlock()
	}
	d := newDriver(t, m, CDCConfig{MaxSettledBytesPerPoll: 300}, old)
	for i := 0; i < 6; i++ {
		d.poll()
		if i == 1 {
			d.kill(len(d.run)) // a restart in the middle changes nothing
		}
		m.advance(time.Second)
	}
	all := d.finish()
	requireExactlyOnce(t, all, m.deletes)
	is.Equal(keysOf(all), []string{"a", big, "c"}) // tuple order, none skipped
}

// Same property for the smaller-than-evicted case across several evictions:
// every key is delivered, in tuple order, whatever the sizes.
func TestCDC_SettledEvictionMixedSizes(t *testing.T) {
	is := is.New(t)
	m := newAWSModel(tNow, false)
	old := position.CDCV2{S: position.Tuple{Time: tNow.Add(-time.Hour), Inf: true}}
	var want []string
	for i, n := range []int{1, 200, 1, 150, 1, 1, 220, 1} {
		k := string(rune('a'+i)) + strings.Repeat("x", n)
		want = append(want, k)
		at := tNow.Add(-30 * time.Minute)
		m.mu.Lock()
		m.addVersion(k, at, at, false, "")
		m.mu.Unlock()
	}
	d := newDriver(t, m, CDCConfig{MaxSettledBytesPerPoll: 420}, old)
	for i := 0; i < 20; i++ {
		d.poll()
		m.advance(time.Second)
	}
	all := d.finish()
	requireExactlyOnce(t, all, m.deletes)
	is.Equal(keysOf(all), want)
}

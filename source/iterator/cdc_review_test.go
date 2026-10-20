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
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-s3/source/position"
	"github.com/matryer/is"
	"github.com/rs/zerolog"
)

func countKey(rs []opencdc.Record, k string) int {
	n := 0
	for _, r := range rs {
		if string(r.Key.Bytes()) == k {
			n++
		}
	}
	return n
}

func lastBody(rs []opencdc.Record, k string) string {
	last := ""
	for _, r := range rs {
		if string(r.Key.Bytes()) == k {
			last = body(r)
		}
	}
	return last
}

func tipBody(m *awsModel, k string) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.keys[k][len(m.keys[k])-1].body
}

// sameSecondSetup: X (key k) completes in second s while Y (key k) was
// initiated in s and keeps running.
func sameSecondSetup(m *awsModel) (x, y *mUpload) {
	m.advance(100 * time.Millisecond)
	x = m.startUpload("k")
	m.advance(100 * time.Millisecond)
	y = m.startUpload("k")
	m.advance(100 * time.Millisecond)
	m.complete(x)
	return x, y
}

// A pipeline upgraded from a v0.9.x position starts with S above the second of
// a running upload. The same-second hold must still apply there (S is lowered
// below the upload), or the upload would be tracked ambiguously and its final
// version lost.
func TestCDC_UpgradeFromV1WithSameSecondUpload(t *testing.T) {
	for _, mode := range stampModes {
		t.Run(mode.name, func(t *testing.T) {
			m := newAWSModel(tNow, false)
			m.minio = mode.minio
			m.advance(-2 * time.Hour)
			_, y := sameSecondSetup(m)
			m.advance(2 * time.Hour)
			cfg := CDCConfig{Bucket: "b", Lookback: lb, MultipartHold: true, MaxTrackedUploads: 8, MaxPendingUploadAge: 24 * time.Hour,
				MaxEarlyObjects: 16, MaxSettledBytesPerPoll: 1 << 20, Now: m.clock, PollingPeriod: time.Hour}
			w := newCDCIterator(cfg, m.client(), position.CDCV2{S: position.Tuple{Time: m.clock().Add(-10 * time.Second).Add(-lb), Inf: true}}, nil)
			w.upgradeFloor = true
			var got []opencdc.Record
			emit := func(r opencdc.Record) error { got = append(got, r); return nil }
			step := func(n int) {
				for i := 0; i < n; i++ {
					if err := w.step(context.Background(), emit); err != nil {
						t.Fatal(err)
					}
					m.advance(time.Minute)
				}
			}
			step(6)
			m.complete(y)
			final := tipBody(m, "k")
			step(6)
			if lastBody(got, "k") != final {
				t.Errorf("the final version of k was lost after the v1 upgrade (last %q, final %q)", lastBody(got, "k"), final)
			}
			if n := countKey(got, "k"); n > 2 {
				t.Errorf("k delivered %d times", n)
			}
		})
	}
}

// A position written by v0.11.x can already hold an entry for an upload whose
// second is shared by an earlier multipart version of the key, which is below
// S. That version used to match the entry on every poll, spending it, and the
// upload's own version was lost. The entry of a still-running upload now
// excludes the versions that predate it.
func TestCDC_LegacyAmbiguousEntryDoesNotMatchEarlierVersion(t *testing.T) {
	for _, mode := range stampModes {
		for _, versioned := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/versioned=%v", mode.name, versioned), func(t *testing.T) {
				is := is.New(t)
				m := newAWSModel(tNow, versioned)
				m.minio = mode.minio
				_, y := sameSecondSetup(m)
				m.advance(lb + time.Minute)
				st := position.CDCV2{
					S:       position.Tuple{Time: m.clock().Add(-lb).UTC().Truncate(time.Second), Inf: true},
					Tracked: []position.TrackedUpload{{KeyHash: position.KeyHash("k"), Initiated: y.initiated}},
				}
				d := newDriver(t, m, CDCConfig{}, st)
				for i := 0; i < 8; i++ {
					d.poll()
					m.advance(time.Minute)
				}
				is.Equal(countKey(d.run, "k"), 0) // the earlier version is not delivered again
				m.complete(y)
				final := tipBody(m, "k")
				for i := 0; i < 4; i++ {
					d.poll()
					m.advance(time.Minute)
				}
				all := d.finish()
				requireExactlyOnce(t, all, m.deletes)
				is.Equal(countKey(all, "k"), 1)
				is.Equal(lastBody(all, "k"), final)
				is.Equal(len(d.it.State().Tracked), 0)
			})
		}
	}
}

// The upload completes after the same-second probe (itself a versions
// listing) and before the page that holds its key: the version is delivered
// as an ordinary settled one while its entry is in R, and once S passes it the
// entry would match it again. Item 3: the delivery drops the entry.
func TestCDC_UploadCompletesAfterTheProbe(t *testing.T) {
	is := is.New(t)
	m := newAWSModel(tNow, false)
	d := newDriver(t, m, CDCConfig{}, startState(m))
	m.put("a", 0)
	m.put("b", 0)
	u := m.startUpload("z")
	m.advance(lb + time.Minute)
	m.pageSize = 1
	base := m.versionsPages
	m.onVersionsPage = func(n int) {
		if n == base+2 { // page 1 of the poll, then the probe; z is on page 3
			m.complete(u)
		}
	}
	d.poll()
	is.Equal(m.versionsPages-base, 5) // page 1, probe, pages 2 and 3, the check of the delivered version
	m.onVersionsPage = nil
	m.pageSize = 1000
	for i := 0; i < 4; i++ {
		m.advance(time.Minute)
		d.poll()
	}
	all := d.finish()
	requireExactlyOnce(t, all, m.deletes)
	is.Equal(countKey(all, "z"), 1)
	is.Equal(len(d.it.State().Tracked), 0)
}

// A same-second upload is released from the hold at maxPendingUploadAge, so
// that settling resumes. It is tracked from then on (it would be lost if it
// completed afterwards: AWS stamps it below S), and its final version is
// delivered once.
func TestCDC_HeldUploadReleasedAtMaxAgeIsStillTracked(t *testing.T) {
	for _, mode := range stampModes {
		t.Run(mode.name, func(t *testing.T) {
			is := is.New(t)
			m := newAWSModel(tNow, false)
			m.minio = mode.minio
			d := newDriver(t, m, CDCConfig{MaxEarlyObjects: 2, MaxPendingUploadAge: 30 * time.Minute}, startState(m))
			_, y := sameSecondSetup(m)
			for i := 0; i < 45; i++ {
				m.put(fmt.Sprintf("q%02d", i), 0)
				m.advance(time.Minute)
				d.poll()
			}
			is.True(d.it.State().S.Time.After(y.initiated.Add(30 * time.Minute))) // the hold is released
			is.Equal(len(d.it.State().Tracked), 1)                                // and the upload is tracked
			m.complete(y)
			final := tipBody(m, "k")
			for i := 0; i < 8; i++ {
				m.advance(time.Minute)
				d.poll()
			}
			all := d.finish()
			requireExactlyOnce(t, all, m.deletes)
			is.Equal(lastBody(all, "k"), final)
			is.Equal(countKey(all, "k"), 2) // X's version, then Y's
			is.Equal(len(d.it.State().Tracked), 0)
		})
	}
}

// An upload past maxPendingUploadAge that shares its second with nothing is
// not tracked; that is logged at error level, because its object can be lost.
func TestCDC_StaleUploadIgnoredIsAnError(t *testing.T) {
	is := is.New(t)
	m := newAWSModel(tNow, false)
	d := newDriver(t, m, CDCConfig{MaxPendingUploadAge: 30 * time.Minute}, startState(m))
	var buf bytes.Buffer
	log := zerolog.New(&buf)
	d.it = newCDCIterator(d.cfg, m.client(), startState(m), &log)
	m.startUpload("lone")
	m.advance(40 * time.Minute) // first seen past maxPendingUploadAge
	for i := 0; i < 5; i++ {
		m.advance(time.Minute)
		d.poll()
	}
	out := buf.String()
	is.True(strings.Contains(out, `"level":"error"`) && strings.Contains(out, "s3.source.stale_upload_ignored"))
	is.Equal(m.versionCalls["lone"], 1) // probed once, then remembered
}

// The positive answers of the same-second probe are remembered while the
// upload runs: one versions listing for the key, however many polls, and the
// memory is released when the upload ends.
func TestCDC_SameSecondProbeIsCached(t *testing.T) {
	is := is.New(t)
	m := newAWSModel(tNow, false)
	d := newDriver(t, m, CDCConfig{}, startState(m))
	_, y := sameSecondSetup(m)
	for i := 0; i < 12; i++ {
		m.advance(time.Minute)
		d.poll()
	}
	is.Equal(m.versionCalls["k"], 1)
	is.Equal(len(d.it.sameSecond), 1)
	m.abort(y)
	m.advance(time.Minute)
	d.poll()
	is.Equal(len(d.it.sameSecond), 0)
}

func legacyState(m *awsModel, ups ...*mUpload) position.CDCV2 {
	st := position.CDCV2{S: position.Tuple{Time: m.clock().Add(-lb).UTC().Truncate(time.Second), Inf: true}}
	for _, u := range ups {
		st.Tracked = append(st.Tracked, position.TrackedUpload{KeyHash: position.KeyHash(u.key), Initiated: u.initiated})
	}
	return st
}

func requireFinalOnce(t *testing.T, m *awsModel, d *driver, k string) {
	t.Helper()
	all := d.finish()
	if got, want := lastBody(all, k), tipBody(m, k); got != want {
		t.Errorf("the final version %q of %s was never delivered (last %q)", want, k, got)
	}
	requireExactlyOnce(t, all, m.deletes)
}

// The upload completes between the baseline's versions listing and the
// re-list of the uploads, so the baseline cannot be trusted. The entry must not
// match anything in that poll: the earlier version (the tip in this poll's
// listing) used to match it, which spent the entry before the upload's own
// version, stamped in the same second, could arrive.
func TestCDC_BaselineWhileTheUploadCompletes(t *testing.T) {
	for _, versioned := range []bool{false, true} {
		t.Run(fmt.Sprintf("versioned=%v", versioned), func(t *testing.T) {
			m := newAWSModel(tNow, versioned)
			_, y := sameSecondSetup(m)
			m.advance(lb + time.Minute)
			d := newDriver(t, m, CDCConfig{}, legacyState(m, y))
			base := m.versionsPages
			m.onVersionsPage = func(n int) {
				if n == base+2 { // main page 1, then the baseline listing of k
					m.complete(y)
				}
			}
			d.poll()
			m.onVersionsPage = nil
			for i := 0; i < 5; i++ {
				m.advance(time.Minute)
				d.poll()
			}
			requireFinalOnce(t, m, d, "k")
		})
	}
}

// A legacy entry for Y, three uploads of the key in one second: X completed
// before the upgrade, Z completes after the baseline was taken. Z's version
// matches Y's entry while Y still runs; it is delivered, but the entry stays
// (its upload is listed) with Z's version added to its baseline, so Y's own
// version is delivered when Y completes.
func TestCDC_ThirdSameSecondUploadCompletesAfterTheBaseline(t *testing.T) {
	for _, legacyZ := range []bool{false, true} {
		t.Run(fmt.Sprintf("Z-also-legacy=%v", legacyZ), func(t *testing.T) {
			m := newAWSModel(tNow, false)
			m.advance(100 * time.Millisecond)
			x := m.startUpload("k")
			m.advance(100 * time.Millisecond)
			y := m.startUpload("k")
			m.advance(100 * time.Millisecond)
			z := m.startUpload("k")
			m.advance(100 * time.Millisecond)
			m.complete(x)
			m.advance(lb + time.Minute)
			st := legacyState(m, y)
			if legacyZ {
				st = legacyState(m, y, z)
			}
			d := newDriver(t, m, CDCConfig{}, st)
			for i := 0; i < 3; i++ {
				d.poll()
				m.advance(time.Minute)
			}
			m.complete(z) // a multipart version stamped in the second, after the baseline
			for i := 0; i < 3; i++ {
				d.poll()
				m.advance(time.Minute)
			}
			m.complete(y)
			for i := 0; i < 4; i++ {
				d.poll()
				m.advance(time.Minute)
			}
			requireFinalOnce(t, m, d, "k")
		})
	}
}

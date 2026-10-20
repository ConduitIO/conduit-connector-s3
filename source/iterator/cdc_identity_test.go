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
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"strings"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-s3/source/position"
	"github.com/matryer/is"
	"github.com/rs/zerolog"
)

// legacyCDCPosition encodes state the way v0.11.0 to v0.11.2 did (format 0x02):
// tracked uploads without identity. Only the parts these tests use (an
// infinite S, no early entries) are supported.
func legacyCDCPosition(s time.Time, tracked []position.TrackedUpload) opencdc.Position {
	buf := []byte{0x00, 0x02, 'c'}
	buf = binary.AppendVarint(buf, s.UnixMilli())
	buf = append(buf, 1)               // infinite S
	buf = binary.AppendUvarint(buf, 0) // key
	buf = binary.AppendUvarint(buf, 0) // version id
	buf = binary.AppendUvarint(buf, 0) // no early entries
	buf = binary.AppendUvarint(buf, uint64(len(tracked)))
	for _, u := range tracked {
		buf = append(buf, u.KeyHash[:]...)
		buf = binary.AppendVarint(buf, u.Initiated.UnixMilli())
	}
	buf = binary.BigEndian.AppendUint32(buf, crc32.Checksum(buf, crc32.MakeTable(crc32.Castagnoli)))
	return append(buf, 0x00)
}

func resumeFrom(t *testing.T, p opencdc.Position) position.CDCV2 {
	t.Helper()
	parsed, err := position.ParseV2(p)
	if err != nil || parsed.CDC == nil {
		t.Fatalf("legacy position: %v", err)
	}
	return *parsed.CDC
}

// The failure v0.11.x positions can carry: an upload U1 was tracked while an
// earlier multipart version V0 of the key sat in U1's second (V0 matched U1's
// entry and was delivered again on every poll, and once U1 completed its entry
// was gone). After the upgrade the entry of the still-running upload gets a
// baseline holding V0, so V0 stops matching, and U1's own object is delivered
// once.
func TestCDC_UpgradeFromV0111_LegacyEntryOfRunningUpload(t *testing.T) {
	for _, mode := range stampModes {
		for _, versioned := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/versioned=%v", mode.name, versioned), func(t *testing.T) {
				is := is.New(t)
				m := newAWSModel(tNow, versioned)
				m.minio = mode.minio
				u0 := m.startUpload("k")
				m.advance(100 * time.Millisecond)
				v0 := m.complete(u0)
				m.advance(200 * time.Millisecond)
				u1 := m.startUpload("k") // same second as v0
				m.advance(lb + time.Minute)

				legacy := legacyCDCPosition(m.clock().Add(-time.Minute), []position.TrackedUpload{{KeyHash: position.KeyHash("k"), Initiated: u1.initiated}})
				state := resumeFrom(t, legacy)
				is.True(!state.Tracked[0].Verified)
				d := newDriver(t, m, CDCConfig{}, state)
				for i := 0; i < 5; i++ {
					m.advance(time.Minute)
					d.poll()
				}
				is.Equal(len(d.run), 0) // v0 is not delivered again
				e := d.it.State().Tracked
				is.Equal(len(e), 1)
				is.True(e[0].Verified)
				is.Equal(e[0].UploadHash, position.UploadID(u1.id))
				is.Equal(len(e[0].Baseline), 1)
				if mode.minio {
					is.Equal(e[0].Baseline[0], position.VersionHash("k", v0.id, v0.etag, v0.modified))
				}

				v1 := m.complete(u1)
				d.kill(0) // and the verified entry survives a restart from its position
				for i := 0; i < 8; i++ {
					m.advance(time.Minute)
					d.poll()
				}
				all := d.finish()
				requireExactlyOnce(t, all, m.deletes)
				is.Equal(countBody(all, v1.body), 1)
				is.Equal(countBody(all, v0.body), 0)
				is.Equal(len(d.it.State().Tracked), 0)
			})
		}
	}
}

// A legacy entry whose upload is gone (the object may be listed already) keeps
// matching by key and second until it resolves: the object is delivered once.
func TestCDC_UpgradeFromV0111_LegacyEntryOfCompletedUpload(t *testing.T) {
	is := is.New(t)
	m := newAWSModel(tNow, false)
	u := m.startUpload("k")
	m.advance(lb + time.Minute)
	v := m.complete(u) // stamped below the cut, multipart ETag
	m.advance(time.Second)
	legacy := legacyCDCPosition(m.clock().Add(-time.Minute), []position.TrackedUpload{{KeyHash: position.KeyHash("k"), Initiated: u.initiated}})
	d := newDriver(t, m, CDCConfig{}, resumeFrom(t, legacy))
	for i := 0; i < 4; i++ {
		d.poll()
		m.advance(time.Minute)
	}
	all := d.finish()
	requireExactlyOnce(t, all, m.deletes)
	is.Equal(countBody(all, v.body), 1)
	is.Equal(len(d.it.State().Tracked), 0)
}

// A newly tracked upload carries its identity and an empty, verified baseline.
func TestCDC_NewEntriesAreVerified(t *testing.T) {
	is := is.New(t)
	m := newAWSModel(tNow, false)
	d := newDriver(t, m, CDCConfig{}, startState(m))
	u := m.startUpload("big")
	m.advance(lb + time.Minute)
	d.poll()
	e := d.it.State().Tracked
	is.Equal(len(e), 1)
	is.True(e[0].Verified)
	is.Equal(len(e[0].Baseline), 0)
	is.Equal(e[0].UploadHash, position.UploadID(u.id))
}

// The residual of the in-memory baseline: the upload ends while the source is
// stopped. With nothing persisted, the restarted source finds its entry without
// a listed upload, so it cannot rebuild the baseline, and the earlier
// same-second version (below S, already delivered) matches the entry and is
// delivered again. The verified entry in the position keeps its baseline.
func TestCDC_BaselineSurvivesARestartThatSpansTheEndOfTheUpload(t *testing.T) {
	for _, end := range []string{"abort", "complete"} {
		t.Run(end, func(t *testing.T) {
			is := is.New(t)
			m := newAWSModel(tNow, false)
			_, y := sameSecondSetup(m)
			m.advance(lb + time.Minute)
			legacy := legacyCDCPosition(m.clock().Add(-time.Minute), []position.TrackedUpload{{KeyHash: position.KeyHash("k"), Initiated: y.initiated}})
			d := newDriver(t, m, CDCConfig{}, resumeFrom(t, legacy))
			d.poll() // verifies the entry
			m.put("other", 0)
			m.advance(time.Second)
			is.Equal(keysOf(d.poll()), []string{"other"}) // a record whose position carries the verified entry
			d.kill(len(d.run))
			want := 0
			if end == "abort" {
				m.abort(y) // while the source is down
			} else {
				m.complete(y)
				want = 1
			}
			for i := 0; i < 6; i++ {
				m.advance(time.Minute)
				d.poll()
			}
			all := d.finish()
			requireExactlyOnce(t, all, m.deletes)
			is.Equal(countKey(all, "k"), want) // the earlier version is not delivered again
		})
	}
}

// More same-second versions than a baseline holds: the entry is never dropped
// for that. The overflow is kept in memory, an error is logged once, and the
// version is not delivered again on every poll.
func TestCDC_FullBaselineKeepsTheEntry(t *testing.T) {
	is := is.New(t)
	m := newAWSModel(tNow, false)
	_, y := sameSecondSetup(m)
	m.advance(lb + time.Minute)
	d := newDriver(t, m, CDCConfig{}, legacyState(m, y))
	var buf bytes.Buffer
	log := zerolog.New(&buf)
	d.it.log, d.it.sampled = &log, &log
	d.poll() // verifies the entry
	is.Equal(len(d.it.State().Tracked), 1)
	d.it.r[0].Baseline = make([]position.Hash, position.MaxBaseline) // full
	m.mu.Lock()                                                      // a third same-second multipart version, matching the entry while y runs
	m.addVersion("k", y.initiated, m.now, false, `"00000000000000000000000000000001-2"`)
	m.mu.Unlock()
	for i := 0; i < 4; i++ {
		m.advance(time.Minute)
		d.poll()
	}
	is.Equal(strings.Count(buf.String(), "s3.source.upload_identity_unavailable"), 1)
	is.Equal(len(d.it.State().Tracked), 1) // kept
	is.Equal(countKey(d.run, "k"), 1)      // once, not on every poll
	m.complete(y)
	for i := 0; i < 3; i++ {
		m.advance(time.Minute)
		d.poll()
	}
	is.Equal(lastBody(d.finish(), "k"), tipBody(m, "k")) // Y's own version arrives
}

// K1: a tracked upload completes between the uploads listing and the versions
// listing; its own version is delivered while it is still listed. After a kill
// the version must not come again: the entry goes with that delivery, because
// the version is the newest multipart version of the key in its second.
func TestCDC_UploadCompletesBetweenTheListingsThenKill(t *testing.T) {
	for _, versioned := range []bool{false, true} {
		t.Run(fmt.Sprintf("versioned=%v", versioned), func(t *testing.T) {
			is := is.New(t)
			m := newAWSModel(tNow, versioned)
			d := newDriver(t, m, CDCConfig{}, startState(m))
			u := m.startUpload("big")
			for i := 0; i < 8; i++ {
				m.advance(time.Minute)
				d.poll()
			}
			is.Equal(len(d.it.State().Tracked), 1)
			fired := false
			m.onUploadsPage = func(int) {
				if !fired {
					fired = true
					m.complete(u)
				}
			}
			m.advance(time.Minute)
			is.Equal(keysOf(d.poll()), []string{"big"})
			m.onUploadsPage = nil
			is.Equal(len(d.it.State().Tracked), 0)
			d.kill(len(d.run))
			for i := 0; i < 4; i++ {
				m.advance(time.Minute)
				d.poll()
			}
			all := d.finish()
			requireExactlyOnce(t, all, m.deletes)
			is.Equal(countKey(all, "big"), 1)
		})
	}
}

// R1: legacy Y (baseline taken) and a third same-second upload Z that
// completed before a poll. Y completes during that poll, after the main
// listing (which shows Z as the tip) and before Z is delivered. Y has ended,
// but Z's version is not the newest multipart version in the second any more
// (Y's is), so Y's entry stays and Z's version joins its baseline; Y's own
// version is then delivered.
func TestCDC_OtherUploadEndsWhileDelivering(t *testing.T) {
	for _, versioned := range []bool{false, true} {
		t.Run(fmt.Sprintf("versioned=%v", versioned), func(t *testing.T) {
			m := newAWSModel(tNow, versioned)
			m.advance(100 * time.Millisecond)
			x := m.startUpload("k")
			m.advance(100 * time.Millisecond)
			y := m.startUpload("k")
			m.advance(100 * time.Millisecond)
			z := m.startUpload("k")
			m.advance(100 * time.Millisecond)
			m.complete(x)
			m.advance(lb + time.Minute)
			d := newDriver(t, m, CDCConfig{}, legacyState(m, y))
			for i := 0; i < 3; i++ {
				d.poll()
				m.advance(time.Minute)
			}
			m.complete(z)
			base := m.versionsPages
			fired := false
			m.onVersionsPage = func(n int) {
				if !fired && n > base {
					fired = true
					m.complete(y) // after the main listing page that shows Z as the tip
				}
			}
			d.poll()
			m.onVersionsPage = nil
			for i := 0; i < 4; i++ {
				m.advance(time.Minute)
				d.poll()
			}
			requireFinalOnce(t, m, d, "k")
		})
	}
}

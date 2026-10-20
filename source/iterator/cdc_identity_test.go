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
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-s3/source/position"
	"github.com/matryer/is"
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

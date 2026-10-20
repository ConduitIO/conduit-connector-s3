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

package position

import (
	"encoding/hex"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
)

// Positions written by v0.11.1 (format 0x02, tracked uploads without identity),
// captured from that release's encoder. The upgrade must read them and carry
// their tracked uploads as unverified.
const (
	v0111CDC      = "000263f6b9eefbbc6600076469722f6b65790276310288276825b2afe8ee2a58b139f6d50140ee4aa84632cfa495a4eeb77579f877d2d653b4f102e63450b119528214870de45426d1c485f6ffb6f8bc660308616f5ebd978e54fe2b66b03cfcb2f6c5fff4bc66979e7ecb00"
	v0111Snapshot = "000273f6b9eefbbc66086469722f6c61737401e63450b119528214870de45426d1c485f6ffb6f8bc66b4fa172900"
)

func mustHex(t *testing.T, s string) opencdc.Position {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestParseV2_UpgradeFromV0111(t *testing.T) {
	ts := time.UnixMilli(1760529600123).UTC()
	p, err := ParseV2(mustHex(t, v0111CDC))
	if err != nil {
		t.Fatalf("a v0.11.1 CDC position must parse: %v", err)
	}
	want := []TrackedUpload{
		{KeyHash: KeyHash("big/one"), Initiated: ts.Add(-time.Hour)},
		{KeyHash: KeyHash("big/two"), Initiated: ts.Add(-2 * time.Hour)},
	}
	if p.CDC == nil || !reflect.DeepEqual(p.CDC.Tracked, want) {
		t.Fatalf("tracked = %+v, want unverified %+v", p.CDC, want)
	}
	if p.CDC.S.Key != "dir/key" || p.CDC.S.VersionID != "v1" || len(p.CDC.Early) != 2 {
		t.Fatalf("S and E must survive the upgrade: %+v", p.CDC)
	}
	s, err := ParseV2(mustHex(t, v0111Snapshot))
	if err != nil || s.Snapshot == nil || s.Snapshot.LastKey != "dir/last" || len(s.Snapshot.Tracked) != 1 || s.Snapshot.Tracked[0].Verified {
		t.Fatalf("snapshot position: %+v, %v", s.Snapshot, err)
	}

	// Re-encoding writes the new format and keeps the entries unverified
	// until the iterator verifies them.
	out, err := p.CDC.ToRecordPosition()
	if err != nil {
		t.Fatal(err)
	}
	if out[1] != v2Version {
		t.Fatalf("version byte = %#x, want %#x", out[1], v2Version)
	}
	back, err := ParseV2(out)
	if err != nil || !reflect.DeepEqual(back.CDC, p.CDC) {
		t.Fatalf("round trip of the migrated position: %+v, %v", back.CDC, err)
	}
}

func TestParseV2_TrackedIdentityRejects(t *testing.T) {
	good := CDCV2{S: Tuple{Time: ms(0), Inf: true}, Tracked: []TrackedUpload{{KeyHash: KeyHash("k"), Initiated: ms(-time.Hour), UploadHash: UploadID("u"), Verified: true, Baseline: []Hash{{1}}}}}
	enc := func(c CDCV2) error { _, err := c.ToRecordPosition(); return err }
	if err := enc(good); err != nil {
		t.Fatal(err)
	}
	tooMany := good
	tooMany.Tracked = []TrackedUpload{{KeyHash: KeyHash("k"), Initiated: ms(0), Verified: true, Baseline: make([]Hash, MaxBaseline+1)}}
	if err := enc(tooMany); !errors.Is(err, ErrInvalid) {
		t.Fatalf("baseline above the limit: %v", err)
	}
	unverified := good
	unverified.Tracked = []TrackedUpload{{KeyHash: KeyHash("k"), Initiated: ms(0), Baseline: []Hash{{1}}}}
	if err := enc(unverified); !errors.Is(err, ErrInvalid) {
		t.Fatalf("baseline on an unverified entry: %v", err)
	}
	// a flag other than 0 or 1 is corruption, even with a valid checksum
	p, _ := good.ToRecordPosition()
	b := body(p)
	flag := len(b) - 1 - 16 - 1 // flag, count, one baseline hash
	b[flag] = 2
	if _, err := ParseV2(reseal(b)); !errors.Is(err, ErrInvalid) {
		t.Fatalf("bad verified flag: %v", err)
	}
}

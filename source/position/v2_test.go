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
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math/rand"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
)

var t0 = time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)

func ms(d time.Duration) time.Time { return t0.Add(d) }

func sampleCDC() CDCV2 {
	return CDCV2{
		S: Tuple{Time: ms(0), Key: "dir/obj-ü", VersionID: "null"},
		Early: []EarlyEntry{
			{Modified: ms(5 * time.Millisecond), Hash: VersionHash("b", "null", `"e"`, ms(5*time.Millisecond))},
			{Modified: ms(0), Hash: VersionHash("dir/obj-z", "null", `"e"`, ms(0))}, // same ms as S, key above
			{Modified: ms(time.Minute), Hash: VersionHash("a", "v1", `"e2"`, ms(time.Minute))},
		},
		Tracked: []TrackedUpload{{KeyHash: KeyHash("big/upload"), Initiated: ms(-time.Hour)}},
	}
}

func TestV2_RoundTrip(t *testing.T) {
	cases := map[string]any{
		"snapshot, empty":        SnapshotV2{Cut: ms(0)},
		"snapshot, key and R0":   SnapshotV2{Cut: ms(-5 * time.Minute), LastKey: "k/" + strings.Repeat("x", MaxKeyBytes-2), Tracked: []TrackedUpload{{KeyHash: KeyHash("u"), Initiated: ms(-time.Hour)}}},
		"cdc, infinite S, empty": CDCV2{S: Tuple{Time: ms(0), Inf: true}},
		"cdc, full":              sampleCDC(),
		"cdc, infinite S with E": CDCV2{S: Tuple{Time: ms(0), Inf: true}, Early: []EarlyEntry{{Modified: ms(time.Millisecond), Hash: Hash{1}}}},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			var p opencdc.Position
			var err error
			switch v := c.(type) {
			case SnapshotV2:
				p, err = v.ToRecordPosition()
			case CDCV2:
				p, err = v.ToRecordPosition()
			}
			if err != nil {
				t.Fatal(err)
			}
			if p[len(p)-1] != 0x00 || p[0] != 0x00 {
				t.Fatalf("marker or terminator missing: % x", p)
			}
			got, err := ParseV2(p)
			if err != nil {
				t.Fatal(err)
			}
			switch v := c.(type) {
			case SnapshotV2:
				if !reflect.DeepEqual(*got.Snapshot, normSnap(v)) || got.CDC != nil {
					t.Fatalf("got %+v, want %+v", got.Snapshot, v)
				}
			case CDCV2:
				if !reflect.DeepEqual(*got.CDC, normCDC(v)) || got.Snapshot != nil {
					t.Fatalf("got %+v, want %+v", got.CDC, v)
				}
			}
		})
	}
}

// normSnap and normCDC put times in UTC and empty slices to nil, as the parser
// returns them.
func normSnap(s SnapshotV2) SnapshotV2 {
	s.Cut = s.Cut.UTC()
	s.Tracked = normTracked(s.Tracked)
	return s
}

func normCDC(c CDCV2) CDCV2 {
	c.S.Time = c.S.Time.UTC()
	if len(c.Early) == 0 {
		c.Early = nil
	}
	for i := range c.Early {
		c.Early[i].Modified = c.Early[i].Modified.UTC()
	}
	c.Tracked = normTracked(c.Tracked)
	return c
}

func normTracked(in []TrackedUpload) []TrackedUpload {
	if len(in) == 0 {
		return nil
	}
	for i := range in {
		in[i].Initiated = in[i].Initiated.UTC()
	}
	return in
}

func mustEncode(t *testing.T, c CDCV2) opencdc.Position {
	t.Helper()
	p, err := c.ToRecordPosition()
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// reseal recomputes the CRC over a modified body, so that a test reaches the
// check behind it.
func reseal(body []byte) opencdc.Position {
	b := append([]byte(nil), body...)
	b = binary.BigEndian.AppendUint32(b, crc32.Checksum(b, crcTable))
	return append(b, 0x00)
}

func body(p opencdc.Position) []byte { return append([]byte(nil), p[:len(p)-trailerLen]...) }

func TestParseV2_RejectsMalformed(t *testing.T) {
	good := mustEncode(t, sampleCDC())
	snapGood, _ := SnapshotV2{Cut: ms(0), LastKey: "k"}.ToRecordPosition()

	dupEarly := sampleCDC()
	dupEarly.Early = append(dupEarly.Early, dupEarly.Early[0])
	dupTracked := sampleCDC()
	dupTracked.Tracked = append(dupTracked.Tracked, dupTracked.Tracked[0])

	cases := map[string]opencdc.Position{
		"empty-ish":      {0x00},
		"bad version":    func() opencdc.Position { b := body(good); b[1] = 0x03; return reseal(b) }(),
		"unknown type":   func() opencdc.Position { b := body(good); b[2] = 'x'; return reseal(b) }(),
		"no terminator":  good[:len(good)-1],
		"bad terminator": append(append(opencdc.Position(nil), good[:len(good)-1]...), 0x01),
		"truncated":      reseal(body(good)[:len(body(good))-3]),
		"trailing bytes": reseal(append(body(good), 0x00)),
		"snapshot trail": reseal(append(body(snapGood), 0x07)),
		"bad inf flag": func() opencdc.Position {
			b := body(mustEncode(t, CDCV2{S: Tuple{Time: ms(0), Inf: true}}))
			b[headerLen+binary.PutVarint(make([]byte, 10), ms(0).UnixMilli())] = 2
			return reseal(b)
		}(),
		"dup early":       rawCDC(dupEarly),
		"dup tracked":     rawCDC(dupTracked),
		"early count big": reseal(append(header(v2TypeCDC), cdcHeadAndCount(MaxEarlyEntries+1)...)),
	}
	for name, p := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := ParseV2(p)
			if !errors.Is(err, ErrInvalid) {
				t.Fatalf("want ErrInvalid, got %v", err)
			}
			if !strings.Contains(err.Error(), "reset the pipeline's position") {
				t.Fatalf("error does not name the fix: %v", err)
			}
		})
	}
}

// rawCDC encodes without the encoder's own checks (duplicates are allowed by
// the encoder input but must be rejected by the parser).
func rawCDC(c CDCV2) opencdc.Position {
	buf := header(v2TypeCDC)
	buf, _ = c.appendS(buf)
	buf = binary.AppendUvarint(buf, uint64(len(c.Early)))
	for _, e := range c.Early {
		buf = binary.AppendUvarint(buf, uint64(e.Modified.UnixMilli()-c.S.Time.UnixMilli()))
		buf = append(buf, e.Hash[:]...)
	}
	buf, _ = appendTracked(buf, c.Tracked)
	return trailer(buf)
}

func cdcHeadAndCount(n uint64) []byte {
	var b []byte
	b = binary.AppendVarint(b, ms(0).UnixMilli())
	b = append(b, 1)
	b = appendString(b, "")
	b = appendString(b, "")
	return binary.AppendUvarint(b, n)
}

// Every single-byte corruption is detected (CRC-32C catches all of them).
func TestParseV2_EveryByteFlipIsRejected(t *testing.T) {
	good := mustEncode(t, sampleCDC())
	for i := range good {
		for _, mask := range []byte{0x01, 0x80, 0xff} {
			p := append(opencdc.Position(nil), good...)
			p[i] ^= mask
			if _, err := ParseV2(p); err == nil {
				t.Fatalf("flip of byte %d with %#x accepted", i, mask)
			}
		}
	}
}

func TestV2_EncodeRejectsInvalidState(t *testing.T) {
	below := sampleCDC()
	below.Early = []EarlyEntry{{Modified: ms(-time.Millisecond)}}
	atInf := CDCV2{S: Tuple{Time: ms(0), Inf: true}, Early: []EarlyEntry{{Modified: ms(0)}}}
	infKey := CDCV2{S: Tuple{Time: ms(0), Inf: true, Key: "k"}}
	longKey := CDCV2{S: Tuple{Time: ms(0), Key: strings.Repeat("k", MaxKeyBytes+1)}}
	negTime := SnapshotV2{Cut: time.UnixMilli(-1)}
	for name, f := range map[string]func() error{
		"early below S":       func() error { _, err := below.ToRecordPosition(); return err },
		"early at infinite S": func() error { _, err := atInf.ToRecordPosition(); return err },
		"infinite S with key": func() error { _, err := infKey.ToRecordPosition(); return err },
		"key too long":        func() error { _, err := longKey.ToRecordPosition(); return err },
		"time before 1970":    func() error { _, err := negTime.ToRecordPosition(); return err },
	} {
		if err := f(); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: want ErrInvalid, got %v", name, err)
		}
	}
}

// parseV095 is the v0.9.5 parser, copied verbatim, to check what a rollback
// does with a v2 position: it must fail, never panic.
func parseV095(p opencdc.Position) (key string, err error) {
	s := string(p)
	index := strings.LastIndex(s, "_")
	if index == -1 {
		return "", errors.New("invalid position format, no '_' found")
	}
	_, err = strconv.ParseInt(s[index+2:], 10, 64)
	if err != nil {
		return "", fmt.Errorf("could not parse the position timestamp: %w", err)
	}
	if s[index+1] != 'c' && s[index+1] != 's' {
		return "", errors.New("invalid position format")
	}
	return s[:index], nil
}

func randomV2(r *rand.Rand) opencdc.Position {
	key := func() string {
		b := make([]byte, r.Intn(40))
		for i := range b {
			b[i] = "ab_cs0123456789/"[r.Intn(16)]
		}
		return string(b)
	}
	if r.Intn(2) == 0 {
		p, _ := SnapshotV2{Cut: ms(time.Duration(r.Intn(1e6)) * time.Millisecond), LastKey: key()}.ToRecordPosition()
		return p
	}
	c := CDCV2{S: Tuple{Time: ms(0), Key: key(), VersionID: key()}}
	for i := 0; i < r.Intn(5); i++ {
		var h Hash
		r.Read(h[:])
		c.Early = append(c.Early, EarlyEntry{Modified: ms(time.Duration(i+1) * time.Millisecond), Hash: h})
	}
	p, _ := c.ToRecordPosition()
	return p
}

// A rollback to v0.9.x hands it a v2 position. It must fail cleanly (Open
// fails loudly), not panic. Without the 0x00 terminator a position ending in
// '_' made that parser slice past the end.
func TestV2_OldParserNeverPanicsOnV2(t *testing.T) {
	r := rand.New(rand.NewSource(20261015))
	for i := 0; i < 20000; i++ {
		p := randomV2(r)
		func() {
			defer func() {
				if v := recover(); v != nil {
					t.Fatalf("v0.9.5 parser panicked on % x: %v", p, v)
				}
			}()
			if _, err := parseV095(p); err == nil {
				t.Fatalf("v0.9.5 parser accepted a v2 position: % x", p)
			}
		}()
	}
}

// Regression: the v1 parser itself panicked on a position ending in '_'.
func TestParseRecordPosition_TrailingUnderscoreDoesNotPanic(t *testing.T) {
	if _, err := ParseRecordPosition(opencdc.Position("some/key_")); err == nil {
		t.Fatal("want an error")
	}
}

func TestParse_Dispatch(t *testing.T) {
	if got, err := Parse(nil); err != nil || got.V1 != nil || got.V2 != nil {
		t.Fatalf("nil: %+v %v", got, err)
	}
	got, err := Parse(opencdc.Position("a/key_c1792000000"))
	if err != nil || got.V1 == nil || got.V1.Type != TypeCDC || got.V1.Key != "a/key" {
		t.Fatalf("v1 cdc: %+v %v", got, err)
	}
	got, err = Parse(opencdc.Position("a/key_s1792000000"))
	if err != nil || got.V1 == nil || got.V1.Type != TypeSnapshot {
		t.Fatalf("v1 snapshot: %+v %v", got, err)
	}
	got, err = Parse(mustEncode(t, sampleCDC()))
	if err != nil || got.V2 == nil || got.V2.CDC == nil {
		t.Fatalf("v2: %+v %v", got, err)
	}
	if _, err := Parse(opencdc.Position("garbage")); !errors.Is(err, ErrInvalid) {
		t.Fatalf("garbage: %v", err)
	}
}

func TestVersionHash_LengthPrefixed(t *testing.T) {
	if VersionHash("ab", "c", "", t0) == VersionHash("a", "bc", "", t0) {
		t.Fatal("field boundaries are ambiguous")
	}
	if VersionHash("k", "v", "e", t0) == VersionHash("k", "v", "e", t0.Add(time.Millisecond)) {
		t.Fatal("LastModified is not part of the hash")
	}
	if KeyHash("ab") == KeyHash("ab\x00") {
		t.Fatal("key hash collides")
	}
}

func TestTuple_Compare(t *testing.T) {
	s := Tuple{Time: ms(0), Key: "m", VersionID: "v2"}
	for _, c := range []struct {
		mod       time.Time
		key, ver  string
		wantAbove bool // S is above the version
	}{
		{ms(-time.Millisecond), "z", "z", true},
		{ms(time.Millisecond), "a", "a", false},
		{ms(0), "a", "z", true},
		{ms(0), "z", "a", false},
		{ms(0), "m", "v1", true},
		{ms(0), "m", "v3", false},
	} {
		if got := s.Compare(c.mod, c.key, c.ver) > 0; got != c.wantAbove {
			t.Errorf("Compare(%v,%q,%q) above=%v, want %v", c.mod, c.key, c.ver, got, c.wantAbove)
		}
	}
	if s.Compare(ms(0), "m", "v2") != 0 {
		t.Error("equal tuple not 0")
	}
	inf := Tuple{Time: ms(0), Inf: true}
	if inf.Compare(ms(0), "\xff\xff", "\xff") <= 0 || inf.Compare(ms(time.Millisecond), "", "") >= 0 {
		t.Error("infinite tuple ordering wrong")
	}
}

func FuzzParse(f *testing.F) {
	r := rand.New(rand.NewSource(1))
	for i := 0; i < 20; i++ {
		f.Add([]byte(randomV2(r)))
	}
	f.Add([]byte("key_c123"))
	f.Add([]byte("key_"))
	f.Add([]byte{0x00, 0x02, 'c'})
	f.Fuzz(func(t *testing.T, b []byte) {
		got, err := Parse(b)
		if err != nil {
			if !errors.Is(err, ErrInvalid) {
				t.Fatalf("error without code: %v", err)
			}
			return
		}
		if got.V2 == nil {
			return
		}
		var p opencdc.Position
		if got.V2.Snapshot != nil {
			p, err = got.V2.Snapshot.ToRecordPosition()
		} else {
			p, err = got.V2.CDC.ToRecordPosition()
		}
		if err != nil {
			t.Fatalf("parsed state does not re-encode: %v", err)
		}
		again, err := ParseV2(p)
		if err != nil || !reflect.DeepEqual(again, *got.V2) {
			t.Fatalf("round trip changed the state: %v", err)
		}
	})
}

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
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
)

// Position format v2 (design: ConduitIO/conduit
// docs/design-documents/20261015-s3-source-exact-resume.md, "Position format
// v2"):
//
//	byte 0      0x00          marker: v1 positions start with an S3 key, which cannot contain NUL
//	byte 1      0x02          format version
//	byte 2      's' or 'c'    type
//	payload                   see encodeSnapshot / encodeCDC
//	4 bytes     CRC-32C (Castagnoli, big endian) over bytes 0 .. end of payload
//	1 byte      0x00          terminator
//
// The terminator keeps the v0.9.x parser, which reads the byte after the last
// '_', from indexing past the end on a rollback: a v2 position never ends in
// '_'. The CRC rejects truncated or corrupted positions.
const (
	v2Marker     byte = 0x00
	v2Version    byte = 0x02
	v2Terminator byte = 0x00

	v2TypeSnapshot byte = 's'
	v2TypeCDC      byte = 'c'

	headerLen  = 3
	trailerLen = 5 // CRC + terminator

	// HashLen is the length of the hashes stored in a position.
	HashLen = 16

	// MaxKeyBytes is the longest key a position can hold; S3 keys are at most
	// 1024 bytes.
	MaxKeyBytes = 1024
	// MaxEarlyEntries and MaxTrackedUploads are hard limits of the format,
	// above any configurable bound, so a corrupt length cannot make the
	// parser allocate without limit. MaxEarlyEntries is the SDK acceptance
	// suite's maximum batch (10000) rounded up to the next power of two.
	MaxEarlyEntries   = 16384
	MaxTrackedUploads = 10000
)

// ErrInvalid is the stable error code of a position that cannot be parsed.
// The connector fails Open with it; the fix is to reset the pipeline's
// position (which re-snapshots).
var ErrInvalid = errors.New("s3.source.position_invalid")

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// minTime and maxTime bound the times a position may carry (unix ms).
const (
	minUnixMilli = 0
	maxUnixMilli = 253402300799999 // 9999-12-31T23:59:59.999Z
)

// Hash is a 16-byte digest stored in a position.
type Hash [HashLen]byte

// VersionHash identifies one object version for the early set E: the first
// 16 bytes of SHA-256 over the length-prefixed key, version id, ETag and
// LastModified (unix ms). Length prefixes make the input unambiguous whatever
// the fields contain.
func VersionHash(key, versionID, etag string, modified time.Time) Hash {
	var buf []byte
	buf = binary.AppendUvarint(buf, uint64(len(key)))
	buf = append(buf, key...)
	buf = binary.AppendUvarint(buf, uint64(len(versionID)))
	buf = append(buf, versionID...)
	buf = binary.AppendUvarint(buf, uint64(len(etag)))
	buf = append(buf, etag...)
	buf = binary.AppendVarint(buf, modified.UnixMilli())
	sum := sha256.Sum256(buf)
	var h Hash
	copy(h[:], sum[:HashLen])
	return h
}

// KeyHash identifies an object key for the tracked-upload set R.
func KeyHash(key string) Hash {
	var buf []byte
	buf = binary.AppendUvarint(buf, uint64(len(key)))
	buf = append(buf, key...)
	sum := sha256.Sum256(buf)
	var h Hash
	copy(h[:], sum[:HashLen])
	return h
}

// Tuple is a point in the total order of versions: (LastModified, key,
// version id). Inf is the tuple (Time, +inf): above every version with
// LastModified <= Time.
type Tuple struct {
	Time      time.Time
	Inf       bool
	Key       string
	VersionID string
}

// Compare orders t against the version (modified, key, versionID): negative
// if t is below it, zero if equal, positive if above.
func (t Tuple) Compare(modified time.Time, key, versionID string) int {
	tm, vm := t.Time.UnixMilli(), modified.UnixMilli()
	switch {
	case tm < vm:
		return -1
	case tm > vm:
		return 1
	case t.Inf:
		return 1
	}
	if c := compareStrings(t.Key, key); c != 0 {
		return c
	}
	return compareStrings(t.VersionID, versionID)
}

func compareStrings(a, b string) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

// EarlyEntry is one version delivered above S (the early set E).
type EarlyEntry struct {
	Modified time.Time
	Hash     Hash
}

// TrackedUpload is one multipart upload in progress whose initiation time is
// below the cut (the set R).
type TrackedUpload struct {
	KeyHash   Hash
	Initiated time.Time
}

// SnapshotV2 is the state of a snapshot in progress: the snapshot cut C0, the
// last key delivered, and the uploads tracked at snapshot start (R0).
type SnapshotV2 struct {
	Cut     time.Time
	LastKey string
	Tracked []TrackedUpload
}

// CDCV2 is the state of change capture: the settled cut S, the early set E and
// the tracked uploads R.
type CDCV2 struct {
	S       Tuple
	Early   []EarlyEntry
	Tracked []TrackedUpload
}

// V2 is a parsed v2 position: exactly one of Snapshot and CDC is set.
type V2 struct {
	Snapshot *SnapshotV2
	CDC      *CDCV2
}

// IsV2 reports whether p is in format v2 (it may still be invalid).
func IsV2(p opencdc.Position) bool {
	return len(p) > 0 && p[0] == v2Marker
}

// ToRecordPosition encodes the snapshot state as a v2 position.
func (s SnapshotV2) ToRecordPosition() (opencdc.Position, error) {
	if err := checkTime(s.Cut); err != nil {
		return nil, err
	}
	if len(s.LastKey) > MaxKeyBytes {
		return nil, fmt.Errorf("%w: key longer than %d bytes", ErrInvalid, MaxKeyBytes)
	}
	buf := header(v2TypeSnapshot)
	buf = binary.AppendVarint(buf, s.Cut.UnixMilli())
	buf = appendString(buf, s.LastKey)
	buf, err := appendTracked(buf, s.Tracked)
	if err != nil {
		return nil, err
	}
	return trailer(buf), nil
}

// ToRecordPosition encodes the CDC state as a v2 position. Entries of E are
// written in the order given (insertion order); entries must lie above S.
func (c CDCV2) ToRecordPosition() (opencdc.Position, error) {
	buf := header(v2TypeCDC)
	buf, err := c.appendS(buf)
	if err != nil {
		return nil, err
	}
	if len(c.Early) > MaxEarlyEntries {
		return nil, fmt.Errorf("%w: %d early entries, more than %d", ErrInvalid, len(c.Early), MaxEarlyEntries)
	}
	buf = binary.AppendUvarint(buf, uint64(len(c.Early)))
	base := c.S.Time.UnixMilli()
	for _, e := range c.Early {
		d := e.Modified.UnixMilli() - base
		if d < 0 || (c.S.Inf && d == 0) {
			return nil, fmt.Errorf("%w: early entry at %v is not above S", ErrInvalid, e.Modified)
		}
		buf = binary.AppendUvarint(buf, uint64(d))
		buf = append(buf, e.Hash[:]...)
	}
	buf, err = appendTracked(buf, c.Tracked)
	if err != nil {
		return nil, err
	}
	return trailer(buf), nil
}

func (c CDCV2) appendS(buf []byte) ([]byte, error) {
	if err := checkTime(c.S.Time); err != nil {
		return nil, err
	}
	if len(c.S.Key) > MaxKeyBytes || len(c.S.VersionID) > MaxKeyBytes {
		return nil, fmt.Errorf("%w: key or version id longer than %d bytes", ErrInvalid, MaxKeyBytes)
	}
	if c.S.Inf && (c.S.Key != "" || c.S.VersionID != "") {
		return nil, fmt.Errorf("%w: an infinite S carries no key", ErrInvalid)
	}
	buf = binary.AppendVarint(buf, c.S.Time.UnixMilli())
	if c.S.Inf {
		buf = append(buf, 1)
	} else {
		buf = append(buf, 0)
	}
	buf = appendString(buf, c.S.Key)
	buf = appendString(buf, c.S.VersionID)
	return buf, nil
}

func header(typ byte) []byte {
	return []byte{v2Marker, v2Version, typ}
}

func trailer(buf []byte) opencdc.Position {
	buf = binary.BigEndian.AppendUint32(buf, crc32.Checksum(buf, crcTable))
	return append(buf, v2Terminator)
}

func appendString(buf []byte, s string) []byte {
	buf = binary.AppendUvarint(buf, uint64(len(s)))
	return append(buf, s...)
}

func appendTracked(buf []byte, tracked []TrackedUpload) ([]byte, error) {
	if len(tracked) > MaxTrackedUploads {
		return nil, fmt.Errorf("%w: %d tracked uploads, more than %d", ErrInvalid, len(tracked), MaxTrackedUploads)
	}
	buf = binary.AppendUvarint(buf, uint64(len(tracked)))
	for _, u := range tracked {
		if err := checkTime(u.Initiated); err != nil {
			return nil, err
		}
		buf = append(buf, u.KeyHash[:]...)
		buf = binary.AppendVarint(buf, u.Initiated.UnixMilli())
	}
	return buf, nil
}

func checkTime(t time.Time) error {
	ms := t.UnixMilli()
	if ms < minUnixMilli || ms > maxUnixMilli {
		return fmt.Errorf("%w: time %v out of range", ErrInvalid, t)
	}
	return nil
}

// ParseV2 parses a v2 position strictly: marker, version, type, CRC,
// terminator, lengths, limits, duplicates and trailing bytes are all checked.
// Every error wraps ErrInvalid; nothing is ever silently defaulted.
func ParseV2(p opencdc.Position) (V2, error) {
	if len(p) < headerLen+trailerLen {
		return V2{}, invalid(p, "too short")
	}
	if p[0] != v2Marker || p[1] != v2Version {
		return V2{}, invalid(p, "unknown marker or version")
	}
	if p[len(p)-1] != v2Terminator {
		return V2{}, invalid(p, "missing terminator")
	}
	body := p[:len(p)-trailerLen]
	want := binary.BigEndian.Uint32(p[len(p)-trailerLen : len(p)-1])
	if crc32.Checksum(body, crcTable) != want {
		return V2{}, invalid(p, "checksum mismatch")
	}
	r := &reader{b: body[headerLen:]}
	var out V2
	switch body[2] {
	case v2TypeSnapshot:
		s := &SnapshotV2{}
		s.Cut = r.time()
		s.LastKey = r.string(MaxKeyBytes)
		s.Tracked = r.tracked()
		out.Snapshot = s
	case v2TypeCDC:
		c := &CDCV2{}
		c.S.Time = r.time()
		switch r.byte() {
		case 0:
		case 1:
			c.S.Inf = true
		default:
			r.fail("bad infinity flag")
		}
		c.S.Key = r.string(MaxKeyBytes)
		c.S.VersionID = r.string(MaxKeyBytes)
		if c.S.Inf && (c.S.Key != "" || c.S.VersionID != "") {
			r.fail("an infinite S carries no key")
		}
		c.Early = r.early(c.S)
		c.Tracked = r.tracked()
		out.CDC = c
	default:
		return V2{}, invalid(p, "unknown type")
	}
	if r.err != nil {
		return V2{}, invalid(p, r.err.Error())
	}
	if len(r.b) != 0 {
		return V2{}, invalid(p, "trailing bytes")
	}
	return out, nil
}

func invalid(p opencdc.Position, why string) error {
	return fmt.Errorf("%w: %s (position of %d bytes); reset the pipeline's position to start over with a snapshot", ErrInvalid, why, len(p))
}

// reader decodes the payload; the first error sticks and later reads return
// zero values.
type reader struct {
	b   []byte
	err error
}

func (r *reader) fail(why string) {
	if r.err == nil {
		r.err = errors.New(why)
	}
	r.b = nil
}

func (r *reader) byte() byte {
	if r.err != nil {
		return 0
	}
	if len(r.b) < 1 {
		r.fail("truncated")
		return 0
	}
	c := r.b[0]
	r.b = r.b[1:]
	return c
}

func (r *reader) uvarint() uint64 {
	if r.err != nil {
		return 0
	}
	v, n := binary.Uvarint(r.b)
	if n <= 0 {
		r.fail("bad uvarint")
		return 0
	}
	r.b = r.b[n:]
	return v
}

func (r *reader) varint() int64 {
	if r.err != nil {
		return 0
	}
	v, n := binary.Varint(r.b)
	if n <= 0 {
		r.fail("bad varint")
		return 0
	}
	r.b = r.b[n:]
	return v
}

func (r *reader) time() time.Time {
	ms := r.varint()
	if r.err == nil && (ms < minUnixMilli || ms > maxUnixMilli) {
		r.fail("time out of range")
	}
	return time.UnixMilli(ms).UTC()
}

func (r *reader) string(maxLen uint64) string {
	n := r.uvarint()
	if r.err != nil {
		return ""
	}
	if n > MaxKeyBytes || n > maxLen {
		r.fail("bad string length")
		return ""
	}
	// n <= MaxKeyBytes, so the conversion cannot overflow.
	l := int(n) // bounded by MaxKeyBytes just above
	if l > len(r.b) {
		r.fail("bad string length")
		return ""
	}
	s := string(r.b[:l])
	r.b = r.b[l:]
	return s
}

func (r *reader) hash() Hash {
	var h Hash
	if r.err != nil {
		return h
	}
	if len(r.b) < HashLen {
		r.fail("truncated hash")
		return h
	}
	copy(h[:], r.b[:HashLen])
	r.b = r.b[HashLen:]
	return h
}

func (r *reader) count(limit uint64) int {
	n := r.uvarint()
	if r.err != nil {
		return 0
	}
	if n > limit {
		r.fail("too many entries")
		return 0
	}
	return int(n) // bounded by limit just above
}

func (r *reader) early(s Tuple) []EarlyEntry {
	n := r.count(MaxEarlyEntries)
	if n == 0 {
		return nil
	}
	out := make([]EarlyEntry, 0, n)
	seen := make(map[Hash]struct{}, n)
	base := s.Time.UnixMilli()
	for i := 0; i < n && r.err == nil; i++ {
		d := r.uvarint()
		h := r.hash()
		if r.err != nil {
			break
		}
		if d > maxUnixMilli {
			r.fail("early entry time out of range")
			break
		}
		delta := int64(d) // d <= maxUnixMilli, far below MaxInt64
		if base+delta > maxUnixMilli {
			r.fail("early entry time out of range")
			break
		}
		if s.Inf && d == 0 {
			r.fail("early entry not above S")
			break
		}
		if _, dup := seen[h]; dup {
			r.fail("duplicate early entry")
			break
		}
		seen[h] = struct{}{}
		out = append(out, EarlyEntry{Modified: time.UnixMilli(base + delta).UTC(), Hash: h})
	}
	return out
}

func (r *reader) tracked() []TrackedUpload {
	n := r.count(MaxTrackedUploads)
	if n == 0 {
		return nil
	}
	out := make([]TrackedUpload, 0, n)
	type id struct {
		h  Hash
		ms int64
	}
	seen := make(map[id]struct{}, n)
	for i := 0; i < n && r.err == nil; i++ {
		h := r.hash()
		t := r.time()
		if r.err != nil {
			break
		}
		k := id{h, t.UnixMilli()}
		if _, dup := seen[k]; dup {
			r.fail("duplicate tracked upload")
			break
		}
		seen[k] = struct{}{}
		out = append(out, TrackedUpload{KeyHash: h, Initiated: t})
	}
	return out
}

// Parsed is a persisted position of either format. Exactly one of V1 and V2 is
// set, or neither for a nil position (a fresh start).
type Parsed struct {
	V1 *Position
	V2 *V2
}

// Parse reads a persisted position of either format. v1 positions are read
// only to upgrade them (see the design's "Upgrade from v1 positions").
func Parse(p opencdc.Position) (Parsed, error) {
	if len(p) == 0 {
		return Parsed{}, nil
	}
	if IsV2(p) {
		v, err := ParseV2(p)
		if err != nil {
			return Parsed{}, err
		}
		return Parsed{V2: &v}, nil
	}
	v, err := ParseRecordPosition(p)
	if err != nil {
		return Parsed{}, fmt.Errorf("%w: %w", ErrInvalid, err)
	}
	return Parsed{V1: &v}, nil
}

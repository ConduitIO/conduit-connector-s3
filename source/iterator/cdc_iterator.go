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
	"container/heap"
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-s3/source/position"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/rs/zerolog"
	"gopkg.in/tomb.v2"
)

// Defaults of the change capture bounds (design "Settings").
const (
	// DefaultMaxEarlyObjects bounds the early set E kept in every position.
	// It is the SDK acceptance suite's maximum batch (10000) rounded up to
	// the next power of two, so that a full burst plus the objects already in
	// flight is delivered at once instead of being deferred for
	// lookbackPeriod. Memory: an entry is about 20 bytes in every in-flight
	// position, so the full bound costs about 330 KiB per position (see the
	// config reference for the formula).
	DefaultMaxEarlyObjects        = 16384
	DefaultMaxSettledBytesPerPoll = 64 << 20
)

// CDCConfig configures a CDCIterator.
type CDCConfig struct {
	Bucket, Prefix      string
	PollingPeriod       time.Duration
	Lookback            time.Duration
	MultipartHold       bool
	MaxTrackedUploads   int
	MaxPendingUploadAge time.Duration
	MaxEarlyObjects     int
	// MaxSettledBytesPerPoll bounds the memory of the settled candidates of
	// one poll (keys can be up to 1 KiB, so it is a byte budget).
	MaxSettledBytesPerPoll int64
	// Now is the host clock; nil means time.Now.
	Now func() time.Time
}

// CDCIterator captures changes by listing object versions every polling
// period, and resumes exactly from any of its positions.
//
// The state is the design's (S, E, R): ConduitIO/conduit
// docs/design-documents/20261015-s3-source-exact-resume.md.
//
//   - S, the settled cut: every version with tuple (LastModified, key, version
//     id) <= S has been delivered, except the versions matching an entry of R.
//   - E, the early set: the versions above S that were delivered, as
//     (LastModified, hash). Bounded by MaxEarlyObjects.
//   - R, the tracked uploads: multipart uploads still in progress whose
//     initiation time is below the cut. AWS stamps the completed object with
//     that time, so it lands below S; it is delivered because it matches R.
//
// Each poll: list uploads (Date Tu), list versions (Date of the first page Tv),
// cut C = min(Tu, Tv, host) - lookback, lowered below an old upload that does
// not fit R. Then deliver, in this order: versions <= S matching R; settled
// versions (<= C) above S and not in E, smallest tuple first, moving S with
// each; S := (C, +inf) if no settled version was deferred; early versions
// (> C, not in E) while E has room.
//
// Every record's position is (S, E, R) as of that record, so resuming from any
// record delivers exactly the versions not yet delivered: those above S that
// are not in E, plus those below S that match R.
//
// Only the latest version of a key is delivered; intermediate versions written
// between polls are collapsed. Listing-based change capture rests on the
// visibility assumptions A1 to A3 of the design; objects outside them can be
// missed, as documented.
type CDCIterator struct {
	cfg    CDCConfig
	client *s3.Client
	log    *zerolog.Logger

	s     position.Tuple
	early []position.EarlyEntry
	inE   map[position.Hash]struct{}
	r     []position.TrackedUpload
	// sameSecond remembers the uploads that share their initiation second
	// with a listed version of their key (see cutFor).
	sameSecond map[trackedID]struct{}
	holds      *holdWarner
	// staleClean remembers uploads past maxPendingUploadAge that share their
	// second with nothing; base is the in-memory baseline of tracked uploads
	// (see baselines); listed is the set of uploads of the current poll.
	staleClean map[trackedID]struct{}
	// pending are the entries of this poll whose upload is listed but has no
	// baseline (it could not be trusted); they match nothing in this poll.
	pending map[trackedID]struct{}
	// listedUp maps the listed uploads to their ids; running caches, per poll,
	// whether a listed upload is still in progress (see checkRunning).
	listedUp map[trackedID]string
	running  map[trackedID]bool
	base     baselines
	listed   map[trackedID]struct{}
	// upgradeFloor is set when resuming from a v1 position: the first poll
	// lowers S below any old upload that cannot be tracked (design "Upgrade").
	upgradeFloor bool

	buffer chan opencdc.Record
	ticker *time.Ticker
	tomb   *tomb.Tomb

	fullWarned time.Time
	// sampled rate-limits the per-poll multipart warnings.
	sampled *zerolog.Logger
}

// NewCDCIterator starts change capture from state (S, E, R) and polls every
// PollingPeriod.
func NewCDCIterator(cfg CDCConfig, client *s3.Client, state position.CDCV2, log *zerolog.Logger) *CDCIterator {
	w := newCDCIterator(cfg, client, state, log)
	w.start()
	return w
}

// NewCDCIteratorFromV1 resumes from a position written by v0.9.x: change
// capture from (seconds - lookback, +inf) with E and R empty. It redelivers
// the lookback window once, as v0.9.x did on every restart, and is exact from
// then on.
func NewCDCIteratorFromV1(cfg CDCConfig, client *s3.Client, p position.Position, log *zerolog.Logger) *CDCIterator {
	floor := p.Timestamp.Add(-cfg.Lookback).UTC().Truncate(time.Millisecond)
	if floor.Before(time.UnixMilli(0)) {
		floor = time.UnixMilli(0).UTC()
	}
	w := newCDCIterator(cfg, client, position.CDCV2{S: position.Tuple{Time: floor, Inf: true}}, log)
	w.upgradeFloor = true
	w.start()
	return w
}

func newCDCIterator(cfg CDCConfig, client *s3.Client, state position.CDCV2, log *zerolog.Logger) *CDCIterator {
	if log == nil {
		nop := zerolog.Nop()
		log = &nop
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	w := &CDCIterator{
		cfg: cfg, client: client, log: log,
		s:          state.S,
		r:          append([]position.TrackedUpload(nil), state.Tracked...),
		inE:        make(map[position.Hash]struct{}, len(state.Early)),
		sameSecond: map[trackedID]struct{}{},
		holds:      newHoldWarner(),
		staleClean: map[trackedID]struct{}{},
		pending:    map[trackedID]struct{}{},
		listedUp:   map[trackedID]string{},
		running:    map[trackedID]bool{},
		base:       baselines{},
		listed:     map[trackedID]struct{}{},
		buffer:     make(chan opencdc.Record, 1),
		tomb:       &tomb.Tomb{},
	}
	for _, e := range state.Early {
		w.addEarly(e)
	}
	sampled := log.Sample(&zerolog.BurstSampler{Burst: 3, Period: time.Minute})
	w.sampled = &sampled
	return w
}

func (w *CDCIterator) start() {
	w.ticker = time.NewTicker(w.cfg.PollingPeriod)
	w.tomb.Go(func() error {
		defer close(w.buffer)
		emit := func(r opencdc.Record) error {
			select {
			case w.buffer <- r:
				return nil
			case <-w.tomb.Dying():
				return w.tomb.Err()
			}
		}
		for {
			select {
			case <-w.tomb.Dying():
				return w.tomb.Err()
			case <-w.ticker.C:
				if err := w.step(w.tomb.Context(nil), emit); err != nil { //nolint:staticcheck // SA1012 tomb expects nil
					return err
				}
			}
		}
	})
}

// HasNext reports whether a record is buffered, or whether the iterator
// stopped (Next then reports why).
func (w *CDCIterator) HasNext(_ context.Context) bool {
	return len(w.buffer) > 0 || !w.tomb.Alive()
}

// Next returns the next record, or the error the iterator stopped with.
func (w *CDCIterator) Next(ctx context.Context) (opencdc.Record, error) {
	select {
	case r, ok := <-w.buffer:
		if !ok {
			<-w.tomb.Dead()
			return opencdc.Record{}, w.tomb.Err()
		}
		return r, nil
	case <-w.tomb.Dead():
		return opencdc.Record{}, w.tomb.Err()
	case <-ctx.Done():
		return opencdc.Record{}, ctx.Err()
	}
}

func (w *CDCIterator) Stop() {
	if w.ticker != nil {
		w.ticker.Stop()
	}
	w.tomb.Kill(errors.New("cdc iterator is stopped"))
}

// State returns the current (S, E, R). Tests and the handoff use it.
func (w *CDCIterator) State() position.CDCV2 {
	return position.CDCV2{
		S:       w.s,
		Early:   append([]position.EarlyEntry(nil), w.early...),
		Tracked: append([]position.TrackedUpload(nil), w.r...),
	}
}

func (w *CDCIterator) addEarly(e position.EarlyEntry) {
	if _, ok := w.inE[e.Hash]; ok {
		return
	}
	w.inE[e.Hash] = struct{}{}
	w.early = append(w.early, e)
}

// pruneEarly drops early entries that are at or below S: entries older than
// S's millisecond, and, for an infinite S, entries in that millisecond too.
// An entry in S's own millisecond but above S in key order is kept; keeping a
// delivered version too long only costs room, never a redelivery.
func (w *CDCIterator) pruneEarly() {
	keep := w.early[:0]
	for _, e := range w.early {
		below := e.Modified.Before(w.s.Time) || (w.s.Inf && !e.Modified.After(w.s.Time))
		if below {
			delete(w.inE, e.Hash)
			continue
		}
		keep = append(keep, e)
	}
	w.early = keep
}

func (w *CDCIterator) position() (opencdc.Position, error) {
	return position.CDCV2{S: w.s, Early: w.early, Tracked: w.r}.ToRecordPosition()
}

// candidate is one listed version (the latest of its key).
type candidate struct {
	key, versionID, etag string
	modified             time.Time
	operation            opencdc.Operation
}

func (c candidate) bytes() int64 { return int64(len(c.key)+len(c.versionID)+len(c.etag)) + 96 }

func (c candidate) less(o candidate) bool {
	if !c.modified.Equal(o.modified) {
		return c.modified.Before(o.modified)
	}
	if c.key != o.key {
		return c.key < o.key
	}
	return c.versionID < o.versionID
}

// maxHeap keeps the largest tuple on top, so a bounded heap evicts the newest.
type maxHeap []candidate

func (h maxHeap) Len() int           { return len(h) }
func (h maxHeap) Less(i, j int) bool { return h[j].less(h[i]) }
func (h maxHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *maxHeap) Push(x any)        { *h = append(*h, x.(candidate)) }
func (h *maxHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// pollPlan is what one poll decided to deliver, in order.
type pollPlan struct {
	tracked  []candidate // versions <= S matching R
	settled  []candidate // ascending tuple order
	deferred bool        // settled candidates were left out (memory budget)
	early    []candidate // ascending tuple order, at most the room in E
	waiting  int         // early candidates that did not fit
	cut      time.Time
	resolved map[trackedID]struct{} // R entries whose upload is gone
}

// step runs one poll and emits its records. State moves with each emitted
// record, so every record's position is exact for itself. A failure returns
// before anything is emitted for the failed listing.
func (w *CDCIterator) step(ctx context.Context, emit func(opencdc.Record) error) error {
	start := w.cfg.Now()
	host := start
	tu := host
	var uploads []types.MultipartUpload
	if w.cfg.MultipartHold {
		var err error
		uploads, tu, err = listUploads(ctx, w.client, w.cfg.Bucket, w.cfg.Prefix, host, w.log)
		if err != nil {
			return err // AccessDenied stops the iterator (design R4): no fallback
		}
	}

	plan, err := w.plan(ctx, uploads, tu, host)
	if err != nil {
		return err
	}
	if d := w.cfg.Now().Sub(start); d > w.cfg.Lookback && w.cfg.Lookback > 0 {
		w.log.Warn().Str("code", "s3.source.poll_slow").Dur("duration", d).Dur("lookback", w.cfg.Lookback).
			Msg("a poll took longer than lookbackPeriod: delivery is still exact (the cut uses the first page's Date), but latency suffers; narrow the prefix or expire noncurrent versions")
	}

	// R entries for old uploads are added before anything is emitted: S may
	// pass their initiation time in this poll, so every position from now on
	// must carry them (Invariant 3).
	for id := range plan.resolved {
		w.dropTracked(id)
	}

	// 1. Tracked: completed objects of tracked uploads, below S.
	for _, c := range plan.tracked {
		if _, err := w.deliver(ctx, c, emit, func() { w.dropTrackedFor(c) }); err != nil {
			return err
		}
	}
	// 2. Settled, in tuple order; S moves with each.
	for _, c := range plan.settled {
		if _, err := w.deliver(ctx, c, emit, func() {
			w.s = position.Tuple{Time: c.modified, Key: c.key, VersionID: c.versionID}
			w.pruneEarly()
			w.dropTrackedFor(c) // an upload that completed between the two listings
		}); err != nil {
			return err
		}
	}
	// 3. S jumps to the cut only if nothing settled was left out (B2):
	// everything visible at or below the cut was just delivered, and by the
	// assumptions nothing new can appear there.
	if !plan.deferred && (plan.cut.After(w.s.Time) || (plan.cut.Equal(w.s.Time) && !w.s.Inf)) {
		w.s = position.Tuple{Time: plan.cut, Inf: true}
		w.pruneEarly()
	}
	// 4. Early, while E has room.
	for _, c := range plan.early {
		if len(w.early) >= w.cfg.MaxEarlyObjects {
			plan.waiting++
			continue
		}
		if _, err := w.deliver(ctx, c, emit, func() {
			w.addEarly(position.EarlyEntry{Modified: c.modified, Hash: position.VersionHash(c.key, c.versionID, c.etag, c.modified)})
			w.dropTrackedFor(c)
		}); err != nil {
			return err
		}
	}
	if plan.waiting > 0 && w.cfg.Now().Sub(w.fullWarned) > time.Minute {
		w.fullWarned = w.cfg.Now()
		w.log.Warn().Str("code", "s3.source.early_window_full").Int("waiting", plan.waiting).
			Int("max_early_objects", w.cfg.MaxEarlyObjects).Dur("lookback", w.cfg.Lookback).
			Msg("more new objects than cdc.maxEarlyObjects: the rest are delivered once they settle, up to lookbackPeriod later")
	}
	return nil
}

// deliver fetches c, moves the state with apply, encodes the position and
// emits the record. A version that changed or disappeared after the listing is
// skipped without touching the state (its successor is a later candidate).
func (w *CDCIterator) deliver(ctx context.Context, c candidate, emit func(opencdc.Record) error, apply func()) (bool, error) {
	if err := w.checkRunning(ctx, c); err != nil {
		return false, err
	}
	var body []byte
	var meta opencdc.Metadata
	if c.operation != opencdc.OperationDelete {
		object, err := w.fetch(ctx, c)
		if err != nil {
			if isGone(err) || errors.Is(err, errVersionChanged) {
				w.log.Debug().Str("key", c.key).Str("version_id", c.versionID).Msg("version changed or was deleted after it was listed; its successor is delivered instead")
				return false, nil
			}
			return false, fmt.Errorf("could not fetch S3 object %q: %w", c.key, err)
		}
		defer object.Body.Close()
		body, err = io.ReadAll(object.Body)
		if err != nil {
			return false, fmt.Errorf("could not read S3 object %q: %w", c.key, err)
		}
		meta = opencdc.Metadata{MetadataS3HeaderPrefix + MetadataContentType: aws.ToString(object.ContentType)}
		for k, v := range object.Metadata {
			meta[k] = v
		}
	} else {
		meta = opencdc.Metadata{}
	}
	apply()
	pos, err := w.position()
	if err != nil {
		// A state that cannot be encoded is a bug; stopping is safe (nothing
		// was emitted for it, and the last emitted position is unchanged).
		return false, fmt.Errorf("could not encode the position: %w", err)
	}
	var rec opencdc.Record
	key := opencdc.RawData(c.key)
	switch c.operation {
	case opencdc.OperationCreate:
		rec = sdk.Util.Source.NewRecordCreate(pos, meta, key, opencdc.RawData(body))
	case opencdc.OperationUpdate:
		rec = sdk.Util.Source.NewRecordUpdate(pos, meta, key, nil, opencdc.RawData(body))
	default:
		rec = sdk.Util.Source.NewRecordDelete(pos, meta, key, nil)
	}
	return true, emit(rec)
}

var errVersionChanged = errors.New("the object was overwritten after it was listed")

// fetch reads the listed version: by version id in a versioned bucket,
// otherwise with If-Match on the ETag and the same Last-Modified second
// (design R8: a same-content overwrite keeps the ETag, not the time).
func (w *CDCIterator) fetch(ctx context.Context, c candidate) (*s3.GetObjectOutput, error) {
	in := &s3.GetObjectInput{Bucket: aws.String(w.cfg.Bucket), Key: aws.String(c.key)}
	if c.versionID != "" && c.versionID != nullVersionID {
		in.VersionId = aws.String(c.versionID)
	} else {
		in.IfMatch = aws.String(c.etag)
	}
	out, err := w.client.GetObject(ctx, in)
	if err != nil {
		return nil, err
	}
	if in.VersionId == nil && (out.LastModified == nil || out.LastModified.Unix() != c.modified.Unix()) {
		out.Body.Close()
		return nil, errVersionChanged
	}
	return out, nil
}

func (w *CDCIterator) dropTracked(id trackedID) {
	keep := w.r[:0]
	for _, u := range w.r {
		if idOf(u) != id {
			keep = append(keep, u)
		}
	}
	w.r = keep
}

// dropTrackedFor drops the R entry that c, a version about to be delivered, is
// the completed object of. Every delivery path calls it: if an upload
// completes between the uploads listing and the versions listing, its object
// is delivered as a settled or early version while its entry is still in R,
// and once S passes it the entry would match it again (a duplicate). The
// entry is dropped in the same state step as the delivery, so the position of
// the record already excludes it.
func (w *CDCIterator) dropTrackedFor(c candidate) {
	if c.operation == opencdc.OperationDelete {
		return
	}
	// Every entry that c matches is settled by this delivery.
	for _, u := range append([]position.TrackedUpload(nil), w.r...) {
		if !w.entryMatches(u, c) {
			continue
		}
		id := idOf(u)
		if _, listed := w.listed[id]; listed && w.running[id] {
			// The upload is still in progress, so c is not its object (a
			// third upload of the key stamped it in the same second). Keep the
			// entry for the upload's own version, and remember c so that it
			// does not match again.
			w.base[id] = append(w.base[id], position.VersionHash(c.key, c.versionID, c.etag, c.modified))
			continue
		}
		w.dropTracked(id)
	}
}

// checkRunning finds out, before c is delivered, whether the uploads whose
// entries c matches are still in progress. They were listed at Tu, but one may
// have completed since: c is then its own object and its entry goes (dropTrackedFor).
// Still running, the entry stays and c joins its baseline.
func (w *CDCIterator) checkRunning(ctx context.Context, c candidate) error {
	if c.operation == opencdc.OperationDelete {
		return nil
	}
	for _, u := range w.r {
		id := idOf(u)
		upload, listed := w.listedUp[id]
		if !listed || !w.entryMatches(u, c) {
			continue
		}
		if _, done := w.running[id]; done {
			continue
		}
		in, err := uploadInProgress(ctx, w.client, w.cfg.Bucket, c.key, upload)
		if err != nil {
			return err
		}
		w.running[id] = in
	}
	return nil
}

// entryMatches reports whether c is the completed object of the tracked
// upload u: matchesTracked's rule, minus the versions in u's baseline.
func (w *CDCIterator) entryMatches(u position.TrackedUpload, c candidate) bool {
	if _, pending := w.pending[idOf(u)]; pending {
		return false
	}
	return matchesTracked([]position.TrackedUpload{u}, c.key, c.modified, c.etag) &&
		!w.base.excludes(idOf(u), c.key, c.versionID, c.modified, c.etag)
}

func (w *CDCIterator) matchesTracked(c candidate) bool {
	for _, u := range w.r {
		if w.entryMatches(u, c) {
			return true
		}
	}
	return false
}

// plan lists the versions and decides what this poll delivers. It does not
// change the state, except for adding newly tracked uploads to R (which must
// happen before any record of this poll is emitted).
func (w *CDCIterator) plan(ctx context.Context, uploads []types.MultipartUpload, tu, host time.Time) (pollPlan, error) {
	in := &s3.ListObjectVersionsInput{Bucket: aws.String(w.cfg.Bucket), Prefix: aws.String(w.cfg.Prefix)}
	pl := &planner{w: w, settled: &maxHeap{}, early: &maxHeap{}, room: w.cfg.MaxEarlyObjects - len(w.early), matched: map[trackedID]struct{}{}}
	col := &versionCollector{consider: pl.consider}
	for first := true; ; first = false {
		out, err := w.client.ListObjectVersions(ctx, in)
		if err != nil {
			return pollPlan{}, fmt.Errorf("couldn't list object versions: %w", err)
		}
		if first {
			cut, err := w.cutFor(ctx, uploads, tu, responseDate(out.ResultMetadata, host, w.log), host)
			if err != nil {
				return pollPlan{}, err
			}
			pl.p.cut = cut
		}
		for i := range out.Versions {
			col.version(&out.Versions[i])
		}
		for i := range out.DeleteMarkers {
			col.deleteMarker(&out.DeleteMarkers[i])
		}
		if !aws.ToBool(out.IsTruncated) {
			break
		}
		if aws.ToString(out.NextKeyMarker) == "" && aws.ToString(out.NextVersionIdMarker) == "" {
			return pollPlan{}, errors.New("couldn't list object versions: the listing is truncated but has no next marker")
		}
		in.KeyMarker, in.VersionIdMarker = out.NextKeyMarker, out.NextVersionIdMarker
	}
	col.flush()

	p := pl.p
	p.resolved = w.resolved(uploads, pl.matched)
	p.settled = sortedAsc(pl.settled)
	p.early = sortedAsc(pl.early)
	sort.Slice(p.tracked, func(i, j int) bool { return p.tracked[i].less(p.tracked[j]) })
	return p, nil
}

// cutFor computes this poll's cut from T = min(Tu, Tv, host) and the uploads,
// updates R with newly tracked uploads, and applies the v1 upgrade floor. The
// cut never moves S backwards (a host clock that jumped back): settling then
// pauses until it catches up.
func (w *CDCIterator) cutFor(ctx context.Context, uploads []types.MultipartUpload, tu, tv, host time.Time) (time.Time, error) {
	// Uploads that share their initiation second with another object of the
	// key hold the cut instead of being tracked (see cutAndTrack). A positive
	// answer cannot turn negative while the upload runs, so it is remembered;
	// a negative one is not, because the upload is tracked right away or, when
	// R is full, is probed again next poll. For an upload past
	// maxPendingUploadAge, which is not tracked unless it conflicts, a negative
	// answer is remembered too, or every poll would probe it again.
	w.listed = make(map[trackedID]struct{}, len(uploads))
	w.listedUp = make(map[trackedID]string, len(uploads))
	w.running = map[trackedID]bool{}
	for _, u := range uploads {
		if u.Key != nil && u.Initiated != nil {
			id := idOf(position.TrackedUpload{KeyHash: position.KeyHash(*u.Key), Initiated: u.Initiated.UTC().Truncate(time.Millisecond)})
			w.listed[id] = struct{}{}
			w.listedUp[id] = aws.ToString(u.UploadId)
		}
	}
	for _, m := range []map[trackedID]struct{}{w.sameSecond, w.staleClean} {
		for id := range m {
			if _, ok := w.listed[id]; !ok {
				delete(m, id)
			}
		}
	}
	conflict := func(u types.MultipartUpload, stale bool) (bool, error) {
		id := idOf(position.TrackedUpload{KeyHash: position.KeyHash(*u.Key), Initiated: u.Initiated.UTC().Truncate(time.Millisecond)})
		if _, ok := w.sameSecond[id]; ok {
			return true, nil
		}
		if _, ok := w.staleClean[id]; ok {
			return false, nil
		}
		found, err := versionInSecond(ctx, w.client, w.cfg.Bucket, aws.ToString(u.Key), *u.Initiated)
		switch {
		case err != nil:
		case found:
			w.sameSecond[id] = struct{}{}
		case stale:
			w.staleClean[id] = struct{}{}
		default:
			// Tracked right away (or held for lack of room): nothing of the
			// key was in the upload's second, so its baseline is empty.
			w.base[id] = []position.Hash{}
		}
		return found, err
	}
	now := w.cfg.Now()
	w.holds.begin()
	held := func(u types.MultipartUpload) { w.holds.hold(now, u, w.log) }
	settled := w.s.Time
	if w.upgradeFloor {
		// S of a v1 position lies above any upload that is running; the hold
		// below lowers it (see further down), so it must not count as passed.
		settled = time.Time{}
	}
	var cut time.Time
	cut, r, err := cutAndTrack(earliest(tu, tv, host), w.cfg.Lookback, tu, uploads, w.r,
		trackOpts{Settled: settled, MaxTracked: w.cfg.MaxTrackedUploads, MaxAge: w.cfg.MaxPendingUploadAge, Conflict: conflict, Held: held}, w.sampled)
	w.holds.end(now)
	if err != nil {
		return time.Time{}, err
	}
	w.r = r
	// Entries of v0.11.x positions, and entries tracked because S had already
	// passed their second, get their baseline now, before this poll matches
	// any version against them.
	if err := w.base.verify(ctx, w.client, w.cfg.Bucket, uploads, w.r); err != nil {
		return time.Time{}, err
	}
	w.base.prune(w.r)
	w.markPending()
	cut = cut.UTC().Truncate(time.Millisecond)
	if w.upgradeFloor {
		// Upgrade from v1: S may not lie above an old upload that could not
		// be tracked (design "Upgrade from v1 positions").
		if cut.Before(w.s.Time) {
			w.s = position.Tuple{Time: cut, Inf: true}
		}
		w.upgradeFloor = false
	}
	if cut.Before(w.s.Time) {
		cut = w.s.Time
	}
	return cut, nil
}

// markPending finds the entries whose upload was listed at Tu but got no
// baseline (the upload ended while the baseline was taken): they match nothing
// in this poll, because the tip of their key may be an earlier version. Next
// poll they either get a baseline or resolve under the old rule, when their
// upload's version is the tip.
func (w *CDCIterator) markPending() {
	w.pending = map[trackedID]struct{}{}
	for _, e := range w.r {
		id := idOf(e)
		if _, listed := w.listed[id]; !listed {
			continue
		}
		if _, ok := w.base[id]; !ok {
			w.pending[id] = struct{}{}
		}
	}
}

// resolved lists the R entries whose upload is gone (completed or aborted
// before the uploads listing). A completed version that was listed was matched
// and is delivered (which drops its entry); the others are dropped this poll.
func (w *CDCIterator) resolved(uploads []types.MultipartUpload, matched map[trackedID]struct{}) map[trackedID]struct{} {
	out := map[trackedID]struct{}{}
	if !w.cfg.MultipartHold {
		return out
	}
	listed := map[trackedID]struct{}{}
	for _, u := range uploads {
		if u.Key != nil && u.Initiated != nil {
			listed[idOf(position.TrackedUpload{KeyHash: position.KeyHash(*u.Key), Initiated: u.Initiated.UTC().Truncate(time.Millisecond)})] = struct{}{}
		}
	}
	for _, u := range w.r {
		id := idOf(u)
		if _, ok := listed[id]; ok {
			continue
		}
		if _, ok := matched[id]; ok {
			continue
		}
		out[id] = struct{}{}
	}
	return out
}

// planner sorts the candidates of one poll into tracked, settled (bounded by
// bytes) and early (bounded by the room in E).
type planner struct {
	w            *CDCIterator
	p            pollPlan
	settled      *maxHeap
	settledBytes int64
	early        *maxHeap
	room         int
	matched      map[trackedID]struct{}
	// evicted is the smallest tuple pushed out of the settled heap this poll.
	// Invariant 3: the budget is in bytes and candidates differ in size, so a
	// later, smaller candidate above an evicted one can still fit. Delivering
	// it would move S past the evicted tuple and skip that record for good, so
	// nothing at or above the watermark is admitted.
	evicted *candidate
}

func (pl *planner) consider(c candidate) {
	w := pl.w
	if w.s.Compare(c.modified, c.key, c.versionID) >= 0 {
		// At or below S: only the completed object of a tracked upload.
		if c.operation != opencdc.OperationDelete && w.matchesTracked(c) {
			pl.p.tracked = append(pl.p.tracked, c)
			for _, u := range w.r {
				if w.entryMatches(u, c) {
					id := idOf(u)
					pl.matched[id] = struct{}{}
					if _, listed := w.listed[id]; listed {
						// The upload is still in progress, so this version is not
						// its object: it should have been in the baseline. Report
						// it; the delivery is a duplicate at worst.
						w.sampled.Error().Str("code", "s3.source.tracked_match_while_upload_listed").
							Str("key", c.key).Str("version_id", c.versionID).
							Msg("a version at or below S matched a tracked multipart upload that is still in progress; the key's final state should be checked")
					}
				}
			}
		}
		return
	}
	if _, ok := w.inE[position.VersionHash(c.key, c.versionID, c.etag, c.modified)]; ok {
		return
	}
	if !c.modified.After(pl.p.cut) {
		if pl.evicted != nil && !c.less(*pl.evicted) {
			pl.p.deferred = true
			return
		}
		heap.Push(pl.settled, c)
		pl.settledBytes += c.bytes()
		for pl.settledBytes > w.cfg.MaxSettledBytesPerPoll && pl.settled.Len() > 1 {
			d := heap.Pop(pl.settled).(candidate)
			pl.settledBytes -= d.bytes()
			if pl.evicted == nil || d.less(*pl.evicted) {
				pl.evicted = &d
			}
			pl.p.deferred = true
		}
		return
	}
	if pl.room <= 0 {
		pl.p.waiting++
		return
	}
	heap.Push(pl.early, c)
	for pl.early.Len() > pl.room {
		heap.Pop(pl.early)
		pl.p.waiting++
	}
}

func sortedAsc(h *maxHeap) []candidate {
	out := make([]candidate, len(*h))
	copy(out, *h)
	sort.Slice(out, func(i, j int) bool { return out[i].less(out[j]) })
	return out
}

// versionCollector turns the listing into one candidate per key: the latest
// version or delete marker, classified as update when older versions exist.
// Versions of a key are contiguous in the listing, also across pages.
type versionCollector struct {
	consider func(candidate)
	curKey   string
	curOlder bool
	pending  *candidate
}

func (c *versionCollector) flush() {
	if c.pending != nil {
		c.consider(*c.pending)
		c.pending = nil
	}
}

func (c *versionCollector) version(v *types.ObjectVersion) {
	if aws.ToString(v.Key) != c.curKey {
		c.flush()
		c.curKey, c.curOlder = aws.ToString(v.Key), false
	}
	if !aws.ToBool(v.IsLatest) {
		c.curOlder = true
		if c.pending != nil {
			c.pending.operation = opencdc.OperationUpdate
		}
		return
	}
	if v.LastModified == nil {
		return
	}
	op := opencdc.OperationCreate
	if c.curOlder {
		op = opencdc.OperationUpdate
	}
	vid := aws.ToString(v.VersionId)
	if vid == "" {
		vid = nullVersionID
	}
	c.pending = &candidate{
		key: aws.ToString(v.Key), versionID: vid, etag: aws.ToString(v.ETag),
		modified: v.LastModified.UTC().Truncate(time.Millisecond), operation: op,
	}
}

func (c *versionCollector) deleteMarker(v *types.DeleteMarkerEntry) {
	if !aws.ToBool(v.IsLatest) || v.LastModified == nil {
		return
	}
	vid := aws.ToString(v.VersionId)
	if vid == "" {
		vid = nullVersionID
	}
	c.consider(candidate{
		key: aws.ToString(v.Key), versionID: vid,
		modified: v.LastModified.UTC().Truncate(time.Millisecond), operation: opencdc.OperationDelete,
	})
}

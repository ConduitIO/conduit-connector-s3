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

// CDCIterator scans the bucket periodically and detects changes made to it.
//
// This is listing-based change detection, which is best effort and bounded by
// lookbackPeriod. Lossless S3 change capture needs bucket event notifications;
// listing cannot give that guarantee.
//
// Selection rule. S3 lists in key order, not in modification order, reports
// LastModified with one-second resolution on AWS, and stamps a multipart upload
// with the time it was initiated, not the time it completed. "Newest
// LastModified seen so far" alone is therefore not a watermark: an object can
// become visible later with an older timestamp than one already delivered. The
// floor is the newest LastModified observed minus lookback, and every poll
// selects the versions at or after it that were not delivered yet
// (deduplicated by key, version id, ETag and LastModified).
//
// It uses the store's own timestamps. The host clock has one use, as an upper
// cap on the newest timestamp: floor = min(newest, host) - lookback. A store
// that stamps an object in the future (a clock that stepped forward and back,
// or a lying endpoint) would otherwise raise the floor above everything
// written afterwards. The cap can only lower the floor (more redelivery, never
// a higher floor), and the lookback is kept in full.
//
// An object is missed if it becomes visible more than lookback after its
// LastModified relative to the newest object observed (an upload that takes
// longer than the lookback, replica lag longer than the lookback), and in the
// other cases listed in the connector documentation.
type CDCIterator struct {
	bucket   string
	prefix   string
	client   *s3.Client
	buffer   chan opencdc.Record
	ticker   *time.Ticker
	lookback time.Duration
	log      *zerolog.Logger
	// maxSeen is the newest LastModified observed, starting from the position
	// (or snapshot) the iterator resumed from. Zero: nothing observed yet, so
	// everything is selected. It only moves forward and is only touched by the
	// startCDC goroutine.
	maxSeen time.Time
	// delivered holds the versions already handed over whose LastModified is
	// not older than the floor (see floor). It keeps the overlap between polls
	// (the lookback window) from re-emitting objects. It is rebuilt empty on
	// restart, so a restart redelivers the window. It is
	// bounded by deliveredBytes <= maxDeliveredBytes: evicting an entry that is
	// still inside the window would turn into duplicates, so overflow stops the
	// iterator instead.
	delivered      map[deliveredKey]struct{}
	deliveredBytes int64
	skewWarned     bool
	clamped        bool
	clampWarned    bool
	// minFloor is raised when the dedupe set had to shrink (see step): the
	// effective floor is max(computed, minFloor). It only moves forward.
	minFloor time.Time
	caches   chan []CacheEntry
	tomb     *tomb.Tomb
}

// deliveredKey identifies one stored version of an object. The ETag is part of
// it because in an unversioned bucket the version id is always "null" and
// LastModified has one-second resolution, so an overwrite within the same
// second differs from the earlier write only by its ETag.
type deliveredKey struct {
	key       string
	versionID string
	etag      string
	modified  time.Time
}

// bytes is an estimate of the memory one tracked version takes: its strings
// plus the map and struct overhead.
func (k deliveredKey) bytes() int64 {
	return int64(len(k.key)+len(k.versionID)+len(k.etag)) + entryOverheadBytes
}

// entryOverheadBytes is the per-entry overhead of the estimate. Measured cost
// was 2-7% above an estimate with 200, so the constant is higher.
const entryOverheadBytes = 260

func (e CacheEntry) bytes() int64 { return e.deliveredKey().bytes() }

type CacheEntry struct {
	key          string
	versionID    string
	etag         string
	operation    opencdc.Operation
	lastModified time.Time
	// posTime is the timestamp that goes into the record's position:
	// min(lastModified, host clock at the poll that observed it). A
	// future-stamped object must not leave a position that lies in the future,
	// or a restart from it would put the floor above objects delivered after it.
	posTime time.Time
}

func (e CacheEntry) deliveredKey() deliveredKey {
	return deliveredKey{e.key, e.versionID, e.etag, e.lastModified}
}

// maxDeliveredBytes caps the memory of the dedupe set (about 256 MiB). A
// variable so tests can lower it.
var maxDeliveredBytes int64 = 256 << 20

// ErrDedupeWindowOverflow is the stable error code of an iterator that stopped
// because the object versions that share one second of LastModified need more
// memory than maxDeliveredBytes to deduplicate. (A window that is merely too
// long is shortened instead, with a WARN s3.source.dedupe_window_shrunk.)
var ErrDedupeWindowOverflow = errors.New("s3.source.dedupe_window_overflow")

// DedupeWindowOverflowError carries the numbers. ClockSkew is how far the
// newest object timestamp is ahead of this host's clock, when that is more than
// the lookback: the window is then as long as the skew, and the fix is the
// clock, not the lookback.
type DedupeWindowOverflowError struct {
	Bytes     int64
	ClockSkew time.Duration
}

func (e *DedupeWindowOverflowError) Error() string {
	if e.ClockSkew > 0 {
		return fmt.Sprintf("%s: the object versions inside the CDC window need about %d MiB to track, more than the limit of %d MiB, "+
			"because the newest object timestamp is %s ahead of this host's clock, which stretches the window by that much; "+
			"fix the clock of the host running Conduit (or the store's clock), lowering lookbackPeriod will not help",
			ErrDedupeWindowOverflow.Error(), e.Bytes>>20, maxDeliveredBytes>>20, e.ClockSkew.Round(time.Second))
	}
	return fmt.Sprintf("%s: the object versions that share one second of LastModified need about %d MiB to track, more than the limit of %d MiB; "+
		"the window can be shortened but not below one second, and lookbackPeriod cannot help (config path: lookbackPeriod); "+
		"split the writes over time or use a prefix that holds fewer objects",
		ErrDedupeWindowOverflow.Error(), e.Bytes>>20, maxDeliveredBytes>>20)
}

func (e *DedupeWindowOverflowError) Is(target error) bool { return target == ErrDedupeWindowOverflow }

// NewCDCIterator returns a CDCIterator and starts the process of listening to
// changes every pollingPeriod. It selects objects modified at or after
// from - lookback (everything, if from is the zero time).
func NewCDCIterator(
	bucket, prefix string,
	pollingPeriod, lookback time.Duration,
	client *s3.Client,
	from time.Time,
	log *zerolog.Logger,
) (*CDCIterator, error) {
	cdc := newCDCIterator(bucket, prefix, lookback, client, from, log)
	cdc.start(pollingPeriod)
	return cdc, nil
}

// start begins polling every pollingPeriod. Nothing may touch the iterator's
// state from outside after it was called.
func (w *CDCIterator) start(pollingPeriod time.Duration) {
	w.ticker = time.NewTicker(pollingPeriod)
	w.tomb.Go(w.startCDC)
	w.tomb.Go(w.flush)
}

// seedDelivered marks object versions as already delivered, before start. The
// snapshot-to-CDC handoff uses it to hand over what the snapshot delivered
// inside the CDC window (see SnapshotIterator.delivered, #1040). Entries below
// the floor are dropped by the first poll like any other. A seed that does not
// fit the dedupe budget is not used beyond the budget: those objects are
// delivered again, which is at-least-once, never a skip.
func (w *CDCIterator) seedDelivered(seed map[deliveredKey]struct{}) {
	for k := range seed {
		if _, ok := w.delivered[k]; ok {
			continue
		}
		if w.deliveredBytes+k.bytes() > maxDeliveredBytes/2 {
			return
		}
		w.delivered[k] = struct{}{}
		w.deliveredBytes += k.bytes()
	}
}

// newCDCIterator builds the iterator without starting it (tests drive step
// directly, which makes them independent of timing).
func newCDCIterator(
	bucket, prefix string,
	lookback time.Duration,
	client *s3.Client,
	from time.Time,
	log *zerolog.Logger,
) *CDCIterator {
	if log == nil {
		nop := zerolog.Nop()
		log = &nop
	}
	return &CDCIterator{
		bucket:    bucket,
		prefix:    prefix,
		client:    client,
		buffer:    make(chan opencdc.Record, 1),
		caches:    make(chan []CacheEntry),
		tomb:      &tomb.Tomb{},
		lookback:  lookback,
		log:       log,
		maxSeen:   from,
		delivered: make(map[deliveredKey]struct{}),
	}
}

// HasNext returns a boolean that indicates whether the iterator has any objects in the buffer or not.
func (w *CDCIterator) HasNext(_ context.Context) bool {
	return len(w.buffer) > 0 || !w.tomb.Alive() // if tomb is dead we return true so caller will fetch error with Next
}

// Next returns the next record from the buffer.
func (w *CDCIterator) Next(ctx context.Context) (opencdc.Record, error) {
	select {
	case r, ok := <-w.buffer:
		if !ok {
			// The buffer is closed when the iterator stops. Receiving from it
			// then yields a zero record with no error, which would be emitted
			// as a record; report why the iterator stopped instead.
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
	// stop the two goRoutines
	if w.ticker != nil {
		w.ticker.Stop()
	}
	w.tomb.Kill(errors.New("cdc iterator is stopped"))
}

// startCDC scans the S3 bucket every polling period for changes
// made at or after the floor.
func (w *CDCIterator) startCDC() error {
	defer close(w.caches)

	handover := func(cache []CacheEntry) error {
		select {
		case w.caches <- cache:
			return nil
		case <-w.tomb.Dying():
			return w.tomb.Err()
		}
	}
	for {
		select {
		case <-w.tomb.Dying():
			return w.tomb.Err()
		case <-w.ticker.C: // detect changes every polling period
			if err := w.step(w.tomb.Context(nil), time.Now(), handover); err != nil { //nolint:staticcheck // SA1012 tomb expects nil
				return err
			}
		}
	}
}

// maxCandidateBytes bounds the memory of the candidates of one poll (versions
// to hand over). Peak memory is about the dedupe budget plus the candidates plus
// the per-poll "seen" map, i.e. roughly 1.5 times maxDeliveredBytes. A variable
// so tests can lower it.
var maxCandidateBytes = maxDeliveredBytes / 4

// step runs one poll: list, decide, hand over, then (and only then) move state.
// A catch-up is delivered in successive polls, oldest first, so memory stays
// bounded and the order by LastModified that restart positions rely on holds.
func (w *CDCIterator) step(ctx context.Context, hostNow time.Time, handover func([]CacheEntry) error) error {
	base := w.baseMax(hostNow)
	cache, deferredFrom, err := w.poll(ctx, laterOf(w.floorFor(base, hostNow), w.effMinFloor(hostNow)))
	if err != nil {
		return err
	}
	// A poll succeeded: from now on the clamped position is the state. (Nothing
	// changes if the first poll fails.)
	w.maxSeen = base
	w.clamped = true
	if len(cache) == 0 {
		return nil
	}
	sort.Slice(cache, func(i, j int) bool {
		return cache[i].lastModified.Before(cache[j].lastModified)
	})

	newMax, rawMax := w.observe(cache, hostNow)
	if rawMax.Sub(hostNow) > w.lookback {
		w.warnHostClockBehind(rawMax, hostNow)
	}
	newFloor := laterOf(w.floorFor(newMax, hostNow), w.effMinFloor(hostNow))

	keep, seen := w.trackedAfter(cache, newFloor)
	newMinFloor := w.effMinFloor(hostNow)
	var untrack untrackPlan
	if keepBytes(keep) > maxDeliveredBytes {
		minFloor, plan, err := w.shrink(keep, shrinkContext{
			newMax: newMax, rawMax: rawMax, hostNow: hostNow, newFloor: newFloor, deferredFrom: deferredFrom,
		})
		if err != nil {
			return err
		}
		newMinFloor, untrack = minFloor, plan
		newFloor = laterOf(newFloor, newMinFloor)
	}

	if err := handover(cache); err != nil {
		return err
	}

	// Only after the handover does any state move.
	w.maxSeen = newMax
	w.minFloor = newMinFloor
	for k := range seen {
		if _, already := w.delivered[k]; !already && !k.modified.Before(newFloor) && !untrack.has(k) {
			w.delivered[k] = struct{}{}
			w.deliveredBytes += k.bytes()
		}
	}
	for k := range w.delivered {
		if k.modified.Before(newFloor) || untrack.has(k) {
			delete(w.delivered, k)
			w.deliveredBytes -= k.bytes()
		}
	}
	return nil
}

// baseMax is the newest-seen time a poll starts from. A resume position that
// lies in the future is clamped to the host clock. This build never writes one
// (position timestamps are capped at the host clock when observed); only an
// older release could have persisted it. The clamp is applied to the value
// used, and becomes state only once a poll succeeded.
func (w *CDCIterator) baseMax(hostNow time.Time) time.Time {
	if w.clamped || !w.maxSeen.After(hostNow) {
		return w.maxSeen
	}
	if !w.clampWarned {
		w.clampWarned = true
		w.log.Warn().Time("position_timestamp", w.maxSeen).Time("host", hostNow).
			Msg("the resume position carries a timestamp in the future (written by an older version of the connector, or the host clock moved back); using this host's clock instead")
	}
	return hostNow
}

// observe applies the observation-time cap: the newest timestamp, and the
// timestamp that goes into each record's position, never exceed the host clock
// at this poll. A future-stamped object can then only lower the floor. rawMax
// is the uncapped newest, for diagnostics.
func (w *CDCIterator) observe(cache []CacheEntry, hostNow time.Time) (newMax, rawMax time.Time) {
	newMax, rawMax = w.maxSeen, w.maxSeen // maxSeen is already clamped by step
	for i := range cache {
		if cache[i].lastModified.After(rawMax) {
			rawMax = cache[i].lastModified
		}
		pt := cache[i].lastModified
		if pt.After(hostNow) {
			pt = hostNow
		}
		cache[i].posTime = pt
		if pt.After(newMax) {
			newMax = pt
		}
	}
	return newMax, rawMax
}

type tracked struct {
	k     deliveredKey
	isNew bool
}

func keepBytes(keep []tracked) int64 {
	var n int64
	for _, t := range keep {
		n += t.k.bytes()
	}
	return n
}

// trackedAfter lists what would be tracked after this poll: everything below
// the new floor needs no tracking (it is never selected again), so a catch-up
// over old objects is never capped by the memory bound. seen are the distinct
// versions of this batch.
func (w *CDCIterator) trackedAfter(cache []CacheEntry, newFloor time.Time) (keep []tracked, seen map[deliveredKey]struct{}) {
	seen = make(map[deliveredKey]struct{}, len(cache))
	for k := range w.delivered {
		if !k.modified.Before(newFloor) {
			keep = append(keep, tracked{k, false})
		}
	}
	for _, e := range cache {
		k := e.deliveredKey()
		if _, dup := seen[k]; dup {
			continue
		}
		seen[k] = struct{}{}
		if _, already := w.delivered[k]; !already && !k.modified.Before(newFloor) {
			keep = append(keep, tracked{k, true})
		}
	}
	return keep, seen
}

type shrinkContext struct {
	newMax, rawMax, hostNow, newFloor time.Time
	// deferredFrom is the cutoff of the candidate budget: candidates newer than
	// it were left for later polls. Zero if nothing was deferred.
	deferredFrom time.Time
}

// untrackPlan says which tracked entries are forgotten: whole seconds up to
// upTo (inclusive), plus individually dropped entries.
type untrackPlan struct {
	upTo int64
	drop map[deliveredKey]struct{}
}

func (u untrackPlan) has(k deliveredKey) bool {
	if k.modified.Unix() <= u.upTo && u.upTo >= 0 {
		return true
	}
	_, ok := u.drop[k]
	return ok
}

// shrink handles a window that does not fit the memory bound. Instead of
// stopping, it forgets tracked entries and returns a floor above the ones
// forgotten by whole seconds, so they are not delivered again. The candidates
// of this poll are still handed over (they are not lost, only no longer
// tracked). The cost is a lower tolerance for objects that become visible
// late: the effective lookback is shorter until the window moves on.
//
// Normally the oldest seconds go first. When the candidate budget deferred
// something (deferredFrom), the second of the cutoff and everything after it
// must NOT be forgotten by second: the candidates of that second that were
// handed over now have to stay tracked, or the next poll picks the same subset
// again (a real S3 lists in key order) and the deferred rest never gets its
// turn. Budget is then freed by dropping the newest tracked entries above the
// cutoff second one by one; they come back later as duplicates, never a skip.
// If the cutoff second and what is older than the entries dropped still do not
// fit, it stops.
func (w *CDCIterator) shrink(keep []tracked, sc shrinkContext) (minFloor time.Time, plan untrackPlan, err error) {
	deferred := !sc.deferredFrom.IsZero()
	cutSec := sc.deferredFrom.Unix()

	perSecond := map[int64]int64{}
	var total int64
	for _, t := range keep {
		perSecond[t.k.modified.Unix()] += t.k.bytes()
		total += t.k.bytes()
	}
	secs := make([]int64, 0, len(perSecond))
	for sec := range perSecond {
		secs = append(secs, sec)
	}
	sort.Slice(secs, func(i, j int) bool { return secs[i] < secs[j] })

	// The minimum floor never exceeds the host second: a future-stamped entry
	// can raise the floor only up to the present, never past it, or objects
	// written from now on (stamped at the present) would be hidden. So only
	// seconds strictly below the host second may be forgotten by second.
	hostSec := sc.hostNow.Unix()
	blockedByHost := false
	plan.upTo = -1
	for i := 0; i < len(secs) && total > maxDeliveredBytes; i++ {
		if (deferred && secs[i] >= cutSec) || (!deferred && i == len(secs)-1) {
			break
		}
		if secs[i] >= hostSec {
			blockedByHost = true
			break
		}
		total -= perSecond[secs[i]]
		plan.upTo = secs[i]
	}
	if total > maxDeliveredBytes && deferred {
		plan.drop, total = dropNewestAbove(keep, cutSec, total)
	}
	if total > maxDeliveredBytes {
		if blockedByHost {
			// What is left does not fit because entries stamped at or after the
			// host second (a store clock that is ahead, or bogus timestamps) take
			// the budget, and forgetting them by second would hide new objects.
			return time.Time{}, untrackPlan{}, w.stopFloorInTheFuture(sc.hostNow, total, sc)
		}
		return time.Time{}, untrackPlan{}, w.stopOverflow(total, sc, "the objects of one second alone need more tracking memory than the limit")
	}

	minFloor = laterOf(time.Unix(plan.upTo+1, 0), w.effMinFloor(sc.hostNow))
	if deferred {
		if capped := sc.deferredFrom.Truncate(time.Second); capped.Before(minFloor) {
			minFloor = capped
		}
	}
	minFloor = laterOf(minFloor, w.effMinFloor(sc.hostNow))

	evicted := len(plan.drop)
	for _, t := range keep {
		if plan.upTo >= 0 && t.k.modified.Unix() <= plan.upTo {
			evicted++
		}
	}
	w.log.Warn().Str("code", "s3.source.dedupe_window_shrunk").
		Dur("configured_lookback", w.lookback).
		Dur("effective_lookback", sc.newMax.Sub(laterOf(sc.newFloor, minFloor)).Round(time.Second)).
		Int("evicted_entries", evicted).
		Msg("more object versions are inside the CDC lookback window than the dedupe memory limit allows; the window was shortened. " +
			"Objects that become visible late by more than the effective lookback can be missed until the window moves on; " +
			"lower lookbackPeriod to avoid this")
	return minFloor, plan, nil
}

// dropNewestAbove picks the newest tracked entries with a LastModified second
// above cutSec to forget one by one until total fits the budget again.
func dropNewestAbove(keep []tracked, cutSec, total int64) (map[deliveredKey]struct{}, int64) {
	var above []tracked
	for _, t := range keep {
		if t.k.modified.Unix() > cutSec {
			above = append(above, t)
		}
	}
	sort.Slice(above, func(i, j int) bool { return above[i].k.modified.After(above[j].k.modified) })
	drop := map[deliveredKey]struct{}{}
	for _, t := range above {
		if total <= maxDeliveredBytes {
			break
		}
		drop[t.k] = struct{}{}
		total -= t.k.bytes()
	}
	return drop, total
}

// FloorInTheFutureError is the error of an iterator that stopped because
// object versions stamped at or after the host clock's present second take the
// tracking budget, so the window could only be shortened by raising its start
// past the present, which would hide new objects.
type FloorInTheFutureError struct {
	Host     time.Time
	Newest   time.Time
	Needed   int64
	Lookback time.Duration
}

func (e *FloorInTheFutureError) Error() string {
	return fmt.Sprintf("%s: the object versions stamped at or after this host's present second (%s) need about %d KiB to track, more than the limit of %d KiB, "+
		"and the window cannot be shortened past the present without hiding new objects; the newest object timestamp is %s ahead of this host's clock. "+
		"Object timestamps from the future (a store clock that is ahead, or bogus timestamps) are the cause: "+
		"fix the clock of the host running Conduit or of the store, or remove the future-stamped objects",
		ErrDedupeWindowOverflow.Error(), e.Host.UTC().Format(time.RFC3339), e.Needed>>10, maxDeliveredBytes>>10, e.Newest.Sub(e.Host).Round(time.Second))
}

func (e *FloorInTheFutureError) Is(target error) bool { return target == ErrDedupeWindowOverflow }

func (w *CDCIterator) stopFloorInTheFuture(host time.Time, needed int64, sc shrinkContext) error {
	if sc.rawMax.Sub(host) > w.lookback {
		w.warnHostClockBehind(sc.rawMax, host)
	}
	err := &FloorInTheFutureError{Host: host, Newest: sc.rawMax, Needed: needed, Lookback: w.lookback}
	w.log.Warn().Str("code", ErrDedupeWindowOverflow.Error()).
		Msg("the shortened window would have to start in the future; stopping the CDC iterator. " + err.Error())
	return err
}

// effMinFloor is the minimum floor in effect: never above the host second, even
// if the host clock moved back since it was set.
func (w *CDCIterator) effMinFloor(hostNow time.Time) time.Time {
	if limit := hostNow.Truncate(time.Second); w.minFloor.After(limit) {
		return limit
	}
	return w.minFloor
}

// stopOverflow warns, then returns the error that stops the iterator.
func (w *CDCIterator) stopOverflow(needed int64, sc shrinkContext, why string) error {
	skew := time.Duration(0)
	if d := sc.rawMax.Sub(sc.hostNow); d > w.lookback {
		skew = d
		w.warnHostClockBehind(sc.rawMax, sc.hostNow)
	}
	err := &DedupeWindowOverflowError{Bytes: needed, ClockSkew: skew}
	w.log.Warn().Str("code", ErrDedupeWindowOverflow.Error()).Int64("needed_bytes", needed).
		Int64("limit_bytes", maxDeliveredBytes).
		Msg(why + "; stopping the CDC iterator. " + err.Error())
	return err
}

func laterOf(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}

// floorFor is the lower bound of a poll: min(newest observed LastModified,
// host) minus lookback, floored to the second (AWS timestamps may be
// second-truncated).
func (w *CDCIterator) floorFor(newest, hostNow time.Time) time.Time {
	if newest.IsZero() {
		return time.Time{}
	}
	// The host clock only caps the newest timestamp (see CDCIterator).
	if newest.After(hostNow) {
		if newest.Sub(hostNow) > w.lookback {
			w.warnHostClockBehind(newest, hostNow)
		}
		newest = hostNow
	}
	return newest.Add(-w.lookback).Truncate(time.Second)
}

func (w *CDCIterator) warnHostClockBehind(newest, hostNow time.Time) {
	if w.skewWarned {
		return
	}
	w.skewWarned = true
	w.log.Warn().Str("code", "s3.source.host_clock_behind_store").
		Dur("skew", newest.Sub(hostNow)).Dur("lookbackPeriod", w.lookback).
		Time("newest_last_modified", newest).Time("host", hostNow).
		Msg("the newest object timestamp is ahead of this host's clock by more than lookbackPeriod: either the host clock is behind the store's or a timestamp is bogus. The CDC window is stretched by that difference, which causes redelivery and can exhaust the dedupe memory; fix the clock")
}

// isNew reports whether a version must be emitted: it was modified at or after
// the floor and has not been delivered yet.
func (w *CDCIterator) isNew(floor time.Time, key, versionID, etag string, modified time.Time) bool {
	if modified.Before(floor) {
		return false
	}
	_, done := w.delivered[deliveredKey{key, versionID, etag, modified}]
	return !done
}

// flush: go routine that will get the objects from the bucket and flush the detected changes into the buffer.
func (w *CDCIterator) flush() error {
	defer close(w.buffer)

	for {
		select {
		case <-w.tomb.Dying():
			return w.tomb.Err()
		case cache := <-w.caches:
			for _, entry := range cache {
				output, err := w.buildRecord(entry)
				if errors.Is(err, errObjectGone) {
					// Deleted after it was listed: there is nothing to
					// deliver. A versioned bucket reports the delete marker
					// in a later poll. (A failing GetObject for another reason,
					// for example an archived Glacier object, still stops the
					// iterator: see the known limits.)
					w.log.Debug().Str("key", entry.key).Msg("object was deleted after it was listed, skipping it")
					continue
				}
				if err != nil {
					return fmt.Errorf("could not build record for %q: %w", entry.key, err)
				}

				select {
				case w.buffer <- output:
					// worked fine
				case <-w.tomb.Dying():
					return w.tomb.Err()
				}
			}
		}
	}
}

// poll lists every version in the bucket (all pages) and returns the changes
// that must be emitted. Whether a version is the latest one, and whether its key
// has older versions (an update), is decided across pages: a key's versions can
// straddle a page boundary.
func (w *CDCIterator) poll(ctx context.Context, floor time.Time) (cache []CacheEntry, deferredFrom time.Time, err error) {
	c := &collector{w: w, floor: floor}
	input := &s3.ListObjectVersionsInput{ // default is 1000 keys max
		Bucket: aws.String(w.bucket),
		Prefix: aws.String(w.prefix),
	}

	for {
		objects, lerr := w.client.ListObjectVersions(ctx, input)
		if lerr != nil {
			return nil, time.Time{}, fmt.Errorf("couldn't get latest objects: %w", lerr)
		}

		// no flush between pages: the versions of a key can continue on the next one
		for i := range objects.Versions {
			c.version(&objects.Versions[i])
		}
		for i := range objects.DeleteMarkers {
			c.deleteMarker(&objects.DeleteMarkers[i])
		}

		if !aws.ToBool(objects.IsTruncated) {
			break
		}
		// Both markers: with only the key marker the rest of a key's versions
		// would be skipped when a page ends in the middle of them.
		if aws.ToString(objects.NextKeyMarker) == "" && aws.ToString(objects.NextVersionIdMarker) == "" {
			// A truncated listing that cannot be continued is a broken
			// response, not the end: ending here would hide every key after it,
			// and retrying from the start would loop forever.
			return nil, time.Time{}, errors.New("couldn't get latest objects: the listing is truncated but has no next marker")
		}
		input.KeyMarker = objects.NextKeyMarker
		input.VersionIdMarker = objects.NextVersionIdMarker
	}

	c.flush()
	return []CacheEntry(c.cands), c.cutoff, nil
}

// collector gathers the candidates of one poll within the memory budget.
type collector struct {
	w     *CDCIterator
	floor time.Time

	cands candidateHeap
	bytes int64
	// cutoff: once a candidate was dropped for the budget, nothing newer than
	// the dropped one may be kept. Dropping the newest until things fit is not
	// enough on its own: with keys of different lengths a newer, shorter
	// candidate seen later still fits in the slack, and the new floor (newest
	// delivered - lookback) would then pass the dropped, older object. A
	// candidate with exactly the cutoff timestamp is still accepted, but may be
	// dropped again; what matters is that every dropped one stays selectable
	// (floor and minimum floor never rise above the cutoff second) and that the
	// ones handed over stay tracked, so the next poll moves on to the rest.
	cutoff time.Time

	// Versions of one key are contiguous in the listing, so update
	// classification needs one pending entry, not a map of every key with older
	// versions.
	curKey   string
	curOlder bool
	pending  *CacheEntry
}

func (c *collector) add(e CacheEntry) {
	if !c.cutoff.IsZero() && e.lastModified.After(c.cutoff) {
		return // newer than something already dropped: left for a later poll
	}
	heap.Push(&c.cands, e)
	c.bytes += e.bytes()
	for c.bytes > maxCandidateBytes && c.cands.Len() > 1 {
		dropped := heap.Pop(&c.cands).(CacheEntry)
		c.bytes -= dropped.bytes()
		if c.cutoff.IsZero() || dropped.lastModified.Before(c.cutoff) {
			c.cutoff = dropped.lastModified
		}
	}
}

func (c *collector) flush() {
	if c.pending != nil {
		c.add(*c.pending)
		c.pending = nil
	}
}

func (c *collector) version(v *types.ObjectVersion) {
	if *v.Key != c.curKey {
		c.flush()
		c.curKey, c.curOlder = *v.Key, false
	}
	if !aws.ToBool(v.IsLatest) {
		// this is a version that is not the latest, this means this object
		// was updated
		c.curOlder = true
		if c.pending != nil {
			c.pending.operation = opencdc.OperationUpdate
		}
		return
	}
	if !c.w.isNew(c.floor, *v.Key, aws.ToString(v.VersionId), aws.ToString(v.ETag), *v.LastModified) {
		return
	}
	op := opencdc.OperationCreate
	if c.curOlder {
		op = opencdc.OperationUpdate
	}
	c.pending = &CacheEntry{
		key: *v.Key, versionID: aws.ToString(v.VersionId), etag: aws.ToString(v.ETag),
		lastModified: *v.LastModified, operation: op,
	}
}

func (c *collector) deleteMarker(v *types.DeleteMarkerEntry) {
	if aws.ToBool(v.IsLatest) && c.w.isNew(c.floor, *v.Key, aws.ToString(v.VersionId), "", *v.LastModified) {
		c.add(CacheEntry{
			key: *v.Key, versionID: aws.ToString(v.VersionId),
			lastModified: *v.LastModified, operation: opencdc.OperationDelete,
		})
	}
}

// candidateHeap is a max-heap by LastModified (the newest on top), so the
// newest candidate is the one dropped when the budget is exceeded.
type candidateHeap []CacheEntry

func (h candidateHeap) Len() int           { return len(h) }
func (h candidateHeap) Less(i, j int) bool { return h[i].lastModified.After(h[j].lastModified) }
func (h candidateHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *candidateHeap) Push(x any)        { *h = append(*h, x.(CacheEntry)) }
func (h *candidateHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

func (w *CDCIterator) fetchS3Object(entry CacheEntry) (*s3.GetObjectOutput, []byte, error) {
	object, err := w.client.GetObject(w.tomb.Context(nil), //nolint:staticcheck // SA1012 tomb expects nil
		&s3.GetObjectInput{
			Bucket: aws.String(w.bucket),
			Key:    aws.String(entry.key),
		})
	if err != nil {
		return nil, nil, fmt.Errorf("could not get S3 object: %w", err)
	}

	rawBody, err := io.ReadAll(object.Body)
	if err != nil {
		return nil, nil, fmt.Errorf("could not read S3 object body: %w", err)
	}

	return object, rawBody, nil
}

var errObjectGone = errors.New("object was deleted after it was listed")

// createRecord creates the record for the object fetched from S3 (for updates and inserts)
func (w *CDCIterator) buildRecord(entry CacheEntry) (opencdc.Record, error) {
	var object *s3.GetObjectOutput
	var payload []byte

	switch entry.operation {
	case opencdc.OperationCreate, opencdc.OperationUpdate:
		var err error
		object, payload, err = w.fetchS3Object(entry)
		var noSuchKey *types.NoSuchKey
		if errors.As(err, &noSuchKey) {
			return opencdc.Record{}, errObjectGone
		}
		if err != nil {
			return opencdc.Record{}, fmt.Errorf("could not fetch S3 object for %v: %w", entry.key, err)
		}
	}

	pt := entry.posTime
	if pt.IsZero() {
		pt = entry.lastModified
	}
	p := position.Position{
		Key:       entry.key,
		Timestamp: pt,
		Type:      position.TypeCDC,
	}

	m := opencdc.Metadata{}
	if object != nil {
		m[MetadataS3HeaderPrefix+MetadataContentType] = *object.ContentType
		for key, val := range object.Metadata {
			m[key] = val
		}
	}

	switch entry.operation {
	case opencdc.OperationCreate:
		return sdk.Util.Source.NewRecordCreate(
			p.ToRecordPosition(), m,
			opencdc.RawData(entry.key),
			opencdc.RawData(payload),
		), nil
	case opencdc.OperationUpdate:

		return sdk.Util.Source.NewRecordUpdate(
			p.ToRecordPosition(), m,
			opencdc.RawData(entry.key),
			nil, // TODO we could actually attach last version
			opencdc.RawData(payload),
		), nil
	case opencdc.OperationDelete:
		return sdk.Util.Source.NewRecordDelete(
			p.ToRecordPosition(), m,
			opencdc.RawData(entry.key),
			nil,
		), nil
	}

	return opencdc.Record{}, fmt.Errorf("invalid operation %v", entry.operation)
}

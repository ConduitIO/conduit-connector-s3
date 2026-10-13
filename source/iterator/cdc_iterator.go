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
	"io"
	"sort"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-s3/source/position"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/rs/zerolog"
	"gopkg.in/tomb.v2"
)

// CDCIterator scans the bucket periodically and detects changes made to it.
//
// Selection rule (invariant 3, no skipped objects). S3 lists in key order, not
// in modification order, reports LastModified with one-second resolution on
// AWS, and stamps a multipart upload with the time it was initiated, not the
// time it completed. "Newest LastModified seen so far" alone is therefore not
// a watermark: an object can become visible later with an older timestamp than
// one already delivered. The watermark is the newest LastModified observed
// minus lookback, and every poll selects the versions at or after it that were
// not delivered yet (deduplicated by key, version id, ETag and LastModified).
//
// It uses only the store's own timestamps, so a host clock that is wrong in
// either direction cannot move it. The host clock is used for one thing, as an
// upper cap on the newest timestamp (cap = host + lookback): a store that
// stamps objects in the far future (a clock that stepped forward and back, or a
// lying endpoint) would otherwise raise the watermark above everything written
// afterwards. The cap can only lower the watermark, which means more
// redelivery, never a skip.
//
// An object is only missed if it becomes visible more than lookback after its
// LastModified relative to the newest object observed (an upload that takes
// longer than the lookback).
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
	// restart, so a restart redelivers the window; it never skips it. It is
	// bounded by deliveredBytes <= maxDeliveredBytes: evicting an entry that is
	// still inside the window would turn into duplicates, so overflow stops the
	// iterator instead.
	delivered      map[deliveredKey]struct{}
	deliveredBytes int64
	lastCapWarn    time.Time
	caches         chan []CacheEntry
	tomb           *tomb.Tomb
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
// plus the map and struct overhead (measured at about 200 bytes).
func (k deliveredKey) bytes() int64 {
	return int64(len(k.key)+len(k.versionID)+len(k.etag)) + 200
}

type CacheEntry struct {
	key          string
	versionID    string
	etag         string
	operation    opencdc.Operation
	lastModified time.Time
}

func (e CacheEntry) deliveredKey() deliveredKey {
	return deliveredKey{e.key, e.versionID, e.etag, e.lastModified}
}

// maxDeliveredBytes caps the memory of the dedupe set (about 256 MiB). A
// variable so tests can lower it.
var maxDeliveredBytes int64 = 256 << 20

// ErrDedupeWindowOverflow is the stable error code of an iterator that stopped
// because the object versions modified within lookbackPeriod of the newest one
// need more memory than maxDeliveredBytes to deduplicate. Lower lookbackPeriod.
var ErrDedupeWindowOverflow = errors.New("s3.source.dedupe_window_overflow")

// DedupeWindowOverflowError carries the numbers.
type DedupeWindowOverflowError struct{ Bytes int64 }

func (e *DedupeWindowOverflowError) Error() string {
	return fmt.Sprintf("%s: the object versions modified within the CDC lookback window need about %d MiB to track, more than the limit of %d MiB; "+
		"lower lookbackPeriod (config path: lookbackPeriod) so that fewer objects fall inside the window",
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
	if log == nil {
		nop := zerolog.Nop()
		log = &nop
	}
	cdc := CDCIterator{
		bucket:    bucket,
		prefix:    prefix,
		client:    client,
		buffer:    make(chan opencdc.Record, 1),
		caches:    make(chan []CacheEntry),
		ticker:    time.NewTicker(pollingPeriod),
		tomb:      &tomb.Tomb{},
		lookback:  lookback,
		log:       log,
		maxSeen:   from,
		delivered: make(map[deliveredKey]struct{}),
	}

	// start listening to changes
	cdc.tomb.Go(cdc.startCDC)
	cdc.tomb.Go(cdc.flush)

	return &cdc, nil
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
	w.ticker.Stop()
	w.tomb.Kill(errors.New("cdc iterator is stopped"))
}

// startCDC scans the S3 bucket every polling period for changes
// made at or after the floor.
func (w *CDCIterator) startCDC() error {
	defer close(w.caches)

	for {
		select {
		case <-w.tomb.Dying():
			return w.tomb.Err()
		case <-w.ticker.C: // detect changes every polling period
			floor := w.floor(time.Now())

			cache, err := w.poll(w.tomb.Context(nil), floor) //nolint:staticcheck // SA1012 tomb expects nil
			if err != nil {
				return err
			}
			if len(cache) == 0 {
				continue
			}
			sort.Slice(cache, func(i, j int) bool {
				return cache[i].lastModified.Before(cache[j].lastModified)
			})

			// Everything below the new floor needs no tracking (it is never
			// selected again), so only the entries that stay inside the window
			// count towards the memory bound. Overflow is decided BEFORE the
			// handover: a catch-up over old objects never trips it, a window
			// that is genuinely too large stops the iterator without having
			// delivered a partial batch it could not deduplicate.
			newMax := w.maxSeen
			for _, e := range cache {
				if e.lastModified.After(newMax) {
					newMax = e.lastModified
				}
			}
			newFloor := w.floorFor(newMax, time.Now())
			retained := w.deliveredBytes
			for k := range w.delivered {
				if k.modified.Before(newFloor) {
					retained -= k.bytes()
				}
			}
			for _, e := range cache {
				if !e.lastModified.Before(newFloor) {
					retained += e.deliveredKey().bytes()
				}
			}
			if retained > maxDeliveredBytes {
				return &DedupeWindowOverflowError{Bytes: retained}
			}

			select {
			case w.caches <- cache:
			case <-w.tomb.Dying():
				return w.tomb.Err()
			}

			// Only after the handover does any state move.
			w.maxSeen = newMax
			for _, e := range cache {
				if k := e.deliveredKey(); !k.modified.Before(newFloor) {
					w.delivered[k] = struct{}{}
					w.deliveredBytes += k.bytes()
				}
			}
			for k := range w.delivered {
				if k.modified.Before(newFloor) {
					delete(w.delivered, k)
					w.deliveredBytes -= k.bytes()
				}
			}
		}
	}
}

// floor is the lower bound of a poll: newest observed LastModified minus
// lookback, floored to the second (AWS timestamps may be second-truncated).
func (w *CDCIterator) floor(hostNow time.Time) time.Time {
	return w.floorFor(w.maxSeen, hostNow)
}

func (w *CDCIterator) floorFor(newest, hostNow time.Time) time.Time {
	if newest.IsZero() {
		return time.Time{}
	}
	// The host clock only caps the newest timestamp (see CDCIterator).
	if limit := hostNow.Add(w.lookback); newest.After(limit) {
		if hostNow.Sub(w.lastCapWarn) > time.Minute {
			w.lastCapWarn = hostNow
			w.log.Warn().Time("newest_last_modified", newest).Time("host", hostNow).
				Msg("object timestamps are ahead of this host's clock by more than lookbackPeriod; the CDC watermark follows this host's clock instead, which causes redelivery but cannot skip objects")
		}
		newest = limit
	}
	return newest.Add(-w.lookback).Truncate(time.Second)
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
func (w *CDCIterator) poll(ctx context.Context, floor time.Time) ([]CacheEntry, error) {
	var (
		cache   []CacheEntry
		updated = make(map[string]bool)
		input   = &s3.ListObjectVersionsInput{ // default is 1000 keys max
			Bucket: aws.String(w.bucket),
			Prefix: aws.String(w.prefix),
		}
	)

	for {
		objects, err := w.client.ListObjectVersions(ctx, input)
		if err != nil {
			return nil, fmt.Errorf("couldn't get latest objects: %w", err)
		}

		for _, v := range objects.Versions {
			if aws.ToBool(v.IsLatest) {
				if w.isNew(floor, *v.Key, aws.ToString(v.VersionId), aws.ToString(v.ETag), *v.LastModified) {
					cache = append(cache, CacheEntry{
						key: *v.Key, versionID: aws.ToString(v.VersionId), etag: aws.ToString(v.ETag),
						lastModified: *v.LastModified, operation: opencdc.OperationCreate,
					})
				}
			} else {
				// this is a version that is not the latest, this means this
				// object was updated
				updated[*v.Key] = true
			}
		}
		for _, v := range objects.DeleteMarkers {
			if aws.ToBool(v.IsLatest) && w.isNew(floor, *v.Key, aws.ToString(v.VersionId), "", *v.LastModified) {
				cache = append(cache, CacheEntry{
					key: *v.Key, versionID: aws.ToString(v.VersionId),
					lastModified: *v.LastModified, operation: opencdc.OperationDelete,
				})
			}
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
			return nil, errors.New("couldn't get latest objects: the listing is truncated but has no next marker")
		}
		input.KeyMarker = objects.NextKeyMarker
		input.VersionIdMarker = objects.NextVersionIdMarker
	}

	for i, e := range cache {
		if e.operation == opencdc.OperationCreate && updated[e.key] {
			cache[i].operation = opencdc.OperationUpdate
		}
	}
	return cache, nil
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

// createRecord creates the record for the object fetched from S3 (for updates and inserts)
func (w *CDCIterator) buildRecord(entry CacheEntry) (opencdc.Record, error) {
	var object *s3.GetObjectOutput
	var payload []byte

	switch entry.operation {
	case opencdc.OperationCreate, opencdc.OperationUpdate:
		var err error
		object, payload, err = w.fetchS3Object(entry)
		if err != nil {
			return opencdc.Record{}, fmt.Errorf("could not fetch S3 object for %v: %w", entry.key, err)
		}
	}

	p := position.Position{
		Key:       entry.key,
		Timestamp: entry.lastModified,
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

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
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-s3/source/position"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/rs/zerolog"
)

// SnapshotIterator to iterate through S3 objects in a specific bucket.
type SnapshotIterator struct {
	bucket          string
	client          *s3.Client
	paginator       *s3.ListObjectsV2Paginator
	page            *s3.ListObjectsV2Output
	index           int
	maxLastModified time.Time
	// startedAt is when the snapshot began listing. Objects written after
	// this point may be missing from the snapshot, so CDC must cover them.
	startedAt time.Time
	// startedAtFromStore is set once startedAt was replaced by the store's
	// clock (Date header of the first page), the one LastModified is on.
	startedAtFromStore bool
	warnedNoDate       bool
	trustHost          bool
	log                *zerolog.Logger
	// listErr is a failed page listing. It must reach the caller: treating it
	// as the end of the snapshot would hand over to CDC and silently skip every
	// object not yet listed.
	listErr error
	// lastToken is the continuation token the last page was requested with.
	lastToken string
}

// NewSnapshotIterator takes the s3 bucket, the client, and the position.
// it returns a snapshotIterator starting from the position provided.
func NewSnapshotIterator(bucket, prefix string, client *s3.Client, p position.Position, log *zerolog.Logger, trustHostClock bool) (*SnapshotIterator, error) {
	if log == nil {
		nop := zerolog.Nop()
		log = &nop
	}
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	}

	return &SnapshotIterator{
		bucket:          bucket,
		client:          client,
		paginator:       s3.NewListObjectsV2Paginator(client, input),
		maxLastModified: p.Timestamp,
		startedAt:       time.Now(),
		log:             log,
		trustHost:       trustHostClock,
	}, nil
}

// shouldRefreshPage returns a boolean indicating whether the SnapshotIterator is empty or not.
func (w *SnapshotIterator) shouldRefreshPage() bool {
	return w.page == nil || len(w.page.Contents) == w.index
}

// refreshPage retrieves the next page from s3. It returns sdk.ErrBackoffRetry
// only when the listing is really exhausted; every other outcome that is not a
// page is an error. Nothing about the iterator's position changes unless a page
// was fetched successfully, so a failed call can be retried without skipping.
func (w *SnapshotIterator) refreshPage(ctx context.Context) error {
	for w.paginator.HasMorePages() {
		var date string
		nextPage, err := w.paginator.NextPage(ctx, captureDate(&date))
		if err != nil {
			// The paginator did not advance, so the same page is fetched again.
			return fmt.Errorf("could not fetch next page: %w", err)
		}
		// A page that claims to be truncated but offers no way to get the next
		// one (no or repeated continuation token) is a broken listing, not the
		// end of the bucket: ending here would skip everything after it.
		next := aws.ToString(nextPage.NextContinuationToken)
		if aws.ToBool(nextPage.IsTruncated) && (next == "" || next == w.lastToken) {
			return fmt.Errorf("could not fetch next page: the listing is truncated but has no new continuation token")
		}
		w.lastToken = next
		if !w.startedAtFromStore {
			switch d, st := classifyStoreDate(w.startedAt, date); st {
			case headerOK:
				// min(host start, store time): never later than the real start.
				w.startedAtFromStore = true
				if d.Before(w.startedAt) {
					w.startedAt = d
				}
			case headerOutOfRange:
				// Either clock is badly wrong; a handoff point derived from
				// either would be a guess that can skip objects.
				return &StoreClockOutOfRangeError{Host: w.startedAt, Store: d, Polls: 1}
			default:
				// No Date header. The handoff point would be a guess from the
				// host clock, which loses objects if it is ahead of the
				// store's: refuse, unless the operator opted in.
				if !w.trustHost {
					return fmt.Errorf("%w: the object store sent no usable Date header, so the snapshot-to-CDC handoff point is unknown; "+
						"use a store that sends Date, or set trustHostClock only if this host's clock agrees with the store's to within lookbackPeriod", ErrStoreClockUnavailable)
				}
				if !w.warnedNoDate {
					w.warnedNoDate = true
					w.log.Warn().Msg("trustHostClock is set and the object store sent no usable Date header: the snapshot-to-CDC handoff point comes from this host's clock, which loses objects if it is ahead of the store's by more than lookbackPeriod")
				}
			}
		}
		w.page, w.index = nextPage, 0
		if len(nextPage.Contents) > 0 {
			return nil
		}
	}
	w.page, w.index = nil, 0
	return sdk.ErrBackoffRetry
}

// fail records a listing or read error. It is sticky: once the snapshot has
// failed it never reports "no more objects" or moves on, whatever the caller
// does next. Treating a failure as the end of the snapshot would hand over to
// CDC and silently skip every object not yet read.
func (w *SnapshotIterator) fail(err error) error {
	w.listErr = err
	return err
}

// HasNext returns a boolean that indicates whether the iterator has more objects to return or not.
// A failed listing is not "no more objects": it returns true and Next reports
// the error.
func (w *SnapshotIterator) HasNext(ctx context.Context) bool {
	if w.listErr != nil {
		return true
	}
	if w.shouldRefreshPage() {
		err := w.refreshPage(ctx)
		if err != nil {
			if !errors.Is(err, sdk.ErrBackoffRetry) {
				_ = w.fail(err)
				return true
			}
			return false
		}
	}
	return true
}

// Next returns the next record in the iterator.
// returns an empty record and an error if anything wrong happened. The position
// (page index, max LastModified) only advances once the record is fully built,
// and an error is sticky. An object that was deleted between being listed and
// being read is skipped: it is gone, so there is nothing to deliver, and the
// CDC phase reports the deletion.
func (w *SnapshotIterator) Next(ctx context.Context) (opencdc.Record, error) {
	for {
		if w.listErr != nil {
			return opencdc.Record{}, w.listErr
		}
		if w.shouldRefreshPage() {
			err := w.refreshPage(ctx)
			if err != nil {
				if errors.Is(err, sdk.ErrBackoffRetry) {
					return opencdc.Record{}, err
				}
				return opencdc.Record{}, w.fail(err)
			}
		}

		// after making sure the object is available, get the object's key
		key := w.page.Contents[w.index].Key

		// read object
		object, err := w.client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(w.bucket),
			Key:    key,
		})
		if err != nil {
			var noSuchKey *types.NoSuchKey
			if errors.As(err, &noSuchKey) {
				w.log.Debug().Str("key", *key).Msg("object was deleted after it was listed, skipping it")
				w.index++
				continue
			}
			return opencdc.Record{}, w.fail(fmt.Errorf("could not fetch the next object: %w", err))
		}

		return w.buildRecord(key, object)
	}
}

func (w *SnapshotIterator) buildRecord(key *string, object *s3.GetObjectOutput) (opencdc.Record, error) {
	defer object.Body.Close()

	rawBody, err := io.ReadAll(object.Body)
	if err != nil {
		return opencdc.Record{}, w.fail(fmt.Errorf("could not read the object's body: %w", err))
	}

	// Everything that can fail is behind us: only now move the position.
	w.index++
	if lm := aws.ToTime(object.LastModified); w.maxLastModified.Before(lm) {
		w.maxLastModified = lm
	}

	p := position.Position{
		Key:       *key,
		Type:      position.TypeSnapshot,
		Timestamp: w.maxLastModified,
	}

	m := opencdc.Metadata{
		MetadataS3HeaderPrefix + MetadataContentType: aws.ToString(object.ContentType),
	}
	for key, val := range object.Metadata {
		m[key] = val
	}

	// create the record
	return sdk.Util.Source.NewRecordSnapshot(
		p.ToRecordPosition(), m,
		opencdc.RawData(*key),
		opencdc.RawData(rawBody),
	), nil
}

func (w *SnapshotIterator) Stop() {
	// nothing to stop
}

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
	log             *zerolog.Logger
	// listErr is a failed page listing. It must reach the caller: treating it
	// as the end of the snapshot would hand over to CDC and silently skip every
	// object not yet listed.
	listErr error
	// lastToken is the continuation token the last page was requested with.
	lastToken string

	// anchor is the newest LastModified among the objects that existed when
	// the snapshot started, found by one listing pass before the first page
	// is read. The snapshot-to-CDC handoff is anchored to it, NOT to the
	// newest LastModified the snapshot happens to read: later pages can hold
	// objects written long after the snapshot began, and handing over from
	// those would leave an object written early in the snapshot, under an
	// already listed key range, below the CDC floor whenever the snapshot
	// took longer than lookbackPeriod (#1032). Cost: one extra full listing of
	// the prefix (one ListObjectsV2 request per 1000 keys) per snapshot. Zero
	// for an empty bucket, which makes CDC select everything.
	prefix   string
	anchor   time.Time
	anchored bool
}

// NewSnapshotIterator takes the s3 bucket, the client, and the position.
// it returns a snapshotIterator starting from the position provided.
func NewSnapshotIterator(bucket, prefix string, client *s3.Client, p position.Position, log *zerolog.Logger) (*SnapshotIterator, error) {
	if log == nil {
		nop := zerolog.Nop()
		log = &nop
	}
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	}

	return &SnapshotIterator{
		prefix:          prefix,
		bucket:          bucket,
		client:          client,
		paginator:       s3.NewListObjectsV2Paginator(client, input),
		maxLastModified: p.Timestamp,
		log:             log,
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
	if !w.anchored {
		if err := w.findAnchor(ctx); err != nil {
			return fmt.Errorf("could not list the bucket to anchor the snapshot: %w", err)
		}
	}
	for w.paginator.HasMorePages() {
		nextPage, err := w.paginator.NextPage(ctx)
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
		w.page, w.index = nextPage, 0
		if len(nextPage.Contents) > 0 {
			return nil
		}
	}
	w.page, w.index = nil, 0
	return sdk.ErrBackoffRetry
}

// findAnchor lists the whole prefix once and records the newest LastModified.
// It runs before the first page of the snapshot proper, so everything the
// snapshot lists afterwards that is newer than the anchor was written after the
// snapshot started.
func (w *SnapshotIterator) findAnchor(ctx context.Context) error {
	pg := s3.NewListObjectsV2Paginator(w.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(w.bucket),
		Prefix: aws.String(w.prefix),
	})
	var newest time.Time
	var last string
	for pg.HasMorePages() {
		page, err := pg.NextPage(ctx)
		if err != nil {
			return err
		}
		for _, o := range page.Contents {
			if o.LastModified != nil && o.LastModified.After(newest) {
				newest = *o.LastModified
			}
		}
		next := aws.ToString(page.NextContinuationToken)
		if aws.ToBool(page.IsTruncated) && (next == "" || next == last) {
			return errors.New("the listing is truncated but has no new continuation token")
		}
		last = next
	}
	w.anchor, w.anchored = newest, true
	return nil
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

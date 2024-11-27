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
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-s3/source/position"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

// SnapshotIterator to iterate through S3 objects in a specific bucket.
type SnapshotIterator struct {
	bucket          string
	client          *s3.Client
	paginator       *s3.ListObjectsV2Paginator
	page            *s3.ListObjectsV2Output
	index           int
	maxLastModified time.Time
}

// NewSnapshotIterator takes the s3 bucket, the client, and the position.
// it returns a snapshotIterator starting from the position provided.
func NewSnapshotIterator(bucket, prefix string, client *s3.Client, p position.Position) (*SnapshotIterator, error) {
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	}

	return &SnapshotIterator{
		bucket:          bucket,
		client:          client,
		paginator:       s3.NewListObjectsV2Paginator(client, input),
		maxLastModified: p.Timestamp,
	}, nil
}

// shouldRefreshPage returns a boolean indicating whether the SnapshotIterator is empty or not.
func (w *SnapshotIterator) shouldRefreshPage() bool {
	return w.page == nil || len(w.page.Contents) == w.index
}

// refreshPage retrieves the next page from s3
// returns an error if the end of bucket is reached
func (w *SnapshotIterator) refreshPage(ctx context.Context) error {
	w.page = nil
	w.index = 0
	for w.paginator.HasMorePages() {
		nextPage, err := w.paginator.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("could not fetch next page: %w", err)
		}
		if len(nextPage.Contents) > 0 {
			w.page = nextPage
			break
		}
	}
	if w.page == nil {
		return sdk.ErrBackoffRetry
	}
	return nil
}

// HasNext returns a boolean that indicates whether the iterator has more objects to return or not.
func (w *SnapshotIterator) HasNext(ctx context.Context) bool {
	if w.shouldRefreshPage() {
		err := w.refreshPage(ctx)
		if err != nil {
			return false
		}
	}
	return true
}

// Next returns the next record in the iterator.
// returns an empty record and an error if anything wrong happened.
func (w *SnapshotIterator) Next(ctx context.Context) (opencdc.Record, error) {
	if w.shouldRefreshPage() {
		err := w.refreshPage(ctx)
		if err != nil {
			return opencdc.Record{}, err
		}
	}

	// after making sure the object is available, get the object's key
	key := w.page.Contents[w.index].Key
	w.index++

	// read object
	object, err := w.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(w.bucket),
		Key:    key,
	})
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("could not fetch the next object: %w", err)
	}

	// check if maxLastModified should be updated
	if w.maxLastModified.Before(*object.LastModified) {
		w.maxLastModified = *object.LastModified
	}

	rawBody, err := io.ReadAll(object.Body)
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("could not read the object's body: %w", err)
	}

	p := position.Position{
		Key:       *key,
		Type:      position.TypeSnapshot,
		Timestamp: w.maxLastModified,
	}

	m := opencdc.Metadata{
		MetadataS3HeaderPrefix + MetadataContentType: *object.ContentType,
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

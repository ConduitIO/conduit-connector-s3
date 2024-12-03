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
	"gopkg.in/tomb.v2"
)

// CDCIterator scans the bucket periodically and detects changes made to it.
type CDCIterator struct {
	bucket       string
	prefix       string
	client       *s3.Client
	buffer       chan opencdc.Record
	ticker       *time.Ticker
	lastModified time.Time
	caches       chan []CacheEntry
	tomb         *tomb.Tomb
}

type CacheEntry struct {
	key          string
	operation    opencdc.Operation
	lastModified time.Time
}

// NewCDCIterator returns a CDCIterator and starts the process of listening to changes every pollingPeriod.
func NewCDCIterator(
	bucket, prefix string,
	pollingPeriod time.Duration,
	client *s3.Client,
	from time.Time,
) (*CDCIterator, error) {
	cdc := CDCIterator{
		bucket:       bucket,
		prefix:       prefix,
		client:       client,
		buffer:       make(chan opencdc.Record, 1),
		caches:       make(chan []CacheEntry),
		ticker:       time.NewTicker(pollingPeriod),
		tomb:         &tomb.Tomb{},
		lastModified: from,
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
	case r := <-w.buffer:
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
// only detects the changes made after the w.lastModified
func (w *CDCIterator) startCDC() error {
	defer close(w.caches)

	// we initialize two caches that we reuse so we don't allocate a new one every time
	cache := make([]CacheEntry, 0)
	nextCache := make([]CacheEntry, 0)
	for {
		select {
		case <-w.tomb.Dying():
			return w.tomb.Err()
		case <-w.ticker.C: // detect changes every polling period
			err := w.populateCache(w.tomb.Context(nil), &cache, nil) //nolint:staticcheck // SA1012 tomb expects nil
			if err != nil {
				return err
			}
			if len(cache) == 0 {
				continue
			}
			sort.Slice(cache, func(i, j int) bool {
				return cache[i].lastModified.Before(cache[j].lastModified)
			})

			select {
			case w.caches <- cache:
				// worked fine
				w.lastModified = cache[len(cache)-1].lastModified
				cache, nextCache = nextCache, cache // switch caches
				cache = cache[:0]                   // empty cache
			case <-w.tomb.Dying():
				return w.tomb.Err()
			}
		}
	}
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

// getLatestObjects gets all the latest version of objects in S3 bucket
func (w *CDCIterator) populateCache(ctx context.Context, cache *[]CacheEntry, keyMarker *string) error {
	listObjectInput := &s3.ListObjectVersionsInput{ // default is 1000 keys max
		Bucket:    aws.String(w.bucket),
		Prefix:    aws.String(w.prefix),
		KeyMarker: keyMarker,
	}
	objects, err := w.client.ListObjectVersions(ctx, listObjectInput)
	if err != nil {
		return fmt.Errorf("couldn't get latest objects: %w", err)
	}

	updatedObjects := make(map[string]bool)

	for _, v := range objects.Versions {
		if *v.IsLatest && v.LastModified.After(w.lastModified) {
			*cache = append(*cache, CacheEntry{key: *v.Key, lastModified: *v.LastModified, operation: opencdc.OperationCreate})
		} else {
			// this is a version that is not the latest, this means this object
			// was updated
			updatedObjects[*v.Key] = true
		}
	}
	for i, entry := range *cache {
		if updatedObjects[entry.key] {
			entry.operation = opencdc.OperationUpdate
			(*cache)[i] = entry
		}
	}

	for _, v := range objects.DeleteMarkers {
		if *v.IsLatest && v.LastModified.After(w.lastModified) {
			*cache = append(*cache, CacheEntry{key: *v.Key, lastModified: *v.LastModified, operation: opencdc.OperationDelete})
		}
	}

	if *objects.IsTruncated {
		return w.populateCache(ctx, cache, objects.NextKeyMarker)
	}
	return nil
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

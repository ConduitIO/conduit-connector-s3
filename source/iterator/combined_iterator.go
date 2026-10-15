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
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-s3/source/position"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/rs/zerolog"
)

const (
	MetadataS3HeaderPrefix = "s3.header."
	MetadataContentType    = "contentType"
)

type CombinedIterator struct {
	snapshotIterator *SnapshotIterator
	cdcIterator      *CDCIterator

	bucket        string
	prefix        string
	pollingPeriod time.Duration
	lookback      time.Duration
	client        *s3.Client
	log           *zerolog.Logger
	// failed is a handoff failure. It is sticky so that a retry cannot skip
	// the record that was in hand when the handoff failed.
	failed error
}

func NewCombinedIterator(
	ctx context.Context,
	bucket, prefix string,
	pollingPeriod, lookback time.Duration,
	client *s3.Client,
	p position.Position,
) (*CombinedIterator, error) {
	var err error
	c := &CombinedIterator{
		bucket:        bucket,
		prefix:        prefix,
		pollingPeriod: pollingPeriod,
		lookback:      lookback,
		client:        client,
		log:           sdk.Logger(ctx),
	}

	switch p.Type {
	case position.TypeSnapshot:
		if len(p.Key) != 0 {
			sdk.Logger(ctx).
				Warn().
				Str("position", string(p.ToRecordPosition())).
				Msg("previous snapshot did not complete successfully. snapshot will be restarted for consistency.")
		}
		p = position.Position{} // always start snapshot from the beginning, so position is nil
		c.snapshotIterator, err = NewSnapshotIterator(bucket, prefix, lookback, client, p, c.log)
		if err != nil {
			return nil, fmt.Errorf("could not create the snapshot iterator: %w", err)
		}
	case position.TypeCDC:
		c.cdcIterator, err = NewCDCIterator(bucket, prefix, pollingPeriod, lookback, client, p.Timestamp, c.log)
		if err != nil {
			return nil, fmt.Errorf("could not create the CDC iterator: %w", err)
		}
	default:
		return nil, fmt.Errorf("invalid position type (%d)", p.Type)
	}
	return c, nil
}

func (c *CombinedIterator) HasNext(ctx context.Context) bool {
	if c.failed != nil {
		return true // Next reports the error
	}
	switch {
	case c.snapshotIterator != nil:
		// case of empty bucket or end of bucket
		if !c.snapshotIterator.HasNext(ctx) {
			if err := c.switchToCDCIterator(); err != nil {
				c.failed = err
				return true
			}
			return false
		}
		return true
	case c.cdcIterator != nil:
		return c.cdcIterator.HasNext(ctx)
	default:
		return false
	}
}

func (c *CombinedIterator) Next(ctx context.Context) (opencdc.Record, error) {
	if c.failed != nil {
		return opencdc.Record{}, c.failed
	}
	switch {
	case c.snapshotIterator != nil:
		r, err := c.snapshotIterator.Next(ctx)
		if err != nil {
			return opencdc.Record{}, err
		}
		if !c.snapshotIterator.HasNext(ctx) {
			// The last snapshot record carries the CDC position. It must hold
			// the handoff watermark, not the snapshot's max LastModified, or a
			// restart right after the handoff would skip what the snapshot
			// missed. It is built from the record's key, not parsed back from
			// the position, so there is no error path that could drop the
			// record after the snapshot iterator already consumed it.
			cdcPos := position.Position{
				Key:       string(r.Key.Bytes()),
				Timestamp: c.handoffTimestamp(),
				Type:      position.TypeCDC,
			}
			if err := c.switchToCDCIterator(); err != nil {
				c.failed = err // sticky: the record in hand is not lost silently
				return opencdc.Record{}, err
			}
			r.Position = cdcPos.ToRecordPosition()
		}
		return r, nil

	case c.cdcIterator != nil:
		return c.cdcIterator.Next(ctx)
	default:
		return opencdc.Record{}, errors.New("no initialized iterator")
	}
}

func (c *CombinedIterator) Stop() {
	if c.cdcIterator != nil {
		c.cdcIterator.Stop()
	}
}

// handoffTimestamp is the position CDC resumes from after a snapshot: the
// newest LastModified among the objects that existed when the snapshot started
// (see SnapshotIterator.anchor; zero for an empty bucket, which makes CDC
// select everything). The CDC iterator selects from this minus lookbackPeriod,
// so objects written while the snapshot ran, which the snapshot may have listed
// past, are covered however long the snapshot took.
//
// Redelivery this causes is bounded by lookbackPeriod plus the writes during the
// snapshot (at-least-once permits that).
func (c *CombinedIterator) handoffTimestamp() time.Time {
	return c.snapshotIterator.anchor
}

// switchToCDCIterator hands over from the snapshot to CDC. The CDC iterator
// starts with what the snapshot delivered inside its window as already
// delivered, so a run without failures delivers each object once (#1040).
// After a restart from the CDC position that seed is gone and the window is
// delivered again (at-least-once, documented).
func (c *CombinedIterator) switchToCDCIterator() error {
	timestamp := c.handoffTimestamp()
	cdc := newCDCIterator(c.bucket, c.prefix, c.lookback, c.client, timestamp, c.log)
	cdc.seedDelivered(c.snapshotIterator.delivered)
	cdc.start(c.pollingPeriod)
	c.cdcIterator = cdc
	c.snapshotIterator = nil
	return nil
}

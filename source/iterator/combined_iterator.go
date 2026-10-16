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

// CombinedConfig configures a CombinedIterator.
type CombinedConfig struct {
	Bucket, Prefix         string
	PollingPeriod          time.Duration
	Lookback               time.Duration
	MultipartHold          bool
	MaxTrackedUploads      int
	MaxPendingUploadAge    time.Duration
	MaxEarlyObjects        int
	MaxSettledBytesPerPoll int64
}

// CombinedIterator runs the snapshot, then change capture.
//
// Every snapshot record carries a snapshot position. When the snapshot listing
// is exhausted (in this run, or right away when resuming from the position of
// the last snapshot record), the iterator switches to change capture from the
// snapshot cut. There is no look-ahead: a snapshot record never carries a
// change capture position (design N5).
type CombinedIterator struct {
	snapshotIterator *SnapshotIterator
	cdcIterator      *CDCIterator

	cfg    CombinedConfig
	client *s3.Client
	log    *zerolog.Logger
	// failed is a handoff failure. It is sticky so that a retry cannot skip
	// the record that was in hand when the handoff failed.
	failed error
}

func NewCombinedIterator(ctx context.Context, cfg CombinedConfig, client *s3.Client, rp opencdc.Position) (*CombinedIterator, error) {
	c := &CombinedIterator{cfg: cfg, client: client, log: sdk.Logger(ctx)}
	parsed, err := position.Parse(rp)
	if err != nil {
		return nil, err
	}
	switch {
	case parsed.V2 != nil && parsed.V2.Snapshot != nil:
		c.snapshotIterator = NewSnapshotIterator(c.snapshotConfig(), client, parsed.V2.Snapshot, c.log)
	case parsed.V2 != nil:
		c.cdcIterator = NewCDCIterator(c.cdcConfig(), client, *parsed.V2.CDC, c.log)
	case parsed.V1 != nil && parsed.V1.Type == position.TypeCDC:
		c.log.Warn().Str("code", "s3.source.position_upgraded").Str("position", string(rp)).
			Msg("resuming from a change capture position written by an earlier version: the lookback window is delivered once more; positions from now on resume exactly")
		c.cdcIterator = NewCDCIteratorFromV1(c.cdcConfig(), client, *parsed.V1, c.log)
	default:
		if parsed.V1 != nil {
			// A v1 snapshot position holds no snapshot cut, so the snapshot
			// cannot be resumed exactly; it starts over once, as v1 itself
			// did on every restart.
			c.log.Warn().Str("code", "s3.source.position_upgraded").Str("position", string(rp)).
				Msg("resuming from a snapshot position written by an earlier version: the snapshot starts over once; positions from now on resume exactly")
		}
		c.snapshotIterator = NewSnapshotIterator(c.snapshotConfig(), client, nil, c.log)
	}
	return c, nil
}

func (c *CombinedIterator) snapshotConfig() SnapshotConfig {
	return SnapshotConfig{
		Bucket: c.cfg.Bucket, Prefix: c.cfg.Prefix, Lookback: c.cfg.Lookback,
		MultipartHold: c.cfg.MultipartHold, MaxTrackedUploads: c.cfg.MaxTrackedUploads,
		MaxPendingUploadAge: c.cfg.MaxPendingUploadAge,
	}
}

func (c *CombinedIterator) cdcConfig() CDCConfig {
	return CDCConfig{
		Bucket: c.cfg.Bucket, Prefix: c.cfg.Prefix, PollingPeriod: c.cfg.PollingPeriod, Lookback: c.cfg.Lookback,
		MultipartHold: c.cfg.MultipartHold, MaxTrackedUploads: c.cfg.MaxTrackedUploads,
		MaxPendingUploadAge: c.cfg.MaxPendingUploadAge, MaxEarlyObjects: c.cfg.MaxEarlyObjects,
		MaxSettledBytesPerPoll: c.cfg.MaxSettledBytesPerPoll,
	}
}

func (c *CombinedIterator) HasNext(ctx context.Context) bool {
	if c.failed != nil {
		return true // Next reports the error
	}
	switch {
	case c.snapshotIterator != nil:
		if c.snapshotIterator.HasNext(ctx) {
			return true
		}
		// The listing is exhausted (a failure would have returned true).
		if err := c.switchToCDCIterator(); err != nil {
			c.failed = err
			return true
		}
		return false
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
		if errors.Is(err, sdk.ErrBackoffRetry) {
			if err := c.switchToCDCIterator(); err != nil {
				c.failed = err
				return opencdc.Record{}, err
			}
		}
		return r, err
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

// switchToCDCIterator starts change capture from the snapshot cut: S = (C0,
// +inf), E empty, R = R0. The snapshot delivered exactly the settled objects
// at or below C0 (except tracked uploads), so this is exact (design
// "Snapshot", handoff).
func (c *CombinedIterator) switchToCDCIterator() error {
	cut, tracked := c.snapshotIterator.Cut()
	c.cdcIterator = NewCDCIterator(c.cdcConfig(), c.client, position.CDCV2{
		S:       position.Tuple{Time: cut.UTC().Truncate(time.Millisecond), Inf: true},
		Tracked: tracked,
	}, c.log)
	c.snapshotIterator = nil
	return nil
}

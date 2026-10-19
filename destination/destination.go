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

package destination

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-s3/destination/format"
	"github.com/conduitio/conduit-connector-s3/destination/partitioner"
	"github.com/conduitio/conduit-connector-s3/destination/writer"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

// Default batching: the destination buffers records and writes them as one
// object per batch. With no batching, every record is its own PutObject, so
// the per-object HTTP round trip and AWS SDK request setup are paid per record
// (measured at ~1.5 ms/record against a local MinIO, ~360x slower than a batch
// of 1000). The reference S3 sink (Kafka Connect) batches by default
// (flush.size); this connector used not to, so the default was one object per
// record. BatchDelay bounds the latency of an incomplete batch on a slow
// pipeline: a record is flushed at most this long after it arrives.
//
// Batching never weakens Invariant 1: the SDK acks a record only after the
// PutObject of its batch succeeded, and a batch that is not yet flushed is
// redelivered on restart (at-least-once).
const (
	defaultBatchSize  = 1000
	defaultBatchDelay = 1 * time.Second
)

// Destination S3 Connector persists records to an S3 storage. The records are usually
// buffered and written in batches for performance reasons. The buffer size is
// determined by config.
type Destination struct {
	sdk.UnimplementedDestination

	config  Config
	encoder format.Encoder
	Writer  writer.Writer
	// partition is nil without partitioning (partitioner "none").
	partition partitioner.Func
	// now is the clock of the wallclock timestamp extractor; a field so tests
	// can fix it.
	now func() time.Time
}

func NewDestination() sdk.Destination {
	// The batching defaults are set on the config before the SDK parses it, so
	// they become the defaults of sdk.batch.size and sdk.batch.delay (the SDK's
	// own default is 0, i.e. batching off: one PutObject per record). A user can
	// still override them, or set sdk.batch.size to 0 to disable batching.
	d := &Destination{
		config: Config{
			DefaultDestinationMiddleware: sdk.DefaultDestinationMiddleware{
				DestinationWithBatch: sdk.DestinationWithBatch{
					BatchSize:  defaultBatchSize,
					BatchDelay: defaultBatchDelay,
				},
			},
		},
	}
	return sdk.DestinationWithMiddleware(d)
}

func (d *Destination) Config() sdk.DestinationConfig {
	return &d.config
}

// Open makes sure everything is prepared to receive records.
func (d *Destination) Open(ctx context.Context) error {
	d.config.LogEndpointWarnings(ctx)

	enc, err := d.config.Encoder()
	if err != nil {
		return err
	}
	d.encoder = enc

	// initializing the writer
	w, err := writer.NewS3(ctx, &writer.S3Config{
		AccessKeyID:     d.config.AWSAccessKeyID,
		SecretAccessKey: d.config.AWSSecretAccessKey,
		Region:          d.config.AWSRegion,
		Bucket:          d.config.AWSBucket,
		KeyPrefix:       d.config.Prefix,
		Endpoint:        d.config.AWSEndpoint,
		PathStyle:       d.config.AWSPathStyle,
		// Config.Validate restricts both settings to the values the SDK enum
		// defines, so the conversion cannot produce an unknown mode.
		ServerSideEncryption: types.ServerSideEncryption(d.config.ServerSideEncryption),
		SSEKMSKeyID:          d.config.SSEKMSKeyID,
		ConditionalWrites:    d.config.ConditionalWrites,
	})
	if err != nil {
		return err
	}

	d.Writer = w
	if d.config.ServerSideEncryption == SSENone {
		d.checkBucketEncryption(ctx, w)
	}
	d.partition, err = partitioner.New(d.config.PartitionerConfig())
	if err != nil {
		return err // Validate already checked it; kept for callers that skip it
	}
	if d.now == nil {
		d.now = time.Now
	}
	return nil
}

// checkBucketEncryption warns when no encryption is configured and the bucket
// has no default encryption either, so objects are stored unencrypted. On AWS
// every bucket has at least SSE-S3 by default; S3-compatible stores often do
// not. It never refuses to start: an unencrypted bucket can be deliberate, and
// the check needs a permission (s3:GetEncryptionConfiguration) the connector
// does not otherwise need.
func (d *Destination) checkBucketEncryption(ctx context.Context, w *writer.S3) {
	algo, err := w.BucketDefaultEncryption(ctx)
	logger := sdk.Logger(ctx)
	switch {
	case err != nil:
		var apiErr smithy.APIError
		code := ""
		if errors.As(err, &apiErr) {
			code = apiErr.ErrorCode()
		}
		logger.Debug().Err(err).Str("error_code", code).Str("bucket", d.config.AWSBucket).
			Msg("could not read the bucket's default encryption; objects get whatever the bucket applies")
	case algo == "":
		logger.Warn().Str("code", "s3.destination.bucket_unencrypted").Str("bucket", d.config.AWSBucket).
			Msgf("%s is not set and the bucket has no default encryption, so objects are stored unencrypted; set %s (AES256, aws:kms or aws:kms:dsse) or configure default encryption on the bucket",
				ConfigKeyServerSideEncryption, ConfigKeyServerSideEncryption)
	default:
		logger.Debug().Str("bucket", d.config.AWSBucket).Str("default_encryption", algo).
			Msg("no encryption configured: the bucket's default encryption applies")
	}
}

// Write writes a slice of records into a Destination: one object per batch,
// or with a partitioner one object per partition directory.
//
// It returns the number of records, from the start of the slice, that are
// stored: the SDK acks exactly those and nacks the rest with the error.
func (d *Destination) Write(ctx context.Context, records []opencdc.Record) (int, error) {
	if d.partition == nil {
		batch := &writer.Batch{Records: records, Format: d.encoder}
		err := d.Writer.Write(ctx, batch)
		// Invariant 1: report as written only what the writer stored. A
		// record the format refuses is nacked together with the records after
		// it; the records before it are in the object the writer wrote.
		return batch.Written(err), err
	}
	return d.writePartitioned(ctx, records)
}

// group is the records of one batch that go to one directory. first is the
// index of the group's first record in the batch.
type group struct {
	dir     string
	first   int
	records []opencdc.Record
}

// writePartitioned splits records by directory and writes one object per
// directory, in the order in which each directory first appears.
//
// The count it returns is the index of the first record that is not stored:
// the first record of the first group whose write failed, or the first record
// that cannot be partitioned or that the format refuses. Every record before
// that index belongs to a group whose first record comes earlier, and groups
// are written in that order, so all of them were written. Records after it
// that are in groups written successfully are written again on retry: a
// duplicate, never a loss.
//
// The format check runs here, in batch order, and not in the writer: a
// refusal reported by the writer would carry an index into its group, not
// into the batch, and acking up to that index would ack records of later
// groups that were never written.
func (d *Destination) writePartitioned(ctx context.Context, records []opencdc.Record) (int, error) {
	now := d.now()
	var groups []*group
	byDir := map[string]*group{}
	limit, stopErr := len(records), error(nil)
	for i, r := range records {
		if err := d.encoder.Check(r); err != nil {
			// Refused like a record without a partition: everything before
			// it is still written and acked.
			limit, stopErr = i, &format.RecordError{Index: i, Position: r.Position, Err: err}
			break
		}
		dir, err := d.partition(r, now)
		if err != nil {
			// Records from here on are not written in this call; the
			// earlier ones still are, so that the ones before i are acked.
			limit, stopErr = i, err
			break
		}
		g, ok := byDir[dir]
		if !ok {
			g = &group{dir: dir, first: i}
			byDir[dir] = g
			groups = append(groups, g)
		}
		g.records = append(g.records, r)
	}

	for _, g := range groups {
		if err := d.Writer.Write(ctx, &writer.Batch{Records: g.records, Format: d.encoder, Dir: g.dir}); err != nil {
			// Invariant 1: report as written only records whose object
			// PutObject confirmed. Groups are written in order of their first
			// record, so everything before g.first is stored. This also holds
			// for a refusal inside the group, which the check above makes
			// unreachable: part of the group may be stored, but nothing of it
			// is reported (a duplicate on retry, never a loss).
			return g.first, fmt.Errorf("writing %d records to %s: %w", len(g.records), g.dir, err)
		}
		sdk.Logger(ctx).Debug().Str("dir", g.dir).Int("records", len(g.records)).Msg("wrote object")
	}
	if stopErr != nil {
		return limit, stopErr
	}
	return len(records), nil
}

// Teardown gracefully disconnects the client
func (d *Destination) Teardown(_ context.Context) error {
	return nil // nothing to do
}

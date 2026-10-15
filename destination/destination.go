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

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-s3/destination/writer"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

// Destination S3 Connector persists records to an S3 storage. The records are usually
// buffered and written in batches for performance reasons. The buffer size is
// determined by config.
type Destination struct {
	sdk.UnimplementedDestination

	config Config
	Writer writer.Writer
}

func NewDestination() sdk.Destination {
	return sdk.DestinationWithMiddleware(&Destination{})
}

func (d *Destination) Config() sdk.DestinationConfig {
	return &d.config
}

// Open makes sure everything is prepared to receive records.
func (d *Destination) Open(ctx context.Context) error {
	d.config.LogEndpointWarnings(ctx)

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
	})
	if err != nil {
		return err
	}

	d.Writer = w
	if d.config.ServerSideEncryption == SSENone {
		d.checkBucketEncryption(ctx, w)
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

// Write writes a slice of records into a Destination.
func (d *Destination) Write(ctx context.Context, records []opencdc.Record) (int, error) {
	err := d.Writer.Write(ctx, &writer.Batch{
		Records: records,
		Format:  d.config.Format,
	})
	if err != nil {
		return 0, err
	}
	return len(records), nil
}

// Teardown gracefully disconnects the client
func (d *Destination) Teardown(_ context.Context) error {
	return nil // nothing to do
}

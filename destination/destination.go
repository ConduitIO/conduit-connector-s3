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

	"github.com/conduitio/conduit-connector-s3/config"
	"github.com/conduitio/conduit-connector-s3/destination/writer"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

// Destination S3 Connector persists records to an S3 storage. The records are usually
// buffered and written in batches for performance reasons. The buffer size is
// determined by config.
type Destination struct {
	sdk.UnimplementedDestination

	Config Config
	Writer writer.Writer
}

func NewDestination() sdk.Destination {
	return sdk.DestinationWithMiddleware(&Destination{}, sdk.DefaultDestinationMiddleware()...)
}

func (d *Destination) Parameters() map[string]sdk.Parameter {
	return map[string]sdk.Parameter{
		config.ConfigKeyAWSAccessKeyID: {
			Default:     "",
			Required:    true,
			Description: "AWS access key id.",
		},
		config.ConfigKeyAWSSecretAccessKey: {
			Default:     "",
			Required:    true,
			Description: "AWS secret access key.",
		},
		config.ConfigKeyAWSRegion: {
			Default:     "",
			Required:    true,
			Description: "the AWS S3 bucket region.",
		},
		config.ConfigKeyAWSBucket: {
			Default:     "",
			Required:    true,
			Description: "the AWS S3 bucket name.",
		},
		ConfigKeyFormat: {
			Default:     "",
			Required:    false,
			Description: `the destination format, either "json" or "parquet".`,
		},
		ConfigKeyPrefix: {
			Default:     "",
			Required:    false,
			Description: "the key prefix for S3 destination.",
		},
	}
}

// Configure parses and initializes the config.
func (d *Destination) Configure(ctx context.Context, cfg map[string]string) error {
	configuration, err := Parse(cfg)

	if err != nil {
		return err
	}

	d.Config = configuration

	return nil
}

// Open makes sure everything is prepared to receive records.
func (d *Destination) Open(ctx context.Context) error {
	// initializing the writer
	w, err := writer.NewS3(ctx, &writer.S3Config{
		AccessKeyID:     d.Config.AWSAccessKeyID,
		SecretAccessKey: d.Config.AWSSecretAccessKey,
		Region:          d.Config.AWSRegion,
		Bucket:          d.Config.AWSBucket,
		KeyPrefix:       d.Config.Prefix,
	})
	if err != nil {
		return err
	}

	d.Writer = w
	return nil
}

// Write writes a slice of records into a Destination.
func (d *Destination) Write(ctx context.Context, records []sdk.Record) (int, error) {
	err := d.Writer.Write(ctx, &writer.Batch{
		Records: records,
		Format:  d.Config.Format,
	})
	if err != nil {
		return 0, err
	}
	return len(records), nil
}

// Teardown gracefully disconnects the client
func (d *Destination) Teardown(ctx context.Context) error {
	return nil // nothing to do
}

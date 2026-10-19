// Copyright © 2026 Meroxa, Inc.
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

package destination_test

import (
	"context"
	"testing"
	"time"

	s3Conn "github.com/conduitio/conduit-connector-s3"
	"github.com/conduitio/conduit-connector-s3/config"
	"github.com/conduitio/conduit-connector-s3/destination"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
)

// TestNewDestination_DefaultBatching is the regression test for the default
// batching: without an explicit sdk.batch.size the SDK used to hand the
// destination one record per Write, so every record became its own PutObject
// (~1.5 ms/record against MinIO, ~360x slower than a batch of 1000). The
// destination now batches by default; the user can still set sdk.batch.size
// to 0 to restore one object per record.
func TestNewDestination_DefaultBatching(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	base := map[string]string{
		config.ConfigKeyAWSAccessKeyID:       "access-key-123",
		config.ConfigKeyAWSSecretAccessKey:   "secret-key-321",
		config.ConfigKeyAWSRegion:            "us-west-2",
		config.ConfigKeyAWSBucket:            "foobucket",
		destination.ConfigKeyFormat:          "json",
		"sdk.schema.extract.key.enabled":     "false",
		"sdk.schema.extract.payload.enabled": "false",
	}
	params := s3Conn.Connector.NewSpecification().DestinationParams

	t.Run("default", func(t *testing.T) {
		is := is.New(t)
		d := destination.NewDestination()
		cfg := map[string]string{}
		for k, v := range base {
			cfg[k] = v
		}
		is.NoErr(sdk.Util.ParseConfig(ctx, cfg, d.Config(), params))
		got := d.Config().(*destination.Config)
		is.Equal(got.BatchSize, 1000)
		is.Equal(got.BatchDelay, 1*time.Second)
	})

	t.Run("explicit override", func(t *testing.T) {
		is := is.New(t)
		d := destination.NewDestination()
		cfg := map[string]string{}
		for k, v := range base {
			cfg[k] = v
		}
		cfg["sdk.batch.size"] = "50"
		cfg["sdk.batch.delay"] = "20ms"
		is.NoErr(sdk.Util.ParseConfig(ctx, cfg, d.Config(), params))
		got := d.Config().(*destination.Config)
		is.Equal(got.BatchSize, 50)
		is.Equal(got.BatchDelay, 20*time.Millisecond)
	})

	t.Run("disable with zero", func(t *testing.T) {
		is := is.New(t)
		d := destination.NewDestination()
		cfg := map[string]string{}
		for k, v := range base {
			cfg[k] = v
		}
		cfg["sdk.batch.size"] = "0"
		cfg["sdk.batch.delay"] = "0"
		is.NoErr(sdk.Util.ParseConfig(ctx, cfg, d.Config(), params))
		got := d.Config().(*destination.Config)
		is.Equal(got.BatchSize, 0)
		is.Equal(got.BatchDelay, time.Duration(0))
	})
}

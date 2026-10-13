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

package source

import (
	"context"
	"fmt"
	"time"

	"github.com/conduitio/conduit-connector-s3/config"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

const (
	// MaxLookbackPeriod is the largest accepted lookbackPeriod.
	MaxLookbackPeriod = 24 * time.Hour
	// WarnLookbackPeriod is the lookbackPeriod above which Open warns.
	WarnLookbackPeriod = time.Hour

	// ConfigKeyPollingPeriod is the config name for the S3 CDC polling period
	ConfigKeyPollingPeriod = "pollingPeriod"
	// ConfigKeyLookbackPeriod is the config name for the CDC lookback window
	ConfigKeyLookbackPeriod = "lookbackPeriod"
)

// Config represents source configuration with S3 configurations
type Config struct {
	sdk.DefaultSourceMiddleware
	config.Config

	// polling period for the CDC mode, formatted as a time.Duration string.
	PollingPeriod time.Duration `json:"pollingPeriod" default:"1s"`

	// How far back, from the newest object seen, every CDC poll looks for
	// objects that became visible late. S3 stamps an object with the time its
	// upload began (a multipart upload with the time it was initiated), not
	// the time it completed, and lists in key order, so an object can show up
	// after newer ones were already delivered. Each poll selects the objects
	// modified since (newest LastModified seen - lookbackPeriod) that were not
	// delivered yet. Listing based CDC is best effort and bounded by this
	// value: an upload that completes later than this value after newer
	// objects were seen can still be missed (see the connector description for
	// the full list of known limits). Cost: memory for the objects modified within the window
	// (limit about 256 MiB, the source stops with
	// s3.source.dedupe_window_overflow beyond it, lower this value then), and
	// redelivery (at-least-once) of the objects modified within the window
	// after a restart and when the snapshot hands over to CDC. Listing cost
	// does not depend on it. Values above 1h log a warning, above 24h are
	// rejected, and 0 disables the lookback. Known limits of the
	// deduplication: an identical re-put of an object within the same second
	// is not seen as a change, an overwrite in an unversioned bucket is
	// reported as a create, and intermediate versions that appear and are
	// superseded within one poll are not emitted.
	LookbackPeriod time.Duration `json:"lookbackPeriod" default:"5m"`
}

// Validate implements sdk.Validatable: the SDK calls it automatically after
// configuration parsing. It runs the middleware validations and the ones the
// parameter specifications cannot express, such as the aws.endpoint URL shape.
func (c *Config) Validate(ctx context.Context) error {
	if err := c.DefaultSourceMiddleware.Validate(ctx); err != nil {
		return err
	}
	if c.LookbackPeriod < 0 {
		return fmt.Errorf("%s: must not be negative, got %v (use 0 to disable the lookback)", ConfigKeyLookbackPeriod, c.LookbackPeriod)
	}
	if c.LookbackPeriod > MaxLookbackPeriod {
		return fmt.Errorf("%s: %v is above the maximum of %v (every restart would redeliver that much history); use a smaller value", ConfigKeyLookbackPeriod, c.LookbackPeriod, MaxLookbackPeriod)
	}
	return config.ValidateEndpoint(c.AWSEndpoint)
}

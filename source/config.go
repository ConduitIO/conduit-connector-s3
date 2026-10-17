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

	ConfigKeyMaxEarlyObjects        = "cdc.maxEarlyObjects"
	ConfigKeyMaxSettledBytesPerPoll = "cdc.maxSettledBytesPerPoll"
	ConfigKeyMultipartHold          = "cdc.multipartHold"
	ConfigKeyMaxTrackedUploads      = "cdc.maxTrackedUploads"
	ConfigKeyMaxPendingUploadAge    = "cdc.maxPendingUploadAge"

	// MinExactLookback is the smallest lookbackPeriod exact resume is claimed
	// for. Values from 1s up to it are deprecated (accepted with a warning,
	// rejected two minor releases later); 0 is accepted and lossy.
	MinExactLookback = 30 * time.Second
	// maxBound is the format limit of the early and tracked sets.
	maxBound = 10000
)

// Config represents source configuration with S3 configurations
type Config struct {
	sdk.DefaultSourceMiddleware
	config.Config

	// polling period for the CDC mode, formatted as a time.Duration string.
	PollingPeriod time.Duration `json:"pollingPeriod" default:"1s"`

	// The late-visibility tolerance of change capture. An object is
	// "settled" once it is older than this (by the store's clock); the
	// source assumes that no object becomes visible in listings later than
	// this after its LastModified (replication lag, a slow PUT and clock skew
	// between store nodes must stay below it; multipart uploads are tracked
	// separately). New objects are delivered at once while they fit
	// cdc.maxEarlyObjects, the rest when they settle, so this is also the
	// maximum extra latency on a busy bucket. Positions resume exactly
	// whatever the value. 30s or more is supported; 1s to 29s is deprecated
	// (a warning; rejected in a later release); 0 is accepted but lossy for
	// late-visible objects; above 24h is rejected.
	LookbackPeriod time.Duration `json:"lookbackPeriod" default:"5m"`

	// The largest number of new objects delivered before they settle (the
	// early set kept in every position, about 20 bytes per object). A bucket
	// written faster than cdc.maxEarlyObjects / lookbackPeriod gets the excess
	// up to lookbackPeriod later. Memory: positions take up to records in
	// flight x (80 + key length + 20 x this) bytes. 0 delivers everything only
	// once settled. At most 10000.
	MaxEarlyObjects int `json:"cdc.maxEarlyObjects" default:"2048"`
	// The memory budget, in bytes, for the settled objects one poll delivers
	// (a catch-up after downtime continues on the next poll).
	MaxSettledBytesPerPoll int `json:"cdc.maxSettledBytesPerPoll" default:"67108864"`
	// Track in-progress multipart uploads, so that an upload running longer
	// than lookbackPeriod is not missed (AWS stamps the completed object with
	// the time the upload started). Needs s3:ListBucketMultipartUploads.
	// false skips it and is lossy for such uploads.
	MultipartHold bool `json:"cdc.multipartHold" default:"true"`
	// The largest number of long-running multipart uploads tracked in a
	// position. When more run at once, the oldest untracked one delays
	// settling until a slot frees up. At most 10000.
	MaxTrackedUploads int `json:"cdc.maxTrackedUploads" default:"256"`
	// Multipart uploads initiated longer ago than this are not tracked (an
	// abandoned upload would otherwise occupy a slot forever); if one still
	// completes, its object can be missed. Add an AbortIncompleteMultipartUpload
	// lifecycle rule with DaysAfterInitiation at most this. Must be at least
	// lookbackPeriod.
	MaxPendingUploadAge time.Duration `json:"cdc.maxPendingUploadAge" default:"24h"`
}

// Validate implements sdk.Validatable: the SDK calls it automatically after
// configuration parsing. It runs the middleware validations and the ones the
// parameter specifications cannot express, such as the aws.endpoint URL shape and
// the both-or-neither rule for the static AWS credentials.
func (c *Config) Validate(ctx context.Context) error {
	if err := c.DefaultSourceMiddleware.Validate(ctx); err != nil {
		return err
	}
	if c.LookbackPeriod < 0 {
		return fmt.Errorf("%s: must not be negative, got %v", ConfigKeyLookbackPeriod, c.LookbackPeriod)
	}
	if c.LookbackPeriod > MaxLookbackPeriod {
		return fmt.Errorf("%s: %v is above the maximum of %v; use a smaller value", ConfigKeyLookbackPeriod, c.LookbackPeriod, MaxLookbackPeriod)
	}
	if c.MaxEarlyObjects < 0 || c.MaxEarlyObjects > maxBound {
		return fmt.Errorf("%s: %d is outside 0 to %d", ConfigKeyMaxEarlyObjects, c.MaxEarlyObjects, maxBound)
	}
	if c.MaxTrackedUploads < 0 || c.MaxTrackedUploads > maxBound {
		return fmt.Errorf("%s: %d is outside 0 to %d", ConfigKeyMaxTrackedUploads, c.MaxTrackedUploads, maxBound)
	}
	if c.MaxSettledBytesPerPoll < 1 {
		return fmt.Errorf("%s: must be positive, got %d", ConfigKeyMaxSettledBytesPerPoll, c.MaxSettledBytesPerPoll)
	}
	if c.MaxPendingUploadAge < c.LookbackPeriod {
		return fmt.Errorf("%s: %v is below %s (%v); an upload younger than the lookback must always be tracked, so raise %s",
			ConfigKeyMaxPendingUploadAge, c.MaxPendingUploadAge, ConfigKeyLookbackPeriod, c.LookbackPeriod, ConfigKeyMaxPendingUploadAge)
	}
	if err := config.ValidateEndpoint(c.AWSEndpoint); err != nil {
		return err
	}
	return c.ValidateAuth(ctx)
}

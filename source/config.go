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

	// How far back every CDC poll looks for objects that became visible late.
	// S3 stamps an object with the time its upload began (a multipart upload
	// with the time it was initiated), not the time it completed, so an
	// object can show up in a listing after newer objects were already
	// delivered. Each poll therefore selects objects modified since
	// (start of the previous poll - lookbackPeriod). An upload that takes
	// longer than this value can still be missed, and so can objects when
	// this machine's clock is ahead of the store's by more than it. The cost
	// is memory for the objects modified within the window and, after a
	// restart or the switch from snapshot to CDC, redelivery of the objects
	// modified within the window (at-least-once). Listing cost does not
	// depend on it: every poll lists the whole prefix anyway. Use 0 to
	// disable the lookback.
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
	return config.ValidateEndpoint(c.AWSEndpoint)
}

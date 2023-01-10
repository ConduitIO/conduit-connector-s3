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

//go:generate paramgen -output=paramgen_src.go Config

package source

import (
	"time"

	"github.com/conduitio/conduit-connector-s3/config"
)

const (
	// ConfigKeyPollingPeriod is the config name for the S3 CDC polling period
	ConfigKeyPollingPeriod = "pollingPeriod"
)

// Config represents source configuration with S3 configurations
type Config struct {
	config.Config
	// polling period for the CDC mode, formatted as a time.Duration string.
	PollingPeriod time.Duration `json:"pollingPeriod" default:"1s"`
}

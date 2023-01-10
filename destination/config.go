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

//go:generate paramgen -output=paramgen_dest.go Config

package destination

import (
	"github.com/conduitio/conduit-connector-s3/config"
	"github.com/conduitio/conduit-connector-s3/destination/format"
)

const (
	// ConfigKeyFormat is the config name for destination format.
	ConfigKeyFormat = "format"
)

// Config represents S3 configuration with Destination specific configurations
type Config struct {
	config.Config
	// the destination format, either "json" or "parquet".
	Format format.Format `validate:"required,inclusion=parquet|json"`
}

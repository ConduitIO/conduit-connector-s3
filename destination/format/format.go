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

package format

import (
	"fmt"

	sdk "github.com/conduitio/conduit-connector-sdk"
)

// Format defines the format the data will be persisted in by Destination
type Format string

const (
	// Parquet data format https://parquet.apache.org/
	Parquet Format = "parquet"

	// JSON format
	JSON Format = "json"

	// Original format
	Original Format = "original"
)

// All is a variable containing all supported format for enumeration
var All = []Format{
	Parquet,
	JSON,
	Original,
}

// Parse takes a string and returns a corresponding format or an error
func Parse(name string) (Format, error) {
	switch name {
	case "parquet":
		return Parquet, nil
	case "json":
		return JSON, nil
	case "original":
		return Original, nil
	default:
		return "", fmt.Errorf("unsupported format: %q", name)
	}
}

// Ext returns a preferable file extension for the given format
func (f Format) Ext() string {
	switch f {
	case Parquet:
		return "parquet"
	case JSON:
		return "json"
	default:
		return "bin"
	}
}

// MimeType returns MIME type (IANA media type or Content-Type) for the format
func (f Format) MimeType() string {
	switch f {
	case JSON:
		return "application/json"
	default:
		return "application/octet-stream"
	}
}

// MakeBytes returns a slice of bytes representing records in a given format
func (f Format) MakeBytes(records []sdk.Record) ([]byte, error) {
	switch f {
	case Parquet:
		return makeParquetBytes(records)
	case JSON:
		return makeJSONBytes(records)
	case Original:
		return makeOriginalBytes(records)
	default:
		return nil, fmt.Errorf("unsupported format: %s", f)
	}
}

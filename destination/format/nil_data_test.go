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
	"strings"
	"testing"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/matryer/is"
)

// TestMakeBytes_NilData is the regression test for #1030: a delete record has
// no After image and used to panic both encoders.
func TestMakeBytes_NilData(t *testing.T) {
	recs := []opencdc.Record{
		{Operation: opencdc.OperationDelete, Position: opencdc.Position("p"), Key: opencdc.RawData("k"),
			Payload: opencdc.Change{Before: opencdc.RawData("old")}},
		{Operation: opencdc.OperationCreate, Position: opencdc.Position("q"), // keyless
			Payload: opencdc.Change{After: opencdc.RawData("new")}},
	}

	t.Run("json", func(t *testing.T) {
		is := is.New(t)
		b, err := makeJSONBytes(recs)
		is.NoErr(err)
		is.True(strings.Contains(string(b), `"Operation":"delete"`))
		is.True(strings.Contains(string(b), `"Key":"k"`))
	})
	t.Run("parquet", func(t *testing.T) {
		is := is.New(t)
		b, err := makeParquetBytes(recs)
		is.NoErr(err)
		is.True(len(b) > 0)
	})
}

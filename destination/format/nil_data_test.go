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
	"encoding/json"
	"strings"
	"testing"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/matryer/is"
	"github.com/xitongsys/parquet-go-source/buffer"
	"github.com/xitongsys/parquet-go/reader"
)

// TestMakeBytes_NilData is the regression test for #1030: a delete record has
// no After image and used to panic both encoders.
//
// Decision recorded here (nil Key): a record without a Key is stored with an
// empty key ("") rather than rejected. The destination's job is to persist what
// the pipeline hands it, keyless records are legal in OpenCDC (for example from
// a raw-payload source), and failing the write would stop a pipeline over a
// record that carries all its data in the Payload. The empty key is explicit in
// the stored object, so a consumer can tell. If rejecting keyless records is
// wanted it is a behaviour change and belongs in its own issue.
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
		lines := strings.Split(strings.TrimSpace(string(b)), "\n")
		is.Equal(len(lines), 2)

		var del, keyless map[string]any
		is.NoErr(json.Unmarshal([]byte(lines[0]), &del))
		is.NoErr(json.Unmarshal([]byte(lines[1]), &keyless))
		is.Equal(del["Operation"], "delete")
		is.Equal(del["Key"], "k")
		is.Equal(del["Payload"], "") // no After image: stored as empty, not as the Before image
		is.Equal(del["Position"], "p")
		is.Equal(keyless["Operation"], "create")
		is.Equal(keyless["Key"], "") // documented: nil Key is stored empty
		is.Equal(keyless["Payload"], "new")
	})
	t.Run("parquet", func(t *testing.T) {
		is := is.New(t)
		b, err := makeParquetBytes(recs)
		is.NoErr(err)

		pf := buffer.NewBufferFileFromBytes(b)
		pr, err := reader.NewParquetReader(pf, new(parquetRecord), 1)
		is.NoErr(err)
		defer pr.ReadStop()
		is.Equal(pr.GetNumRows(), int64(2))

		rows := make([]parquetRecord, 2)
		is.NoErr(pr.Read(&rows))
		is.Equal(rows[0].Operation, "delete")
		is.Equal(rows[0].Key, "k")
		is.Equal(rows[0].Payload, "")
		is.Equal(rows[0].Position, "p")
		is.Equal(rows[1].Operation, "create")
		is.Equal(rows[1].Key, "")
		is.Equal(rows[1].Payload, "new")
	})
}

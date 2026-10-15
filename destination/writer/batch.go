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

package writer

import (
	"errors"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-s3/destination/format"
)

// Batch describes the data that needs to be saved by the Writer
type Batch struct {
	Format  format.Encoder
	Records []opencdc.Record
	// Dir is the partition directory under the key prefix, without leading
	// or trailing slash; empty for no partitioning.
	Dir string
}

// Bytes returns a byte representation for the Writer to write into a file.
//
// If a record cannot be encoded the error is a *format.RecordError and the
// bytes hold exactly the records before it (nil if none). A Writer stores
// those bytes when there are any and then returns the error, so that the
// destination reports exactly the stored prefix as written.
func (b *Batch) Bytes() ([]byte, error) {
	return b.Format.MakeBytes(b.Records)
}

// Written returns how many records of the batch a Writer stored when Write
// returned err: all of them for nil, the stored prefix for a
// *format.RecordError, none otherwise.
func (b *Batch) Written(err error) int {
	if err == nil {
		return len(b.Records)
	}
	var re *format.RecordError
	if errors.As(err, &re) {
		return re.Index
	}
	return 0
}

// LastPosition returns the position of the last record in the batch.
func (b *Batch) LastPosition() opencdc.Position {
	if len(b.Records) == 0 {
		return nil
	}

	last := b.Records[len(b.Records)-1]
	return last.Position
}

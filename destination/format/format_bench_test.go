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

package format

import (
	"fmt"
	"testing"

	"github.com/conduitio/conduit-commons/opencdc"
)

// benchRecords returns n records shaped like the ones a CDC source produces:
// a create with a small raw payload, a key and metadata.
func benchRecords(n int) []opencdc.Record {
	recs := make([]opencdc.Record, n)
	for i := range recs {
		recs[i] = opencdc.Record{
			Operation: opencdc.OperationCreate,
			Position:  opencdc.Position(fmt.Sprintf("pos-%d", i)),
			Key:       opencdc.RawData(fmt.Sprintf("key-%d", i)),
			Payload:   opencdc.Change{After: opencdc.RawData(fmt.Sprintf("this is a message #%d", i))},
			Metadata:  opencdc.Metadata{"opencdc.collection": "topic"},
		}
	}
	return recs
}

// BenchmarkEncodeJSON isolates the per-record cost of encoding a batch to one
// object's bytes (the CPU and allocation cost before the PutObject round trip).
func BenchmarkEncodeJSON(b *testing.B) {
	for _, n := range []int{1, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("batch=%d", n), func(b *testing.B) {
			recs := benchRecords(n)
			b.ReportAllocs()
			b.SetBytes(int64(n))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := JSON.MakeBytes(recs); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkEncodeByteArray isolates the bytearray format's per-record cost.
func BenchmarkEncodeByteArray(b *testing.B) {
	for _, n := range []int{1, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("batch=%d", n), func(b *testing.B) {
			recs := benchRecords(n)
			b.ReportAllocs()
			b.SetBytes(int64(n))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := ByteArray.MakeBytes(recs); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

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
	"bytes"
	"unicode/utf8"

	"github.com/conduitio/conduit-commons/opencdc"
)

// makeByteArrayBytes writes the payload bytes of every record followed by sep.
// Structured payloads are written as their JSON encoding. Records without a
// payload are refused before this is called (Encoder.check).
func makeByteArrayBytes(records []opencdc.Record, sep string) []byte {
	var buf bytes.Buffer
	for _, r := range records {
		buf.Write(r.Payload.After.Bytes())
		buf.WriteString(sep)
	}
	return buf.Bytes()
}

func utf8Valid(b []byte) bool { return utf8.Valid(b) }

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
	"bytes"
	"encoding/json"

	"github.com/conduitio/conduit-commons/opencdc"
)

type jsonRecord struct {
	// TODO save schema type
	Operation string            `json:"Operation"`
	Position  string            `json:"Position"`
	Payload   string            `json:"Payload"`
	Key       string            `json:"Key"`
	Metadata  map[string]string `json:"Metadata"`
}

func makeJSONBytes(records []opencdc.Record) ([]byte, error) {
	buf := bytes.NewBuffer([]byte{})

	for _, r := range records {
		r := jsonRecord{
			Operation: r.Operation.String(),
			Position:  string(r.Position),
			Payload:   string(r.Payload.After.Bytes()),
			Key:       string(r.Key.Bytes()),
			Metadata:  r.Metadata,
		}

		bytes, err := json.Marshal(r)

		if err != nil {
			return nil, err
		}

		buf.Write(bytes)
		buf.WriteByte('\n')
	}

	return buf.Bytes(), nil
}

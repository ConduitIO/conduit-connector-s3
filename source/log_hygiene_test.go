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

package source

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/matryer/is"
	"github.com/rs/zerolog"
)

// TestSource_AckLogDoesNotContainPosition: a position holds the object key.
func TestSource_AckLogDoesNotContainPosition(t *testing.T) {
	is := is.New(t)
	var buf bytes.Buffer
	ctx := zerolog.New(&buf).Level(zerolog.TraceLevel).WithContext(context.Background())

	is.NoErr((&Source{}).Ack(ctx, opencdc.Position("customers/SECRET-acme/invoice.csv_s1792000000")))

	out := buf.String()
	is.True(strings.Contains(out, "got ack"))
	is.True(strings.Contains(out, `"position_hash"`))
	is.True(!strings.Contains(out, "SECRET"))
}

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
	"context"
	"errors"
	"fmt"
	"os"
	"path"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-s3/destination/format"
)

// Local writer dumps bytes into a local file. The file will be placed in a
// directory defined by path property.
type Local struct {
	Path     string
	Position opencdc.Position
	Count    uint
}

var _ Writer = (*Local)(nil)

// Write writes a batch into a file on a local file system so it could later be
// compared to a reference file.
func (w *Local) Write(_ context.Context, batch *Batch) error {
	w.Count++

	path := path.Join(
		w.Path,
		fmt.Sprintf("local-%04d.%s", w.Count, batch.Format.Ext()),
	)

	bytes, refused := batch.Bytes()
	var re *format.RecordError
	if refused != nil && !errors.As(refused, &re) {
		return refused
	}
	if len(bytes) == 0 {
		return refused
	}

	err := os.WriteFile(path, bytes, 0o600)
	if err != nil {
		return err
	}

	w.Position = batch.Records[batch.Written(refused)-1].Position

	return refused
}

// LastPosition returns the last persisted position
func (w *Local) LastPosition() opencdc.Position {
	return w.Position
}

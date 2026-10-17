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
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-s3/config"
	"github.com/matryer/is"
)

// failingIterator stands in for the CDC/snapshot iterator whose background
// goroutine failed: HasNext reports true and Next returns the goroutine's
// error, which is how those errors reach Read.
type failingIterator struct{ err error }

func (f failingIterator) HasNext(context.Context) bool { return true }
func (f failingIterator) Next(context.Context) (opencdc.Record, error) {
	return opencdc.Record{}, f.err
}
func (f failingIterator) Stop() {}

func TestSource_ReadScrubsIteratorErrorsUnderAmbientCredentials(t *testing.T) {
	iterErr := errors.New("operation error S3: ListObjectsV2, AccessDenied: User: arn:aws:sts::123456789012:assumed-role/ConduitRole/i-0abc is not authorized")

	t.Run("ambient credentials: scrubbed", func(t *testing.T) {
		is := is.New(t)
		s := &Source{iterator: failingIterator{iterErr}, auth: config.NewAuth(true)}
		_, err := s.Read(context.Background())
		is.True(err != nil)
		is.True(!strings.Contains(err.Error(), "123456789012"))
		is.True(errors.Is(err, iterErr)) // classification still works
	})
	t.Run("static credentials: unchanged", func(t *testing.T) {
		is := is.New(t)
		s := &Source{iterator: failingIterator{iterErr}, auth: config.NewAuth(false)}
		_, err := s.Read(context.Background())
		is.Equal(err, iterErr)
	})
}

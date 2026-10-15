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

package iterator

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/conduitio/conduit-connector-s3/source/position"
	"github.com/matryer/is"
)

// handoffStore serves the same objects to ListObjectsV2 (the snapshot),
// ListObjectVersions (CDC) and GetObject, the way MinIO does: the listings
// carry milliseconds, GetObject's Last-Modified header whole seconds.
type handoffStore struct {
	fakeStore
	mu        sync.Mutex
	objects   map[string]handoffObject
	versioned bool
}

type handoffObject struct {
	modified time.Time
	etag     string
	version  string
}

func newHandoffStore(versioned bool) *handoffStore {
	st := &handoffStore{objects: map[string]handoffObject{}, versioned: versioned}
	st.handler = st.serve
	return st
}

func (s *handoffStore) put(key string, modified time.Time, etag string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v := "null"
	if s.versioned {
		v = fmt.Sprintf("v-%s-%d", key, modified.UnixNano())
	}
	s.objects[key] = handoffObject{modified: modified, etag: etag, version: v}
}

func (s *handoffStore) serve(r *http.Request) *http.Response {
	s.mu.Lock()
	defer s.mu.Unlock()
	keys := make([]string, 0, len(s.objects))
	for k := range s.objects {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	const ms = "2006-01-02T15:04:05.000Z"
	switch {
	case isList(r):
		var b strings.Builder
		b.WriteString(`<?xml version="1.0"?><ListBucketResult><Name>b</Name><IsTruncated>false</IsTruncated>`)
		for _, k := range keys {
			o := s.objects[k]
			fmt.Fprintf(&b, `<Contents><Key>%s</Key><LastModified>%s</LastModified><ETag>%s</ETag><Size>1</Size></Contents>`,
				k, o.modified.UTC().Format(ms), o.etag)
		}
		b.WriteString(`</ListBucketResult>`)
		return xmlResp(200, b.String())
	case r.URL.Query().Has("versions"):
		var b strings.Builder
		b.WriteString(`<?xml version="1.0"?><ListVersionsResult><IsTruncated>false</IsTruncated>`)
		for _, k := range keys {
			o := s.objects[k]
			fmt.Fprintf(&b, `<Version><Key>%s</Key><VersionId>%s</VersionId><IsLatest>true</IsLatest><LastModified>%s</LastModified><ETag>%s</ETag></Version>`,
				k, o.version, o.modified.UTC().Format(ms), o.etag)
		}
		b.WriteString(`</ListVersionsResult>`)
		return xmlResp(200, b.String())
	default:
		k := keyOf(r)
		o := s.objects[k]
		h := http.Header{
			"Content-Type":   {"text/plain"},
			"Last-Modified":  {o.modified.UTC().Format(http.TimeFormat)}, // whole seconds
			"Etag":           {o.etag},
			"Content-Length": {fmt.Sprint(len(k))},
		}
		if s.versioned {
			h.Set("X-Amz-Version-Id", o.version)
		}
		return &http.Response{StatusCode: 200, Header: h, Body: io.NopCloser(strings.NewReader(k))}
	}
}

// readFor reads from the combined iterator for d and counts the keys.
func readFor(ctx context.Context, c *CombinedIterator, d time.Duration) map[string]int {
	seen := map[string]int{}
	for deadline := time.Now().Add(d); time.Now().Before(deadline); {
		if !c.HasNext(ctx) {
			time.Sleep(2 * time.Millisecond)
			continue
		}
		rctx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		rec, err := c.Next(rctx)
		cancel()
		if err == nil {
			seen[string(rec.Key.Bytes())]++
		}
	}
	return seen
}

// TestCombinedIterator_HandoffDoesNotRedeliverTheSnapshot is the regression
// test for #1040: an object the snapshot delivered was delivered again by CDC
// at the handoff, because CDC starts at (snapshot start - lookback) with an
// empty dedupe set. Nothing failed, so each object must arrive once.
func TestCombinedIterator_HandoffDoesNotRedeliverTheSnapshot(t *testing.T) {
	for _, versioned := range []bool{false, true} {
		t.Run(fmt.Sprintf("versioned=%v", versioned), func(t *testing.T) {
			is := is.New(t)
			ctx := context.Background()
			st := newHandoffStore(versioned)
			now := time.Now().Truncate(time.Millisecond)
			st.put("a", now.Add(-2*time.Second+123*time.Millisecond), `"e-a"`)
			st.put("b", now.Add(-time.Second+794*time.Millisecond), `"e-b"`)
			st.put("c", now.Add(-500*time.Millisecond), `"e-c"`)

			c, err := NewCombinedIterator(ctx, "b", "", 5*time.Millisecond, 5*time.Minute, st.client(), position.Position{})
			is.NoErr(err)
			t.Cleanup(c.Stop)

			seen := readFor(ctx, c, 500*time.Millisecond)
			is.Equal(seen, map[string]int{"a": 1, "b": 1, "c": 1})
			is.True(st.count("versions") > 3) // CDC polled several times after the handoff
		})
	}
}

// The seed must only suppress the exact versions the snapshot delivered: an
// object overwritten after the snapshot read it is a change and is delivered.
func TestCombinedIterator_HandoffStillDeliversLaterOverwrites(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	st := newHandoffStore(false)
	now := time.Now().Truncate(time.Millisecond)
	st.put("a", now.Add(-time.Second), `"e-a"`)
	st.put("b", now.Add(-time.Second), `"e-b"`)

	c, err := NewCombinedIterator(ctx, "b", "", 5*time.Millisecond, 5*time.Minute, st.client(), position.Position{})
	is.NoErr(err)
	t.Cleanup(c.Stop)

	is.Equal(readFor(ctx, c, 200*time.Millisecond), map[string]int{"a": 1, "b": 1})
	st.put("b", now.Add(10*time.Millisecond), `"e-b2"`) // overwritten after the snapshot
	is.Equal(readFor(ctx, c, 300*time.Millisecond), map[string]int{"b": 1})
}

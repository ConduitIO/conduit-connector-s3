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
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/conduitio/conduit-connector-s3/internal/logsafe"
	"github.com/matryer/is"
	"github.com/rs/zerolog"
)

func upload(key string, at time.Time) types.MultipartUpload {
	return types.MultipartUpload{Key: aws.String(key), Initiated: aws.Time(at)}
}

// The warning comes after the hold lasted more than 15 minutes, names the key
// and the age, and repeats at most once per key per 15 minutes.
func TestHoldWarner(t *testing.T) {
	is := is.New(t)
	var buf bytes.Buffer
	log := zerolog.New(&buf)
	h := newHoldWarner()
	a, b := upload("a", tNow), upload("b", tNow)
	poll := func(at time.Duration, ups ...types.MultipartUpload) {
		h.begin()
		for _, u := range ups {
			h.hold(tNow.Add(at), u, &log)
		}
		h.end(tNow.Add(at))
	}
	lines := func() int { return strings.Count(buf.String(), "\n") }

	poll(0, a)
	poll(15*time.Minute, a)
	is.Equal(lines(), 0) // exactly 15 minutes: not yet
	poll(16*time.Minute, a, b)
	is.Equal(lines(), 1) // a only: b is first seen now
	is.True(strings.Contains(buf.String(), `"key_hash":"`+logsafe.KeyHash([]byte("a"))+`"`))
	is.True(strings.Contains(buf.String(), `"hold_age":960000`)) // 16m in ms
	poll(30*time.Minute, a, b)
	is.Equal(lines(), 1) // a warned 14m ago, b is 14m old
	poll(31*time.Minute, a, b)
	is.Equal(lines(), 2) // a again, 15m after the first; b is 15m old
	poll(32*time.Minute, a, b)
	is.Equal(lines(), 3)    // b, older than 15m now
	poll(33 * time.Minute)  // both holds end
	poll(34*time.Minute, a) // a holds again: its age starts over
	poll(40*time.Minute, a)
	is.Equal(lines(), 3)
}

// End to end: two uploads of one key in one second hold the cut for 45
// minutes; the source warns twice for the key, 15 minutes apart, and the hold
// does not lose the objects.
func TestCDC_SameSecondHoldWarnsEvery15Minutes(t *testing.T) {
	is := is.New(t)
	m := newAWSModel(tNow, false)
	d := newDriver(t, m, CDCConfig{}, startState(m))
	var buf bytes.Buffer
	log := zerolog.New(&buf)
	d.it = newCDCIterator(d.cfg, m.client(), startState(m), &log)
	u1 := m.startUpload("k")
	m.advance(300 * time.Millisecond)
	u2 := m.startUpload("k")
	for i := 0; i < 45; i++ {
		m.advance(time.Minute)
		d.poll()
	}
	out := buf.String()
	is.Equal(strings.Count(out, "s3.source.cut_held_same_second"), 2)
	is.True(strings.Contains(out, `"key_hash":"`+logsafe.KeyHash([]byte("k"))+`"`))
	m.complete(u1)
	m.complete(u2)
	for i := 0; i < 12; i++ {
		m.advance(time.Minute)
		d.poll()
	}
	is.Equal(len(d.finish()), 1) // the final version, once
}

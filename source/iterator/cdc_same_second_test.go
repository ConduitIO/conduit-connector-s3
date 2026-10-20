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
	"fmt"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/matryer/is"
)

// stampModes are the two ways a store stamps the object of a completed
// multipart upload: AWS with the initiation time at one-second resolution,
// MinIO with the completion time.
var stampModes = []struct {
	name  string
	minio bool
}{{"aws", false}, {"minio", true}}

func countBody(rs []opencdc.Record, b string) int {
	n := 0
	for _, r := range rs {
		if body(r) == b {
			n++
		}
	}
	return n
}

// An upload is initiated in the second of a version of its key that a
// previous upload completed (AWS stamps both with that second; MinIO stamped
// the first one at its completion, in the same second). Matching tracked
// uploads by key and second let that earlier version take the upload's entry:
// it was delivered again on every poll while the upload ran (the entry was
// re-created each time), and the entry was gone when the upload completed, so
// on AWS the final version, stamped below S, was never delivered.
func TestCDC_UploadInSecondOfEarlierVersion(t *testing.T) {
	for _, mode := range stampModes {
		for _, versioned := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/versioned=%v", mode.name, versioned), func(t *testing.T) {
				is := is.New(t)
				m := newAWSModel(tNow, versioned)
				m.minio = mode.minio
				d := newDriver(t, m, CDCConfig{}, startState(m))
				u0 := m.startUpload("k")
				m.advance(100 * time.Millisecond)
				v0 := m.complete(u0)
				m.advance(200 * time.Millisecond)
				u1 := m.startUpload("k") // same second as v0
				m.advance(2 * time.Second)
				d.poll() // v0 is delivered (early)
				for i := 0; i < 20; i++ {
					m.advance(time.Minute)
					d.poll() // u1 outlives the lookback
					if i == 10 {
						d.kill(len(d.run))
					}
				}
				v1 := m.complete(u1)
				for i := 0; i < 12; i++ {
					m.advance(time.Minute)
					d.poll()
					if i == 3 {
						d.kill(len(d.run) / 2)
					}
				}
				all := d.finish()
				requireExactlyOnce(t, all, m.deletes)
				is.Equal(body(all[len(all)-1]), v1.body) // the final version is delivered
				is.Equal(countBody(all, v1.body), 1)
				is.Equal(countBody(all, v0.body), 1)
			})
		}
	}
}

// Two uploads of one key initiated in the same second: the entry of one must
// not be spent on the object of the other.
func TestCDC_TwoUploadsOfAKeyInOneSecond(t *testing.T) {
	for _, mode := range stampModes {
		for _, versioned := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/versioned=%v", mode.name, versioned), func(t *testing.T) {
				is := is.New(t)
				m := newAWSModel(tNow, versioned)
				m.minio = mode.minio
				d := newDriver(t, m, CDCConfig{}, startState(m))
				u1 := m.startUpload("k")
				m.advance(300 * time.Millisecond)
				u2 := m.startUpload("k")
				for i := 0; i < 15; i++ {
					m.advance(time.Minute)
					d.poll()
				}
				v2 := m.complete(u2) // the later upload completes first
				m.advance(time.Minute)
				d.poll()
				d.kill(len(d.run))
				for i := 0; i < 3; i++ {
					m.advance(time.Minute)
					d.poll()
				}
				v1 := m.complete(u1) // and the earlier one is the final version
				for i := 0; i < 12; i++ {
					m.advance(time.Minute)
					d.poll()
				}
				all := d.finish()
				is.Equal(body(all[len(all)-1]), v1.body) // the final version is delivered
				requireExactlyOnce(t, all, m.deletes)
				is.True(countBody(all, v2.body) <= 1)
				is.Equal(len(d.it.State().Tracked), 0)
			})
		}
	}
}

// An upload that completes between the uploads listing and a later page of the
// versions listing is delivered as an ordinary version, below the cut, while
// its entry is in R. The entry must go with that delivery: once S passes the
// version, the entry matched it again.
func TestCDC_UploadCompletesBetweenTheListings(t *testing.T) {
	is := is.New(t)
	m := newAWSModel(tNow, false)
	d := newDriver(t, m, CDCConfig{}, startState(m))
	m.put("a", 0)
	m.put("b", 0)
	u := m.startUpload("z")
	m.advance(lb + time.Minute)
	m.pageSize = 1
	m.onVersionsPage = func(n int) {
		if n == 1 {
			m.complete(u) // after page 1, before page 2 holds z
		}
	}
	d.poll()
	m.onVersionsPage = nil
	m.pageSize = 1000
	for i := 0; i < 4; i++ {
		m.advance(time.Minute)
		d.poll()
	}
	all := d.finish()
	requireExactlyOnce(t, all, m.deletes)
	is.Equal(len(all), 3)
	is.Equal(len(d.it.State().Tracked), 0)
}

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
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/matryer/is"
	"github.com/rs/zerolog"
)

func TestClockGuard_StoreNow(t *testing.T) {
	host := time.Date(2026, 10, 12, 12, 0, 0, 0, time.UTC)
	hdr := func(d time.Duration) string { return host.Add(d).Format(http.TimeFormat) }

	newGuard := func(lookback time.Duration) (*clockGuard, *bytes.Buffer) {
		var buf bytes.Buffer
		l := zerolog.New(&buf)
		return &clockGuard{log: &l, lookback: lookback}, &buf
	}

	t.Run("store clock is used and agreeing clocks are silent", func(t *testing.T) {
		is := is.New(t)
		g, buf := newGuard(5 * time.Minute)
		is.Equal(g.storeNow(host, hdr(-time.Second)), host.Add(-time.Second))
		is.Equal(buf.Len(), 0)
	})
	t.Run("skew over half the lookback warns once a minute", func(t *testing.T) {
		is := is.New(t)
		g, buf := newGuard(5 * time.Minute)
		got := g.storeNow(host, hdr(-10*time.Minute))
		is.Equal(got, host.Add(-10*time.Minute)) // the store's clock wins
		is.True(strings.Contains(buf.String(), "differs from the object store's"))
		g.storeNow(host.Add(time.Second), hdr(-10*time.Minute))
		is.Equal(strings.Count(buf.String(), "differs from the object store's"), 1) // rate limited
		g.storeNow(host.Add(2*time.Minute), hdr(-10*time.Minute))
		is.Equal(strings.Count(buf.String(), "differs from the object store's"), 2)
	})
	t.Run("missing or bad header falls back to the host clock with a warning", func(t *testing.T) {
		is := is.New(t)
		for _, h := range []string{"", "not a date"} {
			g, buf := newGuard(5 * time.Minute)
			is.Equal(g.storeNow(host, h), host)
			is.True(strings.Contains(buf.String(), "no usable Date header"))
		}
	})
}

// TestResolveStoreTime: the Date header comes from whatever endpoint is
// configured, so it is validated, and it can only ever lower the watermark.
func TestResolveStoreTime(t *testing.T) {
	host := time.Date(2026, 10, 12, 12, 0, 0, 0, time.UTC)
	hdr := func(d time.Duration) string { return host.Add(d).Format(http.TimeFormat) }

	for _, tc := range []struct {
		name   string
		header string
		want   time.Time
		wantOK bool
	}{
		{"store behind the host is used", hdr(-time.Hour), host.Add(-time.Hour), true},
		{"store slightly ahead is capped at the host", hdr(2 * time.Second), host, true},
		{"store 2h ahead is capped at the host", hdr(2 * time.Hour), host, true},
		{"far future is ignored", hdr(30 * 24 * time.Hour), host, false},
		{"far past is ignored", hdr(-30 * 24 * time.Hour), host, false},
		{"just beyond the bound is ignored", hdr(maxStoreClockDrift + time.Minute), host, false},
		{"garbage is ignored", "soon", host, false},
		{"missing is ignored", "", host, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			got, ok := resolveStoreTime(host, tc.header)
			is.Equal(got, tc.want)
			is.Equal(ok, tc.wantOK)
			is.True(!got.After(host)) // never above the host clock: cannot skip objects
		})
	}
}

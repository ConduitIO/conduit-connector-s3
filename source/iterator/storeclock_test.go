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
	"errors"
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
		got, adv, err := g.storeNow(host, hdr(-time.Second))
		is.NoErr(err)
		is.True(adv)
		is.Equal(got, host.Add(-time.Second))
		is.Equal(buf.Len(), 0)
	})
	t.Run("skew over half the lookback warns once a minute", func(t *testing.T) {
		is := is.New(t)
		g, buf := newGuard(5 * time.Minute)
		got, _, _ := g.storeNow(host, hdr(-10*time.Minute))
		is.Equal(got, host.Add(-10*time.Minute)) // the store's clock wins
		is.True(strings.Contains(buf.String(), "differs from the object store's"))
		_, _, _ = g.storeNow(host.Add(time.Second), hdr(-10*time.Minute))
		is.Equal(strings.Count(buf.String(), "differs from the object store's"), 1) // rate limited
		_, _, _ = g.storeNow(host.Add(2*time.Minute), hdr(-10*time.Minute))
		is.Equal(strings.Count(buf.String(), "differs from the object store's"), 2)
	})
	t.Run("a lost header reuses the last good offset", func(t *testing.T) {
		is := is.New(t)
		g, _ := newGuard(5 * time.Minute)
		_, _, _ = g.storeNow(host, hdr(-10*time.Minute)) // host is 10m ahead
		later := host.Add(time.Minute)
		got, adv, err := g.storeNow(later, "")
		is.NoErr(err)
		is.True(adv)
		is.Equal(got, later.Add(-10*time.Minute)) // not the host clock
		got, adv, _ = g.storeNow(later, "not a date")
		is.True(adv)
		is.Equal(got, later.Add(-10*time.Minute))
	})
	t.Run("no offset ever: hold, then fail with a stable code, never the host clock", func(t *testing.T) {
		is := is.New(t)
		g, _ := newGuard(5 * time.Minute)
		var err error
		for i := 1; i <= maxBadDatePolls; i++ {
			var adv bool
			_, adv, err = g.storeNow(host, "")
			is.True(!adv) // held, the host clock is never used
			if i < maxBadDatePolls {
				is.NoErr(err)
			}
		}
		is.True(errors.Is(err, ErrStoreClockUnavailable))
		is.True(strings.Contains(err.Error(), "s3.source.store_clock_unavailable"))
	})
	t.Run("trustHostClock is the explicit opt-in to the host clock", func(t *testing.T) {
		is := is.New(t)
		g, buf := newGuard(5 * time.Minute)
		g.trustHost = true
		got, adv, err := g.storeNow(host, "")
		is.NoErr(err)
		is.True(adv)
		is.Equal(got, host)
		is.True(strings.Contains(buf.String(), "trustHostClock is set"))
	})
	t.Run("a single in-bound outlier cannot raise the watermark", func(t *testing.T) {
		is := is.New(t)
		g, _ := newGuard(5 * time.Minute)
		_, _, _ = g.storeNow(host, hdr(-10*time.Minute))           // true offset: 10m
		got, adv, err := g.storeNow(host.Add(time.Second), hdr(0)) // outlier: claims store == host
		is.NoErr(err)
		is.True(adv)
		is.Equal(got, host.Add(time.Second).Add(-10*time.Minute)) // still the lowest plausible time
	})
	t.Run("an outlier that claims an earlier time only lowers it, and ages out", func(t *testing.T) {
		is := is.New(t)
		g, _ := newGuard(5 * time.Minute)
		_, _, _ = g.storeNow(host, hdr(-time.Second))
		got, _, _ := g.storeNow(host, hdr(-20*time.Hour)) // in bound, absurd
		is.Equal(got, host.Add(-20*time.Hour))            // more redelivery, not a skip
		later := host.Add(offsetWindow + 2*time.Minute)
		got, _, _ = g.storeNow(later, later.Add(-time.Second).Format(http.TimeFormat))
		is.Equal(got, later.Add(-time.Second)) // the window moved on
	})
	t.Run("alternating good and bad headers still trip the breaker", func(t *testing.T) {
		is := is.New(t)
		g, _ := newGuard(5 * time.Minute)
		var err error
		for i := 0; i < 2*maxBadDatePolls && err == nil; i++ {
			_, _, _ = g.storeNow(host, hdr(-time.Second))
			_, _, err = g.storeNow(host, hdr(-30*time.Hour))
		}
		is.True(errors.Is(err, ErrStoreClockOutOfRange))
	})
	t.Run("out of range holds, then fails loudly naming both clocks", func(t *testing.T) {
		is := is.New(t)
		g, _ := newGuard(5 * time.Minute)
		var err error
		for i := 1; i <= maxBadDatePolls; i++ {
			var adv bool
			_, adv, err = g.storeNow(host, hdr(-30*time.Hour))
			is.True(!adv)
			if i < maxBadDatePolls {
				is.NoErr(err)
			}
		}
		is.True(errors.Is(err, ErrStoreClockOutOfRange))
		msg := err.Error()
		is.True(strings.Contains(msg, "s3.source.store_clock_out_of_range"))
		is.True(strings.Contains(msg, host.UTC().Format(time.RFC3339)))
		is.True(strings.Contains(msg, host.Add(-30*time.Hour).UTC().Format(time.RFC3339)))
	})
	t.Run("out of range never falls back to the host clock even with an offset", func(t *testing.T) {
		is := is.New(t)
		g, _ := newGuard(5 * time.Minute)
		_, _, _ = g.storeNow(host, hdr(-10*time.Minute))
		_, adv, err := g.storeNow(host, hdr(-30*time.Hour))
		is.NoErr(err)
		is.True(!adv)
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

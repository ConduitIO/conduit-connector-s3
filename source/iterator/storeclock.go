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
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/rs/zerolog"
)

// The CDC watermark is compared with LastModified, which is on the store's
// clock. Taking "now" from the host clock alone would make every new object
// look old when the host runs ahead of the store by more than the lookback. So
// "now" is read from the store: the Date header of the response to the poll's
// first listing call. The header has one-second resolution, which is harmless:
// it is truncated, i.e. conservative.
//
// The header comes from whatever endpoint is configured, so it is never
// trusted blindly (invariant 3, a wrong watermark skips objects for good), and
// the watermark is never moved on a reading that could be too high:
//
//   - A valid header (parsed, within maxStoreClockDrift of the host) gives an
//     offset host-store. The offset used is the LARGEST one seen in the last
//     offsetWindow, never less than 0, so now = host - offset is the lowest
//     plausible store time. A single in-bound outlier that claims a later store
//     time cannot raise the watermark; one that claims an earlier time can only
//     lower it (more redelivery).
//   - Missing or unparsable: the same estimate from the remembered offsets. If
//     no valid header was ever seen, the watermark is held (objects are still
//     listed and delivered) and after maxBadDatePolls such polls within
//     badPollWindow the iterator fails with ErrStoreClockUnavailable. The host
//     clock alone is used only when the operator opted in (trustHostClock).
//   - Outside maxStoreClockDrift: nothing can be assumed. Held, and failed with
//     ErrStoreClockOutOfRange when maxBadDatePolls of the last badPollWindow
//     polls were like that (a sliding window, not consecutive, so alternating
//     good and bad headers cannot stay under the threshold).
//
// What cannot be defended against: an endpoint that serves consistently false
// but in-bound Date headers for longer than offsetWindow.

// skewWarnInterval limits the clock-skew and missing-header warnings.
const skewWarnInterval = time.Minute

// maxStoreClockDrift is how far the Date header may be from this host's clock
// and still be believed. The header comes from whatever endpoint is configured
// (a proxy, an S3-compatible store); a far-future value would push the
// watermark past objects that were never delivered (invariant 3). Outside the
// bound the header is ignored.
const maxStoreClockDrift = 24 * time.Hour

// maxBadDatePolls bad polls within the last badPollWindow polls trip the
// circuit breaker.
const (
	maxBadDatePolls = 10
	badPollWindow   = 60
	// offsetWindow is how long an observed host-store offset keeps counting.
	offsetWindow = time.Hour
)

// maxDeliveredEntries caps the in-memory set of already delivered versions
// (those modified within the lookback window, or since the watermark when it is
// held). Evicting entries would turn into duplicates or, done wrong, skips, so
// when the cap is reached the iterator fails instead. A variable so tests can
// lower it.
var maxDeliveredEntries = 1_000_000

// ErrStoreClockOutOfRange is the stable error code of an iterator that stopped
// because the object store's Date header and this host's clock are more than
// 24h apart, so no watermark can be derived safely. Fix the host clock, or the
// endpoint if it is a proxy that rewrites headers.
var ErrStoreClockOutOfRange = errors.New("s3.source.store_clock_out_of_range")

// ErrStoreClockUnavailable is the stable error code of an iterator that
// stopped because the object store never sent a usable Date header, so the
// store's clock is unknown. Set trustHostClock only if the clock of the host
// running Conduit is known to agree with the store's to within lookbackPeriod.
var ErrStoreClockUnavailable = errors.New("s3.source.store_clock_unavailable")

// ErrDedupeWindowOverflow is the stable error code of an iterator that stopped
// because more objects than maxDeliveredEntries were modified within the
// window it must remember.
var ErrDedupeWindowOverflow = errors.New("s3.source.dedupe_window_overflow")

// DedupeWindowOverflowError carries the numbers.
type DedupeWindowOverflowError struct{ Entries int }

func (e *DedupeWindowOverflowError) Error() string {
	return fmt.Sprintf("%s: %d object versions modified within the CDC lookback window exceed the limit of %d that can be tracked in memory; "+
		"reduce lookbackPeriod, or check the store's Date header if the watermark is being held",
		ErrDedupeWindowOverflow.Error(), e.Entries, maxDeliveredEntries)
}

func (e *DedupeWindowOverflowError) Is(target error) bool { return target == ErrDedupeWindowOverflow }

// StoreClockOutOfRangeError carries both clocks.
type StoreClockOutOfRangeError struct {
	Host, Store time.Time
	Polls       int
}

func (e *StoreClockOutOfRangeError) Error() string {
	return fmt.Sprintf("%s: the object store's Date header (%s) is more than %s away from this host's clock (%s) in %d of the last polls; "+
		"no safe CDC watermark can be derived. Fix the clock of the host running Conduit, or check the endpoint (aws.endpoint) if it rewrites headers",
		ErrStoreClockOutOfRange.Error(), e.Store.UTC().Format(time.RFC3339), maxStoreClockDrift, e.Host.UTC().Format(time.RFC3339), e.Polls)
}

func (e *StoreClockOutOfRangeError) Is(target error) bool { return target == ErrStoreClockOutOfRange }

// captureDate returns a request option that stores the Date response header in
// *dst.
func captureDate(dst *string) func(*s3.Options) {
	return func(o *s3.Options) {
		o.APIOptions = append(o.APIOptions, func(stack *middleware.Stack) error {
			return stack.Deserialize.Add(middleware.DeserializeMiddlewareFunc("captureDate",
				func(ctx context.Context, in middleware.DeserializeInput, next middleware.DeserializeHandler) (
					middleware.DeserializeOutput, middleware.Metadata, error,
				) {
					out, md, err := next.HandleDeserialize(ctx, in)
					if resp, ok := out.RawResponse.(*smithyhttp.Response); ok && *dst == "" {
						*dst = resp.Header.Get("Date")
					}
					return out, md, err
				}), middleware.After)
		})
	}
}

// parseStoreDate parses an HTTP Date header.
func parseStoreDate(h string) (time.Time, bool) {
	if h == "" {
		return time.Time{}, false
	}
	t, err := http.ParseTime(h)
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}

// headerState classifies a Date header against the host clock.
type headerState int

const (
	headerOK         headerState = iota // parsed and within maxStoreClockDrift
	headerUnusable                      // missing or unparsable
	headerOutOfRange                    // parsed but too far from the host clock
)

// classifyStoreDate parses the header and says whether it can be believed.
func classifyStoreDate(hostNow time.Time, header string) (time.Time, headerState) {
	d, ok := parseStoreDate(header)
	if !ok {
		return time.Time{}, headerUnusable
	}
	if diff := d.Sub(hostNow); diff > maxStoreClockDrift || -diff > maxStoreClockDrift {
		return d, headerOutOfRange
	}
	return d, headerOK
}

// resolveStoreTime returns min(hostNow, store Date) when the header is valid,
// and ok=false otherwise (hostNow is returned and must not be relied on).
// Used where there is no history to fall back on (the snapshot's start).
func resolveStoreTime(hostNow time.Time, header string) (t time.Time, ok bool) {
	d, st := classifyStoreDate(hostNow, header)
	if st != headerOK {
		return hostNow, false
	}
	if d.Before(hostNow) {
		return d, true
	}
	return hostNow, true
}

// clockGuard turns Date headers into store-clock timestamps for CDC polls,
// remembering what it learned, and warns, rate-limited.
type clockGuard struct {
	log        *zerolog.Logger
	lookback   time.Duration
	trustHost  bool
	lastWarned time.Time

	samples []offsetSample // largest offset per minute, oldest first
	bad     [badPollWindow]bool
	badNext int
	warned  bool // the one-time "trusting the host clock" WARN was given
}

type offsetSample struct {
	minute int64
	offset time.Duration
}

func (g *clockGuard) warnAllowed(now time.Time) bool {
	if !g.lastWarned.IsZero() && now.Sub(g.lastWarned) < skewWarnInterval {
		return false
	}
	g.lastWarned = now
	return true
}

func (g *clockGuard) record(bad bool) int {
	g.bad[g.badNext] = bad
	g.badNext = (g.badNext + 1) % badPollWindow
	n := 0
	for _, b := range g.bad {
		if b {
			n++
		}
	}
	return n
}

func (g *clockGuard) addOffset(now time.Time, offset time.Duration) {
	m := now.Unix() / 60
	if n := len(g.samples); n > 0 && g.samples[n-1].minute == m {
		if offset > g.samples[n-1].offset {
			g.samples[n-1].offset = offset
		}
	} else {
		g.samples = append(g.samples, offsetSample{m, offset})
	}
	// drop samples older than the window, but always keep the newest one
	for len(g.samples) > 1 && m-g.samples[0].minute > int64(offsetWindow/time.Minute) {
		g.samples = g.samples[1:]
	}
}

// maxOffset is the largest host-store offset seen in the window, at least 0.
func (g *clockGuard) maxOffset() time.Duration {
	var m time.Duration
	for _, s := range g.samples {
		if s.offset > m {
			m = s.offset
		}
	}
	return m
}

// storeNow decides what the watermark may do after a poll that started at
// hostNow and whose first response carried header. It returns the store-clock
// time to advance to, whether the watermark may advance at all, and an error
// when the iterator must stop.
func (g *clockGuard) storeNow(hostNow time.Time, header string) (t time.Time, advance bool, err error) {
	d, st := classifyStoreDate(hostNow, header)
	switch st {
	case headerOK:
		g.record(false)
		offset := hostNow.Sub(d)
		g.addOffset(hostNow, offset)
		if g.lookback > 0 {
			if skew := offset; (skew > g.lookback/2 || -skew > g.lookback/2) && g.warnAllowed(hostNow) {
				g.log.Warn().Dur("host_minus_store", skew).Dur("lookbackPeriod", g.lookback).
					Msg("the clock of this host differs from the object store's by more than half of lookbackPeriod; the lowest plausible store time is used for the CDC watermark, but fix the clocks")
			}
		}
		return hostNow.Add(-g.maxOffset()), true, nil

	case headerOutOfRange:
		if n := g.record(true); n >= maxBadDatePolls {
			return time.Time{}, false, &StoreClockOutOfRangeError{Host: hostNow, Store: d, Polls: n}
		}
		if g.warnAllowed(hostNow) {
			g.log.Warn().Time("store_date", d).Time("host", hostNow).
				Msg("the object store's Date header is more than 24h away from this host's clock; the CDC watermark is held (objects are still delivered) and the iterator fails if it persists")
		}
		return time.Time{}, false, nil

	default: // headerUnusable
		if len(g.samples) > 0 {
			// the lowest plausible store time, from what was learned before
			if g.warnAllowed(hostNow) {
				g.log.Warn().Str("date_header", header).
					Msg("the object store sent no usable Date header; estimating its clock from earlier ones")
			}
			return hostNow.Add(-g.maxOffset()), true, nil
		}
		if g.trustHost {
			if !g.warned {
				g.warned = true
				g.log.Warn().
					Msg("trustHostClock is set and the object store sends no usable Date header: using this host's clock for the CDC watermark, which loses objects if it is ahead of the store's by more than lookbackPeriod")
			}
			return hostNow, true, nil
		}
		if n := g.record(true); n >= maxBadDatePolls {
			return time.Time{}, false, fmt.Errorf("%w: the object store sent no usable Date header in %d of the last polls, so its clock is unknown and the CDC watermark would be a guess; "+
				"use a store that sends Date, or set trustHostClock only if this host's clock agrees with the store's to within lookbackPeriod", ErrStoreClockUnavailable, n)
		}
		return time.Time{}, false, nil // hold the watermark
	}
}

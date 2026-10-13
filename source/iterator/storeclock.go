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
// trusted blindly (invariant 3, a wrong watermark skips objects for good):
//
//   - Valid and within maxStoreClockDrift of the host: now = min(host, store).
//     The header can only lower the watermark, never raise it above the host.
//     The offset host-store is remembered.
//   - Missing or unparsable: now = host - last good offset (the store's clock,
//     estimated). With no good offset ever seen, the watermark is held where it
//     is (objects are still listed and delivered, nothing is skipped); only
//     after maxUnusableDatePolls consecutive such polls, i.e. a store that
//     evidently never sends Date, does the host clock advance it, with one
//     WARN.
//   - Outside maxStoreClockDrift of the host: either the store or the host
//     clock is badly wrong and nothing can be assumed. The watermark is held,
//     and after maxUnusableDatePolls consecutive polls the iterator fails with
//     ErrStoreClockOutOfRange rather than guess.

// skewWarnInterval limits the clock-skew and missing-header warnings.
const skewWarnInterval = time.Minute

// maxStoreClockDrift is how far the Date header may be from this host's clock
// and still be believed. The header comes from whatever endpoint is configured
// (a proxy, an S3-compatible store); a far-future value would push the
// watermark past objects that were never delivered (invariant 3). Outside the
// bound the header is ignored.
const maxStoreClockDrift = 24 * time.Hour

// maxUnusableDatePolls is how many consecutive polls with a missing,
// unparsable or out-of-range Date header are tolerated (watermark held) before
// the iterator concludes that the store never sends one (host clock is used,
// WARN) or that the clocks are irreconcilable (the iterator fails).
const maxUnusableDatePolls = 10

// ErrStoreClockOutOfRange is the stable error code of an iterator that stopped
// because the object store's Date header and this host's clock are more than
// 24h apart, so no watermark can be derived safely. Fix the host clock, or the
// endpoint if it is a proxy that rewrites headers.
var ErrStoreClockOutOfRange = errors.New("s3.source.store_clock_out_of_range")

// StoreClockOutOfRangeError carries both clocks.
type StoreClockOutOfRangeError struct {
	Host, Store time.Time
	Polls       int
}

func (e *StoreClockOutOfRangeError) Error() string {
	return fmt.Sprintf("%s: the object store's Date header (%s) is more than %s away from this host's clock (%s) in %d consecutive polls; "+
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
	lastWarned time.Time

	haveOffset bool          // a valid header was seen
	offset     time.Duration // host minus store at that time
	unusable   int           // consecutive polls with a missing/unparsable header
	outOfRange int           // consecutive polls with an out-of-range header
	hostOnly   bool          // the one-time "store sends no Date" WARN was given
}

func (g *clockGuard) warnAllowed(now time.Time) bool {
	if !g.lastWarned.IsZero() && now.Sub(g.lastWarned) < skewWarnInterval {
		return false
	}
	g.lastWarned = now
	return true
}

// storeNow decides what the watermark may do after a poll that started at
// hostNow and whose first response carried header. It returns the store-clock
// time to advance to, whether the watermark may advance at all, and an error
// when the iterator must stop.
func (g *clockGuard) storeNow(hostNow time.Time, header string) (t time.Time, advance bool, err error) {
	d, st := classifyStoreDate(hostNow, header)
	switch st {
	case headerOK:
		g.unusable, g.outOfRange = 0, 0
		g.haveOffset, g.offset = true, hostNow.Sub(d)
		if g.lookback > 0 {
			if skew := g.offset; (skew > g.lookback/2 || -skew > g.lookback/2) && g.warnAllowed(hostNow) {
				g.log.Warn().Dur("host_minus_store", skew).Dur("lookbackPeriod", g.lookback).
					Msg("the clock of this host differs from the object store's by more than half of lookbackPeriod; the earlier of the two is used for the CDC watermark, but fix the clocks")
			}
		}
		if d.Before(hostNow) {
			return d, true, nil
		}
		return hostNow, true, nil

	case headerOutOfRange:
		g.unusable = 0
		g.outOfRange++
		if g.outOfRange >= maxUnusableDatePolls {
			return time.Time{}, false, &StoreClockOutOfRangeError{Host: hostNow, Store: d, Polls: g.outOfRange}
		}
		if g.warnAllowed(hostNow) {
			g.log.Warn().Time("store_date", d).Time("host", hostNow).
				Msg("the object store's Date header is more than 24h away from this host's clock; the CDC watermark is held (objects are still delivered) and the iterator fails if it persists")
		}
		return time.Time{}, false, nil

	default: // headerUnusable
		g.outOfRange = 0
		g.unusable++
		if g.haveOffset {
			// the store's clock, estimated from the last good header
			if g.warnAllowed(hostNow) {
				g.log.Warn().Str("date_header", header).
					Msg("the object store sent no usable Date header; estimating its clock from the last good one")
			}
			return hostNow.Add(-g.offset), true, nil
		}
		if g.unusable >= maxUnusableDatePolls {
			// the store evidently does not send Date: the host clock is all
			// there is, and is only safe if it agrees with the store's to
			// within lookbackPeriod
			if !g.hostOnly {
				g.hostOnly = true
				g.log.Warn().
					Msg("the object store never sends a usable Date header; using this host's clock for the CDC watermark, which is only safe if it agrees with the store's clock to within lookbackPeriod")
			}
			return hostNow, true, nil
		}
		return time.Time{}, false, nil // hold the watermark
	}
}

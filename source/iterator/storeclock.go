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
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/rs/zerolog"
)

// The CDC watermark is compared with LastModified, which is on the store's
// clock. Taking "now" from the host clock would make every new object look old
// when the host runs ahead of the store by more than the lookback. So "now"
// is read from the store: the Date header of the response to the poll's first
// listing call. The header has one-second resolution, which is harmless: it
// is truncated, i.e. conservative.

// skewWarnInterval limits the clock-skew and missing-header warnings.
const skewWarnInterval = time.Minute

// maxStoreClockDrift is how far the Date header may be from this host's clock
// and still be believed. The header comes from whatever endpoint is configured
// (a proxy, an S3-compatible store); a far-future value would push the
// watermark past objects that were never delivered (invariant 3). Outside the
// bound the header is ignored.
const maxStoreClockDrift = 24 * time.Hour

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

// resolveStoreTime returns the time a poll or snapshot may treat as "now":
// min(hostNow, store Date). ok is false when the header is missing, unparsable
// or further than maxStoreClockDrift from hostNow, in which case hostNow is
// returned.
//
// Invariant 3, fail-safe in both directions. A host clock ahead of the store is
// corrected by the store's (earlier) date. A store date ahead of the host can
// only be a lie or a skew in the harmless direction (LastModified is on the
// store's clock, so ahead means objects look newer), and min never lets it
// raise the watermark above the host clock: the worst case is more redelivery,
// never a skipped object.
func resolveStoreTime(hostNow time.Time, header string) (t time.Time, ok bool) {
	d, parsed := parseStoreDate(header)
	if !parsed {
		return hostNow, false
	}
	if diff := d.Sub(hostNow); diff > maxStoreClockDrift || -diff > maxStoreClockDrift {
		return hostNow, false
	}
	if d.Before(hostNow) {
		return d, true
	}
	return hostNow, true
}

// clockGuard warns about clock problems, rate-limited.
type clockGuard struct {
	log        *zerolog.Logger
	lookback   time.Duration
	lastWarned time.Time
}

func (g *clockGuard) warnAllowed(now time.Time) bool {
	if !g.lastWarned.IsZero() && now.Sub(g.lastWarned) < skewWarnInterval {
		return false
	}
	g.lastWarned = now
	return true
}

// storeNow returns resolveStoreTime(hostNow, header) and warns when the header
// cannot be used (fallback to the host clock, which is only safe if it agrees
// with the store's to within lookbackPeriod) or when the two clocks differ by
// more than lookback/2.
func (g *clockGuard) storeNow(hostNow time.Time, header string) time.Time {
	t, ok := resolveStoreTime(hostNow, header)
	if !ok {
		if g.warnAllowed(hostNow) {
			g.log.Warn().Str("date_header", header).
				Msg("the object store sent no usable Date header (missing, unparsable or more than 24h from this host's clock); using this host's clock for the CDC watermark, which is only safe if it agrees with the store's clock to within lookbackPeriod")
		}
		return t
	}
	if d, _ := parseStoreDate(header); g.lookback > 0 {
		skew := hostNow.Sub(d)
		if (skew > g.lookback/2 || -skew > g.lookback/2) && g.warnAllowed(hostNow) {
			g.log.Warn().Dur("host_minus_store", skew).Dur("lookbackPeriod", g.lookback).
				Msg("the clock of this host differs from the object store's by more than half of lookbackPeriod; the earlier of the two is used for the CDC watermark, but fix the clocks")
		}
	}
	return t
}

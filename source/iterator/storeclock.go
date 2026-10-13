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

// clockGuard turns Date headers into store-clock timestamps and warns about
// skew, rate-limited.
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

// storeNow returns the store's notion of the time at which hostNow was read.
// header is the Date header of a response received right after hostNow. When it
// is missing or unparsable it falls back to the host clock and says so; when
// the two clocks differ by more than lookback/2 it warns, because beyond the
// lookback objects can be missed.
func (g *clockGuard) storeNow(hostNow time.Time, header string) time.Time {
	t, ok := parseStoreDate(header)
	if !ok {
		if g.warnAllowed(hostNow) {
			g.log.Warn().Str("date_header", header).
				Msg("the object store sent no usable Date header; using this host's clock for the CDC watermark, which is only safe if it agrees with the store's clock to within lookbackPeriod")
		}
		return hostNow
	}
	skew := hostNow.Sub(t)
	if g.lookback > 0 && (skew > g.lookback/2 || -skew > g.lookback/2) && g.warnAllowed(hostNow) {
		g.log.Warn().Dur("host_minus_store", skew).Dur("lookbackPeriod", g.lookback).
			Msg("the clock of this host differs from the object store's by more than half of lookbackPeriod; the store's clock is used for the CDC watermark, but fix the host clock")
	}
	return t
}

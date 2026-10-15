// Copyright © 2022 Meroxa, Inc.
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

package source_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/conduitio/conduit-commons/opencdc"
	s3Conn "github.com/conduitio/conduit-connector-s3"
	"github.com/conduitio/conduit-connector-s3/config"
	"github.com/conduitio/conduit-connector-s3/source"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/google/uuid"
	"github.com/matryer/is"
)

// These are the regression tests for #1029: the source must not skip objects.
// They need MINIO_ENDPOINT (see `make test-integration-s3`). The TestMinIO_ name
// prefix is what the CI selector (Makefile, test.yml) matches: name every new
// test that needs MinIO TestMinIO_<Name> or it will not run in CI.

// minioSourceEnv returns an S3 client and a source config for a fresh bucket on
// the MinIO at endpointOverride (or MINIO_ENDPOINT when empty).
func minioSourceEnv(t *testing.T, endpointOverride string) (*s3.Client, map[string]string) {
	t.Helper()
	endpoint := os.Getenv(envMinIOEndpoint)
	if endpoint == "" {
		t.Skipf("%v env var must be set", envMinIOEndpoint)
	}
	requireNoSDKEndpointEnv(t)
	if endpointOverride != "" {
		endpoint = endpointOverride
	}
	cfg := map[string]string{
		config.ConfigKeyAWSAccessKeyID:     os.Getenv("AWS_ACCESS_KEY_ID"),
		config.ConfigKeyAWSSecretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
		config.ConfigKeyAWSRegion:          os.Getenv("AWS_REGION"),
		config.ConfigKeyAWSEndpoint:        endpoint,
		config.ConfigKeyAWSPathStyle:       "true",
		source.ConfigKeyPollingPeriod:      "100ms",
	}
	// The test's own client talks to MinIO directly: a proxy that lies about
	// time would make the SDK's clock-skew correction sign this client's
	// requests with a wrong time.
	direct := map[string]string{}
	for k, v := range cfg {
		direct[k] = v
	}
	direct[config.ConfigKeyAWSEndpoint] = os.Getenv(envMinIOEndpoint)
	client := newEndpointS3Client(t, direct)
	bucket := "conduit-s3-noskip-" + uuid.NewString()
	createTestBucket(t, client, bucket)
	t.Cleanup(func() {
		clearTestBucket(t, client, bucket)
		deleteTestBucket(t, client, bucket)
	})
	cfg[config.ConfigKeyAWSBucket] = bucket
	return client, cfg
}

func openSource(ctx context.Context, t *testing.T, cfg map[string]string, pos opencdc.Position) *source.Source {
	t.Helper()
	s := &source.Source{}
	err := sdk.Util.ParseConfig(ctx, cfg, s.Config(), s3Conn.Connector.NewSpecification().SourceParams)
	if err != nil {
		t.Fatalf("parse config: %v", err)
	}
	if err := s.Open(ctx, pos); err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = s.Teardown(ctx) })
	return s
}

func putObject(ctx context.Context, t *testing.T, client *s3.Client, bucket, key string) {
	t.Helper()
	_, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   strings.NewReader(key),
	})
	if err != nil {
		t.Fatalf("put %s: %v", key, err)
	}
}

// startCDC drives an opened source over an empty bucket until it has switched
// from the (empty) snapshot to CDC: without it, an object written right after
// Open would be listed by the snapshot instead.
func startCDC(ctx context.Context, t *testing.T, s *source.Source) {
	t.Helper()
	if _, err := s.Read(ctx); !errors.Is(err, sdk.ErrBackoffRetry) {
		t.Fatalf("expected an empty snapshot (ErrBackoffRetry), got %v", err)
	}
}

// drainKeys reads records until none arrives for quiet, returning the keys seen.
func drainKeys(ctx context.Context, s *source.Source, quiet time.Duration) map[string]int {
	seen := map[string]int{}
	for {
		rec, err := readWithTimeout(ctx, s, quiet)
		if err != nil {
			return seen
		}
		seen[string(rec.Key.Bytes())]++
	}
}

// waitBudget bounds how long a test waits for a record it expects (the first
// one of a snapshot, or an object delivered late via CDC). It is only spent
// when the test fails: a delivered record ends the wait at once.
const waitBudget = 120 * time.Second

// drainUntil reads records into seen until done(seen) holds or budget runs
// out. Use it instead of drainKeys when the test waits for a specific object.
// drainKeys ends at the first quiet period, and under load the first CDC poll
// after a snapshot can take longer than any short quiet period (9s measured at
// a load average of 450), so the test would report as lost an object that was
// only late.
func drainUntil(ctx context.Context, s *source.Source, seen map[string]int, budget time.Duration, done func(map[string]int) bool) {
	deadline := time.Now().Add(budget)
	for !done(seen) && time.Now().Before(deadline) {
		rec, err := readWithTimeout(ctx, s, 5*time.Second)
		if err == nil {
			seen[string(rec.Key.Bytes())]++
			continue
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			return
		}
	}
}

// bothLate is done for the a-late/z-late tests: both injected objects arrived.
func bothLate(seen map[string]int) bool { return seen["a-late"] > 0 && seen["z-late"] > 0 }

// TestMinIO_SnapshotHandoffDoesNotSkipObjects: an object created while the
// snapshot is paginating, under a page the snapshot already passed, must not be
// lost at the snapshot-to-CDC handoff.
//
// The snapshot lists in key order, one page (1000 keys) at a time, and hands
// over to CDC at the largest LastModified it has read. "a-late" is created
// after the first page was listed and so is missing from the snapshot, but it
// is created before "z-late", which the snapshot does read and which therefore
// pushes the CDC watermark past a-late's LastModified.
func TestMinIO_SnapshotHandoffDoesNotSkipObjects(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	client, cfg := minioSourceEnv(t, "")
	bucket := cfg[config.ConfigKeyAWSBucket]

	for i := 0; i < 1001; i++ {
		putObject(ctx, t, client, bucket, fmt.Sprintf("k%04d", i))
	}

	s := openSource(ctx, t, cfg, nil)

	seen := map[string]int{}
	rec, err := readWithTimeout(ctx, s, waitBudget) // lists page 1 (k0000..k0998 + ...)
	is.NoErr(err)
	seen[string(rec.Key.Bytes())]++

	// concurrent writes during the snapshot
	putObject(ctx, t, client, bucket, "a-late")
	time.Sleep(1100 * time.Millisecond) // GetObject's LastModified has one-second resolution
	putObject(ctx, t, client, bucket, "z-late")

	drainUntil(ctx, s, seen, waitBudget, bothLate)

	t.Logf("distinct=%d a-late=%d z-late=%d first=%s", len(seen), seen["a-late"], seen["z-late"], string(rec.Key.Bytes()))
	is.True(seen["z-late"] > 0) // control: the later write is delivered
	if seen["a-late"] == 0 {
		t.Fatalf("a-late was written during the snapshot and never delivered (%d distinct keys seen)", len(seen))
	}
}

var (
	lastModifiedRe = regexp.MustCompile(`<LastModified>[^<]*</LastModified>`)
	versionRe      = regexp.MustCompile(`(?s)<Version>.*?</Version>`)
	keyRe          = regexp.MustCompile(`<Key>([^<]*)</Key>`)
)

// proxyOpts shapes a forwarding proxy in front of MinIO.
type proxyOpts struct {
	// lastModified, if set, replaces every LastModified in XML responses with
	// the returned value (RFC 3339). MinIO reports milliseconds and AWS whole
	// seconds; this makes the same-second cases reproducible on demand.
	lastModified func(orig string) string
	// shift moves the store's clock: every LastModified in XML and every Date
	// header is shifted by it (negative: the store is behind the host).
	shift time.Duration
	// date, if set, replaces the Date header of every response (an empty
	// return value sends an empty Date).
	date func(orig string) string
	// hide, if set, removes the listed object versions for which it returns
	// true from ListObjectVersions responses.
	hide func(key string) bool
	// before runs for every request before it is forwarded. Returning a
	// non-zero status answers the request with that S3 error instead.
	before func(r *http.Request) (status int)
}

// minioProxy forwards to endpoint, applying opts, and returns its URL.
func minioProxy(t *testing.T, endpoint string, opts proxyOpts) string {
	t.Helper()
	target, err := url.Parse(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	rp := httputil.NewSingleHostReverseProxy(target)
	rp.ModifyResponse = func(resp *http.Response) error {
		if opts.date != nil {
			resp.Header.Set("Date", opts.date(resp.Header.Get("Date")))
		}
		if opts.shift != 0 {
			if d, err := http.ParseTime(resp.Header.Get("Date")); err == nil {
				resp.Header.Set("Date", d.Add(opts.shift).UTC().Format(http.TimeFormat))
			}
		}
		// A real store reports the same LastModified in a listing and on
		// GetObject, and the source checks that they match (a fetch must return
		// the listed version). So the GetObject Last-Modified header is
		// rewritten the same way as the listings.
		if lmh := resp.Header.Get("Last-Modified"); lmh != "" && (opts.lastModified != nil || opts.shift != 0) {
			if d, err := http.ParseTime(lmh); err == nil {
				orig := d.UTC().Format("2006-01-02T15:04:05.000Z")
				if opts.lastModified != nil {
					orig = opts.lastModified(orig)
				}
				if t, err := time.Parse(time.RFC3339Nano, orig); err == nil {
					resp.Header.Set("Last-Modified", t.Add(opts.shift).UTC().Format(http.TimeFormat))
				}
			}
		}
		if (opts.lastModified == nil && opts.shift == 0 && opts.hide == nil) ||
			!strings.Contains(resp.Header.Get("Content-Type"), "xml") {
			return nil
		}
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		_ = resp.Body.Close()
		if opts.hide != nil {
			body = versionRe.ReplaceAllFunc(body, func(m []byte) []byte {
				k := keyRe.FindSubmatch(m)
				if k != nil && opts.hide(string(k[1])) {
					return nil
				}
				return m
			})
		}
		body = lastModifiedRe.ReplaceAllFunc(body, func(m []byte) []byte {
			orig := strings.TrimSuffix(strings.TrimPrefix(string(m), "<LastModified>"), "</LastModified>")
			if opts.lastModified != nil {
				orig = opts.lastModified(orig)
			}
			if t, err := time.Parse(time.RFC3339Nano, orig); err == nil && opts.shift != 0 {
				orig = t.Add(opts.shift).UTC().Format("2006-01-02T15:04:05.000Z")
			}
			return []byte("<LastModified>" + orig + "</LastModified>")
		})
		resp.Body = io.NopCloser(strings.NewReader(string(body)))
		resp.ContentLength = int64(len(body))
		resp.Header.Set("Content-Length", strconv.Itoa(len(body)))
		return nil
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if opts.before != nil {
			if status := opts.before(r); status != 0 {
				w.Header().Set("Content-Type", "application/xml")
				w.WriteHeader(status)
				_, _ = io.WriteString(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>AccessDenied</Code><Message>injected</Message></Error>`)
				return
			}
		}
		rp.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

// fixedSecond makes every object look like it was modified at the same whole
// second, which is the worst case of AWS's one-second resolution. The second is
// 3s in the future so that the objects written after the first poll still carry
// the same timestamp as the first one (and so that a watermark taken from the
// newest LastModified, as the old code did, is exactly that second).
func fixedSecond() func(string) string {
	fixed := time.Now().UTC().Truncate(time.Second).Add(3 * time.Second).Format("2006-01-02T15:04:05.000Z")
	return func(string) string { return fixed }
}

// TestMinIO_CDCSameSecondObjectsNotSkipped: S3 reports LastModified with
// one-second resolution, and the CDC watermark used to be a strict
// "LastModified > watermark". An object that lands later in the same second as
// the last one the poll delivered was then never emitted. Here every listed
// timestamp is the same second, so the case is deterministic.
func TestMinIO_CDCSameSecondObjectsNotSkipped(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	proxy := minioProxy(t, os.Getenv(envMinIOEndpoint), proxyOpts{lastModified: fixedSecond()})
	client, cfg := minioSourceEnv(t, proxy)
	bucket := cfg[config.ConfigKeyAWSBucket]

	s := openSource(ctx, t, cfg, nil)
	startCDC(ctx, t, s)

	putObject(ctx, t, client, bucket, "same-second-1")
	rec, err := readWithTimeout(ctx, s, 10*time.Second)
	is.NoErr(err)
	is.Equal(string(rec.Key.Bytes()), "same-second-1")

	putObject(ctx, t, client, bucket, "same-second-2")
	seen := drainKeys(ctx, s, 3*time.Second)
	if seen["same-second-2"] == 0 {
		t.Fatalf("same-second-2 shares its LastModified second with a delivered object and was never delivered: %v", seen)
	}
	is.Equal(seen["same-second-1"], 0) // and the delivered one is not delivered again
}

// TestMinIO_CDCSameSecondOverwriteDelivered: in an unversioned bucket the
// version id is always "null", so an overwrite within the same second differs
// from the earlier write only by its ETag.
func TestMinIO_CDCSameSecondOverwriteDelivered(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	proxy := minioProxy(t, os.Getenv(envMinIOEndpoint), proxyOpts{lastModified: fixedSecond()})
	client, cfg := minioSourceEnv(t, proxy)
	bucket := cfg[config.ConfigKeyAWSBucket]
	s := openSource(ctx, t, cfg, nil)
	startCDC(ctx, t, s)

	put := func(body string) {
		_, err := client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String("k"), Body: strings.NewReader(body)})
		is.NoErr(err)
	}
	put("v1")
	rec, err := readWithTimeout(ctx, s, 10*time.Second)
	is.NoErr(err)
	is.Equal(string(rec.Payload.After.Bytes()), "v1")

	put("v2")
	rec, err = readWithTimeout(ctx, s, 10*time.Second)
	if err != nil {
		t.Fatalf("overwrite with a different ETag in the same second was dropped: %v", err)
	}
	is.Equal(string(rec.Payload.After.Bytes()), "v2")
}

// TestMinIO_CDCPollSpanningPagesDoesNotSkip: a poll that needs more than one
// ListObjectVersions page. Between page 1 and page 2 two objects appear: "a-late"
// sorts into the part of the key space page 1 already listed, "z-late" into
// page 2. A watermark taken from the newest LastModified of the listing
// (z-late) would move past a-late, which this poll never saw, and skip it for
// good.
func TestMinIO_CDCPollSpanningPagesDoesNotSkip(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()

	var (
		injected  atomic.Bool
		client    *s3.Client
		bucketRef atomic.Value
	)
	proxy := minioProxy(t, os.Getenv(envMinIOEndpoint), proxyOpts{before: func(r *http.Request) int {
		q := r.URL.Query()
		_, versions := q["versions"]
		if versions && q.Get("key-marker") != "" && injected.CompareAndSwap(false, true) {
			b := bucketRef.Load().(string)
			putObject(ctx, t, client, b, "a-late")
			time.Sleep(1100 * time.Millisecond)
			putObject(ctx, t, client, b, "z-late")
		}
		return 0
	}})
	var cfg map[string]string
	client, cfg = minioSourceEnv(t, proxy)
	bucket := cfg[config.ConfigKeyAWSBucket]
	bucketRef.Store(bucket)
	// lookbackPeriod decides how much of the snapshot the handoff redelivers:
	// CDC starts at the newest snapshot object's LastModified minus the
	// lookback, truncated to the second. With the default of 5m that is all
	// 1001 objects written below, and the injected objects only arrive behind
	// them: on an idle machine the test used 55s of its 60s budget, and it
	// timed out at a high load average. 100ms redelivers about one second of
	// writes instead. The test still needs the 1001 objects (the poll must span
	// two ListObjectVersions pages), but not their redelivery.
	//
	// What the test proves is unchanged, and it gets stricter: a-late is
	// written during the poll that listed page 1, so the next floor must be
	// built from that poll's start on the host clock, not from the newer
	// LastModified of z-late. With a 5m lookback, a floor of "newest
	// LastModified minus 5m" would still reach a-late and hide that bug; with
	// 100ms it does not. The price: the store's clock must not lag the host's
	// by more than the gap between a-late and z-late (1.1s), which holds for a
	// MinIO on the same kernel as the test.
	cfg[source.ConfigKeyLookbackPeriod] = "100ms"

	for i := 0; i < 1001; i++ {
		putObject(ctx, t, client, bucket, fmt.Sprintf("k%04d", i))
	}
	s := openSource(ctx, t, cfg, nil)

	// snapshot, then CDC polls; keep reading until the injected objects show
	// up or the budget runs out. The budget is only spent when the test fails.
	// It is generous because CI runners and loaded laptops are slow: with the
	// short lookback the test needs 15-31s at a load average of 350-600 on 16
	// cores (about 10s idle).
	seen := map[string]int{}
	total := 0
	deadline := time.Now().Add(120 * time.Second)
	for time.Now().Before(deadline) && (seen["a-late"] == 0 || seen["z-late"] == 0) {
		rec, err := readWithTimeout(ctx, s, 5*time.Second)
		if err == nil {
			seen[string(rec.Key.Bytes())]++
			total++
		}
	}
	t.Logf("read %d records (%d distinct keys) before a-late and z-late were both seen or the budget ran out", total, len(seen))
	is.True(injected.Load()) // the CDC poll did reach its second page
	is.True(seen["z-late"] > 0)
	if seen["a-late"] == 0 {
		t.Fatalf("a-late appeared during a multi-page poll and was never delivered")
	}
}

// TestMinIO_SnapshotListErrorIsNotEndOfSnapshot: a failed page listing used to
// make the snapshot look finished, so the source handed over to CDC and the
// objects it had not listed yet were never delivered.
func TestMinIO_SnapshotListErrorIsNotEndOfSnapshot(t *testing.T) {
	ctx := context.Background()
	var failList atomic.Bool
	proxy := minioProxy(t, os.Getenv(envMinIOEndpoint), proxyOpts{before: func(r *http.Request) int {
		if failList.Load() && r.URL.Query().Get("list-type") == "2" {
			return http.StatusForbidden // not retried by the SDK
		}
		return 0
	}})
	client, cfg := minioSourceEnv(t, proxy)
	bucket := cfg[config.ConfigKeyAWSBucket]
	for i := 0; i < 3; i++ {
		putObject(ctx, t, client, bucket, fmt.Sprintf("obj-%d", i))
	}

	failList.Store(true)
	s := openSource(ctx, t, cfg, nil)

	rec, err := s.Read(ctx)
	if err == nil {
		t.Fatalf("expected the listing error, got record %q", rec.Key.Bytes())
	}
	if errors.Is(err, sdk.ErrBackoffRetry) {
		t.Fatalf("listing error was swallowed as a retry/end of snapshot: %v", err)
	}
	if !strings.Contains(err.Error(), "AccessDenied") && !strings.Contains(err.Error(), "403") {
		t.Fatalf("error does not carry the listing failure: %v", err)
	}
}

// TestMinIO_HandoffRedeliveryIsBounded: the snapshot-to-CDC handoff covers
// objects written while the snapshot ran by starting CDC at (snapshot start -
// lookback). That redelivers objects, but only the ones modified within the
// lookback; an older object is delivered exactly once.
func TestMinIO_HandoffRedeliveryIsBounded(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	client, cfg := minioSourceEnv(t, "")
	setLookback(cfg, "2s")
	bucket := cfg[config.ConfigKeyAWSBucket]

	putObject(ctx, t, client, bucket, "old")
	time.Sleep(3500 * time.Millisecond) // older than the lookback by the time the snapshot starts
	putObject(ctx, t, client, bucket, "recent")

	s := openSource(ctx, t, cfg, nil)
	seen := drainKeys(ctx, s, 4*time.Second)

	is.Equal(seen["old"], 1)     // outside the window: never redelivered
	is.True(seen["recent"] >= 1) // delivered by the snapshot
	is.True(seen["recent"] <= 2) // at most one redelivery by CDC
	is.Equal(len(seen), 2)       // nothing else appears
}

// TestMinIO_ObjectsCreatedRightAfterOpenAreDelivered is the regression test
// for #1032: on an empty bucket the source went straight to CDC at the client's
// time.Now(), so objects written in the first moments after Open could carry an
// earlier (server, millisecond or second resolution) LastModified and were lost.
func TestMinIO_ObjectsCreatedRightAfterOpenAreDelivered(t *testing.T) {
	ctx := context.Background()
	client, cfg := minioSourceEnv(t, "")
	bucket := cfg[config.ConfigKeyAWSBucket]
	s := openSource(ctx, t, cfg, nil)

	const n = 100
	go func() {
		for i := 0; i < n; i++ {
			_, _ = client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(fmt.Sprintf("obj-%03d", i)), Body: strings.NewReader("x")})
			time.Sleep(10 * time.Millisecond)
		}
	}()

	seen := map[string]int{}
	deadline := time.Now().Add(20 * time.Second)
	for len(seen) < n && time.Now().Before(deadline) {
		rec, err := readWithTimeout(ctx, s, 2*time.Second)
		if err == nil {
			seen[string(rec.Key.Bytes())]++
		}
	}
	for i := 0; i < n; i++ {
		if k := fmt.Sprintf("obj-%03d", i); seen[k] == 0 {
			t.Errorf("%s was written after Open and never delivered (%d of %d)", k, len(seen), n)
		}
	}
}

// TestMinIO_CDCLookback: an object that is listed late (here: hidden from
// ListObjectVersions for a while, like a multipart upload that completes after
// newer objects were already delivered) is only delivered if the lookback is
// longer than the delay. This is what the lookback is for, and what proves the
// other tests do not pass by accident with lookbackPeriod=0.
func TestMinIO_CDCLookback(t *testing.T) {
	const hideFor = 4 * time.Second
	for _, tc := range []struct {
		name          string
		lookback      string // "" is the default
		wantDelivered bool
	}{
		{"default lookback delivers the late object", "", true},
		{"lookback 0 loses it", "0s", false},
		// documented bound: an object that becomes visible later than the
		// lookback after its LastModified can be missed
		{"lookback shorter than the delay loses it", "1s", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			ctx := context.Background()
			var hidden atomic.Bool
			proxy := minioProxy(t, os.Getenv(envMinIOEndpoint), proxyOpts{hide: func(key string) bool {
				return key == "late" && hidden.Load()
			}})
			client, cfg := minioSourceEnv(t, proxy)
			if tc.lookback != "" {
				setLookback(cfg, tc.lookback)
			}
			bucket := cfg[config.ConfigKeyAWSBucket]
			s := openSource(ctx, t, cfg, nil)
			startCDC(ctx, t, s)

			hidden.Store(true)
			putObject(ctx, t, client, bucket, "late")
			time.Sleep(3100 * time.Millisecond) // "newer" is 3s newer than "late" (the lookback is measured between them)
			putObject(ctx, t, client, bucket, "newer")
			rec, err := readWithTimeout(ctx, s, 10*time.Second)
			is.NoErr(err)
			is.Equal(string(rec.Key.Bytes()), "newer") // late is invisible

			time.Sleep(hideFor)
			hidden.Store(false)
			seen := drainKeys(ctx, s, 3*time.Second)

			if tc.wantDelivered {
				is.True(seen["late"] >= 1) // delivered once it became visible
			} else if seen["late"] != 0 {
				t.Fatalf("late was delivered although the lookback (%s) is shorter than the %s it was hidden: %v", tc.lookback, hideFor, seen)
			}
		})
	}
}

// TestMinIO_CDCClockSkew: the host clock is ahead of the store's by more than
// the lookback (the store's Date headers and LastModified are shifted 10
// minutes into the past). The watermark must come from the store's clock;
// taken from the host's, every new object would look older than the watermark
// and be lost.
func TestMinIO_CDCClockSkew(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	proxy := minioProxy(t, os.Getenv(envMinIOEndpoint), proxyOpts{shift: -10 * time.Minute})
	client, cfg := minioSourceEnv(t, proxy)
	bucket := cfg[config.ConfigKeyAWSBucket]
	s := openSource(ctx, t, cfg, nil)
	startCDC(ctx, t, s)

	putObject(ctx, t, client, bucket, "skewed")
	rec, err := readWithTimeout(ctx, s, 10*time.Second)
	if err != nil {
		t.Fatalf("object written while the host clock is 10m ahead of the store's was never delivered: %v", err)
	}
	is.Equal(string(rec.Key.Bytes()), "skewed")
}

// TestMinIO_HostileDateHeader: the Date header comes from whatever endpoint is
// configured. Whatever it says, an object must never be lost silently. The
// AWS SDK itself uses the Date header to correct its signing clock, so a date
// far from the truth makes MinIO reject the source's next requests: that is an
// error and fails the pipeline loudly, which is acceptable. What is not
// acceptable is silence. (The watermark arithmetic for every kind of lie is
// covered without signing in TestResolveStoreTime and the fake-store tests in
// source/iterator.)
func TestMinIO_HostileDateHeader(t *testing.T) {
	future := func(d time.Duration) func(string) string {
		return func(string) string { return time.Now().Add(d).UTC().Format(http.TimeFormat) }
	}
	for _, tc := range []struct {
		name string
		date func(string) string
	}{
		{"10 minutes ahead", future(10 * time.Minute)},
		{"far future", future(100 * 24 * time.Hour)},
		{"far past", future(-100 * 24 * time.Hour)},
		{"garbage", func(string) string { return "next tuesday" }},
		{"missing", func(string) string { return "" }},
	} {
		silentLoss := func(t *testing.T, err error, what string) {
			t.Helper()
			if err == nil || !errors.Is(err, context.DeadlineExceeded) {
				return // delivered, or failed loudly
			}
			t.Fatalf("%s was silently lost with a %s Date header", what, tc.name)
		}
		t.Run(tc.name+"/cdc from empty bucket", func(t *testing.T) {
			ctx := context.Background()
			proxy := minioProxy(t, os.Getenv(envMinIOEndpoint), proxyOpts{date: tc.date})
			client, cfg := minioSourceEnv(t, proxy)
			s := openSource(ctx, t, cfg, nil)
			if _, err := s.Read(ctx); err != nil && !errors.Is(err, sdk.ErrBackoffRetry) {
				return // failed loudly
			}
			putObject(ctx, t, client, cfg[config.ConfigKeyAWSBucket], "obj")
			_, err := readWithTimeout(ctx, s, 8*time.Second)
			silentLoss(t, err, "obj")
		})
		t.Run(tc.name+"/snapshot handoff", func(t *testing.T) {
			ctx := context.Background()
			proxy := minioProxy(t, os.Getenv(envMinIOEndpoint), proxyOpts{date: tc.date})
			client, cfg := minioSourceEnv(t, proxy)
			bucket := cfg[config.ConfigKeyAWSBucket]
			putObject(ctx, t, client, bucket, "old")
			s := openSource(ctx, t, cfg, nil)
			if _, err := readWithTimeout(ctx, s, 8*time.Second); err != nil {
				silentLoss(t, err, "old")
				return
			}
			putObject(ctx, t, client, bucket, "new") // written after the snapshot's only page
			deadline := time.Now().Add(8 * time.Second)
			for time.Now().Before(deadline) {
				rec, err := readWithTimeout(ctx, s, time.Second)
				if err == nil && string(rec.Key.Bytes()) == "new" {
					return
				}
				if err != nil && !errors.Is(err, context.DeadlineExceeded) {
					return // failed loudly
				}
			}
			t.Fatalf("new was silently lost with a %s Date header", tc.name)
		})
	}
}

// setLookback sets lookbackPeriod when the connector has that parameter. The
// parameter does not exist on the code these tests are written against, so on
// it the tests run with the old behavior instead of failing on "unrecognized
// parameter", and fail for the real reason (a lost object).
func setLookback(cfg map[string]string, v string) {
	if _, ok := s3Conn.Connector.NewSpecification().SourceParams["lookbackPeriod"]; ok {
		cfg["lookbackPeriod"] = v
	}
}

// TestMinIO_SnapshotLongerThanLookbackDoesNotSkip: the snapshot takes longer
// than lookbackPeriod. An object written early in the snapshot, under a key
// range that was already listed, must still be delivered, although objects
// written much later are read by the snapshot's second page. The handoff is
// anchored to the objects that existed when the snapshot started, not to the
// newest object the snapshot happens to read (#1032).
func TestMinIO_SnapshotLongerThanLookbackDoesNotSkip(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	client, cfg := minioSourceEnv(t, "")
	setLookback(cfg, "2s")
	bucket := cfg[config.ConfigKeyAWSBucket]

	for i := 0; i < 1001; i++ { // two ListObjectsV2 pages
		putObject(ctx, t, client, bucket, fmt.Sprintf("k%04d", i))
	}
	s := openSource(ctx, t, cfg, nil)

	seen := map[string]int{}
	rec, err := readWithTimeout(ctx, s, waitBudget) // anchor pass + page 1
	is.NoErr(err)
	seen[string(rec.Key.Bytes())]++

	putObject(ctx, t, client, bucket, "a-late") // sorts into the page that was already listed
	time.Sleep(3100 * time.Millisecond)         // the snapshot has now run for longer than the lookback
	putObject(ctx, t, client, bucket, "z-late") // read by page 2, 3s newer than a-late

	drainUntil(ctx, s, seen, waitBudget, bothLate)
	is.True(seen["z-late"] > 0)
	if seen["a-late"] == 0 {
		t.Fatalf("a-late was written early in a snapshot that outlasted the lookback and was never delivered (%d distinct keys)", len(seen))
	}
}

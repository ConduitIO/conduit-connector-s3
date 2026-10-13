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
// Like TestSource_MinIO they need MINIO_ENDPOINT (see `make test-integration-s3`).

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
	client := newEndpointS3Client(t, cfg)
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

// TestSource_SnapshotHandoffDoesNotSkipObjects: an object created while the
// snapshot is paginating, under a page the snapshot already passed, must not be
// lost at the snapshot-to-CDC handoff.
//
// The snapshot lists in key order, one page (1000 keys) at a time, and hands
// over to CDC at the largest LastModified it has read. "a-late" is created
// after the first page was listed and so is missing from the snapshot, but it
// is created before "z-late", which the snapshot does read and which therefore
// pushes the CDC watermark past a-late's LastModified.
func TestSource_SnapshotHandoffDoesNotSkipObjects(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	client, cfg := minioSourceEnv(t, "")
	bucket := cfg[config.ConfigKeyAWSBucket]

	for i := 0; i < 1001; i++ {
		putObject(ctx, t, client, bucket, fmt.Sprintf("k%04d", i))
	}

	s := openSource(ctx, t, cfg, nil)

	seen := map[string]int{}
	rec, err := readWithTimeout(ctx, s, 10*time.Second) // lists page 1 (k0000..k0998 + ...)
	is.NoErr(err)
	seen[string(rec.Key.Bytes())]++

	// concurrent writes during the snapshot
	putObject(ctx, t, client, bucket, "a-late")
	time.Sleep(1100 * time.Millisecond) // GetObject's LastModified has one-second resolution
	putObject(ctx, t, client, bucket, "z-late")

	for k, n := range drainKeys(ctx, s, 3*time.Second) {
		seen[k] += n
	}

	t.Logf("distinct=%d a-late=%d z-late=%d first=%s", len(seen), seen["a-late"], seen["z-late"], string(rec.Key.Bytes()))
	is.True(seen["z-late"] > 0) // control: the later write is delivered
	if seen["a-late"] == 0 {
		t.Fatalf("a-late was written during the snapshot and never delivered (%d distinct keys seen)", len(seen))
	}
}

var lastModifiedRe = regexp.MustCompile(`(<LastModified>[^<.]*)\.\d+(Z</LastModified>)`)

// secondPrecisionProxy forwards to endpoint, truncating every LastModified in
// listing responses to whole seconds, which is what AWS S3 reports. MinIO
// reports milliseconds, so without this the same-second collisions that AWS
// users hit cannot be produced on demand.
func secondPrecisionProxy(t *testing.T, endpoint string) string {
	t.Helper()
	target, err := url.Parse(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	rp := httputil.NewSingleHostReverseProxy(target)
	rp.ModifyResponse = func(resp *http.Response) error {
		if !strings.Contains(resp.Header.Get("Content-Type"), "xml") {
			return nil
		}
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		_ = resp.Body.Close()
		body = lastModifiedRe.ReplaceAll(body, []byte("$1.000$2"))
		resp.Body = io.NopCloser(strings.NewReader(string(body)))
		resp.ContentLength = int64(len(body))
		resp.Header.Set("Content-Length", strconv.Itoa(len(body)))
		return nil
	}
	srv := httptest.NewServer(rp)
	t.Cleanup(srv.Close)
	return srv.URL
}

// TestSource_CDCSameSecondObjectsNotSkipped: S3 reports LastModified with
// one-second resolution, and the CDC watermark used to be a strict
// "LastModified > watermark". An object that lands later in the same second as
// the last one the poll delivered was then never emitted.
func TestSource_CDCSameSecondObjectsNotSkipped(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	proxy := secondPrecisionProxy(t, os.Getenv(envMinIOEndpoint))
	client, cfg := minioSourceEnv(t, proxy)
	bucket := cfg[config.ConfigKeyAWSBucket]

	// seed so the snapshot ends with a real watermark, then let the second pass
	putObject(ctx, t, client, bucket, "seed")
	time.Sleep(1100 * time.Millisecond)

	s := openSource(ctx, t, cfg, nil)
	rec, err := readWithTimeout(ctx, s, 10*time.Second)
	is.NoErr(err)
	is.Equal(string(rec.Key.Bytes()), "seed")

	// start at the beginning of a wall-clock second so that both puts land in it
	time.Sleep(time.Until(time.Now().Truncate(time.Second).Add(time.Second + 50*time.Millisecond)))
	putObject(ctx, t, client, bucket, "same-second-1")
	// A poll delivers it and advances the watermark. Redelivery of "seed" after
	// the snapshot handoff is allowed (at-least-once), so skip over it.
	for {
		rec, err = readWithTimeout(ctx, s, 5*time.Second)
		is.NoErr(err)
		if string(rec.Key.Bytes()) == "same-second-1" {
			break
		}
	}
	putObject(ctx, t, client, bucket, "same-second-2")

	h1, err := client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String("same-second-1")})
	is.NoErr(err)
	h2, err := client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String("same-second-2")})
	is.NoErr(err)
	if h1.LastModified.Unix() != h2.LastModified.Unix() {
		t.Skip("puts straddled a second boundary; rerun")
	}

	seen := drainKeys(ctx, s, 3*time.Second)
	if seen["same-second-2"] == 0 {
		t.Fatalf("same-second-2 shares a one-second LastModified with an already delivered object and was never delivered: %v", seen)
	}
}

// TestSource_ObjectsCreatedRightAfterOpenAreDelivered is the regression test
// for #1032: on an empty bucket the source went straight to CDC at the client's
// time.Now(), so objects written in the first moments after Open could carry an
// earlier (server, millisecond or second resolution) LastModified and were lost.
func TestSource_ObjectsCreatedRightAfterOpenAreDelivered(t *testing.T) {
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

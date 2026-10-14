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
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/conduitio/conduit-connector-s3/config"
	"github.com/conduitio/conduit-connector-s3/source"
)

var blockRe = regexp.MustCompile(`(?s)<(Version|Contents|DeleteMarker)>.*?</(Version|Contents|DeleteMarker)>`)

// r6proxy rewrites LastModified per key and can hide keys from listings.
func r6proxy(t *testing.T, endpoint string, lmFor func(key string) (time.Time, bool), hide func(key string) bool, failList func(r *http.Request) bool) string {
	t.Helper()
	target, _ := url.Parse(endpoint)
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
		body = blockRe.ReplaceAllFunc(body, func(m []byte) []byte {
			k := keyRe.FindSubmatch(m)
			if k == nil {
				return m
			}
			key := string(k[1])
			if hide != nil && hide(key) {
				return nil
			}
			if lmFor != nil {
				if ts, ok := lmFor(key); ok {
					return lastModifiedRe.ReplaceAll(m, []byte("<LastModified>"+ts.UTC().Format("2006-01-02T15:04:05.000Z")+"</LastModified>"))
				}
			}
			return m
		})
		resp.Body = io.NopCloser(strings.NewReader(string(body)))
		resp.ContentLength = int64(len(body))
		resp.Header.Set("Content-Length", strconv.Itoa(len(body)))
		return nil
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if failList != nil && failList(r) {
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(403)
			_, _ = io.WriteString(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>AccessDenied</Code><Message>injected</Message></Error>`)
			return
		}
		rp.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

// R5-B1: a +2h object, pollingPeriod 1s. Later objects and a late-visible one
// (within the lookback) must still be delivered.
func TestMinIO_FutureObjectKeepsLookback(t *testing.T) {
	ctx := context.Background()
	var hidden atomic.Bool
	future := time.Now().Add(2 * time.Hour)
	proxy := r6proxy(t, os.Getenv(envMinIOEndpoint),
		func(k string) (time.Time, bool) { return future, k == "bogus" },
		func(k string) bool { return k == "late" && hidden.Load() }, nil)
	client, cfg := minioSourceEnv(t, proxy)
	cfg[source.ConfigKeyPollingPeriod] = "1s"
	bucket := cfg[config.ConfigKeyAWSBucket]
	s := openSource(ctx, t, cfg, nil)
	startCDC(ctx, t, s)

	putObject(ctx, t, client, bucket, "bogus")
	rec, err := readWithTimeout(ctx, s, 15*time.Second)
	if err != nil || string(rec.Key.Bytes()) != "bogus" {
		t.Fatalf("bogus: %v %v", rec.Key, err)
	}
	hidden.Store(true)
	putObject(ctx, t, client, bucket, "late")
	time.Sleep(2100 * time.Millisecond)
	putObject(ctx, t, client, bucket, "newer")
	rec, err = readWithTimeout(ctx, s, 15*time.Second)
	if err != nil || string(rec.Key.Bytes()) != "newer" {
		t.Fatalf("newer lost after a +2h object: %v %v", rec.Key, err)
	}
	time.Sleep(3 * time.Second)
	hidden.Store(false)
	seen := drainKeys(ctx, s, 4*time.Second)
	if seen["late"] == 0 {
		t.Fatalf("late (visible 5s after its LastModified, lookback 5m) lost: %v", seen)
	}
}

// F2: the bucket holds one object stamped in the future; the snapshot runs
// longer than the lookback; an object written early in it must be delivered.
func TestMinIO_FutureAnchorLongSnapshot(t *testing.T) {
	ctx := context.Background()
	future := time.Now().Add(2 * time.Hour)
	proxy := r6proxy(t, os.Getenv(envMinIOEndpoint),
		func(k string) (time.Time, bool) { return future, k == "zz-bogus" }, nil, nil)
	client, cfg := minioSourceEnv(t, proxy)
	setLookback(cfg, "2s")
	bucket := cfg[config.ConfigKeyAWSBucket]
	for i := 0; i < 1101; i++ {
		putObject(ctx, t, client, bucket, fmt.Sprintf("k%04d", i))
	}
	putObject(ctx, t, client, bucket, "zz-bogus")
	s := openSource(ctx, t, cfg, nil)
	seen := map[string]int{}
	rec, err := readWithTimeout(ctx, s, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	seen[string(rec.Key.Bytes())]++
	putObject(ctx, t, client, bucket, "a-late")
	time.Sleep(3100 * time.Millisecond)
	putObject(ctx, t, client, bucket, "z-late")
	for k, n := range drainKeys(ctx, s, 4*time.Second) {
		seen[k] += n
	}
	if seen["a-late"] == 0 {
		t.Fatalf("a-late LOST with a future-stamped object in the bucket: z-late=%d distinct=%d", seen["z-late"], len(seen))
	}
}

// Versioned bucket: snapshot lists current versions (ListObjectsV2) while CDC
// lists all versions; the anchor must still cover overwrites and deletes made
// during a snapshot that outlasts the lookback.
func TestMinIO_VersionedBucketAnchor(t *testing.T) {
	ctx := context.Background()
	client, cfg := minioSourceEnv(t, "")
	setLookback(cfg, "2s")
	bucket := cfg[config.ConfigKeyAWSBucket]
	if _, err := client.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
		Bucket:                  aws.String(bucket),
		VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled},
	}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 1101; i++ {
		putObject(ctx, t, client, bucket, fmt.Sprintf("k%04d", i))
	}
	// older versions + a delete marker that is the newest thing in the bucket
	putObject(ctx, t, client, bucket, "k0500")
	putObject(ctx, t, client, bucket, "dead")
	time.Sleep(1100 * time.Millisecond)
	if _, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String("dead")}); err != nil {
		t.Fatal(err)
	}
	s := openSource(ctx, t, cfg, nil)
	seen := map[string]int{}
	rec, err := readWithTimeout(ctx, s, 60*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	seen[string(rec.Key.Bytes())]++
	putObject(ctx, t, client, bucket, "a-late")
	putObject(ctx, t, client, bucket, "k0001") // overwrite in an already listed range
	time.Sleep(3100 * time.Millisecond)
	putObject(ctx, t, client, bucket, "z-late")
	ops := map[string]string{}
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		r, err := readWithTimeout(ctx, s, 4*time.Second)
		if err != nil {
			break
		}
		seen[string(r.Key.Bytes())]++
		ops[string(r.Key.Bytes())] = r.Operation.String()
	}
	t.Logf("a-late=%d z-late=%d k0001=%d(%s) dead=%d(%s) distinct=%d", seen["a-late"], seen["z-late"], seen["k0001"], ops["k0001"], seen["dead"], ops["dead"], len(seen))
	if seen["a-late"] == 0 || seen["z-late"] == 0 {
		t.Fatal("lost")
	}
}

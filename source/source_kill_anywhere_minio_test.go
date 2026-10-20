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
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

// killWriter writes to a bucket and remembers what every key ends as.
type killWriter struct {
	mu     sync.Mutex
	seq    int
	final  map[string]string // key -> body of the last write ("" = deleted)
	client *s3.Client
	bucket string
}

func (w *killWriter) body(key string) string {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.seq++
	return fmt.Sprintf("%s#%d", key, w.seq)
}

func (w *killWriter) put(ctx context.Context, t *testing.T, key string) {
	t.Helper()
	b := w.body(key)
	if _, err := w.client.PutObject(ctx, &s3.PutObjectInput{Bucket: &w.bucket, Key: &key, Body: strings.NewReader(b)}); err != nil {
		t.Errorf("put %s: %v", key, err)
		return
	}
	w.mu.Lock()
	w.final[key] = b
	w.mu.Unlock()
}

func (w *killWriter) del(ctx context.Context, t *testing.T, key string) {
	t.Helper()
	if _, err := w.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &w.bucket, Key: &key}); err != nil {
		t.Errorf("delete %s: %v", key, err)
		return
	}
	w.mu.Lock()
	w.final[key] = ""
	w.mu.Unlock()
}

// multipart uploads a two-part object, waiting hold between initiating the
// upload and completing it (spanning many polls and kills).
func (w *killWriter) multipart(ctx context.Context, t *testing.T, key string, hold time.Duration) {
	t.Helper()
	b := w.body(key)
	cu, err := w.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: &w.bucket, Key: &key})
	if err != nil {
		t.Errorf("create multipart upload %s: %v", key, err)
		return
	}
	p1, err := w.client.UploadPart(ctx, &s3.UploadPartInput{Bucket: &w.bucket, Key: &key, UploadId: cu.UploadId, PartNumber: aws.Int32(1), Body: bytes.NewReader(bytes.Repeat([]byte("x"), 5<<20))})
	if err != nil {
		t.Errorf("upload part 1 of %s: %v", key, err)
		return
	}
	time.Sleep(hold)
	p2, err := w.client.UploadPart(ctx, &s3.UploadPartInput{Bucket: &w.bucket, Key: &key, UploadId: cu.UploadId, PartNumber: aws.Int32(2), Body: strings.NewReader(b)})
	if err != nil {
		t.Errorf("upload part 2 of %s: %v", key, err)
		return
	}
	_, err = w.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{Bucket: &w.bucket, Key: &key, UploadId: cu.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{{ETag: p1.ETag, PartNumber: aws.Int32(1)}, {ETag: p2.ETag, PartNumber: aws.Int32(2)}}}})
	if err != nil {
		t.Errorf("complete multipart upload %s: %v", key, err)
		return
	}
	w.mu.Lock()
	w.final[key] = "MP:" + b
	w.mu.Unlock()
}

// killBody is the body a record carries, in the form killWriter records it.
func killBody(r opencdc.Record) string {
	if r.Payload.After == nil {
		return ""
	}
	b := r.Payload.After.Bytes()
	if len(b) > 5<<20 {
		return "MP:" + string(b[5<<20:])
	}
	return string(b)
}

// TestMinIO_KillAnywhereVersionedBucket kills a running source at random
// points, with partial acks, in a versioned bucket under concurrent writes:
// overwrites, same-second bursts, deletes, and multipart uploads that span many
// polls, including uploads initiated in the second an earlier upload of the
// same key completed in, and several uploads of one key in one second. After
// every kill the source resumes from the last acked position. No acked record
// may come twice (a redelivery of a record that was not acked is expected),
// and every key's final state must be the last record delivered for it.
//
// S3_KILL_SEEDS (default 1) is the number of seeds; each takes about 50
// seconds.
func TestMinIO_KillAnywhereVersionedBucket(t *testing.T) {
	seeds := 1
	if v, err := strconv.Atoi(os.Getenv("S3_KILL_SEEDS")); err == nil {
		seeds = v
	}
	for seed := int64(1); seed <= int64(seeds); seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) { killAnywhere(t, seed) })
	}
}

func killAnywhere(t *testing.T, seed int64) { //nolint:gocyclo // one scenario read top to bottom
	ctx := context.Background()
	r := rand.New(rand.NewSource(seed))
	client, cfg := minioSourceEnv(t, "")
	lookback := 3 * time.Second
	setLookback(cfg, "3s")
	cfg["cdc.maxEarlyObjects"] = "4"
	bucket := cfg["aws.bucket"]
	_, err := client.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{Bucket: &bucket,
		VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled}})
	if err != nil {
		t.Fatal(err)
	}
	w := &killWriter{final: map[string]string{}, client: client, bucket: bucket}
	for i := 0; i < 25; i++ {
		w.put(ctx, t, fmt.Sprintf("snap-%02d", i))
	}
	time.Sleep(lookback + 2*time.Second) // settled: the snapshot delivers them

	keys := []string{"a", "b", "c", "d", "e", "snap-03", "snap-17"}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		wr := rand.New(rand.NewSource(seed + 1000))
		var mp sync.WaitGroup
		defer mp.Wait()

		// An upload that is initiated in the second a previous upload of its
		// key completed in, and outlives several polls.
		time.Sleep(time.Duration(1000-time.Now().Nanosecond()/1e6%1000+50) * time.Millisecond) // just after a second starts
		w.multipart(ctx, t, "mp-same", 0)
		mp.Add(1)
		go func() { defer mp.Done(); w.multipart(ctx, t, "mp-same", 9*time.Second) }()
		// Two uploads of one key initiated within one second.
		mp.Add(2)
		go func() { defer mp.Done(); w.multipart(ctx, t, "mp-pair", 7*time.Second) }()
		go func() {
			defer mp.Done()
			time.Sleep(100 * time.Millisecond)
			w.multipart(ctx, t, "mp-pair", 10*time.Second)
		}()

		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			switch op := wr.Intn(10); {
			case op < 5:
				w.put(ctx, t, keys[wr.Intn(len(keys))])
			case op < 7: // same-second burst on one key and a neighbour
				k := keys[wr.Intn(len(keys))]
				w.put(ctx, t, k)
				w.put(ctx, t, k)
				w.put(ctx, t, k+"x")
			case op == 7:
				w.del(ctx, t, keys[wr.Intn(len(keys))])
			case op == 8 && i%3 == 0:
				mp.Add(1)
				k := "mp-" + strconv.Itoa(wr.Intn(3))
				hold := time.Duration(2+wr.Intn(8)) * time.Second
				go func() { defer mp.Done(); w.multipart(ctx, t, k, hold) }()
			}
			time.Sleep(time.Duration(50+wr.Intn(400)) * time.Millisecond)
		}
	}()

	var acked []opencdc.Record
	var pos opencdc.Position
	kills := 0
	for deadline := time.Now().Add(25 * time.Second); time.Now().Before(deadline); kills++ {
		s := openSource(ctx, t, cfg, pos)
		n := 1 + r.Intn(12)
		var run []opencdc.Record
		for len(run) < n {
			rec, err := readWithTimeout(ctx, s, time.Duration(200+r.Intn(1500))*time.Millisecond)
			if err != nil {
				break
			}
			run = append(run, rec)
		}
		j := 0
		if len(run) > 0 {
			j = r.Intn(len(run) + 1) // the source dies with the rest unacked
		}
		acked = append(acked, run[:j]...)
		if j > 0 {
			pos = run[j-1].Position
		}
		_ = s.Teardown(ctx)
	}
	close(stop)
	wg.Wait()

	// Final drain: everything settles and is delivered.
	s := openSource(ctx, t, cfg, pos)
	for {
		rec, err := readWithTimeout(ctx, s, 3*lookback+3*time.Second)
		if err != nil {
			if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, sdk.ErrBackoffRetry) {
				t.Fatalf("read: %v", err)
			}
			break
		}
		acked = append(acked, rec)
	}
	t.Logf("seed %d: %d kills, %d acked records", seed, kills, len(acked))

	seen := map[string]int{}
	last := map[string]opencdc.Record{}
	for _, rec := range acked {
		k := string(rec.Key.Bytes())
		last[k] = rec
		if rec.Operation == opencdc.OperationDelete {
			continue
		}
		seen[k+"|"+killBody(rec)]++
	}
	for id, n := range seen {
		if n > 1 {
			t.Errorf("seed %d: delivered %d times: %.80s", seed, n, id)
		}
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	for k, want := range w.final {
		got, ok := last[k]
		switch {
		case want == "":
			if ok && got.Operation != opencdc.OperationDelete {
				t.Errorf("seed %d: key %s ends deleted, last delivered %v %q", seed, k, got.Operation, killBody(got))
			}
		case !ok:
			t.Errorf("seed %d: key %s (final %q) was never delivered", seed, k, want)
		case killBody(got) != want:
			t.Errorf("seed %d: key %s: last delivered %q (%v), final %q", seed, k, killBody(got), got.Operation, want)
		}
	}
}

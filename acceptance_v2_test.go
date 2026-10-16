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

package s3

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"go.uber.org/goleak"
)

// envMinIOEndpoint names the MinIO endpoint the acceptance suite runs
// against. It is MINIO_ENDPOINT and not AWS_ENDPOINT_URL for the same reason
// as in the destination and source MinIO tests: the AWS SDK reads
// AWS_ENDPOINT_URL itself, so the connector's own aws.endpoint plumbing would
// not be exercised.
const envMinIOEndpoint = "MINIO_ENDPOINT"

var acceptanceBucketSeq atomic.Int64

// acceptanceDriver adapts the S3 connector to acceptance suite v2.
//
// The default driver round-trips records through the connector's own source
// and destination, which does not work here: the source returns one record
// per object (key = object key, payload = object body), while the destination
// writes a whole batch into one object as JSON lines. So the driver seeds the
// source by putting one object per record, and reads the destination back by
// parsing the JSON lines it wrote. Every case gets a fresh bucket.
type acceptanceDriver struct {
	sdk.ConfigurableAcceptanceTestDriver

	endpoint string
	client   *s3.Client
	bucket   string
	idSeq    atomic.Int64
}

func newAcceptanceDriver(endpoint string) *acceptanceDriver {
	return &acceptanceDriver{
		endpoint: endpoint,
		ConfigurableAcceptanceTestDriver: sdk.ConfigurableAcceptanceTestDriver{
			Config: sdk.ConfigurableAcceptanceTestDriverConfig{
				Connector:        Connector,
				GenerateDataType: sdk.GenerateRawData,
				GoleakOptions: []goleak.Option{
					// Idle keep-alive connections of the AWS SDK's HTTP
					// transport, shared by the driver's own client.
					goleak.IgnoreTopFunction("internal/poll.runtime_pollWait"),
					goleak.IgnoreTopFunction("net/http.(*persistConn).readLoop"),
					goleak.IgnoreTopFunction("net/http.(*persistConn).writeLoop"),
				},
				ReadTimeout:  5 * time.Second,
				WriteTimeout: 10 * time.Second,
				// Claims: only what the suite can prove against MinIO.
				//   - Destination: at-least-once. A replayed batch is written
				//     again under a new, time-based object key (#1030, #621).
				//   - No ordering claim: objects are listed in key order, not
				//     write order, and the destination makes no per-key order
				//     promise across objects.
				//   - No schema evolution claim: the destination stores the
				//     payload bytes as a string and has no schema policy.
				//   - No ExactResume: after a restart the source redelivers the
				//     objects within the lookback window (at-least-once).
				Capabilities: sdk.AcceptanceCapabilities{
					Destination: sdk.DestinationCapabilities{Idempotence: sdk.IdempotenceNo},
				},
			},
		},
	}
}

func (d *acceptanceDriver) newClient(t *testing.T) *s3.Client {
	cfg, err := awsConfig.LoadDefaultConfig(context.Background(),
		awsConfig.WithRegion(os.Getenv("AWS_REGION")),
		awsConfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			os.Getenv("AWS_ACCESS_KEY_ID"), os.Getenv("AWS_SECRET_ACCESS_KEY"), "")))
	if err != nil {
		t.Fatal(err)
	}
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(d.endpoint)
		o.UsePathStyle = true
	})
}

func (d *acceptanceDriver) BeforeTest(t *testing.T) {
	d.ConfigurableAcceptanceTestDriver.BeforeTest(t)
	d.client = d.newClient(t)
	d.bucket = fmt.Sprintf("acc-v2-%d-%d", time.Now().UnixNano()%1_000_000_000, acceptanceBucketSeq.Add(1))
	if _, err := d.client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: aws.String(d.bucket)}); err != nil {
		t.Fatalf("create bucket %s: %v", d.bucket, err)
	}
}

func (d *acceptanceDriver) AfterTest(t *testing.T) {
	ctx := context.Background()
	p := s3.NewListObjectsV2Paginator(d.client, &s3.ListObjectsV2Input{Bucket: aws.String(d.bucket)})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			t.Logf("cleanup: list %s: %v", d.bucket, err)
			break
		}
		for _, o := range page.Contents {
			_, _ = d.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(d.bucket), Key: o.Key})
		}
	}
	if _, err := d.client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(d.bucket)}); err != nil {
		t.Logf("cleanup: delete bucket %s: %v", d.bucket, err)
	}
	d.ConfigurableAcceptanceTestDriver.AfterTest(t)
}

func (d *acceptanceDriver) base() config.Config {
	return config.Config{
		"aws.accessKeyId":     os.Getenv("AWS_ACCESS_KEY_ID"),
		"aws.secretAccessKey": os.Getenv("AWS_SECRET_ACCESS_KEY"),
		"aws.region":          os.Getenv("AWS_REGION"),
		"aws.bucket":          d.bucket,
		"aws.endpoint":        d.endpoint,
		"aws.pathStyle":       "true",
		// The suite compares payload bytes; schema extraction would encode
		// them and is not what this driver tests.
		"sdk.schema.extract.key.enabled":     "false",
		"sdk.schema.extract.payload.enabled": "false",
	}
}

func (d *acceptanceDriver) SourceConfig(*testing.T) config.Config {
	c := d.base()
	c["pollingPeriod"] = "200ms"
	return c
}

func (d *acceptanceDriver) DestinationConfig(*testing.T) config.Config {
	c := d.base()
	c["format"] = "json"
	return c
}

// GenerateRecord returns a record with a unique key and payload. The key is
// used as the object key when seeding the source, so it must be unique and
// a valid S3 key.
func (d *acceptanceDriver) GenerateRecord(_ *testing.T, op opencdc.Operation) opencdc.Record {
	id := d.idSeq.Add(1)
	now := time.Now().UnixNano()
	return opencdc.Record{
		Position:  opencdc.Position(fmt.Sprintf("p-%d-%d", now, id)),
		Operation: op,
		Metadata:  opencdc.Metadata{},
		Key:       opencdc.RawData(fmt.Sprintf("key-%d-%d", now, id)),
		Payload:   opencdc.Change{After: opencdc.RawData(fmt.Sprintf("payload-%d-%d-%d", now, id, id*7919))},
	}
}

func (d *acceptanceDriver) GenerateRecordWithKey(t *testing.T, op opencdc.Operation, key string) opencdc.Record {
	r := d.GenerateRecord(t, op)
	r.Key = opencdc.RawData(key)
	return r
}

func (d *acceptanceDriver) GenerateLargeRecord(t *testing.T, op opencdc.Operation, size int) opencdc.Record {
	r := d.GenerateRecord(t, op)
	prefix := string(r.Payload.After.Bytes())
	if pad := size - len(prefix); pad > 0 {
		prefix += strings.Repeat("x", pad)
	}
	r.Payload.After = opencdc.RawData(prefix)
	return r
}

// WriteToSource puts one object per record: key = record key, body = payload.
func (d *acceptanceDriver) WriteToSource(t *testing.T, recs []opencdc.Record) []opencdc.Record {
	ctx := context.Background()
	for _, r := range recs {
		if _, err := d.client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(d.bucket),
			Key:    aws.String(string(r.Key.Bytes())),
			Body:   bytes.NewReader(r.Payload.After.Bytes()),
		}); err != nil {
			t.Fatalf("seed object %s: %v", r.Key.Bytes(), err)
		}
	}
	return recs
}

// destinationLine is the JSON line shape destination/format/json.go writes.
type destinationLine struct {
	Operation string            `json:"Operation"`
	Position  string            `json:"Position"`
	Payload   string            `json:"Payload"`
	Key       string            `json:"Key"`
	Metadata  map[string]string `json:"Metadata"`
}

// ReadAllFromDestination returns every record in every object the
// destination wrote, duplicates included, so the replay cases can tell
// at-least-once from loss.
func (d *acceptanceDriver) ReadAllFromDestination(t *testing.T) []opencdc.Record {
	ctx := context.Background()
	var out []opencdc.Record
	p := s3.NewListObjectsV2Paginator(d.client, &s3.ListObjectsV2Input{Bucket: aws.String(d.bucket)})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			t.Fatalf("list %s: %v", d.bucket, err)
		}
		for _, o := range page.Contents {
			obj, err := d.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(d.bucket), Key: o.Key})
			if err != nil {
				t.Fatalf("get %s: %v", aws.ToString(o.Key), err)
			}
			body, err := io.ReadAll(obj.Body)
			_ = obj.Body.Close()
			if err != nil {
				t.Fatalf("read %s: %v", aws.ToString(o.Key), err)
			}
			sc := bufio.NewScanner(bytes.NewReader(body))
			sc.Buffer(make([]byte, 0, 1<<20), 16<<20)
			for sc.Scan() {
				var l destinationLine
				if err := json.Unmarshal(sc.Bytes(), &l); err != nil {
					t.Fatalf("object %s: bad JSON line: %v", aws.ToString(o.Key), err)
				}
				var op opencdc.Operation
				if err := op.UnmarshalText([]byte(l.Operation)); err != nil {
					t.Fatalf("object %s: bad operation %q: %v", aws.ToString(o.Key), l.Operation, err)
				}
				out = append(out, opencdc.Record{
					Position:  opencdc.Position(l.Position),
					Operation: op,
					Metadata:  l.Metadata,
					Key:       opencdc.RawData(l.Key),
					Payload:   opencdc.Change{After: opencdc.RawData(l.Payload)},
				})
			}
			if err := sc.Err(); err != nil {
				t.Fatalf("object %s: %v", aws.ToString(o.Key), err)
			}
		}
	}
	return out
}

func (d *acceptanceDriver) ReadFromDestination(t *testing.T, _ []opencdc.Record) []opencdc.Record {
	return d.ReadAllFromDestination(t)
}

// TestAcceptanceV2 runs SDK acceptance suite v2 against MinIO. It needs
// MINIO_ENDPOINT and the AWS_* variables `make test-integration-s3` sets, and
// is skipped without them. Set CONDUIT_ACCEPTANCE_REPORT to a fresh path to get
// the JSON report that certification reads.
func TestAcceptanceV2(t *testing.T) {
	endpoint := os.Getenv(envMinIOEndpoint)
	if endpoint == "" {
		t.Skipf("%s is not set; run `make test-integration-s3`", envMinIOEndpoint)
	}
	if v := os.Getenv("AWS_ENDPOINT_URL"); v != "" {
		t.Fatalf("AWS_ENDPOINT_URL is set (%q): the AWS SDK would use it instead of the connector's aws.endpoint; unset it", v)
	}
	sdk.AcceptanceTestV2(t, newAcceptanceDriver(endpoint))
}

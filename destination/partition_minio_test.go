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

package destination_test

import (
	"context"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/conduitio/conduit-commons/opencdc"
	s3Conn "github.com/conduitio/conduit-connector-s3"
	"github.com/conduitio/conduit-connector-s3/config"
	"github.com/conduitio/conduit-connector-s3/destination"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/google/uuid"
	"github.com/matryer/is"
)

// TestS3MinIO_Partitioners writes one batch through the production
// destination (with middleware) with each partitioner and checks the object
// keys and which records each object holds: the Kafka Connect S3 sink layout,
// <prefix>/<topic>/<encoded partition>/.
func TestS3MinIO_Partitioners(t *testing.T) {
	ctx := context.Background()
	endpoint := os.Getenv(EnvMinIOEndpoint)
	if endpoint == "" {
		t.Skipf("%v env var must be set", EnvMinIOEndpoint)
	}
	requireNoSDKEndpointEnv(t)
	env := getEnv(EnvAWSAccessKeyID, EnvAWSSecretAccessKey, EnvAWSS3Bucket, EnvAWSRegion)
	failOnEmptyEnv(t, env)
	env[EnvMinIOEndpoint] = endpoint
	bucket := env[EnvAWSS3Bucket]
	s3Client := newPathStyleS3Client(t, env)
	_, err := s3Client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
	if err != nil && !strings.Contains(err.Error(), "BucketAlreadyOwnedByYou") && !strings.Contains(err.Error(), "BucketAlreadyExists") {
		t.Fatalf("create bucket: %v", err)
	}

	h10 := time.Date(2026, 10, 14, 10, 15, 0, 0, time.UTC)
	h11 := time.Date(2026, 10, 14, 11, 45, 0, 0, time.UTC)
	mk := func(i int, topic, partition, region string, at time.Time) opencdc.Record {
		return opencdc.Record{
			Operation: opencdc.OperationCreate,
			Position:  opencdc.Position(fmt.Sprintf("p%d", i)),
			Key:       opencdc.RawData(fmt.Sprintf("k%d", i)),
			Metadata: opencdc.Metadata{
				opencdc.MetadataCollection: topic,
				"kafka.partition":          partition,
				opencdc.MetadataCreatedAt:  strconv.FormatInt(at.UnixNano(), 10),
			},
			Payload: opencdc.Change{After: opencdc.RawData(fmt.Sprintf(`{"region":%q,"i":%d}`, region, i))},
		}
	}
	batch := []opencdc.Record{
		mk(0, "orders", "0", "eu", h10),
		mk(1, "orders", "1", "us", h11),
		mk(2, "orders", "0", "eu", h11),
		mk(3, "payments", "0", "us", h10),
	}

	tests := []struct {
		name string
		cfg  map[string]string
		want map[string][]string // directory -> positions
	}{
		{"default", map[string]string{"partitioner": "default"}, map[string][]string{
			"orders/partition=0": {"p0", "p2"}, "orders/partition=1": {"p1"}, "payments/partition=0": {"p3"},
		}},
		{"field", map[string]string{"partitioner": "field", "partitioner.fields": "region"}, map[string][]string{
			"orders/region=eu": {"p0", "p2"}, "orders/region=us": {"p1"}, "payments/region=us": {"p3"},
		}},
		{"hourly", map[string]string{"partitioner": "hourly"}, map[string][]string{
			"orders/year=2026/month=10/day=14/hour=10":   {"p0"},
			"orders/year=2026/month=10/day=14/hour=11":   {"p1", "p2"},
			"payments/year=2026/month=10/day=14/hour=10": {"p3"},
		}},
		{"time, Hive-style daily from a payload field", map[string]string{
			"partitioner": "time", "partitioner.pathFormat": "'dt'=YYYY-MM-dd", "partitioner.duration": "24h",
			"partitioner.timestampExtractor": "recordField", "partitioner.timestampField": "i",
		}, map[string][]string{
			"orders/dt=1970-01-01": {"p0", "p1", "p2"}, "payments/dt=1970-01-01": {"p3"}, // i is epoch millis 0..3
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			prefix := "part-" + uuid.NewString()
			cfg := map[string]string{
				config.ConfigKeyAWSAccessKeyID:       env[EnvAWSAccessKeyID],
				config.ConfigKeyAWSSecretAccessKey:   env[EnvAWSSecretAccessKey],
				config.ConfigKeyAWSRegion:            env[EnvAWSRegion],
				config.ConfigKeyAWSBucket:            bucket,
				config.ConfigKeyAWSEndpoint:          endpoint,
				config.ConfigKeyAWSPathStyle:         "true",
				config.ConfigKeyPrefix:               prefix,
				destination.ConfigKeyFormat:          "json",
				"sdk.schema.extract.key.enabled":     "false",
				"sdk.schema.extract.payload.enabled": "false",
			}
			for k, v := range tc.cfg {
				cfg[k] = v
			}
			underTest := destination.NewDestination()
			is.NoErr(sdk.Util.ParseConfig(ctx, cfg, underTest.Config(), s3Conn.Connector.NewSpecification().DestinationParams))
			is.NoErr(underTest.Open(ctx))
			n, err := underTest.Write(ctx, batch)
			is.NoErr(err)
			is.Equal(n, len(batch))
			is.NoErr(underTest.Teardown(ctx))

			got := map[string][]string{}
			list, err := s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String(prefix + "/")})
			is.NoErr(err)
			for _, o := range list.Contents {
				key := strings.TrimPrefix(aws.ToString(o.Key), prefix+"/")
				dir := key[:strings.LastIndex(key, "/")]
				if !strings.HasSuffix(key, ".json") {
					t.Fatalf("unexpected object name %s", key)
				}
				obj, err := s3Client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: o.Key})
				is.NoErr(err)
				body, err := io.ReadAll(obj.Body)
				_ = obj.Body.Close()
				is.NoErr(err)
				for _, line := range strings.Split(strings.TrimSpace(string(body)), "\n") {
					var pos string
					if i := strings.Index(line, `"Position":"`); i >= 0 {
						rest := line[i+len(`"Position":"`):]
						pos = rest[:strings.IndexByte(rest, '"')]
					}
					got[dir] = append(got[dir], pos)
				}
			}
			for _, v := range got {
				sort.Strings(v)
			}
			is.Equal(got, tc.want)
		})
	}
}

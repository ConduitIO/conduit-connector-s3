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
	"os"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/conduitio/conduit-commons/opencdc"
	s3Conn "github.com/conduitio/conduit-connector-s3"
	"github.com/conduitio/conduit-connector-s3/config"
	"github.com/conduitio/conduit-connector-s3/destination"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/google/uuid"
)

// benchWriteRecords returns n create records with a small raw payload.
func benchWriteRecords(n int) []opencdc.Record {
	recs := make([]opencdc.Record, n)
	for i := range recs {
		recs[i] = opencdc.Record{
			Operation: opencdc.OperationCreate,
			Position:  opencdc.Position(fmt.Sprintf("pos-%d", i)),
			Key:       opencdc.RawData(fmt.Sprintf("key-%d", i)),
			Payload:   opencdc.Change{After: opencdc.RawData(fmt.Sprintf("this is a message #%d", i))},
			Metadata:  opencdc.Metadata{"opencdc.collection": "topic"},
		}
	}
	return recs
}

// BenchmarkWriteMinIO measures the full destination write path against a real
// MinIO endpoint: one PutObject per Write call. Run it with
// MINIO_ENDPOINT=http://localhost:9000 and the AWS_* env vars set (see the
// Makefile test-integration-s3 target). It shows the throughput gap between
// one record per object (the default: sdk.batch.size is 0) and a batch.
func BenchmarkWriteMinIO(b *testing.B) {
	endpoint := os.Getenv(EnvMinIOEndpoint)
	if endpoint == "" {
		b.Skipf("%v env var must be set", EnvMinIOEndpoint)
	}
	if v := os.Getenv(EnvAWSEndpointURL); v != "" {
		b.Fatalf("%s is set (%q): unset it before running the MinIO benchmark", EnvAWSEndpointURL, v)
	}
	env := getEnv(EnvAWSAccessKeyID, EnvAWSSecretAccessKey, EnvAWSS3Bucket, EnvAWSRegion)
	for k, v := range env {
		if v == "" {
			b.Fatalf("%v env var must be set for the MinIO benchmark", k)
		}
	}

	ctx := context.Background()
	bucket := env[EnvAWSS3Bucket]
	awsCfg, err := awsConfig.LoadDefaultConfig(
		ctx,
		awsConfig.WithRegion(env[EnvAWSRegion]),
		awsConfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			env[EnvAWSAccessKeyID], env[EnvAWSSecretAccessKey], "",
		)),
	)
	if err != nil {
		b.Fatalf("load aws config: %v", err)
	}
	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})
	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
	if err != nil && !strings.Contains(err.Error(), "BucketAlreadyOwnedByYou") && !strings.Contains(err.Error(), "BucketAlreadyExists") {
		b.Fatalf("create bucket: %v", err)
	}

	for _, n := range []int{1, 100, 1000} {
		b.Run(fmt.Sprintf("batch=%d", n), func(b *testing.B) {
			prefix := fmt.Sprintf("bench-%d-%s", n, uuid.NewString())
			d := destination.NewDestination()
			err := sdk.Util.ParseConfig(ctx, map[string]string{
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
			}, d.Config(), s3Conn.Connector.NewSpecification().DestinationParams)
			if err != nil {
				b.Fatal(err)
			}
			if err := d.Open(ctx); err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = d.Teardown(ctx) })

			recs := benchWriteRecords(n)
			b.ReportAllocs()
			b.SetBytes(int64(n))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				w, err := d.Write(ctx, recs)
				if err != nil || w != n {
					b.Fatalf("write: n=%d err=%v", w, err)
				}
			}
		})
	}
}

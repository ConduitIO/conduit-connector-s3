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

package destination_test

import (
	"context"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/conduitio/conduit-commons/opencdc"
	s3Conn "github.com/conduitio/conduit-connector-s3"
	"github.com/conduitio/conduit-connector-s3/config"
	"github.com/conduitio/conduit-connector-s3/destination"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/google/uuid"
	"github.com/matryer/is"
	"github.com/xitongsys/parquet-go-source/buffer"
	"github.com/xitongsys/parquet-go/reader"
)

// TestS3MinIO_UpdateAndDeleteRecords writes update and delete records (the
// shapes a CDC source produces: a delete has no After image) through the
// production destination (with middleware) and checks they are stored.
// Regression test for #1030.
func TestS3MinIO_UpdateAndDeleteRecords(t *testing.T) {
	is := is.New(t)
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

	for _, format := range []string{"json", "parquet"} {
		t.Run(format, func(t *testing.T) {
			is := is.New(t)
			underTest := destination.NewDestination()
			prefix := "ops-" + format + "-" + uuid.NewString() // unique per run: the bucket outlives the test
			err := sdk.Util.ParseConfig(ctx, map[string]string{
				config.ConfigKeyAWSAccessKeyID:       env[EnvAWSAccessKeyID],
				config.ConfigKeyAWSSecretAccessKey:   env[EnvAWSSecretAccessKey],
				config.ConfigKeyAWSRegion:            env[EnvAWSRegion],
				config.ConfigKeyAWSBucket:            bucket,
				config.ConfigKeyAWSEndpoint:          endpoint,
				config.ConfigKeyAWSPathStyle:         "true",
				config.ConfigKeyPrefix:               prefix,
				destination.ConfigKeyFormat:          format,
				"sdk.schema.extract.key.enabled":     "false",
				"sdk.schema.extract.payload.enabled": "false",
			}, underTest.Config(), s3Conn.Connector.NewSpecification().DestinationParams)
			is.NoErr(err)
			is.NoErr(underTest.Open(ctx))

			recs := []opencdc.Record{
				{
					Operation: opencdc.OperationUpdate, Position: opencdc.Position("p1"), Key: opencdc.RawData("k1"),
					Payload: opencdc.Change{Before: opencdc.RawData("old-1"), After: opencdc.RawData("new-1")},
				},
				{
					Operation: opencdc.OperationDelete, Position: opencdc.Position("p2"), Key: opencdc.RawData("k2"),
					Payload: opencdc.Change{Before: opencdc.RawData("old-2")},
				},
			}
			n, err := underTest.Write(ctx, recs)
			is.NoErr(err)
			is.Equal(n, len(recs))
			is.NoErr(underTest.Teardown(ctx))

			list, err := s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String(prefix + "/")})
			is.NoErr(err)
			is.Equal(len(list.Contents), 1) // one batch, one object, under this run's prefix
			obj, err := s3Client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: list.Contents[0].Key})
			is.NoErr(err)
			defer obj.Body.Close()
			body, err := io.ReadAll(obj.Body)
			is.NoErr(err)
			switch format {
			case "json":
				lines := strings.Split(strings.TrimSpace(string(body)), "\n")
				is.Equal(len(lines), 2)
				is.True(strings.Contains(lines[0], `"Operation":"update"`))
				is.True(strings.Contains(lines[0], `"Payload":"new-1"`))
				is.True(strings.Contains(lines[1], `"Operation":"delete"`))
				is.True(strings.Contains(lines[1], `"Key":"k2"`))
				is.True(strings.Contains(lines[1], `"Payload":""`)) // delete: no After image
			case "parquet":
				pr, err := reader.NewParquetReader(buffer.NewBufferFileFromBytes(body), new(struct {
					Operation string `parquet:"name=operation, type=BYTE_ARRAY"`
					Position  string `parquet:"name=position, type=BYTE_ARRAY"`
					Payload   string `parquet:"name=payload, type=BYTE_ARRAY"`
					Key       string `parquet:"name=key, type=BYTE_ARRAY"`
				}), 1)
				is.NoErr(err)
				defer pr.ReadStop()
				is.Equal(pr.GetNumRows(), int64(2))
				rows := make([]struct {
					Operation string `parquet:"name=operation, type=BYTE_ARRAY"`
					Position  string `parquet:"name=position, type=BYTE_ARRAY"`
					Payload   string `parquet:"name=payload, type=BYTE_ARRAY"`
					Key       string `parquet:"name=key, type=BYTE_ARRAY"`
				}, 2)
				is.NoErr(pr.Read(&rows))
				is.Equal(rows[0].Operation, "update")
				is.Equal(rows[0].Payload, "new-1")
				is.Equal(rows[1].Operation, "delete")
				is.Equal(rows[1].Key, "k2")
				is.Equal(rows[1].Payload, "")
			}
		})
	}
}

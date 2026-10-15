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
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
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

// TestS3MinIO_ServerSideEncryption writes through the production destination
// with and without aws.serverSideEncryption and reads the stored object's
// encryption back. With no setting no header is sent (#1047), so MinIO, which
// has no bucket default encryption here, stores the object unencrypted; with
// AES256 the object is stored with SSE-S3.
func TestS3MinIO_ServerSideEncryption(t *testing.T) {
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

	for _, tt := range []struct {
		name string
		sse  string
	}{
		{"default", ""},
		{"AES256", "AES256"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			is := is.New(t)
			underTest := destination.NewDestination()
			prefix := "sse-" + tt.name + "-" + uuid.NewString()
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
			if tt.sse != "" {
				cfg[destination.ConfigKeyServerSideEncryption] = tt.sse
			}
			is.NoErr(sdk.Util.ParseConfig(ctx, cfg, underTest.Config(), s3Conn.Connector.NewSpecification().DestinationParams))
			is.NoErr(underTest.Open(ctx))
			n, err := underTest.Write(ctx, []opencdc.Record{{
				Operation: opencdc.OperationCreate, Position: opencdc.Position("p1"), Key: opencdc.RawData("k1"),
				Payload: opencdc.Change{After: opencdc.RawData("v1")},
			}})
			is.NoErr(err)
			is.Equal(n, 1)
			is.NoErr(underTest.Teardown(ctx))

			list, err := s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String(prefix + "/")})
			is.NoErr(err)
			is.Equal(len(list.Contents), 1)
			head, err := s3Client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: list.Contents[0].Key})
			is.NoErr(err)
			is.Equal(string(head.ServerSideEncryption), tt.sse)
		})
	}
}

// TestS3MinIO_BucketDefaultKMSApplies is the #1047 scenario: a bucket whose
// default encryption is SSE-KMS, written with the default configuration. The
// hardcoded AES256 header used to override it with SSE-S3; now no header is
// sent and the object is stored with the bucket's aws:kms default.
func TestS3MinIO_BucketDefaultKMSApplies(t *testing.T) {
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
	s3Client := newPathStyleS3Client(t, env)

	// A bucket of its own: default encryption is bucket-wide.
	bucket := "sse-kms-" + strings.ToLower(uuid.NewString()[:8])
	_, err := s3Client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
	is.NoErr(err)
	t.Cleanup(func() {
		list, _ := s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(bucket)})
		if list != nil {
			for _, o := range list.Contents {
				_, _ = s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: o.Key})
			}
		}
		_, _ = s3Client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
	})
	// my-minio-key is the key MINIO_KMS_SECRET_KEY defines in test/docker-compose.yml.
	_, err = s3Client.PutBucketEncryption(ctx, &s3.PutBucketEncryptionInput{
		Bucket: aws.String(bucket),
		ServerSideEncryptionConfiguration: &types.ServerSideEncryptionConfiguration{Rules: []types.ServerSideEncryptionRule{{
			ApplyServerSideEncryptionByDefault: &types.ServerSideEncryptionByDefault{
				SSEAlgorithm:   types.ServerSideEncryptionAwsKms,
				KMSMasterKeyID: aws.String("my-minio-key"),
			},
		}}},
	})
	is.NoErr(err)

	underTest := destination.NewDestination()
	cfg := map[string]string{
		config.ConfigKeyAWSAccessKeyID:       env[EnvAWSAccessKeyID],
		config.ConfigKeyAWSSecretAccessKey:   env[EnvAWSSecretAccessKey],
		config.ConfigKeyAWSRegion:            env[EnvAWSRegion],
		config.ConfigKeyAWSBucket:            bucket,
		config.ConfigKeyAWSEndpoint:          endpoint,
		config.ConfigKeyAWSPathStyle:         "true",
		destination.ConfigKeyFormat:          "json",
		"sdk.schema.extract.key.enabled":     "false",
		"sdk.schema.extract.payload.enabled": "false",
	}
	is.NoErr(sdk.Util.ParseConfig(ctx, cfg, underTest.Config(), s3Conn.Connector.NewSpecification().DestinationParams))
	is.NoErr(underTest.Open(ctx))
	n, err := underTest.Write(ctx, []opencdc.Record{{
		Operation: opencdc.OperationCreate, Position: opencdc.Position("p1"), Key: opencdc.RawData("k1"),
		Payload: opencdc.Change{After: opencdc.RawData("v1")},
	}})
	is.NoErr(err)
	is.Equal(n, 1)
	is.NoErr(underTest.Teardown(ctx))

	list, err := s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(bucket)})
	is.NoErr(err)
	is.Equal(len(list.Contents), 1)
	head, err := s3Client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: list.Contents[0].Key})
	is.NoErr(err)
	is.Equal(head.ServerSideEncryption, types.ServerSideEncryptionAwsKms) // the bucket default, not SSE-S3
}

// TestS3MinIO_TwoDestinationsNeverOverwrite is the integration test for
// s3#1064: two destination instances on one bucket and prefix (two pipelines,
// or two Conduit instances) write concurrently. Every acked batch must be its
// own object; with keys from the clock alone, writes in the same tick
// overwrote each other. Conditional writes are on (the default).
func TestS3MinIO_TwoDestinationsNeverOverwrite(t *testing.T) {
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
	prefix := "two-writers-" + uuid.NewString()
	open := func() sdk.Destination {
		d := destination.NewDestination()
		is.NoErr(sdk.Util.ParseConfig(ctx, map[string]string{
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
		}, d.Config(), s3Conn.Connector.NewSpecification().DestinationParams))
		is.NoErr(d.Open(ctx))
		t.Cleanup(func() { _ = d.Teardown(ctx) })
		return d
	}
	const n = 300
	ds := []sdk.Destination{open(), open()}
	var wg sync.WaitGroup
	for di, d := range ds {
		wg.Add(1)
		go func(di int, d sdk.Destination) {
			defer wg.Done()
			for i := 0; i < n; i++ {
				w, err := d.Write(ctx, []opencdc.Record{{
					Operation: opencdc.OperationCreate, Position: opencdc.Position(fmt.Sprintf("d%d-%d", di, i)),
					Payload: opencdc.Change{After: opencdc.RawData(fmt.Sprintf("d%d-%d", di, i))},
				}})
				if err != nil || w != 1 {
					t.Errorf("write: %d %v", w, err)
					return
				}
			}
		}(di, d)
	}
	wg.Wait()

	objects := 0
	p := s3.NewListObjectsV2Paginator(s3Client, &s3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String(prefix + "/")})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		is.NoErr(err)
		objects += len(page.Contents)
	}
	is.Equal(objects, len(ds)*n) // one object per acked batch: none overwritten
}

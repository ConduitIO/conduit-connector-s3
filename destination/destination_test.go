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
	"net"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/conduitio/conduit-commons/opencdc"
	s3Conn "github.com/conduitio/conduit-connector-s3"
	"github.com/conduitio/conduit-connector-s3/config"
	"github.com/conduitio/conduit-connector-s3/destination"
	"github.com/conduitio/conduit-connector-s3/destination/filevalidator"
	"github.com/conduitio/conduit-connector-s3/destination/writer"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
)

const (
	EnvAWSAccessKeyID     = "AWS_ACCESS_KEY_ID"
	EnvAWSSecretAccessKey = "AWS_SECRET_ACCESS_KEY"
	EnvAWSS3Bucket        = "AWS_S3_BUCKET"
	EnvAWSRegion          = "AWS_REGION"
	EnvAWSEndpoint        = "AWS_ENDPOINT_URL"
)

func TestLocalParquet(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	underTest := &destination.Destination{}

	cfg := map[string]string{
		config.ConfigKeyAWSAccessKeyID:     "123",
		config.ConfigKeyAWSSecretAccessKey: "secret",
		config.ConfigKeyAWSRegion:          "us-west-2",
		config.ConfigKeyAWSBucket:          "foobucket",
		destination.ConfigKeyFormat:        "parquet",
	}
	err := sdk.Util.ParseConfig(ctx, cfg, underTest.Config(), s3Conn.Connector.NewSpecification().DestinationParams)
	is.NoErr(err) // failed to parse the configuration

	err = underTest.Open(ctx)
	is.NoErr(err) // failed to open the destination

	underTest.Writer = &writer.Local{
		Path: "./fixtures",
	}

	// generate 50 records and write them in 2 batches
	records := generateRecords(50)
	count, err := underTest.Write(ctx, records[:25])
	is.NoErr(err)
	is.Equal(count, 25)

	count, err = underTest.Write(ctx, records[25:])
	is.NoErr(err)
	is.Equal(count, 25)

	err = underTest.Teardown(ctx)
	is.NoErr(err)

	// The code above should produce two files in the fixtures directory:
	// - local-0001.parquet
	// - local-0002.parquet
	// ... that we would compare to two reference files to make sure they're correct.

	validator := &filevalidator.Local{
		Path: "./fixtures",
	}

	err = validateReferences(
		validator,
		"local-0001.parquet", "reference-1.parquet",
		"local-0002.parquet", "reference-2.parquet",
	)
	is.NoErr(err)
}

func TestLocalJSON(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	underTest := &destination.Destination{}

	cfg := map[string]string{
		config.ConfigKeyAWSAccessKeyID:     "123",
		config.ConfigKeyAWSSecretAccessKey: "secret",
		config.ConfigKeyAWSRegion:          "us-west-2",
		config.ConfigKeyAWSBucket:          "foobucket",
		destination.ConfigKeyFormat:        "json",
	}
	err := sdk.Util.ParseConfig(ctx, cfg, underTest.Config(), s3Conn.Connector.NewSpecification().DestinationParams)
	is.NoErr(err) // failed to parse the configuration

	err = underTest.Open(context.Background())
	is.NoErr(err) // failed to open the destination

	underTest.Writer = &writer.Local{
		Path: "./fixtures",
	}

	// generate 50 records and write them in 2 batches
	records := generateRecords(50)
	count, err := underTest.Write(ctx, records[:25])
	is.NoErr(err)
	is.Equal(count, 25)

	count, err = underTest.Write(ctx, records[25:])
	is.NoErr(err)
	is.Equal(count, 25)

	err = underTest.Teardown(ctx)
	is.NoErr(err)

	// The code above should produce two files in the fixtures directory:
	// - local-0001.json
	// - local-0002.json
	// ... that we would compare to two reference files to make sure they're correct.

	validator := &filevalidator.Local{
		Path: "./fixtures",
	}

	err = validateReferences(
		validator,
		"local-0001.json", "reference-1.json",
		"local-0002.json", "reference-2.json",
	)
	is.NoErr(err)
}

func TestS3Parquet(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	env := getEnv(
		EnvAWSAccessKeyID,
		EnvAWSSecretAccessKey,
		EnvAWSS3Bucket,
		EnvAWSRegion,
	)
	skipOnEmptyEnv(t, env)

	underTest := &destination.Destination{}

	cfg := map[string]string{
		config.ConfigKeyAWSAccessKeyID:     env[EnvAWSAccessKeyID],
		config.ConfigKeyAWSSecretAccessKey: env[EnvAWSSecretAccessKey],
		config.ConfigKeyAWSRegion:          env[EnvAWSRegion],
		config.ConfigKeyAWSBucket:          env[EnvAWSS3Bucket],
		config.ConfigKeyPrefix:             "test",
		destination.ConfigKeyFormat:        "parquet",
	}

	err := sdk.Util.ParseConfig(ctx, cfg, underTest.Config(), s3Conn.Connector.NewSpecification().DestinationParams)
	is.NoErr(err) // failed to parse the configuration

	err = underTest.Open(ctx)
	is.NoErr(err) // failed to initialize destination

	// generate 50 records and write them in 2 batches
	records := generateRecords(50)
	count, err := underTest.Write(ctx, records[:25])
	is.NoErr(err)
	is.Equal(count, 25)

	count, err = underTest.Write(ctx, records[25:])
	is.NoErr(err)
	is.Equal(count, 25)

	writer, ok := underTest.Writer.(*writer.S3)
	is.True(ok) // Destination writer expected to be writer.S3

	err = underTest.Teardown(ctx)
	is.NoErr(err)

	// check if only two files are written
	is.Equal(len(writer.FilesWritten), 2) // Expected writer to have written 2 files

	validator := &filevalidator.S3{
		AccessKeyID:     env[EnvAWSAccessKeyID],
		SecretAccessKey: env[EnvAWSSecretAccessKey],
		Bucket:          env[EnvAWSS3Bucket],
		Region:          env[EnvAWSRegion],
	}

	err = validateReferences(
		validator,
		writer.FilesWritten[0], "reference-1.parquet",
		writer.FilesWritten[1], "reference-2.parquet",
	)
	is.NoErr(err)
}

// TestS3MinIO exercises the destination write path against a MinIO
// S3-compatible store (test/docker-compose.yml, `make test-integration-s3`).
// It is the regression test for ConduitIO/conduit-connector-s3#963: without a
// way to configure a custom endpoint and path-style addressing, the connector
// reached MinIO with virtual-hosted addressing (<bucket>.localhost:9000,
// RFC 6761), which the default MinIO setup does not recognize: it misparses
// the bucket as part of the object key and answers PutObject with a 400
// "MalformedXML: The XML you provided was not well-formed". The test
// therefore configures aws.endpoint and aws.pathStyle and asserts the write
// succeeds and the object actually lands: on the unfixed code the PutObject
// fails, on the fixed code it succeeds.
//
// It requires the following environment variables (set by `make
// test-integration-s3`) and is skipped otherwise: AWS_ENDPOINT_URL,
// AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_S3_BUCKET, AWS_REGION.
func TestS3MinIO(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()

	env := getEnv(
		EnvAWSAccessKeyID,
		EnvAWSSecretAccessKey,
		EnvAWSS3Bucket,
		EnvAWSRegion,
		EnvAWSEndpoint,
	)
	skipOnEmptyEnv(t, env)

	// The env vars may be set while no MinIO is running (e.g. a local shell
	// with leftovers): probe the endpoint and skip instead of hard-failing.
	if !endpointReachable(env[EnvAWSEndpoint]) {
		t.Skipf("endpoint %q not reachable, skipping MinIO integration test", env[EnvAWSEndpoint])
	}

	// Create the bucket with a path-style client. The connector's own client
	// is configured with the same addressing style below.
	bucket := env[EnvAWSS3Bucket]
	s3Client := newPathStyleS3Client(t, env)
	_, err := s3Client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
	if err != nil && !strings.Contains(err.Error(), "BucketAlreadyOwnedByYou") && !strings.Contains(err.Error(), "BucketAlreadyExists") {
		t.Fatalf("create bucket: %v", err)
	}

	underTest := &destination.Destination{}

	cfg := map[string]string{
		config.ConfigKeyAWSAccessKeyID:     env[EnvAWSAccessKeyID],
		config.ConfigKeyAWSSecretAccessKey: env[EnvAWSSecretAccessKey],
		config.ConfigKeyAWSRegion:          env[EnvAWSRegion],
		config.ConfigKeyAWSBucket:          bucket,
		config.ConfigKeyAWSEndpoint:        env[EnvAWSEndpoint],
		config.ConfigKeyAWSPathStyle:       "true",
		config.ConfigKeyPrefix:             "test",
		destination.ConfigKeyFormat:        "json",
	}
	err = sdk.Util.ParseConfig(ctx, cfg, underTest.Config(), s3Conn.Connector.NewSpecification().DestinationParams)
	is.NoErr(err) // failed to parse the configuration

	err = underTest.Open(ctx)
	is.NoErr(err) // failed to initialize destination

	// generate 50 records and write them in a single batch
	records := generateRecords(50)
	count, err := underTest.Write(ctx, records)
	is.NoErr(err) // PutObject against MinIO failed, see issue #963
	is.Equal(count, 50)

	s3Writer, ok := underTest.Writer.(*writer.S3)
	is.True(ok) // Destination writer expected to be writer.S3

	err = underTest.Teardown(ctx)
	is.NoErr(err)

	// the object must have actually landed, not just reported success
	is.Equal(len(s3Writer.FilesWritten), 1)
	obj, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(s3Writer.FilesWritten[0]),
	})
	is.NoErr(err)
	defer obj.Body.Close()
	body, err := io.ReadAll(obj.Body)
	is.NoErr(err)
	is.True(strings.Contains(string(body), `"this is a message #1"`))
}

// endpointReachable reports whether the endpoint's host:port accepts TCP
// connections. Used to skip the MinIO integration tests when the environment
// variables are set but no MinIO is running.
func endpointReachable(endpoint string) bool {
	u, err := url.Parse(endpoint)
	if err != nil || u.Host == "" {
		return false
	}
	conn, err := net.DialTimeout("tcp", u.Host, 2*time.Second)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

// newPathStyleS3Client returns an S3 client using path-style addressing, used
// by the MinIO integration test for bucket setup and verification.
func newPathStyleS3Client(t *testing.T, env map[string]string) *s3.Client {
	t.Helper()
	cfg, err := awsConfig.LoadDefaultConfig(
		context.Background(),
		awsConfig.WithRegion(env[EnvAWSRegion]),
		awsConfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			env[EnvAWSAccessKeyID],
			env[EnvAWSSecretAccessKey],
			"",
		)),
	)
	if err != nil {
		t.Fatalf("load aws config: %v", err)
	}
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(env[EnvAWSEndpoint])
		o.UsePathStyle = true
	})
}

func generateRecords(count int) []opencdc.Record {
	var result []opencdc.Record

	for i := 0; i < count; i++ {
		result = append(result, opencdc.Record{
			Operation: opencdc.OperationCreate,
			Position:  []byte(strconv.Itoa(i)),
			Payload: opencdc.Change{
				After: opencdc.RawData(fmt.Sprintf("this is a message #%d", i+1)),
			},
			Key: opencdc.RawData(fmt.Sprintf("key-%d", i)),
			Metadata: map[string]string{
				opencdc.MetadataCreatedAt: strconv.FormatInt(time.Date(2020, 1, 1, 1, 0, 0, 0, time.UTC).Add(time.Duration(i)*time.Second).UnixNano(), 10),
			},
		})
	}

	return result
}

func validateReferences(validator filevalidator.FileValidator, paths ...string) error {
	for i := 0; i < len(paths); i += 2 {
		fileName := paths[i]
		referencePath := paths[i+1]
		reference, err := os.ReadFile(path.Join("./fixtures", referencePath))
		if err != nil {
			return err
		}

		err = validator.Validate(fileName, reference)
		if err != nil {
			return err
		}
	}

	return nil
}

func getEnv(keys ...string) map[string]string {
	envVars := make(map[string]string, len(keys))
	for _, k := range keys {
		envVars[k] = os.Getenv(k)
	}
	return envVars
}

func skipOnEmptyEnv(t *testing.T, vars map[string]string) {
	for k, v := range vars {
		if v == "" {
			t.Skipf("%v env var must be set", k)
		}
	}
}

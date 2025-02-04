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
	"os"
	"path"
	"strconv"
	"testing"
	"time"

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

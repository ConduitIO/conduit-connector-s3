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
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/conduitio/conduit-commons/opencdc"
	s3Conn "github.com/conduitio/conduit-connector-s3"
	"github.com/conduitio/conduit-connector-s3/config"
	"github.com/conduitio/conduit-connector-s3/source"
	"github.com/conduitio/conduit-connector-s3/source/position"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/google/uuid"
	"github.com/matryer/is"
)

type Object struct {
	key      string
	content  string
	metadata map[string]string
}

func TestSource_SuccessfulSnapshot(t *testing.T) {
	is := is.New(t)
	client, cfg := prepareIntegrationTest(t)

	ctx := context.Background()
	testBucket := cfg[config.ConfigKeyAWSBucket]
	underTest := &source.Source{}
	err := sdk.Util.ParseConfig(ctx, cfg, underTest.Config(), s3Conn.Connector.NewSpecification().SourceParams)
	is.NoErr(err) // failed to configure the source

	err = underTest.Open(ctx, nil)
	is.NoErr(err) // failed to open the source

	testFiles := addObjectsToBucket(ctx, t, testBucket, "", client, 5)

	// read and assert
	var lastPosition opencdc.Position
	for _, file := range testFiles {
		rec, err := readAndAssert(ctx, t, underTest, file)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		lastPosition = rec.Position
	}

	// assert last position from snapshot has a CDC type
	pos, _ := position.ParseRecordPosition(lastPosition)
	is.Equal(pos.Type, position.TypeCDC)

	_, err = underTest.Read(ctx)
	is.True(errors.Is(err, sdk.ErrBackoffRetry))

	_ = underTest.Teardown(ctx)
}

func TestSource_SnapshotRestart(t *testing.T) {
	is := is.New(t)
	client, cfg := prepareIntegrationTest(t)

	ctx := context.Background()
	testBucket := cfg[config.ConfigKeyAWSBucket]
	underTest := &source.Source{}
	err := sdk.Util.ParseConfig(ctx, cfg, underTest.Config(), s3Conn.Connector.NewSpecification().SourceParams)
	is.NoErr(err) // failed to parse the configuration

	// set a non nil position
	err = underTest.Open(ctx, []byte("file3_s0"))
	is.NoErr(err) // failed to open the source

	testFiles := addObjectsToBucket(ctx, t, testBucket, "", client, 10)

	// read and assert
	for _, file := range testFiles {
		// first position is not nil, then snapshot will start from beginning
		_, err := readAndAssert(ctx, t, underTest, file)
		is.NoErr(err)
	}
	_ = underTest.Teardown(ctx)
}

func TestSource_EmptyBucket(t *testing.T) {
	is := is.New(t)
	_, cfg := prepareIntegrationTest(t)

	ctx := context.Background()
	underTest := &source.Source{}
	err := sdk.Util.ParseConfig(ctx, cfg, underTest.Config(), s3Conn.Connector.NewSpecification().SourceParams)
	is.NoErr(err)

	err = underTest.Open(context.Background(), nil)
	is.NoErr(err)

	_, err = underTest.Read(ctx)
	is.True(errors.Is(err, sdk.ErrBackoffRetry))

	_ = underTest.Teardown(ctx)
}

func TestSource_StartCDCAfterEmptyBucket(t *testing.T) {
	is := is.New(t)
	client, cfg := prepareIntegrationTest(t)

	ctx := context.Background()
	testBucket := cfg[config.ConfigKeyAWSBucket]
	underTest := &source.Source{}
	err := sdk.Util.ParseConfig(ctx, cfg, underTest.Config(), s3Conn.Connector.NewSpecification().SourceParams)
	is.NoErr(err)

	err = underTest.Open(context.Background(), nil)
	is.NoErr(err)

	// read bucket while empty
	_, err = underTest.Read(ctx)
	is.True(errors.Is(err, sdk.ErrBackoffRetry))

	// write files to bucket
	addObjectsToBucket(ctx, t, testBucket, "", client, 3)

	// read one record and assert position type is CDC
	obj, err := readWithTimeout(ctx, underTest, time.Second*10)
	if err != nil {
		t.Fatal(err)
	}

	pos, _ := position.ParseRecordPosition(obj.Position)
	is.Equal(pos.Type, position.TypeCDC)

	_ = underTest.Teardown(ctx)
}

func TestSource_NonExistentBucket(t *testing.T) {
	is := is.New(t)
	_, cfg := prepareIntegrationTest(t)
	ctx := context.Background()

	underTest := &source.Source{}

	// set the bucket name to a unique uuid
	cfg[config.ConfigKeyAWSBucket] = uuid.NewString()

	err := sdk.Util.ParseConfig(ctx, cfg, underTest.Config(), s3Conn.Connector.NewSpecification().SourceParams)
	is.NoErr(err) // error while configuring the source

	// bucket existence check at "Open"
	err = underTest.Open(context.Background(), nil)
	is.True(err != nil) // should return an error for non-existent buckets
}

func TestSource_CDC_ReadRecordsInsert(t *testing.T) {
	is := is.New(t)
	client, cfg := prepareIntegrationTest(t)

	ctx := context.Background()
	testBucket := cfg[config.ConfigKeyAWSBucket]
	underTest := &source.Source{}
	err := sdk.Util.ParseConfig(ctx, cfg, underTest.Config(), s3Conn.Connector.NewSpecification().SourceParams)
	is.NoErr(err)

	err = underTest.Open(context.Background(), nil)
	is.NoErr(err)

	testFiles := addObjectsToBucket(ctx, t, testBucket, "", client, 3)

	// read and assert
	for _, file := range testFiles {
		_, err := readAndAssert(ctx, t, underTest, file)
		is.NoErr(err)
	}

	// make sure the update action has a different lastModifiedDate
	// because CDC iterator detects files from after maxLastModifiedDate by initial load
	time.Sleep(time.Second)

	content := uuid.NewString()
	buf := strings.NewReader(content)
	testFileName := "test-file"
	// insert a file to the bucket
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(testBucket),
		Key:           aws.String(testFileName),
		Body:          buf,
		ContentLength: aws.Int64(int64(buf.Len())),
	})
	is.NoErr(err)

	obj, err := readWithTimeout(ctx, underTest, time.Second*15)
	is.NoErr(err)

	// the insert should have been detected
	is.Equal(string(obj.Key.Bytes()), testFileName)

	_ = underTest.Teardown(ctx)
}

func TestSource_CDC_UpdateWithVersioning(t *testing.T) {
	is := is.New(t)
	client, cfg := prepareIntegrationTest(t)

	ctx := context.Background()
	testBucket := cfg[config.ConfigKeyAWSBucket]
	underTest := &source.Source{}

	// make the bucket versioned
	_, err := client.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
		Bucket:                  aws.String(testBucket),
		VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled},
	})
	is.NoErr(err) // couldn't create a versioned bucket

	err = sdk.Util.ParseConfig(ctx, cfg, underTest.Config(), s3Conn.Connector.NewSpecification().SourceParams)
	is.NoErr(err)

	err = underTest.Open(context.Background(), nil)
	is.NoErr(err)

	testFiles := addObjectsToBucket(ctx, t, testBucket, "", client, 1)

	// read and assert
	_, err = readAndAssert(ctx, t, underTest, testFiles[0])
	is.NoErr(err)

	// make sure the update action has a different lastModifiedDate
	// because CDC iterator detects files from after maxLastModifiedDate by initial load
	time.Sleep(time.Second)

	content := uuid.NewString()
	buf := strings.NewReader(content)
	testFileName := testFiles[0].key
	expectedOperation := opencdc.OperationUpdate
	// PutObject here will update an already existing object, this would just change the lastModified date
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(testBucket),
		Key:           aws.String(testFileName),
		Body:          buf,
		ContentLength: aws.Int64(int64(buf.Len())),
	})
	is.NoErr(err)

	obj, err := readWithTimeout(ctx, underTest, time.Second*10)
	is.NoErr(err)

	// the update should be detected

	is.Equal(string(obj.Key.Bytes()), testFileName)
	is.Equal(string(obj.Payload.After.Bytes()), content)
	is.Equal(obj.Operation, expectedOperation)

	_ = underTest.Teardown(ctx)
}

func TestSource_CDC_DeleteWithVersioning(t *testing.T) {
	is := is.New(t)
	client, cfg := prepareIntegrationTest(t)

	ctx := context.Background()
	testBucket := cfg[config.ConfigKeyAWSBucket]
	underTest := &source.Source{}
	err := sdk.Util.ParseConfig(ctx, cfg, underTest.Config(), s3Conn.Connector.NewSpecification().SourceParams)
	is.NoErr(err)

	err = underTest.Open(context.Background(), nil)
	is.NoErr(err)

	testFiles := addObjectsToBucket(ctx, t, testBucket, "", client, 5)

	// make the bucket versioned
	_, err = client.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
		Bucket:                  aws.String(testBucket),
		VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled},
	})
	is.NoErr(err) // couldn't create a versioned bucket

	// read and assert
	for _, file := range testFiles {
		_, err := readAndAssert(ctx, t, underTest, file)
		is.NoErr(err) // unexpected error
	}

	// make sure the update action has a different lastModifiedDate
	// because CDC iterator detects files from after maxLastModifiedDate by initial load
	time.Sleep(time.Second)

	testFileName := "file0001" // already exists in the bucket
	expectedOperation := opencdc.OperationDelete
	// Delete a file that exists in the bucket
	_, err = client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(testBucket),
		Key:    aws.String(testFileName),
	})
	is.NoErr(err)

	obj, err := readWithTimeout(ctx, underTest, time.Second*10)
	is.NoErr(err)

	is.Equal(string(obj.Key.Bytes()), testFileName)
	is.Equal(obj.Operation, expectedOperation)

	err = underTest.Teardown(ctx)
	is.NoErr(err)
}

func TestSource_CDC_EmptyBucketWithDeletedObjects(t *testing.T) {
	is := is.New(t)
	client, cfg := prepareIntegrationTest(t)

	ctx := context.Background()
	testBucket := cfg[config.ConfigKeyAWSBucket]
	underTest := &source.Source{}
	err := sdk.Util.ParseConfig(ctx, cfg, underTest.Config(), s3Conn.Connector.NewSpecification().SourceParams)
	is.NoErr(err)

	err = underTest.Open(context.Background(), nil)
	is.NoErr(err)

	// make the bucket versioned
	_, err = client.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
		Bucket:                  aws.String(testBucket),
		VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled},
	})
	is.NoErr(err) // couldn't create a versioned bucket

	// add one file
	testFiles := addObjectsToBucket(ctx, t, testBucket, "", client, 1)

	// delete the added file
	testFileName := "file0000"
	// Delete a file that exists in the bucket
	_, err = client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(testBucket),
		Key:    aws.String(testFileName),
	})
	is.NoErr(err)

	// we need the deleted file's modified date to be in the past
	time.Sleep(time.Second)

	// read and assert
	for _, file := range testFiles {
		_, err := readAndAssert(ctx, t, underTest, file)
		is.True(errors.Is(err, sdk.ErrBackoffRetry)) // unexpected error
	}

	// should have changed to CDC
	// CDC should NOT read the deleted object
	_, err = readWithTimeout(ctx, underTest, time.Second)
	is.True(errors.Is(err, context.DeadlineExceeded)) // error should be DeadlineExceeded

	err = underTest.Teardown(ctx)
	is.NoErr(err)
}

func TestSource_CDCPosition(t *testing.T) {
	is := is.New(t)
	client, cfg := prepareIntegrationTest(t)

	ctx := context.Background()
	testBucket := cfg[config.ConfigKeyAWSBucket]
	underTest := &source.Source{}
	err := sdk.Util.ParseConfig(ctx, cfg, underTest.Config(), s3Conn.Connector.NewSpecification().SourceParams)
	is.NoErr(err)

	addObjectsToBucket(ctx, t, testBucket, "", client, 2)

	// make the bucket versioned
	_, err = client.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
		Bucket:                  aws.String(testBucket),
		VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled},
	})
	is.NoErr(err) // couldn't create a versioned bucket

	testFileName := "file0001" // already exists in the bucket
	expectedOperation := opencdc.OperationDelete
	// Delete a file that exists in the bucket
	_, err = client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(testBucket),
		Key:    aws.String(testFileName),
	})
	is.NoErr(err)

	// initialize the connector to start detecting changes from the past, so all the bucket is new data
	err = underTest.Open(context.Background(), []byte("file0001_c1634049397"))
	is.NoErr(err)

	_, err = underTest.Read(ctx)
	// error is expected after resetting the connector with a new CDC position
	is.True(err != nil) // S3 connector should return a BackoffRetry error for the first Read() call after starting CDC

	obj, err := readWithTimeout(ctx, underTest, time.Second*10)
	is.NoErr(err)
	// the Read should return the first file from the bucket, since in has the oldest modified date
	is.Equal(string(obj.Key.Bytes()), "file0000")

	// next read should return the deleted file
	obj2, err := readWithTimeout(ctx, underTest, time.Second*10)
	is.NoErr(err)
	is.Equal(string(obj2.Key.Bytes()), testFileName)
	is.Equal(obj2.Operation, expectedOperation)

	err = underTest.Teardown(ctx)
	is.NoErr(err)
}

func TestSource_SnapshotWithPrefix(t *testing.T) {
	is := is.New(t)
	client, cfg := prepareIntegrationTest(t)

	ctx := context.Background()
	testBucket := cfg[config.ConfigKeyAWSBucket]
	testPrefix := "conduit-test-snapshot-prefix-"
	cfg[config.ConfigKeyPrefix] = testPrefix
	underTest := &source.Source{}
	err := sdk.Util.ParseConfig(ctx, cfg, underTest.Config(), s3Conn.Connector.NewSpecification().SourceParams)
	is.NoErr(err)

	err = underTest.Open(context.Background(), nil)
	is.NoErr(err)

	// put two items without prefix
	_ = addObjectsToBucket(ctx, t, testBucket, "", client, 2)
	// put two items with prefix
	testFiles := addObjectsToBucket(ctx, t, testBucket, testPrefix, client, 2)

	// read and assert
	for _, file := range testFiles {
		_, err := readAndAssert(ctx, t, underTest, file)
		is.NoErr(err) // unexpected error
	}

	// should return ErrBackoffRetry and not read items without the specified prefix
	_, err = underTest.Read(ctx)
	is.True(errors.Is(err, sdk.ErrBackoffRetry))

	err = underTest.Teardown(ctx)
	is.NoErr(err)
}

func TestSource_CDCWithPrefix(t *testing.T) {
	is := is.New(t)
	client, cfg := prepareIntegrationTest(t)

	ctx := context.Background()
	testBucket := cfg[config.ConfigKeyAWSBucket]
	testPrefix := "conduit-test-cdc-prefix-"
	cfg[config.ConfigKeyPrefix] = testPrefix
	underTest := &source.Source{}
	err := sdk.Util.ParseConfig(ctx, cfg, underTest.Config(), s3Conn.Connector.NewSpecification().SourceParams)
	is.NoErr(err)

	err = underTest.Open(context.Background(), nil)
	is.NoErr(err)

	// should return ErrBackoffRetry, switch to CDC
	_, err = underTest.Read(ctx)
	is.True(errors.Is(err, sdk.ErrBackoffRetry))

	// put more items for CDC iterator
	_ = addObjectsToBucket(ctx, t, testBucket, "", client, 2)
	testFiles := addObjectsToBucket(ctx, t, testBucket, testPrefix, client, 1)

	// read one record
	obj, err := readWithTimeout(ctx, underTest, time.Second*10)
	is.NoErr(err)

	// compare the keys
	gotKey := string(obj.Key.Bytes())
	is.Equal(gotKey, testFiles[0].key)

	// should return ErrBackoffRetry and not read items without the specified prefix
	_, err = underTest.Read(ctx)
	is.True(errors.Is(err, sdk.ErrBackoffRetry))

	err = underTest.Teardown(ctx)
	is.NoErr(err)
}

func prepareIntegrationTest(t *testing.T) (*s3.Client, map[string]string) {
	cfg, err := parseIntegrationConfig()
	if err != nil {
		t.Skip(err)
	}

	client, err := newS3Client(cfg)
	if err != nil {
		t.Fatalf("could not create S3 client: %v", err)
	}

	bucket := "conduit-s3-source-test-" + uuid.NewString()
	createTestBucket(t, client, bucket)
	t.Cleanup(func() {
		clearTestBucket(t, client, bucket)
		deleteTestBucket(t, client, bucket)
	})

	cfg[config.ConfigKeyAWSBucket] = bucket

	return client, cfg
}

func newS3Client(cfg map[string]string) (*s3.Client, error) {
	awsCredsProvider := credentials.NewStaticCredentialsProvider(
		cfg[config.ConfigKeyAWSAccessKeyID],
		cfg[config.ConfigKeyAWSSecretAccessKey],
		"",
	)

	awsConfig, err := awsconfig.LoadDefaultConfig(
		context.Background(),
		awsconfig.WithRegion(cfg[config.ConfigKeyAWSRegion]),
		awsconfig.WithCredentialsProvider(awsCredsProvider),
	)
	if err != nil {
		return nil, err
	}

	client := s3.NewFromConfig(awsConfig)
	return client, nil
}

func createTestBucket(t *testing.T, client *s3.Client, bucket string) {
	start := time.Now()
	defer func() {
		t.Logf("created test bucket %q in %v", bucket, time.Since(start))
	}()

	// By default, s3 buckets are created in us-east-1 if no LocationConstraint
	// is specified. As our test client is configured to use the region coming from
	// AWS_REGION env var, it might differ from the default one, so here we force
	// the bucket creation to use the same region.
	// More info at https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingBucket.html#bucket-config-options-intro

	region := os.Getenv("AWS_REGION")

	// createConfig must be a pointer so that we only set it to nonnil when
	// region is a valid location constraint. Otherwise this might fail in ci
	// with a MalformedXML error.
	var createConfig *types.CreateBucketConfiguration
	for _, value := range types.BucketLocationConstraint.Values("") {
		if region == string(value) {
			createConfig = &types.CreateBucketConfiguration{
				LocationConstraint: value,
			}
			break
		}
	}

	_, err := client.CreateBucket(context.Background(), &s3.CreateBucketInput{
		Bucket:                    &bucket,
		CreateBucketConfiguration: createConfig,
	})
	if err != nil {
		t.Fatalf("could not create bucket: %v", err)
	}
}

func deleteTestBucket(t *testing.T, client *s3.Client, bucket string) {
	start := time.Now()
	defer func() {
		t.Logf("deleted test bucket %q in %v", bucket, time.Since(start))
	}()

	_, err := client.DeleteBucket(context.Background(), &s3.DeleteBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		t.Fatalf("could not delete bucket: %v", err)
	}
}

func clearTestBucket(t *testing.T, client *s3.Client, bucket string) {
	ctx := context.Background()

	start := time.Now()
	defer func() {
		t.Logf("cleared test bucket %q in %v", bucket, time.Since(start))
	}()

	var deleteObjects []types.ObjectIdentifier

	var nextKey *string
	for {
		input := &s3.ListObjectVersionsInput{
			Bucket:    aws.String(bucket),
			KeyMarker: nextKey,
		}
		list, err := client.ListObjectVersions(ctx, input)
		if err != nil {
			t.Fatalf("could not list object versions: %v", err)
		}

		for _, d := range list.Versions {
			deleteObjects = append(deleteObjects, types.ObjectIdentifier{
				Key:       d.Key,
				VersionId: d.VersionId,
			})
		}
		for _, d := range list.DeleteMarkers {
			deleteObjects = append(deleteObjects, types.ObjectIdentifier{
				Key:       d.Key,
				VersionId: d.VersionId,
			})
		}

		if !*list.IsTruncated {
			break
		}
		nextKey = list.NextKeyMarker
	}

	if len(deleteObjects) > 0 {
		_, err := client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(bucket),
			Delete: &types.Delete{
				Objects: deleteObjects,
			},
		})
		if err != nil {
			t.Fatalf("could not delete objects: %v", err)
		}
	}
}

func parseIntegrationConfig() (map[string]string, error) {
	awsAccessKeyID := os.Getenv("AWS_ACCESS_KEY_ID")

	if awsAccessKeyID == "" {
		return map[string]string{}, errors.New("AWS_ACCESS_KEY_ID env var must be set")
	}

	awsSecretAccessKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
	if awsSecretAccessKey == "" {
		return map[string]string{}, errors.New("AWS_SECRET_ACCESS_KEY env var must be set")
	}

	awsRegion := os.Getenv("AWS_REGION")
	if awsRegion == "" {
		return map[string]string{}, errors.New("AWS_REGION env var must be set")
	}

	return map[string]string{
		config.ConfigKeyAWSAccessKeyID:     awsAccessKeyID,
		config.ConfigKeyAWSSecretAccessKey: awsSecretAccessKey,
		config.ConfigKeyAWSRegion:          awsRegion,
		source.ConfigKeyPollingPeriod:      "100ms",
	}, nil
}

var testMetadata = map[string]string{
	"key1": "value1",
	"key2": "value2",
}

func addObjectsToBucket(ctx context.Context, t *testing.T, testBucket, prefix string, client *s3.Client, num int) []Object {
	testFiles := make([]Object, num)
	for i := 0; i < num; i++ {
		key := fmt.Sprintf("%sfile%04d", prefix, i)
		content := uuid.NewString()
		buf := strings.NewReader(content)
		testFiles[i] = Object{
			key:      key,
			content:  content,
			metadata: testMetadata,
		}
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:        aws.String(testBucket),
			Key:           aws.String(key),
			Body:          buf,
			ContentLength: aws.Int64(int64(buf.Len())),
			Metadata:      testMetadata,
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	return testFiles
}

// readWithTimeout will try to read the next record until the timeout is reached.
func readWithTimeout(ctx context.Context, source *source.Source, timeout time.Duration) (opencdc.Record, error) {
	timeoutTimer := time.After(timeout)

	for {
		rec, err := source.Read(ctx)
		if !errors.Is(err, sdk.ErrBackoffRetry) {
			return rec, err
		}

		select {
		case <-time.After(time.Millisecond * 100):
			// try again
		case <-timeoutTimer:
			return opencdc.Record{}, context.DeadlineExceeded
		}
	}
}

// readAndAssert will read the next record and assert that the returned record is
// the same as the wanted object.
func readAndAssert(ctx context.Context, t *testing.T, source *source.Source, want Object) (opencdc.Record, error) {
	got, err := source.Read(ctx)
	if err != nil {
		return got, err
	}

	gotKey := string(got.Key.Bytes())
	gotPayload := string(got.Payload.After.Bytes())
	if gotKey != want.key {
		t.Fatalf("expected key: %s\n got: %s", want.key, gotKey)
	}
	if gotPayload != want.content {
		t.Fatalf("expected content: %s\n got: %s", want.content, gotPayload)
	}
	for key, val := range want.metadata {
		if got.Metadata[key] != val {
			t.Fatalf("expected metadata key %q to be %q, got %q", key, val, got.Metadata[key])
		}
	}

	return got, err
}

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

package source

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
	"github.com/conduitio/conduit-connector-s3/config"
	"github.com/conduitio/conduit-connector-s3/source/position"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/google/uuid"
)

type Object struct {
	key      string
	content  string
	metadata map[string]string
}

func TestSource_SuccessfulSnapshot(t *testing.T) {
	client, cfg := prepareIntegrationTest(t)

	ctx := context.Background()
	testBucket := cfg[config.ConfigKeyAWSBucket]
	source := &Source{}
	err := source.Configure(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	err = source.Open(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}

	testFiles := addObjectsToBucket(ctx, t, testBucket, "", client, 5)

	// read and assert
	var lastPosition opencdc.Position
	for _, file := range testFiles {
		rec, err := readAndAssert(ctx, t, source, file)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		lastPosition = rec.Position
	}

	// assert last position from snapshot has a CDC type
	pos, _ := position.ParseRecordPosition(lastPosition)
	if pos.Type != position.TypeCDC {
		t.Fatalf("expected last position from snapshot to have a CDC type, got: %s", lastPosition)
	}

	_, err = source.Read(ctx)
	if !errors.Is(err, sdk.ErrBackoffRetry) {
		t.Fatalf("expected a BackoffRetry error, got: %v", err)
	}

	_ = source.Teardown(ctx)
}

func TestSource_SnapshotRestart(t *testing.T) {
	client, cfg := prepareIntegrationTest(t)

	ctx := context.Background()
	testBucket := cfg[config.ConfigKeyAWSBucket]
	source := &Source{}
	err := source.Configure(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	// set a non nil position
	err = source.Open(context.Background(), []byte("file3_s0"))
	if err != nil {
		t.Fatal(err)
	}

	testFiles := addObjectsToBucket(ctx, t, testBucket, "", client, 10)

	// read and assert
	for _, file := range testFiles {
		// first position is not nil, then snapshot will start from beginning
		_, err := readAndAssert(ctx, t, source, file)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	_ = source.Teardown(ctx)
}

func TestSource_EmptyBucket(t *testing.T) {
	_, cfg := prepareIntegrationTest(t)

	ctx := context.Background()
	source := &Source{}
	err := source.Configure(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	err = source.Open(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}

	_, err = source.Read(ctx)

	if !errors.Is(err, sdk.ErrBackoffRetry) {
		t.Fatalf("expected a BackoffRetry error, got: %v", err)
	}
	_ = source.Teardown(ctx)
}

func TestSource_StartCDCAfterEmptyBucket(t *testing.T) {
	client, cfg := prepareIntegrationTest(t)

	ctx := context.Background()
	testBucket := cfg[config.ConfigKeyAWSBucket]
	source := &Source{}
	err := source.Configure(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	err = source.Open(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}

	// read bucket while empty
	_, err = source.Read(ctx)

	if !errors.Is(err, sdk.ErrBackoffRetry) {
		t.Fatalf("expected a BackoffRetry error, got: %v", err)
	}

	// write files to bucket
	addObjectsToBucket(ctx, t, testBucket, "", client, 3)

	// read one record and assert position type is CDC
	obj, err := readWithTimeout(ctx, source, time.Second*10)
	if err != nil {
		t.Fatal(err)
	}
	pos, _ := position.ParseRecordPosition(obj.Position)
	if pos.Type != position.TypeCDC {
		t.Fatalf("expected first position after reading an empty bucket to be CDC, got: %s", obj.Position)
	}
	_ = source.Teardown(ctx)
}

func TestSource_NonExistentBucket(t *testing.T) {
	_, cfg := prepareIntegrationTest(t)

	source := &Source{}

	// set the bucket name to a unique uuid
	cfg[config.ConfigKeyAWSBucket] = uuid.NewString()

	err := source.Configure(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}

	// bucket existence check at "Open"
	err = source.Open(context.Background(), nil)
	if err == nil {
		t.Fatal("should return an error for non existent buckets")
	}
}

func TestSource_CDC_ReadRecordsInsert(t *testing.T) {
	client, cfg := prepareIntegrationTest(t)

	ctx := context.Background()
	testBucket := cfg[config.ConfigKeyAWSBucket]
	source := &Source{}
	err := source.Configure(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	err = source.Open(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}

	testFiles := addObjectsToBucket(ctx, t, testBucket, "", client, 3)

	// read and assert
	for _, file := range testFiles {
		_, err := readAndAssert(ctx, t, source, file)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
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
	if err != nil {
		t.Fatal(err)
	}

	obj, err := readWithTimeout(ctx, source, time.Second*15)
	if err != nil {
		t.Fatal(err)
	}

	// the insert should have been detected
	if strings.Compare(string(obj.Key.Bytes()), testFileName) != 0 {
		t.Fatalf("expected key: %s, got: %s", testFileName, string(obj.Key.Bytes()))
	}

	_ = source.Teardown(ctx)
}

func TestSource_CDC_UpdateWithVersioning(t *testing.T) {
	client, cfg := prepareIntegrationTest(t)

	ctx := context.Background()
	testBucket := cfg[config.ConfigKeyAWSBucket]
	source := &Source{}

	// make the bucket versioned
	_, err := client.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
		Bucket:                  aws.String(testBucket),
		VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled},
	})
	if err != nil {
		t.Fatalf("couldn't create a versioned bucket: %v", err)
	}

	err = source.Configure(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	err = source.Open(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}

	testFiles := addObjectsToBucket(ctx, t, testBucket, "", client, 1)

	// read and assert
	_, err = readAndAssert(ctx, t, source, testFiles[0])
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

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
	if err != nil {
		t.Fatal(err)
	}

	obj, err := readWithTimeout(ctx, source, time.Second*10)
	if err != nil {
		t.Fatal(err)
	}

	// the update should be detected
	if strings.Compare(string(obj.Key.Bytes()), testFileName) != 0 {
		t.Fatalf("expected key: %s, got: %s", testFileName, string(obj.Key.Bytes()))
	}
	if strings.Compare(string(obj.Payload.After.Bytes()), content) != 0 {
		t.Fatalf("expected payload: %s, got: %s", content, string(obj.Payload.After.Bytes()))
	}
	if obj.Operation != expectedOperation {
		t.Fatalf("expected operation: %s, got: %s", expectedOperation, obj.Operation)
	}

	_ = source.Teardown(ctx)
}

func TestSource_CDC_DeleteWithVersioning(t *testing.T) {
	client, cfg := prepareIntegrationTest(t)

	ctx := context.Background()
	testBucket := cfg[config.ConfigKeyAWSBucket]
	source := &Source{}
	err := source.Configure(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	err = source.Open(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}

	testFiles := addObjectsToBucket(ctx, t, testBucket, "", client, 5)

	// make the bucket versioned
	_, err = client.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
		Bucket:                  aws.String(testBucket),
		VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled},
	})
	if err != nil {
		t.Fatalf("couldn't create a versioned bucket: %v", err)
	}

	// read and assert
	for _, file := range testFiles {
		_, err := readAndAssert(ctx, t, source, file)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
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
	if err != nil {
		t.Fatal(err)
	}

	obj, err := readWithTimeout(ctx, source, time.Second*10)
	if err != nil {
		t.Fatal(err)
	}

	if strings.Compare(string(obj.Key.Bytes()), testFileName) != 0 {
		t.Fatalf("expected key: %s, got: %s", testFileName, string(obj.Key.Bytes()))
	}
	if obj.Operation != expectedOperation {
		t.Fatalf("expected operation: %s, got: %s", expectedOperation, obj.Operation)
	}

	_ = source.Teardown(ctx)
}

func TestSource_CDC_EmptyBucketWithDeletedObjects(t *testing.T) {
	client, cfg := prepareIntegrationTest(t)

	ctx := context.Background()
	testBucket := cfg[config.ConfigKeyAWSBucket]
	source := &Source{}
	err := source.Configure(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	err = source.Open(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}

	// make the bucket versioned
	_, err = client.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
		Bucket:                  aws.String(testBucket),
		VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled},
	})
	if err != nil {
		t.Fatalf("couldn't create a versioned bucket")
	}

	// add one file
	testFiles := addObjectsToBucket(ctx, t, testBucket, "", client, 1)

	// delete the added file
	testFileName := "file0000"
	// Delete a file that exists in the bucket
	_, err = client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(testBucket),
		Key:    aws.String(testFileName),
	})
	if err != nil {
		t.Fatal(err)
	}

	// we need the deleted file's modified date to be in the past
	time.Sleep(time.Second)

	// read and assert
	for _, file := range testFiles {
		_, err := readAndAssert(ctx, t, source, file)
		if !errors.Is(err, sdk.ErrBackoffRetry) {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	// should have changed to CDC
	// CDC should NOT read the deleted object
	_, err = readWithTimeout(ctx, source, time.Second)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error should be DeadlineExceeded")
	}

	_ = source.Teardown(ctx)
}

func TestSource_CDCPosition(t *testing.T) {
	client, cfg := prepareIntegrationTest(t)

	ctx := context.Background()
	testBucket := cfg[config.ConfigKeyAWSBucket]
	source := &Source{}
	err := source.Configure(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}

	addObjectsToBucket(ctx, t, testBucket, "", client, 2)

	// make the bucket versioned
	_, err = client.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
		Bucket:                  aws.String(testBucket),
		VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled},
	})
	if err != nil {
		t.Fatalf("couldn't create a versioned bucket")
	}

	testFileName := "file0001" // already exists in the bucket
	expectedOperation := opencdc.OperationDelete
	// Delete a file that exists in the bucket
	_, err = client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(testBucket),
		Key:    aws.String(testFileName),
	})
	if err != nil {
		t.Fatal(err)
	}

	// initialize the connector to start detecting changes from the past, so all the bucket is new data
	err = source.Open(context.Background(), []byte("file0001_c1634049397"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = source.Read(ctx)
	// error is expected after resetting the connector with a new CDC position
	if err == nil {
		t.Fatalf("S3 connector should return a BackoffRetry error for the first Read() call after starting CDC")
	}

	obj, err := readWithTimeout(ctx, source, time.Second*10)
	if err != nil {
		t.Fatal(err)
	}
	// the Read should return the first file from the bucket, since in has the oldest modified date
	if strings.Compare(string(obj.Key.Bytes()), "file0000") != 0 {
		t.Fatalf("expected key: 'file0000', got: %s", string(obj.Key.Bytes()))
	}

	// next read should return the deleted file
	obj2, err := readWithTimeout(ctx, source, time.Second*10)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Compare(string(obj2.Key.Bytes()), testFileName) != 0 {
		t.Fatalf("expected key: %s, got: %s", testFileName, string(obj2.Key.Bytes()))
	}
	if obj2.Operation != expectedOperation {
		t.Fatalf("expected operation: %s, got: %s", expectedOperation, obj2.Operation)
	}
	_ = source.Teardown(ctx)
}

func TestSource_SnapshotWithPrefix(t *testing.T) {
	client, cfg := prepareIntegrationTest(t)

	ctx := context.Background()
	testBucket := cfg[config.ConfigKeyAWSBucket]
	testPrefix := "conduit-test-snapshot-prefix-"
	cfg[config.ConfigKeyPrefix] = testPrefix
	source := &Source{}
	err := source.Configure(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}

	err = source.Open(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}

	// put two items without prefix
	_ = addObjectsToBucket(ctx, t, testBucket, "", client, 2)
	// put two items with prefix
	testFiles := addObjectsToBucket(ctx, t, testBucket, testPrefix, client, 2)

	// read and assert
	for _, file := range testFiles {
		_, err := readAndAssert(ctx, t, source, file)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	// should return ErrBackoffRetry and not read items without the specified prefix
	_, err = source.Read(ctx)
	if !errors.Is(err, sdk.ErrBackoffRetry) {
		t.Fatalf("expected a BackoffRetry error, got: %v", err)
	}

	_ = source.Teardown(ctx)
}

func TestSource_CDCWithPrefix(t *testing.T) {
	client, cfg := prepareIntegrationTest(t)

	ctx := context.Background()
	testBucket := cfg[config.ConfigKeyAWSBucket]
	testPrefix := "conduit-test-cdc-prefix-"
	cfg[config.ConfigKeyPrefix] = testPrefix
	source := &Source{}
	err := source.Configure(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}

	err = source.Open(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}

	// should return ErrBackoffRetry, switch to CDC
	_, err = source.Read(ctx)
	if !errors.Is(err, sdk.ErrBackoffRetry) {
		t.Fatalf("expected a BackoffRetry error, got: %v", err)
	}

	// put more items for CDC iterator
	_ = addObjectsToBucket(ctx, t, testBucket, "", client, 2)
	testFiles := addObjectsToBucket(ctx, t, testBucket, testPrefix, client, 1)

	// read one record
	obj, err := readWithTimeout(ctx, source, time.Second*10)
	if err != nil {
		t.Fatal(err)
	}

	// compare the keys
	gotKey := string(obj.Key.Bytes())
	if gotKey != testFiles[0].key {
		t.Fatalf("expected key: %s\n got: %s", testFiles[0].key, gotKey)
	}

	// should return ErrBackoffRetry and not read items without the specified prefix
	_, err = source.Read(ctx)
	if !errors.Is(err, sdk.ErrBackoffRetry) {
		t.Fatalf("expected a BackoffRetry error, got: %v", err)
	}

	_ = source.Teardown(ctx)
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
		ConfigKeyPollingPeriod:             "100ms",
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
func readWithTimeout(ctx context.Context, source *Source, timeout time.Duration) (opencdc.Record, error) {
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
func readAndAssert(ctx context.Context, t *testing.T, source *Source, want Object) (opencdc.Record, error) {
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

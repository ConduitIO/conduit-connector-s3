package s3

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/conduitio/conduit-connector-s3/config"
	"github.com/conduitio/conduit-connector-s3/destination"
	"github.com/conduitio/conduit-connector-s3/source"
	"github.com/conduitio/conduit-connector-sdk"
	"github.com/google/uuid"
	"github.com/matryer/is"
	"go.uber.org/goleak"
)

func TestAcceptance(t *testing.T) {
	awsAccessKeyID := os.Getenv("AWS_ACCESS_KEY_ID")
	if awsAccessKeyID == "" {
		t.Skip("AWS_ACCESS_KEY_ID env var must be set")
	}

	awsSecretAccessKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
	if awsSecretAccessKey == "" {
		t.Skip("AWS_SECRET_ACCESS_KEY env var must be set")
	}

	cfg := map[string]string{
		config.ConfigKeyAWSAccessKeyID:     awsAccessKeyID,
		config.ConfigKeyAWSSecretAccessKey: awsSecretAccessKey,
		config.ConfigKeyAWSRegion:          "us-east-1",
		config.ConfigKeyAWSBucket:          "will be set before test",
		destination.ConfigKeyFormat:        "json",
	}

	client := newS3Client(t, cfg)

	sdk.AcceptanceTest(t, AcceptanceTestDriver{
		ConfigurableAcceptanceTestDriver: sdk.ConfigurableAcceptanceTestDriver{
			Config: sdk.ConfigurableAcceptanceTestDriverConfig{
				Connector: sdk.Connector{
					NewSpecification: Specification,
					NewSource:        source.NewSource,
					NewDestination:   destination.NewDestination,
				},
				SourceConfig:      cfg,
				DestinationConfig: cfg,

				BeforeTest: func(t *testing.T) {
					cfg[config.ConfigKeyAWSBucket] = "conduit-s3-acceptance-test-" + uuid.NewString()
					createTestBucket(t, client, cfg[config.ConfigKeyAWSBucket])
				},
				AfterTest: func(t *testing.T) {
					clearTestBucket(t, client, cfg[config.ConfigKeyAWSBucket])
					deleteTestBucket(t, client, cfg[config.ConfigKeyAWSBucket])
				},
				GoleakOptions: []goleak.Option{
					// TODO identify leaks and remove options
					goleak.IgnoreTopFunction("internal/poll.runtime_pollWait"),
					goleak.IgnoreTopFunction("net/http.(*persistConn).writeLoop"),
				},
			},
		},
	})
}

type AcceptanceTestDriver struct {
	sdk.ConfigurableAcceptanceTestDriver
}

func (d AcceptanceTestDriver) WriteToSource(t *testing.T, records []sdk.Record) []sdk.Record {
	is := is.New(t)
	out := d.ConfigurableAcceptanceTestDriver.WriteToSource(t, records)
	// the destination writes whole records in JSON format, change the
	// expectation to contain JSON payloads
	for i := range out {
		recPayload, err := json.Marshal(out[i])
		is.NoErr(err)
		out[i].Payload = sdk.RawData(recPayload)
	}
	return out
}

func (d AcceptanceTestDriver) ReadFromDestination(t *testing.T, records []sdk.Record) []sdk.Record {
	is := is.New(t)
	out := d.ConfigurableAcceptanceTestDriver.ReadFromDestination(t, records)
	// the destination writes whole records in JSON format, parse the records to
	// match the expectation
	for i := range out {
		var rec sdk.Record
		err := json.Unmarshal(out[i].Payload.Bytes(), &rec)
		is.NoErr(err)
		rec.Metadata = nil
		out[i] = rec
	}
	return out
}

func newS3Client(t *testing.T, cfg map[string]string) *s3.Client {
	is := is.New(t)

	awsCredsProvider := credentials.NewStaticCredentialsProvider(
		cfg[config.ConfigKeyAWSAccessKeyID],
		cfg[config.ConfigKeyAWSSecretAccessKey],
		"",
	)

	awsConfig, err := awsconfig.LoadDefaultConfig(
		context.Background(),
		awsconfig.WithRegion(cfg[config.ConfigKeyAWSRegion]),
		awsconfig.WithCredentialsProvider(awsCredsProvider),
		awsconfig.WithHTTPClient(
			awshttp.NewBuildableClient().WithTransportOptions(func(transport *http.Transport) {
				transport.DisableKeepAlives = true // easiest way to prevent false goroutine leak failures
			}),
		),
	)
	is.NoErr(err)

	return s3.NewFromConfig(awsConfig)
}

func clearTestBucket(t *testing.T, client *s3.Client, bucket string) {
	is := is.New(t)
	ctx := context.Background()

	var deleteObjects []types.ObjectIdentifier

	var nextKey *string
	for {
		input := &s3.ListObjectVersionsInput{
			Bucket:    aws.String(bucket),
			KeyMarker: nextKey,
		}
		list, err := client.ListObjectVersions(ctx, input)
		is.NoErr(err)

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

		if !list.IsTruncated {
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
		is.NoErr(err)
	}
}

func createTestBucket(t *testing.T, client *s3.Client, bucket string) {
	is := is.New(t)
	_, err := client.CreateBucket(context.Background(), &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})
	is.NoErr(err)
}

func deleteTestBucket(t *testing.T, client *s3.Client, bucket string) {
	is := is.New(t)
	_, err := client.DeleteBucket(context.Background(), &s3.DeleteBucketInput{
		Bucket: aws.String(bucket),
	})
	is.NoErr(err)
}

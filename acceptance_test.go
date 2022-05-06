package s3

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/conduitio/conduit-connector-s3/config"
	"github.com/conduitio/conduit-connector-s3/destination"
	"github.com/conduitio/conduit-connector-s3/source"
	"github.com/conduitio/conduit-connector-sdk"

	"github.com/google/uuid"
	"testing"
)

func TestAcceptance(t *testing.T) {
	cfg := map[string]string{
		// todo: fill the first 2 values
		config.ConfigKeyAWSAccessKeyID:     "",
		config.ConfigKeyAWSSecretAccessKey: "",
		config.ConfigKeyAWSRegion:          "us-east-1",
		config.ConfigKeyAWSBucket:          "maha-test",
		destination.ConfigKeyFormat:        "json",
	}

	sdk.AcceptanceTest(t, sdk.ConfigurableAcceptanceTestDriver{
		Config: sdk.ConfigurableAcceptanceTestDriverConfig{
			Connector: sdk.Connector{
				NewSpecification: Specification,
				NewSource:        source.NewSource,
				NewDestination:   destination.NewDestination,
			},
			SourceConfig:      cfg,
			DestinationConfig: cfg,

			BeforeTest: func(t *testing.T) {
				name := "test-bucket-" + uuid.NewString()
				cfg[config.ConfigKeyAWSBucket] = name
				client, _ := newS3Client(cfg)
				_, _ = client.CreateBucket(context.Background(), &s3.CreateBucketInput{
					Bucket: &name,
				})
			},
			AfterTest: func(t *testing.T) {
				name := cfg[config.ConfigKeyAWSBucket]
				client, _ := newS3Client(cfg)
				clearTestBucket(client, name)
				deleteTestBucket(client, name)
			},
			//Skip: []string{"TestDestination_Configure_Success"},

		},
	})
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

func clearTestBucket(client *s3.Client, bucket string) {
	ctx := context.Background()

	var deleteObjects []types.ObjectIdentifier

	var nextKey *string
	for {
		input := &s3.ListObjectVersionsInput{
			Bucket:    aws.String(bucket),
			KeyMarker: nextKey,
		}
		list, _ := client.ListObjectVersions(ctx, input)

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
		_, _ = client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(bucket),
			Delete: &types.Delete{
				Objects: deleteObjects,
			},
		})
	}
}

func deleteTestBucket(client *s3.Client, bucket string) {

	_, _ = client.DeleteBucket(context.Background(), &s3.DeleteBucketInput{
		Bucket: aws.String(bucket),
	})
}

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

package writer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/conduitio/conduit-commons/opencdc"
)

// S3FilesWrittenLength defines the number of last filenames an S3 Writer keep
// track of when storing files in S3. This is only used in tests.
const S3FilesWrittenLength = 100

// S3 writer stores batch bytes into an S3 bucket as a file.
type S3 struct {
	KeyPrefix    string
	Bucket       string
	Position     opencdc.Position
	Error        error
	FilesWritten []string
	Client       *s3.Client

	// ServerSideEncryption is sent with every PutObject when not empty.
	ServerSideEncryption types.ServerSideEncryption
	// SSEKMSKeyID is sent with every PutObject when not empty.
	SSEKMSKeyID string
}

var _ Writer = (*S3)(nil)

// S3Config is a type used to initialize an S3 Writer
type S3Config struct {
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
	Region          string
	Bucket          string
	KeyPrefix       string
	// Endpoint is the S3-compatible endpoint to connect to (for example
	// http://localhost:9000 for MinIO). Empty means the default AWS endpoints.
	Endpoint string
	// PathStyle enables path-style addressing (http://endpoint/bucket/key).
	// Required by S3-compatible stores that do not support virtual-hosted
	// addressing, such as the default MinIO setup (started without
	// MINIO_DOMAIN), which ignores the bucket in the host name and takes the
	// first path segment as the bucket instead: PutObject then fails with a
	// 404 NoSuchBucket when a key prefix is configured, and with a 400
	// MalformedXML when it is not -- the shape reported in
	// https://github.com/ConduitIO/conduit-connector-s3/issues/963.
	PathStyle bool
	// ServerSideEncryption is the encryption mode requested for every object.
	// Empty sends no header, so the bucket's default encryption applies. An
	// explicit header takes precedence over the bucket default (#1047).
	ServerSideEncryption types.ServerSideEncryption
	// SSEKMSKeyID is the KMS key for aws:kms and aws:kms:dsse. Empty lets S3
	// use the bucket's default key or the AWS managed key.
	SSEKMSKeyID string
}

// NewS3 takes an S3Config reference and produces an S3 Writer
func NewS3(ctx context.Context, cfg *S3Config) (*S3, error) {
	awsCredsProvider := credentials.NewStaticCredentialsProvider(
		cfg.AccessKeyID,
		cfg.SecretAccessKey,
		cfg.SessionToken,
	)

	awsConfig, err := config.LoadDefaultConfig(
		ctx,
		config.WithRegion(cfg.Region),
		config.WithCredentialsProvider(awsCredsProvider),
	)
	if err != nil {
		return nil, err
	}

	return &S3{
		Bucket:       cfg.Bucket,
		KeyPrefix:    cfg.KeyPrefix,
		FilesWritten: make([]string, 0, S3FilesWrittenLength),

		ServerSideEncryption: cfg.ServerSideEncryption,
		SSEKMSKeyID:          cfg.SSEKMSKeyID,
		Client: s3.NewFromConfig(awsConfig, func(o *s3.Options) {
			if cfg.Endpoint != "" {
				o.BaseEndpoint = aws.String(cfg.Endpoint)
			}
			o.UsePathStyle = cfg.PathStyle
		}),
	}, nil
}

// Write stores the batch on AWS S3 as a file
func (w *S3) Write(ctx context.Context, batch *Batch) error {
	batchBytes, err := batch.Bytes()
	if err != nil {
		return err
	}

	key := fmt.Sprintf(
		"%d.%s",
		time.Now().UnixNano(),
		batch.Format.Ext(),
	)

	// path.Join drops empty elements, so no prefix and no directory give
	// the bare name as before.
	key = path.Join(w.KeyPrefix, batch.Dir, key)

	input := &s3.PutObjectInput{
		Bucket:             aws.String(w.Bucket),
		Key:                aws.String(key),
		ACL:                types.ObjectCannedACLPrivate, // TODO: config?
		Body:               bytes.NewReader(batchBytes),
		ContentLength:      aws.Int64(int64(len(batchBytes))),
		ContentType:        aws.String(batch.Format.MimeType()),
		ContentDisposition: aws.String("attachment"),
	}
	// No encryption header unless one is configured: an explicit header
	// overrides the bucket's default encryption, and the hardcoded AES256 that
	// used to be here silently replaced an SSE-KMS bucket default with SSE-S3
	// (#1047).
	if w.ServerSideEncryption != "" {
		input.ServerSideEncryption = w.ServerSideEncryption
	}
	if w.SSEKMSKeyID != "" {
		input.SSEKMSKeyId = aws.String(w.SSEKMSKeyID)
	}

	_, err = w.Client.PutObject(ctx, input)
	if err != nil {
		var apiErr smithy.APIError
		if w.SSEKMSKeyID != "" && errors.As(err, &apiErr) {
			// KMS problems (unknown key, key in another region, no
			// kms:GenerateDataKey permission) come back from S3 as plain API
			// errors; name the setting so the fix is findable.
			return fmt.Errorf("PutObject with server-side encryption %s and KMS key %q (config aws.sseKmsKeyId) failed; if the error concerns the key, check that it exists in the bucket's region and that these credentials may use it: %w",
				w.ServerSideEncryption, w.SSEKMSKeyID, err)
		}
		return err
	}

	// Log written file names in here so we could access those files in tests.
	// Also, truncate to last 100 elements to prevent memory leaks in
	// production.
	w.FilesWritten = append(w.FilesWritten, key)
	if len(w.FilesWritten) > S3FilesWrittenLength {
		w.FilesWritten = w.FilesWritten[len(w.FilesWritten)-S3FilesWrittenLength:]
	}

	w.Position = batch.LastPosition()

	return nil
}

// errCodeNoBucketEncryption is the S3 error code of GetBucketEncryption for a
// bucket without a default encryption configuration.
const errCodeNoBucketEncryption = "ServerSideEncryptionConfigurationNotFoundError"

// BucketDefaultEncryption returns the algorithm of the bucket's default
// encryption ("AES256", "aws:kms", ...), or "" if the bucket has none. Any other
// failure (no s3:GetEncryptionConfiguration permission, a store without the
// API) is returned as an error; callers treat it as "unknown".
func (w *S3) BucketDefaultEncryption(ctx context.Context) (string, error) {
	out, err := w.Client.GetBucketEncryption(ctx, &s3.GetBucketEncryptionInput{Bucket: aws.String(w.Bucket)})
	if err != nil {
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) && apiErr.ErrorCode() == errCodeNoBucketEncryption {
			return "", nil
		}
		return "", err
	}
	if out.ServerSideEncryptionConfiguration == nil {
		return "", nil
	}
	for _, r := range out.ServerSideEncryptionConfiguration.Rules {
		if r.ApplyServerSideEncryptionByDefault != nil && r.ApplyServerSideEncryptionByDefault.SSEAlgorithm != "" {
			return string(r.ApplyServerSideEncryptionByDefault.SSEAlgorithm), nil
		}
	}
	return "", nil
}

// LastPosition returns the last persisted position
func (w *S3) LastPosition() opencdc.Position {
	return w.Position
}

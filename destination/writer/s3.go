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
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"path"
	"strings"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-s3/destination/format"
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

	// ConditionalWrites sends If-None-Match: * with every PutObject, so that
	// the store refuses to replace an existing object instead of silently
	// overwriting it.
	ConditionalWrites bool

	// id is random per writer and seq counts its writes: together they make
	// every key unique by construction (see objectName).
	id  string
	seq atomic.Uint64
	now func() time.Time
}

// ErrKeyExists is the stable error code of a PutObject the store refused
// because an object with the key already exists (If-None-Match: *). Keys are
// unique by construction, so this means another writer used the same key: the
// write fails loudly and the records are not acknowledged, instead of
// replacing an object whose records were acknowledged earlier.
var ErrKeyExists = errors.New("s3.destination.key_exists")

// ErrConditionalConflict is the stable error code of a conditional PutObject
// that the store refused because a conflicting operation on the same key was
// in progress (AWS: 409 ConditionalRequestConflict). It is transient: the
// records are not acknowledged, and a retry writes them under a new key.
var ErrConditionalConflict = errors.New("s3.destination.conditional_conflict")

// ErrConditionalWritesUnsupported is the stable error code of a store that
// does not support conditional PutObject (If-None-Match: *).
var ErrConditionalWritesUnsupported = errors.New("s3.destination.conditional_writes_unsupported")

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
	// ConditionalWrites sends If-None-Match: * with every PutObject.
	ConditionalWrites bool
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

	id, err := newWriterID()
	if err != nil {
		return nil, err
	}

	return &S3{
		Bucket:            cfg.Bucket,
		KeyPrefix:         cfg.KeyPrefix,
		FilesWritten:      make([]string, 0, S3FilesWrittenLength),
		ConditionalWrites: cfg.ConditionalWrites,
		id:                id,
		now:               time.Now,

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
	// A refused record leaves the bytes of the records before it: store
	// those, then report the refusal (see Batch.Bytes).
	batchBytes, refused := batch.Bytes()
	var re *format.RecordError
	if refused != nil && !errors.As(refused, &re) {
		return refused
	}
	if len(batchBytes) == 0 {
		if refused == nil && len(batch.Records) > 0 {
			// Records that encode to nothing would be acked without being
			// stored (invariant 1); the format checks make this unreachable,
			// so it is a bug if it happens.
			return fmt.Errorf("%d records encoded to an empty object; not writing or acknowledging them", len(batch.Records))
		}
		return refused
	}

	key := w.objectName(batch.Format.Ext())

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
	if w.ConditionalWrites {
		// Invariant 1 and 3: never replace an object whose records may
		// already be acknowledged. Keys are unique by construction; this makes
		// any collision that still happens fail loudly instead.
		input.IfNoneMatch = aws.String("*")
	}

	_, err := w.Client.PutObject(ctx, input)
	if err != nil {
		if err := w.conditionalWriteError(err, key); err != nil {
			return err
		}
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

	// Invariant 1: only the records in the object PutObject just confirmed
	// are reported as written; a refused record and everything after it are
	// left to the caller to nack (Batch.Written).
	w.Position = batch.Records[batch.Written(refused)-1].Position

	return refused
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

// objectName returns the name of the next object: the write time in
// nanoseconds (so objects sort by write time, as before), a random id of this
// writer, and the writer's write counter. The id makes keys of different
// writers (two pipelines, two Conduit instances on one bucket and prefix)
// distinct; the counter makes keys of one writer distinct even within one
// clock tick or after the clock stepped back. Keys used to be the timestamp
// alone, so two writers in the same tick overwrote each other (s3#1064).
func (w *S3) objectName(ext string) string {
	// Invariant 1: a key never repeats, so a PutObject never replaces an
	// object whose records were acknowledged.
	return fmt.Sprintf("%d-%s-%d.%s", w.now().UnixNano(), w.id, w.seq.Add(1), ext)
}

// newWriterID returns 64 random bits as 16 hex characters: the chance that
// two writers draw the same id is negligible, and a key also differs by its
// time and counter.
func newWriterID() (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("could not generate the writer id: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}

// conditionalWriteError translates the store's answer to a conditional
// PutObject. It returns nil when err is not about the condition.
func (w *S3) conditionalWriteError(err error, key string) error {
	if !w.ConditionalWrites {
		return nil
	}
	code, msg := "", ""
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		code, msg = apiErr.ErrorCode(), apiErr.ErrorMessage()
	}
	status := 0
	var re interface{ HTTPStatusCode() int }
	if errors.As(err, &re) {
		status = re.HTTPStatusCode()
	}
	switch {
	case code == "PreconditionFailed" || status == http.StatusPreconditionFailed:
		// Invariant 1: the records are nacked, never acked against an
		// object this write did not create.
		return fmt.Errorf("%w: the object %q already exists (bucket %q), so it was not overwritten and the records are not acknowledged. "+
			"Either another writer used the same key, or the AWS SDK retried this writer's own PutObject after its response was lost and the first attempt had succeeded: "+
			"then the records are stored once already, and redelivering them writes a duplicate under a new key (at-least-once): %w",
			ErrKeyExists, key, w.Bucket, err)
	case code == "ConditionalRequestConflict" || status == http.StatusConflict:
		return fmt.Errorf("%w: the store refused the conditional PutObject of %q because a conflicting operation on that key was in progress; "+
			"this is transient: the records are not acknowledged, and a retry writes them under a new key: %w",
			ErrConditionalConflict, key, err)
	case code == "NotImplemented" || status == http.StatusNotImplemented,
		(code == "InvalidArgument" || code == "InvalidRequest" || status == http.StatusBadRequest || status == http.StatusMethodNotAllowed) && mentionsCondition(msg):
		return fmt.Errorf("%w: the store does not support conditional writes (If-None-Match: *): %s; set conditionalWrites to false to write without them (keys stay unique by construction): %w",
			ErrConditionalWritesUnsupported, code, err)
	}
	return nil
}

// mentionsCondition reports whether an error message refers to the
// conditional header. Only then is a 400 or 405 taken as "conditional writes
// unsupported"; any other 400 is an ordinary error.
func mentionsCondition(msg string) bool {
	m := strings.ToLower(msg)
	return strings.Contains(m, "if-none-match") || strings.Contains(m, "conditional")
}

// LastPosition returns the last persisted position
func (w *S3) LastPosition() opencdc.Position {
	return w.Position
}

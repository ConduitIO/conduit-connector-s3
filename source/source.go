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
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/conduitio/conduit-commons/lang"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-s3/config"
	"github.com/conduitio/conduit-connector-s3/internal/logsafe"
	"github.com/conduitio/conduit-connector-s3/source/iterator"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

type Iterator interface {
	HasNext(ctx context.Context) bool
	Next(ctx context.Context) (opencdc.Record, error)
	Stop()
}

// Source connector
type Source struct {
	sdk.UnimplementedSource

	config   Config
	iterator Iterator
	client   *s3.Client
	auth     config.Auth
}

func NewSource() sdk.Source {
	return sdk.SourceWithMiddleware(
		&Source{
			config: Config{
				DefaultSourceMiddleware: sdk.DefaultSourceMiddleware{
					SourceWithSchemaExtraction: sdk.SourceWithSchemaExtraction{
						PayloadEnabled: lang.Ptr(false),
						KeyEnabled:     lang.Ptr(false),
					},
				},
			},
		},
	)
}

func (s *Source) Config() sdk.SourceConfig {
	return &s.config
}

// Open prepare the plugin to start sending records from the given position
func (s *Source) Open(ctx context.Context, rp opencdc.Position) error {
	s.config.LogEndpointWarnings(ctx)
	s.logLookbackWarnings(ctx)
	if err := checkBucketName(s.config.AWSBucket); err != nil {
		return err
	}

	client, auth, err := config.NewS3Client(ctx, s.config.ClientParams(), config.PurposeOpen)
	if err != nil {
		return err
	}
	s.client, s.auth = client, auth

	// check if bucket exists
	err = s.bucketExists(ctx, s.config.AWSBucket)
	if err != nil {
		return s.auth.Scrub(ctx, err)
	}
	if err := s.probeListings(ctx); err != nil {
		return err
	}

	s.iterator, err = iterator.NewCombinedIterator(ctx, iterator.CombinedConfig{
		Bucket:                 s.config.AWSBucket,
		Prefix:                 s.config.Prefix,
		PollingPeriod:          s.config.PollingPeriod,
		Lookback:               s.config.LookbackPeriod,
		MultipartHold:          s.config.MultipartHold,
		MaxTrackedUploads:      s.config.MaxTrackedUploads,
		MaxPendingUploadAge:    s.config.MaxPendingUploadAge,
		MaxEarlyObjects:        s.config.MaxEarlyObjects,
		MaxSettledBytesPerPoll: int64(s.config.MaxSettledBytesPerPoll),
	}, s.client, rp)
	if err != nil {
		// The iterator lists the bucket at creation, so this can carry AWS
		// identity details (AccessDenied with the assumed-role ARN).
		return fmt.Errorf("couldn't create a combined iterator: %w", s.auth.Scrub(ctx, err))
	}
	return nil
}

// Read gets the next object from the S3 bucket
func (s *Source) Read(ctx context.Context) (opencdc.Record, error) {
	if !s.iterator.HasNext(ctx) {
		return opencdc.Record{}, sdk.ErrBackoffRetry
	}
	r, err := s.iterator.Next(ctx)
	if err != nil {
		return opencdc.Record{}, s.auth.Scrub(ctx, err)
	}
	return r, nil
}

func (s *Source) Teardown(_ context.Context) error {
	if s.iterator != nil {
		s.iterator.Stop()
	}
	return nil
}

func (s *Source) bucketExists(ctx context.Context, bucketName string) error {
	// check if the bucket exists
	_, err := s.client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(bucketName),
	})
	return err
}

func (s *Source) Ack(ctx context.Context, position opencdc.Position) error {
	// A position holds the object key, so only its size and a hash are logged.
	sdk.Logger(ctx).Debug().Int("position_size", len(position)).Str("position_hash", logsafe.KeyHash(position)).Msg("got ack")
	return nil // no ack needed
}

// ErrStoreUnsupported is the stable error code of a store whose listing does
// not support listing-based change capture.
var ErrStoreUnsupported = errors.New("s3.source.store_unsupported")

func (s *Source) logLookbackWarnings(ctx context.Context) {
	lb := s.config.LookbackPeriod
	log := sdk.Logger(ctx).Warn().Dur(ConfigKeyLookbackPeriod, lb)
	switch {
	case lb == 0:
		log.Str("code", "s3.source.lookback_short").
			Msg("lookbackPeriod is 0: an object that becomes visible in listings after its LastModified (replication lag, a slow PUT) can be missed; only multipart uploads are covered. Exact resume is not claimed for this setting")
	case lb < MinExactLookback:
		log.Str("code", "s3.source.lookback_short").Dur("supported_minimum", MinExactLookback).
			Msg("lookbackPeriod below 30s is deprecated and will be rejected in a later release: it gives almost no tolerance for late-visible objects (the store's Date is rounded down by up to a second, and a listing takes time of its own)")
	case lb > WarnLookbackPeriod:
		log.Msg("lookbackPeriod is large: new objects that do not fit cdc.maxEarlyObjects wait up to this long before they are delivered")
	}
	if !s.config.MultipartHold {
		sdk.Logger(ctx).Warn().Str("code", "s3.source.multipart_hold_disabled").
			Msg("cdc.multipartHold is false: a multipart upload that runs longer than lookbackPeriod is stamped with its start time and can be missed. Exact resume is not claimed for this setting")
	}
}

// checkBucketName rejects S3 Express One Zone directory buckets: they have no
// versions listing and do not list in key order.
func checkBucketName(bucket string) error {
	if strings.HasSuffix(bucket, "--x-s3") {
		return fmt.Errorf("%w: %q is an S3 Express One Zone directory bucket, which cannot be read with listing-based change capture (no versions listing, no key order); use a general purpose bucket", ErrStoreUnsupported, bucket)
	}
	return nil
}

// probeListings checks, once at Open, that the store supports the listings
// change capture needs and that the credentials may use them. A failure here
// is cheaper to act on than an iterator that stops later.
func (s *Source) probeListings(ctx context.Context) error {
	_, err := s.client.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
		Bucket: aws.String(s.config.AWSBucket), Prefix: aws.String(s.config.Prefix), MaxKeys: aws.Int32(1),
	})
	if err != nil {
		switch apiCode(err) {
		case "NotImplemented", "MethodNotAllowed", "InvalidRequest":
			return fmt.Errorf("%w: the store does not support ListObjectVersions (%s), which change capture needs; tested stores are AWS S3 and MinIO: %w", ErrStoreUnsupported, apiCode(err), err)
		case "AccessDenied":
			return fmt.Errorf("listing object versions was denied; grant s3:ListBucketVersions on bucket %q: %w", s.config.AWSBucket, err)
		}
		return fmt.Errorf("could not list object versions: %w", err)
	}
	if !s.config.MultipartHold {
		return nil
	}
	_, err = s.client.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{
		Bucket: aws.String(s.config.AWSBucket), Prefix: aws.String(s.config.Prefix), MaxUploads: aws.Int32(1),
	})
	if err != nil {
		if apiCode(err) == "AccessDenied" {
			return fmt.Errorf("%w: listing in-progress multipart uploads under %q was denied; grant s3:ListBucketMultipartUploads, or set %s to false (lossy for multipart uploads longer than %s): %w",
				iterator.ErrMultipartListingDenied, s.config.Prefix, ConfigKeyMultipartHold, ConfigKeyLookbackPeriod, err)
		}
		return fmt.Errorf("could not list multipart uploads: %w", err)
	}
	return nil
}

func apiCode(err error) string {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		return apiErr.ErrorCode()
	}
	return ""
}

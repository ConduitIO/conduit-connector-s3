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
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/conduitio/conduit-connector-s3/source/iterator"
	"github.com/conduitio/conduit-connector-s3/source/position"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

// Source connector
type Source struct {
	sdk.UnimplementedSource

	config   Config
	iterator Iterator
	client   *s3.Client
}

type Iterator interface {
	HasNext(ctx context.Context) bool
	Next(ctx context.Context) (sdk.Record, error)
	Stop()
}

func NewSource() sdk.Source {
	return sdk.SourceWithMiddleware(&Source{}, sdk.DefaultSourceMiddleware()...)
}

func (s *Source) Parameters() map[string]sdk.Parameter {
	return s.config.Parameters()
}

// Configure parses and stores the configurations
// returns an error in case of invalid config
func (s *Source) Configure(ctx context.Context, cfg map[string]string) error {
	var sourceConfig Config
	err := sdk.Util.ParseConfig(cfg, &sourceConfig)
	if err != nil {
		return err
	}

	s.config = sourceConfig

	return nil
}

// Open prepare the plugin to start sending records from the given position
func (s *Source) Open(ctx context.Context, rp sdk.Position) error {
	awsCredsProvider := credentials.NewStaticCredentialsProvider(
		s.config.AWSAccessKeyID,
		s.config.AWSSecretAccessKey,
		"",
	)

	s3Config, err := awsConfig.LoadDefaultConfig(
		ctx,
		awsConfig.WithRegion(s.config.AWSRegion),
		awsConfig.WithCredentialsProvider(awsCredsProvider),
	)
	if err != nil {
		return err
	}

	s.client = s3.NewFromConfig(s3Config)

	// check if bucket exists
	err = s.bucketExists(ctx, s.config.AWSBucket)
	if err != nil {
		return err
	}

	// parse position to start from
	p, err := position.ParseRecordPosition(rp)
	if err != nil {
		return err
	}

	s.iterator, err = iterator.NewCombinedIterator(
		s.config.AWSBucket, s.config.Prefix, s.config.PollingPeriod, s.client, p,
	)
	if err != nil {
		return fmt.Errorf("couldn't create a combined iterator: %w", err)
	}
	return nil
}

// Read gets the next object from the S3 bucket
func (s *Source) Read(ctx context.Context) (sdk.Record, error) {
	if !s.iterator.HasNext(ctx) {
		return sdk.Record{}, sdk.ErrBackoffRetry
	}
	r, err := s.iterator.Next(ctx)
	if err != nil {
		return sdk.Record{}, err
	}
	return r, nil
}

func (s *Source) Teardown(ctx context.Context) error {
	if s.iterator != nil {
		s.iterator.Stop()
		s.iterator = nil
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

func (s *Source) Ack(ctx context.Context, position sdk.Position) error {
	sdk.Logger(ctx).Debug().Str("position", string(position)).Msg("got ack")
	return nil // no ack needed
}

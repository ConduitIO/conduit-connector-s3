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

package filevalidator

import (
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// S3 validates S3 files
type S3 struct {
	AccessKeyID     string
	SecretAccessKey string
	Region          string
	Bucket          string
}

// Validate takes a name of an S3 file and compares the contents of a file with
// this name to a byte-slice returning an error if they don't match.
func (v *S3) Validate(name string, reference []byte) error {
	awsCredsProvider := credentials.NewStaticCredentialsProvider(
		v.AccessKeyID,
		v.SecretAccessKey,
		"",
	)

	awsConfig, err := config.LoadDefaultConfig(
		context.TODO(),
		config.WithRegion(v.Region),
		config.WithCredentialsProvider(awsCredsProvider),
	)
	if err != nil {
		return err
	}

	client := s3.NewFromConfig(awsConfig)

	object, err := client.GetObject(
		context.TODO(),
		&s3.GetObjectInput{
			Bucket: aws.String(v.Bucket),
			Key:    aws.String(name),
		},
	)
	if err != nil {
		return err
	}

	data, err := io.ReadAll(object.Body)
	if err != nil {
		return err
	}

	err = compareBytes(data, reference)
	if err != nil {
		return fmt.Errorf(
			"%s (%dB) and its reference (%dB) have different bytes: %w",
			name,
			len(data),
			len(reference),
			err,
		)
	}

	_, err = client.DeleteObject(
		context.TODO(),
		&s3.DeleteObjectInput{
			Bucket: aws.String(v.Bucket),
			Key:    aws.String(name),
		},
	)
	if err != nil {
		return err
	}

	return nil
}

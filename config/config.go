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

package config

const (
	// ConfigKeyAWSAccessKeyID is the config name for AWS access secret key
	ConfigKeyAWSAccessKeyID = "aws.accessKeyId"

	// ConfigKeyAWSSecretAccessKey is the config name for AWS secret access key
	ConfigKeyAWSSecretAccessKey = "aws.secretAccessKey"

	// ConfigKeyAWSRegion is the config name for AWS region
	ConfigKeyAWSRegion = "aws.region"

	// ConfigKeyAWSBucket is the config name for AWS S3 bucket
	ConfigKeyAWSBucket = "aws.bucket"

	// ConfigKeyPrefix is the config name for S3 key prefix.
	ConfigKeyPrefix = "prefix"
)

// Config represents configuration needed for S3
type Config struct {
	// AWS access key id.
	AWSAccessKeyID string `json:"aws.accessKeyId" validate:"required"`
	// AWS secret access key.
	AWSSecretAccessKey string `json:"aws.secretAccessKey" validate:"required"`
	// the AWS S3 bucket region
	AWSRegion string `json:"aws.region" validate:"required"`
	// the AWS S3 bucket name.
	AWSBucket string `json:"aws.bucket" validate:"required"`
	// the S3 key prefix.
	Prefix string
}

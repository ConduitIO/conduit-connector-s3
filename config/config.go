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

import (
	"fmt"
	"net/url"

	cconfig "github.com/conduitio/conduit-commons/config"
)

const (
	// ConfigKeyAWSAccessKeyID is the config name for AWS access secret key
	ConfigKeyAWSAccessKeyID = "aws.accessKeyId"

	// ConfigKeyAWSSecretAccessKey is the config name for AWS secret access key
	ConfigKeyAWSSecretAccessKey = "aws.secretAccessKey"

	// ConfigKeyAWSRegion is the config name for AWS region
	ConfigKeyAWSRegion = "aws.region"

	// ConfigKeyAWSBucket is the config name for AWS S3 bucket
	ConfigKeyAWSBucket = "aws.bucket"

	// ConfigKeyAWSEndpoint is the config name for a custom S3-compatible endpoint.
	ConfigKeyAWSEndpoint = "aws.endpoint"

	// ConfigKeyAWSPathStyle is the config name for path-style addressing.
	ConfigKeyAWSPathStyle = "aws.pathStyle"

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
	// The endpoint to connect to. Set this when using an S3-compatible object
	// store such as MinIO (for example http://localhost:9000). Leave empty to
	// use the default AWS endpoints. The AWS_ENDPOINT_URL environment variable
	// is also honored when this is empty.
	AWSEndpoint string `json:"aws.endpoint"`
	// Use path-style addressing (http://endpoint/bucket/key) instead of
	// virtual-hosted addressing (http://bucket.endpoint/key). Set this to true
	// when using an S3-compatible store that does not support virtual-hosted
	// addressing: the default MinIO setup misparses virtual-hosted requests
	// and answers with a 400 MalformedXML. Leave false for AWS S3.
	AWSPathStyle bool `json:"aws.pathStyle" default:"false"`
	// the S3 key prefix.
	Prefix string
}

// ValidateEndpoint validates the aws.endpoint configuration: it must be a URL
// with an http or https scheme and a host. An empty endpoint is valid (the
// default AWS endpoints are used). It is called from the Validate methods of
// the destination and source configs, which the SDK invokes after
// configuration parsing, so a bad endpoint fails at parse time instead of at
// the first request with an opaque SDK error.
func ValidateEndpoint(endpoint string) error {
	if endpoint == "" {
		return nil
	}
	u, err := url.Parse(endpoint)
	if err != nil {
		return fmt.Errorf("%s: invalid endpoint URL %q: %w", ConfigKeyAWSEndpoint, endpoint, cconfig.ErrInvalidParameterValue)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("%s: invalid endpoint URL %q, scheme must be http or https: %w", ConfigKeyAWSEndpoint, endpoint, cconfig.ErrInvalidParameterValue)
	}
	if u.Host == "" {
		return fmt.Errorf("%s: invalid endpoint URL %q, host is missing: %w", ConfigKeyAWSEndpoint, endpoint, cconfig.ErrInvalidParameterValue)
	}
	return nil
}

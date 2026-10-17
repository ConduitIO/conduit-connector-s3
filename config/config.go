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
	"context"
	"fmt"
	"net"
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	cconfig "github.com/conduitio/conduit-commons/config"
	sdk "github.com/conduitio/conduit-connector-sdk"
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

	// schemeHTTP and schemeHTTPS are the only endpoint URL schemes the S3
	// client can speak.
	schemeHTTP  = "http"
	schemeHTTPS = "https"
)

// ErrInvalidParameterValue is conduit-commons' error for a parameter with an
// invalid value, re-exported so that the source and destination config
// packages can wrap it without importing a second package named config, which
// confuses the specification generator (it resolves the embedded config.Config
// to the wrong type).
var ErrInvalidParameterValue = cconfig.ErrInvalidParameterValue

// Config represents configuration needed for S3
type Config struct {
	// AWS access key id. Optional, but must be set together with
	// aws.secretAccessKey. When both are unset the connector uses the Conduit
	// host's ambient AWS identity (the AWS SDK default credential chain:
	// environment, shared config and SSO, web identity/IRSA, ECS and EC2
	// roles), but only if the Conduit operator enabled
	// connectors.ambient-credentials and listed the bucket, and only as the
	// built-in connector (builtin:s3). Otherwise the connector refuses with
	// connector.ambient_credentials_disabled.
	AWSAccessKeyID string `json:"aws.accessKeyId"`
	// AWS secret access key. Optional, but must be set together with
	// aws.accessKeyId. See aws.accessKeyId for ambient credentials.
	AWSSecretAccessKey string `json:"aws.secretAccessKey"`
	// the AWS S3 bucket region
	AWSRegion string `json:"aws.region" validate:"required"`
	// the AWS S3 bucket name.
	AWSBucket string `json:"aws.bucket" validate:"required"`
	// The endpoint to connect to. Set this when using an S3-compatible object
	// store such as MinIO (for example http://localhost:9000). Leave empty to
	// use the default AWS endpoints. The AWS_ENDPOINT_URL environment variable
	// is also honored when this is empty. Note that an http:// endpoint sends
	// every request, including the SigV4 Authorization header that carries the
	// access key ID, in the clear: use https:// for anything that is not on
	// localhost.
	AWSEndpoint string `json:"aws.endpoint"`
	// Use path-style addressing (http://endpoint/bucket/key) instead of
	// virtual-hosted addressing (http://bucket.endpoint/key). Set this to true
	// when using an S3-compatible store that does not support virtual-hosted
	// addressing. The default MinIO setup (started without MINIO_DOMAIN) is
	// one: it ignores the bucket in the host name and takes the first path
	// segment as the bucket instead. What comes back depends on the key: with
	// a prefix configured the remaining segments are still a key, so PutObject
	// fails with a 404 NoSuchBucket; without a prefix there is no key left and
	// MinIO answers with a 400 MalformedXML. Leave false for AWS S3.
	AWSPathStyle bool `json:"aws.pathStyle" default:"false"`
	// the S3 key prefix.
	Prefix string
}

// ValidateAuth checks the credential configuration at configure time: exactly
// one static key is an error; no static keys runs every ambient-credentials
// check (operator policy, region, bucket allowlist, proxy, resolved endpoint)
// without making a request. It is called from the Validate methods of the
// destination and source configs, so a pipeline the policy refuses fails when
// it is created or validated, not on the first record.
func (c Config) ValidateAuth(ctx context.Context) error {
	_, _, err := NewS3Client(ctx, c.ClientParams(), PurposeValidate)
	return err
}

// LoadAWSConfig builds the AWS SDK configuration shared by the source and the
// destination. With a non-empty accessKeyID and secretAccessKey it pins a
// static credentials provider (sessionToken may be empty). With both empty it
// installs no provider, so the SDK default credential chain applies:
// environment variables, shared config and credentials files, web identity
// (IRSA), then the ECS or EC2 instance role. Credentials are resolved lazily
// by the SDK on the first request, not here.
func LoadAWSConfig(ctx context.Context, region, accessKeyID, secretAccessKey, sessionToken string) (aws.Config, error) {
	opts := []func(*awsconfig.LoadOptions) error{awsconfig.WithRegion(region)}
	if accessKeyID != "" || secretAccessKey != "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(accessKeyID, secretAccessKey, sessionToken),
		))
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return aws.Config{}, fmt.Errorf("load AWS configuration: %w", err)
	}
	return cfg, nil
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
		// Keep the url.Parse cause: without it the user only learns that the
		// URL is invalid, not what is wrong with it.
		return fmt.Errorf("%s: invalid endpoint URL %q: %w (set it to a URL such as http://localhost:9000, or leave it empty to use the default AWS endpoints): %w",
			ConfigKeyAWSEndpoint, endpoint, err, cconfig.ErrInvalidParameterValue)
	}
	if u.Scheme != schemeHTTP && u.Scheme != schemeHTTPS {
		return fmt.Errorf("%s: invalid endpoint URL %q, scheme must be http or https (set it to a URL such as http://localhost:9000): %w",
			ConfigKeyAWSEndpoint, endpoint, cconfig.ErrInvalidParameterValue)
	}
	if u.Host == "" {
		return fmt.Errorf("%s: invalid endpoint URL %q, host is missing (set it to a URL such as http://localhost:9000): %w",
			ConfigKeyAWSEndpoint, endpoint, cconfig.ErrInvalidParameterValue)
	}
	return nil
}

// LogEndpointWarnings warns about endpoint configurations that are valid but
// are very likely a misconfiguration. It is called from Open in both the
// source and the destination, after the configuration has been validated.
//
// It is a no-op when aws.endpoint is empty, so the default AWS path is
// unaffected.
func (c Config) LogEndpointWarnings(ctx context.Context) {
	if c.AWSEndpoint == "" {
		return
	}

	u, err := url.Parse(c.AWSEndpoint)
	if err != nil {
		// ValidateEndpoint already reported this; nothing useful to add here.
		return
	}

	if u.Scheme == schemeHTTP && !isLoopbackHost(u.Hostname()) {
		sdk.Logger(ctx).Warn().
			Str(ConfigKeyAWSEndpoint, c.AWSEndpoint).
			Msg("connecting to a plaintext http endpoint: requests, including the SigV4 Authorization header that carries the access key ID, are sent unencrypted and can be read on the wire; use an https endpoint unless the store is on localhost")
	}

	// The virtual-hosted addressing hint is for S3-compatible stores. Real AWS
	// S3 endpoints (regional, dualstack, FIPS, access point, China) require it,
	// so telling those users to set aws.pathStyle would be wrong.
	if !c.AWSPathStyle && !isAWSS3Host(u.Hostname()) {
		sdk.Logger(ctx).Warn().
			Str(ConfigKeyAWSEndpoint, c.AWSEndpoint).
			Bool(ConfigKeyAWSPathStyle, c.AWSPathStyle).
			Msg("aws.endpoint is set but aws.pathStyle is false: requests are addressed virtual-hosted style (http://<bucket>.<endpoint>/<key>), which many S3-compatible stores do not support; if requests fail to resolve the host or come back with 404 NoSuchBucket or 400 MalformedXML, set aws.pathStyle to true")
	}
}

// isLoopbackHost reports whether host addresses the local machine, in which
// case plaintext http carries no meaningful exposure. Per RFC 6761 "localhost"
// and any name under it are guaranteed to resolve to a loopback address.
func isLoopbackHost(host string) bool {
	if host == "localhost" || strings.HasSuffix(host, ".localhost") {
		return true
	}
	if ip := net.ParseIP(host); ip != nil {
		return ip.IsLoopback()
	}
	return false
}

// awsDomainSuffixes are the DNS suffixes under which AWS serves S3 endpoints:
// the global and China partitions, and the dual-stack api.aws names.
var awsDomainSuffixes = []string{".amazonaws.com", ".amazonaws.com.cn", ".api.aws"}

// isAWSS3Host reports whether host is an AWS S3 endpoint, as opposed to a
// custom S3-compatible store. It matches hosts under an AWS domain suffix that
// carry an S3 label, which covers s3.amazonaws.com, s3.<region>.amazonaws.com,
// the legacy s3-<region>.amazonaws.com, dualstack (s3.dualstack.<region>...),
// FIPS (s3-fips.<region>...), access points (s3-accesspoint.<region>...,
// <name>-<account>.s3-accesspoint.<region>...), bucket-prefixed virtual-hosted
// names, VPC interface endpoints, and the amazonaws.com.cn and api.aws
// variants. The suffix is anchored with a leading dot, so look-alikes such as
// s3.amazonaws.com.example.com or evilamazonaws.com do not match, and other AWS
// services (sqs.us-east-1.amazonaws.com) have no S3 label.
func isAWSS3Host(host string) bool {
	host = strings.ToLower(strings.TrimSuffix(host, "."))
	for _, suffix := range awsDomainSuffixes {
		rest, ok := strings.CutSuffix(host, suffix)
		if !ok {
			continue
		}
		for _, label := range strings.Split(rest, ".") {
			if label == "s3" || strings.HasPrefix(label, "s3-") {
				return true
			}
		}
	}
	return false
}

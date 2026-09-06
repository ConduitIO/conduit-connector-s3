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
	// is also honored when this is empty. Note that an http:// endpoint sends
	// every request, including the SigV4 Authorization header that carries the
	// access key ID, in the clear: use https:// for anything that is not on
	// localhost.
	AWSEndpoint string `json:"aws.endpoint"`
	// Use path-style addressing (http://endpoint/bucket/key) instead of
	// virtual-hosted addressing (http://bucket.endpoint/key). Set this to true
	// when using an S3-compatible store that does not support virtual-hosted
	// addressing. The default MinIO setup (started without MINIO_DOMAIN) is
	// one: it does not recognize the bucket in the host name and takes the
	// first path segment as the bucket instead, so requests fail -- against
	// the MinIO setup in this repository with a 404 NoSuchBucket on PutObject
	// and a 400 Bad Request on HeadBucket; other S3-compatible stores answer
	// such requests with a 400 MalformedXML. Leave false for AWS S3.
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

	if !c.AWSPathStyle {
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

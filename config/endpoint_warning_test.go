// Copyright © 2026 Meroxa, Inc.
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
	"bytes"
	"context"
	"testing"

	"github.com/matryer/is"
	"github.com/rs/zerolog"
)

func TestIsAWSS3Host(t *testing.T) {
	testCases := []struct {
		host string
		want bool
	}{
		// AWS S3 endpoints
		{host: "s3.amazonaws.com", want: true},
		{host: "s3.us-west-1.amazonaws.com", want: true},
		{host: "s3.eu-central-1.amazonaws.com", want: true},
		{host: "s3-us-west-2.amazonaws.com", want: true}, // legacy dash form
		{host: "s3-external-1.amazonaws.com", want: true},
		{host: "s3.dualstack.us-east-1.amazonaws.com", want: true},
		{host: "s3-fips.us-gov-west-1.amazonaws.com", want: true},
		{host: "s3-fips.dualstack.us-east-2.amazonaws.com", want: true},
		{host: "s3.fips.us-east-1.amazonaws.com", want: true},
		{host: "s3-accesspoint.us-east-1.amazonaws.com", want: true},
		{host: "s3-accesspoint-fips.us-east-1.amazonaws.com", want: true},
		{host: "myap-123456789012.s3-accesspoint.us-east-1.amazonaws.com", want: true},
		{host: "mybucket.s3.us-west-2.amazonaws.com", want: true}, // virtual-hosted bucket
		{host: "mybucket.s3.amazonaws.com", want: true},
		{host: "bucket.vpce-0a1b2c3d-4e5f.s3.us-east-1.vpce.amazonaws.com", want: true},
		{host: "S3.US-WEST-1.AMAZONAWS.COM", want: true},  // DNS is case-insensitive
		{host: "s3.us-west-1.amazonaws.com.", want: true}, // trailing dot
		{host: "s3.cn-north-1.amazonaws.com.cn", want: true},
		{host: "s3.dualstack.cn-northwest-1.amazonaws.com.cn", want: true},
		{host: "s3.dualstack.us-east-1.api.aws", want: true},

		// custom S3-compatible stores and non-matches
		{host: "127.0.0.1", want: false},
		{host: "minio.internal", want: false},
		{host: "s3.example.com", want: false},
		{host: "play.min.io", want: false},
		{host: "nyc3.digitaloceanspaces.com", want: false},
		{host: "s3.us-west-001.backblazeb2.com", want: false},
		{host: "abc.r2.cloudflarestorage.com", want: false},
		{host: "", want: false},
		// look-alikes: suffix must be anchored on a label boundary
		{host: "s3.amazonaws.com.example.com", want: false},
		{host: "s3.evilamazonaws.com", want: false},
		{host: "s3-amazonaws.com", want: false},
		{host: "amazonaws.com", want: false},
		{host: "s3.api.aws.example.com", want: false},
		// AWS but not S3
		{host: "sqs.us-east-1.amazonaws.com", want: false},
		{host: "dynamodb.us-east-1.amazonaws.com", want: false},
		{host: "sts.amazonaws.com", want: false},
	}
	for _, tc := range testCases {
		t.Run(tc.host, func(t *testing.T) {
			is := is.New(t)
			is.Equal(isAWSS3Host(tc.host), tc.want)
		})
	}
}

// TestLogEndpointWarnings_PathStyleHint pins the wiring: the virtual-hosted
// hint fires for custom endpoints only, and never for AWS S3 endpoints
// (ConduitIO/conduit-connector-s3#1022). The plaintext-http warning is
// independent of that and still applies to AWS hosts.
func TestLogEndpointWarnings_PathStyleHint(t *testing.T) {
	const hint = "aws.pathStyle is false"
	const plaintext = "plaintext http endpoint"

	testCases := []struct {
		name          string
		endpoint      string
		pathStyle     bool
		wantHint      bool
		wantPlaintext bool
	}{
		{name: "no endpoint", endpoint: ""},
		{name: "aws regional https", endpoint: "https://s3.us-west-1.amazonaws.com"},
		{name: "aws dualstack https", endpoint: "https://s3.dualstack.us-east-1.amazonaws.com"},
		{name: "aws china https", endpoint: "https://s3.cn-north-1.amazonaws.com.cn"},
		{name: "aws regional http still warns about plaintext", endpoint: "http://s3.us-west-1.amazonaws.com", wantPlaintext: true},
		{name: "custom store virtual-hosted", endpoint: "https://s3.example.com", wantHint: true},
		{name: "custom store with path style", endpoint: "https://s3.example.com", pathStyle: true},
		{name: "minio on localhost", endpoint: "http://localhost:9000", wantHint: true},
		{name: "minio on localhost with path style", endpoint: "http://localhost:9000", pathStyle: true},
		{name: "look-alike host", endpoint: "https://s3.amazonaws.com.example.com", wantHint: true},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			var buf bytes.Buffer
			logger := zerolog.New(&buf)
			ctx := logger.WithContext(context.Background())

			Config{AWSEndpoint: tc.endpoint, AWSPathStyle: tc.pathStyle}.LogEndpointWarnings(ctx)

			is.Equal(bytes.Contains(buf.Bytes(), []byte(hint)), tc.wantHint)
			is.Equal(bytes.Contains(buf.Bytes(), []byte(plaintext)), tc.wantPlaintext)
		})
	}
}

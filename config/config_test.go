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
	"errors"
	"strings"
	"testing"

	"github.com/conduitio/conduit-commons/config"
	"github.com/matryer/is"
)

// exampleConfig deliberately contains neither aws.endpoint nor aws.pathStyle:
// it is the configuration every existing AWS user has, and TestParseConfig
// pins that it still decodes to the zero values for the two new fields, i.e.
// that the default AWS path is unchanged. The configuration that does set them
// is exampleConfigWithEndpoint below.
var exampleConfig = config.Config{
	"aws.accessKeyId":     "access-key-123",
	"aws.secretAccessKey": "secret-key-321",
	"aws.region":          "us-west-2",
	"aws.bucket":          "foobucket",
	"prefix":              "conduit-",
}

var exampleConfigWithEndpoint = config.Config{
	"aws.accessKeyId":     "access-key-123",
	"aws.secretAccessKey": "secret-key-321",
	"aws.region":          "us-west-2",
	"aws.bucket":          "foobucket",
	"aws.endpoint":        "http://localhost:9000",
	"aws.pathStyle":       "true",
	"prefix":              "conduit-",
}

func TestParseConfig(t *testing.T) {
	is := is.New(t)
	var got Config
	err := exampleConfig.DecodeInto(&got)
	want := Config{
		AWSAccessKeyID:     "access-key-123",
		AWSSecretAccessKey: "secret-key-321",
		AWSRegion:          "us-west-2",
		AWSBucket:          "foobucket",
		Prefix:             "conduit-",
		// neither new key is present: both must stay at their zero value
		AWSEndpoint:  "",
		AWSPathStyle: false,
	}
	is.NoErr(err)
	is.Equal(want, got)
}

func TestParseConfig_Endpoint(t *testing.T) {
	is := is.New(t)
	var got Config
	err := exampleConfigWithEndpoint.DecodeInto(&got)
	want := Config{
		AWSAccessKeyID:     "access-key-123",
		AWSSecretAccessKey: "secret-key-321",
		AWSRegion:          "us-west-2",
		AWSBucket:          "foobucket",
		AWSEndpoint:        "http://localhost:9000",
		AWSPathStyle:       true,
		Prefix:             "conduit-",
	}
	is.NoErr(err)
	is.Equal(want, got)
}

func TestValidateAWSEndpoint(t *testing.T) {
	is := is.New(t)

	testCases := []struct {
		name     string
		endpoint string
		wantErr  bool
		// wantCause, when set, must appear in the error message: it pins that
		// the underlying url.Parse failure is reported, not swallowed.
		wantCause string
	}{
		{name: "empty", endpoint: "", wantErr: false},
		{name: "http", endpoint: "http://localhost:9000", wantErr: false},
		{name: "https", endpoint: "https://s3.example.com", wantErr: false},
		{name: "scheme missing", endpoint: "localhost:9000", wantErr: true},
		{name: "unsupported scheme", endpoint: "ftp://s3.example.com", wantErr: true},
		{name: "not a URL", endpoint: "not a url", wantErr: true},
		{name: "unparseable", endpoint: "http://[::1", wantErr: true, wantCause: "missing ']' in host"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			err := ValidateEndpoint(tc.endpoint)
			if tc.wantErr {
				is.True(err != nil)
				// the error must name the failing config parameter
				is.True(strings.Contains(err.Error(), ConfigKeyAWSEndpoint))
				// and it must stay machine-actionable: dropping the %w on the
				// sentinel would make every caller's errors.Is check silently
				// false, which a substring assertion would not catch
				is.True(errors.Is(err, config.ErrInvalidParameterValue))
				// and it must suggest a fix
				is.True(strings.Contains(err.Error(), "http://localhost:9000"))
				if tc.wantCause != "" {
					is.True(strings.Contains(err.Error(), tc.wantCause))
				}
			} else {
				is.NoErr(err)
			}
		})
	}
}

func TestIsLoopbackHost(t *testing.T) {
	testCases := []struct {
		host string
		want bool
	}{
		{host: "localhost", want: true},
		{host: "minio.localhost", want: true},
		{host: "127.0.0.1", want: true},
		{host: "127.1.2.3", want: true},
		{host: "::1", want: true},
		{host: "minio.internal", want: false},
		{host: "s3.example.com", want: false},
		{host: "169.254.169.254", want: false},
		{host: "notlocalhost", want: false},
		{host: "", want: false},
	}
	for _, tc := range testCases {
		t.Run(tc.host, func(t *testing.T) {
			is := is.New(t)
			is.Equal(isLoopbackHost(tc.host), tc.want)
		})
	}
}

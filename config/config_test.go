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
	"strings"
	"testing"

	"github.com/conduitio/conduit-commons/config"
	"github.com/matryer/is"
)

var exampleConfig = config.Config{
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
	}{
		{name: "empty", endpoint: "", wantErr: false},
		{name: "http", endpoint: "http://localhost:9000", wantErr: false},
		{name: "https", endpoint: "https://s3.example.com", wantErr: false},
		{name: "scheme missing", endpoint: "localhost:9000", wantErr: true},
		{name: "unsupported scheme", endpoint: "ftp://s3.example.com", wantErr: true},
		{name: "not a URL", endpoint: "not a url", wantErr: true},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			err := ValidateEndpoint(tc.endpoint)
			if tc.wantErr {
				is.True(err != nil)
				// the error must name the failing config parameter
				is.True(strings.Contains(err.Error(), ConfigKeyAWSEndpoint))
			} else {
				is.NoErr(err)
			}
		})
	}
}

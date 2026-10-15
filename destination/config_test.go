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

package destination

import (
	"context"
	"strings"
	"testing"

	cconfig "github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-connector-s3/config"
	"github.com/matryer/is"
)

var exampleConfig = cconfig.Config{
	"aws.accessKeyId":     "access-key-123",
	"aws.secretAccessKey": "secret-key-321",
	"aws.region":          "us-west-2",
	"aws.bucket":          "foobucket",
	"format":              "json",
}

func TestParseConfig(t *testing.T) {
	is := is.New(t)
	var got Config
	err := exampleConfig.DecodeInto(&got)
	want := Config{
		Config: config.Config{
			AWSAccessKeyID:     "access-key-123",
			AWSSecretAccessKey: "secret-key-321",
			AWSRegion:          "us-west-2",
			AWSBucket:          "foobucket",
		},
		Format: "json",
	}

	is.NoErr(err)
	is.Equal(want, got)
}

func TestParseConfig_Partitioner(t *testing.T) {
	is := is.New(t)
	c := cconfig.Config{}
	for k, v := range exampleConfig {
		c[k] = v
	}
	c["partitioner"] = "field"
	c["partitioner.fields"] = "region,address.country"

	var got Config
	is.NoErr(c.DecodeInto(&got))
	is.Equal(got.Partitioner, "field")
	is.Equal(got.PartitionerFields, []string{"region", "address.country"})
	is.NoErr(got.Validate(context.Background()))
}

func TestValidate_PartitionerErrorsNameTheSetting(t *testing.T) {
	tests := map[string]cconfig.Config{
		"partitioner.fields":     {"partitioner": "field"},
		"partitioner.pathFormat": {"partitioner": "time", "partitioner.duration": "1h"},
		"partitioner.timezone":   {"partitioner": "hourly", "partitioner.timezone": "Nowhere/Land"},
	}
	for want, extra := range tests {
		t.Run(want, func(t *testing.T) {
			is := is.New(t)
			c := cconfig.Config{}
			for k, v := range exampleConfig {
				c[k] = v
			}
			for k, v := range extra {
				c[k] = v
			}
			var got Config
			is.NoErr(c.DecodeInto(&got))
			err := got.Validate(context.Background())
			is.True(err != nil)
			is.True(strings.Contains(err.Error(), want))
			is.True(strings.Contains(err.Error(), "s3.destination.partitioner_config"))
		})
	}
}

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
	"strings"
	"testing"
	"time"

	cconfig "github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-connector-s3/config"
	"github.com/matryer/is"
)

var exampleConfig = cconfig.Config{
	"aws.accessKeyId":     "access-key-123",
	"aws.secretAccessKey": "secret-key-321",
	"aws.region":          "us-west-2",
	"aws.bucket":          "foobucket",
	"pollingPeriod":       "2s",
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
		PollingPeriod: 2 * time.Second,
	}

	is.NoErr(err)
	is.Equal(want, got)
}

func TestConfig_Validate_LookbackPeriod(t *testing.T) {
	for _, tc := range []struct {
		name    string
		in      time.Duration
		wantErr bool
	}{
		{"zero is accepted (lossy, warned at Open)", 0, false},
		{"deprecated short value is accepted (warned at Open)", 10 * time.Second, false},
		{"default", 5 * time.Minute, false},
		{"above the warning threshold is accepted", 2 * time.Hour, false},
		{"exactly the maximum", MaxLookbackPeriod, false},
		{"above the maximum", MaxLookbackPeriod + time.Second, true},
		{"negative", -time.Second, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			c := Config{LookbackPeriod: tc.in, MaxEarlyObjects: 2048, MaxTrackedUploads: 256, MaxSettledBytesPerPoll: 1 << 26, MaxPendingUploadAge: MaxLookbackPeriod}
			err := c.Validate(t.Context())
			is.Equal(err != nil, tc.wantErr)
		})
	}
}

func TestConfig_Validate_CDCBounds(t *testing.T) {
	ok := func() Config {
		return Config{LookbackPeriod: 5 * time.Minute, MaxEarlyObjects: 2048, MaxTrackedUploads: 256, MaxSettledBytesPerPoll: 1 << 26, MaxPendingUploadAge: 24 * time.Hour}
	}
	for _, tc := range []struct {
		name string
		mut  func(*Config)
		want string // config key named in the error; "" for valid
	}{
		{"defaults", func(*Config) {}, ""},
		{"settled-only (no early set)", func(c *Config) { c.MaxEarlyObjects = 0 }, ""},
		{"early set too large", func(c *Config) { c.MaxEarlyObjects = 10001 }, ConfigKeyMaxEarlyObjects},
		{"negative early set", func(c *Config) { c.MaxEarlyObjects = -1 }, ConfigKeyMaxEarlyObjects},
		{"tracked set too large", func(c *Config) { c.MaxTrackedUploads = 10001 }, ConfigKeyMaxTrackedUploads},
		{"zero settled budget", func(c *Config) { c.MaxSettledBytesPerPoll = 0 }, ConfigKeyMaxSettledBytesPerPoll},
		{"pending age below the lookback", func(c *Config) { c.MaxPendingUploadAge = time.Minute }, ConfigKeyMaxPendingUploadAge},
	} {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			c := ok()
			tc.mut(&c)
			err := c.Validate(t.Context())
			if tc.want == "" {
				is.NoErr(err)
				return
			}
			is.True(err != nil)
			is.True(strings.Contains(err.Error(), tc.want))
		})
	}
}

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

package s3_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	cconfig "github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/hostpolicy"
	s3 "github.com/conduitio/conduit-connector-s3"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
)

// TestCredentialsConfig runs the full SDK parse + validate path (parameter
// specs, then the Validate hooks) for the source and the destination, so it
// covers both the removal of the "required" validation in connector.yaml and
// the both-or-neither rule. It needs no AWS account.
func TestCredentialsConfig(t *testing.T) {
	base := func(extras ...map[string]string) cconfig.Config {
		c := cconfig.Config{
			"aws.region": "us-west-2",
			"aws.bucket": "foobucket",
		}
		for _, extra := range extras {
			for k, v := range extra {
				c[k] = v
			}
		}
		return c
	}

	testCases := []struct {
		name     string
		extra    map[string]string
		wantErr  string
		wantCode hostpolicy.Code
	}{
		// No keys and no operator policy on the context: refused before the
		// default chain is loaded.
		{name: "no static keys, no operator policy", extra: nil, wantCode: hostpolicy.CodeAmbientCredentialsDisabled},
		{name: "both static keys", extra: map[string]string{"aws.accessKeyId": "id", "aws.secretAccessKey": "secret"}},
		{name: "only access key id", extra: map[string]string{"aws.accessKeyId": "id"}, wantErr: "aws.secretAccessKey"},
		{name: "only secret access key", extra: map[string]string{"aws.secretAccessKey": "secret"}, wantErr: "aws.accessKeyId"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			spec := s3.Connector.NewSpecification()
			for kind, run := range map[string]func() error{
				"source": func() error {
					src := s3.Connector.NewSource()
					return sdk.Util.ParseConfig(context.Background(), base(tc.extra), src.Config(), spec.SourceParams)
				},
				"destination": func() error {
					dst := s3.Connector.NewDestination()
					return sdk.Util.ParseConfig(context.Background(), base(tc.extra, map[string]string{"format": "json"}), dst.Config(), spec.DestinationParams)
				},
			} {
				t.Run(kind, func(t *testing.T) {
					is := is.New(t)
					err := run()
					if tc.wantCode != "" {
						he, ok := hostpolicy.AsError(err)
						is.True(ok)
						is.Equal(he.Code, tc.wantCode)
						return
					}
					if tc.wantErr == "" {
						is.NoErr(err)
						return
					}
					is.True(err != nil)
					is.True(errors.Is(err, cconfig.ErrRequiredParameterMissing))
					is.True(strings.Contains(err.Error(), tc.wantErr))
				})
			}
		})
	}
}

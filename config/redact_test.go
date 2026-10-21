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
	"errors"
	"strings"
	"testing"

	cconfig "github.com/conduitio/conduit-commons/config"
	"github.com/matryer/is"
	"github.com/rs/zerolog"
)

// The user info of an endpoint URL is a credential, even when the URL is
// otherwise wrong, so no validation error or warning may repeat it.
func TestValidateEndpoint_DoesNotEchoUserInfo(t *testing.T) {
	const user, pass = "SECRET-user", "SECRET-pass"
	testCases := []struct {
		name     string
		endpoint string
		wantText string // the remaining, non-secret part that must stay
	}{
		{name: "unparseable port", endpoint: "http://" + user + ":" + pass + "@localhost:badport", wantText: "invalid port"},
		{name: "unparseable host", endpoint: "http://" + user + ":" + pass + "@[::1", wantText: "missing ']' in host"},
		{name: "bad scheme", endpoint: "ftp://" + user + ":" + pass + "@s3.example.com", wantText: "scheme must be http or https"},
		{name: "token as user name", endpoint: "ftp://" + user + "@s3.example.com", wantText: "scheme must be http or https"},
		{name: "host missing", endpoint: "http://" + user + ":" + pass + "@", wantText: "host is missing"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			err := ValidateEndpoint(tc.endpoint)
			is.True(err != nil)
			is.True(errors.Is(err, cconfig.ErrInvalidParameterValue)) // still machine-actionable
			is.True(strings.Contains(err.Error(), tc.wantText))       // still says what is wrong
			is.True(!strings.Contains(err.Error(), "SECRET"))
		})
	}
}

func TestLogEndpointWarnings_DoesNotEchoUserInfo(t *testing.T) {
	is := is.New(t)
	var buf bytes.Buffer
	ctx := zerolog.New(&buf).WithContext(context.Background())

	// plaintext http on a non-loopback host, and virtual-hosted addressing: both warnings
	Config{AWSEndpoint: "http://SECRET-user:SECRET-pass@s3.example.com:9000"}.LogEndpointWarnings(ctx)

	out := buf.String()
	is.True(strings.Contains(out, "plaintext http endpoint"))
	is.True(strings.Contains(out, "aws.pathStyle is false"))
	is.True(strings.Contains(out, "s3.example.com:9000"))
	is.True(!strings.Contains(out, "SECRET"))
}

func TestRedactURL(t *testing.T) {
	testCases := []struct{ in, want string }{
		{"http://localhost:9000", "http://localhost:9000"},
		{"https://s3.example.com/path?x=1", "https://s3.example.com/path?x=1"},
		{"http://user:pass@localhost:9000", "http://redacted:xxxxx@localhost:9000"},
		{"http://token@localhost:9000/p", "http://redacted:xxxxx@localhost:9000/p"},
		{"http://user:pass@localhost:badport", "http://redacted:xxxxx@localhost:badport"},
		{"not a url", "not a url"},
		{"", ""},
	}
	for _, tc := range testCases {
		t.Run(tc.in, func(t *testing.T) {
			is := is.New(t)
			is.Equal(redactURL(tc.in), tc.want)
		})
	}
}

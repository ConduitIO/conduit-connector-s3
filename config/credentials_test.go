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
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	cconfig "github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/hostpolicy"
	"github.com/matryer/is"
)

func TestCheckCredentialPair(t *testing.T) {
	testCases := []struct {
		name     string
		id, sec  string
		wantPath string
	}{
		{name: "neither set"},
		{name: "both set", id: "id", sec: "secret"},
		{name: "only access key id", id: "id", wantPath: ConfigKeyAWSSecretAccessKey},
		{name: "only secret access key", sec: "secret", wantPath: ConfigKeyAWSAccessKeyID},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			err := CheckCredentialPair(tc.id, tc.sec)
			if tc.wantPath == "" {
				is.NoErr(err)
				return
			}
			// stays machine-actionable both ways
			is.True(errors.Is(err, cconfig.ErrRequiredParameterMissing))
			he, ok := hostpolicy.AsError(err)
			is.True(ok)
			is.Equal(he.Code, hostpolicy.CodeCredentialsIncomplete)
			is.Equal(he.ConfigPath, tc.wantPath)
			is.True(strings.Contains(he.Suggestion, "set both"))
		})
	}
}

// isolateAWSEnv removes every ambient source of AWS credentials so a test only
// sees what it sets itself, regardless of the developer or CI machine. It
// needs no AWS account and makes no network call.
func isolateAWSEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{
		"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN",
		// legacy aliases the SDK still honors
		"AWS_ACCESS_KEY", "AWS_SECRET_KEY",
		"AWS_PROFILE", "AWS_DEFAULT_PROFILE", "AWS_ROLE_ARN",
		"AWS_WEB_IDENTITY_TOKEN_FILE", "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
		"AWS_CONTAINER_CREDENTIALS_FULL_URI", "AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL_S3",
		"AWS_ENDPOINT_URL_STS", "AWS_IGNORE_CONFIGURED_ENDPOINT_URLS", "AWS_CA_BUNDLE",
		"AWS_EC2_METADATA_SERVICE_ENDPOINT", "AWS_MAX_ATTEMPTS", "AWS_RETRY_MODE",
		"AWS_REGION", "AWS_DEFAULT_REGION",
		"HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy",
	} {
		t.Setenv(k, "")
	}
	dir := t.TempDir()
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(dir, "no-credentials"))
	t.Setenv("AWS_CONFIG_FILE", filepath.Join(dir, "no-config"))
	// no instance role lookup, which would otherwise try to reach 169.254.169.254
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
}

func TestLoadAWSConfig_StaticCredentials(t *testing.T) {
	is := is.New(t)
	isolateAWSEnv(t)
	// static keys must win over the environment
	t.Setenv("AWS_ACCESS_KEY_ID", "env-id")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "env-secret")

	cfg, err := LoadAWSConfig(context.Background(), "us-west-2", "static-id", "static-secret", "")
	is.NoErr(err)
	is.Equal(cfg.Region, "us-west-2")

	creds, err := cfg.Credentials.Retrieve(context.Background())
	is.NoErr(err)
	is.Equal(creds.AccessKeyID, "static-id")
	is.Equal(creds.SecretAccessKey, "static-secret")
	is.Equal(creds.Source, "StaticCredentials")
}

func TestLoadAWSConfig_DefaultChain_Env(t *testing.T) {
	is := is.New(t)
	isolateAWSEnv(t)
	t.Setenv("AWS_ACCESS_KEY_ID", "env-id")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "env-secret")
	t.Setenv("AWS_SESSION_TOKEN", "env-token")

	cfg, err := LoadAWSConfig(context.Background(), "eu-west-1", "", "", "")
	is.NoErr(err)
	is.Equal(cfg.Region, "eu-west-1")

	creds, err := cfg.Credentials.Retrieve(context.Background())
	is.NoErr(err)
	is.Equal(creds.AccessKeyID, "env-id")
	is.Equal(creds.SecretAccessKey, "env-secret")
	is.Equal(creds.SessionToken, "env-token")
	is.Equal(creds.Source, "EnvConfigCredentials")
}

func TestLoadAWSConfig_DefaultChain_SharedProfile(t *testing.T) {
	is := is.New(t)
	isolateAWSEnv(t)

	credsFile := filepath.Join(t.TempDir(), "credentials")
	is.NoErr(os.WriteFile(credsFile, []byte(
		"[conduit]\naws_access_key_id = profile-id\naws_secret_access_key = profile-secret\n",
	), 0o600))
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", credsFile)
	t.Setenv("AWS_PROFILE", "conduit")

	cfg, err := LoadAWSConfig(context.Background(), "us-east-1", "", "", "")
	is.NoErr(err)

	creds, err := cfg.Credentials.Retrieve(context.Background())
	is.NoErr(err)
	is.Equal(creds.AccessKeyID, "profile-id")
	is.Equal(creds.SecretAccessKey, "profile-secret")
}

// TestLoadAWSConfig_DefaultChain_Empty pins that an empty chain is not a
// load-time error: the SDK resolves lazily, so the failure shows up when the
// credentials are first used (documented in the connector description).
func TestLoadAWSConfig_DefaultChain_Empty(t *testing.T) {
	is := is.New(t)
	isolateAWSEnv(t)

	cfg, err := LoadAWSConfig(context.Background(), "us-east-1", "", "", "")
	is.NoErr(err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err = cfg.Credentials.Retrieve(ctx)
	is.True(err != nil)
}

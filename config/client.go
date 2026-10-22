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
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	smithyendpoints "github.com/aws/smithy-go/endpoints"
	cconfig "github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/hostpolicy"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

// Credential selection and the ambient-credentials policy.
//
// Static keys (both aws.accessKeyId and aws.secretAccessKey set) work as they
// always have, with any endpoint; the operator policy is not consulted.
// Exactly one key set is an error, never a fall-through to the host identity.
// No keys means the AWS SDK default credential chain (the host's ambient
// identity), which is allowed only when the Conduit operator enabled it: the
// engine puts a hostpolicy.Policy on the context of built-in connector calls,
// and a missing policy (standalone plugin, older engine, test) denies.
//
// Design: ConduitIO/conduit docs/design-documents/20261012-builtin-connector-ambient-credentials.md.

// sdkConfiguredEndpoint names the endpoint source in errors when it came from
// AWS_ENDPOINT_URL_S3, AWS_ENDPOINT_URL or the shared config rather than from
// aws.endpoint; the resolved SDK options do not say which of those won.
const sdkConfiguredEndpoint = "SDK-configured endpoint (environment or shared config)"

var regionPattern = regexp.MustCompile(`^[a-z0-9-]+$`)

// ClientParams is what NewS3Client needs from the source or destination
// configuration.
type ClientParams struct {
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
	Region          string
	Bucket          string
	Endpoint        string
	PathStyle       bool
}

// ClientParams returns the client parameters of c.
func (c Config) ClientParams() ClientParams {
	return ClientParams{
		AccessKeyID:     c.AWSAccessKeyID,
		SecretAccessKey: c.AWSSecretAccessKey,
		Region:          c.AWSRegion,
		Bucket:          c.AWSBucket,
		Endpoint:        c.AWSEndpoint,
		PathStyle:       c.AWSPathStyle,
	}
}

// Purpose says why NewS3Client is called.
type Purpose int

const (
	// PurposeValidate checks the configuration at configure time. Refusals are
	// reported; use is not.
	PurposeValidate Purpose = iota
	// PurposeOpen builds the client the connector runs with, and reports the
	// use of ambient credentials (the audit log line).
	PurposeOpen
)

// Auth records how a client authenticates, so errors can be treated
// accordingly. The zero value means static credentials.
type Auth struct {
	ambient bool
}

// NewAuth returns an Auth for ambient (true) or static (false) credentials.
// NewS3Client is the normal way to get one; this exists for code that holds
// a client built elsewhere, such as tests with a stub iterator.
func NewAuth(ambient bool) Auth { return Auth{ambient: ambient} }

// Ambient reports whether the client uses the host's ambient credentials.
func (a Auth) Ambient() bool { return a.ambient }

// CheckCredentialPair returns connector.credentials_incomplete when exactly
// one of the static keys is set. It never consults the policy: a typo in one
// key must not turn a static-credential pipeline into one that runs as the
// host. The error also wraps cconfig.ErrRequiredParameterMissing.
func CheckCredentialPair(accessKeyID, secretAccessKey string) error {
	hasID, hasSecret := accessKeyID != "", secretAccessKey != ""
	var missing, present string
	switch {
	case hasID && !hasSecret:
		missing, present = ConfigKeyAWSSecretAccessKey, ConfigKeyAWSAccessKeyID
	case !hasID && hasSecret:
		missing, present = ConfigKeyAWSAccessKeyID, ConfigKeyAWSSecretAccessKey
	default:
		return nil
	}
	he := hostpolicy.NewError(hostpolicy.CodeCredentialsIncomplete,
		fmt.Sprintf("%s is set but %s is missing", present, missing)).At(missing)
	he.Suggestion = fmt.Sprintf("set both %s and %s to use static credentials, or remove %s to use ambient credentials (if the Conduit operator allows them)",
		ConfigKeyAWSAccessKeyID, ConfigKeyAWSSecretAccessKey, present)
	return fmt.Errorf("%w: %w", he, cconfig.ErrRequiredParameterMissing)
}

// NewS3Client builds the S3 client the source and the destination use. It is
// the single place credentials are chosen and the ambient-credentials policy
// is enforced.
func NewS3Client(ctx context.Context, p ClientParams, purpose Purpose) (*s3.Client, Auth, error) {
	if err := CheckCredentialPair(p.AccessKeyID, p.SecretAccessKey); err != nil {
		return nil, Auth{}, err
	}
	if p.AccessKeyID != "" {
		// Static keys: unchanged behavior, policy not consulted.
		awsCfg, err := LoadAWSConfig(ctx, p.Region, p.AccessKeyID, p.SecretAccessKey, p.SessionToken)
		if err != nil {
			return nil, Auth{}, err
		}
		return s3.NewFromConfig(awsCfg, func(o *s3.Options) {
			if p.Endpoint != "" {
				o.BaseEndpoint = aws.String(p.Endpoint)
			}
			o.UsePathStyle = p.PathStyle
			o.HTTPClient = HideObjectKeyInErrors(awsCfg.HTTPClient)
		}), Auth{}, nil
	}
	client, err := newAmbientClient(ctx, p, purpose)
	return client, Auth{ambient: true}, err
}

// deny reports a refusal to the engine and returns it.
func deny(ctx context.Context, pol hostpolicy.Policy, err error, path string) error {
	he, ok := hostpolicy.AsError(err)
	if !ok {
		return err
	}
	if path != "" {
		he = he.At(path)
	}
	// The engine's Observe hook writes the WARN log line and the metric.
	pol.Report(ctx, hostpolicy.Denied(he))
	return he
}

func newAmbientClient(ctx context.Context, p ClientParams, purpose Purpose) (*s3.Client, error) {
	pol := hostpolicy.FromContext(ctx)

	// Invariant (ambient credentials): nothing below loads the default chain
	// unless the operator's policy, put on the context by the engine, allows it.
	if !pol.AmbientCredentials {
		he := hostpolicy.NewError(hostpolicy.CodeAmbientCredentialsDisabled,
			"no static credentials are set and ambient credentials are not enabled on this Conduit instance")
		he.Suggestion = fmt.Sprintf("set %s and %s, or ask the operator to set connectors.ambient-credentials.enabled=true "+
			"and list this bucket in connectors.ambient-credentials.allow-resources; the pipeline must use builtin:s3",
			ConfigKeyAWSAccessKeyID, ConfigKeyAWSSecretAccessKey)
		return nil, deny(ctx, pol, he, ConfigKeyAWSAccessKeyID)
	}
	if !regionPattern.MatchString(p.Region) {
		return nil, fmt.Errorf("%s: %q is not a valid AWS region name (lower-case letters, digits and dashes): %w",
			ConfigKeyAWSRegion, p.Region, cconfig.ErrInvalidParameterValue)
	}
	if err := pol.CheckResource(p.Bucket); err != nil {
		return nil, deny(ctx, pol, err, ConfigKeyAWSBucket)
	}
	if err := pol.CheckProxy(os.Getenv); err != nil {
		return nil, deny(ctx, pol, err, ConfigKeyAWSEndpoint)
	}

	awsCfg, err := LoadAWSConfig(ctx, p.Region, "", "", "")
	if err != nil {
		return nil, err
	}

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if p.Endpoint != "" {
			o.BaseEndpoint = aws.String(p.Endpoint)
		}
		o.UsePathStyle = p.PathStyle
		// The guards go on the S3 service client only. awsCfg.HTTPClient is
		// what the credential providers (IMDS, ECS, STS) use, and they must
		// reach exactly the link-local addresses DialControl refuses.
		o.HTTPClient = HideObjectKeyInErrors(guardedHTTPClient(awsCfg.HTTPClient))
		o.EndpointResolverV2 = &checkingResolver{next: s3.NewDefaultEndpointResolverV2(), policy: pol, configured: p.Endpoint}
	})

	// Check the endpoint the SDK resolved, whatever source won (aws.endpoint,
	// AWS_ENDPOINT_URL_S3, AWS_ENDPOINT_URL, profile endpoint_url or services).
	// Nil means the SDK derives it from the validated region: accepted.
	endpointHost := ""
	if be := client.Options().BaseEndpoint; be != nil {
		path := ""
		if p.Endpoint != "" && *be == p.Endpoint {
			path = ConfigKeyAWSEndpoint
		}
		u, perr := url.Parse(*be)
		if perr != nil {
			return nil, deny(ctx, pol, hostpolicy.NewError(hostpolicy.CodeEndpointNotAllowed,
				endpointLabel(path)+": endpoint is not a valid URL"), path)
		}
		if err := pol.CheckEndpoint(u); err != nil {
			he, _ := hostpolicy.AsError(err)
			he = he.At(path)
			he.Message = endpointLabel(path) + ": " + he.Message
			return nil, deny(ctx, pol, he, "")
		}
		endpointHost = u.Host
	}

	if purpose == PurposeOpen {
		// The engine's Observe hook writes the audit log line and the metric.
		pol.Report(ctx, hostpolicy.Used(p.Region, endpointHost))
	}
	return client, nil
}

func endpointLabel(path string) string {
	if path == "" {
		return sdkConfiguredEndpoint
	}
	return path
}

// guardedHTTPClient is the HTTP client the SDK configuration built (so a
// custom CA bundle from AWS_CA_BUNDLE or the profile still applies) with two
// changes: the dialer refuses link-local and metadata addresses, and
// redirects are refused unless they stay on the same host over https.
// Same-host redirects follow the SDK's own rule (only 307 and 308, which keep
// the method). WithDialerOptions clones, so base itself, which the credential
// providers use, stays unguarded.
func guardedHTTPClient(base aws.HTTPClient) *http.Client {
	b, ok := base.(*awshttp.BuildableClient)
	if !ok {
		b = awshttp.NewBuildableClient()
	}
	b = b.WithDialerOptions(func(d *net.Dialer) {
		d.Control = hostpolicy.DialControl()
	})
	guard := hostpolicy.RedirectGuard()
	return &http.Client{
		Timeout:   b.GetTimeout(),
		Transport: b.GetTransport(),
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if err := guard(req, via); err != nil {
				return err
			}
			if req.Response != nil && req.Response.StatusCode != http.StatusTemporaryRedirect &&
				req.Response.StatusCode != http.StatusPermanentRedirect {
				return http.ErrUseLastResponse
			}
			return nil
		},
	}
}

// checkingResolver applies the endpoint check on every request, so an
// endpoint that changes after Configure (or a per-operation override) cannot
// widen the set. It checks the base endpoint, not the resolved URI, which
// carries the virtual-host bucket prefix.
type checkingResolver struct {
	next   s3.EndpointResolverV2
	policy hostpolicy.Policy
	// configured is aws.endpoint; a refusal names that config path only when
	// the checked endpoint is exactly it.
	configured string
}

func (r *checkingResolver) ResolveEndpoint(ctx context.Context, params s3.EndpointParameters) (smithyendpoints.Endpoint, error) {
	if params.Endpoint != nil {
		u, err := url.Parse(*params.Endpoint)
		if err != nil {
			return smithyendpoints.Endpoint{}, hostpolicy.NewError(hostpolicy.CodeEndpointNotAllowed, "endpoint is not a valid URL")
		}
		if err := r.policy.CheckEndpoint(u); err != nil {
			path := ""
			if r.configured != "" && *params.Endpoint == r.configured {
				path = ConfigKeyAWSEndpoint
			}
			return smithyendpoints.Endpoint{}, deny(ctx, r.policy, err, path)
		}
	}
	ep, err := r.next.ResolveEndpoint(ctx, params)
	if err != nil {
		return ep, fmt.Errorf("resolve S3 endpoint: %w", err)
	}
	return ep, nil
}

// arnPattern matches AWS ARNs that carry an account ID, such as an assumed
// role arn:aws:sts::123456789012:assumed-role/Role/session.
var arnPattern = regexp.MustCompile(`arn:aws[a-zA-Z-]*:[a-zA-Z0-9-]*:[a-zA-Z0-9-]*:[0-9]{12}:[^\s"',;)]*`)

// accountPattern matches a 12-digit account ID directly after an "account" or
// "account id" label. Other 12-digit numbers (request IDs, byte counts) are
// left alone.
var accountPattern = regexp.MustCompile(`(?i)(\baccount(?:[ _-]?id)?["']?\s*[:=]?\s*["']?)[0-9]{12}\b`)

// ScrubText replaces role ARNs and account IDs in AWS error text.
func ScrubText(s string) string {
	s = arnPattern.ReplaceAllString(s, "arn:<redacted>")
	return accountPattern.ReplaceAllString(s, "${1}<redacted>")
}

// scrubbedError carries the scrubbed message for the pipeline author while
// keeping the original error in the chain, so error classification (retry,
// backoff, not-found) keeps working.
type scrubbedError struct {
	msg string
	err error
}

func (e *scrubbedError) Error() string { return e.msg }
func (e *scrubbedError) Unwrap() error { return e.err }

// Scrub returns err with role ARNs and account IDs removed from its message
// when the client uses ambient credentials, and logs the full text to the
// engine log, which the operator owns. With static credentials the identity
// is the pipeline author's own, and err is returned unchanged.
func (a Auth) Scrub(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	// Whatever the credentials: the store's message text is not passed on.
	err = withholdStoreMessage(err)
	if !a.ambient {
		return err
	}
	msg := err.Error()
	scrubbed := ScrubText(msg)
	if scrubbed == msg {
		return err
	}
	var already *scrubbedError
	if errors.As(err, &already) {
		return err
	}
	sdk.Logger(ctx).Warn().Err(err).Msg("AWS error with identity details; the pipeline sees a scrubbed copy")
	return &scrubbedError{msg: scrubbed, err: err}
}

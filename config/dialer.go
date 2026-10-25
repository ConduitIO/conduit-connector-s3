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
	"net"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/ec2rolecreds"
	"github.com/aws/aws-sdk-go-v2/credentials/endpointcreds"
	"github.com/aws/aws-sdk-go-v2/feature/ec2/imds"
	"github.com/conduitio/conduit-commons/dialer"
)

// Host-supplied dialer.
//
// When the host (the Conduit engine) puts a dialer on the context of a call
// that builds a client, every network connection the connector opens goes
// through it: the S3 service client of the source and the destination, and the
// HTTP clients of the credential providers (STS for AssumeRole and web
// identity, SSO, the EC2 instance metadata client, the ECS / container
// credentials endpoint client). The dialer is read once, when the client is
// built, and captured; later calls on the client do not consult the context.
// Without a dialer on the context nothing changes: the SDK default dialer is
// used, together with the existing dial-time address checks.
//
// The AWS SDK rebuilds the net.Dialer of a *awshttp.BuildableClient in two
// places, and each resets Transport.DialContext, which would silently replace
// the host dialer: the EC2 instance metadata client always does it, and every
// service client does it when a defaults mode other than "legacy" is set. The
// instance metadata client is therefore built from a frozen (plain) HTTP
// client, and the defaults mode is pinned to legacy while a host dialer is in
// use (see dialerLoadOptions).

// hostDialer returns the dialer the host supplied on ctx, if any.
func hostDialer(ctx context.Context) (dialer.DialContextFunc, bool) {
	return dialer.FromContext(ctx)
}

// withDialContext returns b with dial installed as the transport's DialContext.
func withDialContext(b *awshttp.BuildableClient, dial dialer.DialContextFunc) *awshttp.BuildableClient {
	return b.WithTransportOptions(func(tr *http.Transport) {
		tr.DialContext = dial // install site: host dialer
	})
}

// imdsHTTPClient is the HTTP client of the EC2 instance metadata provider. It
// mirrors the fail-fast timeouts the SDK applies to that client (it only does
// so for a BuildableClient, which this one cannot be), so a host without the
// service does not stall start-up.
func imdsHTTPClient(dial dialer.DialContextFunc) aws.HTTPClient {
	const (
		dialTimeout           = 250 * time.Millisecond
		responseHeaderTimeout = 500 * time.Millisecond
	)
	return awshttp.NewBuildableClient().WithTransportOptions(func(tr *http.Transport) {
		tr.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) { // install site: host dialer
			ctx, cancel := context.WithTimeout(ctx, dialTimeout)
			defer cancel()
			return dial(ctx, network, address)
		}
		tr.ResponseHeaderTimeout = responseHeaderTimeout
	}).Freeze()
}

// dialerLoadOptions returns the config load options that route credential
// resolution through dial.
func dialerLoadOptions(dial dialer.DialContextFunc) []func(*awsconfig.LoadOptions) error {
	hc := withDialContext(awshttp.NewBuildableClient(), dial)
	return []func(*awsconfig.LoadOptions) error{
		// Used by the service clients and by the STS, SSO and web identity
		// providers the SDK builds from the loaded config. A custom CA bundle
		// (AWS_CA_BUNDLE) is applied on top of it by the SDK, keeping the dialer.
		awsconfig.WithHTTPClient(hc),
		// A defaults mode other than legacy makes every service client rebuild
		// the dialer of this client, dropping the host dialer. Trade-off: the
		// connect timeouts and retry mode a defaults mode would set are not
		// applied while a host dialer is in use.
		awsconfig.WithDefaultsMode(aws.DefaultsModeLegacy),
		// The IMDS client takes its HTTP client from the config, but it must
		// keep its short timeouts and not be rebuilt by the SDK.
		awsconfig.WithEC2RoleCredentialOptions(func(o *ec2rolecreds.Options) {
			o.Client = imds.New(imds.Options{HTTPClient: imdsHTTPClient(dial)})
		}),
		// The container credentials provider ignores the config's HTTP client
		// and builds its own, so it is set here.
		awsconfig.WithEndpointCredentialOptions(func(o *endpointcreds.Options) {
			o.HTTPClient = hc
		}),
	}
}

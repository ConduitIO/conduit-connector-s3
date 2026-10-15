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
	"fmt"
	"strings"

	"github.com/conduitio/conduit-connector-s3/config"
	"github.com/conduitio/conduit-connector-s3/destination/format"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

const (
	// ConfigKeyFormat is the config name for destination format.
	ConfigKeyFormat = "format"

	// ConfigKeyServerSideEncryption is the config name for the server-side
	// encryption mode sent with every PutObject.
	ConfigKeyServerSideEncryption = "aws.serverSideEncryption"

	// ConfigKeySSEKMSKeyID is the config name for the KMS key used with
	// aws:kms or aws:kms:dsse server-side encryption.
	ConfigKeySSEKMSKeyID = "aws.sseKmsKeyId"
)

// Server-side encryption modes accepted by aws.serverSideEncryption. The empty
// value sends no encryption header.
const (
	SSENone      = ""
	SSEAES256    = "AES256"
	SSEKMS       = "aws:kms"
	SSEKMSDSSE   = "aws:kms:dsse"
	sseModesHelp = `"" (bucket default), "AES256", "aws:kms" or "aws:kms:dsse"`
)

// Config represents S3 configuration with Destination specific configurations
type Config struct {
	sdk.DefaultDestinationMiddleware
	config.Config

	// the destination format, either "json" or "parquet".
	Format format.Format `validate:"required,inclusion=parquet|json"`

	// Server-side encryption to request for every object written: AES256
	// (SSE-S3), aws:kms (SSE-KMS) or aws:kms:dsse (DSSE-KMS). Leave empty to
	// send no encryption header, so the bucket's default encryption applies
	// (on AWS every bucket encrypts with SSE-S3 by default; an S3-compatible
	// store may store the object unencrypted). An explicit value overrides the
	// bucket default: setting AES256 on a bucket whose default is SSE-KMS
	// stores objects with SSE-S3 instead.
	ServerSideEncryption string `json:"aws.serverSideEncryption"`
	// The KMS key ID, key ARN, alias name or alias ARN to encrypt objects
	// with. Only valid when aws.serverSideEncryption is aws:kms or
	// aws:kms:dsse; when empty with those modes, S3 uses the bucket's default
	// KMS key, or the AWS managed key aws/s3.
	SSEKMSKeyID string `json:"aws.sseKmsKeyId"`
}

// Validate implements sdk.Validatable: the SDK calls it automatically after
// configuration parsing. It runs the middleware validations and the ones the
// parameter specifications cannot express, such as the aws.endpoint URL shape.
func (c *Config) Validate(ctx context.Context) error {
	if err := c.DefaultDestinationMiddleware.Validate(ctx); err != nil {
		return err
	}
	if err := config.ValidateEndpoint(c.AWSEndpoint); err != nil {
		return err
	}
	return c.validateEncryption()
}

// validateEncryption checks aws.serverSideEncryption and aws.sseKmsKeyId. A
// KMS key without a KMS mode is rejected rather than ignored: the user asked
// for a specific key, and silently writing objects without it would be the
// same kind of downgrade as #1047.
func (c *Config) validateEncryption() error {
	switch c.ServerSideEncryption {
	case SSENone, SSEAES256, SSEKMS, SSEKMSDSSE:
	default:
		return fmt.Errorf("%s: unsupported value %q, must be one of %s: %w",
			ConfigKeyServerSideEncryption, c.ServerSideEncryption, sseModesHelp, config.ErrInvalidParameterValue)
	}
	if strings.TrimSpace(c.SSEKMSKeyID) != c.SSEKMSKeyID {
		// A key id with surrounding whitespace is never valid, and trimming
		// silently would hide a copy-paste error in a security setting.
		return fmt.Errorf("%s: %q has leading or trailing whitespace; remove it: %w",
			ConfigKeySSEKMSKeyID, c.SSEKMSKeyID, config.ErrInvalidParameterValue)
	}
	if c.SSEKMSKeyID != "" && c.ServerSideEncryption != SSEKMS && c.ServerSideEncryption != SSEKMSDSSE {
		return fmt.Errorf("%s is set but %s is %q: a KMS key is only used with aws:kms or aws:kms:dsse (set %s to aws:kms, or remove %s): %w",
			ConfigKeySSEKMSKeyID, ConfigKeyServerSideEncryption, c.ServerSideEncryption,
			ConfigKeyServerSideEncryption, ConfigKeySSEKMSKeyID, config.ErrInvalidParameterValue)
	}
	return nil
}

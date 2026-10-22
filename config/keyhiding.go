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
	"errors"
	"net/http"
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/smithy-go"
)

// keyHidingHTTPClient wraps the HTTP client of an S3 client so that a failed
// request does not carry the object key.
//
// The SDK builds the request URL from the bucket and the object key, and a
// transport failure (a refused connection, a timeout, a TLS error) comes back
// as a *url.Error that repeats the whole URL, which the SDK then puts into the
// error text of the operation ("request send failed, Get \"https://...\""). An
// object key is the record key, and errors reach the DLQ, the API and CLI
// output. The URL is cut down to scheme and host.
type keyHidingHTTPClient struct{ base aws.HTTPClient }

// HideObjectKeyInErrors returns base (or the SDK's default HTTP client when
// base is nil) wrapped so that the errors of a failed request name only the
// scheme and host of the URL, never the path, which holds the object key.
func HideObjectKeyInErrors(base aws.HTTPClient) aws.HTTPClient {
	if base == nil {
		base = awshttp.NewBuildableClient()
	}
	return keyHidingHTTPClient{base: base}
}

func (c keyHidingHTTPClient) Do(req *http.Request) (*http.Response, error) {
	resp, err := c.base.Do(req)
	if err != nil {
		return resp, hideURLPath(err)
	}
	return resp, nil
}

// hideURLPath returns err with the URL of a *url.Error reduced to its scheme
// and host. Anything else is returned as it is.
func hideURLPath(err error) error {
	var ue *url.Error
	if !errors.As(err, &ue) {
		return err
	}
	hidden := "(URL hidden)"
	if u, perr := url.Parse(ue.URL); perr == nil && u.Host != "" {
		hidden = u.Scheme + "://" + u.Host + "/(object key hidden)"
	}
	return &url.Error{Op: ue.Op, URL: hidden, Err: ue.Err}
}

// storeMessageError is an error of the store without the text of its message.
type storeMessageError struct {
	msg string
	err error
}

func (e *storeMessageError) Error() string { return e.msg }
func (e *storeMessageError) Unwrap() error { return e.err }

// withholdStoreMessage returns err with the message of the store's answer
// (the <Message> of the S3 error response) replaced, keeping the operation, the
// HTTP status and the error code. S3-compatible stores put the object key,
// parts of the request and sometimes values into it, and errors reach the DLQ,
// the API and CLI output. The original error stays in the chain.
func withholdStoreMessage(err error) error {
	var already *storeMessageError
	if errors.As(err, &already) {
		return err
	}
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		return err
	}
	text := err.Error()
	if m := apiErr.ErrorMessage(); m != "" {
		text = strings.ReplaceAll(text, m, "(message withheld: it can quote object keys)")
	}
	if text == err.Error() {
		return err
	}
	return &storeMessageError{msg: text, err: err}
}

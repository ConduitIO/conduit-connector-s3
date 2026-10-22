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
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/matryer/is"
)

// The SDK puts the object key in the request URL, and a transport failure
// repeats the URL in its error. Errors reach the DLQ, the API and CLI output,
// and an object key is the record key.
func TestS3Client_TransportErrorsDoNotContainTheObjectKey(t *testing.T) {
	const key = "customers/SECRET-acme/invoice-4711.csv"
	srv := httptest.NewServer(http.NotFoundHandler())
	endpoint := srv.URL
	srv.Close() // nothing listens any more

	client, _, err := NewS3Client(context.Background(), ClientParams{
		AccessKeyID: "a", SecretAccessKey: "b", Region: "us-east-1", Bucket: "b", Endpoint: endpoint, PathStyle: true,
	}, PurposeOpen)
	if err != nil {
		t.Fatal(err)
	}

	for name, call := range map[string]func() error{
		"GetObject": func() error {
			_, err := client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String("b"), Key: aws.String(key)})
			return err
		},
		"PutObject": func() error {
			_, err := client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String("b"), Key: aws.String(key)})
			return err
		},
		"ListObjectVersions with a key marker": func() error {
			_, err := client.ListObjectVersions(context.Background(), &s3.ListObjectVersionsInput{Bucket: aws.String("b"), KeyMarker: aws.String(key)})
			return err
		},
		"ListObjectsV2 starting after a key": func() error {
			_, err := client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: aws.String("b"), StartAfter: aws.String(key)})
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			is := is.New(t)
			err := call()
			is.True(err != nil)
			is.True(!strings.Contains(err.Error(), "SECRET"))
			is.True(!strings.Contains(err.Error(), "invoice"))
		})
	}
}

// S3-compatible stores put the object key and parts of the request into the
// <Message> of an error response. The code and the status stay; the message
// goes.
func TestScrub_WithholdsTheStoreMessage(t *testing.T) {
	const key = "customers/SECRET-acme/invoice-4711.csv"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`<?xml version="1.0"?><Error><Code>InvalidObjectName</Code><Message>Object name ` + key + ` contains unsupported characters</Message></Error>`))
	}))
	defer srv.Close()

	for name, params := range map[string]ClientParams{
		"static keys": {AccessKeyID: "a", SecretAccessKey: "b", Region: "us-east-1", Bucket: "b", Endpoint: srv.URL, PathStyle: true},
	} {
		t.Run(name, func(t *testing.T) {
			is := is.New(t)
			client, auth, err := NewS3Client(context.Background(), params, PurposeOpen)
			is.NoErr(err)
			_, err = client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String("b"), Key: aws.String(key)})
			is.True(err != nil)
			is.True(strings.Contains(err.Error(), "SECRET")) // the premise: the store's message reaches the error

			got := auth.Scrub(context.Background(), err)
			is.True(!strings.Contains(got.Error(), "SECRET"))
			is.True(strings.Contains(got.Error(), "InvalidObjectName")) // the code stays
			is.True(strings.Contains(got.Error(), "StatusCode: 400"))   // and the status
			var apiErr smithy.APIError
			is.True(errors.As(got, &apiErr)) // and the chain, for classification
		})
	}
}

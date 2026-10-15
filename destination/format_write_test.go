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

package destination_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/conduitio/conduit-commons/opencdc"
	s3Conn "github.com/conduitio/conduit-connector-s3"
	"github.com/conduitio/conduit-connector-s3/config"
	"github.com/conduitio/conduit-connector-s3/destination"
	"github.com/conduitio/conduit-connector-s3/destination/format"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/google/uuid"
	"github.com/matryer/is"
	"github.com/xitongsys/parquet-go-source/buffer"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/reader"
)

// objectCapture is a fake S3 endpoint that records PutObject requests and
// answers with status (200 when zero).
type objectCapture struct {
	mu     sync.Mutex
	status int
	keys   []string
	bodies [][]byte
	types  []string
}

func (p *objectCapture) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.status != 0 && p.status != http.StatusOK {
		w.WriteHeader(p.status)
		_, _ = w.Write([]byte(`<?xml version="1.0"?><Error><Code>AccessDenied</Code><Message>injected</Message></Error>`))
		return
	}
	if r.Method == http.MethodPut {
		p.keys = append(p.keys, r.URL.Path)
		p.bodies = append(p.bodies, body)
		p.types = append(p.types, r.Header.Get("Content-Type"))
	}
	w.Header().Set("ETag", `"e"`)
	w.WriteHeader(http.StatusOK)
}

func openAgainst(ctx context.Context, t *testing.T, endpoint string, cfg map[string]string) *destination.Destination {
	t.Helper()
	d := &destination.Destination{}
	full := map[string]string{
		config.ConfigKeyAWSAccessKeyID:     "a",
		config.ConfigKeyAWSSecretAccessKey: "b",
		config.ConfigKeyAWSRegion:          "us-east-1",
		config.ConfigKeyAWSBucket:          "bucket",
		config.ConfigKeyAWSEndpoint:        endpoint,
		config.ConfigKeyAWSPathStyle:       "true",
	}
	for k, v := range cfg {
		full[k] = v
	}
	if err := sdk.Util.ParseConfig(ctx, full, d.Config(), s3Conn.Connector.NewSpecification().DestinationParams); err != nil {
		t.Fatalf("parse config: %v", err)
	}
	if err := d.Open(ctx); err != nil {
		t.Fatalf("open: %v", err)
	}
	return d
}

func raw(pos, payload string) opencdc.Record {
	return opencdc.Record{
		Operation: opencdc.OperationCreate, Position: opencdc.Position(pos),
		Key: opencdc.RawData("k"), Payload: opencdc.Change{After: opencdc.RawData(payload)},
	}
}

// A record the format refuses must not be acked, and neither may anything
// after it; the records before it are stored in one object and reported as
// written (invariant 1, and invariant 3: nothing acked is missing).
func TestWrite_RefusedRecordReportsOnlyTheStoredPrefix(t *testing.T) {
	ctx := context.Background()
	tombstone := opencdc.Record{Operation: opencdc.OperationDelete, Position: opencdc.Position("p2"), Key: opencdc.RawData("k")}
	binary := raw("p2", string([]byte{0xff, 0x80}))

	for _, tc := range []struct {
		name    string
		cfg     map[string]string
		refused opencdc.Record
		code    string
		want    string
	}{
		{"bytearray tombstone", map[string]string{destination.ConfigKeyFormat: "bytearray"}, tombstone, format.CodeNullValue, "a\nb\n"},
		{"json binary", map[string]string{destination.ConfigKeyFormat: "json"}, binary, format.CodeInvalidUTF8, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			srv := &objectCapture{}
			ts := httptest.NewServer(srv)
			defer ts.Close()
			d := openAgainst(ctx, t, ts.URL, tc.cfg)

			n, err := d.Write(ctx, []opencdc.Record{raw("p0", "a"), raw("p1", "b"), tc.refused, raw("p3", "c")})
			is.Equal(n, 2) // the stored prefix, nothing of the refused record or after
			is.True(err != nil)
			is.True(strings.Contains(err.Error(), tc.code))
			is.Equal(len(srv.bodies), 1) // one object, the prefix
			if tc.want != "" {
				is.Equal(string(srv.bodies[0]), tc.want)
			} else {
				is.Equal(strings.Count(string(srv.bodies[0]), "\n"), 2)
				is.True(!strings.Contains(string(srv.bodies[0]), `"p2"`))
			}

			// refused first: nothing is written, nothing is acked
			n, err = d.Write(ctx, []opencdc.Record{tc.refused, raw("p3", "c")})
			is.Equal(n, 0)
			is.True(err != nil)
			is.Equal(len(srv.bodies), 1)
		})
	}
}

// A failed PutObject reports nothing as written, even when a record was also
// refused: the prefix was not stored.
func TestWrite_PutObjectFailureReportsNothing(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	srv := &objectCapture{status: http.StatusForbidden}
	ts := httptest.NewServer(srv)
	defer ts.Close()
	d := openAgainst(ctx, t, ts.URL, map[string]string{destination.ConfigKeyFormat: "bytearray"})
	tombstone := opencdc.Record{Operation: opencdc.OperationDelete, Position: opencdc.Position("p1"), Key: opencdc.RawData("k")}

	n, err := d.Write(ctx, []opencdc.Record{raw("p0", "a"), tombstone})
	is.Equal(n, 0)
	is.True(err != nil)
	is.True(!errors.Is(err, context.Canceled))
	is.True(!strings.Contains(err.Error(), format.CodeNullValue)) // the PutObject error, not the refusal
}

func TestWrite_KeySuffixAndContentType(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name       string
		cfg        map[string]string
		suffix, ct string
	}{
		{"json", map[string]string{"format": "json"}, ".json", "application/json"},
		{"json gzip", map[string]string{"format": "json", "compression": "gzip"}, ".json.gz", "application/gzip"},
		{"bytearray", map[string]string{"format": "bytearray"}, ".bin", "application/octet-stream"},
		{"bytearray json ext", map[string]string{"format": "bytearray", "format.bytearray.extension": "json"}, ".json", "application/octet-stream"},
		{"bytearray gzip", map[string]string{"format": "bytearray", "format.bytearray.extension": ".json", "compression": "gzip"}, ".json.gz", "application/gzip"},
		{"parquet snappy", map[string]string{"format": "parquet", "compression": "snappy"}, ".parquet", "application/octet-stream"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			srv := &objectCapture{}
			ts := httptest.NewServer(srv)
			defer ts.Close()
			d := openAgainst(ctx, t, ts.URL, tc.cfg)
			n, err := d.Write(ctx, []opencdc.Record{raw("p0", "a")})
			is.NoErr(err)
			is.Equal(n, 1)
			is.Equal(len(srv.keys), 1)
			is.True(strings.HasSuffix(srv.keys[0], tc.suffix))
			is.Equal(srv.types[0], tc.ct)
		})
	}
}

func TestDestinationConfig_RejectsInvalidFormatOptions(t *testing.T) {
	ctx := context.Background()
	for name, cfg := range map[string]map[string]string{
		"snappy with json": {"format": "json", "compression": "snappy"},
		"unknown codec":    {"format": "parquet", "compression": "zstd"},
		"bad separator":    {"format": "bytearray", "format.bytearray.separator": `\q`},
	} {
		t.Run(name, func(t *testing.T) {
			d := destination.NewDestination()
			full := map[string]string{
				config.ConfigKeyAWSAccessKeyID: "a", config.ConfigKeyAWSSecretAccessKey: "b",
				config.ConfigKeyAWSRegion: "us-east-1", config.ConfigKeyAWSBucket: "bucket",
			}
			for k, v := range cfg {
				full[k] = v
			}
			err := sdk.Util.ParseConfig(ctx, full, d.Config(), s3Conn.Connector.NewSpecification().DestinationParams)
			if err == nil {
				t.Fatal("invalid configuration was accepted")
			}
		})
	}
}

// TestS3MinIO_Formats writes every format and compression through the
// production destination to MinIO and reads the objects back.
func TestS3MinIO_Formats(t *testing.T) {
	ctx := context.Background()
	endpoint := os.Getenv(EnvMinIOEndpoint)
	if endpoint == "" {
		t.Skipf("%v env var must be set", EnvMinIOEndpoint)
	}
	requireNoSDKEndpointEnv(t)
	env := getEnv(EnvAWSAccessKeyID, EnvAWSSecretAccessKey, EnvAWSS3Bucket, EnvAWSRegion)
	failOnEmptyEnv(t, env)
	env[EnvMinIOEndpoint] = endpoint
	bucket := env[EnvAWSS3Bucket]
	client := newPathStyleS3Client(t, env)
	_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
	if err != nil && !strings.Contains(err.Error(), "BucketAlreadyOwnedByYou") && !strings.Contains(err.Error(), "BucketAlreadyExists") {
		t.Fatalf("create bucket: %v", err)
	}

	bin := string([]byte{0x00, 0xff, 0x80})
	structured := raw("p2", "")
	structured.Payload.After = opencdc.StructuredData{"id": 1}
	recs := []opencdc.Record{raw("p0", `{"x":1}`), raw("p1", bin), structured}

	for _, tc := range []struct {
		name   string
		cfg    map[string]string
		suffix string
		check  func(is *is.I, body []byte)
	}{
		{"bytearray", map[string]string{"format": "bytearray"}, ".bin", func(is *is.I, b []byte) {
			is.Equal(string(b), `{"x":1}`+"\n"+bin+"\n"+`{"id":1}`+"\n")
		}},
		{"bytearray gzip json", map[string]string{"format": "bytearray", "format.bytearray.extension": ".json", "compression": "gzip"}, ".json.gz", func(is *is.I, b []byte) {
			zr, err := gzip.NewReader(bytes.NewReader(b))
			is.NoErr(err)
			plain, err := io.ReadAll(zr)
			is.NoErr(err)
			is.Equal(string(plain), `{"x":1}`+"\n"+bin+"\n"+`{"id":1}`+"\n")
		}},
		{"json gzip", map[string]string{"format": "json", "compression": "gzip"}, ".json.gz", nil},
		{"parquet snappy", map[string]string{"format": "parquet", "compression": "snappy"}, ".parquet", func(is *is.I, b []byte) {
			pr, err := reader.NewParquetReader(buffer.NewBufferFileFromBytes(b), new(struct {
				Payload string `parquet:"name=payload, type=BYTE_ARRAY"`
			}), 1)
			is.NoErr(err)
			defer pr.ReadStop()
			is.Equal(pr.Footer.RowGroups[0].Columns[0].MetaData.Codec, parquet.CompressionCodec_SNAPPY)
			is.Equal(pr.GetNumRows(), int64(3))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			prefix := "fmt-" + uuid.NewString()
			cfg := map[string]string{
				config.ConfigKeyAWSAccessKeyID:       env[EnvAWSAccessKeyID],
				config.ConfigKeyAWSSecretAccessKey:   env[EnvAWSSecretAccessKey],
				config.ConfigKeyAWSRegion:            env[EnvAWSRegion],
				config.ConfigKeyAWSBucket:            bucket,
				config.ConfigKeyAWSEndpoint:          endpoint,
				config.ConfigKeyAWSPathStyle:         "true",
				config.ConfigKeyPrefix:               prefix,
				"sdk.schema.extract.key.enabled":     "false",
				"sdk.schema.extract.payload.enabled": "false",
			}
			for k, v := range tc.cfg {
				cfg[k] = v
			}
			w := tc.check
			batch := recs
			if tc.cfg["format"] == "json" {
				batch = []opencdc.Record{recs[0], recs[2]} // json refuses the binary record
			}
			d := destination.NewDestination()
			is.NoErr(sdk.Util.ParseConfig(ctx, cfg, d.Config(), s3Conn.Connector.NewSpecification().DestinationParams))
			is.NoErr(d.Open(ctx))
			n, err := d.Write(ctx, batch)
			is.NoErr(err)
			is.Equal(n, len(batch))
			is.NoErr(d.Teardown(ctx))

			list, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String(prefix + "/")})
			is.NoErr(err)
			is.Equal(len(list.Contents), 1)
			is.True(strings.HasSuffix(aws.ToString(list.Contents[0].Key), tc.suffix))
			obj, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: list.Contents[0].Key})
			is.NoErr(err)
			defer obj.Body.Close()
			body, err := io.ReadAll(obj.Body)
			is.NoErr(err)
			if w == nil {
				zr, err := gzip.NewReader(bytes.NewReader(body))
				is.NoErr(err)
				plain, err := io.ReadAll(zr)
				is.NoErr(err)
				is.Equal(strings.Count(string(plain), "\n"), 2)
				return
			}
			w(is, body)
		})
	}
}

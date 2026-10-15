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

package format

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"strings"

	"github.com/conduitio/conduit-commons/opencdc"
)

// Format defines the format the data will be persisted in by Destination
type Format string

const (
	// Parquet data format https://parquet.apache.org/
	Parquet Format = "parquet"

	// JSON format: one OpenCDC-shaped JSON object per line.
	JSON Format = "json"

	// ByteArray format: the payload bytes of each record followed by a
	// separator. Kafka Connect's ByteArrayFormat.
	ByteArray Format = "bytearray"
)

// All is a variable containing all supported format for enumeration
var All = []Format{
	Parquet,
	JSON,
	ByteArray,
}

// Compression is the compression applied to an object (json, bytearray) or
// the column codec (parquet).
type Compression string

const (
	// CompressionDefault selects the format's default: none for json and
	// bytearray, gzip for parquet.
	CompressionDefault Compression = ""
	CompressionNone    Compression = "none"
	CompressionGzip    Compression = "gzip"
	// CompressionSnappy is only valid for parquet.
	CompressionSnappy Compression = "snappy"
)

// Config keys of the settings validated here, for error messages.
const (
	ConfigKeyFormat             = "format"
	ConfigKeyBytearraySeparator = "format.bytearray.separator"
	ConfigKeyBytearrayExtension = "format.bytearray.extension"
	ConfigKeyCompression        = "compression"
)

// Stable error codes of records the encoders refuse. They start the error
// message, so they are machine-readable in logs and DLQ records.
const (
	// CodeNullValue: format bytearray got a record without a payload (a
	// delete or a Kafka tombstone).
	CodeNullValue = "s3.destination.null_value"
	// CodeInvalidUTF8: format json got bytes that are not valid UTF-8, which
	// a JSON string cannot hold without replacing them.
	CodeInvalidUTF8 = "s3.destination.invalid_utf8"
)

const (
	extBin     = "bin"
	mimeOctets = "application/octet-stream"
	mimeGzip   = "application/gzip"
	mimeJSON   = "application/json"
)

// Parse takes a string and returns a corresponding format or an error
func Parse(name string) (Format, error) {
	switch Format(name) {
	case Parquet, JSON, ByteArray:
		return Format(name), nil
	default:
		return "", fmt.Errorf("unsupported format: %q", name)
	}
}

// Ext returns a preferable file extension for the given format
func (f Format) Ext() string {
	switch f {
	case Parquet:
		return "parquet"
	case JSON:
		return "json"
	default:
		return extBin
	}
}

// MimeType returns MIME type (IANA media type or Content-Type) for the format
func (f Format) MimeType() string {
	switch f {
	case JSON:
		return mimeJSON
	default:
		return mimeOctets
	}
}

// MakeBytes returns a slice of bytes representing records in a given format
// with the format's default options.
func (f Format) MakeBytes(records []opencdc.Record) ([]byte, error) {
	return Encoder{Format: f}.MakeBytes(records)
}

// Encoder turns a batch of records into the bytes of one object. The zero
// value of every option is the format's default. Build it with NewEncoder,
// which validates the options.
type Encoder struct {
	Format Format
	// Separator is written after every record by the bytearray format, with
	// escapes already resolved.
	Separator string
	// Extension of bytearray objects, without the leading dot.
	Extension   string
	Compression Compression
}

// NewEncoder validates the format options and returns the encoder. separator
// and extension are the raw setting values: the separator may contain the
// escapes \n, \r, \t and \\, the extension may start with a dot. Errors name
// the config path and the fix.
func NewEncoder(f Format, separator, extension string, c Compression) (Encoder, error) {
	if _, err := Parse(string(f)); err != nil {
		return Encoder{}, fmt.Errorf("%s: %w (use json, parquet or bytearray)", ConfigKeyFormat, err)
	}
	e := Encoder{Format: f, Compression: c}
	switch c {
	case CompressionDefault, CompressionNone, CompressionGzip:
	case CompressionSnappy:
		if f != Parquet {
			return Encoder{}, fmt.Errorf("%s: %q is only supported with format parquet; use none or gzip with format %s",
				ConfigKeyCompression, c, f)
		}
	default:
		return Encoder{}, fmt.Errorf("%s: unsupported value %q (use none or gzip, or snappy with format parquet)", ConfigKeyCompression, c)
	}
	if f == ByteArray {
		sep, err := unescape(separator)
		if err != nil {
			return Encoder{}, fmt.Errorf("%s: %w", ConfigKeyBytearraySeparator, err)
		}
		e.Separator = sep
		e.Extension = strings.TrimPrefix(extension, ".")
		if e.Extension == "" {
			e.Extension = extBin
		}
		if strings.ContainsAny(e.Extension, "/\\") {
			return Encoder{}, fmt.Errorf("%s: %q must not contain a path separator (use for example .json or .bin)", ConfigKeyBytearrayExtension, extension)
		}
	}
	return e, nil
}

// unescape resolves \n, \r, \t and \\ in a separator setting. Any other
// backslash sequence is an error, so a typo does not become a literal.
func unescape(s string) (string, error) {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] != '\\' {
			b.WriteByte(s[i])
			continue
		}
		if i+1 == len(s) {
			return "", errors.New(`trailing backslash (escape it as \\)`)
		}
		i++
		switch s[i] {
		case 'n':
			b.WriteByte('\n')
		case 'r':
			b.WriteByte('\r')
		case 't':
			b.WriteByte('\t')
		case '\\':
			b.WriteByte('\\')
		default:
			return "", fmt.Errorf(`unsupported escape \%c (supported: \n, \r, \t, \\)`, s[i])
		}
	}
	return b.String(), nil
}

func (e Encoder) compression() Compression {
	if e.Compression != CompressionDefault {
		return e.Compression
	}
	if e.Format == Parquet {
		return CompressionGzip
	}
	return CompressionNone
}

// gzipObject reports whether the whole object is gzipped (json and bytearray;
// for parquet the compression is the column codec).
func (e Encoder) gzipObject() bool {
	return e.Format != Parquet && e.compression() == CompressionGzip
}

// Ext returns the file extension of the objects the encoder writes, without
// the leading dot: "json", "json.gz", "parquet", "bin", or the configured
// bytearray extension.
func (e Encoder) Ext() string {
	ext := e.Format.Ext()
	if e.Format == ByteArray && e.Extension != "" {
		ext = e.Extension
	}
	if e.gzipObject() {
		ext += ".gz"
	}
	return ext
}

// MimeType returns the Content-Type of the objects the encoder writes.
func (e Encoder) MimeType() string {
	if e.gzipObject() {
		return mimeGzip
	}
	return e.Format.MimeType()
}

// RecordError is a record the encoder refuses. Index is its position in the
// batch; the bytes returned with it hold exactly the records before Index.
type RecordError struct {
	Index    int
	Position opencdc.Position
	Err      error
}

func (e *RecordError) Error() string {
	return fmt.Sprintf("record %d (position %q): %v", e.Index, e.Position, e.Err)
}

func (e *RecordError) Unwrap() error { return e.Err }

// MakeBytes encodes records into the bytes of one object. If a record cannot
// be encoded, it returns the bytes of the records before it (nil if it is the
// first) and a *RecordError with its index: the caller can store that prefix
// and report exactly it as written. Records are checked before anything is
// encoded, so the prefix is a complete, valid object and never contains part
// of the refused record.
func (e Encoder) MakeBytes(records []opencdc.Record) ([]byte, error) {
	n := len(records)
	var refused error
	for i, r := range records {
		if err := e.Check(r); err != nil {
			n, refused = i, &RecordError{Index: i, Position: r.Position, Err: err}
			break
		}
	}
	if n == 0 {
		return nil, refused
	}
	b, err := e.encode(records[:n])
	if err != nil {
		// Not attributable to one record: nothing of the batch is stored.
		return nil, err
	}
	return b, refused
}

// Check refuses a record the format cannot store without changing it
// (invariant 6: never silently mangle data). MakeBytes runs it for every
// record; the partitioned write runs it in batch order before grouping (see
// Destination.writePartitioned).
func (e Encoder) Check(r opencdc.Record) error {
	switch e.Format {
	case ByteArray:
		// A Kafka tombstone (null value) reaches the destination as empty
		// raw data after the gRPC hop between Conduit and a plugin, not as a
		// nil payload, so both are refused. An empty Kafka value is
		// therefore indistinguishable from a tombstone and is refused too:
		// written as zero bytes plus a separator it would be acked and never
		// recoverable as a record.
		if r.Payload.After == nil || len(r.Payload.After.Bytes()) == 0 {
			return fmt.Errorf("%s: format bytearray stores the payload and this %s record has none (a delete, a Kafka tombstone or an empty value); "+
				"drop such records before the destination with a filter processor, or use format json or parquet, which store the operation",
				CodeNullValue, r.Operation)
		}
	case JSON:
		for name, b := range map[string][]byte{
			"key":      dataBytes(r.Key),
			"payload":  dataBytes(r.Payload.After),
			"position": r.Position,
		} {
			if !utf8Valid(b) {
				return fmt.Errorf("%s: the record %s is not valid UTF-8, which a JSON string cannot hold without replacing bytes; "+
					"use format bytearray or parquet for binary data", CodeInvalidUTF8, name)
			}
		}
		for k, v := range r.Metadata {
			if !utf8Valid([]byte(k)) || !utf8Valid([]byte(v)) {
				return fmt.Errorf("%s: metadata field %q is not valid UTF-8, which a JSON string cannot hold without replacing bytes; "+
					"remove or encode the field with a processor before the destination", CodeInvalidUTF8, k)
			}
		}
	case Parquet:
	}
	return nil
}

func (e Encoder) encode(records []opencdc.Record) ([]byte, error) {
	var b []byte
	var err error
	switch e.Format {
	case Parquet:
		b, err = makeParquetBytes(records, e.compression())
	case JSON:
		b, err = makeJSONBytes(records)
	case ByteArray:
		b = makeByteArrayBytes(records, e.Separator)
	default:
		return nil, fmt.Errorf("unsupported format: %s", e.Format)
	}
	if err != nil || !e.gzipObject() {
		return b, err
	}
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(b); err != nil {
		return nil, fmt.Errorf("gzip: %w", err)
	}
	if err := zw.Close(); err != nil {
		return nil, fmt.Errorf("gzip: %w", err)
	}
	return buf.Bytes(), nil
}

// dataString returns the bytes of d as a string, or "" when d is nil.
//
// Records legitimately carry nil data: a delete has no After image, and a
// keyless record has no Key. Calling Bytes on a nil opencdc.Data panics, which
// took the destination down on the first delete (#1030) before the batch was
// written, so nothing was acked and the pipeline could not make progress.
// Before images are not stored by either format (documented limitation).
func dataString(d opencdc.Data) string {
	return string(dataBytes(d))
}

func dataBytes(d opencdc.Data) []byte {
	if d == nil {
		return nil
	}
	return d.Bytes()
}

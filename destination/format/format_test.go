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

package format

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/matryer/is"
	"github.com/xitongsys/parquet-go-source/buffer"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/reader"
)

func rec(pos, key, payload string) opencdc.Record {
	return opencdc.Record{
		Operation: opencdc.OperationCreate,
		Position:  opencdc.Position(pos),
		Key:       opencdc.RawData(key),
		Payload:   opencdc.Change{After: opencdc.RawData(payload)},
	}
}

func gunzip(t *testing.T, b []byte) []byte {
	t.Helper()
	zr, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		t.Fatalf("not gzip: %v", err)
	}
	out, err := io.ReadAll(zr)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func mustEncoder(t *testing.T, f Format, sep, ext string, c Compression) Encoder {
	t.Helper()
	e, err := NewEncoder(f, sep, ext, c)
	if err != nil {
		t.Fatalf("NewEncoder(%s, %q, %q, %q): %v", f, sep, ext, c, err)
	}
	return e
}

// TestEncoder_JSONRefusesInvalidUTF8 is the regression test for silent data
// mangling in format json: encoding/json replaces bytes that are not valid
// UTF-8 with U+FFFD, so a binary payload was stored changed and acked
// (invariant 6). The record must be refused instead, with a stable code.
func TestEncoder_JSONRefusesInvalidUTF8(t *testing.T) {
	bin := string([]byte{0xff, 0xfe, 'x', 0x80})
	cases := map[string]opencdc.Record{
		"payload":  rec("p", "k", bin),
		"key":      rec("p", bin, "v"),
		"position": rec(bin, "k", "v"),
		"metadata": func() opencdc.Record { r := rec("p", "k", "v"); r.Metadata = opencdc.Metadata{"m": bin}; return r }(),
	}
	for name, r := range cases {
		t.Run(name, func(t *testing.T) {
			is := is.New(t)
			b, err := JSON.MakeBytes([]opencdc.Record{r})
			if err == nil {
				t.Fatalf("binary %s was accepted and stored as %q", name, b)
			}
			is.True(strings.HasPrefix(errors.Unwrap(err).Error(), CodeInvalidUTF8))
			if name == "metadata" {
				is.True(strings.Contains(err.Error(), "processor"))
			} else {
				is.True(strings.Contains(err.Error(), "bytearray"))
			}
			is.Equal(b, nil)
		})
	}
}

func TestEncoder_JSONStructuredAndUTF8StillWork(t *testing.T) {
	is := is.New(t)
	r := rec("p", "ключ", "héllo ✓")
	r2 := rec("q", "k", "")
	r2.Payload.After = opencdc.StructuredData{"a": 1}
	b, err := JSON.MakeBytes([]opencdc.Record{r, r2})
	is.NoErr(err)
	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	is.Equal(len(lines), 2)
	var got map[string]any
	is.NoErr(json.Unmarshal([]byte(lines[0]), &got))
	is.Equal(got["Payload"], "héllo ✓")
	is.Equal(got["Key"], "ключ")
	is.NoErr(json.Unmarshal([]byte(lines[1]), &got))
	is.Equal(got["Payload"], `{"a":1}`)
}

func TestEncoder_ByteArray(t *testing.T) {
	bin := string([]byte{0x00, 0xff, 0x80, '\n'})
	structured := rec("s", "k", "")
	structured.Payload.After = opencdc.StructuredData{"id": 1}
	recs := []opencdc.Record{rec("a", "k", `{"x":1}`), rec("b", "k", bin), structured}

	for _, tc := range []struct {
		name, sep, ext string
		c              Compression
		wantSep        string
		wantExt        string
		wantMime       string
	}{
		{"defaults", `\n`, ".bin", "", "\n", "bin", "application/octet-stream"},
		{"crlf", `\r\n`, "bin", CompressionNone, "\r\n", "bin", "application/octet-stream"},
		{"tab and json ext", `\t`, ".json", "", "\t", "json", "application/octet-stream"},
		{"literal and backslash", `|\\|`, "dat", "", `|\|`, "dat", "application/octet-stream"},
		{"empty separator", "", "", "", "", "bin", "application/octet-stream"},
		{"gzip", `\n`, ".json", CompressionGzip, "\n", "json.gz", "application/gzip"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			e := mustEncoder(t, ByteArray, tc.sep, tc.ext, tc.c)
			is.Equal(e.Ext(), tc.wantExt)
			is.Equal(e.MimeType(), tc.wantMime)
			b, err := e.MakeBytes(recs)
			is.NoErr(err)
			if tc.c == CompressionGzip {
				b = gunzip(t, b)
			}
			want := `{"x":1}` + tc.wantSep + bin + tc.wantSep + `{"id":1}` + tc.wantSep
			is.Equal(string(b), want) // payload bytes verbatim, binary included
		})
	}
}

func TestEncoder_ByteArrayNullValueStoresThePrefix(t *testing.T) {
	is := is.New(t)
	del := opencdc.Record{Operation: opencdc.OperationDelete, Position: opencdc.Position("p2"), Key: opencdc.RawData("k")}
	e := mustEncoder(t, ByteArray, `\n`, "", "")
	b, err := e.MakeBytes([]opencdc.Record{rec("p0", "k", "a"), rec("p1", "k", "b"), del, rec("p3", "k", "c")})
	var re *RecordError
	is.True(errors.As(err, &re))
	is.Equal(re.Index, 2)
	is.True(strings.HasPrefix(re.Err.Error(), CodeNullValue))
	is.True(strings.Contains(err.Error(), `"p2"`))   // names the position
	is.True(strings.Contains(err.Error(), "filter")) // suggests the fix
	is.Equal(string(b), "a\nb\n")                    // exactly the records before it

	b, err = e.MakeBytes([]opencdc.Record{del})
	is.True(errors.As(err, &re))
	is.Equal(re.Index, 0)
	is.Equal(b, nil)
}

func TestEncoder_JSONGzipRoundTrip(t *testing.T) {
	is := is.New(t)
	e := mustEncoder(t, JSON, "", "", CompressionGzip)
	is.Equal(e.Ext(), "json.gz")
	is.Equal(e.MimeType(), "application/gzip")
	b, err := e.MakeBytes([]opencdc.Record{rec("p", "k", "v")})
	is.NoErr(err)
	plain, err := JSON.MakeBytes([]opencdc.Record{rec("p", "k", "v")})
	is.NoErr(err)
	is.Equal(gunzip(t, b), plain)

	e = mustEncoder(t, JSON, "", "", "")
	is.Equal(e.Ext(), "json")
	is.Equal(e.MimeType(), "application/json")
}

func TestEncoder_ParquetCodecs(t *testing.T) {
	for _, tc := range []struct {
		c    Compression
		want parquet.CompressionCodec
	}{
		{"", parquet.CompressionCodec_GZIP},
		{CompressionGzip, parquet.CompressionCodec_GZIP},
		{CompressionSnappy, parquet.CompressionCodec_SNAPPY},
		{CompressionNone, parquet.CompressionCodec_UNCOMPRESSED},
	} {
		t.Run(string(tc.c), func(t *testing.T) {
			is := is.New(t)
			e := mustEncoder(t, Parquet, "", "", tc.c)
			is.Equal(e.Ext(), "parquet") // the codec is inside the file
			b, err := e.MakeBytes([]opencdc.Record{rec("p", "k", "v"), rec("q", "k", "w")})
			is.NoErr(err)
			pr, err := reader.NewParquetReader(buffer.NewBufferFileFromBytes(b), new(parquetRecord), 1)
			is.NoErr(err)
			defer pr.ReadStop()
			is.Equal(pr.Footer.RowGroups[0].Columns[0].MetaData.Codec, tc.want)
			rows := make([]parquetRecord, 2)
			is.NoErr(pr.Read(&rows))
			is.Equal(rows[1].Payload, "w")
		})
	}
}

func TestNewEncoder_Rejects(t *testing.T) {
	for _, tc := range []struct {
		name     string
		f        Format
		sep, ext string
		c        Compression
		path     string
	}{
		{"unknown format", "avro", "", "", "", ConfigKeyFormat},
		{"snappy with json", JSON, "", "", CompressionSnappy, ConfigKeyCompression},
		{"snappy with bytearray", ByteArray, `\n`, "", CompressionSnappy, ConfigKeyCompression},
		{"unknown compression", JSON, "", "", "zstd", ConfigKeyCompression},
		{"bad escape", ByteArray, `\x`, "", "", ConfigKeyBytearraySeparator},
		{"trailing backslash", ByteArray, `a\`, "", "", ConfigKeyBytearraySeparator},
		{"extension with slash", ByteArray, `\n`, "a/b", "", ConfigKeyBytearrayExtension},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewEncoder(tc.f, tc.sep, tc.ext, tc.c)
			if err == nil || !strings.HasPrefix(err.Error(), tc.path+":") {
				t.Fatalf("want an error naming %s, got %v", tc.path, err)
			}
		})
	}
}

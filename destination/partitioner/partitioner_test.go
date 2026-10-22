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

package partitioner

import (
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-s3/internal/logsafe"
)

func rec(meta map[string]string, after opencdc.Data) opencdc.Record {
	m := opencdc.Metadata{opencdc.MetadataCollection: "orders"}
	for k, v := range meta {
		if v == "<unset>" {
			delete(m, k)
			continue
		}
		m[k] = v
	}
	return opencdc.Record{Position: opencdc.Position("pos-1"), Metadata: m, Payload: opencdc.Change{After: after}}
}

func createdAt(t time.Time) string { return strconv.FormatInt(t.UnixNano(), 10) }

func mustNew(t *testing.T, cfg Config) Func {
	t.Helper()
	f, err := New(cfg)
	if err != nil {
		t.Fatalf("New(%+v): %v", cfg, err)
	}
	return f
}

func TestPartitioners(t *testing.T) {
	ts := time.Date(2026, 10, 14, 3, 34, 5, 0, time.UTC)
	now := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	tests := []struct {
		name string
		cfg  Config
		rec  opencdc.Record
		want string
	}{
		{
			name: "default",
			cfg:  Config{Kind: KindDefault},
			rec:  rec(map[string]string{MetadataKafkaPartition: "3"}, nil),
			want: "orders/partition=3",
		},
		{
			name: "field, raw JSON payload, nested and numeric",
			cfg:  Config{Kind: KindField, Fields: []string{"region", " address.country ", "n"}},
			rec:  rec(nil, opencdc.RawData(`{"region":"eu-west","address":{"country":"DE"},"n":12345678901234567890}`)),
			want: "orders/region=eu-west/address.country=DE/n=12345678901234567890",
		},
		{
			name: "field, structured payload, escaping",
			cfg:  Config{Kind: KindField, Fields: []string{"k"}},
			rec:  rec(nil, opencdc.StructuredData{"k": "a/b=c%d"}),
			want: "orders/k=a%2Fb%3Dc%25d",
		},
		{
			name: "hourly, record time",
			cfg:  Config{Kind: KindHourly},
			rec:  rec(map[string]string{opencdc.MetadataCreatedAt: createdAt(ts)}, nil),
			want: "orders/year=2026/month=10/day=14/hour=03",
		},
		{
			name: "daily",
			cfg:  Config{Kind: KindDaily},
			rec:  rec(map[string]string{opencdc.MetadataCreatedAt: createdAt(ts)}, nil),
			want: "orders/year=2026/month=10/day=14",
		},
		{
			name: "time, 30m buckets, minute in the pattern",
			cfg:  Config{Kind: KindTime, PathFormat: "YYYY-MM-dd/HH-mm", Duration: 30 * time.Minute},
			rec:  rec(map[string]string{opencdc.MetadataCreatedAt: createdAt(ts)}, nil),
			want: "orders/2026-10-14/03-30",
		},
		{
			name: "time, zone shifts the day",
			cfg:  Config{Kind: KindDaily, Timezone: "America/New_York"},
			rec:  rec(map[string]string{opencdc.MetadataCreatedAt: createdAt(time.Date(2026, 11, 1, 3, 30, 0, 0, time.UTC))}, nil),
			want: "orders/year=2026/month=10/day=31",
		},
		{
			name: "time, 1h buckets across the DST end (01:30 EST)",
			cfg:  Config{Kind: KindHourly, Timezone: "America/New_York"},
			rec:  rec(map[string]string{opencdc.MetadataCreatedAt: createdAt(time.Date(2026, 11, 1, 6, 30, 0, 0, time.UTC))}, nil),
			want: "orders/year=2026/month=11/day=01/hour=01",
		},
		{
			name: "time, 6h buckets in a +05:30 zone",
			cfg:  Config{Kind: KindTime, PathFormat: "'dt'=YYYYMMdd/'h'=HH", Duration: 6 * time.Hour, Timezone: "Asia/Kolkata"},
			rec:  rec(map[string]string{opencdc.MetadataCreatedAt: createdAt(ts)}, nil), // 09:04 IST
			want: "orders/dt=20261014/h=06",
		},
		{
			name: "time, quotes and quote escape",
			cfg:  Config{Kind: KindTime, PathFormat: "'it''s'=YYYY", Duration: time.Hour},
			rec:  rec(map[string]string{opencdc.MetadataCreatedAt: createdAt(ts)}, nil),
			want: "orders/it's=2026",
		},
		{
			name: "recordField, epoch millis in raw JSON",
			cfg:  Config{Kind: KindHourly, TimestampExtractor: ExtractorRecordField, TimestampField: "ts"},
			rec:  rec(nil, opencdc.RawData(`{"ts":`+strconv.FormatInt(ts.UnixMilli(), 10)+`}`)),
			want: "orders/year=2026/month=10/day=14/hour=03",
		},
		{
			name: "recordField, RFC 3339 string in structured data",
			cfg:  Config{Kind: KindHourly, TimestampExtractor: ExtractorRecordField, TimestampField: "meta.at"},
			rec:  rec(nil, opencdc.StructuredData{"meta": map[string]any{"at": "2026-10-14T05:00:00+02:00"}}),
			want: "orders/year=2026/month=10/day=14/hour=03",
		},
		{
			name: "wallclock",
			cfg:  Config{Kind: KindHourly, TimestampExtractor: ExtractorWallclock},
			rec:  rec(nil, nil),
			want: "orders/year=2030/month=01/day=02/hour=03",
		},
		{
			name: "a dot-dot collection cannot leave the prefix",
			cfg:  Config{Kind: KindDefault},
			rec:  rec(map[string]string{opencdc.MetadataCollection: "..", MetadataKafkaPartition: "0"}, nil),
			want: "%2E%2E/partition=0",
		},
		{
			name: "collection is escaped",
			cfg:  Config{Kind: KindDefault},
			rec:  rec(map[string]string{opencdc.MetadataCollection: "a/b", MetadataKafkaPartition: "0"}, nil),
			want: "a%2Fb/partition=0",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := mustNew(t, tc.cfg)(tc.rec, now)
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
}

// A record without the value its partitioner needs is an error, never an
// empty or zero partition (invariant 6).
func TestPartitioners_MissingValueIsAnError(t *testing.T) {
	ts := createdAt(time.Date(2026, 10, 14, 3, 0, 0, 0, time.UTC))
	tests := []struct {
		name string
		cfg  Config
		rec  opencdc.Record
		want string
	}{
		{"no collection", Config{Kind: KindDefault}, rec(map[string]string{opencdc.MetadataCollection: "<unset>", MetadataKafkaPartition: "1"}, nil), opencdc.MetadataCollection},
		{"empty collection", Config{Kind: KindDefault}, rec(map[string]string{opencdc.MetadataCollection: "", MetadataKafkaPartition: "1"}, nil), opencdc.MetadataCollection},
		{"no kafka.partition", Config{Kind: KindDefault}, rec(nil, nil), MetadataKafkaPartition},
		{"missing field", Config{Kind: KindField, Fields: []string{"x"}}, rec(nil, opencdc.RawData(`{"y":1}`)), "is missing"},
		{"null field", Config{Kind: KindField, Fields: []string{"x"}}, rec(nil, opencdc.RawData(`{"x":null}`)), "is null"},
		{"object field", Config{Kind: KindField, Fields: []string{"x"}}, rec(nil, opencdc.RawData(`{"x":{"a":1}}`)), "not a string"},
		{"raw non-JSON", Config{Kind: KindField, Fields: []string{"x"}}, rec(nil, opencdc.RawData(`not json`)), "neither structured"},
		{"delete (no After)", Config{Kind: KindField, Fields: []string{"x"}}, rec(nil, nil), "no payload"},
		{"no createdAt", Config{Kind: KindHourly}, rec(nil, nil), opencdc.MetadataCreatedAt},
		{"bad createdAt", Config{Kind: KindHourly}, rec(map[string]string{opencdc.MetadataCreatedAt: "yesterday"}, nil), opencdc.MetadataCreatedAt},
		{"field time not integer", Config{Kind: KindHourly, TimestampExtractor: ExtractorRecordField, TimestampField: "t"}, rec(nil, opencdc.RawData(`{"t":1.5}`)), "epoch milliseconds"},
		{"field time bad string", Config{Kind: KindHourly, TimestampExtractor: ExtractorRecordField, TimestampField: "t"}, rec(map[string]string{opencdc.MetadataCreatedAt: ts}, opencdc.RawData(`{"t":"2026-10-14"}`)), "RFC 3339"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := mustNew(t, tc.cfg)(tc.rec, time.Now())
			if err == nil {
				t.Fatalf("got partition %q, want an error", got)
			}
			if !errors.Is(err, ErrPartitionValue) {
				t.Fatalf("error does not carry the code: %v", err)
			}
			if !strings.Contains(err.Error(), tc.want) || !strings.Contains(err.Error(), logsafe.KeyHash([]byte("pos-1"))) {
				t.Fatalf("error %q should mention %q and the record position", err, tc.want)
			}
		})
	}
}

func TestNew_ConfigErrors(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
		want string
	}{
		{"unknown kind", Config{Kind: "weekly"}, "partitioner"},
		{"none with fields", Config{Kind: KindNone, Fields: []string{"a"}}, "partitioner.fields"},
		{"field without fields", Config{Kind: KindField, Fields: []string{" ", ""}}, "partitioner.fields"},
		{"time without format", Config{Kind: KindTime, Duration: time.Hour}, "partitioner.pathFormat"},
		{"time without duration", Config{Kind: KindTime, PathFormat: "YYYY"}, "partitioner.duration"},
		{"time with sub-second duration", Config{Kind: KindTime, PathFormat: "YYYY", Duration: time.Millisecond}, "partitioner.duration"},
		{"hourly with a format", Config{Kind: KindHourly, PathFormat: "YYYY"}, "partitioner.pathFormat"},
		{"text month", Config{Kind: KindTime, PathFormat: "MMM", Duration: time.Hour}, "MMM"},
		{"two-digit year", Config{Kind: KindTime, PathFormat: "yy", Duration: time.Hour}, "4-letter year"},
		{"unknown letter", Config{Kind: KindTime, PathFormat: "'w'=ww", Duration: time.Hour}, "unsupported pattern letter"},
		{"unterminated quote", Config{Kind: KindTime, PathFormat: "'year=YYYY", Duration: time.Hour}, "unterminated quote"},
		{"leading slash", Config{Kind: KindTime, PathFormat: "/YYYY", Duration: time.Hour}, "directory level"},
		{"trailing slash", Config{Kind: KindTime, PathFormat: "YYYY/", Duration: time.Hour}, "directory level"},
		{"double slash", Config{Kind: KindTime, PathFormat: "YYYY//MM", Duration: time.Hour}, "directory level"},
		{"dot-dot segment", Config{Kind: KindTime, PathFormat: "YYYY/../MM", Duration: time.Hour}, "directory level"},
		{"quoted dot-dot segment", Config{Kind: KindTime, PathFormat: "YYYY/'..'/MM", Duration: time.Hour}, "directory level"},
		{"bad zone", Config{Kind: KindHourly, Timezone: "Mars/Olympus"}, "partitioner.timezone"},
		{"bad extractor", Config{Kind: KindHourly, TimestampExtractor: "Record"}, "partitioner.timestampExtractor"},
		{"recordField without field", Config{Kind: KindHourly, TimestampExtractor: ExtractorRecordField}, "partitioner.timestampField"},
		{"field set for record extractor", Config{Kind: KindHourly, TimestampField: "t"}, "partitioner.timestampField"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := New(tc.cfg)
			if err == nil {
				t.Fatal("want an error")
			}
			if !errors.Is(err, ErrConfig) || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q should carry ErrConfig and mention %q", err, tc.want)
			}
		})
	}
}

func TestNew_NoneIsNil(t *testing.T) {
	for _, k := range []string{"", KindNone} {
		f, err := New(Config{Kind: k, Timezone: "UTC", TimestampExtractor: ExtractorRecord})
		if err != nil || f != nil {
			t.Fatalf("kind %q: got %v, %v; want nil, nil", k, f != nil, err)
		}
	}
}

func TestEscape(t *testing.T) {
	cases := map[string]string{
		"plain-value_1.2": "plain-value_1.2",
		"a/b":             "a%2Fb",
		"k=v":             "k%3Dv",
		"100%":            "100%25",
		"c:\\x":           "c%3A%5Cx",
		"tab\there":       "tab%09here",
		"ünï":             "ünï", // non-ASCII is left alone, as Hive does
		".":               "%2E",
		"..":              "%2E%2E",
		"...":             "...",
	}
	for in, want := range cases {
		if got := Escape(in); got != want {
			t.Errorf("Escape(%q) = %q, want %q", in, got, want)
		}
	}
}

func FuzzParsePathFormat(f *testing.F) {
	for _, s := range []string{HourlyPathFormat, DailyPathFormat, "YYYY-MM-dd", "'a''b'", "'x", "MMM", ""} {
		f.Add(s)
	}
	ts := time.Date(2026, 10, 14, 3, 4, 5, 0, time.UTC)
	f.Fuzz(func(t *testing.T, s string) {
		pf, err := ParsePathFormat(s)
		if err != nil {
			if !errors.Is(err, ErrConfig) {
				t.Fatalf("error without code: %v", err)
			}
			return
		}
		_ = pf.Format(ts) // must not panic
	})
}

// The documented number encoding of field partitions (it differs from Kafka
// Connect, which writes the Java string of the converted value): raw JSON
// keeps the source text, structured data uses the shortest form.
func TestFieldPartition_NumberEncoding(t *testing.T) {
	f := mustNew(t, Config{Kind: KindField, Fields: []string{"n"}})
	for _, tc := range []struct {
		name  string
		after opencdc.Data
		want  string
	}{
		{"raw 5", opencdc.RawData(`{"n":5}`), "orders/n=5"},
		{"raw 5.0", opencdc.RawData(`{"n":5.0}`), "orders/n=5.0"},
		{"raw 1e3", opencdc.RawData(`{"n":1e3}`), "orders/n=1e3"},
		{"structured float 5.0", opencdc.StructuredData{"n": 5.0}, "orders/n=5"},
		{"structured int 5", opencdc.StructuredData{"n": 5}, "orders/n=5"},
		{"structured float 2.5", opencdc.StructuredData{"n": 2.5}, "orders/n=2.5"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := f(rec(nil, tc.after), time.Now())
			if err != nil || got != tc.want {
				t.Fatalf("got %q, %v; want %q", got, err, tc.want)
			}
		})
	}
}

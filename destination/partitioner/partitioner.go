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

// Package partitioner computes the directory an S3 destination record is
// written under, reproducing the directory layout of the Kafka Connect S3
// sink's partitioners (DefaultPartitioner, FieldPartitioner,
// TimeBasedPartitioner, HourlyPartitioner, DailyPartitioner). The design is
// ConduitIO/conduit docs/design-documents/20261014-s3-sink-parity.md.
//
// A partitioner never invents a value: a record without the value its
// partitioner needs is an error (ErrPartitionValue), never an empty or zero
// partition (data integrity invariant 6). Partition values are escaped the
// way Hive escapes path names, so a value is always exactly one path segment.
package partitioner

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-s3/internal/logsafe"
)

// Kinds of partitioner, the values of the destination setting `partitioner`.
const (
	KindNone    = "none"
	KindDefault = "default"
	KindField   = "field"
	KindTime    = "time"
	KindHourly  = "hourly"
	KindDaily   = "daily"
)

// Timestamp extractors, the values of `partitioner.timestampExtractor`.
const (
	ExtractorRecord      = "record"
	ExtractorRecordField = "recordField"
	ExtractorWallclock   = "wallclock"
)

// Configuration keys, as named in errors.
const (
	keyKind       = "partitioner"
	keyFields     = "partitioner.fields"
	keyPathFormat = "partitioner.pathFormat"
	keyDuration   = "partitioner.duration"
	keyTimezone   = "partitioner.timezone"
	keyExtractor  = "partitioner.timestampExtractor"
	keyTSField    = "partitioner.timestampField"
)

// MetadataKafkaPartition is the record metadata key the default partitioner
// reads the Kafka partition number from.
const MetadataKafkaPartition = "kafka.partition"

// Presets of the hourly and daily partitioners, as in Kafka Connect.
const (
	HourlyPathFormat = "'year'=YYYY/'month'=MM/'day'=dd/'hour'=HH"
	DailyPathFormat  = "'year'=YYYY/'month'=MM/'day'=dd"
)

// ErrPartitionValue is the stable error code of a record that lacks the value
// its partitioner needs. The record is not written (a DLQ, if configured,
// receives it).
var ErrPartitionValue = errors.New("s3.destination.partition_value")

// ErrConfig is the stable error code of an invalid partitioner configuration.
var ErrConfig = errors.New("s3.destination.partitioner_config")

// Config is the partitioner part of the destination configuration.
type Config struct {
	Kind               string
	Fields             []string
	PathFormat         string
	Duration           time.Duration
	Timezone           string
	TimestampExtractor string
	TimestampField     string
}

// Func returns the directory (relative, without leading or trailing slash)
// a record is written under. It is safe for concurrent use.
type Func func(r opencdc.Record, now time.Time) (string, error)

// New validates cfg and returns the partitioner, or nil for KindNone (the
// layout `<prefix>/<UnixNano>.<ext>` of earlier versions). Errors wrap
// ErrConfig and name the setting and the fix.
func New(cfg Config) (Func, error) {
	switch cfg.Kind {
	case "", KindNone:
		if err := unused(cfg, keyFields, keyPathFormat, keyDuration, keyTSField); err != nil {
			return nil, err
		}
		return nil, nil
	case KindDefault:
		if err := unused(cfg, keyFields, keyPathFormat, keyDuration, keyTSField); err != nil {
			return nil, err
		}
		return withCollection(defaultPartition), nil
	case KindField:
		if err := unused(cfg, keyPathFormat, keyDuration, keyTSField); err != nil {
			return nil, err
		}
		fields, err := cleanFields(cfg.Fields)
		if err != nil {
			return nil, err
		}
		return withCollection(fieldPartition(fields)), nil
	case KindHourly, KindDaily:
		if cfg.PathFormat != "" || cfg.Duration != 0 {
			return nil, configErr(keyPathFormat, "partitioner %q fixes partitioner.pathFormat and partitioner.duration; remove them, or use partitioner \"time\" to set your own", cfg.Kind)
		}
		cfg.PathFormat, cfg.Duration = HourlyPathFormat, time.Hour
		if cfg.Kind == KindDaily {
			cfg.PathFormat, cfg.Duration = DailyPathFormat, 24*time.Hour
		}
		return newTime(cfg)
	case KindTime:
		if cfg.PathFormat == "" {
			return nil, configErr(keyPathFormat, "partitioner \"time\" needs partitioner.pathFormat, for example %q", HourlyPathFormat)
		}
		if cfg.Duration < time.Second {
			return nil, configErr(keyDuration, "partitioner \"time\" needs partitioner.duration of at least 1s, for example 1h (Kafka Connect: partition.duration.ms)")
		}
		return newTime(cfg)
	default:
		return nil, configErr(keyKind, "unknown partitioner %q; use one of none, default, field, time, hourly, daily", cfg.Kind)
	}
}

func unused(cfg Config, keys ...string) error {
	for _, k := range keys {
		set := false
		switch k {
		case keyFields:
			set = len(cfg.Fields) > 0
		case keyPathFormat:
			set = cfg.PathFormat != ""
		case keyDuration:
			set = cfg.Duration != 0
		case keyTSField:
			set = cfg.TimestampField != ""
		}
		if set {
			return configErr(k, "%s has no effect with partitioner %q; remove it or choose a partitioner that uses it", k, kindOrNone(cfg.Kind))
		}
	}
	return nil
}

func kindOrNone(k string) string {
	if k == "" {
		return KindNone
	}
	return k
}

func configErr(path, format string, args ...any) error {
	return fmt.Errorf("%w: %s: %s", ErrConfig, path, fmt.Sprintf(format, args...))
}

func valueErr(r opencdc.Record, path, format string, args ...any) error {
	return fmt.Errorf("%w: record at position hash %s: %s (config path: %s)", ErrPartitionValue, logsafe.KeyHash([]byte(r.Position)), fmt.Sprintf(format, args...), path)
}

func cleanFields(in []string) ([]string, error) {
	var out []string
	for _, f := range in {
		if f = strings.TrimSpace(f); f != "" {
			out = append(out, f)
		}
	}
	if len(out) == 0 {
		return nil, configErr(keyFields, "partitioner \"field\" needs at least one field in partitioner.fields, for example \"region,country\" (Kafka Connect: partition.field.name)")
	}
	return out, nil
}

// withCollection prefixes the encoded partition with the record's collection,
// which stands in for the Kafka topic: Kafka Connect writes
// <topics.dir>/<topic>/<encoded partition>/.
func withCollection(encode func(opencdc.Record, time.Time) (string, error)) Func {
	return func(r opencdc.Record, now time.Time) (string, error) {
		coll, err := r.Metadata.GetCollection()
		if err != nil || coll == "" {
			return "", valueErr(r, keyKind, "the record has no %s metadata, which names the directory under the prefix (the Kafka topic for builtin:kafka); set it with a processor or use partitioner \"none\"", opencdc.MetadataCollection)
		}
		enc, err := encode(r, now)
		if err != nil {
			return "", err
		}
		return Escape(coll) + "/" + enc, nil
	}
}

func defaultPartition(r opencdc.Record, _ time.Time) (string, error) {
	p, ok := r.Metadata[MetadataKafkaPartition]
	if !ok || p == "" {
		return "", valueErr(r, keyKind, "the record has no %s metadata, which partitioner \"default\" needs (the Kafka partition number)", MetadataKafkaPartition)
	}
	return "partition=" + Escape(p), nil
}

func fieldPartition(fields []string) func(opencdc.Record, time.Time) (string, error) {
	return func(r opencdc.Record, _ time.Time) (string, error) {
		m, err := payloadMap(r)
		if err != nil {
			return "", valueErr(r, keyFields, "%v", err)
		}
		parts := make([]string, 0, len(fields))
		for _, f := range fields {
			v, err := lookup(m, f)
			if err != nil {
				return "", valueErr(r, keyFields, "field %q: %v", f, err)
			}
			s, err := scalarString(v)
			if err != nil {
				return "", valueErr(r, keyFields, "field %q: %v", f, err)
			}
			parts = append(parts, Escape(f)+"="+Escape(s))
		}
		return strings.Join(parts, "/"), nil
	}
}

func newTime(cfg Config) (Func, error) {
	format, err := ParsePathFormat(cfg.PathFormat)
	if err != nil {
		return nil, err
	}
	tzName := cfg.Timezone
	if tzName == "" {
		tzName = "UTC"
	}
	loc, err := time.LoadLocation(tzName)
	if err != nil {
		return nil, configErr(keyTimezone, "unknown time zone %q (use an IANA name such as UTC or Europe/Berlin): %v", tzName, err)
	}
	extract, err := newExtractor(cfg)
	if err != nil {
		return nil, err
	}
	d := cfg.Duration
	return withCollection(func(r opencdc.Record, now time.Time) (string, error) {
		ts, err := extract(r, now)
		if err != nil {
			return "", err
		}
		return format.Format(bucket(ts, d, loc)), nil
	}), nil
}

// bucket floors t to a multiple of d in loc's wall clock, as Kafka Connect's
// TimeBasedPartitioner does: the zone offset is added before flooring and
// removed after, and the result is formatted in loc.
func bucket(t time.Time, d time.Duration, loc *time.Location) time.Time {
	_, off := t.In(loc).Zone()
	local := t.UnixNano() + int64(off)*int64(time.Second)
	floored := local - mod(local, int64(d))
	// floored is a wall-clock time in loc; turn it back into an instant in
	// loc so that formatting shows exactly these wall-clock fields.
	w := time.Unix(0, floored).UTC()
	return time.Date(w.Year(), w.Month(), w.Day(), w.Hour(), w.Minute(), w.Second(), w.Nanosecond(), loc)
}

func mod(a, b int64) int64 {
	m := a % b
	if m < 0 {
		m += b
	}
	return m
}

func newExtractor(cfg Config) (func(opencdc.Record, time.Time) (time.Time, error), error) {
	switch cfg.TimestampExtractor {
	case "", ExtractorRecord:
		if cfg.TimestampField != "" {
			return nil, configErr(keyTSField, "partitioner.timestampField needs partitioner.timestampExtractor \"recordField\"")
		}
		return func(r opencdc.Record, _ time.Time) (time.Time, error) {
			if _, ok := r.Metadata[opencdc.MetadataCreatedAt]; !ok {
				return time.Time{}, valueErr(r, keyExtractor, "the record has no %s metadata (builtin:kafka sets it to the Kafka record timestamp); use timestampExtractor \"recordField\" or \"wallclock\"", opencdc.MetadataCreatedAt)
			}
			t, err := r.Metadata.GetCreatedAt()
			if err != nil {
				return time.Time{}, valueErr(r, keyExtractor, "%s metadata is not a valid timestamp (%d bytes)", opencdc.MetadataCreatedAt, len(r.Metadata[opencdc.MetadataCreatedAt]))
			}
			return t, nil
		}, nil
	case ExtractorWallclock:
		if cfg.TimestampField != "" {
			return nil, configErr(keyTSField, "partitioner.timestampField needs partitioner.timestampExtractor \"recordField\"")
		}
		return func(_ opencdc.Record, now time.Time) (time.Time, error) { return now, nil }, nil
	case ExtractorRecordField:
		f := strings.TrimSpace(cfg.TimestampField)
		if f == "" {
			return nil, configErr(keyTSField, "timestampExtractor \"recordField\" needs partitioner.timestampField, the payload field that holds the time (Kafka Connect: timestamp.field)")
		}
		return func(r opencdc.Record, _ time.Time) (time.Time, error) {
			m, err := payloadMap(r)
			if err != nil {
				return time.Time{}, valueErr(r, keyTSField, "%v", err)
			}
			v, err := lookup(m, f)
			if err != nil {
				return time.Time{}, valueErr(r, keyTSField, "field %q: %v", f, err)
			}
			t, err := toTime(v)
			if err != nil {
				return time.Time{}, valueErr(r, keyTSField, "field %q: %v", f, err)
			}
			return t, nil
		}, nil
	default:
		return nil, configErr(keyExtractor, "unknown timestamp extractor %q; use record, recordField or wallclock", cfg.TimestampExtractor)
	}
}

// payloadMap returns the payload's After as a map: structured data directly,
// raw data parsed as a JSON object (builtin:kafka emits raw bytes).
func payloadMap(r opencdc.Record) (map[string]any, error) {
	switch d := r.Payload.After.(type) {
	case opencdc.StructuredData:
		return d, nil
	case opencdc.RawData:
		dec := json.NewDecoder(bytes.NewReader(d))
		dec.UseNumber()
		var m map[string]any
		if err := dec.Decode(&m); err != nil || m == nil {
			return nil, errors.New("the payload is neither structured data nor a JSON object")
		}
		return m, nil
	case nil:
		return nil, errors.New("the record has no payload (a delete?)")
	default:
		return nil, fmt.Errorf("unsupported payload type %T", d)
	}
}

// lookup walks a dot-separated path.
func lookup(m map[string]any, path string) (any, error) {
	var cur any = m
	for _, part := range strings.Split(path, ".") {
		mm, ok := asMap(cur)
		if !ok {
			return nil, errors.New("is not inside an object")
		}
		v, ok := mm[part]
		if !ok {
			return nil, errors.New("is missing")
		}
		cur = v
	}
	if cur == nil {
		return nil, errors.New("is null")
	}
	return cur, nil
}

func asMap(v any) (map[string]any, bool) {
	switch m := v.(type) {
	case map[string]any:
		return m, true
	case opencdc.StructuredData:
		return m, true
	}
	return nil, false
}

func scalarString(v any) (string, error) {
	switch x := v.(type) {
	case string:
		return x, nil
	case json.Number:
		return x.String(), nil
	case bool:
		return strconv.FormatBool(x), nil
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return fmt.Sprint(x), nil
	case float32:
		return strconv.FormatFloat(float64(x), 'f', -1, 32), nil
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64), nil
	case time.Time:
		return x.UTC().Format(time.RFC3339Nano), nil
	}
	return "", fmt.Errorf("is a %T, not a string, number or boolean", v)
}

// toTime reads epoch milliseconds (as Kafka Connect's RecordField extractor),
// an RFC 3339 string or a time value.
func toTime(v any) (time.Time, error) {
	switch x := v.(type) {
	case time.Time:
		return x, nil
	case string:
		t, err := time.Parse(time.RFC3339Nano, x)
		if err != nil {
			return time.Time{}, fmt.Errorf("a %d byte string is not an RFC 3339 time", len(x))
		}
		return t, nil
	case json.Number:
		ms, err := x.Int64()
		if err != nil {
			return time.Time{}, fmt.Errorf("%s is not an integer number of epoch milliseconds", x)
		}
		return time.UnixMilli(ms), nil
	case int:
		return time.UnixMilli(int64(x)), nil
	case int32:
		return time.UnixMilli(int64(x)), nil
	case int64:
		return time.UnixMilli(x), nil
	case uint32:
		return time.UnixMilli(int64(x)), nil
	case float64:
		if x != float64(int64(x)) {
			return time.Time{}, fmt.Errorf("a %T value is not an integer number of epoch milliseconds", x)
		}
		return time.UnixMilli(int64(x)), nil
	}
	return time.Time{}, fmt.Errorf("is a %T, not epoch milliseconds, an RFC 3339 string or a time", v)
}

// Escape percent-encodes the characters Hive escapes in partition path names
// (plus `}`), so that a value is exactly one path segment and a Hive-style
// reader decodes it back.
//
// A segment that is exactly "." or ".." is escaped as well (%2E): the writer
// joins the key with path.Join, which would otherwise resolve it and move the
// object out of its directory, or out of the prefix.
func Escape(s string) string {
	if s == "." || s == ".." {
		return strings.Repeat("%2E", len(s))
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if needsEscape(c) {
			fmt.Fprintf(&b, "%%%02X", c)
			continue
		}
		b.WriteByte(c)
	}
	return b.String()
}

func needsEscape(c byte) bool {
	if c < 0x20 || c == 0x7F {
		return true
	}
	return strings.IndexByte("\"#%'*/:=?\\[]^{}", c) >= 0
}

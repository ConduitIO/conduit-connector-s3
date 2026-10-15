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

package destination

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-s3/destination/format"
	"github.com/conduitio/conduit-connector-s3/destination/partitioner"
	"github.com/conduitio/conduit-connector-s3/destination/writer"
	"github.com/matryer/is"
)

// fakeWriter stores batches and fails the write to the directories in fail.
type fakeWriter struct {
	fail    map[string]bool
	written []*writer.Batch
}

func (w *fakeWriter) Write(_ context.Context, b *writer.Batch) error {
	if w.fail[b.Dir] {
		return errors.New("injected PutObject failure")
	}
	w.written = append(w.written, b)
	return nil
}

func (w *fakeWriter) LastPosition() opencdc.Position { return nil }

func partitionedDest(t *testing.T, w writer.Writer) *Destination {
	t.Helper()
	f, err := partitioner.New(partitioner.Config{Kind: partitioner.KindField, Fields: []string{"region"}})
	if err != nil {
		t.Fatal(err)
	}
	return &Destination{
		config:    Config{Format: format.JSON},
		Writer:    w,
		partition: f,
		now:       time.Now,
	}
}

// recs builds records whose region is given by regions; "" means a record the
// partitioner rejects (no region field).
func recs(regions ...string) []opencdc.Record {
	out := make([]opencdc.Record, len(regions))
	for i, r := range regions {
		payload := fmt.Sprintf(`{"region":%q,"i":%d}`, r, i)
		if r == "" {
			payload = fmt.Sprintf(`{"i":%d}`, i)
		}
		out[i] = opencdc.Record{
			Position:  opencdc.Position(fmt.Sprintf("p%d", i)),
			Operation: opencdc.OperationCreate,
			Metadata:  opencdc.Metadata{opencdc.MetadataCollection: "orders"},
			Payload:   opencdc.Change{After: opencdc.RawData(payload)},
		}
	}
	return out
}

func positions(rs []opencdc.Record) []string {
	var out []string
	for _, r := range rs {
		out = append(out, string(r.Position))
	}
	return out
}

func TestWritePartitioned_GroupsInFirstAppearanceOrder(t *testing.T) {
	is := is.New(t)
	w := &fakeWriter{}
	d := partitionedDest(t, w)

	n, err := d.Write(context.Background(), recs("eu", "us", "eu", "ap", "us"))
	is.NoErr(err)
	is.Equal(n, 5)
	is.Equal(len(w.written), 3)
	is.Equal(w.written[0].Dir, "orders/region=eu")
	is.Equal(positions(w.written[0].Records), []string{"p0", "p2"})
	is.Equal(w.written[1].Dir, "orders/region=us")
	is.Equal(positions(w.written[1].Records), []string{"p1", "p4"})
	is.Equal(w.written[2].Dir, "orders/region=ap")
	is.Equal(positions(w.written[2].Records), []string{"p3"})
}

// The count Write returns is what the SDK acks. It must never include a
// record that is not stored (invariant 1), and should include every record
// before the first one that is not.
func TestWritePartitioned_AckCountIsTheStoredPrefix(t *testing.T) {
	tests := []struct {
		name    string
		regions []string
		fail    []string
		wantN   int
		stored  []string // dirs written
		wantErr string
	}{
		{"first group fails", []string{"eu", "us", "eu"}, []string{"orders/region=eu"}, 0, nil, "injected"},
		// us fails: its first record is p1, so only p0 is acked although p2 (eu) is stored too
		{"second group fails", []string{"eu", "us", "eu", "ap"}, []string{"orders/region=us"}, 1, []string{"orders/region=eu"}, "injected"},
		{"last group fails", []string{"eu", "us", "eu", "ap"}, []string{"orders/region=ap"}, 3, []string{"orders/region=eu", "orders/region=us"}, "injected"},
		{"partition error mid-batch", []string{"eu", "us", "", "eu"}, nil, 2, []string{"orders/region=eu", "orders/region=us"}, "s3.destination.partition_value"},
		{"partition error first", []string{"", "eu"}, nil, 0, nil, "s3.destination.partition_value"},
		{"group fails before the partition error", []string{"eu", "us", ""}, []string{"orders/region=us"}, 1, []string{"orders/region=eu"}, "injected"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			w := &fakeWriter{fail: map[string]bool{}}
			for _, f := range tc.fail {
				w.fail[f] = true
			}
			d := partitionedDest(t, w)
			batch := recs(tc.regions...)
			n, err := d.Write(context.Background(), batch)
			is.True(err != nil)
			is.True(strings.Contains(err.Error(), tc.wantErr))
			is.Equal(n, tc.wantN)

			var dirs []string
			stored := map[string]bool{}
			for _, b := range w.written {
				dirs = append(dirs, b.Dir)
				for _, r := range b.Records {
					stored[string(r.Position)] = true
				}
			}
			is.Equal(dirs, tc.stored)
			for _, r := range batch[:n] {
				if !stored[string(r.Position)] {
					t.Fatalf("record %s is acked but not stored", r.Position)
				}
			}
			// the record at n is never stored in this call's failing group
			if n < len(batch) && tc.wantErr == "s3.destination.partition_value" && stored[string(batch[n].Position)] {
				t.Fatalf("record %s failed partitioning but was stored", batch[n].Position)
			}
		})
	}
}

func TestWrite_NoPartitionerKeepsOneObjectPerBatch(t *testing.T) {
	is := is.New(t)
	w := &fakeWriter{}
	d := &Destination{config: Config{Format: format.JSON}, Writer: w}
	n, err := d.Write(context.Background(), recs("eu", "us"))
	is.NoErr(err)
	is.Equal(n, 2)
	is.Equal(len(w.written), 1)
	is.Equal(w.written[0].Dir, "")
}

// encodingWriter stores what the real encoder would: the bytes of the group's
// records before a refused one (Batch.Bytes), and returns the refusal.
type encodingWriter struct {
	stored map[string]bool // positions stored
	dirs   []string
}

func (w *encodingWriter) Write(_ context.Context, b *writer.Batch) error {
	_, err := b.Bytes()
	for _, r := range b.Records[:b.Written(err)] {
		w.stored[string(r.Position)] = true
	}
	w.dirs = append(w.dirs, b.Dir)
	return err
}

func (w *encodingWriter) LastPosition() opencdc.Position { return nil }

// A record the format refuses, in a group whose records interleave with
// another group's, must stop the batch at its own batch index. Here the eu
// group is [p0, p3, p4] and p4 is refused: a writer-side refusal carries the
// index into the group (2), and acking 2 records of the batch would ack p1,
// which is in the us group, never written because the eu write failed first.
// The check runs in batch order before grouping instead.
// Regression test for the review of #1053 rebased onto #1054.
func TestWritePartitioned_FormatRefusalStopsAtItsBatchIndex(t *testing.T) {
	is := is.New(t)
	enc, err := format.NewEncoder(format.ByteArray, `\n`, "", "")
	is.NoErr(err)
	w := &encodingWriter{stored: map[string]bool{}}
	// The default partitioner reads metadata only, so the tombstone below is
	// partitioned like any record and only the format refuses it.
	f, err := partitioner.New(partitioner.Config{Kind: partitioner.KindDefault})
	is.NoErr(err)
	d := &Destination{config: Config{Format: format.ByteArray}, encoder: enc, Writer: w, partition: f, now: time.Now}

	batch := recs("eu", "us", "us", "eu", "eu")
	for _, r := range batch {
		var reg map[string]any
		is.NoErr(json.Unmarshal(r.Payload.After.Bytes(), &reg))
		r.Metadata[partitioner.MetadataKafkaPartition] = fmt.Sprint(reg["region"])
	}
	batch[4].Payload.After = opencdc.RawData{} // a Kafka tombstone after the gRPC hop, in partition eu
	n, err := d.Write(context.Background(), batch)
	is.True(err != nil)
	is.True(strings.Contains(err.Error(), format.CodeNullValue))
	for _, r := range batch[:n] {
		is.True(w.stored[string(r.Position)]) // nothing acked that is not stored
	}
	is.Equal(n, 4)           // p0 to p3 are stored; p4 is refused
	is.True(!w.stored["p4"]) // the refused record is not written
}

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

// Package chaos holds the SIGKILL chaos tests of the S3 connector (ConduitIO/
// conduit#3009, certification). They run a real Conduit process (a pinned
// release binary, CONDUIT_BIN) with this connector built from the working tree
// as a standalone plugin, against MinIO, kill the whole process group with
// SIGKILL at several points, restart it, and check the data integrity
// invariants on what is actually in the bucket:
//
//   - Invariant 1 (no ack before durable): after each kill the restarted
//     pipeline resumes from the source position Conduit persisted. Every input
//     record before that position must already be in the bucket, in an object
//     written before the kill. A record that was acked (its position
//     persisted) but not stored would be missing there.
//   - Invariant 2 (positions monotonic, crash-safe): the resume position after
//     each kill is never before the one after the previous kill. Redelivered
//     records (stored before the kill and again after it) are counted and
//     reported, not failed: at-least-once allows them.
//   - Invariant 3 (at-least-once): after a final run every input record is in
//     the bucket at least once.
//
// Limit of the invariant 1 check: Conduit persists source positions in the
// background, about once a second. An early ack (a record acked before its
// object is stored) is caught only when the gap between the ack and the
// object write is longer than that lag, because a kill inside the lag resumes
// from an older position, which looks correct. Kill points that matter let a
// run stream for a few seconds first (minStream) for that reason, but a
// connector that acks early by milliseconds can pass. The SDK acceptance suite's
// in-process crash cases (DST-04) cover that side.
//
// The resume position is read from the data, not from Conduit's database: the
// input is a file of numbered lines read by the built-in file source, which
// reads in order, so the first line a restarted run writes is the line after
// the persisted position. Objects are attributed to runs by the UnixNano in
// their key, written by the plugin process on this host, against the time each
// run was started.
//
// Everything but this file is behind the build tag `chaos`; run with
// `make test-chaos`. doc.go has no tag so the package always type-checks.
package chaos

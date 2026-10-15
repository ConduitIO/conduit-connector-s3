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

// Package position encodes and parses the S3 source's record positions.
//
// Two formats exist:
//
//   - v1 (`<key>_s<unix seconds>` / `<key>_c<unix seconds>`), written by
//     v0.9.x. It is parsed only to upgrade a pipeline (see Parse).
//   - v2 (binary, starts with 0x00, ends with a CRC-32C and 0x00): the state the
//     source needs to resume exactly. A snapshot position holds the snapshot
//     cut, the last key delivered and the tracked uploads (SnapshotV2). A
//     change capture position holds the settled cut S, the early set E and the
//     tracked uploads R (CDCV2).
//
// Invariants enforced here:
//
//   - every position is self-contained: resuming from any record's position
//     needs nothing else;
//   - parsing is strict and never defaults a field; every failure wraps
//     ErrInvalid, and the source fails Open with it (it never silently starts
//     over);
//   - a v2 position never ends in '_', so the v0.9.x parser fails on it
//     instead of panicking (rollback safety).
//
// Design: ConduitIO/conduit
// docs/design-documents/20261015-s3-source-exact-resume.md.
package position

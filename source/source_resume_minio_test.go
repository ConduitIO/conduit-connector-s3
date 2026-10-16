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

package source_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/matryer/is"
)

// TestMinIO_SnapshotResumeDoesNotRestart is the regression test for s3#1041: a
// restart from a snapshot position started the snapshot over, so every acked
// object came again. Resuming from the position of the third record must
// deliver only the remaining objects.
func TestMinIO_SnapshotResumeDoesNotRestart(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	client, cfg := minioSourceEnv(t, "")
	setLookback(cfg, "1s")
	bucket := cfg["aws.bucket"]
	for i := 0; i < 6; i++ {
		putObject(ctx, t, client, bucket, fmt.Sprintf("obj-%02d", i))
	}
	// Settled before the snapshot starts (older than the lookback), so the
	// snapshot, not change capture, delivers them.
	time.Sleep(3500 * time.Millisecond)

	s := openSource(ctx, t, cfg, nil)
	var acked opencdc.Position
	for i := 0; i < 3; i++ {
		rec, err := readWithTimeout(ctx, s, waitBudget)
		is.NoErr(err)
		is.Equal(string(rec.Key.Bytes()), fmt.Sprintf("obj-%02d", i))
		is.Equal(rec.Operation, opencdc.OperationSnapshot)
		acked = rec.Position
	}
	is.NoErr(s.Teardown(ctx))

	resumed := openSource(ctx, t, cfg, acked)
	seen := drainKeys(ctx, resumed, 4*time.Second)
	for i := 0; i < 3; i++ {
		if n := seen[fmt.Sprintf("obj-%02d", i)]; n != 0 {
			t.Errorf("obj-%02d was acked before the restart and delivered again %d time(s)", i, n)
		}
	}
	for i := 3; i < 6; i++ {
		if n := seen[fmt.Sprintf("obj-%02d", i)]; n != 1 {
			t.Errorf("obj-%02d delivered %d times after the restart, want 1", i, n)
		}
	}
}

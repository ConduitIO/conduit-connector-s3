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

package iterator

import (
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/conduitio/conduit-connector-s3/source/position"
	"github.com/rs/zerolog"
)

// cutAndTrack computes the settled cut from T and the in-progress multipart
// uploads (design "The cut and the uploads", decided 2026-10-15):
//
//   - uploads initiated at or after T - lookback do not matter yet: a version
//     they produce is stamped above the cut;
//   - an older upload initiated more than maxAge before tu (the store clock)
//     is released with a WARN: the README's lifecycle rule aborts such uploads
//     before they can complete;
//   - other old uploads are tracked (kept in existing, or added) while
//     len(tracked) < maxTracked;
//   - the oldest old upload that could not be tracked holds the cut below its
//     initiation time.
//
// existing is the tracked set carried over (nil for a new snapshot). Entries of
// existing whose upload is no longer listed are kept: resolving them needs the
// versions listing (change capture does that).
func cutAndTrack(t time.Time, lookback time.Duration, tu time.Time, uploads []types.MultipartUpload,
	existing []position.TrackedUpload, maxTracked int, maxAge time.Duration, log *zerolog.Logger,
) (time.Time, []position.TrackedUpload) {
	cut := t.Add(-lookback)
	tracked := append([]position.TrackedUpload(nil), existing...)
	known := make(map[trackedID]struct{}, len(tracked))
	for _, u := range tracked {
		known[idOf(u)] = struct{}{}
	}

	old := make([]types.MultipartUpload, 0, len(uploads))
	for _, u := range uploads {
		if u.Initiated != nil && u.Key != nil && u.Initiated.Before(cut) {
			old = append(old, u)
		}
	}
	sort.Slice(old, func(i, j int) bool { return old[i].Initiated.Before(*old[j].Initiated) })

	for _, u := range old {
		if tu.Sub(*u.Initiated) > maxAge {
			log.Warn().Str("code", "s3.source.stale_upload_ignored").
				Str("key", aws.ToString(u.Key)).Str("upload_id", aws.ToString(u.UploadId)).
				Time("initiated", *u.Initiated).Dur("max_pending_upload_age", maxAge).
				Msg("a multipart upload older than cdc.maxPendingUploadAge is not tracked; if it still completes, its object can be missed (add an AbortIncompleteMultipartUpload lifecycle rule, see the README)")
			continue
		}
		e := position.TrackedUpload{KeyHash: position.KeyHash(aws.ToString(u.Key)), Initiated: u.Initiated.UTC().Truncate(time.Millisecond)}
		if _, ok := known[idOf(e)]; ok {
			continue
		}
		if len(tracked) < maxTracked {
			tracked = append(tracked, e)
			known[idOf(e)] = struct{}{}
			continue
		}
		// Invariant 3: an old upload that cannot be tracked must stay above S,
		// so it holds the cut.
		hold := u.Initiated.Add(-time.Second)
		if hold.Before(cut) {
			cut = hold
		}
		log.Warn().Str("code", "s3.source.cut_held").
			Str("key", aws.ToString(u.Key)).Str("upload_id", aws.ToString(u.UploadId)).
			Time("initiated", *u.Initiated).Int("max_tracked_uploads", maxTracked).
			Msg("more long-running multipart uploads than cdc.maxTrackedUploads: this one holds the settled cut, so objects wait longer before they settle")
		break // older ones were handled; the hold is the oldest untracked
	}
	return cut, tracked
}

type trackedID struct {
	h  position.Hash
	ms int64
}

func idOf(u position.TrackedUpload) trackedID { return trackedID{u.KeyHash, u.Initiated.UnixMilli()} }

// matchesTracked reports whether a listed version is the completed object of a
// tracked upload: same key, LastModified equal to the initiation time at the
// second (as AWS reports it), and a multipart ETag ("<hex>-<parts>"), so that a
// plain PUT of the same key in the same second does not match.
func matchesTracked(tracked []position.TrackedUpload, key string, modified time.Time, etag string) bool {
	if len(tracked) == 0 || !isMultipartETag(etag) {
		return false
	}
	h := position.KeyHash(key)
	for _, u := range tracked {
		if u.KeyHash == h && u.Initiated.Unix() == modified.Unix() {
			return true
		}
	}
	return false
}

func isMultipartETag(etag string) bool {
	return strings.Contains(strings.Trim(etag, `"`), "-")
}

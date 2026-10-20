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
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
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
//     len(tracked) < maxTracked, unless tracking would be ambiguous (below);
//   - the oldest old upload that could not be tracked holds the cut below its
//     initiation time.
//
// A tracked upload is recognized by its key and the second of its initiation
// (matchesTracked). That is only sound when nothing else of the key can carry
// that second. So an old upload is not tracked, and holds the cut like an
// overflowing one, when another in-progress or tracked upload of the key was
// initiated in the same second, or when conflict reports that the store
// already lists a version of the key stamped in that second. Its completed
// object is then delivered like any other version, above S, whichever way the
// store stamps it: AWS with the initiation second, MinIO with the completion
// time. The hold needs S below that second; settled (the second of S, zero for
// a new snapshot) says whether it is, and an upload that cannot be held is
// tracked as before.
//
// existing is the tracked set carried over (nil for a new snapshot). Entries of
// existing whose upload is no longer listed are kept: resolving them needs the
// versions listing (change capture does that).
func cutAndTrack(t time.Time, lookback time.Duration, tu time.Time, uploads []types.MultipartUpload,
	existing []position.TrackedUpload, settled time.Time, maxTracked int, maxAge time.Duration,
	conflict func(types.MultipartUpload) (bool, error), held func(types.MultipartUpload), log *zerolog.Logger,
) (time.Time, []position.TrackedUpload, error) {
	cut := t.Add(-lookback)
	tracked := append([]position.TrackedUpload(nil), existing...)
	known := make(map[trackedID]struct{}, len(tracked))
	for _, u := range tracked {
		known[idOf(u)] = struct{}{}
	}

	// An upload is old if its completed object could be stamped at or below
	// the cut. AWS stamps that object with the initiation time at one-second
	// resolution, so the comparison uses the second the upload was initiated
	// in, and "at or below": S = (cut, +inf) covers versions stamped exactly
	// at the cut.
	old := make([]types.MultipartUpload, 0, len(uploads))
	for _, u := range uploads {
		if u.Initiated != nil && u.Key != nil && !stampSecond(*u.Initiated).After(cut) {
			old = append(old, u)
		}
	}
	sort.Slice(old, func(i, j int) bool { return old[i].Initiated.Before(*old[j].Initiated) })

	perSlot := uploadsPerSlot(old, uploads, existing, tu, maxAge)

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
		ambiguous := false
		if stampSecond(*u.Initiated).After(settled) {
			ambiguous = perSlot[slotOf(e)] > 1
			if !ambiguous && len(tracked) < maxTracked && conflict != nil {
				var err error
				if ambiguous, err = conflict(u); err != nil {
					return time.Time{}, nil, err
				}
			}
		}
		if len(tracked) < maxTracked && !ambiguous {
			tracked = append(tracked, e)
			known[idOf(e)] = struct{}{}
			continue
		}
		// Invariant 3: an old upload that cannot be tracked must stay above S,
		// so it holds the cut.
		hold := stampSecond(*u.Initiated).Add(-time.Second)
		if hold.Before(cut) {
			cut = hold
		}
		if ambiguous {
			if held != nil {
				held(u)
			} else {
				log.Warn().Str("code", "s3.source.cut_held_same_second").
					Str("key", aws.ToString(u.Key)).Str("upload_id", aws.ToString(u.UploadId)).
					Time("initiated", *u.Initiated).
					Msg(sameSecondHoldMsg)
			}
			break
		}
		log.Warn().Str("code", "s3.source.cut_held").
			Str("key", aws.ToString(u.Key)).Str("upload_id", aws.ToString(u.UploadId)).
			Time("initiated", *u.Initiated).Int("max_tracked_uploads", maxTracked).
			Msg("more long-running multipart uploads than cdc.maxTrackedUploads: this one holds the settled cut, so objects wait longer before they settle")
		break // older ones were handled; the hold is the oldest untracked
	}
	return cut, tracked, nil
}

const sameSecondHoldMsg = "a multipart upload shares its initiation second with another object of the same key: it cannot be recognized when it completes, so it holds the settled cut until it ends"

// Defaults of the same-second hold warning: how long a hold lasts before it is
// reported, and the minimum time between two reports for one key.
const (
	holdWarnAfter = 15 * time.Minute
	holdWarnEvery = 15 * time.Minute
)

// holdWarner reports same-second holds (see cutAndTrack) that keep the settled
// cut back for longer than holdWarnAfter: one WARN per key per holdWarnEvery,
// naming the key and how long the hold has lasted. A hold is first seen when
// its poll first reports it; the age restarts when the upload ends or stops
// holding. Not safe for concurrent use: the poll goroutine owns it.
type holdWarner struct {
	first  map[trackedID]time.Time // holds seen this or a previous poll
	seen   map[trackedID]struct{}  // holds seen in the current poll
	warned map[string]time.Time    // last warning per key
}

func newHoldWarner() *holdWarner {
	return &holdWarner{first: map[trackedID]time.Time{}, seen: map[trackedID]struct{}{}, warned: map[string]time.Time{}}
}

// begin starts a poll.
func (h *holdWarner) begin() { h.seen = map[trackedID]struct{}{} }

// hold records that u holds the cut at now.
func (h *holdWarner) hold(now time.Time, u types.MultipartUpload, log *zerolog.Logger) {
	id := idOf(position.TrackedUpload{KeyHash: position.KeyHash(aws.ToString(u.Key)), Initiated: u.Initiated.UTC().Truncate(time.Millisecond)})
	h.seen[id] = struct{}{}
	first, ok := h.first[id]
	if !ok {
		h.first[id] = now
		return
	}
	key := aws.ToString(u.Key)
	age := now.Sub(first)
	if age <= holdWarnAfter {
		return
	}
	if last, ok := h.warned[key]; ok && now.Sub(last) < holdWarnEvery {
		return
	}
	h.warned[key] = now
	log.Warn().Str("code", "s3.source.cut_held_same_second").
		Str("key", key).Str("upload_id", aws.ToString(u.UploadId)).
		Time("initiated", *u.Initiated).Dur("hold_age", age).
		Msg(sameSecondHoldMsg + "; the hold has lasted longer than " + holdWarnAfter.String() + ", so objects wait longer than usual before they settle (abort the upload, or add an AbortIncompleteMultipartUpload lifecycle rule)")
}

// end finishes a poll: holds that were not seen are over.
func (h *holdWarner) end(now time.Time) {
	for id := range h.first {
		if _, ok := h.seen[id]; !ok {
			delete(h.first, id)
		}
	}
	for k, t := range h.warned {
		if now.Sub(t) >= holdWarnEvery {
			delete(h.warned, k)
		}
	}
}

// slot is a key and a second of initiation: what matchesTracked identifies an
// upload by.
type slot struct {
	h   position.Hash
	sec int64
}

func slotOf(e position.TrackedUpload) slot { return slot{e.KeyHash, e.Initiated.Unix()} }

// uploadsPerSlot counts the uploads that share each slot: the old ones still
// within maxAge, plus the tracked ones that are no longer listed.
func uploadsPerSlot(old, listed []types.MultipartUpload, existing []position.TrackedUpload, tu time.Time, maxAge time.Duration) map[slot]int {
	perSlot := map[slot]int{}
	for _, u := range old {
		if tu.Sub(*u.Initiated) <= maxAge {
			perSlot[slot{position.KeyHash(aws.ToString(u.Key)), u.Initiated.Unix()}]++
		}
	}
	for _, e := range existing {
		if !isListed(listed, e) {
			perSlot[slotOf(e)]++
		}
	}
	return perSlot
}

// isListed reports whether the tracked entry e is an upload of the listing.
func isListed(uploads []types.MultipartUpload, e position.TrackedUpload) bool {
	for _, u := range uploads {
		if u.Key != nil && u.Initiated != nil && position.KeyHash(*u.Key) == e.KeyHash && u.Initiated.UTC().Truncate(time.Millisecond).Equal(e.Initiated) {
			return true
		}
	}
	return false
}

// nullVersionID is the version id S3 lists for objects in an unversioned
// bucket.
const nullVersionID = "null"

// stampSecond is the earliest LastModified the completed object of an upload
// initiated at t can carry: t rounded down to the second.
func stampSecond(t time.Time) time.Time { return t.UTC().Truncate(time.Second) }

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

// versionInSecond reports whether the store lists a version or delete marker
// of key, in any state, stamped in the second of at. It lists the key's own
// versions only (prefix = key; the key's versions come first, so the walk ends
// at the first other key).
func versionInSecond(ctx context.Context, client *s3.Client, bucket, key string, at time.Time) (bool, error) {
	sec := stampSecond(at)
	in := &s3.ListObjectVersionsInput{Bucket: aws.String(bucket), Prefix: aws.String(key)}
	for {
		out, err := client.ListObjectVersions(ctx, in)
		if err != nil {
			return false, fmt.Errorf("couldn't list the versions of %q: %w", key, err)
		}
		past := false
		for _, v := range out.Versions {
			if aws.ToString(v.Key) != key {
				past = true
				break
			}
			if v.LastModified != nil && stampSecond(*v.LastModified).Equal(sec) {
				return true, nil
			}
		}
		for _, v := range out.DeleteMarkers {
			if aws.ToString(v.Key) != key {
				past = true
				break
			}
			if v.LastModified != nil && stampSecond(*v.LastModified).Equal(sec) {
				return true, nil
			}
		}
		if past || !aws.ToBool(out.IsTruncated) {
			return false, nil
		}
		if aws.ToString(out.NextKeyMarker) == "" && aws.ToString(out.NextVersionIdMarker) == "" {
			return false, fmt.Errorf("couldn't list the versions of %q: the listing is truncated but has no next marker", key)
		}
		in.KeyMarker, in.VersionIdMarker = out.NextKeyMarker, out.NextVersionIdMarker
	}
}

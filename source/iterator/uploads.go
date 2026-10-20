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

// trackOpts are the bounds and callbacks of cutAndTrack.
type trackOpts struct {
	// Settled is the second of S (zero for a new snapshot).
	Settled    time.Time
	MaxTracked int
	MaxAge     time.Duration
	// Conflict reports whether the store lists a version of the upload's key
	// stamped in the upload's second. stale says the upload is older than
	// MaxAge. Nil means no probe.
	Conflict func(u types.MultipartUpload, stale bool) (bool, error)
	// Held, if set, is told about every upload holding the cut for sharing its
	// second; nil logs it.
	Held func(types.MultipartUpload)
}

// cutAndTrack computes the settled cut from T and the in-progress multipart
// uploads (design "The cut and the uploads", decided 2026-10-15):
//
//   - uploads initiated at or after T - lookback do not matter yet: a version
//     they produce is stamped above the cut;
//   - an older upload initiated more than maxAge before tu (the store clock)
//     is released from the hold, with an error-level log unless it is tracked
//     (see below): the README's lifecycle rule aborts such uploads before they
//     can complete;
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
// time. The hold needs S below that second (o.Settled, the second of S, says
// whether it is); an upload that cannot be held is tracked as before, and the
// caller then verifies the entry against the versions listing (change capture
// does). An upload older than o.MaxAge is released from the hold; it is still
// tracked when it shares its second with another object of the key.
//
// existing is the tracked set carried over (nil for a new snapshot). Entries of
// existing whose upload is no longer listed are kept: resolving them needs the
// versions listing (change capture does that).
func cutAndTrack(t time.Time, lookback time.Duration, tu time.Time, uploads []types.MultipartUpload,
	existing []position.TrackedUpload, o trackOpts, log *zerolog.Logger,
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
	old := oldUploads(uploads, cut)

	perSlot := uploadsPerSlot(old, uploads, existing, tu, o.MaxAge)
	perSlotAll := uploadsPerSlot(old, uploads, existing, tu, time.Duration(1<<62))

	for _, u := range old {
		e := position.TrackedUpload{
			KeyHash: position.KeyHash(aws.ToString(u.Key)), Initiated: u.Initiated.UTC().Truncate(time.Millisecond),
			UploadHash: position.UploadID(aws.ToString(u.UploadId)),
		}
		if _, ok := known[idOf(e)]; ok {
			continue
		}
		if tu.Sub(*u.Initiated) > o.MaxAge {
			trackIt, err := releaseStale(u, perSlotAll[slotOf(e)] > 1, len(tracked) < o.MaxTracked, o, log)
			if err != nil {
				return time.Time{}, nil, err
			}
			if trackIt {
				tracked = append(tracked, e)
				known[idOf(e)] = struct{}{}
			}
			continue
		}
		ambiguous, checked := false, false
		if stampSecond(*u.Initiated).After(o.Settled) {
			ambiguous = perSlot[slotOf(e)] > 1
			if !ambiguous && len(tracked) < o.MaxTracked && o.Conflict != nil {
				var err error
				if ambiguous, err = o.Conflict(u, false); err != nil {
					return time.Time{}, nil, err
				}
				checked = true
			}
		}
		if len(tracked) < o.MaxTracked && !ambiguous {
			// Verified: nothing else of the key was in the second when the
			// upload was probed, so its baseline is empty. Without the probe
			// the caller verifies the entry against the listing.
			e.Verified = checked
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
			if o.Held != nil {
				o.Held(u)
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
			Time("initiated", *u.Initiated).Int("max_tracked_uploads", o.MaxTracked).
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

// oldUploads returns the uploads whose completed object could be stamped at or
// below cut, oldest first.
func oldUploads(uploads []types.MultipartUpload, cut time.Time) []types.MultipartUpload {
	old := make([]types.MultipartUpload, 0, len(uploads))
	for _, u := range uploads {
		if u.Initiated != nil && u.Key != nil && !stampSecond(*u.Initiated).After(cut) {
			old = append(old, u)
		}
	}
	sort.Slice(old, func(i, j int) bool { return old[i].Initiated.Before(*old[j].Initiated) })
	return old
}

// releaseStale handles an upload older than o.MaxAge: it no longer holds the
// cut. It is tracked anyway (true) when it shares its initiation second with
// another object of its key: dropped, its object (stamped below S on AWS) would
// be lost, and the entry is exact because its baseline is probed while the
// upload is listed. Otherwise it is ignored, and that is an error-level log.
func releaseStale(u types.MultipartUpload, groupConflict, room bool, o trackOpts, log *zerolog.Logger) (bool, error) {
	if stampSecond(*u.Initiated).After(o.Settled) && room {
		ambiguous := groupConflict
		if !ambiguous && o.Conflict != nil {
			var err error
			if ambiguous, err = o.Conflict(u, true); err != nil {
				return false, err
			}
		}
		if ambiguous {
			log.Warn().Str("code", "s3.source.stale_upload_tracked").
				Str("key", aws.ToString(u.Key)).Str("upload_id", aws.ToString(u.UploadId)).
				Time("initiated", *u.Initiated).Dur("max_pending_upload_age", o.MaxAge).
				Msg("a multipart upload older than cdc.maxPendingUploadAge shares its initiation second with another object of its key: it no longer holds the settled cut and is tracked instead")
			return true, nil
		}
	}
	log.Error().Str("code", "s3.source.stale_upload_ignored").
		Str("key", aws.ToString(u.Key)).Str("upload_id", aws.ToString(u.UploadId)).
		Time("initiated", *u.Initiated).Dur("max_pending_upload_age", o.MaxAge).
		Msg("a multipart upload older than cdc.maxPendingUploadAge is not tracked; if it still completes, its object can be missed (abort it, or add an AbortIncompleteMultipartUpload lifecycle rule, see the README)")
	return false, nil
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
func matchesTracked(tracked []position.TrackedUpload, key, versionID string, modified time.Time, etag string) bool {
	for _, u := range tracked {
		if entryMatches(u, key, versionID, modified, etag) {
			return true
		}
	}
	return false
}

func isMultipartETag(etag string) bool {
	return strings.Contains(strings.Trim(etag, `"`), "-")
}

// keyVersion is one version or delete marker of a key as the versions listing
// reports it.
type keyVersion struct {
	versionID, etag string
	modified        time.Time
	deleteMarker    bool
}

// listKeyVersions calls visit with every version and delete marker of key, in
// any state. It lists the key's own versions only (prefix = key; they come
// first, so the walk ends at the first other key). visit returns true to stop.
func listKeyVersions(ctx context.Context, client *s3.Client, bucket, key string, visit func(keyVersion) bool) error {
	in := &s3.ListObjectVersionsInput{Bucket: aws.String(bucket), Prefix: aws.String(key)}
	for {
		out, err := client.ListObjectVersions(ctx, in)
		if err != nil {
			return fmt.Errorf("couldn't list the versions of %q: %w", key, err)
		}
		past := false
		for _, v := range out.Versions {
			if aws.ToString(v.Key) != key {
				past = true
				break
			}
			if v.LastModified == nil {
				continue
			}
			vid := aws.ToString(v.VersionId)
			if vid == "" {
				vid = nullVersionID
			}
			if visit(keyVersion{versionID: vid, etag: aws.ToString(v.ETag), modified: v.LastModified.UTC().Truncate(time.Millisecond)}) {
				return nil
			}
		}
		for _, v := range out.DeleteMarkers {
			if aws.ToString(v.Key) != key {
				past = true
				break
			}
			if v.LastModified == nil {
				continue
			}
			if visit(keyVersion{modified: v.LastModified.UTC().Truncate(time.Millisecond), deleteMarker: true}) {
				return nil
			}
		}
		if past || !aws.ToBool(out.IsTruncated) {
			return nil
		}
		if aws.ToString(out.NextKeyMarker) == "" && aws.ToString(out.NextVersionIdMarker) == "" {
			return fmt.Errorf("couldn't list the versions of %q: the listing is truncated but has no next marker", key)
		}
		in.KeyMarker, in.VersionIdMarker = out.NextKeyMarker, out.NextVersionIdMarker
	}
}

// versionInSecond reports whether the store lists a version or delete marker
// of key stamped in the second of at.
func versionInSecond(ctx context.Context, client *s3.Client, bucket, key string, at time.Time) (bool, error) {
	sec := stampSecond(at)
	found := false
	err := listKeyVersions(ctx, client, bucket, key, func(v keyVersion) bool {
		found = stampSecond(v.modified).Equal(sec)
		return found
	})
	return found, err
}

// uploadInProgress reports whether the upload with the given id is listed.
func uploadInProgress(ctx context.Context, client *s3.Client, bucket, key, uploadID string) (bool, error) {
	in := &s3.ListMultipartUploadsInput{Bucket: aws.String(bucket), Prefix: aws.String(key)}
	for {
		out, err := client.ListMultipartUploads(ctx, in)
		if err != nil {
			return false, fmt.Errorf("couldn't list the multipart uploads of %q: %w", key, err)
		}
		for _, u := range out.Uploads {
			if aws.ToString(u.Key) == key && aws.ToString(u.UploadId) == uploadID {
				return true, nil
			}
		}
		if !aws.ToBool(out.IsTruncated) {
			return false, nil
		}
		if aws.ToString(out.NextKeyMarker) == "" && aws.ToString(out.NextUploadIdMarker) == "" {
			return false, fmt.Errorf("couldn't list the multipart uploads of %q: the listing is truncated but has no next marker", key)
		}
		in.KeyMarker, in.UploadIdMarker = out.NextKeyMarker, out.NextUploadIdMarker
	}
}

// entryMatches reports whether a listed version is the completed object of
// the tracked upload u: same key, LastModified equal to the initiation time at
// the second (as AWS reports it), and a multipart ETag ("<hex>-<parts>"), so
// that a plain PUT of the same key in the same second does not match. For a
// verified entry the version must also not be in its baseline: the multipart
// versions of the key in that second that predate the upload. versionID is
// empty where the listing has none (the snapshot's).
func entryMatches(u position.TrackedUpload, key, versionID string, modified time.Time, etag string) bool {
	if u.KeyHash != position.KeyHash(key) || u.Initiated.Unix() != modified.Unix() || !isMultipartETag(etag) {
		return false
	}
	if u.Verified && len(u.Baseline) > 0 {
		vh := position.VersionHash(key, versionID, etag, modified)
		for _, b := range u.Baseline {
			if b == vh {
				return false
			}
		}
	}
	return true
}

// verifyEntries gives the entries of r that are not verified an identity and a
// baseline, when their upload is still listed: the upload is in progress, so
// its object does not exist yet and every multipart version of the key in its
// second predates it. That covers entries read from 0x02 positions (v0.11.x)
// and entries tracked because S had passed their second. The uploads are
// listed again after the versions: an upload that completed in between would
// put its own object in the baseline. An entry whose upload is gone stays
// unverified: its object may be listed already, so it keeps matching by key
// and second until it resolves. r is changed in place.
func verifyEntries(ctx context.Context, client *s3.Client, bucket string, uploads []types.MultipartUpload, r []position.TrackedUpload, log *zerolog.Logger) error {
	for i, e := range r {
		if e.Verified {
			continue
		}
		id := idOf(e)
		var up *types.MultipartUpload
		for j := range uploads {
			u := &uploads[j]
			if u.Key != nil && u.Initiated != nil && idOf(position.TrackedUpload{KeyHash: position.KeyHash(*u.Key), Initiated: u.Initiated.UTC().Truncate(time.Millisecond)}) == id {
				up = u
				break
			}
		}
		if up == nil {
			continue
		}
		key := aws.ToString(up.Key)
		var base []position.Hash
		err := listKeyVersions(ctx, client, bucket, key, func(v keyVersion) bool {
			if !v.deleteMarker && stampSecond(v.modified).Equal(stampSecond(e.Initiated)) && isMultipartETag(v.etag) {
				base = append(base, position.VersionHash(key, v.versionID, v.etag, v.modified))
			}
			return false
		})
		if err != nil {
			return err
		}
		still, err := uploadInProgress(ctx, client, bucket, key, aws.ToString(up.UploadId))
		if err != nil {
			return err
		}
		if !still {
			continue
		}
		if len(base) > position.MaxBaseline {
			log.Warn().Str("code", "s3.source.upload_identity_unavailable").Str("key", key).Int("versions_in_second", len(base)).
				Msg("more multipart versions of this key share the initiation second of a tracked upload than a position can hold: the upload keeps matching by key and second")
			continue
		}
		r[i].UploadHash = position.UploadID(aws.ToString(up.UploadId))
		r[i].Verified = true
		r[i].Baseline = base
	}
	return nil
}

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
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsmw "github.com/aws/aws-sdk-go-v2/aws/middleware"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-s3/source/position"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/rs/zerolog"
)

// Defaults of the settings the snapshot and CDC use for multipart uploads.
// They become connector settings in a later change; see the design,
// ConduitIO/conduit docs/design-documents/20261015-s3-source-exact-resume.md.
const (
	DefaultMaxTrackedUploads   = 256
	DefaultMaxPendingUploadAge = 24 * time.Hour
)

// ErrMultipartListingDenied is the stable error code of a source that may not
// list in-progress multipart uploads. It stops the source instead of falling
// back silently: without the listing, an upload that runs longer than the
// lookback can be missed. The fix is the s3:ListBucketMultipartUploads
// permission, or the documented, lossy opt-out.
var ErrMultipartListingDenied = errors.New("s3.source.multipart_listing_denied")

// SnapshotConfig configures a SnapshotIterator.
type SnapshotConfig struct {
	Bucket, Prefix string
	// Lookback is the CDC lookback period: the snapshot delivers only objects
	// that were settled (older than the lookback) when it started.
	Lookback time.Duration
	// MultipartHold enables the multipart uploads listing (on by default;
	// false is the lossy opt-out).
	MultipartHold       bool
	MaxTrackedUploads   int
	MaxPendingUploadAge time.Duration
	// Now is the host clock; nil means time.Now.
	Now func() time.Time
}

// SnapshotIterator delivers the objects that existed and were settled when
// the snapshot started, in key order, and resumes exactly from any of its
// positions.
//
// Invariants (design "Snapshot"):
//
//   - The snapshot cut C0 = min(Tu, Tl) - lookback, where Tu and Tl are the
//     Date headers of the first ListMultipartUploads and ListObjectsV2 pages,
//     capped at the host clock, and lowered below an old upload that could not
//     be tracked. Only objects with LastModified <= C0 are delivered; newer
//     ones, and completed versions of uploads tracked in R0, are left to CDC.
//   - Every record carries a snapshot position (C0, its key, R0). Resuming lists
//     after that key with the same C0 and R0, so nothing acked comes again and
//     nothing is skipped (s3#1041).
//   - A fetched object must be the listed version (If-Match on the ETag and the
//     same Last-Modified second); otherwise it changed after the listing and
//     is a CDC event, so the snapshot skips it.
//   - A failure is sticky: the iterator never reports the end of the snapshot
//     after an error, which would hand over to CDC and skip what is unread.
type SnapshotIterator struct {
	cfg    SnapshotConfig
	client *s3.Client
	log    *zerolog.Logger

	started bool
	cut     time.Time
	tracked []position.TrackedUpload

	startAfter string
	paginator  *s3.ListObjectsV2Paginator
	page       *s3.ListObjectsV2Output
	index      int
	lastToken  string
	exhausted  bool
	// listErr is sticky (see the type doc).
	listErr error
}

// NewSnapshotIterator returns an iterator that starts a new snapshot (resume
// nil) or continues the one in resume.
func NewSnapshotIterator(cfg SnapshotConfig, client *s3.Client, resume *position.SnapshotV2, log *zerolog.Logger) *SnapshotIterator {
	if log == nil {
		nop := zerolog.Nop()
		log = &nop
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	it := &SnapshotIterator{cfg: cfg, client: client, log: log}
	if resume != nil {
		it.started = true
		it.cut = resume.Cut
		it.tracked = resume.Tracked
		it.startAfter = resume.LastKey
	}
	return it
}

// Cut returns the snapshot cut and the uploads tracked at snapshot start. They
// are what change capture starts from after the snapshot. Valid once the
// snapshot has started (after the first HasNext or Next).
func (w *SnapshotIterator) Cut() (time.Time, []position.TrackedUpload) {
	return w.cut, w.tracked
}

func (w *SnapshotIterator) fail(err error) error {
	w.listErr = err
	return err
}

// start computes the cut of a new snapshot and fetches its first page.
func (w *SnapshotIterator) start(ctx context.Context) error {
	host := w.cfg.Now()
	var uploads []types.MultipartUpload
	tu := host
	if w.cfg.MultipartHold {
		var err error
		uploads, tu, err = listUploads(ctx, w.client, w.cfg.Bucket, w.cfg.Prefix, host, w.log)
		if err != nil {
			return err
		}
	}
	w.paginator = s3.NewListObjectsV2Paginator(w.client, w.listInput())
	first, err := w.paginator.NextPage(ctx)
	if err != nil {
		return fmt.Errorf("could not fetch the first snapshot page: %w", err)
	}
	tl := responseDate(first.ResultMetadata, host, w.log)
	t0 := earliest(tu, tl, host)
	conflict := func(u types.MultipartUpload) (bool, error) {
		return versionInSecond(ctx, w.client, w.cfg.Bucket, aws.ToString(u.Key), *u.Initiated)
	}
	cut, tracked, err := cutAndTrack(t0, w.cfg.Lookback, tu, uploads, nil, time.Time{}, w.cfg.MaxTrackedUploads, w.cfg.MaxPendingUploadAge, conflict, w.log)
	if err != nil {
		return err
	}
	w.cut, w.tracked = cut, tracked
	w.started = true
	return w.acceptPage(first)
}

func (w *SnapshotIterator) listInput() *s3.ListObjectsV2Input {
	in := &s3.ListObjectsV2Input{Bucket: aws.String(w.cfg.Bucket), Prefix: aws.String(w.cfg.Prefix)}
	if w.startAfter != "" {
		in.StartAfter = aws.String(w.startAfter)
	}
	return in
}

// acceptPage installs a fetched page. A page that claims to be truncated but
// offers no way to get the next one is a broken listing, not the end of the
// bucket: ending there would skip everything after it.
func (w *SnapshotIterator) acceptPage(p *s3.ListObjectsV2Output) error {
	next := aws.ToString(p.NextContinuationToken)
	if aws.ToBool(p.IsTruncated) && (next == "" || next == w.lastToken) {
		return errors.New("could not fetch next page: the listing is truncated but has no new continuation token")
	}
	w.lastToken = next
	w.page, w.index = p, 0
	return nil
}

// advance makes sure a page with an unread entry is current. It returns
// sdk.ErrBackoffRetry only when the listing is really exhausted.
func (w *SnapshotIterator) advance(ctx context.Context) error {
	if !w.started {
		if err := w.start(ctx); err != nil {
			return err
		}
	}
	if w.paginator == nil {
		w.paginator = s3.NewListObjectsV2Paginator(w.client, w.listInput())
	}
	for w.page == nil || w.index >= len(w.page.Contents) {
		if !w.paginator.HasMorePages() || (w.page != nil && !aws.ToBool(w.page.IsTruncated)) {
			w.exhausted = true
			return sdk.ErrBackoffRetry
		}
		p, err := w.paginator.NextPage(ctx)
		if err != nil {
			// The paginator did not advance, so the same page would be
			// fetched again; the error is sticky anyway.
			return fmt.Errorf("could not fetch next page: %w", err)
		}
		if err := w.acceptPage(p); err != nil {
			return err
		}
	}
	return nil
}

// HasNext reports whether a record may follow. A failure is not "no more
// objects": it returns true and Next reports the error.
func (w *SnapshotIterator) HasNext(ctx context.Context) bool {
	if w.listErr != nil {
		return true
	}
	if w.exhausted {
		return false
	}
	if err := w.advance(ctx); err != nil {
		if errors.Is(err, sdk.ErrBackoffRetry) {
			return false
		}
		_ = w.fail(err)
	}
	return true
}

// Next returns the next settled object. Objects newer than the cut, completed
// versions of tracked uploads, and objects that changed or disappeared after
// the listing are skipped: change capture delivers them.
func (w *SnapshotIterator) Next(ctx context.Context) (opencdc.Record, error) {
	for {
		if w.listErr != nil {
			return opencdc.Record{}, w.listErr
		}
		if err := w.advance(ctx); err != nil {
			if errors.Is(err, sdk.ErrBackoffRetry) {
				return opencdc.Record{}, err
			}
			return opencdc.Record{}, w.fail(err)
		}
		listed := w.page.Contents[w.index]
		if !w.belongsToSnapshot(listed) {
			w.index++
			continue
		}
		rec, skip, err := w.read(ctx, listed)
		if err != nil {
			return opencdc.Record{}, w.fail(err)
		}
		// Everything that can fail is behind us: only now move on.
		w.index++
		if skip {
			continue
		}
		return rec, nil
	}
}

// belongsToSnapshot reports whether the listed object is delivered by the
// snapshot: settled at the start, and not the completed version of a tracked
// upload (change capture delivers those, design "Tracked uploads").
func (w *SnapshotIterator) belongsToSnapshot(o types.Object) bool {
	if o.LastModified == nil || o.LastModified.After(w.cut) {
		return false
	}
	return !matchesTracked(w.tracked, aws.ToString(o.Key), *o.LastModified, aws.ToString(o.ETag))
}

// read fetches the listed version. skip is true when the object changed or
// disappeared after it was listed.
func (w *SnapshotIterator) read(ctx context.Context, listed types.Object) (rec opencdc.Record, skip bool, err error) {
	key := aws.ToString(listed.Key)
	object, err := w.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket:  aws.String(w.cfg.Bucket),
		Key:     listed.Key,
		IfMatch: listed.ETag,
	})
	if err != nil {
		if isGone(err) {
			w.log.Debug().Str("key", key).Msg("object changed or was deleted after it was listed; change capture delivers what replaced it")
			return opencdc.Record{}, true, nil
		}
		return opencdc.Record{}, false, fmt.Errorf("could not fetch the next object: %w", err)
	}
	defer object.Body.Close()
	if !sameSecond(object.LastModified, listed.LastModified) {
		// Same content written again after the listing: the ETag matches, the
		// time does not. Delivering it here would deliver it again from CDC.
		w.log.Debug().Str("key", key).Msg("object was overwritten with the same content after it was listed; change capture delivers it")
		return opencdc.Record{}, true, nil
	}
	body, err := io.ReadAll(object.Body)
	if err != nil {
		return opencdc.Record{}, false, fmt.Errorf("could not read the object's body: %w", err)
	}
	p, err := position.SnapshotV2{Cut: w.cut, LastKey: key, Tracked: w.tracked}.ToRecordPosition()
	if err != nil {
		return opencdc.Record{}, false, fmt.Errorf("could not encode the position for %q: %w", key, err)
	}
	m := opencdc.Metadata{MetadataS3HeaderPrefix + MetadataContentType: aws.ToString(object.ContentType)}
	for k, v := range object.Metadata {
		m[k] = v
	}
	return sdk.Util.Source.NewRecordSnapshot(p, m, opencdc.RawData(key), opencdc.RawData(body)), false, nil
}

func (w *SnapshotIterator) Stop() {}

// isGone reports a GetObject that found the listed version replaced (412 on
// If-Match) or deleted (404).
func isGone(err error) bool {
	var noSuchKey *types.NoSuchKey
	if errors.As(err, &noSuchKey) {
		return true
	}
	var re *smithyhttp.ResponseError
	if errors.As(err, &re) {
		switch re.HTTPStatusCode() {
		case http.StatusPreconditionFailed, http.StatusNotFound:
			return true
		}
	}
	return false
}

func sameSecond(a, b *time.Time) bool {
	if a == nil || b == nil {
		return false
	}
	return a.Unix() == b.Unix()
}

// responseDate returns the Date header of a response, capped at the host
// clock; the host clock when it is missing or unparseable.
func responseDate(md middleware.Metadata, host time.Time, log *zerolog.Logger) time.Time {
	raw, ok := awsmw.GetRawResponse(md).(*smithyhttp.Response)
	if !ok || raw == nil {
		return host
	}
	d, err := http.ParseTime(raw.Header.Get("Date"))
	if err != nil {
		log.Warn().Str("date", raw.Header.Get("Date")).Msg("listing response has no usable Date header; using the host clock for the settled cut")
		return host
	}
	if d.After(host) {
		return host
	}
	return d
}

func earliest(ts ...time.Time) time.Time {
	m := ts[0]
	for _, t := range ts[1:] {
		if t.Before(m) {
			m = t
		}
	}
	return m
}

// listUploads lists the in-progress multipart uploads under the prefix, all
// pages, and returns the Date of the first page (Tu, capped at host).
func listUploads(ctx context.Context, client *s3.Client, bucket, prefix string, host time.Time, log *zerolog.Logger) ([]types.MultipartUpload, time.Time, error) {
	in := &s3.ListMultipartUploadsInput{Bucket: aws.String(bucket), Prefix: aws.String(prefix)}
	var all []types.MultipartUpload
	tu := time.Time{}
	for {
		out, err := client.ListMultipartUploads(ctx, in)
		if err != nil {
			var apiErr smithy.APIError
			if errors.As(err, &apiErr) && apiErr.ErrorCode() == "AccessDenied" {
				return nil, time.Time{}, fmt.Errorf("%w: listing in-progress multipart uploads under %q was denied; grant s3:ListBucketMultipartUploads, or set cdc.multipartHold to false (lossy for multipart uploads longer than lookbackPeriod): %w",
					ErrMultipartListingDenied, prefix, err)
			}
			return nil, time.Time{}, fmt.Errorf("could not list multipart uploads: %w", err)
		}
		if tu.IsZero() {
			tu = responseDate(out.ResultMetadata, host, log)
		}
		all = append(all, out.Uploads...)
		if !aws.ToBool(out.IsTruncated) {
			break
		}
		if aws.ToString(out.NextKeyMarker) == "" && aws.ToString(out.NextUploadIdMarker) == "" {
			return nil, time.Time{}, errors.New("could not list multipart uploads: the listing is truncated but has no next marker")
		}
		in.KeyMarker, in.UploadIdMarker = out.NextKeyMarker, out.NextUploadIdMarker
	}
	return all, tu, nil
}

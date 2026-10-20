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

package iterator

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-s3/source/position"
)

// awsModel is an in-process S3 that behaves like AWS (or, with minio set, like
// MinIO) for change capture:
//
//   - AWS stamps every version at one-second resolution, and stamps the object
//     of a completed multipart upload with the INITIATION time; MinIO stamps
//     at millisecond resolution and stamps a completed upload with the
//     COMPLETION time (minio field);
//
//   - ListObjectVersions in key order (newest version first per key), paged,
//     with a Date header from the store clock (rounded down to the second);
//
//   - versions become visible at visibleAt, which can be later than their
//     LastModified (late visibility, A1);
//
//   - multipart uploads are listed while in progress (with Prefix, paged) and,
//     when completed, produce a version stamped with the INITIATION time and a
//     multipart ETag ("<hex>-<parts>"), visible from completion (A2);
//
//   - GetObject by VersionId, or by If-Match on the latest version (412 on a
//     mismatch), with Last-Modified of what it returns;
//
//   - hooks to advance the clock between listing pages (slow listings, R1/B4)
//     and to make GetObject see a different version than the listing (R8).
type awsModel struct {
	fakeStore
	mu        sync.Mutex
	now       time.Time
	versioned bool
	// minio switches the stamping from AWS's to MinIO's (see the type doc).
	minio    bool
	keys     map[string][]*mVersion // oldest first
	uploads  []*mUpload
	seq      int
	pageSize int
	// tickPerPage advances the clock after each listing page.
	tickPerPage time.Duration
	// denyUploads makes ListMultipartUploads answer AccessDenied.
	denyUploads bool
	// getHook can replace what GetObject returns.
	getHook func(key string, v *mVersion) *mVersion
	// onUploadsPage runs after each ListMultipartUploads page (1-based).
	onUploadsPage func(n int)
	uploadsPages  int
	// onVersionsPage runs after each ListObjectVersions page (1-based).
	onVersionsPage func(n int)
	versionsPages  int
	deletes        map[string]int
}

type mVersion struct {
	id        string
	etag      string
	modified  time.Time
	visibleAt time.Time
	deleted   bool // delete marker
	body      string
}

type mUpload struct {
	key       string
	id        string
	initiated time.Time
}

func newAWSModel(now time.Time, versioned bool) *awsModel {
	m := &awsModel{now: now, versioned: versioned, keys: map[string][]*mVersion{}, pageSize: 1000, deletes: map[string]int{}}
	m.handler = m.serve
	m.fakeStore.uploads = m.serveUploads
	return m
}

func (m *awsModel) clock() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.now
}

func (m *awsModel) advance(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.now = m.now.Add(d)
}

// put writes a plain object stamped now, visible after delay.
func (m *awsModel) put(key string, delay time.Duration) *mVersion {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.addVersion(key, m.now, m.now.Add(delay), false, "")
}

func (m *awsModel) addVersion(key string, modified, visible time.Time, deleted bool, etag string) *mVersion {
	m.seq++
	id := nullVersionID
	if m.versioned {
		id = fmt.Sprintf("v%06d", m.seq)
	}
	if etag == "" && !deleted {
		etag = fmt.Sprintf(`"%032x"`, m.seq)
	}
	stamp := modified.UTC().Truncate(time.Millisecond)
	if !m.minio {
		stamp = stamp.Truncate(time.Second)
	}
	v := &mVersion{id: id, etag: etag, modified: stamp, visibleAt: visible, deleted: deleted, body: fmt.Sprintf("%s#%d", key, m.seq)}
	if !m.versioned {
		m.keys[key] = []*mVersion{v} // an overwrite replaces the null version
	} else {
		m.keys[key] = append(m.keys[key], v)
	}
	return v
}

func (m *awsModel) del(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deletes[key]++
	if m.versioned {
		m.addVersion(key, m.now, m.now, true, "")
		return
	}
	delete(m.keys, key)
}

func (m *awsModel) startUpload(key string) *mUpload {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.seq++
	u := &mUpload{key: key, id: fmt.Sprintf("up%d", m.seq), initiated: m.now.UTC().Truncate(time.Millisecond)}
	m.uploads = append(m.uploads, u)
	return u
}

// complete finishes an upload: AWS stamps the object with the initiation time
// (truncated to the second by addVersion), MinIO with the completion time.
func (m *awsModel) complete(u *mUpload) *mVersion {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.removeUpload(u)
	m.seq++
	stamp := u.initiated
	if m.minio {
		stamp = m.now
	}
	return m.addVersion(u.key, stamp, m.now, false, fmt.Sprintf(`"%032x-3"`, m.seq))
}

func (m *awsModel) abort(u *mUpload) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.removeUpload(u)
}

func (m *awsModel) removeUpload(u *mUpload) {
	for i, x := range m.uploads {
		if x == u {
			m.uploads = append(m.uploads[:i], m.uploads[i+1:]...)
			return
		}
	}
}

func (m *awsModel) date() string { return m.now.UTC().Format(http.TimeFormat) }

func (m *awsModel) serveUploads(r *http.Request) *http.Response {
	m.mu.Lock()
	if m.denyUploads {
		m.mu.Unlock()
		return xmlResp(403, `<?xml version="1.0"?><Error><Code>AccessDenied</Code><Message>denied</Message></Error>`)
	}
	prefix := r.URL.Query().Get("prefix")
	keyMarker := r.URL.Query().Get("key-marker")
	idMarker := r.URL.Query().Get("upload-id-marker")
	ups := make([]*mUpload, 0, len(m.uploads))
	for _, u := range m.uploads {
		if strings.HasPrefix(u.key, prefix) {
			ups = append(ups, u)
		}
	}
	sort.Slice(ups, func(i, j int) bool {
		if ups[i].key != ups[j].key {
			return ups[i].key < ups[j].key
		}
		return ups[i].id < ups[j].id
	})
	var page []*mUpload
	for _, u := range ups {
		if keyMarker != "" && (u.key < keyMarker || (u.key == keyMarker && u.id <= idMarker)) {
			continue
		}
		page = append(page, u)
	}
	truncated := len(page) > m.pageSize
	if truncated {
		page = page[:m.pageSize]
	}
	var b strings.Builder
	fmt.Fprintf(&b, `<?xml version="1.0"?><ListMultipartUploadsResult><Bucket>b</Bucket><IsTruncated>%v</IsTruncated>`, truncated)
	if truncated {
		last := page[len(page)-1]
		fmt.Fprintf(&b, `<NextKeyMarker>%s</NextKeyMarker><NextUploadIdMarker>%s</NextUploadIdMarker>`, last.key, last.id)
	}
	for _, u := range page {
		fmt.Fprintf(&b, `<Upload><Key>%s</Key><UploadId>%s</UploadId><Initiated>%s</Initiated></Upload>`, u.key, u.id, u.initiated.Format(ms3))
	}
	b.WriteString(`</ListMultipartUploadsResult>`)
	resp := xmlResp(200, b.String(), "Date", m.date())
	m.uploadsPages++
	n, hook := m.uploadsPages, m.onUploadsPage
	m.now = m.now.Add(m.tickPerPage)
	m.mu.Unlock()
	if hook != nil {
		hook(n)
	}
	return resp
}

func (m *awsModel) serve(r *http.Request) *http.Response {
	m.mu.Lock()
	if r.URL.Query().Has("versions") {
		resp := m.listVersions(r)
		m.versionsPages++
		n, hook := m.versionsPages, m.onVersionsPage
		m.mu.Unlock()
		if hook != nil {
			hook(n)
		}
		return resp
	}
	defer m.mu.Unlock()
	key := keyOf(r)
	vs := m.visible(key)
	if len(vs) == 0 || vs[len(vs)-1].deleted {
		return xmlResp(404, `<?xml version="1.0"?><Error><Code>NoSuchKey</Code><Message>gone</Message></Error>`)
	}
	var v *mVersion
	if id := r.URL.Query().Get("versionId"); id != "" {
		for _, x := range vs {
			if x.id == id {
				v = x
			}
		}
		if v == nil {
			return xmlResp(404, `<?xml version="1.0"?><Error><Code>NoSuchVersion</Code><Message>gone</Message></Error>`)
		}
	} else {
		v = vs[len(vs)-1]
		if im := r.Header.Get("If-Match"); im != "" && im != v.etag {
			return xmlResp(412, `<?xml version="1.0"?><Error><Code>PreconditionFailed</Code><Message>no</Message></Error>`)
		}
	}
	if m.getHook != nil {
		v = m.getHook(key, v)
	}
	h := http.Header{
		"Content-Type":   {"text/plain"},
		"Last-Modified":  {v.modified.UTC().Format(http.TimeFormat)},
		"Etag":           {v.etag},
		"Content-Length": {strconv.Itoa(len(v.body))},
	}
	if m.versioned {
		h.Set("X-Amz-Version-Id", v.id)
	}
	return &http.Response{StatusCode: 200, Header: h, Body: io.NopCloser(strings.NewReader(v.body))}
}

func (m *awsModel) visible(key string) []*mVersion {
	var out []*mVersion
	for _, v := range m.keys[key] {
		if !v.visibleAt.After(m.now) {
			out = append(out, v)
		}
	}
	return out
}

type mEntry struct {
	key string
	v   *mVersion
	tip bool
}

func (m *awsModel) listVersions(r *http.Request) *http.Response {
	keys := make([]string, 0, len(m.keys))
	for k := range m.keys {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	prefix := r.URL.Query().Get("prefix")
	var all []mEntry
	for _, k := range keys {
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		vs := m.visible(k)
		for i := len(vs) - 1; i >= 0; i-- { // newest first, as S3 lists
			all = append(all, mEntry{k, vs[i], i == len(vs)-1})
		}
	}
	keyMarker := r.URL.Query().Get("key-marker")
	idMarker := r.URL.Query().Get("version-id-marker")
	start := 0
	if keyMarker != "" {
		for i, e := range all {
			if e.key == keyMarker && e.v.id == idMarker {
				start = i + 1
				break
			}
		}
	}
	page := all[start:]
	truncated := len(page) > m.pageSize
	if truncated {
		page = page[:m.pageSize]
	}
	var b strings.Builder
	fmt.Fprintf(&b, `<?xml version="1.0"?><ListVersionsResult><IsTruncated>%v</IsTruncated>`, truncated)
	if truncated {
		last := page[len(page)-1]
		fmt.Fprintf(&b, `<NextKeyMarker>%s</NextKeyMarker><NextVersionIdMarker>%s</NextVersionIdMarker>`, last.key, last.v.id)
	}
	for _, e := range page {
		if e.v.deleted {
			fmt.Fprintf(&b, `<DeleteMarker><Key>%s</Key><VersionId>%s</VersionId><IsLatest>%v</IsLatest><LastModified>%s</LastModified></DeleteMarker>`,
				e.key, e.v.id, e.tip, e.v.modified.Format(ms3))
			continue
		}
		fmt.Fprintf(&b, `<Version><Key>%s</Key><VersionId>%s</VersionId><IsLatest>%v</IsLatest><LastModified>%s</LastModified><ETag>%s</ETag></Version>`,
			e.key, e.v.id, e.tip, e.v.modified.Format(ms3), e.v.etag)
	}
	b.WriteString(`</ListVersionsResult>`)
	resp := xmlResp(200, b.String(), "Date", m.date())
	m.now = m.now.Add(m.tickPerPage)
	return resp
}

// identity names a delivered version for exactness checks.
func identity(r opencdc.Record) string {
	return string(r.Key.Bytes()) + "|" + r.Operation.String() + "|" + body(r)
}

func body(r opencdc.Record) string {
	if r.Payload.After == nil {
		return ""
	}
	return string(r.Payload.After.Bytes())
}

// driver steps a CDC iterator synchronously against the model and can kill
// and resume it from the position of any emitted record.
type driver struct {
	t     *testing.T
	m     *awsModel
	cfg   CDCConfig
	it    *CDCIterator
	start position.CDCV2   // the state the current run started from
	run   []opencdc.Record // emitted since the last (re)start
	acked []opencdc.Record // records acked before kills, plus the current run on finish
}

func newDriver(t *testing.T, m *awsModel, cfg CDCConfig, state position.CDCV2) *driver {
	cfg.Bucket = "b"
	cfg.Now = m.clock
	if cfg.MaxEarlyObjects == 0 {
		cfg.MaxEarlyObjects = 16
	}
	if cfg.MaxSettledBytesPerPoll == 0 {
		cfg.MaxSettledBytesPerPoll = 1 << 20
	}
	if cfg.MaxTrackedUploads == 0 {
		cfg.MaxTrackedUploads = 8
	}
	if cfg.MaxPendingUploadAge == 0 {
		cfg.MaxPendingUploadAge = 24 * time.Hour
	}
	if cfg.Lookback == 0 {
		cfg.Lookback = 5 * time.Minute
	}
	cfg.MultipartHold = true
	d := &driver{t: t, m: m, cfg: cfg, start: state}
	d.it = newCDCIterator(cfg, m.client(), state, nil)
	return d
}

func (d *driver) poll() []opencdc.Record {
	d.t.Helper()
	var got []opencdc.Record
	err := d.it.step(context.Background(), func(r opencdc.Record) error {
		got = append(got, r)
		return nil
	})
	if err != nil {
		d.t.Fatalf("step: %v", err)
	}
	d.run = append(d.run, got...)
	return got
}

// kill drops the iterator and resumes from the position of run[n-1] (the
// last acked record). n == 0 means nothing was acked in this run: Conduit
// resumes from the position it resumed from last time, the run's start state.
func (d *driver) kill(n int) {
	d.t.Helper()
	state := d.start
	if n > 0 {
		p, err := position.ParseV2(d.run[n-1].Position)
		if err != nil || p.CDC == nil {
			d.t.Fatalf("emitted position does not parse: %v", err)
		}
		state = *p.CDC
	}
	d.acked = append(d.acked, d.run[:n]...)
	d.run = nil
	d.start = state
	d.it = newCDCIterator(d.cfg, d.m.client(), state, nil)
}

func (d *driver) finish() []opencdc.Record {
	d.acked = append(d.acked, d.run...)
	d.run = nil
	return d.acked
}

func keysOf(rs []opencdc.Record) []string {
	out := make([]string, len(rs))
	for i, r := range rs {
		out[i] = string(r.Key.Bytes())
	}
	return out
}

// requireExactlyOnce checks no version identity was delivered twice among
// the acked records. Delete records carry no body, so successive delete markers
// of one key look alike; for them the count per key must not exceed the
// number of delete markers the model created.
func requireExactlyOnce(t *testing.T, rs []opencdc.Record, deletesPerKey map[string]int) {
	t.Helper()
	seen := map[string]int{}
	dels := map[string]int{}
	for _, r := range rs {
		if r.Operation == opencdc.OperationDelete {
			dels[string(r.Key.Bytes())]++
			continue
		}
		seen[identity(r)]++
	}
	for id, n := range seen {
		if n > 1 {
			t.Errorf("delivered %d times: %s", n, id)
		}
	}
	for k, n := range dels {
		if n > deletesPerKey[k] {
			t.Errorf("key %s: %d delete records, but only %d delete markers were written", k, n, deletesPerKey[k])
		}
	}
}

var _ = s3.ListObjectVersionsInput{}

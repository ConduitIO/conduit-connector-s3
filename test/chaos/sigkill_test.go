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

//go:build chaos

package chaos

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	envConduitBin = "CONDUIT_BIN"
	envMinIO      = "MINIO_ENDPOINT"
	// envPIDFile, when set, names a file every spawned Conduit process group
	// id is appended to, so whoever runs the test can clean up by PID if the
	// test binary itself dies.
	envPIDFile = "CHAOS_PID_FILE"
	// envSeed overrides the kill schedule seed (default fixed).
	envSeed = "CHAOS_SEED"
	// envArtifacts, when set, is a directory the Conduit logs of every run
	// are copied to (CI uploads it).
	envArtifacts = "CHAOS_ARTIFACTS"

	minioUser   = "conduitminio"
	minioSecret = "conduitminiosecret"
)

// conduitProc is one Conduit run: its own process group, so a kill takes the
// standalone plugin processes with it, as the death of a container or VM
// would. A plugin that outlived the kill could still write after it and
// would blur which run an object belongs to.
type conduitProc struct {
	cmd     *exec.Cmd
	started time.Time
	done    chan error
	log     string
}

type harness struct {
	t        *testing.T
	bin      string
	dir      string
	endpoint string
	bucket   string
	prefix   string
	client   *s3.Client
	runs     []*conduitProc

	cache map[string][]int // object key -> line numbers in it
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	bin := os.Getenv(envConduitBin)
	endpoint := os.Getenv(envMinIO)
	if bin == "" || endpoint == "" {
		t.Fatalf("%s and %s must be set (see `make test-chaos`)", envConduitBin, envMinIO)
	}
	dir := t.TempDir()
	h := &harness{
		t: t, bin: bin, dir: dir, endpoint: endpoint,
		bucket: fmt.Sprintf("chaos-%d", time.Now().UnixNano()),
		prefix: "chaos",
		cache:  map[string][]int{},
	}
	cfg, err := awsConfig.LoadDefaultConfig(context.Background(),
		awsConfig.WithRegion("us-east-1"),
		awsConfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(minioUser, minioSecret, "")))
	if err != nil {
		t.Fatal(err)
	}
	h.client = s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})
	if _, err := h.client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: aws.String(h.bucket)}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	t.Cleanup(h.cleanup)
	for _, d := range []string{"connectors", "pipelines", "processors"} {
		if err := os.MkdirAll(filepath.Join(dir, d), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	h.buildPlugin()
	return h
}

// buildPlugin builds this working tree's connector as a standalone plugin.
func (h *harness) buildPlugin() {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		h.t.Fatal("cannot locate the test source")
	}
	root := filepath.Join(filepath.Dir(file), "..", "..")
	out := filepath.Join(h.dir, "connectors", "conduit-connector-s3")
	cmd := exec.CommandContext(context.Background(), "go", "build", "-o", out,
		"-ldflags", "-X 'github.com/conduitio/conduit-connector-s3.version=v0.0.0-chaos'",
		"./cmd/connector")
	cmd.Dir = root
	if b, err := cmd.CombinedOutput(); err != nil {
		h.t.Fatalf("build plugin: %v\n%s", err, b)
	}
}

func (h *harness) writePipeline(input string, extraDest map[string]string) {
	dest := map[string]string{
		"aws.accessKeyId":     minioUser,
		"aws.secretAccessKey": minioSecret,
		"aws.region":          "us-east-1",
		"aws.bucket":          h.bucket,
		"aws.endpoint":        h.endpoint,
		"aws.pathStyle":       "true",
		"prefix":              h.prefix,
		"format":              "json",
	}
	for k, v := range extraDest {
		dest[k] = v
	}
	keys := make([]string, 0, len(dest))
	for k := range dest {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	fmt.Fprintf(&b, `version: "2.2"
pipelines:
  - id: chaos
    status: running
    connectors:
      - id: src
        type: source
        plugin: builtin:file
        settings:
          path: %q
      - id: dst
        type: destination
        plugin: standalone:s3
        settings:
`, input)
	for _, k := range keys {
		fmt.Fprintf(&b, "          %s: %q\n", k, dest[k])
	}
	if err := os.WriteFile(filepath.Join(h.dir, "pipelines", "chaos.yaml"), []byte(b.String()), 0o600); err != nil {
		h.t.Fatal(err)
	}
}

// start spawns Conduit with a fixed, minimal environment: nothing from the
// test's environment (credentials, AWS_ENDPOINT_URL, proxies) reaches it.
func (h *harness) start() *conduitProc {
	h.t.Helper()
	n := len(h.runs)
	logPath := filepath.Join(h.dir, fmt.Sprintf("conduit-run%d.log", n))
	logf, err := os.Create(logPath)
	if err != nil {
		h.t.Fatal(err)
	}
	cmd := exec.CommandContext(context.Background(), h.bin, "run",
		"--pipelines.path", filepath.Join(h.dir, "pipelines"),
		"--connectors.path", filepath.Join(h.dir, "connectors"),
		"--processors.path", filepath.Join(h.dir, "processors"),
		"--db.type", "badger",
		"--db.badger.path", filepath.Join(h.dir, "db"),
		"--api.enabled=false",
		"--log.format", "json",
		"--pipelines.exit-on-degraded",
	)
	cmd.Dir = h.dir
	cmd.Env = []string{"PATH=/usr/bin:/bin", "HOME=" + h.dir, "TMPDIR=" + h.dir}
	cmd.Stdout, cmd.Stderr = logf, logf
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	p := &conduitProc{cmd: cmd, done: make(chan error, 1), log: logPath}
	p.started = time.Now()
	if err := cmd.Start(); err != nil {
		h.t.Fatal(err)
	}
	if f := os.Getenv(envPIDFile); f != "" {
		if pf, err := os.OpenFile(f, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600); err == nil {
			fmt.Fprintf(pf, "%d\n", cmd.Process.Pid)
			_ = pf.Close()
		}
	}
	go func() { p.done <- cmd.Wait(); _ = logf.Close() }()
	h.runs = append(h.runs, p)
	return p
}

// kill sends SIGKILL to the run's whole process group and waits for it.
func (h *harness) kill(p *conduitProc) {
	_ = syscall.Kill(-p.cmd.Process.Pid, syscall.SIGKILL)
	select {
	case <-p.done:
	case <-time.After(30 * time.Second):
		h.t.Fatalf("conduit did not die after SIGKILL")
	}
}

func (h *harness) cleanup() {
	for _, p := range h.runs {
		if p.cmd.ProcessState == nil {
			_ = syscall.Kill(-p.cmd.Process.Pid, syscall.SIGKILL)
		}
	}
	if a := os.Getenv(envArtifacts); a != "" {
		_ = os.MkdirAll(a, 0o755)
		for _, p := range h.runs {
			if b, err := os.ReadFile(p.log); err == nil {
				_ = os.WriteFile(filepath.Join(a, filepath.Base(p.log)), b, 0o600)
			}
		}
	}
	if h.t.Failed() {
		for i, p := range h.runs {
			b, _ := os.ReadFile(p.log)
			if len(b) > 4000 {
				b = b[len(b)-4000:]
			}
			h.t.Logf("--- conduit run %d log (tail) ---\n%s", i, b)
		}
	}
	ctx := context.Background()
	pg := s3.NewListObjectsV2Paginator(h.client, &s3.ListObjectsV2Input{Bucket: aws.String(h.bucket)})
	for pg.HasMorePages() {
		page, err := pg.NextPage(ctx)
		if err != nil {
			break
		}
		for _, o := range page.Contents {
			_, _ = h.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(h.bucket), Key: o.Key})
		}
	}
	_, _ = h.client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(h.bucket)})
}

// scan reads every object not read yet and returns the whole map.
func (h *harness) scan() map[string][]int {
	h.t.Helper()
	ctx := context.Background()
	pg := s3.NewListObjectsV2Paginator(h.client, &s3.ListObjectsV2Input{Bucket: aws.String(h.bucket), Prefix: aws.String(h.prefix + "/")})
	for pg.HasMorePages() {
		page, err := pg.NextPage(ctx)
		if err != nil {
			h.t.Fatalf("list: %v", err)
		}
		for _, o := range page.Contents {
			key := aws.ToString(o.Key)
			if _, ok := h.cache[key]; ok {
				continue
			}
			obj, err := h.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(h.bucket), Key: o.Key})
			if err != nil {
				h.t.Fatalf("get %s: %v", key, err)
			}
			body, err := io.ReadAll(obj.Body)
			_ = obj.Body.Close()
			if err != nil {
				h.t.Fatalf("read %s: %v", key, err)
			}
			h.cache[key] = parseLines(h.t, key, body)
		}
	}
	return h.cache
}

func parseLines(t *testing.T, key string, body []byte) []int {
	var out []int
	sc := bufio.NewScanner(bytes.NewReader(body))
	for sc.Scan() {
		var l struct{ Payload string }
		if err := json.Unmarshal(sc.Bytes(), &l); err != nil {
			t.Fatalf("object %s: bad JSON line %q: %v", key, sc.Text(), err)
		}
		n, err := strconv.Atoi(strings.TrimPrefix(l.Payload, "line-"))
		if err != nil || !strings.HasPrefix(l.Payload, "line-") {
			t.Fatalf("object %s: unexpected payload %q", key, l.Payload)
		}
		out = append(out, n)
	}
	return out
}

// keyTime is the UnixNano the destination put in the object key.
func keyTime(t *testing.T, key string) time.Time {
	base := strings.TrimSuffix(path.Base(key), ".json")
	n, err := strconv.ParseInt(base, 10, 64)
	if err != nil {
		t.Fatalf("object key %q does not end in <UnixNano>.json", key)
	}
	return time.Unix(0, n)
}

// runOf returns the index of the run that wrote an object at time at.
func (h *harness) runOf(at time.Time) int {
	r := -1
	for i, p := range h.runs {
		if !at.Before(p.started) {
			r = i
		}
	}
	return r
}

// objectsSince counts the objects written by run p.
func (h *harness) objectsOf(p *conduitProc) int {
	n := 0
	for key := range h.scan() {
		if !keyTime(h.t, key).Before(p.started) {
			n++
		}
	}
	return n
}

// waitObjects waits until run p wrote more than have objects, or the deadline.
func (h *harness) waitObjects(p *conduitProc, have int, within time.Duration) bool {
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		select {
		case err := <-p.done:
			h.t.Fatalf("conduit exited on its own: %v (log %s)", err, p.log)
		default:
		}
		if h.objectsOf(p) > have {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return false
}

type killPoint int

const (
	// killStartup: while Conduit starts, usually before the pipeline wrote.
	killStartup killPoint = iota
	// killAfterWrite: as soon as an object of this run is visible.
	killAfterWrite
	// killMidStream: a random time into a running stream, so the kill lands
	// between writes, inside a batch, or inside a PutObject.
	killMidStream
)

func (k killPoint) String() string {
	return [...]string{"startup", "after-write", "mid-stream"}[k]
}

func writeInput(t *testing.T, dir string, n int) string {
	p := filepath.Join(dir, "input.txt")
	var b strings.Builder
	for i := 1; i <= n; i++ {
		fmt.Fprintf(&b, "line-%06d\n", i)
	}
	if err := os.WriteFile(p, []byte(b.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

// TestDestination_SIGKILL kills Conduit repeatedly while it streams a file of
// numbered lines into the S3 destination and checks invariants 1, 2 and 3 on
// the bucket (see doc.go).
func TestDestination_SIGKILL(t *testing.T) {
	const lines = 30000
	seed := int64(20261014)
	if s := os.Getenv(envSeed); s != "" {
		v, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			t.Fatalf("bad %s: %v", envSeed, err)
		}
		seed = v
	}
	rng := rand.New(rand.NewSource(seed))
	t.Logf("kill schedule seed %d (%s to replay)", seed, envSeed)

	h := newHarness(t)
	input := writeInput(t, h.dir, lines)
	// The rate limit stretches the stream over about 30s so kills land
	// mid-stream; batches of 50 make one object hold several records, so a
	// kill can fall inside a batch.
	h.writePipeline(input, map[string]string{
		"sdk.batch.size":     "50",
		"sdk.batch.delay":    "20ms",
		"sdk.rate.perSecond": "1000",
	})
	// Conduit persists source positions in the background, about once a
	// second, so a run killed sooner resumes where the previous one did. That
	// is correct (at-least-once) but checks nothing new; the kills that matter
	// let a run stream for a few seconds first.
	const minStream = 2500 * time.Millisecond

	schedule := []killPoint{killStartup, killAfterWrite, killMidStream, killAfterWrite, killMidStream, killStartup, killMidStream, killAfterWrite}
	for i, kp := range schedule {
		p := h.start()
		switch kp {
		case killStartup:
			time.Sleep(time.Duration(100+rng.Intn(400)) * time.Millisecond)
		case killAfterWrite:
			if !h.waitObjects(p, 0, 60*time.Second) {
				t.Fatalf("cycle %d: run wrote nothing within 60s (log %s)", i, p.log)
			}
			time.Sleep(minStream + time.Duration(rng.Intn(1000))*time.Millisecond)
			// kill the moment the next object becomes visible
			if !h.waitObjects(p, h.objectsOf(p), 30*time.Second) {
				t.Fatalf("cycle %d: run stopped writing (log %s)", i, p.log)
			}
		case killMidStream:
			if !h.waitObjects(p, 0, 60*time.Second) {
				t.Fatalf("cycle %d: run wrote nothing within 60s (log %s)", i, p.log)
			}
			time.Sleep(minStream + time.Duration(rng.Intn(2000))*time.Millisecond)
		}
		h.kill(p)
		t.Logf("cycle %d: SIGKILL at %s, %v after start", i, kp, time.Since(p.started).Round(time.Millisecond))
	}

	// Final run: drain everything, then stop gracefully.
	final := h.start()
	deadline := time.Now().Add(4 * time.Minute)
	for coverage(h.scan(), lines) != lines {
		if time.Now().After(deadline) {
			break // invariant 3 below reports what is missing
		}
		select {
		case err := <-final.done:
			t.Fatalf("final run exited on its own: %v (log %s)", err, final.log)
		default:
		}
		time.Sleep(200 * time.Millisecond)
	}
	_ = final.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-final.done:
	case <-time.After(60 * time.Second):
		t.Errorf("conduit did not stop within 60s of SIGTERM (invariant 7); known engine/SDK hang with standalone destinations that batch, ConduitIO/conduit#3092")
		h.kill(final)
	}

	h.check(lines)
}

func coverage(objs map[string][]int, n int) int {
	seen := make([]bool, n+1)
	c := 0
	for _, ls := range objs {
		for _, l := range ls {
			if l >= 1 && l <= n && !seen[l] {
				seen[l] = true
				c++
			}
		}
	}
	return c
}

// check evaluates the invariants on the bucket.
//
//nolint:gocyclo // one linear list of invariant checks; splitting it hides the order they are reported in
func (h *harness) check(n int) {
	t := h.t
	objs := h.scan()
	// lines written per run
	perRun := make([][]int, len(h.runs))
	for key, ls := range objs {
		r := h.runOf(keyTime(t, key))
		if r < 0 {
			t.Fatalf("object %s predates the first run", key)
		}
		perRun[r] = append(perRun[r], ls...)
	}

	// Invariant 3: every line at least once.
	if c := coverage(objs, n); c != n {
		var missing []int
		seen := map[int]bool{}
		for _, ls := range objs {
			for _, l := range ls {
				seen[l] = true
			}
		}
		for l := 1; l <= n && len(missing) < 20; l++ {
			if !seen[l] {
				missing = append(missing, l)
			}
		}
		t.Errorf("invariant 3: %d of %d lines are not in the bucket after the final run (first missing: %v)", n-c, n, missing)
	}

	// Invariants 1 and 2, per kill: the run after the kill resumes at the
	// persisted position; everything before it must be stored by earlier runs.
	stored := map[int]int{} // line -> times stored by runs up to r
	prevResume := 0
	totalRedelivered := 0
	for r := 0; r < len(h.runs)-1; r++ {
		for _, l := range perRun[r] {
			stored[l]++
		}
		next := perRun[r+1]
		if len(next) == 0 {
			t.Logf("kill %d: the next run wrote nothing before it was killed; its resume position is checked at the next kill", r)
			continue
		}
		first := next[0]
		for _, l := range next {
			if l < first {
				first = l
			}
		}
		resume := first - 1 // lines 1..resume were acked (persisted) before kill r
		maxStored := 0
		for l := range stored {
			if l > maxStored {
				maxStored = l
			}
		}
		for l := 1; l <= resume; l++ {
			if stored[l] == 0 {
				t.Errorf("invariant 1: kill %d: line %d is before the persisted position (the restart resumed at line %d) but no object written before the kill holds it", r, l, first)
				break
			}
		}
		if resume < prevResume {
			t.Errorf("invariant 2: kill %d: the persisted position went back from line %d to %d", r, prevResume, resume)
		}
		if first > maxStored+1 {
			t.Errorf("kill %d: the restart resumed at line %d but only lines up to %d were stored: lines skipped", r, first, maxStored)
		}
		redelivered := 0
		for _, l := range next {
			if stored[l] > 0 {
				redelivered++
			}
		}
		totalRedelivered += redelivered
		t.Logf("kill %d: resumed after line %d (stored up to %d), %d records redelivered", r, resume, maxStored, redelivered)
		prevResume = resume
	}
	dups := 0
	for _, ls := range objs {
		dups += len(ls)
	}
	dups -= coverage(objs, n)
	t.Logf("summary: %d lines, %d objects, %d runs, %d duplicate copies (%d redelivered right after a restart)", n, len(objs), len(h.runs), dups, totalRedelivered)
	if dups > n {
		t.Errorf("more duplicate copies (%d) than lines (%d): a restart probably started over", dups, n)
	}
}

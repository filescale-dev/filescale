package store

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"filescale/internal/db"
	"filescale/internal/erasure"
	"filescale/internal/metrics"
	"filescale/internal/shard"
)

// metric is one series' value in what write produces, -1 if it's missing.
func metric(write func(*metrics.Writer), series string) float64 {
	var b strings.Builder
	write(metrics.NewWriter(&b))
	for _, line := range strings.Split(b.String(), "\n") {
		if v, ok := strings.CutPrefix(line, series+" "); ok {
			f, _ := strconv.ParseFloat(v, 64)
			return f
		}
	}
	return -1
}

// diskLabels are d's labels in the store's metrics.
func diskLabels(d *disk) string { return fmt.Sprintf(`{store="%s",dir="%s"}`, d.id, d.dir) }

const token = "t0ken"

func TestPutGet(t *testing.T) {
	s, srv, _ := newStore(t, 5)
	fid := newID(t)
	stream := blocks(random(100_000))
	u := shardURL(srv, s.IDs()[0], fid, 3)
	if code, b := do(t, "PUT", u, stream); code != http.StatusCreated {
		t.Fatalf("PUT: %d %s", code, b)
	}
	if code, b := do(t, "GET", u, nil); code != http.StatusOK || !bytes.Equal(b, stream) {
		t.Errorf("GET: %d, %d bytes, want %d", code, len(b), len(stream))
	}
	second := erasure.BlockSize + 4
	if code, b := do(t, "GET", u, nil, "Range", fmt.Sprintf("bytes=%d-", second)); code != http.StatusPartialContent || !bytes.Equal(b, stream[second:]) {
		t.Errorf("GET second block: %d, %d bytes", code, len(b))
	}
	onDisk, err := os.ReadFile(s.list[0].path(fid, 3))
	if err != nil || !bytes.Equal(onDisk, append([]byte(shard.Magic), stream...)) {
		t.Errorf("shard file isn't magic + block stream: %v", err)
	}
	for series, want := range map[string]int{
		`filescale_requests_total{method="PUT",code="201"}`:     1,
		`filescale_requests_total{method="GET",code="200"}`:     1,
		`filescale_requests_total{method="GET",code="206"}`:     1,
		`filescale_received_bytes_total`:                        len(stream),
		`filescale_sent_bytes_total`:                            2*len(stream) - second,
		"filescale_disk_inflight_bytes" + diskLabels(s.list[0]): 0,
		"filescale_disk_failed" + diskLabels(s.list[0]):         0,
	} {
		if got := metric(s.Metrics, series); got != float64(want) {
			t.Errorf("%s = %v, want %d", series, got, want)
		}
	}
	if metric(s.Metrics, "filescale_disk_free_bytes"+diskLabels(s.list[0])) <= 0 {
		t.Error("no free bytes reported")
	}
}

func TestPutRejects(t *testing.T) {
	s, srv, _ := newStore(t, 5)
	sid, fid := s.IDs()[0], newID(t)
	u := shardURL(srv, sid, fid, 0)
	path := s.list[0].path(fid, 0)
	stream := blocks(random(1000))

	if code, _ := do(t, "PUT", u, stream, "Authorization", ""); code != http.StatusUnauthorized {
		t.Errorf("no token: %d", code)
	}
	if code, _ := do(t, "PUT", shardURL(srv, newID(t), fid, 0), stream); code != http.StatusNotFound {
		t.Errorf("unknown store: %d", code)
	}
	if code, _ := do(t, "PUT", srv.URL+"/v1/x/y/z", stream); code != http.StatusBadRequest {
		t.Errorf("bad path: %d", code)
	}
	if code, _ := do(t, "PUT", u, []byte{1, 2}); code != http.StatusBadRequest {
		t.Errorf("2-byte shard: %d", code)
	}
	bad := bytes.Clone(stream)
	bad[10] ^= 1
	if code, _ := do(t, "PUT", u, bad); code != http.StatusBadRequest || exists(path) {
		t.Errorf("bad CRC: %d, file left behind: %v", code, exists(path))
	}

	req, _ := http.NewRequest("PUT", u, io.NopCloser(bytes.NewReader(stream))) // unknown length, sent chunked
	req.Header.Set("Authorization", "Bearer "+token)
	if resp, err := http.DefaultClient.Do(req); err != nil || resp.StatusCode != http.StatusLengthRequired {
		t.Errorf("chunked: %v, %v", resp, err)
	}

	req, _ = http.NewRequest("PUT", u, bytes.NewReader(stream))
	req.ContentLength += 100 // the body ends early
	req.Header.Set("Authorization", "Bearer "+token)
	if resp, err := http.DefaultClient.Do(req); err == nil {
		t.Errorf("cut-off body: %s", resp.Status)
	}
	waitFor(t, "cut-off shard removed", func() bool { return !exists(path) })

	// A failed overwrite must leave an existing live shard intact.
	if code, b := do(t, "PUT", u, stream); code != http.StatusCreated {
		t.Fatalf("PUT good: %d %s", code, b)
	}
	if code, _ := do(t, "PUT", u, bad); code != http.StatusBadRequest {
		t.Fatalf("bad overwrite: %d", code)
	}
	if code, b := do(t, "GET", u, nil); code != http.StatusOK || !bytes.Equal(b, stream) {
		t.Errorf("live shard damaged after failed overwrite: %d, %d bytes", code, len(b))
	}
}

// A serve that stops sending mid-shard is dropped at the stall timer, its partial shard removed.
func TestPutStalledClient(t *testing.T) {
	if testing.Short() {
		t.Skip("waits out the stall timer")
	}
	s, srv, _ := newStore(t, 5)
	fid := newID(t)
	stream := blocks(random(100_000))
	c, err := net.Dial("tcp", srv.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	start := time.Now()
	fmt.Fprintf(c, "PUT /v1/%s/%s/0 HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer %s\r\nContent-Length: %d\r\n\r\n",
		s.IDs()[0], fid, token, len(stream))
	c.Write(stream[:1000]) // then nothing, the connection stays up
	c.SetReadDeadline(time.Now().Add(shard.Stall + 15*time.Second))
	resp, err := http.ReadResponse(bufio.NewReader(c), nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if took := time.Since(start); resp.StatusCode != http.StatusBadRequest || took < shard.Stall-time.Second {
		t.Errorf("%s after %v, want 400 at the stall timer", resp.Status, took)
	}
	if exists(s.list[0].path(fid, 0)) {
		t.Error("partial shard left behind")
	}
}

// Run heartbeats every disk at once and stops with its context.
func TestRun(t *testing.T) {
	s, _, m := newStore(t, 5)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		s.Run(ctx, "10.0.0.1:8081")
		close(done)
	}()
	waitFor(t, "first heartbeat", func() bool {
		stores, _ := m.Stores(context.Background())
		return len(stores) == 1 && stores[0].Addr == "10.0.0.1:8081"
	})
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run kept going after its context ended")
	}
}

func TestMinFree(t *testing.T) {
	s, srv, _ := newStore(t, 100)
	u := shardURL(srv, s.IDs()[0], newID(t), 0)
	if code, _ := do(t, "PUT", u, blocks(random(10))); code != http.StatusInsufficientStorage {
		t.Errorf("upload into the reserve: %d", code)
	}
	if code, _ := do(t, "PUT", u+"?repair=1", blocks(random(10))); code != http.StatusCreated {
		t.Errorf("repair into the reserve: %d", code)
	}
}

// Early answers reach a client that waits for 100 Continue before sending the body, as the serve
// does: an answer that first waited to read the body would never come.
func TestPutRejectsBeforeContinue(t *testing.T) {
	s, srv, _ := newStore(t, 100)
	u := shardURL(srv, s.IDs()[0], newID(t), 0)
	for _, tt := range []struct {
		url, auth string
		code      int
	}{
		{u, "Bearer " + token, http.StatusInsufficientStorage},
		{u, "Bearer wrong", http.StatusUnauthorized},
		{shardURL(srv, newID(t), newID(t), 0), "Bearer " + token, http.StatusNotFound},
	} {
		if code := expectPut(t, tt.url, 1<<20, "Authorization", tt.auth); code != tt.code {
			t.Errorf("PUT %s with %q: %d, want %d", tt.url, tt.auth, code, tt.code)
		}
	}
}

// expectPut sends a PUT with Expect: 100-continue and a body that never comes,
// and returns the status, 0 if none came within 2s.
func expectPut(t *testing.T, url string, size int64, hdr ...string) int {
	t.Helper()
	pr, pw := io.Pipe()
	defer pw.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "PUT", url, pr)
	req.ContentLength = size
	req.Header.Set("Expect", "100-continue")
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	c := &http.Client{Transport: &http.Transport{ExpectContinueTimeout: time.Minute}}
	resp, err := c.Do(req)
	if err != nil {
		t.Logf("PUT %s: %v", url, err)
		return 0
	}
	resp.Body.Close()
	return resp.StatusCode
}

func TestHeartbeat(t *testing.T) {
	s, srv, m := newStore(t, 5)
	ctx := context.Background()
	s.Heartbeat(ctx, "10.0.0.1:8081")
	stores, _ := m.Stores(ctx)
	if len(stores) != 1 || stores[0].ID != s.IDs()[0] || stores[0].Addr != "10.0.0.1:8081" || time.Since(stores[0].LastSeen) > time.Minute {
		t.Fatalf("stores = %+v", stores)
	}

	// a disk that fails its probe stops heartbeating and refuses shards
	seen := stores[0].LastSeen
	os.RemoveAll(s.list[0].dir)
	s.Heartbeat(ctx, "10.0.0.1:8081")
	if stores, _ := m.Stores(ctx); !stores[0].LastSeen.Equal(seen) {
		t.Error("failed disk still heartbeats")
	}
	if code, _ := do(t, "PUT", shardURL(srv, s.IDs()[0], newID(t), 0), blocks(random(10))); code != http.StatusServiceUnavailable {
		t.Errorf("PUT to a failed disk: %d", code)
	}
}

func TestCleanup(t *testing.T) {
	s, _, m := newStore(t, 5)
	d := s.list[0]
	ctx := context.Background()
	old := time.Now().Add(-25 * time.Hour)
	orphanOld := writeShardFile(t, d, newID(t), 0, old)
	orphanYoung := writeShardFile(t, d, newID(t), 0, time.Now())
	keptID := newID(t)
	kept := writeShardFile(t, d, keptID, 2, old)
	m.PutShard(ctx, db.Shard{StoreID: d.id, FileID: keptID, Index: 2, Name: "kept"})

	if err := s.Cleanup(ctx); err != nil {
		t.Fatal(err)
	}
	if exists(orphanOld) {
		t.Error("old orphan not removed")
	}
	if !exists(orphanYoung) {
		t.Error("young orphan removed, it may be an upload still committing")
	}
	if !exists(kept) {
		t.Error("indexed shard removed")
	}
	if got := metric(s.Metrics, "filescale_cleanup_removed_total"+diskLabels(d)); got != 1 {
		t.Errorf("removed %v, want 1", got)
	}
	if got := metric(s.Metrics, "filescale_cleanup_last_pass_timestamp_seconds"+diskLabels(d)); time.Since(time.Unix(int64(got), 0)) > time.Minute {
		t.Errorf("last cleanup pass %v", got)
	}
}

// staleDB is a Mem without the rows a replica hasn't got yet: queries at db.All see them, others don't.
type staleDB struct {
	*db.Mem
	fail bool // the ALL query fails, an unreachable replica
}

func (w *staleDB) Shards(ctx context.Context, store db.UUID, b int) ([]db.Shard, error) {
	if !db.IsAll(ctx) {
		return nil, nil
	}
	if w.fail {
		return nil, errors.New("all: unreachable replica")
	}
	return w.Mem.Shards(ctx, store, b)
}

func (w *staleDB) File(ctx context.Context, name string) (*db.File, error) {
	if !db.IsAll(ctx) {
		return nil, nil
	}
	return w.Mem.File(ctx, name)
}

func newStaleStore(t *testing.T) (*Store, *staleDB) {
	m := &staleDB{Mem: db.NewMem()}
	s, err := New(Config{Dirs: []string{t.TempDir()}, MinFree: 5, Token: token, DB: m, Client: shard.NewClient(token)})
	if err != nil {
		t.Fatal(err)
	}
	return s, m
}

// A row a replica hasn't got yet reads as empty: cleanup only deletes what every replica lacks.
func TestCleanupStaleRead(t *testing.T) {
	s, m := newStaleStore(t)
	d := s.list[0]
	ctx := context.Background()
	old := time.Now().Add(-25 * time.Hour)
	keptID, orphanID := newID(t), newID(t)
	kept := writeShardFile(t, d, keptID, 0, old)
	orphan := writeShardFile(t, d, orphanID, 0, old)
	m.Mem.PutShard(ctx, db.Shard{StoreID: d.id, FileID: keptID, Index: 0, Name: "kept"})

	if err := s.Cleanup(ctx); err != nil {
		t.Fatal(err)
	}
	if !exists(kept) {
		t.Error("deleted on a stale read")
	}
	if exists(orphan) {
		t.Error("orphan kept, every replica lacks its row")
	}
}

// An ALL read that fails skips the bucket, and the pass still ends.
func TestCleanupAllFails(t *testing.T) {
	s, m := newStaleStore(t)
	d := s.list[0]
	old := time.Now().Add(-25 * time.Hour)
	id := newID(t)
	other := id
	other[0], other[1] = id[0], id[1] // same bucket, so not a lone file
	first := writeShardFile(t, d, id, 0, old)
	second := writeShardFile(t, d, other, 1, old)
	m.fail = true

	if err := s.Cleanup(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !exists(first) || !exists(second) {
		t.Error("removed without every replica answering")
	}
}

// A bucket full of old files and no rows at all is missing metadata, not garbage.
func TestCleanupSkipsEmptyBucket(t *testing.T) {
	s, _ := newStaleStore(t)
	d := s.list[0]
	old := time.Now().Add(-25 * time.Hour)
	id := newID(t)
	other := id
	other[0], other[1] = id[0], id[1]
	first := writeShardFile(t, d, id, 0, old)
	second := writeShardFile(t, d, other, 1, old)

	if err := s.Cleanup(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !exists(first) || !exists(second) {
		t.Error("emptied a bucket with no rows")
	}
}

func TestScrubStaleRead(t *testing.T) {
	s, m := newStaleStore(t)
	d := s.list[0]
	ctx := context.Background()
	id := newID(t)
	writeShardFile(t, d, id, 0, time.Now())
	m.Mem.PutFile(ctx, &db.File{Name: "f", ID: id, Size: 100, DataShards: 1, Stores: []db.UUID{d.id}})
	m.Mem.Clock = func() time.Time { return time.Now().Add(-25 * time.Hour) }
	m.Mem.PutShard(ctx, db.Shard{StoreID: d.id, FileID: id, Name: "f"})
	m.Mem.Clock = nil

	if err := s.Scrub(ctx, false, time.Time{}); err != nil {
		t.Fatal(err)
	}
	if !hasRow(m.Mem, d.id, id) {
		t.Error("row dropped on a stale read")
	}
}

func TestScrubDropsOrphanRows(t *testing.T) {
	s, _, m := newStore(t, 5)
	d := s.list[0]
	ctx := context.Background()
	oldID, youngID := newID(t), newID(t)
	writeShardFile(t, d, oldID, 0, time.Now())
	writeShardFile(t, d, youngID, 0, time.Now())
	m.Clock = func() time.Time { return time.Now().Add(-25 * time.Hour) }
	m.PutShard(ctx, db.Shard{StoreID: d.id, FileID: oldID, Name: "overwritten"})
	m.Clock = nil
	m.PutShard(ctx, db.Shard{StoreID: d.id, FileID: youngID, Name: "still-committing"})

	if err := s.Scrub(ctx, false, time.Time{}); err != nil {
		t.Fatal(err)
	}
	if hasRow(m, d.id, oldID) {
		t.Error("old orphan row kept")
	}
	if !hasRow(m, d.id, youngID) {
		t.Error("young row dropped")
	}
	if got := metric(s.Metrics, "filescale_scrub_orphan_rows_total"+diskLabels(d)); got != 1 {
		t.Errorf("orphan rows %v, want 1", got)
	}
}

// A disk with shards but no stores row is skipped: a wrong Cassandra, or a decommissioned disk.
func TestUnknownDisk(t *testing.T) {
	m := db.NewMem()
	ctx := context.Background()
	open := func(dirs ...string) (*Store, error) {
		return New(Config{Dirs: dirs, MinFree: 5, Token: token, DB: m, Client: shard.NewClient(token)})
	}
	known, unknown, empty := t.TempDir(), t.TempDir(), t.TempDir()
	s, err := open(known, unknown)
	if err != nil {
		t.Fatal(err)
	}
	s.Heartbeat(ctx, "10.0.0.1:8081")
	for _, d := range s.list {
		writeShardFile(t, d, newID(t), 0, time.Now())
	}
	m.DeleteStore(ctx, s.list[1].id)

	if s, err = open(known, unknown, empty); err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, d := range s.list {
		got = append(got, d.dir)
	}
	if want := []string{known, empty}; !slices.Equal(got, want) {
		t.Errorf("disks = %v, want %v", got, want)
	}
	if _, err := open(unknown); err == nil {
		t.Error("started with only an unknown disk")
	}
}

// A paced scrub spreads its reads over the pass, an unpaced one runs at full speed.
func TestScrubPaced(t *testing.T) {
	s, _, m := newStore(t, 5)
	d := s.list[0]
	ctx := context.Background()
	fid := newID(t)
	path := d.path(fid, 0)
	f, err := create(path)
	if err != nil {
		t.Fatal(err)
	}
	f.Write(append([]byte(shard.Magic), blocks(random(200<<10))...)) // 4 blocks
	f.Close()
	m.PutFile(ctx, &db.File{Name: "paced", ID: fid, Size: 200 << 10, DataShards: 1, Stores: []db.UUID{d.id}})
	m.PutShard(ctx, db.Shard{StoreID: d.id, FileID: fid, Index: 0, Name: "paced"})

	// bucket b is due at start + pass × b/65,536: the last one ~1.5s in, whatever the disk holds
	pass := 1500 * time.Millisecond
	start := time.Now()
	if err := s.scrubDisk(ctx, d, pace{start: start, pass: pass}, time.Time{}); err != nil {
		t.Fatal(err)
	}
	if took := time.Since(start); took < pass*99/100 {
		t.Errorf("paced scrub took %v, the pace wasn't honored", took)
	}
	// resuming at bucket 3/4 of the way in, rebased to now: only the last quarter is left
	start = time.Now()
	resumed := pace{start: start.Add(-pass * 3 / 4), pass: pass, next: db.Buckets * 3 / 4}
	if err := s.scrubDisk(ctx, d, resumed, time.Time{}); err != nil {
		t.Fatal(err)
	}
	if took := time.Since(start); took < pass/5 || took > pass/2 {
		t.Errorf("resumed scrub took %v, want ~%v", took, pass/4)
	}
	start = time.Now()
	if err := s.scrubDisk(ctx, d, pace{}, time.Time{}); err != nil {
		t.Fatal(err)
	}
	if took := time.Since(start); took > 500*time.Millisecond {
		t.Errorf("unpaced scrub took %v", took)
	}
}

// A paced pass saves its progress every 256 buckets, a restart resumes from there, rebased to now
// when it fell behind or the scrub timing changed, and the last whole pass's end survives for the metric.
func TestScrubProgress(t *testing.T) {
	s, _, _ := newStore(t, 5)
	d := s.list[0]
	ctx := context.Background()

	if st := loadScrub(d, DefaultScrubPass, DefaultScrubGap); st.next != 0 || !st.last.IsZero() || time.Since(st.start) > time.Second {
		t.Fatalf("fresh disk: %+v, want a new pass from now", st)
	}

	// a pass started long ago, saved at bucket 512: resumes at 512, due now, not in a rush to catch up
	var saved []int
	ctx2, cancel := context.WithCancel(ctx)
	p := pace{save: func(next int) { // full speed up to the cancel
		saved = append(saved, next)
		saveScrub(d, scrubState{start: time.Now().Add(-48 * time.Hour), next: next, last: time.Unix(1000, 0)})
		if next == 512 {
			cancel()
		}
	}}
	if err := s.scrubDisk(ctx2, d, p, time.Time{}); err == nil {
		t.Fatal("cancelled scrub returned no error")
	}
	if !slices.Equal(saved, []int{256, 512}) {
		t.Errorf("saved at %v, want [256 512]", saved)
	}
	d.scrubbed.Store(0)
	st := loadScrub(d, DefaultScrubPass, DefaultScrubGap)
	if st.next != 512 {
		t.Errorf("resumed at bucket %d, want 512", st.next)
	}
	if due := st.start.Add(bucketOffset(DefaultScrubPass, 512)); time.Until(due) > time.Second || time.Since(due) > time.Second {
		t.Errorf("bucket 512 due at %v, want now", due)
	}
	if got := metric(s.Metrics, "filescale_scrub_last_pass_timestamp_seconds"+diskLabels(d)); got != 1000 {
		t.Errorf("last pass %v after a restart, want 1000", got)
	}

	// lengthening pass mid-pass rebases bucket 512 to now instead of sleeping hours
	saveScrub(d, scrubState{start: time.Now().Add(-bucketOffset(time.Hour, 512)), next: 512})
	st = loadScrub(d, DefaultScrubPass, DefaultScrubGap)
	if due := st.start.Add(bucketOffset(DefaultScrubPass, 512)); time.Until(due) > time.Second || time.Since(due) > time.Second {
		t.Errorf("lengthened pass: bucket 512 due at %v, want now", due)
	}

	// same pass restarted right after a save keeps the remaining sub-step wait
	kept := time.Now().Add(bucketOffset(DefaultScrubPass, 1)/2 - bucketOffset(DefaultScrubPass, 512))
	saveScrub(d, scrubState{start: kept, next: 512})
	if st := loadScrub(d, DefaultScrubPass, DefaultScrubGap); !st.start.Equal(time.Unix(0, kept.UnixNano())) {
		t.Errorf("quick restart: start = %v, want %v", st.start, kept)
	}

	// between passes: the next one starts after the gap, not at once
	next := time.Now().Add(DefaultScrubGap)
	saveScrub(d, scrubState{start: next, last: time.Now()})
	if st := loadScrub(d, DefaultScrubPass, DefaultScrubGap); st.next != 0 || !st.start.Equal(time.Unix(0, next.UnixNano())) {
		t.Errorf("between passes: %+v, want bucket 0 at %v", st, next)
	}
	// shortening gap clamps a saved wait to the new gap
	if st := loadScrub(d, DefaultScrubPass, time.Hour); st.next != 0 || time.Until(st.start) > time.Hour+time.Second || time.Until(st.start) < 59*time.Minute {
		t.Errorf("shortened gap: %+v, want ~1h from now", st)
	}
	if st := loadScrub(d, DefaultScrubPass, 0); st.next != 0 || time.Until(st.start) > time.Second || time.Since(st.start) > time.Second {
		t.Errorf("zero gap: %+v, want now", st)
	}

	for _, bad := range []string{"", "garbage", "1 70000 0", "1 -1 0"} {
		os.WriteFile(filepath.Join(d.dir, "scrub"), []byte(bad), 0o644)
		if st := loadScrub(d, DefaultScrubPass, DefaultScrubGap); st.next != 0 || time.Since(st.start) > time.Second {
			t.Errorf("progress %q: %+v, want a new pass from now", bad, st)
		}
	}
}

func TestScrubLoopTiming(t *testing.T) {
	m := db.NewMem()
	base := Config{Dirs: []string{t.TempDir()}, MinFree: 5, Token: token, DB: m, Client: shard.NewClient(token)}
	badPass := base
	badPass.ScrubPass = -time.Second
	if _, err := New(badPass); err == nil {
		t.Error("negative ScrubPass: want error")
	}
	badGap := base
	badGap.ScrubGap = -time.Second
	if _, err := New(badGap); err == nil {
		t.Error("negative ScrubGap: want error")
	}
	def, err := New(base)
	if err != nil {
		t.Fatal(err)
	}
	if def.ScrubPass != DefaultScrubPass {
		t.Errorf("default ScrubPass = %v, want %v", def.ScrubPass, DefaultScrubPass)
	}

	pass, gap := 300*time.Millisecond, 200*time.Millisecond
	cfg := Config{Dirs: []string{t.TempDir()}, MinFree: 5, Token: token, DB: m, Client: shard.NewClient(token), ScrubPass: pass, ScrubGap: gap}
	s, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if err := s.Scrub(context.Background(), true, time.Time{}); err != nil {
		t.Fatal(err)
	}
	if took := time.Since(start); took < pass*95/100 || took > 5*time.Second {
		t.Errorf("paced Scrub took %v, want ~%v", took, pass)
	}

	d := s.list[0]
	d.scrubbed.Store(0)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		s.scrubLoop(ctx, d)
		close(done)
	}()
	deadline := time.Now().Add(10 * time.Second)
	var first, second int64
	for time.Now().Before(deadline) {
		v := d.scrubbed.Load()
		if first == 0 && v > 0 {
			first = v
		} else if first > 0 && v > first {
			second = v
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("scrubLoop did not stop on cancel")
	}
	if first == 0 || second == 0 {
		t.Fatalf("scrubLoop passes: first=%d second=%d, want two passes", first, second)
	}
	if diff := time.Duration(second - first); diff < (pass+gap)*95/100 {
		t.Errorf("between pass ends: %v, want >= ~%v", diff, pass+gap)
	}
}

// Scrub drops rows that don't match the file's layout — an index past the shards, or a
// store the file doesn't use — old enough not to be an upload still committing.
func TestScrubOrphanRows(t *testing.T) {
	s, _, m := newStore(t, 5)
	d := s.list[0]
	ctx := context.Background()
	fid, gid, live := newID(t), newID(t), newID(t)
	m.PutFile(ctx, &db.File{Name: "f", ID: fid, Size: 100, DataShards: 1, Stores: []db.UUID{newID(t)}})
	m.PutFile(ctx, &db.File{Name: "g", ID: gid, Size: 100, DataShards: 1, Stores: []db.UUID{newID(t)}})
	m.PutFile(ctx, &db.File{Name: "h", ID: live, Size: 100, DataShards: 1, Stores: []db.UUID{d.id}})
	writeShardFile(t, d, live, 0, time.Now()) // the one valid row has its shard
	m.Clock = func() time.Time { return time.Now().Add(-25 * time.Hour) }
	m.PutShard(ctx, db.Shard{StoreID: d.id, FileID: fid, Index: 5, Name: "f"}) // index past 1+0 shards
	m.PutShard(ctx, db.Shard{StoreID: d.id, FileID: gid, Index: 0, Name: "g"}) // d.id holds no copy of g
	m.PutShard(ctx, db.Shard{StoreID: d.id, FileID: live, Index: 0, Name: "h"})
	m.Clock = nil
	if err := s.Scrub(ctx, false, time.Time{}); err != nil {
		t.Fatal(err)
	}
	if hasRow(m, d.id, fid) || hasRow(m, d.id, gid) {
		t.Error("a row outside the file's layout kept")
	}
	if !hasRow(m, d.id, live) {
		t.Error("the valid row dropped")
	}
}

func TestScrubSkipsCorruptMetadata(t *testing.T) {
	for _, tc := range []struct {
		name   string
		stores func(db.UUID) []db.UUID
	}{
		{"doubled", func(id db.UUID) []db.UUID {
			return []db.UUID{newID(t), newID(t), newID(t), newID(t), newID(t), id, newID(t), newID(t), newID(t), newID(t)}
		}},
		{"short", func(id db.UUID) []db.UUID { return []db.UUID{id, newID(t)} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _, m := newStore(t, 5)
			d := s.list[0]
			ctx := context.Background()
			fid := newID(t)
			path := writeShardFile(t, d, fid, 0, time.Now().Add(-25*time.Hour))
			m.PutFile(ctx, &db.File{Name: "bad", ID: fid, Size: 100, DataShards: 1, ParityShards: 4, Stores: tc.stores(d.id)})
			m.Clock = func() time.Time { return time.Now().Add(-25 * time.Hour) }
			m.PutShard(ctx, db.Shard{StoreID: d.id, FileID: fid, Index: 0, Name: "bad"})
			m.Clock = nil

			if err := s.Scrub(ctx, false, time.Time{}); err != nil {
				t.Fatal(err)
			}
			if !hasRow(m, d.id, fid) {
				t.Error("scrub dropped shards_by_store row of corrupt file")
			}
			if !exists(path) {
				t.Error("scrub removed shard file of corrupt file")
			}
			if got := metric(s.Metrics, "filescale_scrub_orphan_rows_total"+diskLabels(d)); got != 0 {
				t.Errorf("orphan rows = %v, want 0", got)
			}
		})
	}
}

func TestScrubFailedMissingLeavesNoFile(t *testing.T) {
	s, _, m := newStore(t, 5)
	d := s.list[0]
	ctx := context.Background()
	fid := newID(t)
	m.PutFile(ctx, &db.File{Name: "m", ID: fid, Size: 100, DataShards: 1, Stores: []db.UUID{d.id}})
	m.PutShard(ctx, db.Shard{StoreID: d.id, FileID: fid, Index: 0, Name: "m"})
	if err := s.Scrub(ctx, false, time.Time{}); err != nil {
		t.Fatal(err)
	}
	if exists(d.path(fid, 0)) {
		t.Error("failed rebuild of missing shard left a file behind")
	}
}

// A shard file without the magic is a 500, and GETs want the store's token too.
func TestGetCorrupt(t *testing.T) {
	s, srv, _ := newStore(t, 5)
	fid := newID(t)
	p := s.list[0].path(fid, 0)
	f, err := create(p) // makes the prefix dirs
	if err != nil {
		t.Fatal(err)
	}
	f.WriteString("junk")
	f.Close()
	if code, _ := do(t, "GET", shardURL(srv, s.IDs()[0], fid, 0), nil); code != http.StatusInternalServerError {
		t.Errorf("junk shard: %d", code)
	}
	req, _ := http.NewRequest("GET", shardURL(srv, s.IDs()[0], newID(t), 0), nil)
	if resp, err := http.DefaultClient.Do(req); err != nil || resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("GET without a token: %v, %v", resp, err)
	}
}

func hasRow(m *db.Mem, store, file db.UUID) bool {
	rows, _ := m.Shards(context.Background(), store, db.Bucket(file))
	return slices.ContainsFunc(rows, func(r db.Shard) bool { return r.FileID == file })
}

func newStore(t *testing.T, minFree int64) (*Store, *httptest.Server, *db.Mem) {
	m := db.NewMem()
	s, err := New(Config{Dirs: []string{t.TempDir()}, MinFree: minFree, Token: token, DB: m, Client: shard.NewClient(token)})
	if err != nil {
		t.Fatal(err)
	}
	srv := newServer(t, s)
	t.Cleanup(srv.Close)
	return s, srv, m
}

func shardURL(srv *httptest.Server, store, file db.UUID, k int) string {
	return fmt.Sprintf("%s/v1/%s/%s/%d", srv.URL, store, file, k)
}

// blocks turns data into a shard's block stream.
func blocks(data []byte) []byte {
	var out []byte
	for first := true; first || len(data) > 0; first = false {
		n := min(len(data), erasure.BlockSize)
		out = append(out, data[:n]...)
		out = binary.LittleEndian.AppendUint32(out, shard.CRC(data[:n]))
		data = data[n:]
	}
	return out
}

func writeShardFile(t *testing.T, d *disk, id db.UUID, k int, mtime time.Time) string {
	path := d.path(id, k)
	f, err := create(path)
	if err != nil {
		t.Fatal(err)
	}
	f.Write(append([]byte(shard.Magic), blocks(random(100))...))
	f.Close()
	os.Chtimes(path, mtime, mtime)
	return path
}

func do(t *testing.T, method, url string, body []byte, hdr ...string) (int, []byte) {
	t.Helper()
	req, err := http.NewRequest(method, url, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b
}

func random(n int) []byte {
	b := make([]byte, n)
	rand.NewChaCha8([32]byte{byte(n), byte(n >> 8), byte(n >> 16)}).Read(b)
	return b
}

func newID(t *testing.T) db.UUID {
	id, err := db.NewID()
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for range 100 {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Errorf("timed out waiting for: %s", what)
}

// newServer is httptest.NewServer on the listener production uses (segments capped at a jumbo frame).
func newServer(t *testing.T, h http.Handler) *httptest.Server {
	ln, err := shard.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := httptest.NewUnstartedServer(h)
	s.Listener.Close()
	s.Listener = ln
	s.Start()
	return s
}

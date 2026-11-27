package serve

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"mime"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zeebo/blake3"

	"filescale/internal/db"
	"filescale/internal/erasure"
	"filescale/internal/metrics"
	"filescale/internal/shard"
	"filescale/internal/store"
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

func TestRoundTrip(t *testing.T) {
	e := newEnv(t, 12)
	sizes := []int{0, 1, 100, 32<<10 - 1, 32 << 10, 512<<10 - 1, 512 << 10, 512<<10 + 1, 3*512<<10 + 12345}
	defer func() {
		total := 0
		for _, n := range sizes {
			total += n
		}
		for series, want := range map[string]float64{
			`filescale_requests_total{method="PUT",code="201"}`: float64(len(sizes)),
			`filescale_requests_total{method="GET",code="200"}`: float64(len(sizes)),
			`filescale_received_bytes_total`:                    float64(total),
			`filescale_sent_bytes_total`:                        float64(total),
			`filescale_inflight{op="upload"}`:                   0,
			`filescale_inflight{op="download"}`:                 0,
			`filescale_parity_stripes_total`:                    0,
		} {
			if got := metric(e.sv.Metrics, series); got != want {
				t.Errorf("%s = %v, want %v", series, got, want)
			}
		}
		for _, n := range e.nodes {
			if got := metric(e.sv.Metrics, fmt.Sprintf(`filescale_store_up{store="%s",addr="%s"}`, n.id, n.addr)); got != 1 {
				t.Errorf("store %s up = %v", n.addr, got)
			}
		}
	}()
	for _, size := range sizes {
		name := fmt.Sprintf("dir/f%d", size)
		data := random(size)
		e.mustPut(name, data)
		resp, got := e.do("GET", name, nil)
		if resp.StatusCode != http.StatusOK || !bytes.Equal(got, data) {
			t.Errorf("size %d: %s, %d bytes back", size, resp.Status, len(got))
			continue
		}
		if sum := blake3.Sum256(data); resp.Header.Get("ETag") != etag(sum[:]) {
			t.Errorf("size %d: ETag %s", size, resp.Header.Get("ETag"))
		}
		f := e.file(name)
		wantData, wantStores := 8, 12
		if size < 32<<10 {
			wantData, wantStores = 1, 5 // parity+1 copies
		}
		if size == 0 {
			wantStores = 0
		}
		if f.DataShards != wantData || len(f.Stores) != wantStores {
			t.Errorf("size %d: %d data shards on %d stores, want %d on %d", size, f.DataShards, len(f.Stores), wantData, wantStores)
		}
		servers := map[string]bool{}
		for k, id := range f.Stores {
			servers[e.node(id).addr] = true
			if !e.hasRow(id, f.ID) {
				t.Errorf("size %d: no shards_by_store row for shard %d", size, k)
			}
		}
		if len(servers) != len(f.Stores) {
			t.Errorf("size %d: shards share a server", size)
		}
	}
}

// A store dying mid-shard fails the whole upload, even with a spare server: no files row, the partial shard is removed.
func TestStoreFailsMidUpload(t *testing.T) {
	e := newEnv(t, 13)
	for _, n := range e.nodes {
		n.failPut.Store(100 << 10)
	}
	if resp, b := e.do("PUT", "x", random(2<<20)); resp.StatusCode != http.StatusServiceUnavailable || !strings.Contains(string(b), "store 127.0.0.1:") {
		t.Fatalf("PUT: %s %s", resp.Status, b)
	}
	if f, _ := e.db.File(context.Background(), "x"); f != nil {
		t.Error("files row written for an upload missing a shard")
	}
	waitFor(t, "partial shards removed", func() bool {
		var n int
		for _, node := range e.nodes {
			filepath.WalkDir(node.dir, func(p string, d os.DirEntry, err error) error {
				if err == nil && !d.IsDir() && strings.Contains(d.Name(), ".") {
					n++
				}
				return nil
			})
		}
		return n == 0
	})
}

// stalled is a PUT body that passes 1 KiB, then reads nothing for pause: a store whose
// disk hung, the fault writeAll's stall timer exists for. The server never sees the serve
// hang up while a body is unread, so it can't wait on the request context.
type stalled struct {
	io.ReadCloser
	left  int64
	pause time.Duration
}

func (s *stalled) Read(p []byte) (int, error) {
	if s.left <= 0 && s.pause > 0 {
		time.Sleep(s.pause)
		s.pause = 0
	}
	if s.left > 0 {
		p = p[:min(len(p), int(s.left))]
	}
	n, err := s.ReadCloser.Read(p)
	s.left -= int64(n)
	return n, err
}

// shardFiles counts the shard files on every store.
func (e *env) shardFiles() int {
	n := 0
	for _, node := range e.nodes {
		filepath.WalkDir(node.dir, func(p string, d os.DirEntry, err error) error {
			if err == nil && !d.IsDir() && strings.Contains(d.Name(), ".") {
				n++
			}
			return nil
		})
	}
	return n
}

// A store that stops reading mid-shard fails the upload on the stall timer, and every
// partial shard is removed once the stores notice. The shards are 8 MiB so they exceed
// the TCP buffers, or a whole shard would sit in them and the serve would never block.
func TestStalledStoreFailsUpload(t *testing.T) {
	if testing.Short() {
		t.Skip("waits out the stall timer")
	}
	e := newEnv(t, 12)
	e.nodes[0].stallPut.Store(true)
	start := time.Now()
	resp, b := e.do("PUT", "st", random(64<<20))
	if resp.StatusCode != http.StatusServiceUnavailable || !strings.Contains(string(b), "accepted no data") {
		t.Fatalf("PUT: %s %s", resp.Status, b)
	}
	if took := time.Since(start); took < shard.Stall || took > shard.Stall+15*time.Second {
		t.Errorf("took %v, want the stall timer at %v", took, shard.Stall)
	}
	if f, _ := e.db.File(context.Background(), "st"); f != nil {
		t.Error("files row for an upload missing a shard")
	}
	for deadline := time.Now().Add(20 * time.Second); e.shardFiles() > 0; time.Sleep(100 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("%d partial shards left", e.shardFiles())
		}
	}
}

// A store that takes every byte but never answers 201 fails the upload at commitWait.
func TestNoCommit(t *testing.T) {
	e := newEnv(t, 12)
	e.nodes[0].noCommit.Store(true)
	old := commitWait
	commitWait = 500 * time.Millisecond
	t.Cleanup(func() { commitWait = old })
	start := time.Now()
	if resp, b := e.do("PUT", "nc", random(100<<10)); resp.StatusCode != http.StatusServiceUnavailable ||
		!strings.Contains(string(b), "failed to commit") {
		t.Fatalf("PUT: %s %s", resp.Status, b)
	}
	if took := time.Since(start); took > 10*time.Second {
		t.Errorf("took %v, want commitWait", took)
	}
	if f, _ := e.db.File(context.Background(), "nc"); f != nil {
		t.Error("files row without the stores' 201")
	}
}

// The upload ignores the client's cancel: one that hangs up right after its body still
// commits, its shards and files row land.
func TestUploadOutlivesClient(t *testing.T) {
	e := newEnv(t, 12)
	data := random(100 << 10)
	u, err := url.Parse(e.url)
	if err != nil {
		t.Fatal(err)
	}
	c, err := net.Dial("tcp", u.Host)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Fprintf(c, "PUT /oc HTTP/1.1\r\nHost: %s\r\nContent-Length: %d\r\n\r\n", u.Host, len(data))
	c.Write(data)
	c.Close() // gone before the 201
	waitFor(t, "upload committed without its client", func() bool {
		f, _ := e.db.File(context.Background(), "oc")
		return f != nil
	})
	if resp, got := e.do("GET", "oc", nil); resp.StatusCode != http.StatusOK || !bytes.Equal(got, data) {
		t.Errorf("GET: %s, %d bytes", resp.Status, len(got))
	}
}

// A store dying on the next part's first PUT fails the upload, but the finished part is
// recorded: resumable from its offset, no files row.
func TestStoreDiesAfterPart(t *testing.T) {
	if testing.Short() {
		t.Skip("timing")
	}
	smallParts(t)
	e := newEnv(t, 12)
	e.nodes[0].failFrom.Store(2) // a store gets one PUT per part: part 0's passes
	e.nodes[0].failPut.Store(100 << 10)
	data := random(3<<20 + 5)
	id, _ := db.NewID()
	u := "d3?upload=" + id.String()
	if resp, _ := e.do("PUT", u, data); resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("PUT: %s", resp.Status)
	}
	if f, _ := e.db.File(context.Background(), "d3"); f != nil {
		t.Error("files row for a failed upload")
	}
	waitFor(t, "part 0 recorded", func() bool {
		rows, _ := e.db.UploadParts(context.Background(), "d3", id)
		return len(rows) == 1 && rows[0].Index == 0
	})
	if resp, _ := e.do("HEAD", u, nil); resp.Header.Get("Upload-Offset") != strconv.Itoa(1<<20) {
		t.Errorf("Upload-Offset %q", resp.Header.Get("Upload-Offset"))
	}
}

func TestContentType(t *testing.T) {
	mime.AddExtensionType(".fstest", "application/x-fstest")
	e := newEnv(t, 12)
	for _, c := range []struct{ name, header, body, want string }{
		{"a.fstest", "", "abc", "application/x-fstest"},
		{"page", "", "<html><body>hi</body></html>", "text/html; charset=utf-8"},
		{"b.fstest", "image/x-mine", "abc", "image/x-mine"},
	} {
		var hdr []string
		if c.header != "" {
			hdr = []string{"Content-Type", c.header}
		}
		if resp, b := e.do("PUT", c.name, []byte(c.body), hdr...); resp.StatusCode != http.StatusCreated {
			t.Fatalf("PUT %s: %s %s", c.name, resp.Status, b)
		}
		resp, _ := e.do("GET", c.name, nil)
		if resp.Header.Get("Content-Type") != c.want {
			t.Errorf("%s: Content-Type %q, want %q", c.name, resp.Header.Get("Content-Type"), c.want)
		}
		if resp.Header.Get("X-Content-Type-Options") != "nosniff" || resp.Header.Get("Content-Disposition") != "attachment" {
			t.Errorf("%s: may render inline: %v", c.name, resp.Header)
		}
	}
	cd := `attachment; filename="a b.html"; filename*=UTF-8''%C3%A9.html`
	resp, _ := e.do("GET", "page?response-content-disposition="+url.QueryEscape(cd), nil)
	if resp.Header.Get("Content-Disposition") != cd {
		t.Errorf("Content-Disposition %q, want %q", resp.Header.Get("Content-Disposition"), cd)
	}
	if resp, _ := e.do("GET", "page?response-content-disposition="+url.QueryEscape(" Inline; filename=a.html"), nil); resp.StatusCode != http.StatusBadRequest {
		t.Errorf("inline: %s", resp.Status)
	}

	// AllowInline: inline is echoed as sent (like S3/SeaweedFS), anything else is still refused,
	// and without the parameter it's still attachment
	e.sv.AllowInline = true
	inline := `inline; filename="a b.html"`
	if resp, _ := e.do("GET", "page?response-content-disposition="+url.QueryEscape(inline), nil); resp.StatusCode != http.StatusOK ||
		resp.Header.Get("Content-Disposition") != inline || resp.Header.Get("X-Content-Type-Options") != "nosniff" {
		t.Errorf("AllowInline, inline: %s %q", resp.Status, resp.Header.Get("Content-Disposition"))
	}
	if resp, _ := e.do("GET", "page?response-content-disposition="+url.QueryEscape("form-data; name=x"), nil); resp.StatusCode != http.StatusBadRequest {
		t.Errorf("AllowInline, form-data: %s", resp.Status)
	}
	if resp, _ := e.do("GET", "page", nil); resp.Header.Get("Content-Disposition") != "attachment" {
		t.Errorf("AllowInline, no parameter: %q", resp.Header.Get("Content-Disposition"))
	}
}

func TestRange(t *testing.T) {
	e := newEnv(t, 12)
	data := random(2<<20 + 7)
	e.mustPut("r", data)
	n := len(data)
	for _, c := range []struct {
		hdr        string
		start, end int
	}{
		{"bytes=0-0", 0, 1},
		{"bytes=100-199", 100, 200},
		{"bytes=65530-65545", 65530, 65546},     // crosses a block
		{"bytes=524280-524300", 524280, 524301}, // crosses a stripe
		{"bytes=-10", n - 10, n},
		{"bytes=2097000-", 2097000, n},
		{"bytes=0-9223372036854775807", 0, n}, // a last byte past the end, as far as it goes
	} {
		resp, got := e.do("GET", "r", nil, "Range", c.hdr)
		if resp.StatusCode != http.StatusPartialContent || !bytes.Equal(got, data[c.start:c.end]) {
			t.Errorf("%s: %s, %d bytes, want %d", c.hdr, resp.Status, len(got), c.end-c.start)
		}
	}
	if resp, got := e.do("GET", "r", nil, "Range", "bytes=0-1,5-9"); resp.StatusCode != http.StatusOK || !bytes.Equal(got, data) {
		t.Errorf("several ranges: %s, want the whole file", resp.Status)
	}
	small := random(1000)
	e.mustPut("s", small)
	if resp, got := e.do("GET", "s", nil, "Range", "bytes=10-19"); resp.StatusCode != http.StatusPartialContent || !bytes.Equal(got, small[10:20]) {
		t.Errorf("small file range: %s", resp.Status)
	}
}

func TestDegradedRead(t *testing.T) {
	e := newEnv(t, 12)
	data := random(1<<20 + 5)
	e.mustPut("d", data)
	f := e.file("d")
	e.kill(e.node(f.Stores[0])) // gone from memberlist
	e.kill(e.node(f.Stores[1]))
	e.node(f.Stores[2]).srv.Close() // still listed, refuses connections
	e.node(f.Stores[3]).srv.Close()
	if resp, got := e.do("GET", "d", nil); resp.StatusCode != http.StatusOK || !bytes.Equal(got, data) {
		t.Fatalf("4 data shards lost: %s, %d bytes", resp.Status, len(got))
	}
	if got := metric(e.sv.Metrics, "filescale_parity_stripes_total"); got != 3 {
		t.Errorf("parity stripes %v, want all 3", got)
	}
	for k := 2; k <= 4; k++ {
		e.kill(e.node(f.Stores[k]))
	}
	if resp, _ := e.do("GET", "d", nil); resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("5 shards lost: %s, want 503 before any data", resp.Status)
	}
	if got := metric(e.sv.Metrics, `filescale_requests_total{method="GET",code="503"}`); got != 1 {
		t.Errorf("GET 503s counted: %v", got)
	}
}

func TestSlowStore(t *testing.T) {
	e := newEnv(t, 12, func(c *Config) { c.SlowRead = 200 * time.Millisecond })
	data := random(1 << 20)
	e.mustPut("slow", data)
	f := e.file("slow")
	e.node(f.Stores[0]).slow.Store(true)
	start := time.Now()
	if resp, got := e.do("GET", "slow", nil); resp.StatusCode != http.StatusOK || !bytes.Equal(got, data) {
		t.Fatalf("%s, %d bytes", resp.Status, len(got))
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Errorf("took %v, the slow shard should be read from parity", d)
	}
	if got := metric(e.sv.Metrics, fmt.Sprintf(`filescale_slow_drops_total{store="%s"}`, f.Stores[0])); got != 1 {
		t.Errorf("slow drops of the slow store: %v", got)
	}
}

func TestTimeToFirstByte(t *testing.T) {
	e := newEnv(t, 12, func(c *Config) { c.SlowRead = 5 * time.Second })
	data := random(1 << 20)
	e.mustPut("t", data)
	f := e.file("t")
	for _, id := range f.Stores[1:8] {
		e.node(id).hiccup.Store(int64(time.Second))
	}
	start := time.Now()
	resp, err := http.Get(e.url + "/t")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	first := make([]byte, erasure.BlockSize)
	if _, err := io.ReadFull(resp.Body, first); err != nil {
		t.Fatal(err)
	}
	ttfb := time.Since(start)
	rest, err := io.ReadAll(resp.Body)
	all := time.Since(start)
	if err != nil || !bytes.Equal(append(first, rest...), data) {
		t.Fatalf("wrong data: %v", err)
	}
	if ttfb > 500*time.Millisecond {
		t.Errorf("first 64 KiB took %v, it only needs shard 0", ttfb)
	}
	if all < time.Second {
		t.Errorf("whole file took %v, shards 1-7 were stalled for 1s", all)
	}
}

// TestSlowShards: a stalled store, or one slower than its peers but under
// -slowRead, is switched to parity. Stores that are all equally slow are not.
func TestSlowShards(t *testing.T) {
	if testing.Short() {
		t.Skip("timing")
	}
	data := random(16 << 20) // 32 stripes
	ms := time.Millisecond
	for _, c := range []struct {
		name        string
		stall, pace time.Duration // shard 0's store: one stall before its first byte, then per 64 KiB
		paceAll     time.Duration // every store, per 64 KiB
		rebuilt     int64         // at least, -1 = none
		under       time.Duration
	}{
		{"1s stall", time.Second, 0, 0, 32, 900 * ms}, // well past -slowRead plus the peers' first block
		{"500ms per block", 0, 500 * ms, 0, 32, time.Second},
		{"50ms per block, under -slowRead", 0, 50 * ms, 0, 24, time.Second}, // switched within the first quarter
		{"every store 5ms per block", 0, 0, 5 * ms, -1, 2 * time.Second},
	} {
		e := newEnv(t, 12, func(cfg *Config) { cfg.SlowRead = 200 * ms })
		e.mustPut("f", data)
		f := e.file("f")
		for _, n := range e.nodes {
			n.pace.Store(int64(c.paceAll))
		}
		slow := e.node(f.Stores[0])
		slow.hiccup.Store(int64(c.stall))
		if c.pace > 0 {
			slow.pace.Store(int64(c.pace))
		}
		start := time.Now()
		resp, got := e.do("GET", "f", nil)
		took := time.Since(start)
		if resp.StatusCode != http.StatusOK || !bytes.Equal(got, data) {
			t.Errorf("%s: %s, %d bytes", c.name, resp.Status, len(got))
			continue
		}
		time.Sleep(50 * ms) // the download's counters land after its last byte
		rebuilt := e.sv.rebuilt.Load()
		t.Logf("%-32s %7v, %2d/32 stripes rebuilt", c.name, took.Round(ms), rebuilt)
		if c.rebuilt < 0 && rebuilt != 0 || rebuilt < c.rebuilt {
			t.Errorf("%s: %d stripes rebuilt, want %d", c.name, rebuilt, max(c.rebuilt, 0))
		}
		if took > c.under {
			t.Errorf("%s: took %v, want under %v", c.name, took, c.under)
		}
	}
}

func TestBadCRCAndScrub(t *testing.T) {
	e := newEnv(t, 12)
	data := random(1 << 20) // 2 stripes
	e.mustPut("c", data)
	f := e.file("c")
	hdr := len(shard.Magic)
	p0 := e.shardPath(f, 0)
	flip(t, p0, hdr+10)                        // shard 0, stripe 0
	flip(t, e.shardPath(f, 1), hdr+65536+4+10) // shard 1, stripe 1
	if resp, got := e.do("GET", "c", nil); resp.StatusCode != http.StatusOK || !bytes.Equal(got, data) {
		t.Fatalf("GET with bad blocks: %s, %d bytes", resp.Status, len(got))
	}

	if err := e.node(f.Stores[0]).st.Scrub(context.Background(), false, time.Time{}); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(p0)
	if err != nil {
		t.Fatal(err)
	}
	block, ok := shard.Check(b[hdr : hdr+erasure.BlockSize+4])
	if !ok || !bytes.Equal(block, data[:erasure.BlockSize]) {
		t.Error("scrub didn't rebuild the bad block")
	}
}

// Bad blocks in stripes 0 and 2 of one shard are two runs, rebuilt in one scrub.
func TestScrubTwoRuns(t *testing.T) {
	e := newEnv(t, 12)
	data := random(1500 << 10) // 3 stripes, the last one short
	e.mustPut("tr", data)
	f := e.file("tr")
	l := erasure.Layout{Size: f.Size, Data: f.DataShards}
	hdr := int64(len(shard.Magic))
	p := e.shardPath(f, 0)
	for _, s := range []int64{0, 2} {
		flip(t, p, int(hdr+l.BlockOffset(s)+10))
	}
	if err := e.node(f.Stores[0]).st.Scrub(context.Background(), false, time.Time{}); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range []int64{0, 1, 2} {
		n := int64(l.BlockLen(s))
		block, ok := shard.Check(b[hdr+l.BlockOffset(s):][:n+4])
		if want := data[s*l.StripeSize():][:n]; !ok || !bytes.Equal(block, want) { // shard 0 leads each stripe
			t.Errorf("stripe %d not rebuilt", s)
		}
	}
	n := e.node(f.Stores[0])
	labels := fmt.Sprintf(`{store="%s",dir="%s"}`, n.id, n.dir)
	if got := metric(n.st.Metrics, "filescale_scrub_rebuilt_blocks_total"+labels); got != 2 {
		t.Errorf("rebuilt blocks %v, want 2", got)
	}
	if got := metric(n.st.Metrics, "filescale_scrub_last_pass_timestamp_seconds"+labels); time.Since(time.Unix(int64(got), 0)) > time.Minute {
		t.Errorf("last scrub pass %v", got)
	}
}

// A store crash can lose a whole shard file or zero its header, scrub puts them back.
func TestScrubRestoresLostShard(t *testing.T) {
	e := newEnv(t, 12)
	data := random(1<<20 + 1000) // 3 stripes, the last one short
	e.mustPut("c", data)
	f := e.file("c")
	var paths []string
	var want [][]byte
	for k := range 3 {
		p := e.shardPath(f, k)
		b, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		paths, want = append(paths, p), append(want, b)
	}
	os.Remove(paths[0])
	os.Truncate(paths[1], int64(len(want[1])/2))
	os.WriteFile(paths[2], make([]byte, len(want[2])), 0o644)
	// until a scrub gets to them, reads go around the missing, cut-off and zeroed shards
	if resp, got := e.do("GET", "c", nil); resp.StatusCode != http.StatusOK || !bytes.Equal(got, data) {
		t.Fatalf("GET before scrub: %s", resp.Status)
	}
	if metric(e.sv.Metrics, "filescale_parity_stripes_total") == 0 {
		t.Error("read the damaged shards without parity")
	}
	for k := range 3 {
		if err := e.node(f.Stores[k]).st.Scrub(context.Background(), false, time.Time{}); err != nil {
			t.Fatal(err)
		}
		if b, err := os.ReadFile(paths[k]); err != nil || !bytes.Equal(b, want[k]) {
			t.Errorf("shard %d not restored: %v, %d of %d bytes", k, err, len(b), len(want[k]))
		}
	}
	if resp, got := e.do("GET", "c", nil); resp.StatusCode != http.StatusOK || !bytes.Equal(got, data) {
		t.Errorf("GET after scrub: %s", resp.Status)
	}
}

// After a reboot a store rebuilds the shards written since just before its last heartbeat, no older ones.
func TestCrashCheck(t *testing.T) {
	e := newEnv(t, 12)
	ctx := context.Background()
	lastSeen := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC) // long before this machine booted
	e.db.Clock = func() time.Time { return lastSeen.Add(-time.Hour) }
	e.mustPut("old", random(100<<10))
	e.db.Clock = nil
	e.mustPut("new", random(100<<10))
	fo, fn := e.file("old"), e.file("new")
	n := e.node(fn.Stores[0])
	if !n.st.CrashedSince(ctx).IsZero() {
		t.Error("crash check without a reboot")
	}
	pOld, pNew := e.shardPath(fo, slices.Index(fo.Stores, n.id)), e.shardPath(fn, 0)
	want, _ := os.ReadFile(pNew)
	os.Remove(pOld)
	os.Remove(pNew)
	e.db.Heartbeat(ctx, db.Store{ID: n.id, Addr: n.addr, LastSeen: lastSeen})

	since := n.st.CrashedSince(ctx)
	if !since.Equal(lastSeen.Add(-2 * time.Minute)) {
		t.Fatalf("since = %v", since)
	}
	if err := n.st.Scrub(ctx, false, since); err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(pNew); err != nil || !bytes.Equal(got, want) {
		t.Errorf("recent shard not rebuilt: %v", err)
	}
	if _, err := os.Stat(pOld); err == nil {
		t.Error("checked a shard written before the crash window")
	}
}

// Run checks the shards written since the last heartbeat first thing after a reboot,
// so it must read last_seen before its own first heartbeat replaces it.
func TestRunCrashCheck(t *testing.T) {
	e := newEnv(t, 12)
	e.mustPut("new", random(100<<10))
	f := e.file("new")
	n, p := e.node(f.Stores[0]), e.shardPath(f, 0)
	want, _ := os.ReadFile(p)
	os.Remove(p)
	e.db.Heartbeat(context.Background(), db.Store{ID: n.id, Addr: n.addr, LastSeen: time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)})
	checks := &lineCount{w: os.Stderr, match: "rebooted since the last heartbeat"}
	log.SetOutput(checks)
	defer log.SetOutput(os.Stderr)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go n.st.Run(ctx, n.addr)
	waitFor(t, "lost shard rebuilt", func() bool {
		got, err := os.ReadFile(p)
		return err == nil && bytes.Equal(got, want)
	})
	if checks.n.Load() != 1 {
		t.Error("no crash check after the reboot")
	}
}

func TestSmallFileCopies(t *testing.T) {
	e := newEnv(t, 12)
	data := random(1000)
	e.mustPut("small", data)
	f := e.file("small")
	for k := range 4 {
		e.kill(e.node(f.Stores[k]))
	}
	if resp, got := e.do("GET", "small", nil); resp.StatusCode != http.StatusOK || !bytes.Equal(got, data) {
		t.Fatalf("4 copies lost: %s", resp.Status)
	}
	e.kill(e.node(f.Stores[4]))
	if resp, _ := e.do("GET", "small", nil); resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("all copies lost: %s", resp.Status)
	}
}

func TestUploadErrors(t *testing.T) {
	e := newEnv(t, 12)
	req, _ := http.NewRequest("PUT", e.url+"/x", io.NopCloser(strings.NewReader("abc"))) // sent chunked
	if resp, err := http.DefaultClient.Do(req); err != nil || resp.StatusCode != http.StatusLengthRequired {
		t.Errorf("no Content-Length: %v %v", resp, err)
	}
	if resp, _ := e.do("PUT", "x", []byte("abc"), "X-Blake3", "zz"); resp.StatusCode != http.StatusBadRequest {
		t.Errorf("malformed X-Blake3: %s", resp.Status)
	}
	if resp, _ := e.do("PUT", "x", []byte("abc"), "X-Blake3", strings.Repeat("00", 32)); resp.StatusCode != http.StatusBadRequest {
		t.Errorf("wrong X-Blake3: %s", resp.Status)
	}
	if f, _ := e.db.File(context.Background(), "x"); f != nil {
		t.Error("files row written despite the blake3 mismatch")
	}
	sum := blake3.Sum256([]byte("abc"))
	if resp, _ := e.do("PUT", "x", []byte("abc"), "X-Blake3", hex.EncodeToString(sum[:])); resp.StatusCode != http.StatusCreated {
		t.Errorf("right X-Blake3: %s", resp.Status)
	}
	id, _ := db.NewID()
	if resp, _ := e.do("DELETE", "x?upload="+id.String(), nil); resp.StatusCode != http.StatusBadRequest {
		t.Errorf("DELETE with ?upload: %s", resp.Status)
	}
}

func TestNotEnoughStores(t *testing.T) {
	e := newEnv(t, 11)
	if resp, b := e.do("PUT", "big", random(100<<10)); resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("11 servers for 8+4: %s %s", resp.Status, b)
	}
	if resp, _ := e.do("PUT", "small", random(100)); resp.StatusCode != http.StatusCreated {
		t.Errorf("small file needs only 5: %s", resp.Status)
	}

	full := newEnv(t, 0)
	for range 12 {
		full.addStore(100)
	}
	full.reload()
	if resp, b := full.do("PUT", "big", random(100<<10)); resp.StatusCode != http.StatusInsufficientStorage {
		t.Errorf("full disks: %s %s", resp.Status, b)
	}
}

func TestStoreRefusalSwapped(t *testing.T) {
	e := newEnv(t, 12)
	full := e.addStore(100) // refuses uploads, but its row claims lots of room
	e.db.Heartbeat(context.Background(), db.Store{ID: full.id, Addr: full.addr, Free: 1 << 60, LastSeen: time.Now()})
	e.reload()
	data := random(100 << 10)
	e.mustPut("swap", data)
	if slices.Contains(e.file("swap").Stores, full.id) {
		t.Error("shard landed on the store that answered 507")
	}
	if got := metric(e.sv.Metrics, fmt.Sprintf(`filescale_put_refused_total{store="%s"}`, full.id)); got != 1 {
		t.Errorf("refusals of the full store: %v", got)
	}
	if resp, got := e.do("GET", "swap", nil); resp.StatusCode != http.StatusOK || !bytes.Equal(got, data) {
		t.Errorf("GET: %s", resp.Status)
	}
}

// A store answering 201 without reading the shard (a broken proxy, say) stored nothing: swapped like a refusal.
func TestStoreAnswersUnread(t *testing.T) {
	e := newEnv(t, 13)
	n := e.nodes[0]
	n.unread.Store(true)
	refused := fmt.Sprintf(`filescale_put_refused_total{store="%s"}`, n.id)
	data := random(100 << 10)
	for i := 0; metric(e.sv.Metrics, refused) == 0; i++ { // an upload takes 12 of the 13
		if i == 20 {
			t.Fatal("never picked the store")
		}
		name := fmt.Sprintf("u%d", i)
		e.mustPut(name, data)
		if slices.Contains(e.file(name).Stores, n.id) {
			t.Errorf("%s: shard placed on the store that never read it", name)
		}
	}
}

// A store that takes the PUT but never says 100 Continue fails the upload after its 2s.
func TestMuteStore(t *testing.T) {
	e := newEnv(t, 12)
	e.nodes[3].mute.Store(true)
	start := time.Now()
	resp, b := e.do("PUT", "m", random(100<<10))
	took := time.Since(start)
	if resp.StatusCode != http.StatusServiceUnavailable || !strings.Contains(string(b), "no 100 Continue within 2s, and no spare server to swap in") {
		t.Fatalf("PUT: %s %s", resp.Status, b)
	}
	if took < acceptWait || took > acceptWait+time.Second {
		t.Errorf("took %v, want about %v", took, acceptWait)
	}
}

func TestTokenAndOnly(t *testing.T) {
	e := newEnv(t, 12)
	for _, c := range []struct {
		only, tokenFor, method, auth string
		want                         int
	}{
		{"", "", "GET", "", 401},
		{"", "", "GET", "Bearer tok", 404},
		{"", "", "PUT", "", 401},
		{"", "upload", "GET", "", 404},
		{"", "upload", "PUT", "", 401},
		{"", "upload", "DELETE", "", 401},
		{"", "download", "PUT", "", 201},
		{"", "download", "GET", "Bearer wrong", 401},
		{"download", "", "PUT", "Bearer tok", 405},
		{"upload", "", "GET", "Bearer tok", 405},
		{"", "", "POST", "Bearer tok", 405},
	} {
		cfg := e.sv.Config
		cfg.Token, cfg.TokenFor, cfg.Only = "tok", c.tokenFor, c.only
		sv := New(cfg)
		sv.Reload(context.Background())
		srv := newServer(t, sv)
		var body []byte
		if c.method == "PUT" {
			body = []byte("hello")
		}
		name := "missing"
		if c.method == "PUT" {
			name = "new"
		}
		resp, _ := request(t, c.method, srv.URL+"/"+name, body, "Authorization", c.auth)
		srv.Close()
		if resp.StatusCode != c.want {
			t.Errorf("%+v: %s", c, resp.Status)
		}
		if c.want == 405 && resp.Header.Get("Allow") == "" {
			t.Errorf("%+v: 405 without Allow", c)
		}
	}
}

func TestNames(t *testing.T) {
	e := newEnv(t, 12)
	for _, name := range []string{strings.Repeat("a", 1025), "%FF"} {
		if resp, _ := e.do("GET", name, nil); resp.StatusCode != http.StatusBadRequest {
			t.Errorf("name %q: %s", name, resp.Status)
		}
	}
	e.mustPut("a//b", []byte("x")) // names are taken as-is, no path cleaning
	if resp, got := e.do("GET", "a//b", nil); resp.StatusCode != http.StatusOK || string(got) != "x" {
		t.Errorf("a//b: %s", resp.Status)
	}
	if resp, _ := e.do("GET", "a/b", nil); resp.StatusCode != http.StatusNotFound {
		t.Errorf("a/b: %s", resp.Status)
	}
	e.mustPut("with%20space", []byte("y"))
	if f, _ := e.db.File(context.Background(), "with space"); f == nil {
		t.Error("percent-encoded name not decoded")
	}
}

func TestHealth(t *testing.T) {
	e := newEnv(t, 12, func(c *Config) { c.Token, c.Only = "tok", "upload" })
	if resp, b := e.do("GET", "", nil); resp.StatusCode != http.StatusOK || string(b) != "ok\n" {
		t.Errorf("GET /: %s %q", resp.Status, b)
	}
	if resp, _ := e.do("PUT", "", []byte("x"), "Authorization", "Bearer tok"); resp.StatusCode != http.StatusBadRequest {
		t.Errorf("PUT /: %s", resp.Status)
	}
	e.sv.reloaded.Store(time.Now().Add(-3 * time.Minute).UnixNano()) // Cassandra gone since
	if resp, _ := e.do("GET", "", nil); resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("GET / without Cassandra: %s", resp.Status)
	}
}

// A repair that loses the CAS to an overwrite deletes the shard it just wrote and keeps
// the old one: the files row moved on without it, so the new shard is garbage.
func TestRepairLosesCAS(t *testing.T) {
	e, h := newHookEnv(t, 13)
	ctx := context.Background()
	data := random(100 << 10)
	e.mustPut("cas", data)
	f := e.file("cas")
	lost := e.node(f.Stores[0])
	rows, err := e.db.Shards(ctx, lost.id, db.Bucket(f.ID))
	if err != nil || len(rows) == 0 {
		t.Fatalf("rows: %v, %v", rows, err)
	}
	// no room left on it: the shard goes to the spare, not back in place
	e.db.Heartbeat(ctx, db.Store{ID: lost.id, Addr: lost.addr, LastSeen: time.Now()})
	e.reload()
	h.onSet = func() (bool, error) { return false, nil } // the name was overwritten meanwhile
	if err := e.sv.repairFile(ctx, rows); err != nil {
		t.Fatal(err)
	}
	h.onSet = nil
	if cur := e.file("cas"); cur.Stores[0] != lost.id || !e.hasRow(lost.id, f.ID) {
		t.Errorf("old shard dropped: %v", cur.Stores)
	}
	var spare *node
	for _, n := range e.nodes {
		if !slices.Contains(f.Stores, n.id) {
			spare = n
		}
	}
	id := f.ID.String()
	if _, err := os.Stat(filepath.Join(spare.dir, id[0:2], id[2:4], id+".0")); err != nil {
		t.Fatal("the repair didn't write to the spare")
	}
	if e.hasRow(spare.id, f.ID) {
		t.Error("the CAS-lost repair left its row on the spare")
	}
	if resp, got := e.do("GET", "cas", nil); resp.StatusCode != http.StatusOK || !bytes.Equal(got, data) {
		t.Errorf("GET: %s", resp.Status)
	}
}

// The pre-check refuses a range whose parts are short a data shard before any data flows,
// while a range inside the healthy parts still works.
func TestDegradedPartPrecheck(t *testing.T) {
	smallParts(t)
	e := newEnv(t, 12)
	data := random(3 << 20) // 3 parts
	e.mustPut("pc", data)
	f := e.file("pc")
	bad := *f
	bad.Stores = slices.Clone(f.Stores)
	dead, _ := db.NewID()
	for i := 12; i < len(f.Stores); i++ {
		bad.Stores[i] = dead // parts 1 and 2 are nowhere
	}
	e.db.PutFile(context.Background(), &bad)
	if resp, _ := e.do("GET", "pc", nil); resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("GET with two dead parts: %s", resp.Status)
	}
	resp, got := e.do("GET", "pc", nil, "Range", "bytes=0-999")
	if resp.StatusCode != http.StatusPartialContent || !bytes.Equal(got, data[:1000]) {
		t.Errorf("range inside part 0: %s, %d bytes", resp.Status, len(got))
	}
	// a matching If-Range keeps the range, so the dead parts stay out of the pre-check
	if r, got := e.do("GET", "pc", nil, "Range", "bytes=0-999", "If-Range", resp.Header.Get("ETag")); r.StatusCode != http.StatusPartialContent || !bytes.Equal(got, data[:1000]) {
		t.Errorf("range inside part 0 with a matching If-Range: %s, %d bytes", r.Status, len(got))
	}
	if r, _ := e.do("GET", "pc", nil, "Range", "bytes=0-999", "If-Range", `"stale"`); r.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("stale If-Range with two dead parts: %s", r.Status)
	}
}

// A stale If-Range gets the whole file, a matching one gets the range.
func TestIfRange(t *testing.T) {
	e := newEnv(t, 12)
	data := random(70000)
	e.mustPut("ir", data)
	resp, _ := e.do("GET", "ir", nil)
	tag := resp.Header.Get("ETag")
	if resp, got := e.do("GET", "ir", nil, "Range", "bytes=10-19", "If-Range", `"stale"`); resp.StatusCode != http.StatusOK || !bytes.Equal(got, data) {
		t.Errorf("stale If-Range: %s, %d bytes", resp.Status, len(got))
	}
	if resp, got := e.do("GET", "ir", nil, "Range", "bytes=10-19", "If-Range", tag); resp.StatusCode != http.StatusPartialContent || !bytes.Equal(got, data[10:20]) {
		t.Errorf("fresh If-Range: %s, %d bytes", resp.Status, len(got))
	}
}

// A new empty server among fuller ones gets more uploads, but not nearly all of them.
func TestPickCap(t *testing.T) {
	alive := map[string]bool{}
	var stores []db.Store
	for i := range 50 {
		st := db.Store{Addr: fmt.Sprint(i), Free: 10, LastSeen: time.Now()}
		if i == 0 {
			st.Free = 100
		}
		stores = append(stores, st)
		alive[st.Addr] = true
	}
	s := New(Config{Alive: func() map[string]bool { return alive }})
	s.stores.Store(&storeView{list: stores})
	hits := 0
	for range 2000 {
		picked, err := s.pick(12, 1, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		if slices.ContainsFunc(picked, func(st db.Store) bool { return st.Addr == "0" }) {
			hits++
		}
	}
	// fair share 24%, about 45% with the cap, over 90% without
	if hits < 600 || hits > 1200 {
		t.Errorf("new server in %d of 2000 uploads", hits)
	}

	// huge free values must not overflow int64 weight sums
	huge := make([]db.Store, 12)
	for i := range huge {
		huge[i] = db.Store{Addr: fmt.Sprint(i), Free: 1 << 60, LastSeen: time.Now()}
	}
	s.stores.Store(&storeView{list: huge})
	if picked, err := s.pick(12, 1, nil, nil); err != nil || len(picked) != 12 {
		t.Errorf("huge Free pick: %d, %v", len(picked), err)
	}
}

// A store not seen for 3 minutes is out of uploads even if memberlist still lists it.
func TestPickDropsStale(t *testing.T) {
	e := newEnv(t, 13)
	// the most room of all, only its age keeps it out
	e.db.Heartbeat(context.Background(), db.Store{ID: e.nodes[0].id, Addr: e.nodes[0].addr, Free: 1 << 50, LastSeen: time.Now().Add(-4 * time.Minute)})
	e.reload()
	picked, err := e.sv.pick(12, 1, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if slices.ContainsFunc(picked, func(st db.Store) bool { return st.ID == e.nodes[0].id }) {
		t.Error("picked a store not seen for 4 minutes")
	}
}

// Run reloads the stores table at once and stops with its context.
func TestRunStops(t *testing.T) {
	e := newEnv(t, 12)
	e.sv.reloaded.Store(0)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		e.sv.Run(ctx, time.Hour)
		close(done)
	}()
	waitFor(t, "first reload", func() bool { return e.sv.reloaded.Load() != 0 })
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run kept going after its context ended")
	}
}

// ServeContent seeks from the start and the end, a negative position is an error.
func TestDownloadSeek(t *testing.T) {
	d := &download{f: &db.File{Size: 100}}
	for _, c := range []struct {
		off    int64
		whence int
		want   int64
	}{
		{10, io.SeekStart, 10},
		{5, io.SeekCurrent, 15},
		{-10, io.SeekEnd, 90},
		{0, io.SeekEnd, 100},
	} {
		if got, err := d.Seek(c.off, c.whence); err != nil || got != c.want {
			t.Errorf("Seek(%d, %d) = %d, %v, want %d", c.off, c.whence, got, err, c.want)
		}
	}
	if _, err := d.Seek(-101, io.SeekEnd); err == nil {
		t.Error("seeked before the start")
	}
}

func TestOverwriteAndDelete(t *testing.T) {
	e := newEnv(t, 12)
	v1, v2 := random(100<<10), random(200<<10)
	e.mustPut("o", v1)
	f1 := e.file("o")
	e.mustPut("o", v2)
	f2 := e.file("o")
	if resp, got := e.do("GET", "o", nil); resp.StatusCode != http.StatusOK || !bytes.Equal(got, v2) {
		t.Fatalf("GET after overwrite: %s", resp.Status)
	}
	waitFor(t, "old shards unindexed", func() bool { return !e.anyRow(f1) })
	if !e.allRows(f2) {
		t.Error("new file lost index rows")
	}

	if resp, _ := e.do("DELETE", "o", nil); resp.StatusCode != http.StatusNoContent {
		t.Fatalf("DELETE: %s", resp.Status)
	}
	if resp, _ := e.do("GET", "o", nil); resp.StatusCode != http.StatusNotFound {
		t.Errorf("GET after DELETE: %s", resp.Status)
	}
	waitFor(t, "deleted shards unindexed", func() bool { return !e.anyRow(f2) })
	if resp, _ := e.do("DELETE", "o", nil); resp.StatusCode != http.StatusNotFound {
		t.Errorf("second DELETE: %s", resp.Status)
	}
}

func TestConditionalAndHead(t *testing.T) {
	e := newEnv(t, 12)
	e.mustPut("e", random(70000))
	resp, _ := e.do("GET", "e", nil)
	tag := resp.Header.Get("ETag")
	if resp, b := e.do("GET", "e", nil, "If-None-Match", tag); resp.StatusCode != http.StatusNotModified || len(b) != 0 {
		t.Errorf("If-None-Match: %s", resp.Status)
	}
	if resp, b := e.do("HEAD", "e", nil); resp.StatusCode != http.StatusOK || resp.ContentLength != 70000 || len(b) != 0 {
		t.Errorf("HEAD: %s, length %d", resp.Status, resp.ContentLength)
	}
}

func TestRepair(t *testing.T) {
	e := newEnv(t, 14)
	ctx := context.Background()
	big, small := random(700<<10), random(1000)
	e.mustPut("big", big)
	e.mustPut("small", small)
	fb := e.file("big")
	lost := e.node(fb.Stores[0])
	e.kill(lost)
	e.db.Heartbeat(ctx, db.Store{ID: lost.id, Addr: lost.addr, LastSeen: time.Now().Add(-25 * time.Hour)})
	want := 1.0 // big's shard 0, plus the small file's copy if the lost store had one
	if slices.Contains(e.file("small").Stores, lost.id) {
		want++
	}

	if err := e.sv.Repair(ctx, 24*time.Hour); err != nil {
		t.Fatal(err)
	}
	for series, v := range map[string]float64{
		"filescale_repaired_shards_total": want,
		"filescale_repair_errors_total":   0,
		"filescale_repair_failed_buckets": 0,
	} {
		if got := metric(e.sv.Metrics, series); got != v {
			t.Errorf("%s = %v, want %v", series, got, v)
		}
	}
	if got := metric(e.sv.Metrics, "filescale_repair_last_pass_timestamp_seconds"); time.Since(time.Unix(int64(got), 0)) > time.Minute {
		t.Errorf("last repair pass %v", got)
	}
	fb2 := e.file("big")
	if fb2.Stores[0] == lost.id {
		t.Fatal("shard 0 not moved")
	}
	if !slices.Equal(fb2.Stores[1:], fb.Stores[1:]) {
		t.Error("repair moved healthy shards")
	}
	if e.hasRow(lost.id, fb.ID) || !e.hasRow(fb2.Stores[0], fb.ID) {
		t.Error("index row not moved")
	}
	if slices.Contains(e.file("small").Stores, lost.id) {
		t.Error("small file copy not repaired")
	}
	stores, _ := e.db.Stores(ctx)
	if slices.ContainsFunc(stores, func(s db.Store) bool { return s.ID == lost.id }) {
		t.Error("empty down store's row kept")
	}

	// the rebuilt shard must be real: lose 4 others so the read needs it
	for k := 1; k <= 4; k++ {
		e.kill(e.node(fb2.Stores[k]))
	}
	e.reload()
	if resp, got := e.do("GET", "big", nil); resp.StatusCode != http.StatusOK || !bytes.Equal(got, big) {
		t.Errorf("GET through the rebuilt shard: %s", resp.Status)
	}
}

func TestNoRepair(t *testing.T) {
	e := newEnv(t, 13, func(cfg *Config) { cfg.NoRepair = true })
	ctx := context.Background()
	e.mustPut("f", random(100<<10))
	f := e.file("f")
	lost := e.node(f.Stores[0])
	e.kill(lost)
	e.db.Heartbeat(ctx, db.Store{ID: lost.id, Addr: lost.addr, LastSeen: time.Now().Add(-25 * time.Hour)})
	if err := e.sv.Repair(ctx, 24*time.Hour); err != nil {
		t.Fatal(err)
	}
	if e.file("f").Stores[0] != lost.id || !e.hasRow(lost.id, f.ID) {
		t.Error("NoRepair serve moved a shard")
	}
	for _, series := range []string{
		"filescale_repaired_shards_total",
		"filescale_repair_errors_total",
		"filescale_repair_last_pass_timestamp_seconds",
		"filescale_repair_pass_duration_seconds",
		"filescale_repair_failed_buckets",
	} {
		if got := metric(e.sv.Metrics, series); got != -1 {
			t.Errorf("%s = %v on NoRepair serve, want omitted", series, got)
		}
	}
}

func TestRepairSkipsMaintenance(t *testing.T) {
	e := newEnv(t, 13)
	ctx := context.Background()
	e.mustPut("m", random(100<<10))
	f := e.file("m")
	n := e.node(f.Stores[0])
	e.kill(n)
	e.db.Heartbeat(ctx, db.Store{ID: n.id, Addr: n.addr, LastSeen: time.Now().Add(-48 * time.Hour)})
	e.db.SetState(n.id, "maintenance")
	if err := e.sv.Repair(ctx, 24*time.Hour); err != nil {
		t.Fatal(err)
	}
	if e.file("m").Stores[0] != n.id {
		t.Error("repaired a store in maintenance")
	}
}

func TestReadOnlyStore(t *testing.T) {
	e := newEnv(t, 13)
	orig := random(100 << 10)
	e.mustPut("existing", orig)
	origFile := e.file("existing")
	ro := e.node(origFile.Stores[0])
	rw := e.node(origFile.Stores[1])

	e.db.SetState(ro.id, "readonly")
	e.reload()

	for series, want := range map[string]float64{
		fmt.Sprintf(`filescale_store_up{store="%s",addr="%s"}`, ro.id, ro.addr):          1,
		fmt.Sprintf(`filescale_store_readonly{store="%s",addr="%s"}`, ro.id, ro.addr):    1,
		fmt.Sprintf(`filescale_store_maintenance{store="%s",addr="%s"}`, ro.id, ro.addr): 0,
		fmt.Sprintf(`filescale_store_up{store="%s",addr="%s"}`, rw.id, rw.addr):          1,
		fmt.Sprintf(`filescale_store_readonly{store="%s",addr="%s"}`, rw.id, rw.addr):    0,
	} {
		if got := metric(e.sv.Metrics, series); got != want {
			t.Errorf("%s = %v, want %v", series, got, want)
		}
	}

	for i := range 5 {
		big := fmt.Sprintf("big-%d", i)
		e.mustPut(big, random(100<<10))
		if slices.Contains(e.file(big).Stores, ro.id) {
			t.Fatalf("upload %s picked readonly store %s", big, ro.id)
		}
		small := fmt.Sprintf("small-%d", i)
		e.mustPut(small, random(100))
		if slices.Contains(e.file(small).Stores, ro.id) {
			t.Fatalf("upload %s picked readonly store %s", small, ro.id)
		}
	}

	picked, err := e.sv.pick(12, 1, nil, origFile.Stores)
	if err != nil {
		t.Fatal(err)
	}
	if slices.ContainsFunc(picked, func(st db.Store) bool { return st.ID == ro.id }) {
		t.Error("pick kept readonly store from prefer")
	}

	// multi-disk host: readonly disk skipped, sibling writable disk still picked
	alive := map[string]bool{"h0": true}
	roID, _ := db.NewID()
	rwID, _ := db.NewID()
	stores := []db.Store{
		{ID: roID, Addr: "h0", State: "readonly", Free: 1 << 50, LastSeen: time.Now()},
		{ID: rwID, Addr: "h0", State: "up", Free: 100, LastSeen: time.Now()},
	}
	for i := 1; i < 12; i++ {
		id, _ := db.NewID()
		addr := fmt.Sprintf("h%d", i)
		stores = append(stores, db.Store{ID: id, Addr: addr, Free: 100, LastSeen: time.Now()})
		alive[addr] = true
	}
	sv := New(Config{Alive: func() map[string]bool { return alive }})
	sv.stores.Store(&storeView{list: stores})
	got, err := sv.pick(12, 1, nil, []db.UUID{roID})
	if err != nil {
		t.Fatalf("multi-disk pick: %v", err)
	}
	if slices.ContainsFunc(got, func(st db.Store) bool { return st.ID == roID }) {
		t.Error("picked readonly disk on multi-disk host")
	}
	if !slices.ContainsFunc(got, func(st db.Store) bool { return st.ID == rwID }) {
		t.Error("skipped writable disk on multi-disk host")
	}

	if resp, body := e.do("GET", "existing", nil); resp.StatusCode != http.StatusOK || !bytes.Equal(body, orig) {
		t.Fatalf("GET with readonly store: %s", resp.Status)
	}
	if rebuilt := metric(e.sv.Metrics, "filescale_parity_stripes_total"); rebuilt != 0 {
		t.Errorf("GET rebuilt %v stripes with live readonly store", rebuilt)
	}

	// dropping below 12 writable servers fails 8+4 uploads
	e.db.SetState(rw.id, "readonly")
	e.reload()
	if resp, b := e.do("PUT", "blocked", random(100<<10)); resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("11 writable servers for 8+4: %s %s", resp.Status, b)
	}

	// restoring to "up" makes both stores writable again
	e.db.SetState(ro.id, "up")
	e.db.SetState(rw.id, "up")
	e.reload()
	if got := metric(e.sv.Metrics, fmt.Sprintf(`filescale_store_readonly{store="%s",addr="%s"}`, ro.id, ro.addr)); got != 0 {
		t.Errorf("readonly after restore = %v, want 0", got)
	}
	var spare *node
	for _, n := range e.nodes {
		if !slices.Contains(origFile.Stores, n.id) {
			spare = n
			break
		}
	}
	e.db.SetState(spare.id, "readonly")
	e.reload()
	e.mustPut("restored", random(100<<10))
	if f := e.file("restored"); !slices.Contains(f.Stores, ro.id) || !slices.Contains(f.Stores, rw.id) {
		t.Errorf("restored stores not picked: %v", f.Stores)
	}
}

func TestRepairWithReadOnlyStore(t *testing.T) {
	t.Run("live_readonly_source_not_target", func(t *testing.T) {
		e := newEnv(t, 14)
		ctx := context.Background()
		data := random(100 << 10)
		e.mustPut("f", data)
		f := e.file("f")

		var spares []*node
		for _, n := range e.nodes {
			if !slices.Contains(f.Stores, n.id) {
				spares = append(spares, n)
			}
		}
		spareRO, spareRW := spares[0], spares[1]
		lost := e.node(f.Stores[0])
		srcRO := e.node(f.Stores[1])

		e.db.SetState(spareRO.id, "readonly")
		e.db.SetState(srcRO.id, "readonly")
		e.kill(lost)
		e.db.Heartbeat(ctx, db.Store{ID: lost.id, Addr: lost.addr, LastSeen: time.Now().Add(-25 * time.Hour)})
		// kill 3 peers so repair must read from srcRO to have 8 shards
		for k := 2; k <= 4; k++ {
			e.kill(e.node(f.Stores[k]))
		}
		if err := e.sv.Repair(ctx, 24*time.Hour); err != nil {
			t.Fatal(err)
		}
		if got := e.file("f").Stores[0]; got != spareRW.id {
			t.Fatalf("repaired shard 0 to %s, want writable spare %s (readonly spare %s)", got, spareRW.id, spareRO.id)
		}
		e.kill(e.node(f.Stores[5]))
		e.reload()
		if resp, got := e.do("GET", "f", nil); resp.StatusCode != http.StatusOK || !bytes.Equal(got, data) {
			t.Errorf("GET through repaired and readonly shards: %s", resp.Status)
		}
	})

	t.Run("dead_readonly_repaired_and_forgotten", func(t *testing.T) {
		e := newEnv(t, 13)
		ctx := context.Background()
		data := random(100 << 10)
		e.mustPut("ro", data)
		f := e.file("ro")
		ro := e.node(f.Stores[0])

		e.db.SetState(ro.id, "readonly")
		e.kill(ro)
		e.db.Heartbeat(ctx, db.Store{ID: ro.id, Addr: ro.addr, LastSeen: time.Now().Add(-25 * time.Hour)})
		e.reload()

		if got := metric(e.sv.Metrics, fmt.Sprintf(`filescale_store_up{store="%s",addr="%s"}`, ro.id, ro.addr)); got != 0 {
			t.Errorf("dead readonly store_up = %v, want 0", got)
		}
		if got := metric(e.sv.Metrics, fmt.Sprintf(`filescale_store_readonly{store="%s",addr="%s"}`, ro.id, ro.addr)); got != 1 {
			t.Errorf("dead readonly store_readonly = %v, want 1", got)
		}

		if err := e.sv.Repair(ctx, 24*time.Hour); err != nil {
			t.Fatal(err)
		}
		f2 := e.file("ro")
		if f2.Stores[0] == ro.id {
			t.Fatal("dead readonly store shard not repaired")
		}
		if e.hasRow(ro.id, f.ID) || !e.hasRow(f2.Stores[0], f.ID) {
			t.Error("index row not moved off dead readonly store")
		}
		stores, _ := e.db.Stores(ctx)
		if slices.ContainsFunc(stores, func(s db.Store) bool { return s.ID == ro.id }) {
			t.Error("empty dead readonly store's row kept")
		}
		for k := 1; k <= 4; k++ {
			e.kill(e.node(f2.Stores[k]))
		}
		e.reload()
		if resp, got := e.do("GET", "ro", nil); resp.StatusCode != http.StatusOK || !bytes.Equal(got, data) {
			t.Errorf("GET through rebuilt shard: %s", resp.Status)
		}
	})
}

// hookDB runs its hooks before File and Shards queries, an error from onShards fails the query.
type hookDB struct {
	db.DB
	onFile   func()
	onShards func(bucket int) error
	onSet    func() (bool, error)
	stale    bool // File reads nil unless db.All, like a replica that hasn't got the row yet
}

func (h *hookDB) File(ctx context.Context, name string) (*db.File, error) {
	if h.onFile != nil {
		h.onFile()
	}
	if h.stale && !db.IsAll(ctx) {
		return nil, nil
	}
	return h.DB.File(ctx, name)
}

func (h *hookDB) Shards(ctx context.Context, store db.UUID, b int) ([]db.Shard, error) {
	if h.onShards != nil {
		if err := h.onShards(b); err != nil {
			return nil, err
		}
	}
	return h.DB.Shards(ctx, store, b)
}

func (h *hookDB) SetShardStore(ctx context.Context, name string, fileID db.UUID, shard int, store db.UUID) (bool, error) {
	if h.onSet != nil {
		return h.onSet()
	}
	return h.DB.SetShardStore(ctx, name, fileID, shard, store)
}

func newHookEnv(t *testing.T, stores int) (*env, *hookDB) {
	h := &hookDB{}
	return newEnv(t, stores, func(c *Config) { h.DB, c.DB = c.DB, h }), h
}

func TestRepairSkipsFailedBucket(t *testing.T) {
	e, h := newHookEnv(t, 13)
	ctx := context.Background()
	e.mustPut("f", random(100<<10))
	f := e.file("f")
	bad := 0 // before the file's bucket
	if db.Bucket(f.ID) == 0 {
		bad = 1
	}
	h.onShards = func(b int) error {
		if b == bad {
			return errors.New("query failed")
		}
		return nil
	}
	n := e.node(f.Stores[0])
	e.kill(n)
	e.db.Heartbeat(ctx, db.Store{ID: n.id, Addr: n.addr, LastSeen: time.Now().Add(-25 * time.Hour)})
	if err := e.sv.Repair(ctx, 24*time.Hour); err == nil || !strings.Contains(err.Error(), "failed buckets: 1,") {
		t.Errorf("err = %v", err)
	}
	if e.file("f").Stores[0] == n.id {
		t.Error("pass stopped at the failed bucket")
	}
}

func TestRepairLeavesStoreBackUp(t *testing.T) {
	e, h := newHookEnv(t, 13)
	ctx := context.Background()
	e.mustPut("f", random(100<<10))
	f := e.file("f")
	if db.Bucket(f.ID) == 0 {
		t.Skip("file in the first bucket, repaired before the store is back")
	}
	n := e.node(f.Stores[0])
	e.db.Heartbeat(ctx, db.Store{ID: n.id, Addr: n.addr, LastSeen: time.Now().Add(-25 * time.Hour)})
	back := false
	h.onShards = func(int) error {
		if !back { // back up as soon as the pass starts
			back = true
			n.st.Heartbeat(ctx, n.addr)
			e.reload()
		}
		return nil
	}
	if err := e.sv.Repair(ctx, 24*time.Hour); err != nil {
		t.Fatal(err)
	}
	if e.file("f").Stores[0] != n.id || !e.hasRow(n.id, f.ID) {
		t.Error("repaired a store that came back")
	}
	stores, _ := e.db.Stores(ctx)
	if !slices.ContainsFunc(stores, func(s db.Store) bool { return s.ID == n.id }) {
		t.Error("deleted a live store's row")
	}
}

// With every other server holding a shard, a store back up is the only place for its own shard.
func TestRepairOntoSameStore(t *testing.T) {
	e := newEnv(t, 12)
	ctx := context.Background()
	data := random(100 << 10)
	e.mustPut("f", data)
	f := e.file("f")
	n := e.node(f.Stores[0])
	rows, _ := e.db.Shards(ctx, n.id, db.Bucket(f.ID))
	if err := e.sv.repairFile(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if e.file("f").Stores[0] != n.id || !e.hasRow(n.id, f.ID) {
		t.Error("shard rebuilt in place lost its index row")
	}
	if resp, got := e.do("GET", "f", nil); resp.StatusCode != http.StatusOK || !bytes.Equal(got, data) {
		t.Errorf("GET: %s", resp.Status)
	}
}

// Two passes covering the same bucket (a serve restarted mid-pass, a split
// memberlist) can pick the same new store. The second update changes nothing
// but still applies, and must leave the index row alone.
func TestRepairTwiceSameStore(t *testing.T) {
	e, h := newHookEnv(t, 13)
	ctx := context.Background()
	data := random(100 << 10)
	e.mustPut("f", data)
	f := e.file("f")
	lost := e.node(f.Stores[0])
	e.kill(lost)
	e.db.Heartbeat(ctx, db.Store{ID: lost.id, Addr: lost.addr, LastSeen: time.Now().Add(-25 * time.Hour)})
	var spare *node
	for _, n := range e.nodes {
		if n.id != lost.id && !slices.Contains(f.Stores, n.id) {
			spare = n
		}
	}
	if spare == nil {
		t.Fatal("no spare store")
	}
	e.db.Heartbeat(ctx, db.Store{ID: spare.id, Addr: spare.addr, LastSeen: time.Now(), Free: 1 << 40})

	// both passes read the file before either moves the shard
	var mu sync.Mutex
	ready := 0
	release := make(chan struct{})
	h.onFile = func() {
		mu.Lock()
		ready++
		if ready == 2 {
			close(release)
		}
		mu.Unlock()
		<-release
	}
	two := New(Config{DB: h, Client: shard.NewClient(storeToken), DataShards: 8, ParityShards: 4, SmallFileSize: 32 << 10,
		SlowRead: time.Second, Alive: e.alive, Serves: func() ([]string, string) { return []string{"me"}, "me" }})
	if err := two.Reload(ctx); err != nil {
		t.Fatal(err)
	}
	e.reload()

	rows, err := e.db.Shards(ctx, lost.id, db.Bucket(f.ID))
	if err != nil || len(rows) == 0 {
		t.Fatalf("rows: %v, %v", rows, err)
	}
	errs := make([]error, 2)
	var wg sync.WaitGroup
	for i, sv := range []*Serve{e.sv, two} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = sv.repairFile(ctx, rows)
		}()
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}

	cur := e.file("f")
	if cur.Stores[0] != spare.id {
		t.Fatalf("shard 0 on %v, want the only store with room %v", cur.Stores[0], spare.id)
	}
	if !e.hasRow(spare.id, f.ID) {
		t.Fatal("the second pass deleted the rebuilt shard's index row")
	}
	if e.hasRow(lost.id, f.ID) {
		t.Error("the lost store kept its row")
	}
	if resp, got := e.do("GET", "f", nil); resp.StatusCode != http.StatusOK || !bytes.Equal(got, data) {
		t.Errorf("GET: %s", resp.Status)
	}
}

// Upload A's old-file lookup hangs while B overwrites the name: whichever wins keeps its index.
func TestOverwriteRace(t *testing.T) {
	e, h := newHookEnv(t, 12)
	started, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	h.onFile = func() {
		if calls.Add(1) == 1 {
			close(started)
			<-release
		}
	}
	a, b := random(100<<10), random(100<<10)
	done := make(chan struct{})
	go func() {
		defer close(done)
		if resp, _ := e.do("PUT", "f", a); resp.StatusCode != http.StatusCreated {
			t.Errorf("PUT A: %s", resp.Status)
		}
	}()
	<-started
	e.mustPut("f", b)
	fb := e.file("f")
	close(release)
	<-done

	cur := e.file("f")
	if cur.ID == fb.ID {
		t.Fatal("A wrote its files row before its old-file lookup returned")
	}
	waitFor(t, "B unindexed", func() bool { return !e.anyRow(fb) })
	if !e.allRows(cur) {
		t.Fatal("live file lost index rows")
	}
	if resp, got := e.do("GET", "f", nil); resp.StatusCode != http.StatusOK || !bytes.Equal(got, a) {
		t.Errorf("GET: %s", resp.Status)
	}

	// a file the name still holds is never unindexed (a newer write lost to clock skew)
	e.sv.unindex(context.Background(), cur)
	if !e.allRows(cur) {
		t.Error("unindexed a live file")
	}
}

func TestRepairDropsOrphans(t *testing.T) {
	e := newEnv(t, 12)
	ctx := context.Background()
	n := e.nodes[0]
	e.kill(n)
	e.db.Heartbeat(ctx, db.Store{ID: n.id, Addr: n.addr, LastSeen: time.Now().Add(-25 * time.Hour)})
	orphan, _ := db.NewID()
	e.db.Clock = func() time.Time { return time.Now().Add(-25 * time.Hour) }
	e.db.PutShard(ctx, db.Shard{StoreID: n.id, FileID: orphan, Name: "never-committed"})
	e.db.Clock = nil
	if err := e.sv.Repair(ctx, 24*time.Hour); err != nil {
		t.Fatal(err)
	}
	if e.hasRow(n.id, orphan) {
		t.Error("orphan row kept")
	}
}

func TestRepairDropsStaleMovedRow(t *testing.T) {
	e := newEnv(t, 13)
	ctx := context.Background()
	data := random(100 << 10)
	e.mustPut("moved", data)
	f := e.file("moved")
	var spare *node
	for _, n := range e.nodes {
		if !slices.Contains(f.Stores, n.id) {
			spare = n
		}
	}
	// Stale row on a down store for a shard that now lives on another store.
	e.kill(spare)
	e.db.Heartbeat(ctx, db.Store{ID: spare.id, Addr: spare.addr, LastSeen: time.Now().Add(-25 * time.Hour)})
	e.db.Clock = func() time.Time { return time.Now().Add(-25 * time.Hour) }
	e.db.PutShard(ctx, db.Shard{StoreID: spare.id, FileID: f.ID, Index: 0, Name: "moved"})
	e.db.Clock = nil
	if err := e.sv.Repair(ctx, 24*time.Hour); err != nil {
		t.Fatal(err)
	}
	if e.hasRow(spare.id, f.ID) {
		t.Error("stale row on down store kept")
	}
	if !slices.Equal(e.file("moved").Stores, f.Stores) {
		t.Error("repair moved a healthy shard because of a stale row")
	}
}

// Shards of a live file must not be dropped as orphans when a replica hasn't got the files row.
func TestRepairStaleRead(t *testing.T) {
	e, h := newHookEnv(t, 13)
	ctx := context.Background()
	e.db.Clock = func() time.Time { return time.Now().Add(-25 * time.Hour) }
	e.mustPut("f", random(100<<10))
	e.db.Clock = nil
	n := e.node(e.file("f").Stores[0])
	h.stale = true
	e.kill(n)
	e.db.Heartbeat(ctx, db.Store{ID: n.id, Addr: n.addr, LastSeen: time.Now().Add(-25 * time.Hour)})
	e.reload()

	if err := e.sv.Repair(ctx, 24*time.Hour); err != nil {
		t.Fatal(err)
	}
	h.stale = false
	if have, want := e.rows(e.file("f")); have != want {
		t.Errorf("%d of %d rows after repair, shards dropped on a stale read", have, want)
	}
}

// An empty file is only its files row: no shards to lose or repair, reads need no store.
func TestEmptyFile(t *testing.T) {
	e := newEnv(t, 12)
	e.mustPut("empty", []byte{})
	if f := e.file("empty"); len(f.Stores) != 0 {
		t.Fatalf("stored on %d stores", len(f.Stores))
	}
	for _, n := range e.nodes {
		e.kill(n)
	}
	e.reload()
	for _, m := range []string{"GET", "HEAD"} {
		if resp, got := e.do(m, "empty", nil); resp.StatusCode != http.StatusOK || len(got) != 0 || resp.ContentLength != 0 {
			t.Errorf("%s with every store down: %s, %d bytes", m, resp.Status, len(got))
		}
	}
}

func TestCorruptMetadata(t *testing.T) {
	e := newEnv(t, 12)
	ctx := context.Background()
	okData := random(10 << 10)
	e.mustPut("ok", okData)

	// Doubled and truncated stores return 500 without leaking file headers or panicking.
	for _, tc := range []struct {
		name   string
		stores int
	}{
		{"doubled", 10},
		{"short", 3},
	} {
		e.mustPut(tc.name, random(10<<10))
		f := e.file(tc.name)
		bad := *f
		bad.Stores = make([]db.UUID, tc.stores)
		copy(bad.Stores, f.Stores)
		e.db.PutFile(ctx, &bad)
		for _, m := range []string{"GET", "HEAD"} {
			resp, _ := e.do(m, tc.name, nil)
			if resp.StatusCode != http.StatusInternalServerError {
				t.Errorf("%s %s: %s, want 500", m, tc.name, resp.Status)
			}
			if resp.Header.Get("ETag") != "" || resp.Header.Get("Content-Disposition") != "" {
				t.Errorf("%s %s: leaked file headers %v", m, tc.name, resp.Header)
			}
		}
	}
	if resp, got := e.do("GET", "ok", nil); resp.StatusCode != http.StatusOK || !bytes.Equal(got, okData) {
		t.Fatalf("GET ok after corrupt reads: %s", resp.Status)
	}

	// DELETE on a corrupt row doesn't panic in unindex and leaves shards_by_store untouched.
	e.mustPut("del", random(10<<10))
	f := e.file("del")
	bad := *f
	bad.Stores = append(slices.Clone(f.Stores), f.Stores...)
	e.db.PutFile(ctx, &bad)
	if resp, _ := e.do("DELETE", "del", nil); resp.StatusCode != http.StatusNoContent {
		t.Fatalf("DELETE corrupt: %s", resp.Status)
	}
	e.sv.unindex(ctx, &bad)
	if have, want := e.rows(f); have != want {
		t.Errorf("unindex deleted shards of corrupt row: %d of %d rows left", have, want)
	}
}

func TestRepairSkipsCorruptMetadata(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*db.File)
	}{
		{"doubled", func(f *db.File) {
			pad := make([]db.UUID, len(f.Stores))
			for i := range pad {
				pad[i], _ = db.NewID()
			}
			f.Stores = append(pad, f.Stores...)
		}},
		{"short", func(f *db.File) { f.Stores = f.Stores[:2] }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t, 13)
			ctx := context.Background()
			e.db.Clock = func() time.Time { return time.Now().Add(-25 * time.Hour) }
			e.mustPut("bad", random(10<<10))
			e.db.Clock = nil
			orig := e.file("bad")
			bad := *orig
			bad.Stores = slices.Clone(orig.Stores)
			tc.mutate(&bad)
			e.db.PutFile(ctx, &bad)

			n := e.node(orig.Stores[0])
			e.kill(n)
			e.db.Heartbeat(ctx, db.Store{ID: n.id, Addr: n.addr, LastSeen: time.Now().Add(-25 * time.Hour)})
			e.reload()

			if err := e.sv.Repair(ctx, 24*time.Hour); err != nil {
				t.Fatal(err)
			}
			if got := metric(e.sv.Metrics, "filescale_repair_errors_total"); got != 1 {
				t.Errorf("repair_errors = %v, want 1", got)
			}
			if have, want := e.rows(orig); have != want {
				t.Errorf("repair dropped shards of corrupt row: %d of %d rows left", have, want)
			}
		})
	}
}

// Losing more stores than parity mid-download cuts the body off: the client gets an error
// after a correct prefix, never wrong bytes.
func TestDownloadCutOff(t *testing.T) {
	e := newEnv(t, 12)
	data := random(16 << 20) // 32 stripes
	e.mustPut("cut", data)
	f := e.file("cut")
	for _, n := range e.nodes {
		n.pace.Store(int64(50 * time.Millisecond)) // ~1.6s for the whole read
	}
	resp, err := http.Get(e.url + "/cut")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	got := make([]byte, 512<<10)
	if _, err := io.ReadFull(resp.Body, got); err != nil {
		t.Fatal(err)
	}
	for k := range 5 { // 7 left, 8 needed
		n := e.node(f.Stores[k])
		n.srv.CloseClientConnections()
		e.kill(n)
	}
	rest, err := io.ReadAll(resp.Body)
	got = append(got, rest...)
	if err == nil || len(got) == len(data) {
		t.Errorf("body ended cleanly at %d of %d bytes", len(got), len(data))
	}
	if !bytes.Equal(got, data[:len(got)]) {
		t.Error("wrong bytes before the cut")
	}
	if got := metric(e.sv.Metrics, "filescale_download_cut_total"); got != 1 {
		t.Errorf("cut counter = %v, want 1", got)
	}
}

// A whole download isn't a cut.
func TestDownloadCutNotCounted(t *testing.T) {
	e := newEnv(t, 12)
	data := random(1 << 20)
	e.mustPut("whole", data)
	if resp, got := e.do("GET", "whole", nil); resp.StatusCode != http.StatusOK || !bytes.Equal(got, data) {
		t.Fatalf("GET: %s", resp.Status)
	}
	if got := metric(e.sv.Metrics, "filescale_download_cut_total"); got != 0 {
		t.Errorf("cut counter = %v, want 0", got)
	}
}

// Within a bucket, repair takes the files missing the most shards first.
func TestRepairMostMissingFirst(t *testing.T) {
	rec := &fileLog{}
	e := newEnv(t, 0, func(c *Config) { rec.DB, c.DB = c.DB, rec })
	ctx := context.Background()
	d1, d2 := newUUID(t), newUUID(t)
	for _, d := range []db.UUID{d1, d2} {
		e.db.Heartbeat(ctx, db.Store{ID: d, Addr: "10.0.0.1:1", LastSeen: time.Now().Add(-25 * time.Hour)})
	}
	e.reload()
	bucket := newUUID(t)
	for i := range 12 { // the even ones lost a shard on each dead store, the odd ones on one of them
		id := newUUID(t)
		id[0], id[1] = bucket[0], bucket[1]
		name := fmt.Sprintf("f%02d", i)
		if i%2 == 0 || i%4 == 1 {
			e.db.PutShard(ctx, db.Shard{StoreID: d1, FileID: id, Name: name})
		}
		if i%2 == 0 || i%4 == 3 {
			e.db.PutShard(ctx, db.Shard{StoreID: d2, FileID: id, Index: 1, Name: name})
		}
	}
	if err := e.sv.Repair(ctx, 24*time.Hour); err != nil {
		t.Fatal(err)
	}
	if len(rec.names) != 12 {
		t.Fatalf("repair looked up %v", rec.names)
	}
	for i, name := range rec.names {
		if (i < 6) != ((name[2]-'0')%2 == 0) {
			t.Fatalf("repair order %v, want the files missing 2 shards first", rec.names)
		}
	}
}

// fileLog records the files looked up, in order.
type fileLog struct {
	db.DB
	mu    sync.Mutex
	names []string
}

func (l *fileLog) File(ctx context.Context, name string) (*db.File, error) {
	l.mu.Lock()
	l.names = append(l.names, name)
	l.mu.Unlock()
	return l.DB.File(ctx, name)
}

func newUUID(t *testing.T) db.UUID {
	id, err := db.NewID()
	if err != nil {
		t.Fatal(err)
	}
	return id
}

const storeToken = "store-token"

// env is a serve in front of real stores over HTTP, with an in-memory DB and a
// fake memberlist.
type env struct {
	t     testing.TB
	db    *db.Mem
	nodes []*node
	sv    *Serve
	url   string
	mu    sync.Mutex
	dead  map[string]bool
}

type node struct {
	st       *store.Store
	srv      *httptest.Server
	dir      string
	addr     string
	id       db.UUID
	slow     atomic.Bool  // GETs hang until the serve gives up
	hiccup   atomic.Int64 // ns, the next GET stalls this long before its first byte
	pace     atomic.Int64 // ns per 64 KiB a GET sends
	sent     atomic.Int64 // bytes GETs sent
	failPut  atomic.Int64 // PUTs die after this many bytes, 0 = they don't
	failFrom atomic.Int64 // failPut applies only from this PUT on, counting all PUTs
	putN     atomic.Int64 // PUTs seen, for failFrom
	mute     atomic.Bool  // PUTs never answer 100 Continue
	stallPut atomic.Bool  // PUTs read 1 KiB, then nothing for longer than the stall timer
	noCommit atomic.Bool  // PUTs read everything and never answer, even a 201
	unread   atomic.Bool  // PUTs answer 201 without reading the shard
}

// meter counts what a store sends and can stall or pace it.
type meter struct {
	http.ResponseWriter
	n *node
}

func (m *meter) Write(p []byte) (int, error) {
	if d := m.n.hiccup.Swap(0); d > 0 {
		time.Sleep(time.Duration(d))
	}
	if d := m.n.pace.Load(); d > 0 {
		time.Sleep(time.Duration(d * int64(len(p)) / erasure.BlockSize))
	}
	n, err := m.ResponseWriter.Write(p)
	m.n.sent.Add(int64(n))
	return n, err
}

func (m *meter) Unwrap() http.ResponseWriter { return m.ResponseWriter }

func newEnv(t testing.TB, stores int, opts ...func(*Config)) *env {
	e := &env{t: t, db: db.NewMem(), dead: map[string]bool{}}
	for range stores {
		e.addStore(5)
	}
	cfg := Config{DB: e.db, Client: shard.NewClient(storeToken), DataShards: 8, ParityShards: 4, SmallFileSize: 32 << 10,
		SlowRead: time.Second, Alive: e.alive, Serves: func() ([]string, string) { return []string{"me"}, "me" }}
	for _, o := range opts {
		o(&cfg)
	}
	e.sv = New(cfg)
	e.reload()
	srv := newServer(t, e.sv)
	t.Cleanup(srv.Close)
	e.url = srv.URL
	return e
}

func (e *env) addStore(minFree int64) *node {
	n := &node{dir: e.t.TempDir()}
	st, err := store.New(store.Config{Dirs: []string{n.dir}, MinFree: minFree, Token: storeToken, DB: e.db, Client: shard.NewClient(storeToken)})
	if err != nil {
		e.t.Fatal(err)
	}
	n.st, n.id = st, st.IDs()[0]
	n.srv = newServer(e.t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			if n.slow.Load() {
				<-r.Context().Done()
				return
			}
			w = &meter{w, n}
		}
		if r.Method == http.MethodPut {
			n.putN.Add(1)
			if n.mute.Load() { // with the body unread the server never sees the serve hang up
				time.Sleep(5 * time.Second)
				return
			}
			if n.unread.Load() {
				w.WriteHeader(http.StatusCreated)
				return
			}
			if left := n.failPut.Load(); left > 0 && (n.failFrom.Load() == 0 || n.putN.Load() >= n.failFrom.Load()) {
				r.Body = &dying{r.Body, left}
			}
			if n.stallPut.Load() {
				r.Body = &stalled{r.Body, 1024, shard.Stall + 5*time.Second}
			}
			if n.noCommit.Load() {
				io.Copy(io.Discard, r.Body) // the first read answers 100 Continue
				<-r.Context().Done()        // took everything, never answers
				return
			}
		}
		st.ServeHTTP(w, r)
	}))
	e.t.Cleanup(n.srv.Close)
	n.addr = n.srv.Listener.Addr().String()
	st.Heartbeat(context.Background(), n.addr)
	e.nodes = append(e.nodes, n)
	return n
}

func (e *env) alive() map[string]bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	m := map[string]bool{}
	for _, n := range e.nodes {
		m[n.addr] = !e.dead[n.addr]
	}
	return m
}

// kill stops a store and drops it from memberlist.
func (e *env) kill(n *node) {
	n.srv.Close()
	e.mu.Lock()
	e.dead[n.addr] = true
	e.mu.Unlock()
}

func (e *env) reload() {
	if err := e.sv.Reload(context.Background()); err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) node(id db.UUID) *node {
	for _, n := range e.nodes {
		if n.id == id {
			return n
		}
	}
	e.t.Fatalf("no store %s", id)
	return nil
}

func (e *env) file(name string) *db.File {
	f, err := e.db.File(context.Background(), name)
	if err != nil || f == nil {
		e.t.Fatalf("file %s: %v, %v", name, f, err)
	}
	return f
}

// shardPath is where shard k of f lives, f a single-part file or a part from f.Part.
func (e *env) shardPath(f *db.File, k int) string {
	id := f.ID.String()
	return filepath.Join(e.node(f.Stores[k]).dir, id[0:2], id[2:4], fmt.Sprintf("%s.%d", id, k))
}

func (e *env) hasRow(store, file db.UUID) bool {
	rows, _ := e.db.Shards(context.Background(), store, db.Bucket(file))
	return slices.ContainsFunc(rows, func(r db.Shard) bool { return r.FileID == file })
}

// rows counts the index rows of every part of f.
func (e *env) rows(f *db.File) (have, want int) {
	for i := range f.NumParts() {
		p := f.Part(i)
		for _, s := range p.Stores {
			if e.hasRow(s, p.ID) {
				have++
			}
		}
		want += len(p.Stores)
	}
	return have, want
}

func (e *env) anyRow(f *db.File) bool {
	have, _ := e.rows(f)
	return have > 0
}

func (e *env) allRows(f *db.File) bool {
	have, want := e.rows(f)
	return have == want
}

func (e *env) do(method, name string, body []byte, hdr ...string) (*http.Response, []byte) {
	e.t.Helper()
	return request(e.t, method, e.url+"/"+name, body, hdr...)
}

func (e *env) mustPut(name string, data []byte) {
	e.t.Helper()
	if resp, b := e.do("PUT", name, data); resp.StatusCode != http.StatusCreated {
		e.t.Fatalf("PUT %s: %s %s", name, resp.Status, b)
	}
}

func request(t testing.TB, method, url string, body []byte, hdr ...string) (*http.Response, []byte) {
	t.Helper()
	var r io.Reader
	if body != nil {
		r = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, url, r)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Logf("%s %s: body cut off: %v", method, url, err)
	}
	return resp, b
}

func random(n int) []byte {
	b := make([]byte, n)
	rand.NewChaCha8([32]byte{byte(n), byte(n >> 8), byte(n >> 16)}).Read(b)
	return b
}

func flip(t *testing.T, path string, off int) {
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	b[off] ^= 0xff
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatal(err)
	}
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

func TestEmptyStore(t *testing.T) {
	e := newEnv(t, 0)
	ctx := context.Background()
	store, _ := db.NewID()
	if !e.sv.empty(ctx, store) {
		t.Error("store with no rows reported non-empty")
	}
	file, _ := db.ParseID("ffff0000-0000-4000-8000-000000000000") // the last bucket
	e.db.PutShard(ctx, db.Shard{StoreID: store, FileID: file, Name: "x"})
	if e.sv.empty(ctx, store) {
		t.Error("row in bucket 0xffff missed")
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	other, _ := db.NewID()
	if e.sv.empty(cancelled, other) {
		t.Error("an interrupted check reported empty")
	}
}

// dying is a PUT body that breaks partway, like a store whose disk or process dies mid-shard.
type dying struct {
	io.ReadCloser
	left int64
}

func (d *dying) Read(p []byte) (int, error) {
	if d.left <= 0 && len(p) > 0 {
		return 0, errors.New("store died mid-shard")
	}
	n, err := d.ReadCloser.Read(p[:min(int64(len(p)), d.left)])
	d.left -= int64(n)
	return n, err
}

// Early answers reach a client that waits for 100 Continue before sending the body (curl does
// for big files, Go clients always): an answer that first waited to read the body would never come.
func TestPutRejectsBeforeContinue(t *testing.T) {
	e := newEnv(t, 12, func(c *Config) { c.Token = "tok" })
	if code := expectPut(t, e.url+"/x", 4<<20, "Authorization", "Bearer wrong"); code != http.StatusUnauthorized {
		t.Errorf("bad token: %d", code)
	}
	if code := expectPut(t, e.url+"/"+strings.Repeat("n", 1025), 4<<20, "Authorization", "Bearer tok"); code != http.StatusBadRequest {
		t.Errorf("long name: %d", code)
	}
	few := newEnv(t, 11)
	if code := expectPut(t, few.url+"/x", 4<<20); code != http.StatusServiceUnavailable {
		t.Errorf("11 servers for 8+4: %d", code)
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

// A failed upload must let go of every store connection: Transport waits for the body reader before returning.
// Here one store refuses with no spare server, after the other 11 already said 100 Continue.
func TestFailedUploadLeaksNothing(t *testing.T) {
	e := newEnv(t, 11)
	full := e.addStore(100)
	e.db.Heartbeat(context.Background(), db.Store{ID: full.id, Addr: full.addr, Free: 1 << 60, LastSeen: time.Now()})
	e.reload()
	base := runtime.NumGoroutine()
	for i := range 5 {
		resp, b := e.do("PUT", fmt.Sprintf("x%d", i), random(2<<20))
		if resp.StatusCode != http.StatusServiceUnavailable || !strings.Contains(string(b), "disk full, and no spare server to swap in") {
			t.Fatalf("PUT: %s %s", resp.Status, b)
		}
	}
	waitFor(t, "store connections of failed uploads released", func() bool { return runtime.NumGoroutine() <= base+10 })
	if t.Failed() {
		t.Logf("goroutines: %d before, %d after", base, runtime.NumGoroutine())
	}
}

// newServer is httptest.NewServer on the listener production uses (segments capped at a jumbo frame).
func newServer(t testing.TB, h http.Handler) *httptest.Server {
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

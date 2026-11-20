package serve

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"maps"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zeebo/blake3"

	"filescale/internal/db"
)

// TestSoak drives a running serve for hours with traffic it can check byte for byte, and
// logs a summary every hour. Wrong data fails the test; failed requests are only counted,
// faults injected alongside it cause them.
//
//	FILESCALE_SOAK=http://serve:port FILESCALE_SOAK_HOURS=48 FILESCALE_SOAK_RATE=10 \
//	  FILESCALE_SOAK_START=<unix time> go test -run '^TestSoak$' -v -timeout 50h ./internal/serve
func TestSoak(t *testing.T) {
	base := os.Getenv("FILESCALE_SOAK")
	if base == "" {
		t.Skip("set FILESCALE_SOAK=<serve url>")
	}
	env := func(k string, def float64) float64 {
		if v, err := strconv.ParseFloat(os.Getenv(k), 64); err == nil {
			return v
		}
		return def
	}
	start := time.Now()
	if s := env("FILESCALE_SOAK_START", 0); s > 0 {
		start = time.Unix(int64(s), 0)
		time.Sleep(time.Until(start))
	}
	cfg := soakConfig{
		run:          time.Duration(env("FILESCALE_SOAK_HOURS", 48) * float64(time.Hour)),
		rate:         env("FILESCALE_SOAK_RATE", 10) * 1e6,
		files:        int(env("FILESCALE_SOAK_FILES", 2000)),
		workers:      8,
		report:       time.Hour,
		burstAt:      30 * time.Minute,
		burstEvery:   time.Hour,
		burstFor:     time.Duration(env("FILESCALE_SOAK_BURST_MIN", 5) * float64(time.Minute)),
		burstWorkers: 100,
		size:         soakSize,
		big:          func() int64 { return 130<<20 + rand.Int64N(170<<20) }, // 3-5 parts
	}
	runSoak(t, base, start, cfg)
}

// TestSoakSelf runs the soak driver for a few seconds against a serve here, so a driver bug
// doesn't show up as wrong data hours into a real run.
func TestSoakSelf(t *testing.T) {
	if testing.Short() {
		t.Skip("timing")
	}
	smallParts(t)
	e := newEnv(t, 12, func(c *Config) { c.ParityShards = 3 })
	cfg := soakConfig{
		run: 20 * time.Second, rate: 50e6, files: 40, workers: 8, report: 10 * time.Second,
		burstAt: 5 * time.Second, burstEvery: 10 * time.Second, burstFor: 2 * time.Second, burstWorkers: 10,
		size: func() int64 { return soakSize() / 16 },
		big:  func() int64 { return 2<<20 + rand.Int64N(3<<20) }, // 3-5 parts of 1 MiB
	}
	s := runSoak(t, e.url, time.Now(), cfg)
	for op, st := range s.total {
		if st.avail > 0 {
			t.Errorf("%s: %d failed requests with nothing broken", op, st.avail)
		}
	}
}

// TestSoakDrop drops the soak's keyspace afterwards: FILESCALE_SOAK_DROP=<cassandra host>/<keyspace>.
func TestSoakDrop(t *testing.T) {
	host, ks, ok := strings.Cut(os.Getenv("FILESCALE_SOAK_DROP"), "/")
	if !ok {
		t.Skip("set FILESCALE_SOAK_DROP=<host>/<keyspace>")
	}
	dropKeyspace(t, host, ks)
}

type soakConfig struct {
	run, report                   time.Duration
	rate                          float64 // bytes/s of the steady load
	files, workers                int
	burstAt, burstEvery, burstFor time.Duration
	burstWorkers                  int
	size, big                     func() int64 // an upload's size, a resumable one's
}

func soakSize() int64 {
	switch r := rand.IntN(100); {
	case r < 1:
		return 0
	case r < 25:
		return 1 + rand.Int64N(2<<10) // ~1 KiB, copies
	case r < 55:
		return 50<<10 + rand.Int64N(100<<10) // ~100 KiB, copies
	case r < 90:
		return 3<<20 + rand.Int64N(2<<20) // ~4 MiB
	case r < 98:
		return 60<<20 + rand.Int64N(8<<20) // one or two parts
	default:
		return 64<<20 + rand.Int64N(64<<20) // two parts
	}
}

const soakBlock = 64 << 10

// soakContent fills b with bytes [off, off+len(b)) of version v of name: 64 KiB blocks, each
// its own ChaCha8 stream, so any range can be made again to check it.
func soakContent(b []byte, name string, v, off int64) {
	blk := make([]byte, soakBlock)
	for len(b) > 0 {
		i := off / soakBlock
		rand.NewChaCha8(sha256.Sum256(fmt.Appendf(nil, "%s\x00%d\x00%d", name, v, i))).Read(blk)
		n := copy(b, blk[off-i*soakBlock:])
		b, off = b[n:], off+int64(n)
	}
}

func soakSum(name string, v, size int64) [32]byte {
	h, buf := blake3.New(), make([]byte, 1<<20)
	for off := int64(0); off < size; off += int64(len(buf)) {
		n := min(int64(len(buf)), size-off)
		soakContent(buf[:n], name, v, off)
		h.Write(buf[:n])
	}
	var sum [32]byte
	h.Sum(sum[:0])
	return sum
}

// soakBody is an upload's body from off to end; cut >= 0 fails it after cut more bytes.
type soakBody struct {
	name        string
	v, off, end int64
	cut         int64
}

func (r *soakBody) Read(p []byte) (int, error) {
	if r.off >= r.end {
		return 0, io.EOF
	}
	if r.cut == 0 {
		return 0, errors.New("cut off on purpose")
	}
	n := min(int64(len(p)), r.end-r.off)
	if r.cut > 0 {
		n = min(n, r.cut)
		r.cut -= n
	}
	soakContent(p[:n], r.name, r.v, r.off)
	r.off += n
	return int(n), nil
}

// soakFile is what the soak knows is stored under name. A write with no clear answer leaves
// it pending until a HEAD tells which version is there.
type soakFile struct {
	mu      sync.Mutex
	name    string
	v, size int64 // v 0 = none
	sum     [32]byte
	pending bool // the write below may or may not have happened
	pv      int64
	psize   int64
	psum    [32]byte
}

type soakStat struct {
	ok, avail, bad, bytes int64
	lat                   []time.Duration
	errs                  map[string]int
}

type soak struct {
	t     *testing.T
	ctx   context.Context // done = the run is over
	base  string
	c     *http.Client
	ver   atomic.Int64
	bad   atomic.Int64
	mu    sync.Mutex
	hour  map[string]*soakStat // since the last report
	total map[string]*soakStat

	limMu sync.Mutex
	next  time.Time
	rate  float64
}

func runSoak(t *testing.T, base string, start time.Time, cfg soakConfig) *soak {
	s := &soak{t: t, base: base, rate: cfg.rate, hour: map[string]*soakStat{}, total: map[string]*soakStat{},
		c: &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 512, IdleConnTimeout: 90 * time.Second,
			DialContext: (&net.Dialer{Timeout: 10 * time.Second}).DialContext}}}
	s.ver.Store(start.UnixNano()) // versions never repeat across runs
	ctx, stop := context.WithDeadline(context.Background(), start.Add(cfg.run))
	defer stop()
	s.ctx = ctx
	t.Logf("soak against %s until %s: %d files, %.0f MB/s, %d at once for %v every %v",
		base, start.Add(cfg.run).Format(time.RFC3339), cfg.files, cfg.rate/1e6, cfg.burstWorkers, cfg.burstFor, cfg.burstEvery)

	files := make([]*soakFile, cfg.files)
	for i := range files {
		files[i] = &soakFile{name: fmt.Sprintf("soak/%d", i)}
	}
	var wg sync.WaitGroup
	for range cfg.workers {
		wg.Go(func() {
			for ctx.Err() == nil {
				f := files[rand.IntN(len(files))]
				if !f.mu.TryLock() {
					continue
				}
				s.step(ctx, f, cfg)
				f.mu.Unlock()
			}
		})
	}
	wg.Go(func() { // bursts: each worker writes its own file and reads it back 9 times
		bfiles := make([]*soakFile, cfg.burstWorkers)
		for i := range bfiles {
			bfiles[i] = &soakFile{name: fmt.Sprintf("burst/%d", i)}
		}
		for at := start.Add(cfg.burstAt); ; at = at.Add(cfg.burstEvery) {
			if !sleepCtx(ctx, time.Until(at)) {
				return
			}
			end := time.Now().Add(cfg.burstFor)
			var bw sync.WaitGroup
			for _, f := range bfiles {
				bw.Go(func() {
					for time.Now().Before(end) && ctx.Err() == nil {
						s.put(ctx, "burst put", f, 4<<20)
						for range 9 {
							s.get(ctx, "burst get", f)
						}
					}
				})
			}
			bw.Wait()
		}
	})
	wg.Go(func() {
		for ctx.Err() == nil {
			if sleepCtx(ctx, cfg.report) {
				s.report("hour", s.swap())
			}
		}
	})
	wg.Wait()
	s.report("last hour", s.swap())
	s.report("whole run", s.total)
	if n := s.bad.Load(); n > 0 {
		t.Errorf("%d wrong answers", n)
	}
	return s
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// wait holds the steady load to its rate in bytes.
func (s *soak) wait(ctx context.Context, n int64) {
	s.limMu.Lock()
	now := time.Now()
	if s.next.Before(now) {
		s.next = now
	}
	at := s.next
	s.next = s.next.Add(time.Duration(float64(n) / s.rate * float64(time.Second)))
	s.limMu.Unlock()
	sleepCtx(ctx, time.Until(at))
}

func (s *soak) step(ctx context.Context, f *soakFile, cfg soakConfig) {
	if f.pending && !s.resync(ctx, f) {
		return
	}
	r := rand.IntN(100)
	switch {
	case f.v == 0 && r < 90 || r >= 66 && r < 96:
		size := cfg.size()
		s.wait(ctx, size)
		s.put(ctx, "put", f, size)
	case r < 45:
		s.wait(ctx, f.size)
		s.get(ctx, "get", f)
	case r < 60:
		s.wait(ctx, min(f.size, 4<<20))
		s.rangeGet(ctx, f)
	case r < 66:
		s.head(ctx, f)
	case r < 99:
		s.del(ctx, f)
	default:
		size := cfg.big()
		s.wait(ctx, size*3/2) // a cut and the rest again
		s.resumable(ctx, f, size)
	}
}

// done counts a request: err nil = fine, else a failure; wrong = the data or an answer was wrong.
func (s *soak) done(op string, start time.Time, n int64, err error, wrong bool) {
	if err != nil && !wrong && s.ctx.Err() != nil {
		return // cut short by the end of the run
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, m := range []map[string]*soakStat{s.hour, s.total} {
		st := m[op]
		if st == nil {
			st = &soakStat{errs: map[string]int{}}
			m[op] = st
		}
		switch {
		case wrong:
			st.bad++
		case err != nil:
			st.avail++
			st.errs[errKind(err)]++
		default:
			st.ok++
			st.bytes += n
			st.lat = append(st.lat, time.Since(start))
		}
	}
	if wrong {
		s.bad.Add(1)
		s.t.Errorf("%s WRONG: %v", op, err)
	}
}

func errKind(err error) string {
	m := err.Error()
	for _, k := range []string{"connection refused", "connection reset", "EOF", "timeout", "deadline", "no 100 Continue",
		"servers", "failed mid-stream", "failed to commit", "shards reachable", "cut off"} {
		if strings.Contains(m, k) {
			return k
		}
	}
	return m[:min(len(m), 60)]
}

func (s *soak) swap() map[string]*soakStat {
	s.mu.Lock()
	defer s.mu.Unlock()
	h := s.hour
	s.hour = map[string]*soakStat{}
	return h
}

func (s *soak) report(what string, m map[string]*soakStat) {
	var b strings.Builder
	fmt.Fprintf(&b, "%s %s:", time.Now().Format("01-02 15:04"), what)
	for _, op := range slices.Sorted(maps.Keys(m)) {
		st := m[op]
		slices.Sort(st.lat)
		p := func(q float64) time.Duration {
			if len(st.lat) == 0 {
				return 0
			}
			return st.lat[int(q*float64(len(st.lat)-1))].Round(time.Millisecond)
		}
		fmt.Fprintf(&b, "\n  %-10s ok %6d  failed %4d  WRONG %d  %8.0f MB  p50 %7v  p99 %7v", op, st.ok, st.avail, st.bad, float64(st.bytes)/1e6, p(0.5), p(0.99))
		if len(st.errs) > 0 {
			fmt.Fprintf(&b, "  %v", st.errs)
		}
	}
	s.t.Log(b.String())
}

func (s *soak) url(f *soakFile) string { return s.base + "/" + f.name }

func (s *soak) req(ctx context.Context, method, url string, body io.Reader, hdr ...string) (*http.Response, error) {
	ctx, cancel := context.WithTimeout(ctx, 20*time.Minute)
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		cancel()
		return nil, err
	}
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	if b, ok := body.(*soakBody); ok {
		req.ContentLength = b.end - b.off
		if req.ContentLength == 0 {
			req.Body = http.NoBody // else Go sends it chunked, and the serve wants a Content-Length
		}
	}
	resp, err := s.c.Do(req)
	if err != nil {
		cancel()
		return nil, err
	}
	resp.Body = &cancelBody{resp.Body, cancel}
	return resp, nil
}

type cancelBody struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (c *cancelBody) Close() error { defer c.cancel(); return c.ReadCloser.Close() }

// status turns an unexpected status into an error, a 5xx is a failure and anything else a wrong answer.
func status(resp *http.Response) (wrong bool, err error) {
	msg, _ := io.ReadAll(io.LimitReader(resp.Body, 200))
	return resp.StatusCode < 500, fmt.Errorf("%s %s", resp.Status, bytes.TrimSpace(msg))
}

func (s *soak) get(ctx context.Context, op string, f *soakFile) {
	start := time.Now()
	if f.pending && !s.resync(ctx, f) {
		return
	}
	resp, err := s.req(ctx, "GET", s.url(f), nil)
	if err != nil {
		s.done(op, start, 0, err, false)
		return
	}
	defer resp.Body.Close()
	if f.v == 0 {
		if resp.StatusCode == http.StatusNotFound {
			s.done(op, start, 0, nil, false)
		} else {
			_, err := status(resp)
			s.done(op, start, 0, fmt.Errorf("%s: deleted, got %v", f.name, err), resp.StatusCode < 500)
		}
		return
	}
	if resp.StatusCode != http.StatusOK {
		wrong, err := status(resp)
		s.done(op, start, 0, fmt.Errorf("%s v%d: %w", f.name, f.v, err), wrong)
		return
	}
	h := blake3.New()
	n, err := io.Copy(h, resp.Body)
	if err != nil { // cut off: a store or the serve went away mid-read
		s.done(op, start, n, fmt.Errorf("%s: body cut at %d of %d: %w", f.name, n, f.size, err), false)
		return
	}
	if n != f.size || !bytes.Equal(h.Sum(nil), f.sum[:]) || resp.Header.Get("ETag") != etag(f.sum[:]) {
		s.done(op, start, n, fmt.Errorf("%s v%d: %d bytes, want %d, hash %x, want %x, ETag %s", f.name, f.v, n, f.size, h.Sum(nil), f.sum, resp.Header.Get("ETag")), true)
		return
	}
	s.done(op, start, n, nil, false)
}

func (s *soak) rangeGet(ctx context.Context, f *soakFile) {
	if f.v == 0 || f.size == 0 {
		s.get(ctx, "get", f)
		return
	}
	start := time.Now()
	a := rand.Int64N(f.size)
	b := a + rand.Int64N(min(f.size-a, 4<<20))
	resp, err := s.req(ctx, "GET", s.url(f), nil, "Range", fmt.Sprintf("bytes=%d-%d", a, b))
	if err != nil {
		s.done("range", start, 0, err, false)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusPartialContent {
		wrong, err := status(resp)
		s.done("range", start, 0, fmt.Errorf("%s v%d bytes %d-%d: %w", f.name, f.v, a, b, err), wrong)
		return
	}
	got, err := io.ReadAll(resp.Body)
	if err != nil {
		s.done("range", start, int64(len(got)), err, false)
		return
	}
	want := make([]byte, b-a+1)
	soakContent(want, f.name, f.v, a)
	if !bytes.Equal(got, want) {
		s.done("range", start, int64(len(got)), fmt.Errorf("%s v%d bytes %d-%d: wrong data, %d bytes", f.name, f.v, a, b, len(got)), true)
		return
	}
	s.done("range", start, int64(len(got)), nil, false)
}

func (s *soak) head(ctx context.Context, f *soakFile) {
	start := time.Now()
	resp, err := s.req(ctx, "HEAD", s.url(f), nil)
	if err != nil {
		s.done("head", start, 0, err, false)
		return
	}
	resp.Body.Close()
	switch {
	case f.v == 0 && resp.StatusCode == http.StatusNotFound,
		f.v != 0 && resp.StatusCode == http.StatusOK && resp.Header.Get("ETag") == etag(f.sum[:]) && resp.ContentLength == f.size:
		s.done("head", start, 0, nil, false)
	case resp.StatusCode >= 500:
		s.done("head", start, 0, errors.New(resp.Status), false)
	default:
		s.done("head", start, 0, fmt.Errorf("%s v%d: %s, ETag %s, length %d", f.name, f.v, resp.Status, resp.Header.Get("ETag"), resp.ContentLength), true)
	}
}

// resync asks which version a write with no clear answer left: the old one, the new one, or
// none after a delete. False if the serve can't say yet.
func (s *soak) resync(ctx context.Context, f *soakFile) bool {
	start := time.Now()
	resp, err := s.req(ctx, "HEAD", s.url(f), nil)
	if err != nil {
		s.done("resync", start, 0, err, false)
		return false
	}
	resp.Body.Close()
	tag := resp.Header.Get("ETag")
	switch {
	case resp.StatusCode >= 500:
		s.done("resync", start, 0, errors.New(resp.Status), false)
		return false
	case resp.StatusCode == http.StatusNotFound && (f.v == 0 || f.pv == 0):
		f.v = 0
	case resp.StatusCode == http.StatusOK && f.pv != 0 && tag == etag(f.psum[:]):
		f.v, f.size, f.sum = f.pv, f.psize, f.psum
	case resp.StatusCode == http.StatusOK && f.v != 0 && tag == etag(f.sum[:]):
	default:
		s.done("resync", start, 0, fmt.Errorf("%s: %s ETag %s is neither v%d nor v%d", f.name, resp.Status, tag, f.v, f.pv), true)
	}
	f.pending = false
	s.done("resync", start, 0, nil, false)
	return true
}

// pend marks a write that may have landed: pv 0 for a delete.
func (f *soakFile) pend(pv, psize int64, psum [32]byte) {
	f.pending, f.pv, f.psize, f.psum = true, pv, psize, psum
}

func (s *soak) put(ctx context.Context, op string, f *soakFile, size int64) {
	if f.pending && !s.resync(ctx, f) {
		return
	}
	start := time.Now()
	v := s.ver.Add(1)
	sum := soakSum(f.name, v, size)
	var hdr []string
	if rand.IntN(2) == 0 {
		hdr = []string{"X-Blake3", hex.EncodeToString(sum[:])}
	}
	resp, err := s.req(ctx, "PUT", s.url(f), &soakBody{name: f.name, v: v, end: size, cut: -1}, hdr...)
	if err != nil {
		f.pend(v, size, sum)
		s.done(op, start, 0, err, false)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		wrong, err := status(resp)
		f.pend(v, size, sum)
		s.done(op, start, 0, fmt.Errorf("%s: %w", f.name, err), wrong)
		return
	}
	if resp.Header.Get("ETag") != etag(sum[:]) {
		f.pend(v, size, sum)
		s.done(op, start, size, fmt.Errorf("%s v%d: ETag %s, want %s", f.name, v, resp.Header.Get("ETag"), etag(sum[:])), true)
		return
	}
	f.v, f.size, f.sum = v, size, sum
	s.done(op, start, size, nil, false)
}

func (s *soak) del(ctx context.Context, f *soakFile) {
	start := time.Now()
	resp, err := s.req(ctx, "DELETE", s.url(f), nil)
	if err != nil {
		f.pend(0, 0, [32]byte{})
		s.done("delete", start, 0, err, false)
		return
	}
	resp.Body.Close()
	switch {
	case f.v != 0 && resp.StatusCode == http.StatusNoContent, f.v == 0 && resp.StatusCode == http.StatusNotFound:
		f.v = 0
		s.done("delete", start, 0, nil, false)
	case resp.StatusCode >= 500:
		f.pend(0, 0, [32]byte{})
		s.done("delete", start, 0, errors.New(resp.Status), false)
	default:
		s.done("delete", start, 0, fmt.Errorf("%s v%d: %s", f.name, f.v, resp.Status), true)
	}
}

// resumable sends a new version with ?upload, cut off at a random byte, then resumes it from
// the offset HEAD gives. One in ten is left cut off, for the uploads TTL and store cleanup.
func (s *soak) resumable(ctx context.Context, f *soakFile, size int64) {
	start := time.Now()
	v := s.ver.Add(1)
	sum := soakSum(f.name, v, size)
	id, _ := db.NewID()
	u := s.url(f) + "?upload=" + id.String()
	if resp, err := s.req(ctx, "PUT", u, &soakBody{name: f.name, v: v, end: size, cut: rand.Int64N(size)}); err == nil {
		resp.Body.Close()
		s.done("resume", start, 0, fmt.Errorf("%s: a cut-off upload answered %s", f.name, resp.Status), resp.StatusCode == http.StatusCreated)
		return
	}
	if rand.IntN(10) == 0 {
		s.done("abandon", start, 0, nil, false)
		return
	}
	sleepCtx(ctx, time.Second) // the last whole part is recorded after its stores' 201s
	resp, err := s.req(ctx, "HEAD", u, nil)
	if err != nil {
		s.done("resume", start, 0, err, false)
		return
	}
	resp.Body.Close()
	var off int64
	switch resp.StatusCode {
	case http.StatusOK:
		off, _ = strconv.ParseInt(resp.Header.Get("Upload-Offset"), 10, 64)
	case http.StatusNotFound: // cut in the first part, nothing recorded
	default:
		s.done("resume", start, 0, fmt.Errorf("%s HEAD ?upload: %s", f.name, resp.Status), resp.StatusCode < 500)
		return
	}
	resp, err = s.req(ctx, "PUT", u, &soakBody{name: f.name, v: v, off: off, end: size, cut: -1},
		"Content-Range", fmt.Sprintf("bytes %d-%d/%d", off, size-1, size), "X-Blake3", hex.EncodeToString(sum[:]))
	if err != nil {
		f.pend(v, size, sum)
		s.done("resume", start, 0, err, false)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated || resp.Header.Get("ETag") != etag(sum[:]) {
		wrong, err := status(resp)
		f.pend(v, size, sum)
		s.done("resume", start, 0, fmt.Errorf("%s v%d from %d of %d: %w, ETag %s", f.name, v, off, size, err, resp.Header.Get("ETag")), wrong || resp.StatusCode == http.StatusCreated)
		return
	}
	f.v, f.size, f.sum = v, size, sum
	s.done("resume", start, size, nil, false)
}

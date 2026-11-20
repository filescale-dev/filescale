package serve

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/zeebo/blake3"

	"filescale/internal/b3"
	"filescale/internal/db"
	"filescale/internal/erasure"
	"filescale/internal/shard"
)

// smallParts cuts files into 1 MiB parts for the test.
func smallParts(t testing.TB) {
	old := minPart
	minPart = 1 << 20
	t.Cleanup(func() { minPart = old })
}

func sameStores(a, b []db.UUID) bool {
	return len(a) == len(b) && !slices.ContainsFunc(a, func(id db.UUID) bool { return !slices.Contains(b, id) })
}

func TestPartSize(t *testing.T) {
	for _, c := range []struct{ size, want int64 }{
		{0, 64 << 20},
		{64 << 20, 64 << 20},
		{64 << 30, 1 << 30},
		{64<<30 + 1, 1<<30 + 1<<20},
	} {
		if got := partSize(c.size); got != c.want || (c.size+got-1)/got > maxParts {
			t.Errorf("partSize(%d) = %d, want %d", c.size, got, c.want)
		}
	}
}

func TestByteRange(t *testing.T) {
	for _, c := range []struct {
		h          string
		start, end int64
	}{
		{"", 0, 100},
		{"bytes=10-19", 10, 20},
		{"bytes=90-", 90, 100},
		{"bytes=-10", 90, 100},
		{"bytes=-200", 0, 100},
		{"bytes=50-500", 50, 100},
		{"bytes=0-9223372036854775807", 0, 100},
		{"items=1-2", 0, 100},
	} {
		if s, e := byteRange(c.h, 100); s != c.start || e != c.end {
			t.Errorf("%q: %d-%d, want %d-%d", c.h, s, e, c.start, c.end)
		}
	}
}

// byteRange must cover every byte ServeContent reads for the same header: the shard
// streams the download opens end where it says.
func FuzzByteRange(f *testing.F) {
	for _, s := range []string{"", "bytes=10-19", "bytes=-10", "bytes=90-", "bytes=0-9223372036854775807",
		"bytes= 5 - 7", "bytes=", "bytes=50-10", "items=1-2"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, h string) {
		if strings.Contains(h, ",") {
			return // download drops a header with several ranges
		}
		const size = 100
		start, end := byteRange(h, size)
		req := httptest.NewRequest("GET", "/", nil)
		req.Header.Set("Range", h)
		w := httptest.NewRecorder()
		http.ServeContent(w, req, "", time.Time{}, bytes.NewReader(make([]byte, size)))
		var from, to int64
		switch w.Code {
		case http.StatusOK:
			from, to = 0, size
		case http.StatusPartialContent:
			var last, total int64
			if _, err := fmt.Sscanf(w.Header().Get("Content-Range"), "bytes %d-%d/%d", &from, &last, &total); err != nil {
				t.Fatalf("%q: Content-Range %q", h, w.Header().Get("Content-Range"))
			}
			to = last + 1
		default:
			return // 416, nothing read
		}
		if start > from || end < to {
			t.Fatalf("%q: byteRange %d-%d, ServeContent reads %d-%d", h, start, end, from, to)
		}
	})
}

// FuzzDownload compares a GET through the shards with http.ServeContent over the same bytes,
// for any Range, with or without If-Range, on files cut every way: copies, one short stripe,
// two stripes, three parts with a 7-byte last one.
func FuzzDownload(f *testing.F) {
	smallParts(f)
	e := newEnv(f, 12)
	sizes := []int{0, 1000, 70000, 512<<10 + 1, 2<<20 + 7}
	files := make([][]byte, len(sizes))
	for i, n := range sizes {
		files[i] = random(n)
		e.mustPut(fmt.Sprint("f", i), files[i])
	}
	for _, h := range []string{"", "bytes=0-0", "bytes=-10", "bytes=5-", "bytes=524287-524289",
		"bytes=1048570-1048590", "bytes=2097150-", "bytes=0-9223372036854775807", "bytes=0-1,5-9"} {
		for i := range files {
			f.Add(uint8(i), h, uint8(0))
		}
	}
	f.Add(uint8(4), "bytes=10-19", uint8(1))
	f.Add(uint8(4), "bytes=10-19", uint8(2))
	f.Fuzz(func(t *testing.T, which uint8, h string, ifRange uint8) {
		for _, c := range []byte(h) {
			if c < ' ' && c != '\t' || c == 0x7f {
				return // a server refuses the header
			}
		}
		i := int(which) % len(files)
		data := files[i]
		sum := blake3.Sum256(data)
		hdr := []string{"Range", h}
		switch ifRange % 3 {
		case 1:
			hdr = append(hdr, "If-Range", etag(sum[:]))
		case 2:
			hdr = append(hdr, "If-Range", `"stale"`)
		}
		resp, got := request(t, "GET", fmt.Sprintf("%s/f%d", e.url, i), nil, hdr...)

		req := httptest.NewRequest("GET", "/", nil)
		for j := 0; j < len(hdr); j += 2 {
			req.Header.Set(hdr[j], strings.Trim(hdr[j+1], " \t")) // as the server reads it
		}
		if strings.Contains(h, ",") {
			req.Header.Del("Range") // so does download
		}
		w := httptest.NewRecorder()
		w.Header().Set("ETag", etag(sum[:]))
		http.ServeContent(w, req, "", time.Time{}, bytes.NewReader(data))
		if resp.StatusCode != w.Code || resp.Header.Get("Content-Range") != w.Header().Get("Content-Range") || !bytes.Equal(got, w.Body.Bytes()) {
			t.Fatalf("f%d (%d bytes), Range %q, If-Range %d: %s %q, %d bytes; want %d %q, %d bytes", i, len(data), h, ifRange%3,
				resp.Status, resp.Header.Get("Content-Range"), len(got), w.Code, w.Header().Get("Content-Range"), w.Body.Len())
		}
	})
}

// recorded cuts the rows at the first one that doesn't continue one upload of total bytes.
func TestRecorded(t *testing.T) {
	state, err := b3.New().MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	part := func(i int, set func(*db.UploadPart)) db.UploadPart {
		p := db.UploadPart{Index: i, Total: 100, Size: 64 << 20, DataShards: 8, ParityShards: 4, HashState: state}
		if set != nil {
			set(&p)
		}
		return p
	}
	rows := []db.UploadPart{part(0, nil), part(1, nil)}
	if got := recorded(rows, 100); len(got) != 2 {
		t.Errorf("clean run kept %d rows", len(got))
	}
	for _, c := range []struct {
		name string
		set  func(*db.UploadPart)
		want int
	}{
		{"index gap", func(p *db.UploadPart) { p.Index = 3 }, 1},
		{"other total", func(p *db.UploadPart) { p.Total = 200 }, 1},
		{"other size", func(p *db.UploadPart) { p.Size = 32 << 20 }, 1},
		{"other shards", func(p *db.UploadPart) { p.DataShards = 10 }, 1},
		{"state from another build", func(p *db.UploadPart) { p.HashState = []byte("sha256 state") }, 1},
	} {
		rows := []db.UploadPart{part(0, nil), part(1, c.set)}
		if got := recorded(rows, 100); len(got) != c.want {
			t.Errorf("%s: kept %d rows, want %d", c.name, len(got), c.want)
		}
	}
}

func TestContentRange(t *testing.T) {
	for _, c := range []struct {
		h       string
		n       int64
		from    int64
		size    int64
		wantErr bool
	}{
		{"bytes 0-9/10", 10, 0, 10, false},
		{"bytes 5-9/10", 5, 5, 10, false},
		{"", 10, 0, 0, true},
		{"bytes 0-9", 10, 0, 0, true},
		{"bytes -1-9/10", 10, 0, 0, true},
		{"bytes 5-4/10", 10, 0, 0, true},
		{"bytes 0-9/11", 10, 0, 0, true},
		{"bytes 0-9/10", 9, 0, 0, true},
	} {
		from, size, err := contentRange(c.h, c.n)
		if c.wantErr && err == nil || !c.wantErr && err != nil {
			t.Errorf("%q: err = %v", c.h, err)
			continue
		}
		if !c.wantErr && (from != c.from || size != c.size) {
			t.Errorf("%q: %d, %d", c.h, from, size)
		}
	}
}

// The invariants contentRange's callers rely on, over arbitrary input.
func FuzzContentRange(f *testing.F) {
	for _, s := range []string{"bytes 0-9/10", "bytes 5-9/10", "bytes -1-9/10", "bytes 0-9/11", ""} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, h string) {
		from, size, err := contentRange(h, 10)
		if err != nil {
			return
		}
		if from < 0 || size-from != 10 {
			t.Fatalf("%q parsed as from %d, size %d", h, from, size)
		}
	})
}

func TestParts(t *testing.T) {
	smallParts(t)
	e := newEnv(t, 14)
	base := runtime.NumGoroutine()
	data := random(3<<20 + 12345)
	e.mustPut("p", data)
	f := e.file("p")
	if f.NumParts() != 4 || f.PartSize != 1<<20 || len(f.Stores) != 4*12 {
		t.Fatalf("%d parts of %d bytes on %d stores", f.NumParts(), f.PartSize, len(f.Stores))
	}
	for i := range f.NumParts() {
		if !sameStores(f.Part(i).Stores, f.Part(0).Stores) {
			t.Errorf("part %d left part 0's servers", i)
		}
	}
	if !e.allRows(f) {
		t.Error("missing index rows")
	}
	sum := blake3.Sum256(data)
	if resp, got := e.do("GET", "p", nil); resp.StatusCode != http.StatusOK || !bytes.Equal(got, data) || resp.Header.Get("ETag") != etag(sum[:]) {
		t.Fatalf("GET: %s, %d bytes, ETag %s", resp.Status, len(got), resp.Header.Get("ETag"))
	}
	n := len(data)
	for _, c := range []struct {
		hdr        string
		start, end int
	}{
		{"bytes=1048476-1048675", 1<<20 - 100, 1<<20 + 100}, // crosses a part
		{"bytes=2097152-2097251", 2 << 20, 2<<20 + 100},     // starts one
		{"bytes=-10", n - 10, n},
		{"bytes=1000000-", 1000000, n},
	} {
		resp, got := e.do("GET", "p", nil, "Range", c.hdr)
		if resp.StatusCode != http.StatusPartialContent || !bytes.Equal(got, data[c.start:c.end]) {
			t.Errorf("%s: %s, %d bytes, want %d", c.hdr, resp.Status, len(got), c.end-c.start)
		}
	}
	if resp, b := e.do("HEAD", "p", nil); resp.StatusCode != http.StatusOK || resp.ContentLength != int64(n) || len(b) != 0 {
		t.Errorf("HEAD: %s, length %d", resp.Status, resp.ContentLength)
	}
	http.DefaultClient.CloseIdleConnections()
	e.sv.Client.HTTP.CloseIdleConnections()
	waitFor(t, "every part's shard streams closed", func() bool { return runtime.NumGoroutine() <= base+10 })

	e.mustPut("p", random(1000))
	waitFor(t, "overwritten parts unindexed", func() bool { return !e.anyRow(f) })
}

// cutOff is a client body that dies after n bytes.
type cutOff struct {
	r io.Reader
	n int
}

func (c *cutOff) Read(p []byte) (int, error) {
	if c.n <= 0 {
		return 0, errors.New("client gone")
	}
	n, err := c.r.Read(p[:min(len(p), c.n)])
	c.n -= n
	return n, err
}

func TestResume(t *testing.T) {
	smallParts(t)
	e := newEnv(t, 12)
	data := random(4<<20 + 777) // 5 parts
	size := len(data)
	id, _ := db.NewID()
	u := "p?upload=" + id.String()

	req, err := http.NewRequest("PUT", e.url+"/"+u, &cutOff{bytes.NewReader(data), 5 << 19}) // in part 2
	if err != nil {
		t.Fatal(err)
	}
	req.ContentLength = int64(size)
	if resp, err := http.DefaultClient.Do(req); err == nil {
		resp.Body.Close()
		t.Fatalf("cut-off PUT: %s", resp.Status)
	}
	offset := func(u string) (int, string) {
		resp, _ := e.do("HEAD", u, nil)
		return resp.StatusCode, resp.Header.Get("Upload-Offset")
	}
	waitFor(t, "parts 0 and 1 recorded", func() bool { _, off := offset(u); return off == strconv.Itoa(2<<20) })
	if f, _ := e.db.File(context.Background(), "p"); f != nil {
		t.Fatal("files row for a cut-off upload")
	}

	rest := func(u string, from int, hdr ...string) *http.Response {
		r := fmt.Sprintf("bytes %d-%d/%d", from, size-1, size)
		resp, b := e.do("PUT", u, data[from:], append([]string{"Content-Range", r}, hdr...)...)
		if resp.StatusCode >= 400 {
			t.Logf("resume at %d: %s %s", from, resp.Status, b)
		}
		return resp
	}
	if resp := rest(u, 3<<20); resp.StatusCode != http.StatusConflict || resp.Header.Get("Upload-Offset") != strconv.Itoa(2<<20) {
		t.Errorf("past the offset: %s, offset %q", resp.Status, resp.Header.Get("Upload-Offset"))
	}
	if resp := rest(u, 1<<20+5); resp.StatusCode != http.StatusConflict {
		t.Errorf("off a part boundary: %s", resp.Status)
	}
	other := fmt.Sprintf("bytes %d-%d/%d", 2<<20, size, size+1) // a different file size
	if resp, _ := e.do("PUT", u, data[2<<20-1:], "Content-Range", other); resp.StatusCode != http.StatusConflict || resp.Header.Get("Upload-Offset") != "0" {
		t.Errorf("other size: %s, offset %q", resp.Status, resp.Header.Get("Upload-Offset"))
	}
	if resp, _ := e.do("PUT", "p", data[2<<20:], "Content-Range", fmt.Sprintf("bytes %d-%d/%d", 2<<20, size-1, size)); resp.StatusCode != http.StatusBadRequest {
		t.Errorf("Content-Range without ?upload: %s", resp.Status)
	}
	unknown, _ := db.NewID() // like expired rows
	if code, _ := offset("p?upload=" + unknown.String()); code != http.StatusNotFound {
		t.Errorf("HEAD of an unknown upload: %d", code)
	}
	if resp := rest("p?upload="+unknown.String(), 1<<20); resp.StatusCode != http.StatusConflict || resp.Header.Get("Upload-Offset") != "0" {
		t.Errorf("unknown upload: %s, offset %q", resp.Status, resp.Header.Get("Upload-Offset"))
	}

	// resume before the offset, the hash covers the whole file
	sum := blake3.Sum256(data)
	if resp := rest(u, 1<<20, "X-Blake3", hex.EncodeToString(sum[:])); resp.StatusCode != http.StatusCreated || resp.Header.Get("ETag") != etag(sum[:]) {
		t.Fatalf("resume: %s, ETag %s", resp.Status, resp.Header.Get("ETag"))
	}
	if resp, got := e.do("GET", "p", nil); resp.StatusCode != http.StatusOK || !bytes.Equal(got, data) {
		t.Fatalf("GET: %s, %d bytes", resp.Status, len(got))
	}
	f1 := e.file("p")
	if f1.NumParts() != 5 || !e.allRows(f1) {
		t.Fatalf("%d parts, all rows %v", f1.NumParts(), e.allRows(f1))
	}

	// the 201 got lost: resending the last part commits again, sharing the rest
	if _, off := offset(u); off != strconv.Itoa(4<<20) {
		t.Fatalf("offset after the commit %s, want the last part's start", off)
	}
	if resp := rest(u, 4<<20); resp.StatusCode != http.StatusCreated {
		t.Fatalf("resend last part: %s", resp.Status)
	}
	f2 := e.file("p")
	if f2.ID == f1.ID || !slices.Equal(f2.Parts[:4], f1.Parts[:4]) || f2.Parts[4] == f1.Parts[4] {
		t.Fatalf("parts %v, were %v", f2.Parts, f1.Parts)
	}
	waitFor(t, "old last part unindexed", func() bool { return !e.anyRow(f1.Part(4)) })
	if !e.allRows(f2) {
		t.Error("shared parts lost index rows")
	}
	if resp, got := e.do("GET", "p", nil); resp.StatusCode != http.StatusOK || !bytes.Equal(got, data) {
		t.Errorf("GET after the resend: %s", resp.Status)
	}
}

func TestCommitChecks(t *testing.T) {
	e := newEnv(t, 12)
	ctx := context.Background()
	e.mustPut("c", random(100<<10))
	f := e.file("c")
	conflict := func(err error) bool {
		var se *statusError
		return errors.As(err, &se) && se.code == http.StatusConflict
	}
	if err := e.sv.commit(ctx, f, time.Now().Add(-24*time.Hour), false); !conflict(err) {
		t.Errorf("part over 23h old: %v", err)
	}
	gone, _ := db.NewID()
	f.Stores[3] = gone
	if err := e.sv.commit(ctx, f, time.Now(), true); !conflict(err) {
		t.Errorf("resumed with a forgotten store: %v", err)
	}
	if err := e.sv.commit(ctx, f, time.Now(), false); err != nil { // only a resumed upload can have one
		t.Errorf("fresh upload: %v", err)
	}
}

func TestPickPrefers(t *testing.T) {
	e := newEnv(t, 14)
	first, err := e.sv.pick(12, 1, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	var prefer []db.UUID
	for _, st := range first {
		prefer = append(prefer, st.ID)
	}
	ids := func(stores []db.Store) []db.UUID {
		var out []db.UUID
		for _, st := range stores {
			out = append(out, st.ID)
		}
		return out
	}
	for range 10 {
		if again, _ := e.sv.pick(12, 1, nil, prefer); !sameStores(ids(again), prefer) {
			t.Fatal("left the preferred stores")
		}
	}
	e.kill(e.node(prefer[0]))
	swapped, err := e.sv.pick(12, 1, nil, prefer)
	if err != nil {
		t.Fatal(err)
	}
	got := ids(swapped)
	if slices.Contains(got, prefer[0]) || slices.ContainsFunc(prefer[1:], func(id db.UUID) bool { return !slices.Contains(got, id) }) {
		t.Errorf("one dead store should swap just that one: %v", got)
	}
}

func TestPartsRepairAndScrub(t *testing.T) {
	smallParts(t)
	e := newEnv(t, 13)
	ctx := context.Background()
	data := random(2<<20 + 5000) // 3 parts
	e.mustPut("r", data)
	f := e.file("r")

	// a bad block in the middle part
	p1 := f.Part(1)
	hdr := len(shard.Magic)
	path := e.shardPath(p1, 0)
	flip(t, path, hdr+10)
	if err := e.node(p1.Stores[0]).st.Scrub(ctx, false, time.Time{}); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if block, ok := shard.Check(b[hdr : hdr+erasure.BlockSize+4]); !ok || !bytes.Equal(block, data[1<<20:1<<20+erasure.BlockSize]) {
		t.Error("scrub didn't rebuild the middle part's block")
	}
	if !e.allRows(e.file("r")) {
		t.Error("scrub dropped a part's rows as orphans")
	}

	// a lost server holds a shard of every part
	lost := e.node(p1.Stores[3])
	e.kill(lost)
	e.db.Heartbeat(ctx, db.Store{ID: lost.id, Addr: lost.addr, LastSeen: time.Now().Add(-25 * time.Hour)})
	if err := e.sv.Repair(ctx, 24*time.Hour); err != nil {
		t.Fatal(err)
	}
	f2 := e.file("r")
	if slices.Contains(f2.Stores, lost.id) || !e.allRows(f2) {
		t.Fatalf("not every part repaired: %v", f2.Stores)
	}
	for i := range f2.NumParts() {
		if e.hasRow(lost.id, f2.Part(i).ID) {
			t.Errorf("part %d's row on the lost store kept", i)
		}
	}
	// the rebuilt shards must be real: lose 4 others so every part needs them
	killed := 0
	for _, id := range f2.Part(0).Stores {
		if !slices.Contains(f.Part(0).Stores, id) || killed == 4 {
			continue // the new one
		}
		e.kill(e.node(id))
		killed++
	}
	e.reload()
	if resp, got := e.do("GET", "r", nil); resp.StatusCode != http.StatusOK || !bytes.Equal(got, data) {
		t.Errorf("GET through the rebuilt shards: %s", resp.Status)
	}
}

// TestSlowStoreAcrossParts: a store that held up one part must not be waited on again in the next.
func TestSlowStoreAcrossParts(t *testing.T) {
	if testing.Short() {
		t.Skip("timing")
	}
	smallParts(t)
	e := newEnv(t, 14, func(c *Config) { c.SlowRead = 200 * time.Millisecond })
	data := random(4 << 20) // four 1 MiB parts
	e.mustPut("p", data)
	f := e.file("p")
	e.node(f.Stores[0]).slow.Store(true)
	start := time.Now()
	resp, got := e.do("GET", "p", nil)
	took := time.Since(start)
	if resp.StatusCode != http.StatusOK || !bytes.Equal(got, data) {
		t.Fatalf("%s, %d bytes", resp.Status, len(got))
	}
	if took > 600*time.Millisecond { // one -slowRead, not one per part
		t.Errorf("%d parts took %v, the slow store was waited on in more than one", f.NumParts(), took)
	}
}

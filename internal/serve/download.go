package serve

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"filescale/internal/db"
	"filescale/internal/erasure"
	"filescale/internal/shard"
)

// ahead is how early the next part's shards open, ~4 round trips at 100 ms and 1 Gbps.
const ahead = 64 << 20

func (s *Serve) download(w http.ResponseWriter, r *http.Request, name string) {
	cd := "attachment"
	if v := r.URL.Query().Get("response-content-disposition"); v != "" {
		// for the filename only, inline would render uploaded html on our domain
		t, _, _ := strings.Cut(v, ";")
		switch t = strings.TrimSpace(t); {
		case strings.EqualFold(t, "attachment"):
		case strings.EqualFold(t, "inline") && s.AllowInline:
		default:
			http.Error(w, "response-content-disposition must be attachment", http.StatusBadRequest)
			return
		}
		cd = v
	}
	f, err := s.DB.File(r.Context(), name)
	if err != nil {
		fail(w, err)
		return
	}
	if f == nil {
		http.NotFound(w, r)
		return
	}
	// always set: without it ServeContent reads 512 bytes and seeks back, reopening every shard
	w.Header().Set("Content-Type", f.ContentType)
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Content-Disposition", cd)
	tag := etag(f.BLAKE3)
	w.Header().Set("ETag", tag)
	if strings.Contains(r.Header.Get("Range"), ",") {
		r.Header.Del("Range") // we don't support multiple ranges
	}
	start, end := byteRange(r.Header.Get("Range"), f.Size)
	if ir := r.Header.Get("If-Range"); ir != "" && ir != tag {
		start, end = 0, f.Size
	}
	d := &download{s: s, ctx: r.Context(), f: f, addrs: s.addrs(f), rc: http.NewResponseController(w), end: end, slow: map[db.UUID]bool{}}
	// ServeContent sends 200 before the first Read, so check the parts the range needs here or the client gets a cut-off 200
	n := f.DataShards + f.ParityShards
	for i := d.partAt(start); r.Method == http.MethodGet && start < end && i <= d.partAt(end-1); i++ {
		live := 0
		for _, a := range d.addrs[i*n : (i+1)*n] {
			if a != "" {
				live++
			}
		}
		if live < f.DataShards {
			http.Error(w, fmt.Sprintf("%d of %d shards reachable, need %d", live, n, f.DataShards), http.StatusServiceUnavailable)
			return
		}
	}
	defer d.close()
	http.ServeContent(w, r, "", time.Time{}, d)
}

// download is the io.ReadSeeker ServeContent reads, one part at a time. Seek only
// records the position, the shard streams open on the first Read.
type download struct {
	s       *Serve
	ctx     context.Context
	f       *db.File
	addrs   []string // part i's are addrs[i*(d+p):]
	rc      *http.ResponseController
	pos     int64
	end     int64 // end of the requested range
	part    int
	l       erasure.Layout   // the part's
	r       *shard.Reader    // the part's
	next    *shard.Reader    // part+1, opened ahead
	slow        map[db.UUID]bool // stores dropped for being slow, no part waits on them again
	rebuilt     int
	deadlineAt  time.Time
}

func (d *download) Seek(off int64, whence int) (int64, error) {
	switch whence {
	case io.SeekCurrent:
		off += d.pos
	case io.SeekEnd:
		off += d.f.Size
	}
	if off < 0 {
		return 0, errors.New("negative position")
	}
	d.pos = off
	return off, nil
}

// Read sends one data slot at a time as soon as it's full, the first 64 KiB
// only waits for shard 0.
func (d *download) Read(p []byte) (int, error) {
	if d.pos >= d.f.Size {
		return 0, io.EOF
	}
	if i := d.partAt(d.pos); d.r == nil || i != d.part {
		if err := d.start(i); err != nil {
			return 0, err
		}
	}
	if next := d.base(d.part + 1); d.next == nil && d.part+1 < d.f.NumParts() && d.end > next && next-d.pos <= ahead {
		r, err := d.open(d.part+1, 0)
		if err != nil {
			return 0, err
		}
		d.next = r
	}
	pos := d.pos - d.base(d.part)
	ss := d.l.StripeSize()
	s := pos / ss
	bs := int64(d.l.BlockLen(s))
	off := pos - s*ss
	k := int(off / bs)
	if now := time.Now(); now.Sub(d.deadlineAt) >= time.Second {
		d.rc.SetWriteDeadline(now.Add(clientStall))
		d.deadlineAt = now
	}
	b, err := d.r.Block(s, k)
	if err != nil {
		log.Printf("download %s: part %d stripe %d: %v", d.f.Name, d.part, s, err)
		if d.ctx.Err() == nil { // not a client that disconnected
			d.s.cut.Add(1)
		}
		return 0, err
	}
	b = b[off-int64(k)*bs:]
	n := copy(p, b[:min(int64(len(b)), d.l.Size-pos)]) // trims the last stripe's padding
	if n == 0 && len(p) > 0 {
		return 0, io.ErrUnexpectedEOF // a short block would spin ServeContent forever
	}
	d.pos += int64(n)
	return n, nil
}

func (d *download) partAt(pos int64) int {
	if d.f.PartSize == 0 {
		return 0
	}
	return int(pos / d.f.PartSize)
}

func (d *download) base(i int) int64 { return int64(i) * d.f.PartSize }

// start moves on to part i, with the reader opened ahead for it if there is one.
func (d *download) start(i int) error {
	if d.r != nil {
		d.drop(d.r)
		d.r = nil
	}
	if d.next != nil && i == d.part+1 {
		d.r, d.next = d.next, nil
	}
	d.part = i
	d.l = erasure.Layout{Size: d.f.Part(i).Size, Data: d.f.DataShards}
	if d.r != nil {
		return nil
	}
	r, err := d.open(i, d.pos-d.base(i))
	d.r = r
	return err
}

// open starts the data shards part i needs from pos in it to the end of the range, a small range touches one store.
func (d *download) open(i int, pos int64) (*shard.Reader, error) {
	part := d.f.Part(i)
	n := part.DataShards + part.ParityShards
	l := erasure.Layout{Size: part.Size, Data: part.DataShards}
	end := max(min(d.end, d.base(i)+part.Size)-d.base(i), pos+1) // at least the byte being read
	ss := l.StripeSize()
	s := pos / ss
	k := int((pos - s*ss) / int64(l.BlockLen(s)))
	last := (end - 1) / ss
	r, err := shard.NewReader(d.ctx, d.s.Client, part, d.addrs[i*n:(i+1)*n], s, last)
	if err != nil {
		return nil, err
	}
	r.Slow, r.OnSlow = d.s.SlowRead, d.slowStore
	for id := range d.slow {
		r.Skip(id)
	}
	to := l.Data
	if last == s {
		to = int((end-1-s*ss)/int64(l.BlockLen(s))) + 1
	}
	var ks []int
	for j := k; j < to; j++ {
		ks = append(ks, j)
	}
	if last > s {
		ks = seq(l.Data)
	}
	r.Start(ks)
	return r, nil
}

// slowStore remembers a store that held up a part: a part already open drops it too,
// and later ones don't open it at all instead of waiting -slowRead again.
func (d *download) slowStore(id db.UUID) {
	log.Printf("download %s: part %d held up by store %s, reading it from parity", d.f.Name, d.part, id)
	d.slow[id] = true
	d.s.inc(&d.s.slow, id)
	if d.next != nil {
		d.next.Skip(id)
	}
}

func (d *download) drop(r *shard.Reader) {
	r.Close()
	d.rebuilt += r.Rebuilt
	d.s.rebuilt.Add(int64(r.Rebuilt))
}

func (d *download) close() {
	for _, r := range []*shard.Reader{d.r, d.next} {
		if r != nil {
			d.drop(r)
		}
	}
	if d.rebuilt > 0 {
		log.Printf("download %s: %d stripes rebuilt from parity", d.f.Name, d.rebuilt)
	}
}

// byteRange is the span of a single-range request, the whole file when there's none.
func byteRange(h string, size int64) (start, end int64) {
	spec, ok := strings.CutPrefix(h, "bytes=")
	if !ok {
		return 0, size
	}
	a, b, ok := strings.Cut(spec, "-")
	if !ok {
		return 0, size
	}
	a, b = strings.TrimSpace(a), strings.TrimSpace(b)
	if a == "" { // suffix: the last b bytes
		if n, err := strconv.ParseInt(b, 10, 64); err == nil {
			return max(0, size-n), size
		}
		return 0, size
	}
	s, err := strconv.ParseInt(a, 10, 64)
	if err != nil {
		return 0, size
	}
	if e, err := strconv.ParseInt(b, 10, 64); err == nil {
		return s, min(e, size-1) + 1 // not e+1, bytes=0-9223372036854775807 is a valid range
	}
	return s, size
}

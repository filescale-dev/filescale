package serve

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"log"
	"maps"
	"mime"
	"net/http"
	"net/http/httptrace"
	"path"
	"slices"
	"sync"
	"time"

	"github.com/klauspost/reedsolomon"

	"filescale/internal/b3"
	"filescale/internal/db"
	"filescale/internal/erasure"
	"filescale/internal/shard"
)

const (
	acceptWait = 2 * time.Second  // store's 100 Continue on a LAN, + 4 round trips to a far store
	indexWait  = 10 * time.Minute // index rows and files row, the parts must stay under 24h old
	maxParts   = 64
)

// minPart is the smallest part a file is cut into, a var so tests can shrink it.
var minPart int64 = 64 << 20

// commitWait is the store's 201 after the last block, a var so tests can shrink it.
var commitWait = 2 * time.Minute

// partSize cuts a file into at most 64 parts of at least minPart, in whole MiB.
func partSize(size int64) int64 {
	p := (size + maxParts - 1) / maxParts
	return max(minPart, (p+1<<20-1)&^(1<<20-1))
}

// part is a finished part of the file, sent by this request or recorded by an earlier one.
type part struct {
	id     db.UUID
	stores []db.UUID
	at     time.Time
}

func (s *Serve) upload(w http.ResponseWriter, r *http.Request, name string, upload db.UUID) {
	size := r.ContentLength
	if size < 0 {
		http.Error(w, "Content-Length required", http.StatusLengthRequired)
		return
	}
	var want []byte
	if h := r.Header.Get("X-Blake3"); h != "" {
		b, err := hex.DecodeString(h)
		if err != nil || len(b) != 32 {
			http.Error(w, "bad X-Blake3", http.StatusBadRequest)
			return
		}
		want = b
	}
	// not the client's: a client gone mid-part must not cancel the part before it, still waiting on its 201s
	ctx, cancel := context.WithCancelCause(context.WithoutCancel(r.Context()))
	defer cancel(nil)

	// the whole file, or the rest of one cut off earlier
	from, total := int64(0), size
	var done []db.UploadPart
	if cr := r.Header.Get("Content-Range"); cr != "" {
		var err error
		if from, total, err = contentRange(cr, size); err != nil || upload == (db.UUID{}) {
			http.Error(w, "Content-Range needs ?upload and bytes <from>-<size-1>/<size>", http.StatusBadRequest)
			return
		}
		if done, err = s.resumed(ctx, w, name, upload, from, total); err != nil {
			fail(w, err)
			return
		}
	}
	h := b3.New()
	d, p, ct := s.DataShards, s.ParityShards, ""
	if total < s.SmallFileSize {
		d = 1 // parity+1 full copies
	}
	var parts []part
	if len(done) > 0 {
		d, p, ct = done[0].DataShards, done[0].ParityShards, done[0].ContentType
		if err := h.UnmarshalBinary(done[len(done)-1].HashState); err != nil {
			fail(w, err)
			return
		}
		for _, row := range done {
			parts = append(parts, part{row.ID, row.Stores, row.Written})
		}
	}
	ps := partSize(total)
	n := int((total + ps - 1) / ps) // 0 for an empty file: its files row is all of it
	layout := func(i int) erasure.Layout { return erasure.Layout{Size: min(ps, total-int64(i)*ps), Data: d} }
	fileID, err := db.NewID()
	if err != nil {
		fail(w, err)
		return
	}
	ids := make([]db.UUID, n)
	for i := range ids {
		switch {
		case i < len(parts):
			ids[i] = parts[i].id
		case n == 1:
			ids[i] = fileID // one part keeps the file's own ID
		default:
			if ids[i], err = db.NewID(); err != nil {
				fail(w, err)
				return
			}
		}
	}
	j, err := newJob(ctx, cancel, w, r.Body, h, layout(len(parts)), p)
	if err != nil {
		fail(w, err)
		return
	}
	old := make(chan *db.File, 1)
	go func() {
		f, _ := s.DB.File(ctx, name)
		old <- f
	}()

	type opened struct {
		puts []*put
		err  error
	}
	open := func(i int, prefer []db.UUID) chan opened {
		ch := make(chan opened, 1)
		go func() {
			puts, err := s.open(ctx, ids[i], layout(i).ShardSize(), seq(d+p), nil, false, prefer)
			ch <- opened{puts, err}
		}()
		return ch
	}
	var prefer []db.UUID // the same servers for every part, unless one fails
	if len(parts) > 0 {
		prefer = parts[len(parts)-1].stores
	}
	var next chan opened
	if n > 0 {
		next = open(len(parts), prefer)
	}
	finished := make(chan error, 1) // the previous part's stores answering 201
	finished <- nil
	for i := len(parts); i < n; i++ {
		o := <-next
		next = nil
		stores := make([]db.UUID, len(o.puts))
		for k, pt := range o.puts {
			stores[k] = pt.store.ID
		}
		err := o.err
		if err == nil {
			var near func()
			if i+1 < n {
				near = func() { next = open(i+1, stores) }
			}
			err = j.stream(layout(i), o.puts, s.Client.Wait(acceptWait, o.puts[0].store.Addr), near)
			if err != nil {
				cancelPuts(o.puts)
			}
		}
		if ferr := <-finished; err == nil { // the part before still lands and is recorded if this one fails
			err = ferr
		}
		if err != nil {
			failUpload(w, fileID, err)
			return
		}
		if next == nil && i+1 < n { // a part shorter than the lead
			next = open(i+1, stores)
		}
		if ct == "" {
			ct = contentType(r, name, j.head)
		}
		var row *db.UploadPart
		if upload != (db.UUID{}) && i+1 < n { // the last part commits, no need to resume it
			state, _ := h.MarshalBinary() // an unusable state ends what recorded resumes from
			row = &db.UploadPart{Name: name, UploadID: upload, Index: i, ID: ids[i], Size: layout(i).Size, Total: total,
				HashState: state, ContentType: ct, DataShards: d, ParityShards: p, Stores: stores}
		}
		parts = append(parts, part{ids[i], stores, time.Now()})
		go func() {
			err := finish(o.puts)
			if err == nil && row != nil {
				if err := s.DB.PutPart(ctx, *row); err != nil { // only costs resuming
					log.Printf("upload %s: recording part %d: %v", fileID, i, err)
				}
			}
			if err != nil {
				cancel(err) // stops the part streaming now
			}
			finished <- err
		}()
	}
	if err := <-finished; err != nil {
		failUpload(w, fileID, err)
		return
	}
	sum := h.Sum(nil)
	if want != nil && !bytes.Equal(want, sum) {
		http.Error(w, "blake3 mismatch", http.StatusBadRequest)
		return
	}

	if ct == "" { // an empty file, nothing streamed
		ct = contentType(r, name, nil)
	}
	f := &db.File{Name: name, ID: fileID, Size: total, BLAKE3: sum, ContentType: ct, DataShards: d, ParityShards: p}
	oldest := time.Now()
	for _, pt := range parts {
		f.Stores = append(f.Stores, pt.stores...)
		if pt.at.Before(oldest) {
			oldest = pt.at
		}
	}
	if n > 1 {
		f.Parts, f.PartSize = ids, ps
	}
	cctx, stop := context.WithTimeout(ctx, indexWait)
	defer stop()
	if err := s.commit(cctx, f, oldest, len(done) > 0); err != nil {
		failUpload(w, fileID, err)
		return
	}
	o := <-old // before our files row: read after it, it could return a newer upload
	if err := s.DB.PutFile(cctx, f); err != nil {
		failUpload(w, fileID, err)
		return
	}
	w.Header().Set("ETag", etag(sum))
	w.Header().Set("Content-Length", "0")
	w.WriteHeader(http.StatusCreated)
	if o != nil {
		go s.unindex(context.WithoutCancel(ctx), o)
	}
}

func failUpload(w http.ResponseWriter, id db.UUID, err error) {
	if se := (*statusError)(nil); !errors.As(err, &se) || se.code >= 500 { // a store's fault, not the client's
		log.Printf("upload %s: %v", id, err)
	}
	fail(w, err)
}

func contentType(r *http.Request, name string, head []byte) string {
	if ct := r.Header.Get("Content-Type"); ct != "" {
		return ct
	}
	if ct := mime.TypeByExtension(path.Ext(name)); ct != "" {
		return ct
	}
	return http.DetectContentType(head)
}

// commit writes every part's index rows, 16 parts at a time, before the files row: without
// them store cleanup would delete a ready file. A part over 23h old may be gone already,
// and a resumed upload's store that was forgotten meanwhile would never be repaired.
func (s *Serve) commit(ctx context.Context, f *db.File, oldest time.Time, resumed bool) error {
	if time.Since(oldest) > db.UploadTTL {
		return &statusError{http.StatusConflict, "upload took over 23h, start it again"}
	}
	n := f.DataShards + f.ParityShards
	errs := make(chan error, len(f.Stores))
	sem := make(chan struct{}, 16*n)
	for i, st := range f.Stores {
		go func() {
			sem <- struct{}{}
			defer func() { <-sem }()
			errs <- s.DB.PutShard(ctx, db.Shard{StoreID: st, FileID: f.Part(i / n).ID, Index: i % n, Name: f.Name})
		}()
	}
	var first error
	for range f.Stores {
		if err := <-errs; err != nil && first == nil {
			first = err
		}
	}
	if first != nil || !resumed {
		return first
	}
	stores, err := s.DB.Stores(ctx)
	if err != nil {
		return err
	}
	known := map[db.UUID]bool{}
	for _, st := range stores {
		known[st.ID] = true
	}
	for i, id := range f.Stores {
		if !known[id] {
			return &statusError{http.StatusConflict, fmt.Sprintf("store %s of part %d is gone, start the upload again with a new id", id, i/n)}
		}
	}
	return nil
}

// job is one PUT's stripe loop, its buffers sized to the first part it sends and reused for the rest.
type job struct {
	ctx        context.Context
	cancel     context.CancelCauseFunc
	rc         *http.ResponseController
	body       io.Reader
	h          hash.Hash
	head       []byte              // the first 512 bytes, to guess a content type
	enc        reedsolomon.Encoder // nil with 1 data shard: the parity shards are plain copies
	buf        []byte
	parity     [][]byte
	blocks     [][]byte
	deadlineAt time.Time
}

func newJob(ctx context.Context, cancel context.CancelCauseFunc, w http.ResponseWriter, body io.Reader, h hash.Hash, l erasure.Layout, parity int) (*job, error) {
	first := l.BlockLen(0) // 64 KiB unless the part is one short stripe
	j := &job{ctx: ctx, cancel: cancel, rc: http.NewResponseController(w), body: body, h: h,
		buf: make([]byte, l.Data*first), parity: make([][]byte, parity), blocks: make([][]byte, l.Data+parity)}
	if l.Data > 1 {
		enc, err := erasure.New(l.Data, parity)
		if err != nil {
			return nil, err
		}
		j.enc = enc
		for i := range j.parity {
			j.parity[i] = make([]byte, first)
		}
	}
	return j, nil
}

// stream reads one part a stripe at a time, encodes it and sends every shard its block.
// near runs once the part looks about lead from its end, to open the next part's stores in time.
func (j *job) stream(l erasure.Layout, puts []*put, lead time.Duration, near func()) error {
	start := time.Now()
	for st := range l.Stripes() {
		bs := l.BlockLen(st)
		sent := st * l.StripeSize()
		n := min(l.StripeSize(), l.Size-sent)
		if now := time.Now(); now.Sub(j.deadlineAt) >= time.Second {
			j.rc.SetReadDeadline(now.Add(clientStall))
			j.deadlineAt = now
		}
		if _, err := io.ReadFull(j.body, j.buf[:n]); err != nil {
			return &statusError{http.StatusBadRequest, "reading upload: " + err.Error()}
		}
		j.h.Write(j.buf[:n])
		if j.head == nil {
			j.head = slices.Clone(j.buf[:min(n, 512)])
		}
		if j.enc == nil {
			for i := range j.blocks {
				j.blocks[i] = j.buf[:n]
			}
		} else {
			clear(j.buf[n : l.Data*bs]) // zero padding in the last stripe
			for i := range l.Data {
				j.blocks[i] = j.buf[i*bs : (i+1)*bs]
			}
			for i := range j.parity {
				j.blocks[l.Data+i] = j.parity[i][:bs]
			}
			if err := j.enc.Encode(j.blocks); err != nil {
				return err
			}
		}
		if err := writeAll(j.ctx, puts, j.blocks); err != nil {
			return err
		}
		// time left at the rate so far, not bytes: a slow client would leave the next PUTs idle past the store's stall
		if sent += n; near != nil && sent < l.Size && float64(time.Since(start))*float64(l.Size-sent)/float64(sent) <= float64(lead) {
			near()
			near = nil
		}
	}
	return nil
}

type put struct {
	store  db.Store
	cancel context.CancelFunc
	done   chan error

	send     chan []byte
	ack      chan struct{}
	closed   chan struct{}
	failOnce sync.Once
	endOnce  sync.Once
	err      error

	cur []byte
	crc [4]byte
	rem []byte
}

func (p *put) fail(err error) {
	p.failOnce.Do(func() {
		p.err = err
		close(p.closed)
	})
}

func (p *put) closeBody() {
	p.endOnce.Do(func() {
		close(p.send)
	})
}

func (p *put) Close() error { return nil }

func (p *put) Read(dst []byte) (int, error) {
	if len(p.cur) == 0 && len(p.rem) == 0 {
		select {
		case <-p.closed:
			return 0, p.err
		default:
		}
		select {
		case <-p.closed:
			return 0, p.err
		case b, ok := <-p.send:
			if !ok {
				return 0, io.EOF
			}
			binary.LittleEndian.PutUint32(p.crc[:], shard.CRC(b))
			p.cur = b
			p.rem = p.crc[:]
		}
	}
	n := copy(dst, p.cur)
	p.cur = p.cur[n:]
	if len(p.cur) == 0 && len(p.rem) > 0 && n < len(dst) {
		m := copy(dst[n:], p.rem)
		p.rem = p.rem[m:]
		n += m
	}
	if len(p.cur) == 0 && len(p.rem) == 0 {
		p.ack <- struct{}{}
	}
	return n, nil
}

func cancelPuts(puts []*put) {
	for _, p := range puts {
		if p != nil {
			p.cancel()
		}
	}
}

// writeAll sends every shard its block at once, a stalled store cancels the part's PUTs.
func writeAll(ctx context.Context, puts []*put, blocks [][]byte) error {
	for i, p := range puts {
		select {
		case <-p.closed:
			return midStreamErr(ctx, puts, p)
		default:
		}
		select {
		case p.send <- blocks[i]:
		case <-p.closed:
			return midStreamErr(ctx, puts, p)
		case <-ctx.Done():
			cancelPuts(puts)
			return &statusError{http.StatusServiceUnavailable, "store failed mid-stream: " + context.Cause(ctx).Error()}
		}
	}
	t := time.NewTimer(shard.Stall)
	defer t.Stop()
	for _, p := range puts {
		select {
		case <-p.ack:
			continue
		default:
		}
		select {
		case <-p.ack:
		case <-p.closed:
			select {
			case <-p.ack:
				continue
			default:
			}
			return midStreamErr(ctx, puts, p)
		case <-ctx.Done():
			cancelPuts(puts)
			return &statusError{http.StatusServiceUnavailable, "store failed mid-stream: " + context.Cause(ctx).Error()}
		case <-t.C:
			cancelPuts(puts)
			return &statusError{http.StatusServiceUnavailable,
				fmt.Sprintf("store failed mid-stream: store %s accepted no data for %v", p.store.Addr, shard.Stall)}
		}
	}
	return nil
}

func midStreamErr(ctx context.Context, puts []*put, failed *put) error {
	if cause := context.Cause(ctx); cause != nil {
		cancelPuts(puts)
		return &statusError{http.StatusServiceUnavailable, "store failed mid-stream: " + cause.Error()}
	}
	cancelPuts(puts)
	return &statusError{http.StatusServiceUnavailable, "store failed mid-stream: " + failed.err.Error()}
}

// finish ends every shard's body and waits for the stores' 201.
func finish(puts []*put) error {
	for _, p := range puts {
		p.closeBody()
	}
	t := time.AfterFunc(commitWait, func() { cancelPuts(puts) })
	defer t.Stop()
	var first error
	for _, p := range puts {
		if err := <-p.done; err != nil && first == nil {
			first = err
		}
	}
	if first != nil {
		return &statusError{http.StatusServiceUnavailable, "store failed to commit: " + first.Error()}
	}
	return nil
}

// open starts a PUT for each shard index and waits for every store's 100 Continue.
// A store failing before any data is sent is swapped for another server.
func (s *Serve) open(ctx context.Context, id db.UUID, size int64, shards []int, skip map[string]bool, repair bool, prefer []db.UUID) ([]*put, error) {
	skip = maps.Clone(skip)
	if skip == nil {
		skip = map[string]bool{}
	}
	stores, err := s.pick(len(shards), size, skip, prefer)
	if err != nil {
		return nil, err
	}
	type result struct {
		i     int
		store db.UUID
		p     *put
		err   error
	}
	results := make(chan result)
	start := func(i int, st db.Store) {
		skip[st.Addr] = true
		go func() {
			p, err := s.start(ctx, st, id, shards[i], size, repair)
			results <- result{i, st.ID, p, err}
		}()
	}
	for i, st := range stores {
		start(i, st)
	}
	puts := make([]*put, len(shards))
	var failed error
	for pending := len(shards); pending > 0; {
		r := <-results
		if r.err == nil {
			puts[r.i] = r.p
			pending--
			continue
		}
		log.Printf("upload %s: %v", id, r.err)
		if ctx.Err() == nil { // not the store's fault when the upload itself is cancelled
			s.inc(&s.refused, r.store)
		}
		if failed == nil {
			alt, err := s.pick(1, size, skip, nil)
			if err == nil {
				start(r.i, alt[0])
				continue
			}
			if se := (*statusError)(nil); errors.As(err, &se) { // every server is in skip: tried or holding a shard
				err = &statusError{se.code, fmt.Sprintf("%v, and no spare server to swap in", r.err)}
			}
			failed = err
		}
		pending--
	}
	if failed != nil {
		cancelPuts(puts)
		return nil, failed
	}
	return puts, nil
}

// start sends the PUT headers and returns once the store answers 100 Continue.
func (s *Serve) start(ctx context.Context, st db.Store, id db.UUID, k int, size int64, repair bool) (*put, error) {
	sent, accepted := make(chan struct{}), make(chan struct{})
	var sentOnce, acceptedOnce sync.Once
	trace := &httptrace.ClientTrace{
		Wait100Continue: func() { sentOnce.Do(func() { close(sent) }) }, // headers flushed
		Got100Continue:  func() { acceptedOnce.Do(func() { close(accepted) }) },
	}
	rctx, cancel := context.WithCancel(httptrace.WithClientTrace(ctx, trace))
	p := &put{
		store:  st,
		cancel: cancel,
		done:   make(chan error, 1),
		send:   make(chan []byte, 1),
		ack:    make(chan struct{}, 1),
		closed: make(chan struct{}),
	}
	// Transport won't return from Do until it stops reading the body, so a cancelled PUT must wake Read
	context.AfterFunc(rctx, func() { p.fail(context.Canceled) })
	url := shard.URL(st.Addr, st.ID, id, k)
	if repair {
		url += "?repair=1"
	}
	req, err := http.NewRequestWithContext(rctx, http.MethodPut, url, p)
	if err != nil {
		cancel()
		return nil, err
	}
	req.ContentLength = size
	req.Header.Set("Expect", "100-continue")
	go func() {
		resp, err := s.Client.Do(req)
		if err == nil {
			if resp.StatusCode != http.StatusCreated {
				msg, _ := io.ReadAll(io.LimitReader(resp.Body, 200))
				err = fmt.Errorf("store %s: %s %s", st.Addr, resp.Status, bytes.TrimSpace(msg))
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		} else {
			err = fmt.Errorf("store %s: %w", st.Addr, err)
		}
		if err != nil {
			p.fail(err)
		} else {
			p.fail(fmt.Errorf("store %s: answered before reading the shard", st.Addr))
		}
		p.done <- err
	}()
	// Reset the timer once headers flush; before that, cap against a hung idle conn.
	wait := s.Client.Wait(acceptWait, st.Addr)
	timer := time.NewTimer(2 * wait)
	defer timer.Stop()
	for {
		select {
		case <-sent:
			sent = nil
			timer.Reset(wait)
		case <-accepted:
			return p, nil
		case err := <-p.done:
			cancel()
			if err == nil {
				err = errors.New("store answered before reading the shard")
			}
			return nil, err
		case <-timer.C:
			cancel()
			return nil, fmt.Errorf("store %s: no 100 Continue within %v", st.Addr, wait)
		}
	}
}

func seq(n int) []int {
	s := make([]int, n)
	for i := range s {
		s[i] = i
	}
	return s
}

package shard

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/klauspost/reedsolomon"

	"filescale/internal/db"
	"filescale/internal/erasure"
)

var (
	ErrUnavailable = errors.New("not enough shards left to read")
	errCRC         = errors.New("bad CRC")
	errStatus      = errors.New("store refused")
	errStalled     = fmt.Errorf("store sent nothing for %v", Stall)
)

var slotPool = sync.Pool{New: func() any { return make([]byte, erasure.BlockSize+4) }}

// Reader reads stripes first..last of a file in order. Each shard streams on
// its own connection into two slots that take turns (stripe s lands in slot
// s%2), at most one stripe ahead of the one being read. A shard that is
// offline, slow or returns a bad CRC is rebuilt from the others.
type Reader struct {
	Slow    time.Duration // switch a shard to parity once it held up all the others this long in a row
	Rebuilt int           // stripes that needed a reconstruction
	OnSlow  func(db.UUID) // told of a store dropped for being slow

	ctx         context.Context
	c           *Client
	f           *db.File
	l           erasure.Layout
	enc         reedsolomon.Encoder
	addrs       []string
	first, last int64

	streams []*stream // per shard, nil = not open
	all     []*stream // every stream opened, for Close
	bad     []bool
	slowed  []bool          // dropped for being slow, not failing: taken back when nothing else is left
	patient bool            // out of alternatives, no more dropping for slowness
	held    []time.Duration // how long in a row the shard held up all the others
	spare   [][]byte        // rebuilt blocks, a dropped shard may still be writing its own slot
	cur     int64
	blocks  [][]byte // stripe cur's blocks so far

	mu     sync.Mutex
	window int64 // cur, for the stream goroutines
	wake   chan struct{}
	notify chan struct{} // a block arrived, recv looks at the others again
}

type stream struct {
	out     chan result
	slots   [2][]byte
	cancel  context.CancelCauseFunc
	done    chan struct{} // closed when the stream ends, err says why
	err     error
	retried bool
	opened  time.Time
	took    time.Duration // open to first block, 0 = none yet
}

type result struct {
	s    int64
	data []byte
	err  error
}

func NewReader(ctx context.Context, c *Client, f *db.File, addrs []string, first, last int64) (*Reader, error) {
	enc, err := erasure.New(f.DataShards, f.ParityShards) // 1 data shard: every parity shard is a plain copy
	if err != nil {
		return nil, err
	}
	n := len(f.Stores)
	r := &Reader{Slow: Stall, ctx: ctx, c: c, f: f, l: erasure.Layout{Size: f.Size, Data: f.DataShards}, enc: enc, addrs: addrs,
		first: first, last: last, cur: first, window: first, wake: make(chan struct{}), notify: make(chan struct{}, 1),
		streams: make([]*stream, n), bad: make([]bool, n), slowed: make([]bool, n), held: make([]time.Duration, n), spare: make([][]byte, n), blocks: make([][]byte, n)}
	for k, a := range addrs {
		r.bad[k] = a == ""
	}
	return r, nil
}

// Start opens shards ahead of their first Block so they connect in parallel.
func (r *Reader) Start(ks []int) {
	for _, k := range ks {
		if !r.bad[k] && r.streams[k] == nil {
			r.open(k)
		}
	}
}

// Skip drops the shard on store for being slow, after another part of the file was held up by it.
func (r *Reader) Skip(store db.UUID) {
	for k, s := range r.f.Stores {
		if s == store && !r.bad[k] {
			r.Bad(k)
			r.slowed[k] = true
		}
	}
}

// Bad stops using shard k, e.g. the one being rebuilt.
func (r *Reader) Bad(k int) {
	r.drop(k)
	r.bad[k] = true
}

// Block returns shard k's block of stripe s. Stripes must be asked for in
// order, a block stays valid until a later stripe is asked for.
func (r *Reader) Block(s int64, k int) ([]byte, error) {
	if s < r.cur || s > r.last {
		return nil, fmt.Errorf("stripe %d outside %d-%d", s, r.cur, r.last)
	}
	if s > r.cur {
		r.advance(s)
	}
	if r.blocks[k] != nil {
		return r.blocks[k], nil
	}
	if r.recv(k) != nil {
		return r.blocks[k], nil
	}
	if err := r.rebuild(k); err != nil {
		return nil, err
	}
	return r.blocks[k], nil
}

func (r *Reader) Close() {
	for _, st := range r.all {
		st.cancel(nil)
	}
	for _, st := range r.all {
		<-st.done
		for _, b := range st.slots {
			putSlot(b)
		}
	}
	for _, b := range r.spare {
		putSlot(b)
	}
}

// slot is a block buffer: from the pool, or exactly sized when the read is one short stripe (a small file).
func (r *Reader) slot() []byte {
	if n := r.l.BlockLen(r.first) + 4; n < erasure.BlockSize+4 {
		return make([]byte, n)
	}
	return slotPool.Get().([]byte)
}

func putSlot(b []byte) {
	if len(b) == erasure.BlockSize+4 {
		slotPool.Put(b)
	}
}

// advance moves to stripe s: its slot's old stripe is done, the streams may refill it.
func (r *Reader) advance(s int64) {
	r.cur = s
	clear(r.blocks)
	r.mu.Lock()
	r.window = s
	close(r.wake)
	r.wake = make(chan struct{})
	r.mu.Unlock()
}

// recv waits for shard k's block of the current stripe, nil if it doesn't come.
// Slow is relative, never a fixed time per block: a shard holding up all the
// others (they have their block, it doesn't) for Slow in a row is dropped, see
// revive. Waiting on everyone isn't slow, under load every store is.
// A dead connection is the stream's own Stall timer.
func (r *Reader) recv(k int) []byte {
	if r.bad[k] {
		return nil
	}
	if r.streams[k] == nil {
		r.open(k)
	}
	var since time.Time // when k became the only one missing
	var timer *time.Timer
	var fire <-chan time.Time
	arm := func(d time.Duration) {
		if timer == nil {
			timer = time.NewTimer(d)
		} else {
			timer.Stop()
			timer.Reset(d)
		}
		fire = timer.C
	}
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	for {
		st := r.streams[k]
		// a timer only while k is holding everyone up; a 5 ms tick woke every reader 200 times a second
		fire = nil
		if !r.patient && r.peersReady(k) {
			// a shard still connecting only holds the others up once it took longer than they did to get going
			if st.took == 0 && time.Since(st.opened) < r.firstTook(k) {
				arm(time.Until(st.opened.Add(r.firstTook(k))))
			} else {
				if since.IsZero() {
					since = time.Now()
				}
				left := r.Slow - r.held[k] - time.Since(since)
				if left <= 0 {
					r.Bad(k)
					r.slowed[k] = true
					if r.OnSlow != nil {
						r.OnSlow(r.f.Stores[k])
					}
					return nil
				}
				arm(left)
			}
		}
		var res result
		select {
		case res = <-st.out:
		case <-st.done: // blocks it sent before ending come first
			select {
			case res = <-st.out:
			default:
				res.err = st.err
				if res.err == nil {
					res.err = errors.New("stream ended early")
				}
			}
		case <-fire: // the wait or the connect grace ran out
			continue
		case <-r.notify: // another shard's block landed: k may be the only one missing now
			continue
		case <-r.ctx.Done():
			return nil
		}
		if res.err != nil {
			r.drop(k)
			if r.ctx.Err() != nil {
				r.bad[k] = true
				return nil
			}
			if errors.Is(res.err, errCRC) || errors.Is(res.err, errStatus) || st.retried {
				log.Printf("file %s shard %d stripe %d on store %s: %v", r.f.ID, k, r.cur, r.f.Stores[k], res.err)
				r.bad[k] = true
				return nil
			}
			log.Printf("file %s shard %d stripe %d on store %s: %v, retrying", r.f.ID, k, r.cur, r.f.Stores[k], res.err)
			r.open(k).retried = true // connection reset or stall: retry once
			continue
		}
		st.retried = false
		if st.took == 0 {
			st.took = time.Since(st.opened)
		}
		if res.s == r.cur { // a shard opened ahead of its first use hands in older stripes first
			if since.IsZero() {
				r.held[k] = 0
			} else {
				r.held[k] += time.Since(since)
			}
			r.blocks[k] = res.data
			return res.data
		}
	}
}

// firstTook is the longest any other open shard took to its first block.
func (r *Reader) firstTook(k int) time.Duration {
	var d time.Duration
	for j, st := range r.streams {
		if j != k && st != nil {
			d = max(d, st.took)
		}
	}
	return d
}

// peersReady reports whether every other open shard has the current stripe's block in hand.
func (r *Reader) peersReady(k int) bool {
	n := 0
	for j, st := range r.streams {
		if j == k || st == nil {
			continue
		}
		if r.blocks[j] == nil && len(st.out) == 0 {
			return false
		}
		n++
	}
	return n > 0
}

// rebuild reconstructs the current stripe's missing data blocks and block k
// from any data_shards good ones, data first.
func (r *Reader) rebuild(k int) error {
	tried := make([]bool, len(r.blocks))
	for {
		have := 0
		for _, b := range r.blocks {
			if b != nil {
				have++
			}
		}
		if have >= r.l.Data {
			break
		}
		var next []int
		for i := range r.blocks {
			if len(next) < r.l.Data-have && r.blocks[i] == nil && !r.bad[i] && !tried[i] {
				next = append(next, i)
				tried[i] = true
			}
		}
		if len(next) == 0 {
			if r.revive(tried) {
				continue
			}
			return ErrUnavailable
		}
		r.Start(next)
		for _, i := range next {
			r.recv(i)
		}
	}
	shards := make([][]byte, len(r.blocks))
	required := make([]bool, len(r.blocks))
	need := false
	for i, b := range r.blocks {
		shards[i] = b
		if b == nil && (i < r.l.Data || i == k) {
			if r.spare[i] == nil {
				r.spare[i] = r.slot()
			}
			shards[i] = r.spare[i][:0] // reused, cap 64 KiB + 4
			required[i] = true
			need = true
		}
	}
	if !need {
		return nil
	}
	if err := r.enc.ReconstructSome(shards, required); err != nil {
		return err
	}
	for i, ok := range required {
		if ok {
			r.blocks[i] = shards[i]
		}
	}
	r.Rebuilt++
	return nil
}

// revive takes back the shards dropped only for being slow once nothing else
// is left, and stops dropping for slowness: slowness alone never fails a read.
func (r *Reader) revive(tried []bool) bool {
	found := false
	for k, s := range r.slowed {
		if s {
			r.slowed[k], r.bad[k], r.held[k], tried[k] = false, false, 0, false
			found = true
		}
	}
	r.patient = r.patient || found
	return found
}

func (r *Reader) open(k int) *stream {
	ctx, cancel := context.WithCancelCause(r.ctx)
	st := &stream{out: make(chan result, 2), cancel: cancel, done: make(chan struct{}), opened: time.Now()}
	r.streams[k] = st
	r.all = append(r.all, st)
	first := r.cur
	go func() {
		err := r.stream(ctx, st, k, first)
		if ctx.Err() != nil {
			err = context.Cause(ctx)
		}
		st.err = err
		close(st.done) // never a send that can be skipped: recv always learns the stream ended
	}()
	return st
}

func (r *Reader) drop(k int) {
	if st := r.streams[k]; st != nil {
		st.cancel(nil)
		r.streams[k] = nil
	}
}

// stream reads shard k's blocks first..last into its two slots, CRC checked.
func (r *Reader) stream(ctx context.Context, st *stream, k int, first int64) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, URL(r.addrs[k], r.f.Stores[k], r.f.ID, k), nil)
	if err != nil {
		return err
	}
	end := r.l.BlockOffset(r.last) + int64(r.l.BlockLen(r.last)) + 4
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", r.l.BlockOffset(first), end-1))
	dead := time.AfterFunc(Stall, func() { st.cancel(errStalled) })
	defer dead.Stop()
	resp, err := r.c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusPartialContent {
		return fmt.Errorf("%w: %s %s", errStatus, r.addrs[k], resp.Status)
	}
	for s := first; s <= r.last; s++ {
		dead.Stop()
		if err := r.wait(ctx, s); err != nil {
			return err
		}
		i := s % 2
		if st.slots[i] == nil {
			st.slots[i] = r.slot()
		}
		buf := st.slots[i][:r.l.BlockLen(s)+4]
		dead.Reset(Stall)
		_, err := io.ReadFull(resp.Body, buf)
		dead.Stop()
		if err != nil {
			return err
		}
		data, ok := Check(buf)
		if !ok {
			return errCRC
		}
		select {
		case st.out <- result{s: s, data: data}:
			select {
			case r.notify <- struct{}{}:
			default: // recv is awake, it will see this one too
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// wait blocks until stripe s's slot is free: s is the stripe being read or the one after.
func (r *Reader) wait(ctx context.Context, s int64) error {
	for {
		r.mu.Lock()
		ok, wake := s <= r.window+1, r.wake
		r.mu.Unlock()
		if ok {
			return nil
		}
		select {
		case <-wake:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

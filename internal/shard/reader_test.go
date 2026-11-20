package shard

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"filescale/internal/db"
	"filescale/internal/erasure"
)

// A held block must survive the streams running ahead: stripe s+2 may not reuse its slot.
func TestReaderKeepsHeldSlots(t *testing.T) {
	data, f, addrs := fakeFile(t, 5*8*erasure.BlockSize+1000, nil, 0, nil)
	l := erasure.Layout{Size: f.Size, Data: 8}
	r := reader(t, f, addrs, 0, l.Stripes()-1)
	var held [][]byte
	for k := range 8 {
		b, err := r.Block(0, k)
		if err != nil {
			t.Fatal(err)
		}
		held = append(held, b)
	}
	time.Sleep(100 * time.Millisecond) // streams fill stripe 1 and wait
	for k, b := range held {
		if !bytes.Equal(b, want(data, l, 0, k)) {
			t.Errorf("shard %d's stripe 0 block changed while held", k)
		}
	}
	for s := range l.Stripes() {
		for k := range 8 {
			b, err := r.Block(s, k)
			if err != nil || !bytes.Equal(b, want(data, l, s, k)) {
				t.Fatalf("stripe %d shard %d: %v", s, k, err)
			}
		}
	}
	if r.Rebuilt != 0 {
		t.Errorf("rebuilt %d stripes with every shard healthy", r.Rebuilt)
	}
}

func TestReaderRebuilds(t *testing.T) {
	data, f, addrs := fakeFile(t, 3*8*erasure.BlockSize, map[int]bool{0: true, 5: true}, 0, nil) // 404
	l := erasure.Layout{Size: f.Size, Data: 8}
	addrs[3] = "" // offline in memberlist
	r := reader(t, f, addrs, 0, l.Stripes()-1)
	for s := range l.Stripes() {
		for k := range 8 {
			if b, err := r.Block(s, k); err != nil || !bytes.Equal(b, want(data, l, s, k)) {
				t.Fatalf("stripe %d shard %d: %v", s, k, err)
			}
		}
	}
	if r.Rebuilt != int(l.Stripes()) {
		t.Errorf("rebuilt %d stripes, want %d", r.Rebuilt, l.Stripes())
	}
	addrs[8], addrs[9] = "", ""
	r = reader(t, f, addrs, 0, 0)
	if _, err := r.Block(0, 0); err != ErrUnavailable {
		t.Errorf("5 shards lost: %v", err)
	}
}

func reader(t *testing.T, f *db.File, addrs []string, first, last int64) *Reader {
	r, err := NewReader(context.Background(), NewClient(""), f, addrs, first, last)
	if err != nil {
		t.Fatal(err)
	}
	r.Slow = time.Second
	t.Cleanup(r.Close)
	return r
}

// fakeFile encodes random data as 8+4 and serves each shard's block stream, delay before each
// response. drop maps a shard to how many of its GETs end early, like a connection dying mid-block.
func fakeFile(t *testing.T, size int, missing map[int]bool, delay time.Duration, drop map[int]int) ([]byte, *db.File, []string) {
	data := make([]byte, size)
	rand.NewChaCha8([32]byte{}).Read(data)
	l := erasure.Layout{Size: int64(size), Data: 8}
	enc, err := erasure.New(8, 4)
	if err != nil {
		t.Fatal(err)
	}
	streams := make([][]byte, 12)
	drops := make([]atomic.Int64, 12)
	for k := range drops {
		drops[k].Store(int64(max(0, drop[k])))
	}
	for s := range l.Stripes() {
		n := l.BlockLen(s)
		stripe := make([]byte, 8*n)
		copy(stripe, data[s*l.StripeSize():])
		shards := make([][]byte, 12)
		for k := range shards {
			if k < 8 {
				shards[k] = stripe[k*n : (k+1)*n]
			} else {
				shards[k] = make([]byte, n)
			}
		}
		if err := enc.Encode(shards); err != nil {
			t.Fatal(err)
		}
		for k, b := range shards {
			streams[k] = binary.LittleEndian.AppendUint32(append(streams[k], b...), CRC(b))
		}
	}
	srv := newServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		k, _ := strconv.Atoi(r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:])
		if missing[k] {
			http.NotFound(w, r)
			return
		}
		if drops[k].Add(-1) >= 0 {
			w.Header().Set("Content-Type", "application/octet-stream")
			w.WriteHeader(http.StatusPartialContent)
			w.Write(streams[k][:10]) // the body ends early, like a connection dying mid-block
			return
		}
		time.Sleep(delay)
		http.ServeContent(w, r, "", time.Time{}, bytes.NewReader(streams[k]))
	}))
	t.Cleanup(srv.Close)
	f := &db.File{Size: int64(size), DataShards: 8, ParityShards: 4, Stores: make([]db.UUID, 12)}
	addrs := make([]string, 12)
	for k := range addrs {
		addrs[k] = srv.Listener.Addr().String()
	}
	return data, f, addrs
}

// want is shard k's block of stripe s, zero padded in the last stripe.
func want(data []byte, l erasure.Layout, s int64, k int) []byte {
	n := int64(l.BlockLen(s))
	b := make([]byte, n)
	if start := s*l.StripeSize() + int64(k)*n; start < int64(len(data)) {
		copy(b, data[start:])
	}
	return b
}

// When every store is equally slow nothing is dropped: the read just takes longer.
func TestReaderAllSlow(t *testing.T) {
	data, f, addrs := fakeFile(t, 2*8*erasure.BlockSize, nil, 300*time.Millisecond, nil)
	l := erasure.Layout{Size: f.Size, Data: 8}
	r := reader(t, f, addrs, 0, l.Stripes()-1)
	r.Slow = 100 * time.Millisecond // under the old fixed limit every shard got dropped
	for s := range l.Stripes() {
		for k := range 8 {
			if b, err := r.Block(s, k); err != nil || !bytes.Equal(b, want(data, l, s, k)) {
				t.Fatalf("stripe %d shard %d: %v", s, k, err)
			}
		}
	}
	if r.Rebuilt != 0 {
		t.Errorf("%d stripes rebuilt, equally slow stores aren't slow", r.Rebuilt)
	}
}

// Shards dropped as slow come back when too few are left, instead of failing the read.
func TestReaderRevivesSlowShards(t *testing.T) {
	data, f, addrs := fakeFile(t, 8*erasure.BlockSize, nil, 0, nil)
	l := erasure.Layout{Size: f.Size, Data: 8}
	r := reader(t, f, addrs, 0, 0)
	for k := range 5 { // one more than there is parity
		r.Bad(k)
		r.slowed[k] = true
	}
	for k := range 8 {
		if b, err := r.Block(0, k); err != nil || !bytes.Equal(b, want(data, l, 0, k)) {
			t.Fatalf("shard %d: %v", k, err)
		}
	}
}

// A stream killed by its stall timer must still tell recv, even once nothing gets dropped for slowness any more.
func TestReaderStreamDiesQuietly(t *testing.T) {
	data, f, addrs := fakeFile(t, 2*8*erasure.BlockSize, nil, 200*time.Millisecond, nil)
	l := erasure.Layout{Size: f.Size, Data: 8}
	r := reader(t, f, addrs, 0, l.Stripes()-1)
	r.patient = true // after revive: only an ended stream can get recv out of waiting
	r.Start(seq(8))
	r.streams[3].cancel(errStalled) // what the stall timer does
	done := make(chan error, 1)
	go func() {
		for s := range l.Stripes() {
			for k := range 8 {
				if b, err := r.Block(s, k); err != nil || !bytes.Equal(b, want(data, l, s, k)) {
					done <- fmt.Errorf("stripe %d shard %d: %v", s, k, err)
					return
				}
			}
		}
		done <- nil
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("hung waiting on a stream that had ended")
	}
}

// A connection dying mid-block is retried once; a shard that keeps dying is rebuilt
// from parity.
func TestReaderRetriesReset(t *testing.T) {
	data, f, addrs := fakeFile(t, 2*8*erasure.BlockSize, nil, 0, map[int]int{3: 1})
	l := erasure.Layout{Size: f.Size, Data: 8}
	r := reader(t, f, addrs, 0, l.Stripes()-1)
	for s := range l.Stripes() {
		for k := range 8 {
			if b, err := r.Block(s, k); err != nil || !bytes.Equal(b, want(data, l, s, k)) {
				t.Fatalf("stripe %d shard %d: %v", s, k, err)
			}
		}
	}
	if r.Rebuilt != 0 {
		t.Errorf("%d stripes rebuilt, the retry succeeded", r.Rebuilt)
	}

	data, f, addrs = fakeFile(t, 8*erasure.BlockSize, nil, 0, map[int]int{0: 999}) // never a full body
	r = reader(t, f, addrs, 0, 0)
	for k := range 8 {
		if b, err := r.Block(0, k); err != nil || !bytes.Equal(b, want(data, l, 0, k)) {
			t.Fatalf("shard %d: %v", k, err)
		}
	}
	if r.Rebuilt == 0 {
		t.Error("a shard that always dies must be rebuilt from parity")
	}
}

func seq(n int) []int {
	s := make([]int, n)
	for i := range s {
		s[i] = i
	}
	return s
}

// newServer is httptest.NewServer on the listener production uses (segments capped at a jumbo frame).
func newServer(t *testing.T, h http.Handler) *httptest.Server {
	ln, err := Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := httptest.NewUnstartedServer(h)
	s.Listener.Close()
	s.Listener = ln
	s.Start()
	return s
}

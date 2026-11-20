package erasure

import (
	"bytes"
	"errors"
	"math/rand/v2"
	"testing"

	"github.com/klauspost/reedsolomon"
)

// A full stripe must survive losing as many shards as it has parity, here all from the data side.
func TestReconstructParityLost(t *testing.T) {
	for _, tc := range []struct{ data, parity int }{{8, 4}, {4, 2}} {
		shards, want := encode(t, tc.data, tc.parity)
		for i := range tc.parity {
			shards[i] = nil
		}
		rebuild(t, tc.data, tc.parity, shards, want)
	}
}

func TestRatioChange(t *testing.T) {
	old, oldWant := encode(t, 8, 4)
	cur, curWant := encode(t, 10, 4)

	for i := range 4 {
		old[i*3] = nil
		cur[i*3] = nil
	}
	rebuild(t, 8, 4, old, oldWant)
	rebuild(t, 10, 4, cur, curWant)
}

func TestRatioChangeNeedsRecordedLayout(t *testing.T) {
	shards, _ := encode(t, 8, 4)
	if err := encoder(t, 10, 4).Reconstruct(shards); !errors.Is(err, reedsolomon.ErrTooFewShards) {
		t.Errorf("10+4 on an 8+4 file: err = %v, want ErrTooFewShards", err)
	}

	shards, want := encode(t, 9, 3)
	shards[0] = nil
	if err := encoder(t, 8, 4).Reconstruct(shards); err != nil || bytes.Equal(shards[0], want[0]) {
		t.Errorf("8+4 on a 9+3 file: err = %v, want silently wrong data", err)
	}
}

func encoder(t *testing.T, data, parity int) reedsolomon.Encoder {
	t.Helper()
	enc, err := New(data, parity)
	if err != nil {
		t.Fatal(err)
	}
	return enc
}

func encode(t *testing.T, data, parity int) (shards, want [][]byte) {
	t.Helper()
	enc := encoder(t, data, parity)
	buf := make([]byte, data*BlockSize)
	rand.NewChaCha8([32]byte{}).Read(buf) // random so every shard differs
	shards, err := enc.Split(buf)
	if err != nil {
		t.Fatal(err)
	}
	if err := enc.Encode(shards); err != nil {
		t.Fatal(err)
	}
	want = make([][]byte, len(shards))
	for i, s := range shards {
		want[i] = bytes.Clone(s)
	}
	return shards, want
}

func rebuild(t *testing.T, data, parity int, shards, want [][]byte) {
	t.Helper()
	if err := encoder(t, data, parity).Reconstruct(shards); err != nil {
		t.Fatal(err)
	}
	for i := range shards {
		if !bytes.Equal(shards[i], want[i]) {
			t.Errorf("%d+%d: shard %d differs after reconstruct", data, parity, i)
		}
	}
}

func TestLayout(t *testing.T) {
	for _, tc := range []struct {
		size    int64
		data    int
		stripes int64
		last    int // block length in the last stripe
		shard   int64
	}{
		{0, 1, 1, 0, 4},
		{19, 8, 1, 3, 3 + 4},
		{512 << 10, 8, 1, BlockSize, BlockSize + 4},
		{512<<10 + 1, 8, 2, 1, BlockSize + 4 + 1 + 4},
		{100 << 10, 1, 2, 36 << 10, BlockSize + 4 + 36<<10 + 4},
	} {
		l := Layout{tc.size, tc.data}
		if s := l.Stripes(); s != tc.stripes {
			t.Errorf("%+v: stripes = %d, want %d", l, s, tc.stripes)
		}
		if n := l.BlockLen(tc.stripes - 1); n != tc.last {
			t.Errorf("%+v: last block = %d, want %d", l, n, tc.last)
		}
		if n := l.ShardSize(); n != tc.shard {
			t.Errorf("%+v: shard size = %d, want %d", l, n, tc.shard)
		}
	}
}

// For any size: every stripe but the last is full, the last one's blocks hold its bytes
// with under one byte of padding per data shard, and a shard is its blocks plus CRCs.
func FuzzLayout(f *testing.F) {
	f.Add(int64(0), 1)
	f.Add(int64(19), 8)
	f.Add(int64(512<<10), 8)
	f.Add(int64(512<<10+1), 8)
	f.Add(int64(1<<40+3), 10)
	f.Fuzz(func(t *testing.T, size int64, data int) {
		if size < 0 || size > 1<<50 || data < 1 || data > 256 {
			return
		}
		l := Layout{size, data}
		last := l.Stripes() - 1
		n := int64(l.BlockLen(last))
		tail := size - last*l.StripeSize() // the bytes in the last stripe
		if last > 0 && (tail <= 0 || l.BlockLen(last-1) != BlockSize) || n > BlockSize || n*int64(data) < tail || n*int64(data)-tail >= int64(data) {
			t.Fatalf("%+v: %d stripes, last block %d for %d bytes", l, last+1, n, tail)
		}
		if l.ShardSize() != last*(BlockSize+4)+n+4 {
			t.Fatalf("%+v: shard size %d", l, l.ShardSize())
		}
	})
}

// BenchmarkReconstructOne is the CPU a degraded read pays per stripe: one 8+4 data block rebuilt.
func BenchmarkReconstructOne(b *testing.B) {
	enc, err := New(8, 4)
	if err != nil {
		b.Fatal(err)
	}
	shards := make([][]byte, 12)
	for i := range shards {
		shards[i] = make([]byte, BlockSize)
	}
	required := make([]bool, 12)
	required[0] = true
	b.SetBytes(8 * BlockSize) // stripe bytes delivered to the client
	for b.Loop() {
		shards[0] = shards[0][:0]
		if err := enc.ReconstructSome(shards, required); err != nil {
			b.Fatal(err)
		}
	}
}

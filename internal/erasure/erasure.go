package erasure

import (
	"sync"

	"github.com/klauspost/reedsolomon"
)

const BlockSize = 64 << 10

var encoders sync.Map // [2]int{data, parity} → reedsolomon.Encoder

func New(data, parity int) (reedsolomon.Encoder, error) {
	key := [2]int{data, parity}
	if enc, ok := encoders.Load(key); ok {
		return enc.(reedsolomon.Encoder), nil
	}
	enc, err := reedsolomon.New(data, parity, reedsolomon.WithAutoGoroutines(BlockSize))
	if err != nil {
		return nil, err
	}
	encoders.Store(key, enc)
	return enc, nil
}

// Layout is a file's block geometry, the same in every shard. Data = 1 means replicated.
type Layout struct {
	Size int64
	Data int
}

func (l Layout) StripeSize() int64 { return int64(l.Data) * BlockSize }

func (l Layout) Stripes() int64 {
	return max(1, (l.Size+l.StripeSize()-1)/l.StripeSize())
}

// BlockLen is the length of stripe s's block in each shard: 64 KiB, less in the last stripe.
func (l Layout) BlockLen(s int64) int {
	if s < l.Stripes()-1 {
		return BlockSize
	}
	last := l.Size - s*l.StripeSize()
	return int((last + int64(l.Data) - 1) / int64(l.Data))
}

// BlockOffset is where stripe s's block starts in a shard's block stream.
func (l Layout) BlockOffset(s int64) int64 { return s * (BlockSize + 4) }

// ShardSize is the length of one shard's block stream, a CRC after every block.
func (l Layout) ShardSize() int64 {
	last := l.Stripes() - 1
	return l.BlockOffset(last) + int64(l.BlockLen(last)) + 4
}

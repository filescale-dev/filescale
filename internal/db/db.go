package db

import (
	"context"
	"fmt"
	"slices"
	"time"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

type UUID = gocql.UUID

type File struct {
	Name         string
	ID           UUID
	Size         int64
	BLAKE3       []byte // of the content
	ContentType  string
	DataShards   int // 1 = replicated, every store holds a full copy
	ParityShards int
	Stores       []UUID // part i's shard k is at i*(DataShards+ParityShards)+k
	Parts        []UUID // nil = one part, stored under the file's own ID
	PartSize     int64
}

func (f *File) NumParts() int { return max(1, len(f.Parts)) }

// CheckLayout reports whether f's size, shards, parts and stores agree.
func (f *File) CheckLayout() error {
	if f.Size < 0 {
		return fmt.Errorf("negative size %d", f.Size)
	}
	if f.DataShards <= 0 || f.ParityShards < 0 {
		return fmt.Errorf("bad shard counts %d+%d", f.DataShards, f.ParityShards)
	}
	if f.Size == 0 {
		if len(f.Stores) != 0 || len(f.Parts) != 0 || f.PartSize != 0 {
			return fmt.Errorf("empty file has %d stores, %d parts, part_size %d", len(f.Stores), len(f.Parts), f.PartSize)
		}
		return nil
	}
	n := f.DataShards + f.ParityShards
	if n <= 0 {
		return fmt.Errorf("bad shard counts %d+%d", f.DataShards, f.ParityShards)
	}
	if len(f.Parts) == 0 {
		if f.PartSize != 0 {
			return fmt.Errorf("single-part file has part_size %d", f.PartSize)
		}
		if len(f.Stores) != n {
			return fmt.Errorf("stores %d != %d", len(f.Stores), n)
		}
		return nil
	}
	if f.PartSize <= 0 {
		return fmt.Errorf("non-positive part_size %d", f.PartSize)
	}
	if want := (f.Size-1)/f.PartSize + 1; want != int64(len(f.Parts)) {
		return fmt.Errorf("parts %d != %d", len(f.Parts), want)
	}
	if want := int64(len(f.Parts)) * int64(n); int64(len(f.Stores)) != want {
		return fmt.Errorf("stores %d != %d", len(f.Stores), want)
	}
	return nil
}

// Part is part i as a file of its own, what reads, repair and scrub work on.
func (f *File) Part(i int) *File {
	p := *f
	if len(f.Parts) == 0 {
		return &p
	}
	n := f.DataShards + f.ParityShards
	p.ID = f.Parts[i]
	p.Size = min(f.PartSize, f.Size-int64(i)*f.PartSize)
	p.Stores = f.Stores[i*n : (i+1)*n]
	p.Parts, p.PartSize = nil, 0
	return &p
}

// PartOf is the index of the part stored under id, -1 if it isn't one of f's.
func (f *File) PartOf(id UUID) int {
	if len(f.Parts) == 0 {
		if id == f.ID {
			return 0
		}
		return -1
	}
	return slices.Index(f.Parts, id)
}

// UploadTTL is how long a finished part of an unfinished upload is kept, under cleanup's 24h.
const UploadTTL = 23 * time.Hour

// UploadPart is an uploads row: a finished part of an upload not committed yet.
type UploadPart struct {
	Name         string
	UploadID     UUID
	Index        int
	ID           UUID
	Size         int64
	Total        int64  // the whole file
	HashState    []byte // BLAKE3 state after this part, see b3
	ContentType  string
	DataShards   int
	ParityShards int
	Stores       []UUID
	Written      time.Time
}

// Shard is a shards_by_store row.
type Shard struct {
	StoreID UUID
	FileID  UUID
	Index   int
	Name    string
	Written time.Time
}

type Store struct {
	ID       UUID
	Addr     string
	State    string // "" (up) or "maintenance"
	Free     int64  // bytes left for uploads
	LastSeen time.Time
}

type allKey struct{}

// All makes File and Shards ask every replica, for reads a delete is decided on:
// one that hasn't got the rows yet (RF raised, node added) reads as empty.
func All(ctx context.Context) context.Context { return context.WithValue(ctx, allKey{}, true) }

func IsAll(ctx context.Context) bool { return ctx.Value(allKey{}) != nil }

func NewID() (UUID, error) { return gocql.RandomUUID() }

func ParseID(s string) (UUID, error) { return gocql.ParseUUID(s) }

const Buckets = 1 << 16

// Bucket is the first 2 bytes of the file ID, the same as its prefix dir.
func Bucket(id UUID) int { return int(id[0])<<8 | int(id[1]) }

type DB interface {
	File(ctx context.Context, name string) (*File, error) // nil if missing
	PutFile(ctx context.Context, f *File) error
	DeleteFile(ctx context.Context, name string) error
	// SetShardStore moves one shard, only while name still holds fileID.
	SetShardStore(ctx context.Context, name string, fileID UUID, shard int, store UUID) (bool, error)
	PutShard(ctx context.Context, s Shard) error
	DeleteShard(ctx context.Context, storeID, fileID UUID) error
	Shards(ctx context.Context, storeID UUID, bucket int) ([]Shard, error)
	Stores(ctx context.Context) ([]Store, error)
	Heartbeat(ctx context.Context, s Store) error // leaves state alone
	DeleteStore(ctx context.Context, id UUID) error
	PutPart(ctx context.Context, p UploadPart) error
	UploadParts(ctx context.Context, name string, upload UUID) ([]UploadPart, error) // by Index
}

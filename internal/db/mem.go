package db

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"sync"
	"time"
)

// Mem is an in-memory DB for tests.
type Mem struct {
	Clock func() time.Time // write time of new shard and uploads rows, time.Now if nil

	mu      sync.Mutex
	files   map[string]File
	shards  map[UUID]map[UUID]Shard
	stores  map[UUID]Store
	uploads map[upload]map[int]UploadPart
}

type upload struct {
	name string
	id   UUID
}

func NewMem() *Mem {
	return &Mem{files: map[string]File{}, shards: map[UUID]map[UUID]Shard{}, stores: map[UUID]Store{}, uploads: map[upload]map[int]UploadPart{}}
}

func (m *Mem) now() time.Time {
	if m.Clock != nil {
		return m.Clock()
	}
	return time.Now()
}

func (m *Mem) File(_ context.Context, name string) (*File, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	f, ok := m.files[name]
	if !ok {
		return nil, nil
	}
	f.Stores = slices.Clone(f.Stores)
	f.Parts = slices.Clone(f.Parts)
	return &f, nil
}

func (m *Mem) PutFile(_ context.Context, f *File) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	c := *f
	c.Stores = slices.Clone(f.Stores)
	c.Parts = slices.Clone(f.Parts)
	m.files[f.Name] = c
	return nil
}

func (m *Mem) PutPart(_ context.Context, p UploadPart) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	p.Written = m.now()
	p.Stores = slices.Clone(p.Stores)
	p.HashState = slices.Clone(p.HashState)
	k := upload{p.Name, p.UploadID}
	if m.uploads[k] == nil {
		m.uploads[k] = map[int]UploadPart{}
	}
	m.uploads[k][p.Index] = p
	return nil
}

// UploadParts hides rows past UploadTTL, like Cassandra's TTL.
func (m *Mem) UploadParts(_ context.Context, name string, id UUID) ([]UploadPart, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []UploadPart
	for _, p := range m.uploads[upload{name, id}] {
		if time.Since(p.Written) < UploadTTL {
			p.Stores = slices.Clone(p.Stores)
			out = append(out, p)
		}
	}
	slices.SortFunc(out, func(a, b UploadPart) int { return a.Index - b.Index })
	return out, nil
}

func (m *Mem) DeleteFile(_ context.Context, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.files, name)
	return nil
}

func (m *Mem) SetShardStore(_ context.Context, name string, fileID UUID, shard int, store UUID) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	f, ok := m.files[name]
	if !ok || f.ID != fileID {
		return false, nil
	}
	if shard < 0 || shard >= len(f.Stores) {
		return false, fmt.Errorf("shard %d out of range [0,%d)", shard, len(f.Stores))
	}
	f.Stores = slices.Clone(f.Stores)
	f.Stores[shard] = store
	m.files[name] = f
	return true, nil
}

func (m *Mem) PutShard(_ context.Context, s Shard) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	s.Written = m.now()
	if m.shards[s.StoreID] == nil {
		m.shards[s.StoreID] = map[UUID]Shard{}
	}
	m.shards[s.StoreID][s.FileID] = s
	return nil
}

func (m *Mem) DeleteShard(_ context.Context, storeID, fileID UUID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.shards[storeID], fileID)
	return nil
}

func (m *Mem) Shards(_ context.Context, storeID UUID, bucket int) ([]Shard, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Shard
	for _, s := range m.shards[storeID] {
		if Bucket(s.FileID) == bucket {
			out = append(out, s)
		}
	}
	return out, nil
}

func (m *Mem) Stores(context.Context) ([]Store, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Store
	for _, s := range m.stores {
		out = append(out, s)
	}
	slices.SortFunc(out, func(a, b Store) int { return bytes.Compare(a.ID[:], b.ID[:]) })
	return out, nil
}

func (m *Mem) Heartbeat(_ context.Context, s Store) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	s.State = m.stores[s.ID].State
	m.stores[s.ID] = s
	return nil
}

func (m *Mem) DeleteStore(_ context.Context, id UUID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.stores, id)
	return nil
}

// SetState is what an operator does in cqlsh.
func (m *Mem) SetState(id UUID, state string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.stores[id]
	s.State = state
	m.stores[id] = s
}

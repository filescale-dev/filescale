package serve

import (
	"context"
	"fmt"
	"log"
	"maps"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"filescale/internal/db"
	"filescale/internal/erasure"
	"filescale/internal/shard"
)

// Repair rebuilds the shards of stores down longer than after, never touching
// maintenance. Each serve takes the buckets that hash to it, no leader.
func (s *Serve) Repair(ctx context.Context, after time.Duration) error {
	if err := s.Reload(ctx); err != nil {
		return err
	}
	all, self := s.Serves()
	slices.Sort(all)
	me := slices.Index(all, self)
	if me < 0 {
		return nil
	}
	start := time.Now()
	// a failed bucket doesn't end the pass, the next one retries it
	failed := 0
	var first error
	for b := me; b < db.Buckets && ctx.Err() == nil; b += len(all) {
		// checked every bucket: a store back up mid-pass is left alone
		down := s.down(after)
		if len(down) == 0 {
			break
		}
		if err := s.repairBucket(ctx, down, b); err != nil {
			failed++
			if first == nil {
				first = err
			}
		}
	}
	if me == 0 {
		s.forget(ctx, s.down(after))
	}
	if ctx.Err() == nil {
		s.repairTook.Store(int64(time.Since(start)))
		s.repairEnded.Store(time.Now().UnixNano())
		s.repairFailed.Store(int64(failed))
	}
	if failed > 0 {
		return fmt.Errorf("failed buckets: %d, first: %w", failed, first)
	}
	return ctx.Err()
}

// down lists stores not seen for longer than after, never maintenance.
func (s *Serve) down(after time.Duration) []db.Store {
	var out []db.Store
	for _, st := range s.stores.Load().list {
		if st.State != "maintenance" && time.Since(st.LastSeen) > after {
			out = append(out, st)
		}
	}
	return out
}

func (s *Serve) repairBucket(ctx context.Context, down []db.Store, b int) error {
	// a part's shards all share its bucket, so this sees every lost shard of these parts
	files := map[db.UUID][]db.Shard{}
	for _, st := range down {
		rows, err := s.DB.Shards(ctx, st.ID, b)
		if err != nil {
			return err
		}
		for _, r := range rows {
			files[r.FileID] = append(files[r.FileID], r)
		}
	}
	jobs := slices.Collect(maps.Values(files))
	slices.SortFunc(jobs, func(a, b []db.Shard) int { return len(b) - len(a) }) // most missing first
	for _, rows := range jobs {
		if err := s.repairFile(ctx, rows); err != nil {
			s.repairErrors.Add(1)
			log.Printf("repair %s: %v", rows[0].Name, err)
		}
	}
	return nil
}

func (s *Serve) repairFile(ctx context.Context, rows []db.Shard) error {
	f, err := s.DB.File(ctx, rows[0].Name)
	if err != nil {
		return err
	}
	orphan := func(f *db.File, r db.Shard) bool {
		if f == nil {
			return true
		}
		pi := f.PartOf(r.FileID)
		return pi < 0 || r.Index < 0 || r.Index >= f.DataShards+f.ParityShards || f.Part(pi).Stores[r.Index] != r.StoreID
	}
	var stale bool
	for _, r := range rows {
		if orphan(f, r) && time.Since(r.Written) > 24*time.Hour {
			stale = true
			break
		}
	}
	if stale {
		// a replica without the file row yet would have a live shard look orphaned
		if f, err = s.DB.File(db.All(ctx), rows[0].Name); err != nil {
			return err
		}
	}
	var lost []db.Shard
	for _, r := range rows {
		if orphan(f, r) {
			if time.Since(r.Written) > 24*time.Hour {
				s.DB.DeleteShard(ctx, r.StoreID, r.FileID)
			}
			continue
		}
		lost = append(lost, r)
	}
	if len(lost) == 0 {
		return nil
	}
	pi := f.PartOf(lost[0].FileID)

	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	p := f.Part(pi)
	l := erasure.Layout{Size: p.Size, Data: p.DataShards}
	rd, err := shard.NewReader(ctx, s.Client, p, s.addrs(p), 0, l.Stripes()-1)
	if err != nil {
		return err
	}
	defer rd.Close()
	idx := make([]int, len(lost))
	for i, r := range lost {
		idx[i] = r.Index
		rd.Bad(r.Index)
	}
	skip := map[string]bool{} // servers already holding a shard of this part
	for k, a := range s.tableAddrs(p) {
		if a != "" && !slices.Contains(idx, k) {
			skip[a] = true
		}
	}
	puts, err := s.open(ctx, p.ID, l.ShardSize(), idx, skip, true, nil)
	if err != nil {
		return err
	}
	blocks := make([][]byte, len(idx))
	for st := range l.Stripes() {
		for i, k := range idx {
			if blocks[i], err = rd.Block(st, k); err != nil {
				cancelPuts(puts)
				return err
			}
		}
		if err := writeAll(ctx, puts, blocks); err != nil {
			return err
		}
	}
	if err := finish(puts); err != nil {
		return err
	}

	n := f.DataShards + f.ParityShards
	for i, r := range lost {
		to := puts[i].store.ID
		if err := s.DB.PutShard(ctx, db.Shard{StoreID: to, FileID: p.ID, Index: r.Index, Name: f.Name}); err != nil {
			return err
		}
		ok, err := s.DB.SetShardStore(ctx, f.Name, f.ID, pi*n+r.Index, to)
		if err != nil {
			return err
		}
		if !ok { // overwritten meanwhile, the new shard is garbage, unless it's the old store's own row
			if to != r.StoreID {
				s.DB.DeleteShard(ctx, to, p.ID)
			}
			continue
		}
		s.repaired.Add(1)
		if to == r.StoreID { // back up mid-bucket and picked for its own shard: rewritten in place
			continue
		}
		if err := s.DB.DeleteShard(ctx, r.StoreID, p.ID); err != nil {
			return err
		}
		log.Printf("repaired %s part %d shard %d: store %s → %s", f.Name, pi, r.Index, r.StoreID, to)
	}
	return nil
}

// forget deletes the stores row of a down store once all its shards have moved.
func (s *Serve) forget(ctx context.Context, down []db.Store) {
	for _, st := range down {
		if s.empty(ctx, st.ID) {
			log.Printf("store %s is empty, deleting its stores row", st.ID)
			s.DB.DeleteStore(ctx, st.ID)
		}
	}
}

// empty checks every bucket of a store, 64 queries at a time, stopping at the first row.
func (s *Serve) empty(ctx context.Context, id db.UUID) bool {
	qctx, stop := context.WithCancel(ctx)
	defer stop()
	buckets := make(chan int)
	var found atomic.Bool
	var wg sync.WaitGroup
	for range 64 {
		wg.Go(func() {
			for b := range buckets {
				if rows, err := s.DB.Shards(db.All(qctx), id, b); err != nil || len(rows) > 0 {
					found.Store(true) // an error counts too, never delete on a guess
					stop()
				}
			}
		})
	}
	for b := 0; b < db.Buckets && qctx.Err() == nil; b++ {
		select {
		case buckets <- b:
		case <-qctx.Done():
		}
	}
	close(buckets)
	wg.Wait()
	return !found.Load() && ctx.Err() == nil
}

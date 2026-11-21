package store

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"filescale/internal/db"
	"filescale/internal/erasure"
	"filescale/internal/shard"
)

// young is how long a file or index row may lack its counterpart, an upload may still be committing.
const young = 24 * time.Hour

// Cleanup deletes shard files older than 24h that have no shards_by_store row, every disk at full speed.
func (s *Store) Cleanup(ctx context.Context) error {
	for _, d := range s.list {
		if err := s.cleanDisk(ctx, d, 0); err != nil {
			return err
		}
	}
	return nil
}

// cleanDisk goes through d one prefix dir and its bucket at a time, spread evenly over pass
// (bucket b starts b/65,536 of the way in), 0 = full speed.
func (s *Store) cleanDisk(ctx context.Context, d *disk, pass time.Duration) error {
	start, step := time.Now(), pass/db.Buckets
	err := d.walk(func(bucket int, dir string, names []string) error {
		sleep(ctx, time.Until(start.Add(step*time.Duration(bucket))))
		if err := ctx.Err(); err != nil {
			return err
		}
		rows, err := s.DB.Shards(ctx, d.id, bucket)
		if err != nil { // skip it, the next pass retries
			log.Printf("cleanup %s bucket %d: %v", d.dir, bucket, err)
			return ctx.Err()
		}
		keep := map[db.UUID]bool{}
		for _, r := range rows {
			keep[r.FileID] = true
		}
		var gone []string // old shard files without a row
		for _, name := range names {
			if id, _, ok := parseName(name); ok && keep[id] {
				continue
			}
			path := filepath.Join(dir, name)
			if st, err := os.Stat(path); err == nil && time.Since(st.ModTime()) > young {
				gone = append(gone, path)
			}
		}
		if len(gone) == 0 {
			return ctx.Err()
		}
		rows, err = s.DB.Shards(db.All(ctx), d.id, bucket)
		if err != nil {
			log.Printf("cleanup %s bucket %d: %v", d.dir, bucket, err)
			return ctx.Err()
		}
		if len(rows) == 0 && len(gone) > 1 { // a disk in use: metadata missing, not garbage
			log.Printf("cleanup %s bucket %d: %d old shards and no rows, skipped", d.dir, bucket, len(gone))
			return ctx.Err()
		}
		for _, r := range rows {
			keep[r.FileID] = true
		}
		for _, path := range gone {
			if id, _, _ := parseName(filepath.Base(path)); keep[id] {
				continue
			}
			log.Printf("cleanup: removing %s", path)
			if os.Remove(path) == nil {
				d.removed.Add(1)
			}
		}
		return ctx.Err()
	})
	if err == nil {
		d.cleaned.Store(time.Now().UnixNano())
	}
	return err
}

// scrubPass is how long a paced scrub pass takes, scrubGap the pause between passes.
const (
	scrubPass = 28 * 24 * time.Hour
	scrubGap  = 24 * time.Hour
)

// Scrub goes through every index row of this store's disks: rebuilds bad blocks, and
// whole shards a crash lost before writeback, from the file's other shards, and drops
// orphaned rows. Rows, not files, so a missing file is found too. Disks run in parallel,
// paced = each spreads its buckets over scrubPass, else full speed. Rows written before since are skipped.
func (s *Store) Scrub(ctx context.Context, paced bool, since time.Time) error {
	errs := make([]error, len(s.list))
	var wg sync.WaitGroup
	for i, d := range s.list {
		wg.Go(func() {
			p := pace{}
			if paced {
				p = pace{start: time.Now(), pass: scrubPass}
			}
			if err := s.scrubDisk(ctx, d, p, since); err != nil {
				errs[i] = fmt.Errorf("disk %s: %w", d.dir, err)
			}
		})
	}
	wg.Wait()
	return errors.Join(errs...)
}

// pace spreads a scrub over its buckets: bucket b is due at start + pass × b/65,536.
// By bucket, not by bytes: file_ids are random, so a disk's shards spread evenly over the
// buckets, and the pass takes the same time however much the disk holds (a disk shared with
// other data can't make it read faster).
type pace struct {
	start time.Time
	pass  time.Duration  // 0 = full speed
	next  int            // first bucket to scrub
	save  func(next int) // records progress every 256 buckets, nil = don't
}

// scrubDisk scrubs one disk's buckets from p.next on, see pace.
func (s *Store) scrubDisk(ctx context.Context, d *disk, p pace, since time.Time) error {
	var failed int
	var first error
	for b := p.next; b < db.Buckets; b++ {
		if p.pass > 0 {
			if wait := time.Until(p.start.Add(p.pass / db.Buckets * time.Duration(b))); wait > 0 {
				sleep(ctx, wait)
			}
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		rows, err := s.DB.Shards(ctx, d.id, b)
		if err != nil {
			log.Printf("scrub %s bucket %d: %v", d.dir, b, err)
			failed++
			if first == nil {
				first = err
			}
		}
		for _, row := range rows {
			if row.Written.Before(since) {
				continue
			}
			if _, err := s.scrubShard(ctx, d, row); err != nil {
				log.Printf("scrub %s shard %d: %v", row.FileID, row.Index, err)
			}
		}
		if p.save != nil && (b+1)%256 == 0 && b+1 < db.Buckets {
			p.save(b + 1)
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if failed > 0 {
		return fmt.Errorf("failed buckets: %d, first: %w", failed, first)
	}
	if since.IsZero() { // a whole pass, not a crash check
		d.scrubbed.Store(time.Now().UnixNano())
	}
	return nil
}

// scrubState is a disk's paced scrub progress, kept in <dir>/scrub so a restart resumes
// the pass where it was instead of starting 4 weeks over.
type scrubState struct {
	start time.Time // bucket b is due at start + scrubPass × b/65,536
	next  int       // first bucket not scrubbed yet in this pass
	last  time.Time // end of the last whole pass, zero = none yet
}

// loadScrub reads d's scrub progress, a fresh pass from now if there's none (or it's unreadable).
// A pass that fell behind (the store was down) is rebased to now, so it doesn't rush to catch up.
func loadScrub(d *disk) scrubState {
	now := time.Now()
	st := scrubState{start: now}
	b, err := os.ReadFile(filepath.Join(d.dir, "scrub"))
	if err == nil {
		var start, last int64
		var next int
		if _, err := fmt.Sscan(string(b), &start, &next, &last); err != nil || next < 0 || next > db.Buckets {
			log.Printf("scrub %s: unreadable progress %q, starting a new pass", d.dir, b)
		} else {
			st = scrubState{start: time.Unix(0, start), next: next}
			if last > 0 {
				st.last = time.Unix(0, last)
			}
		}
	} else if !errors.Is(err, fs.ErrNotExist) {
		log.Printf("scrub %s: %v, starting a new pass", d.dir, err)
	}
	if behind := now.Add(-scrubPass / db.Buckets * time.Duration(st.next)); behind.After(st.start) {
		st.start = behind
	}
	if !st.last.IsZero() {
		d.scrubbed.Store(st.last.UnixNano())
	}
	return st
}

// saveScrub records d's scrub progress. A failed write only costs resuming, it's logged.
func saveScrub(d *disk, st scrubState) {
	var last int64
	if !st.last.IsZero() {
		last = st.last.UnixNano()
	}
	path := filepath.Join(d.dir, "scrub")
	err := writeSync(path+".tmp", fmt.Appendf(nil, "%d %d %d\n", st.start.UnixNano(), st.next, last))
	if err == nil {
		err = os.Rename(path+".tmp", path)
	}
	if err != nil {
		log.Printf("scrub %s: saving progress: %v", d.dir, err)
	}
}

// scrubLoop scrubs d forever, paced passes with scrubGap between them, resuming a pass after a restart.
func (s *Store) scrubLoop(ctx context.Context, d *disk) {
	st := loadScrub(d)
	for ctx.Err() == nil {
		p := pace{start: st.start, pass: scrubPass, next: st.next, save: func(next int) {
			st.next = next
			saveScrub(d, st)
		}}
		if err := s.scrubDisk(ctx, d, p, time.Time{}); err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("scrub %s: %v", d.dir, err)
		}
		now := time.Now()
		st = scrubState{start: now.Add(scrubGap), last: st.last}
		if last := d.scrubbed.Load(); last > 0 {
			st.last = time.Unix(0, last)
		}
		saveScrub(d, st)
	}
}

func (s *Store) scrubShard(ctx context.Context, d *disk, row db.Shard) (int64, error) {
	f, err := s.DB.File(ctx, row.Name)
	if err != nil {
		return 0, err
	}
	k := row.Index
	orphan := func(f *db.File) bool {
		i := -1
		if f != nil {
			i = f.PartOf(row.FileID)
		}
		return i < 0 || k < 0 || k >= f.DataShards+f.ParityShards || f.Part(i).Stores[k] != d.id
	}
	if orphan(f) {
		if time.Since(row.Written) <= young {
			return 0, nil
		}
		// a replica without the file row yet would have a live shard look orphaned
		if f, err = s.DB.File(db.All(ctx), row.Name); err != nil {
			return 0, err
		}
		if orphan(f) {
			log.Printf("scrub: dropping orphan row %s on %s", row.FileID, d.id)
			if err := s.DB.DeleteShard(ctx, d.id, row.FileID); err != nil {
				return 0, err
			}
			d.orphans.Add(1)
			return 0, nil
		}
	}
	f = f.Part(f.PartOf(row.FileID))
	path := d.path(row.FileID, k)
	l := erasure.Layout{Size: f.Size, Data: f.DataShards}
	hdr := int64(len(shard.Magic))
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if errors.Is(err, fs.ErrNotExist) {
		log.Printf("scrub: %s is missing, rebuilding it", path)
		d.missing.Add(1)
		if err := s.rebuildMissing(ctx, path, f, k, l); err != nil {
			return 0, err
		}
		d.blocks.Add(l.Stripes())
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	defer file.Close()
	sequential(file)
	magic := make([]byte, hdr)
	if _, err := file.ReadAt(magic, 0); err != nil || string(magic) != shard.Magic {
		if _, err := file.WriteAt([]byte(shard.Magic), 0); err != nil {
			return 0, err
		}
	}
	buf := make([]byte, erasure.BlockSize+4)
	var bad []int64
	var read int64
	for st := range l.Stripes() {
		n := l.BlockLen(st) + 4
		_, err := file.ReadAt(buf[:n], hdr+l.BlockOffset(st))
		read += int64(n)
		if _, ok := shard.Check(buf[:n]); err != nil || !ok {
			bad = append(bad, st)
		}
	}
	if len(bad) == 0 {
		dontNeed(file)
		return read, nil
	}
	if err := s.rebuild(ctx, file, f, k, bad); err != nil {
		return read, err
	}
	d.blocks.Add(int64(len(bad)))
	return read, nil
}

func (s *Store) rebuildMissing(ctx context.Context, path string, f *db.File, k int, l erasure.Layout) error {
	file, err := createTemp(path)
	if err != nil {
		return err
	}
	tmp := file.Name()
	ok := false
	defer func() {
		file.Close()
		if !ok {
			os.Remove(tmp)
		}
	}()
	preallocate(file, int64(len(shard.Magic))+l.ShardSize())
	if _, err := file.WriteString(shard.Magic); err != nil {
		return err
	}
	bad := make([]int64, l.Stripes())
	for i := range bad {
		bad[i] = int64(i)
	}
	if err := s.rebuild(ctx, file, f, k, bad); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	ok = true
	return nil
}

// rebuild rewrites bad blocks of shard k in place from the file's other shards,
// one reader per run of consecutive bad blocks.
func (s *Store) rebuild(ctx context.Context, file *os.File, f *db.File, k int, bad []int64) error {
	stores, err := s.DB.Stores(ctx)
	if err != nil {
		return err
	}
	addr := map[db.UUID]string{}
	for _, st := range stores {
		addr[st.ID] = st.Addr
	}
	addrs := make([]string, len(f.Stores))
	for i, id := range f.Stores {
		addrs[i] = addr[id]
	}
	l := erasure.Layout{Size: f.Size, Data: f.DataShards}
	for i := 0; i < len(bad); {
		j := i + 1
		for j < len(bad) && bad[j] == bad[j-1]+1 {
			j++
		}
		if err := s.rebuildBlocks(ctx, file, f, addrs, k, bad[i], bad[j-1], l); err != nil {
			return err
		}
		log.Printf("scrub: rebuilt blocks %d-%d of %s shard %d", bad[i], bad[j-1], f.ID, k)
		i = j
	}
	return file.Sync()
}

func (s *Store) rebuildBlocks(ctx context.Context, file *os.File, f *db.File, addrs []string, k int, first, last int64, l erasure.Layout) error {
	r, err := shard.NewReader(ctx, s.Client, f, addrs, first, last)
	if err != nil {
		return err
	}
	defer r.Close()
	r.Bad(k)
	var block []byte
	for st := first; st <= last; st++ {
		b, err := r.Block(st, k)
		if err != nil {
			return err
		}
		block = binary.LittleEndian.AppendUint32(append(block[:0], b...), shard.CRC(b))
		if _, err := file.WriteAt(block, int64(len(shard.Magic))+l.BlockOffset(st)); err != nil {
			return err
		}
	}
	return nil
}

// walk calls fn for every prefix dir that holds files.
func (d *disk) walk(fn func(bucket int, dir string, names []string) error) error {
	tops, err := os.ReadDir(d.dir)
	if err != nil {
		return err
	}
	for _, a := range tops {
		hi, ok := hexByte(a.Name())
		if !ok || !a.IsDir() {
			continue
		}
		subs, err := os.ReadDir(filepath.Join(d.dir, a.Name()))
		if err != nil {
			return err
		}
		for _, b := range subs {
			lo, ok := hexByte(b.Name())
			if !ok || !b.IsDir() {
				continue
			}
			dir := filepath.Join(d.dir, a.Name(), b.Name())
			entries, err := os.ReadDir(dir)
			if err != nil {
				return err
			}
			if len(entries) == 0 {
				continue
			}
			names := make([]string, len(entries))
			for i, e := range entries {
				names[i] = e.Name()
			}
			if err := fn(hi<<8|lo, dir, names); err != nil {
				return err
			}
		}
	}
	return nil
}

func hexByte(s string) (int, bool) {
	if len(s) != 2 {
		return 0, false
	}
	v, err := strconv.ParseUint(s, 16, 8)
	return int(v), err == nil
}

// parseName splits "<file_id>.<shard>".
func parseName(name string) (db.UUID, int, bool) {
	id, k, ok := strings.Cut(name, ".")
	if !ok {
		return db.UUID{}, 0, false
	}
	fid, err1 := db.ParseID(id)
	n, err2 := strconv.Atoi(k)
	return fid, n, err1 == nil && err2 == nil
}

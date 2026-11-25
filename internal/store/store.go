package store

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"filescale/internal/db"
	"filescale/internal/erasure"
	"filescale/internal/metrics"
	"filescale/internal/shard"
)

var blockPool = sync.Pool{New: func() any { return make([]byte, erasure.BlockSize+4) }}

type Config struct {
	Dirs      []string
	MinFree   int64 // percent of each disk kept for repairs
	Token     string
	DB        db.DB
	Client    *shard.Client // fetches other shards when scrub finds a bad block
	ScrubPass time.Duration // paced scrub pass length; 0 = DefaultScrubPass
	ScrubGap  time.Duration // pause between scrub passes
}

type Store struct {
	Config
	disks          map[db.UUID]*disk
	list           []*disk
	mux            *http.ServeMux
	requests       metrics.Requests
	received, sent atomic.Int64 // body bytes
}

type disk struct {
	id       db.UUID
	dir      string
	inflight atomic.Int64 // bytes of PUTs in progress
	failed   atomic.Bool  // failed its last probe
	// scrub and cleanup, for Metrics
	blocks, missing, orphans, removed atomic.Int64
	scrubbed, cleaned                 atomic.Int64 // unix nanos of the last full pass
}

func New(cfg Config) (*Store, error) {
	if cfg.ScrubPass < 0 {
		return nil, fmt.Errorf("scrubPass %v: want >= 0", cfg.ScrubPass)
	}
	if cfg.ScrubGap < 0 {
		return nil, fmt.Errorf("scrubGap %v: want >= 0", cfg.ScrubGap)
	}
	if cfg.ScrubPass == 0 {
		cfg.ScrubPass = DefaultScrubPass
	}
	stores, err := cfg.DB.Stores(context.Background())
	if err != nil {
		return nil, err
	}
	known := map[db.UUID]bool{}
	for _, st := range stores {
		known[st.ID] = true
	}
	s := &Store{Config: cfg, disks: map[db.UUID]*disk{}, mux: http.NewServeMux()}
	for _, dir := range cfg.Dirs {
		id, err := storeID(dir)
		if err != nil {
			return nil, err
		}
		// cleanup would delete every shard on it
		if !known[id] && hasShards(dir) {
			log.Printf("skipping disk %s: store %s has shards but no stores row (wrong -keyspace or -cassandra, or a decommissioned disk), empty it to reuse it", dir, id)
			continue
		}
		d := &disk{id: id, dir: dir}
		s.disks[id] = d
		s.list = append(s.list, d)
	}
	if len(s.list) == 0 {
		return nil, errors.New("no usable disks")
	}
	s.mux.HandleFunc("PUT /v1/{store}/{file}/{shard}", s.put)
	s.mux.HandleFunc("GET /v1/{store}/{file}/{shard}", s.get)
	return s, nil
}

func (s *Store) IDs() []db.UUID {
	var ids []db.UUID
	for _, d := range s.list {
		ids = append(ids, d.id)
	}
	return ids
}

// storeID reads the disk's ID, making one on first start.
func storeID(dir string) (db.UUID, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return db.UUID{}, err
	}
	path := filepath.Join(dir, "store_id")
	b, err := os.ReadFile(path)
	if err == nil {
		return db.ParseID(strings.TrimSpace(string(b)))
	}
	if !errors.Is(err, fs.ErrNotExist) {
		return db.UUID{}, err
	}
	id, err := db.NewID()
	if err != nil {
		return id, err
	}
	return id, writeSync(path, []byte(id.String()+"\n"))
}

// hasShards reports whether dir holds any prefix dir.
func hasShards(dir string) bool {
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if _, ok := hexByte(e.Name()); ok && e.IsDir() {
			return true
		}
	}
	return false
}

func (d *disk) path(file db.UUID, shard int) string {
	id := file.String()
	return filepath.Join(d.dir, id[0:2], id[2:4], id+"."+strconv.Itoa(shard))
}

func (s *Store) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	st := &metrics.Status{ResponseWriter: w, Sent: &s.sent}
	defer func() { s.requests.Add(r.Method, st.Code) }()
	w = st
	if r.Method == http.MethodPut {
		r = metrics.CountBody(r, &s.received)
	}
	if s.Token != "" && subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+s.Token)) != 1 {
		http.Error(w, "bad token", http.StatusUnauthorized)
		return
	}
	s.mux.ServeHTTP(w, r)
}

func (s *Store) target(w http.ResponseWriter, r *http.Request) (*disk, string, bool) {
	sid, err1 := db.ParseID(r.PathValue("store"))
	fid, err2 := db.ParseID(r.PathValue("file"))
	k, err3 := strconv.Atoi(r.PathValue("shard"))
	if err1 != nil || err2 != nil || err3 != nil || k < 0 {
		http.Error(w, "bad shard path", http.StatusBadRequest)
		return nil, "", false
	}
	d := s.disks[sid]
	if d == nil {
		http.Error(w, "unknown store", http.StatusNotFound)
		return nil, "", false
	}
	if d.failed.Load() {
		http.Error(w, "disk failing its probe", http.StatusServiceUnavailable)
		return nil, "", false
	}
	return d, d.path(fid, k), true
}

// put answers before reading the body when it can't take the shard, so with
// Expect: 100-continue the serve hasn't sent any data yet.
func (s *Store) put(w http.ResponseWriter, r *http.Request) {
	d, path, ok := s.target(w, r)
	if !ok {
		return
	}
	size := r.ContentLength
	if size < 0 {
		http.Error(w, "Content-Length required", http.StatusLengthRequired)
		return
	}
	free, total, err := diskSpace(d.dir)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	reserve := total * s.MinFree / 100
	if r.URL.Query().Has("repair") {
		reserve = 0
	}
	inflight := d.inflight.Add(size)
	defer d.inflight.Add(-size)
	if free-reserve < inflight {
		http.Error(w, "disk full", http.StatusInsufficientStorage)
		return
	}
	r.Body.Read(nil) // checks passed: 100 Continue now, creating the file can be slow
	if err := writeShard(path, r.Body, size, http.NewResponseController(w)); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

func writeShard(path string, body io.Reader, size int64, rc *http.ResponseController) error {
	if size < 4 {
		return errors.New("shard too short")
	}
	f, err := createTemp(path)
	if err != nil {
		return err
	}
	tmp := f.Name()
	ok := false
	defer func() {
		f.Close()
		if !ok {
			os.Remove(tmp)
		}
	}()
	preallocate(f, int64(len(shard.Magic))+size)
	if _, err := f.WriteString(shard.Magic); err != nil {
		return err
	}
	var buf []byte
	if size >= erasure.BlockSize+4 {
		buf = blockPool.Get().([]byte)
		defer blockPool.Put(buf)
	} else {
		buf = make([]byte, size)
	}
	var deadlineAt time.Time
	for left := size; left > 0; {
		n := min(left, int64(len(buf)))
		if now := time.Now(); now.Sub(deadlineAt) >= time.Second {
			rc.SetReadDeadline(now.Add(shard.Stall))
			deadlineAt = now
		}
		if _, err := io.ReadFull(body, buf[:n]); err != nil {
			return err
		}
		if _, good := shard.Check(buf[:n]); !good {
			return errors.New("bad CRC")
		}
		if _, err := f.Write(buf[:n]); err != nil {
			return err
		}
		left -= n
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	ok = true
	return nil
}

func createTemp(path string) (*os.File, error) {
	dir, base := filepath.Dir(path), filepath.Base(path)+".tmp-*"
	f, err := os.CreateTemp(dir, base)
	if !errors.Is(err, fs.ErrNotExist) {
		return f, err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return os.CreateTemp(dir, base)
}

func create(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0o644)
	if !errors.Is(err, fs.ErrNotExist) {
		return f, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	return os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0o644)
}

func (s *Store) get(w http.ResponseWriter, r *http.Request) {
	_, path, ok := s.target(w, r)
	if !ok {
		return
	}
	f, err := os.Open(path)
	if errors.Is(err, fs.ErrNotExist) {
		http.NotFound(w, r)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer f.Close()
	sequential(f)
	st, err := f.Stat()
	var magic [len(shard.Magic)]byte
	if err == nil {
		_, err = io.ReadFull(f, magic[:])
	}
	if err != nil || string(magic[:]) != shard.Magic {
		http.Error(w, fmt.Sprintf("not a shard file: %v", err), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	hdr := int64(len(shard.Magic))
	http.ServeContent(w, r, "", time.Time{}, &deadlineReader{
		SectionReader: io.NewSectionReader(f, hdr, st.Size()-hdr),
		rc:            http.NewResponseController(w),
	})
}

type deadlineReader struct {
	*io.SectionReader
	rc   *http.ResponseController
	last time.Time
}

func (d *deadlineReader) Read(p []byte) (int, error) {
	if now := time.Now(); now.Sub(d.last) >= time.Second {
		d.rc.SetWriteDeadline(now.Add(shard.Stall))
		d.last = now
	}
	return d.SectionReader.Read(p)
}

func writeSync(path string, b []byte) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Write(b); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	return f.Close()
}

// Heartbeat beats every disk once.
func (s *Store) Heartbeat(ctx context.Context, addr string) {
	for _, d := range s.list {
		s.beat(ctx, d, addr)
	}
}

// beat probes disk d and updates its stores row. A disk failing the probe stops heartbeating.
func (s *Store) beat(ctx context.Context, d *disk, addr string) {
	free, err := s.probe(d)
	d.failed.Store(err != nil)
	if err != nil {
		log.Printf("disk %s (%s) failed its probe: %v", d.id, d.dir, err)
		return
	}
	if err := s.DB.Heartbeat(ctx, db.Store{ID: d.id, Addr: addr, Free: free, LastSeen: time.Now()}); err != nil {
		log.Printf("heartbeat %s: %v", d.id, err)
	}
}

// probe writes and fsyncs a small file and returns the bytes left for uploads.
func (s *Store) probe(d *disk) (int64, error) {
	if err := writeSync(filepath.Join(d.dir, "probe"), []byte(time.Now().String())); err != nil {
		return 0, err
	}
	free, total, err := diskSpace(d.dir)
	return max(0, free-total*s.MinFree/100-d.inflight.Load()), err
}

// CrashedSince is when shards may have been lost, if the machine rebooted after this store's
// last heartbeat: writes then were still in the page cache (30s writeback, a heartbeat a minute).
// Zero if it hasn't rebooted, a plain process restart keeps the page cache.
func (s *Store) CrashedSince(ctx context.Context) time.Time {
	stores, err := s.DB.Stores(ctx)
	if err != nil {
		log.Printf("crash check: %v", err)
		return time.Time{}
	}
	var last time.Time
	for _, st := range stores {
		if s.disks[st.ID] != nil && st.LastSeen.After(last) {
			last = st.LastSeen
		}
	}
	if boot, err := bootTime(); err != nil || last.IsZero() || boot.Before(last) {
		return time.Time{}
	}
	return last.Add(-2 * time.Minute)
}

// Run gives every disk its own loops, so a slow or stuck disk never holds up the others:
// a heartbeat every minute, paced scrub passes over ScrubPass with ScrubGap between them
// (resuming after a restart), and a cleanup spread over each day.
func (s *Store) Run(ctx context.Context, addr string) {
	since := s.CrashedSince(ctx) // before the first heartbeat replaces last_seen
	if !since.IsZero() {
		log.Printf("rebooted since the last heartbeat, checking shards written after %s", since.Format(time.RFC3339))
	}
	for _, d := range s.list {
		go func() {
			for ctx.Err() == nil {
				s.beat(ctx, d, addr)
				sleep(ctx, time.Minute)
			}
		}()
		go func() {
			if !since.IsZero() {
				if err := s.scrubDisk(ctx, d, pace{}, since); err != nil {
					log.Printf("crash check %s: %v", d.dir, err)
				}
			}
			s.scrubLoop(ctx, d)
		}()
		go func() {
			for ctx.Err() == nil {
				start := time.Now()
				if err := s.cleanDisk(ctx, d, 24*time.Hour); err != nil {
					log.Printf("cleanup %s: %v", d.dir, err)
				}
				sleep(ctx, time.Until(start.Add(24*time.Hour)))
			}
		}()
	}
	<-ctx.Done()
}

func sleep(ctx context.Context, d time.Duration) {
	if d <= 0 {
		return
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

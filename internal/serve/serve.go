package serve

import (
	"context"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"filescale/internal/db"
	"filescale/internal/metrics"
	"filescale/internal/shard"
)

const clientStall = 60 * time.Second

type Config struct {
	DB            db.DB
	Client        *shard.Client
	DataShards    int
	ParityShards  int
	SmallFileSize int64
	SlowRead      time.Duration
	Token         string
	TokenFor      string // "", "upload" or "download"
	Only          string // "", "upload" or "download"
	// AllowInline accepts ?response-content-disposition=inline (else 400): only behind a
	// front that already serves inline from its own domain, an uploaded html file renders.
	AllowInline bool
	NoRepair    bool
	// Alive returns the HTTP addresses of live stores (memberlist).
	Alive func() map[string]bool
	// Serves returns the live serves and this one's address, to split repair work.
	Serves func() (all []string, self string)
}

type storeView struct {
	list []db.Store
	byID map[db.UUID]string
}

type Serve struct {
	Config
	counters
	stores   atomic.Pointer[storeView]
	reloaded atomic.Int64 // unix nanos of the last good Reload
	rebuilt  atomic.Int64 // download stripes rebuilt from parity
}

func New(cfg Config) *Serve {
	s := &Serve{Config: cfg}
	s.stores.Store(&storeView{})
	return s
}

// Reload refreshes the stores table, Run does it every 30s.
func (s *Serve) Reload(ctx context.Context) error {
	stores, err := s.DB.Stores(ctx)
	if err != nil {
		return err
	}
	byID := make(map[db.UUID]string, len(stores))
	for _, st := range stores {
		byID[st.ID] = st.Addr
	}
	s.stores.Store(&storeView{list: stores, byID: byID})
	s.reloaded.Store(time.Now().UnixNano())
	return nil
}

func (s *Serve) Run(ctx context.Context, repairAfter time.Duration) {
	if s.reloaded.Load() == 0 {
		if err := s.Reload(ctx); err != nil {
			log.Printf("stores: %v", err)
		}
	}
	if !s.NoRepair {
		// own goroutine: a long repair must not stall the reload, pick skips stores not seen for 3 min
		go func() {
			repair := time.NewTicker(time.Hour)
			defer repair.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-repair.C:
					if err := s.Repair(ctx, repairAfter); err != nil {
						log.Printf("repair: %v", err)
					}
				}
			}
		}()
	}
	reload := time.NewTicker(30 * time.Second)
	defer reload.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-reload.C:
			if err := s.Reload(ctx); err != nil {
				log.Printf("stores: %v", err)
			}
		}
	}
}

var (
	kinds = map[string]string{"PUT": "upload", "DELETE": "upload", "GET": "download", "HEAD": "download"}
	allow = map[string]string{"": "GET, HEAD, PUT, DELETE", "upload": "PUT, DELETE", "download": "GET, HEAD"}
)

func (s *Serve) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// health check, no token or -only: an empty name is never a file
	if r.URL.Path == "/" && (r.Method == http.MethodGet || r.Method == http.MethodHead) {
		if time.Since(time.Unix(0, s.reloaded.Load())) > 2*time.Minute { // reloads every 30s
			http.Error(w, "no Cassandra for 2 minutes", http.StatusServiceUnavailable)
			return
		}
		fmt.Fprintln(w, "ok")
		return
	}
	st := &metrics.Status{ResponseWriter: w, Sent: &s.sent}
	defer func() { s.requests.Add(r.Method, st.Code) }()
	w = st
	if r.Method == http.MethodPut {
		r = metrics.CountBody(r, &s.received)
	}
	q := r.URL.Query()
	kind := kinds[r.Method]
	if r.Method == http.MethodHead && q.Has("upload") {
		kind = "upload" // where to resume
	}
	if kind == "" || s.Only != "" && kind != s.Only {
		w.Header().Set("Allow", allow[s.Only])
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.Token != "" && (s.TokenFor == "" || s.TokenFor == kind) &&
		subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+s.Token)) != 1 {
		http.Error(w, "bad token", http.StatusUnauthorized)
		return
	}
	name := strings.TrimPrefix(r.URL.Path, "/")
	if name == "" || len(name) > 1024 || !utf8.ValidString(name) {
		http.Error(w, "bad name", http.StatusBadRequest)
		return
	}
	var upload db.UUID // zero: not resumable
	if q.Has("upload") {
		id, err := db.ParseID(q.Get("upload"))
		if err != nil || id == (db.UUID{}) || r.Method != http.MethodPut && r.Method != http.MethodHead {
			http.Error(w, "?upload takes a UUID, on PUT or HEAD", http.StatusBadRequest)
			return
		}
		upload = id
	}
	inflight := &s.downloading
	if kind == "upload" {
		inflight = &s.uploading
	}
	inflight.Add(1)
	defer inflight.Add(-1)
	switch {
	case r.Method == http.MethodPut:
		s.upload(w, r, name, upload)
	case r.Method == http.MethodDelete:
		s.delete(w, r, name)
	case q.Has("upload"):
		s.offset(w, r, name, upload)
	default:
		s.download(w, r, name)
	}
}

func (s *Serve) delete(w http.ResponseWriter, r *http.Request, name string) {
	f, err := s.DB.File(r.Context(), name)
	if err != nil {
		fail(w, err)
		return
	}
	if f == nil {
		http.NotFound(w, r)
		return
	}
	if err := s.DB.DeleteFile(r.Context(), name); err != nil {
		fail(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
	go s.unindex(context.WithoutCancel(r.Context()), f)
}

// unindex removes a file's shards_by_store rows, store cleanup then deletes its shard files.
// Not for parts the name still holds (a newer write lost to clock skew, or a resent last part
// sharing the rest), orphan checks catch the rest.
func (s *Serve) unindex(ctx context.Context, f *db.File) {
	if err := f.CheckLayout(); err != nil {
		log.Printf("unindex %s: corrupt metadata: %v", f.Name, err)
		return
	}
	cur, err := s.DB.File(ctx, f.Name)
	if err != nil {
		return
	}
	if cur != nil {
		if err := cur.CheckLayout(); err != nil {
			log.Printf("unindex %s: corrupt current metadata: %v", f.Name, err)
			return
		}
	}
	for i := range f.NumParts() {
		p := f.Part(i)
		if cur != nil && cur.PartOf(p.ID) >= 0 {
			continue
		}
		var wg sync.WaitGroup
		for _, st := range p.Stores {
			wg.Go(func() {
				if err := s.DB.DeleteShard(ctx, st, p.ID); err != nil {
					log.Printf("unindex %s on %s: %v", p.ID, st, err)
				}
			})
		}
		wg.Wait()
	}
}

// pick chooses n disks on n different servers: the preferred ones that qualify,
// the rest weighted random on free space (capped).
func (s *Serve) pick(n int, size int64, skip map[string]bool, prefer []db.UUID) ([]db.Store, error) {
	alive := s.Alive()
	up, roomy := map[string]bool{}, map[string]bool{}
	var room []db.Store
	for _, st := range s.stores.Load().list {
		if !usable(st, alive) || skip[st.Addr] {
			continue
		}
		up[st.Addr] = true
		if st.Free >= size {
			room = append(room, st)
			roomy[st.Addr] = true
		}
	}
	if len(up) < n {
		return nil, &statusError{http.StatusServiceUnavailable, fmt.Sprintf("need %d servers, %d up", n, len(up))}
	}
	if len(roomy) < n {
		return nil, &statusError{http.StatusInsufficientStorage, fmt.Sprintf("need %d servers with %d bytes free, %d have it", n, size, len(roomy))}
	}
	// weights capped at twice the average, or an empty new server is in nearly every upload
	var sum float64
	for _, st := range room {
		sum += float64(st.Free)
	}
	limit := 2 * sum / float64(len(room))
	var picked []db.Store
	used := map[string]bool{}
	for _, id := range prefer {
		for _, st := range room {
			if st.ID == id && !used[st.Addr] && len(picked) < n {
				picked = append(picked, st)
				used[st.Addr] = true
			}
		}
	}
	weight := func(st db.Store) float64 { return max(1, min(float64(st.Free), limit)) }
	for len(picked) < n {
		var total float64
		for _, st := range room {
			if !used[st.Addr] {
				total += weight(st)
			}
		}
		x := rand.Float64() * total
		for _, st := range room {
			if used[st.Addr] {
				continue
			}
			w := weight(st)
			if x < w {
				picked = append(picked, st)
				used[st.Addr] = true
				break
			}
			x -= w
		}
	}
	return picked, nil
}

// usable is a store uploads can pick: alive in memberlist, not in maintenance, seen in the last 3 minutes.
func usable(st db.Store, alive map[string]bool) bool {
	return st.State != "maintenance" && alive[st.Addr] && time.Since(st.LastSeen) <= 3*time.Minute
}

// addrs maps a file's shards to live store addresses, "" when offline.
func (s *Serve) addrs(f *db.File) []string {
	alive := s.Alive()
	out := s.tableAddrs(f)
	for i, a := range out {
		if !alive[a] {
			out[i] = ""
		}
	}
	return out
}

// tableAddrs maps a file's shards to their stores.addr, live or not.
func (s *Serve) tableAddrs(f *db.File) []string {
	byID := s.stores.Load().byID
	out := make([]string, len(f.Stores))
	for i, id := range f.Stores {
		out[i] = byID[id]
	}
	return out
}

type statusError struct {
	code int
	msg  string
}

func (e *statusError) Error() string { return e.msg }

func fail(w http.ResponseWriter, err error) {
	var se *statusError
	if errors.As(err, &se) {
		http.Error(w, se.msg, se.code)
		return
	}
	http.Error(w, err.Error(), http.StatusServiceUnavailable)
}

func etag(sum []byte) string { return `"` + hex.EncodeToString(sum) + `"` }

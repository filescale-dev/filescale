package serve

import (
	"bytes"
	"cmp"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"filescale/internal/db"
	"filescale/internal/shard"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

// TestRepairTime times rebuilding a dead store's shards, once at 1 serve and once
// at N: a dead disk stays exposed for -repairAfter plus this. Uploads first;
// the stores are remote.
//
//	FILESCALE_REPAIR=1 FILESCALE_LOAD_REMOTE=<addr|id,...> FILESCALE_LOAD_CASSANDRA=<host> \
//	  go test -run TestRepairTime -v -timeout 60m ./internal/serve
func TestRepairTime(t *testing.T) {
	if os.Getenv("FILESCALE_REPAIR") == "" {
		t.Skip("set FILESCALE_REPAIR=1")
	}
	hosts := os.Getenv("FILESCALE_LOAD_CASSANDRA")
	list := os.Getenv("FILESCALE_LOAD_REMOTE")
	token := os.Getenv("FILESCALE_LOAD_TOKEN")
	files, err := strconv.Atoi(cmp.Or(os.Getenv("FILESCALE_REPAIR_FILES"), "200"))
	if err != nil || hosts == "" || list == "" {
		t.Fatal("need FILESCALE_LOAD_REMOTE, FILESCALE_LOAD_CASSANDRA and FILESCALE_REPAIR_FILES")
	}
	size, _ := strconv.Atoi(cmp.Or(os.Getenv("FILESCALE_REPAIR_SIZE"), strconv.Itoa(4<<20)))
	parity, _ := strconv.Atoi(cmp.Or(os.Getenv("FILESCALE_LOAD_PARITY"), "4"))
	serves := []int{1, 2, 4}
	if s := os.Getenv("FILESCALE_REPAIR_SERVES"); s != "" {
		serves = nil
		for _, f := range strings.Split(s, ",") {
			n, _ := strconv.Atoi(f)
			serves = append(serves, n)
		}
	}
	ctx := context.Background()
	run := time.Now().UnixNano() / 1e9
	var rows []db.Store
	all := map[string]bool{}
	for _, e := range strings.Split(list, ",") {
		addr, id, _ := strings.Cut(e, "|")
		uid, err := db.ParseID(id)
		if err != nil {
			t.Fatal(err)
		}
		all[addr] = true
		rows = append(rows, db.Store{ID: uid, Addr: addr, Free: 1 << 50})
	}
	dead := rows[0]
	data := random(size)
	t.Logf("%d files of %d bytes on %d stores, store %s dies", files, size, len(rows), dead.Addr)
	t.Logf("%5s %8s %8s %9s %9s | %s", "serve", "shards", "time", "shard/s", "file/s", "cassandra p50/p99")
	for round, n := range serves {
		// a fresh keyspace per round: nothing of an earlier run's store ids is left in it
		ks := fmt.Sprintf("fsrepair_%d_%d", run, round)
		dropKeyspace(t, hosts, ks)
		t.Cleanup(func() { dropKeyspace(t, hosts, ks) })
		c, err := db.Connect([]string{hosts}, ks, 1)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(c.Close)
		td := &repairDB{DB: c, took: map[string][]time.Duration{}}
		alive := map[string]bool{}
		cli := shard.NewClient(token)
		cli.HTTP.Transport.(*http.Transport).MaxIdleConnsPerHost = 4096
		serves := func(n int) []*Serve {
			names := make([]string, n)
			for i := range names {
				names[i] = fmt.Sprintf("serve%d", i)
			}
			sv := make([]*Serve, n)
			for i := range sv {
				sv[i] = New(Config{DB: td, Client: cli, DataShards: 8, ParityShards: parity, SmallFileSize: 32 << 10, SlowRead: 500 * time.Millisecond,
					Alive: func() map[string]bool { return alive }, Serves: func() ([]string, string) { return names, names[i] }})
			}
			return sv
		}
		up := New(Config{DB: td, Client: cli, DataShards: 8, ParityShards: parity, SmallFileSize: 32 << 10, SlowRead: 500 * time.Millisecond,
			Alive: func() map[string]bool { return alive }, Serves: func() ([]string, string) { return []string{"up"}, "up" }})
		beat := func() {
			for _, r := range rows {
				r.LastSeen = time.Now()
				td.Heartbeat(ctx, r)
			}
		}
		for a := range all {
			alive[a] = true
		}
		beat()
		if err := up.Reload(ctx); err != nil {
			t.Fatal(err)
		}
		stop := make(chan struct{})
		done := make(chan struct{})
		go func() {
			defer close(done)
			for {
				select {
				case <-stop:
					return
				case <-time.After(30 * time.Second):
					beat()
					up.Reload(ctx) // Run reloads alongside its repair goroutine, pick drops stores unseen for 3 minutes
				}
			}
		}()
		srv := httptest.NewServer(up)
		t.Cleanup(srv.Close)
		put := func(i int) {
			name := fmt.Sprintf("repair/%d-%d", round, i)
			req, _ := http.NewRequest("PUT", srv.URL+"/"+name, bytes.NewReader(data))
			req.ContentLength = int64(len(data))
			resp, err := srv.Client().Do(req)
			if err != nil {
				t.Error(err)
				return
			}
			b, _ := io.ReadAll(io.LimitReader(resp.Body, 200))
			resp.Body.Close()
			if resp.StatusCode != http.StatusCreated {
				t.Errorf("%s: %s %s", name, resp.Status, bytes.TrimSpace(b))
			}
		}
		start := time.Now()
		var wg sync.WaitGroup
		puts := 16 // FILESCALE_REPAIR_PUTS: fewer, a big upload over a slow link bursts the stores past their 100 Continue wait
		if v := os.Getenv("FILESCALE_REPAIR_PUTS"); v != "" {
			puts, _ = strconv.Atoi(v)
		}
		for i := 0; i < files; i += puts {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := i; j < min(i+puts, files); j++ {
					put(j)
				}
			}()
		}
		wg.Wait()
		if t.Failed() {
			return
		}
		t.Logf("%d files uploaded in %v", files, time.Since(start).Round(time.Millisecond))

		// the dead store goes down, its rows age past -repairAfter
		close(stop)
		<-done
		delete(alive, dead.Addr)
		dead.LastSeen = time.Now().Add(-2 * time.Hour)
		td.Heartbeat(ctx, dead)
		dead.LastSeen = time.Time{} // Repair's Reload reads it back from Cassandra
		lost, buckets := 0, map[int]bool{}
		for i := range files {
			f, err := td.File(ctx, fmt.Sprintf("repair/%d-%d", round, i))
			if err != nil || f == nil {
				t.Fatalf("file %d: %v", i, err)
			}
			for _, id := range f.Stores {
				if id == dead.ID {
					lost++
					buckets[db.Bucket(f.ID)] = true
				}
			}
		}
		if lost == 0 {
			t.Fatal("dead store holds no shards")
		}
		sv := serves(n)
		// Run reloads its stores every 30s alongside the repair goroutine, pick drops stores unseen for 3 minutes
		refresh := make(chan struct{})
		var rg sync.WaitGroup
		for i := range sv {
			rg.Add(1)
			go func() {
				defer rg.Done()
				for {
					select {
					case <-refresh:
						return
					case <-time.After(30 * time.Second):
						for _, r := range rows {
							if r.ID == dead.ID {
								continue // it must stay down
							}
							r.LastSeen = time.Now()
							td.Heartbeat(ctx, r)
						}
						sv[i].Reload(ctx)
					}
				}
			}()
		}
		defer func() { close(refresh); rg.Wait() }()
		count := &lineCount{w: os.Stderr, match: "repaired "}
		log.SetOutput(count)
		td.clear()
		start = time.Now()
		for i := range sv {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := sv[i].Repair(ctx, time.Hour); err != nil && ctx.Err() == nil {
					t.Error(err)
				}
			}()
		}
		wg.Wait()
		took := time.Since(start)
		log.SetOutput(os.Stderr)
		if got := int(count.n.Load()); got != lost {
			t.Errorf("%d shards repaired, want %d", got, lost)
		}
		left := 0
		for b := range buckets {
			r, err := td.Shards(ctx, dead.ID, b)
			if err != nil {
				t.Fatal(err)
			}
			left += len(r)
		}
		if left != 0 {
			t.Errorf("%d rows left on the dead store", left)
		}
		t.Logf("%5d %8d %8v %9.2f %9.2f | %s", n, lost, took.Round(time.Millisecond),
			float64(lost)/took.Seconds(), float64(files)/took.Seconds(), td.report())
	}
}

// dropKeyspace removes a keyspace of an earlier run, its store ids are long gone.
func dropKeyspace(t *testing.T, host, ks string) {
	c := gocql.NewCluster(host)
	s, err := c.CreateSession()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Query("DROP KEYSPACE IF EXISTS " + ks).Exec(); err != nil {
		t.Fatal(err)
	}
}

// lineCount passes log lines through and counts the ones a prefix appears in.
type lineCount struct {
	w     io.Writer
	match string
	n     atomic.Int64
	mu    sync.Mutex
}

func (l *lineCount) Write(p []byte) (int, error) {
	l.mu.Lock()
	if strings.Contains(string(p), l.match) {
		l.n.Add(1)
	}
	l.mu.Unlock()
	return l.w.Write(p)
}

// repairDB times the metadata calls a repair makes.
type repairDB struct {
	db.DB
	mu   sync.Mutex
	took map[string][]time.Duration
	scan time.Duration // spent in the bucket queries of the pass
	rows int           // shard rows they returned
}

func (r *repairDB) time(op string, start time.Time) {
	r.mu.Lock()
	r.took[op] = append(r.took[op], time.Since(start))
	r.mu.Unlock()
}

func (r *repairDB) clear() {
	r.mu.Lock()
	clear(r.took)
	r.scan, r.rows = 0, 0
	r.mu.Unlock()
}

func (r *repairDB) File(ctx context.Context, name string) (*db.File, error) {
	defer r.time("File", time.Now())
	return r.DB.File(ctx, name)
}

func (r *repairDB) PutShard(ctx context.Context, s db.Shard) error {
	defer r.time("PutShard", time.Now())
	return r.DB.PutShard(ctx, s)
}

func (r *repairDB) SetShardStore(ctx context.Context, name string, id db.UUID, shard int, store db.UUID) (bool, error) {
	defer r.time("SetShardStore", time.Now())
	return r.DB.SetShardStore(ctx, name, id, shard, store)
}

func (r *repairDB) Shards(ctx context.Context, store db.UUID, bucket int) ([]db.Shard, error) {
	start := time.Now()
	rows, err := r.DB.Shards(ctx, store, bucket)
	took := time.Since(start) // before the lock: the empty workers contend on it
	r.mu.Lock()
	r.took["Shards"] = append(r.took["Shards"], took)
	r.scan += took
	r.rows += len(rows)
	r.mu.Unlock()
	return rows, err
}

func (r *repairDB) DeleteShard(ctx context.Context, store, file db.UUID) error {
	defer r.time("DeleteShard", time.Now())
	return r.DB.DeleteShard(ctx, store, file)
}

// report gives each call's p50 and p99 since the last clear.
func (r *repairDB) report() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	ms := func(d time.Duration) string { return strconv.FormatFloat(float64(d.Microseconds())/1000, 'f', 2, 64) }
	var out []string
	for _, op := range []string{"Shards", "File", "PutShard", "SetShardStore", "DeleteShard"} {
		d := r.took[op]
		if len(d) == 0 {
			continue
		}
		slices.Sort(d)
		out = append(out, fmt.Sprintf("%s %d: %s/%s", op, len(d), ms(d[len(d)/2]), ms(d[min(len(d)-1, len(d)*99/100)])))
	}
	return fmt.Sprintf("scan %v rows %d | %s", r.scan.Round(time.Millisecond), r.rows, strings.Join(out, " "))
}

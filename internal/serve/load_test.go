package serve

import (
	"bufio"
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"io/fs"
	"maps"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/debug"
	rmetrics "runtime/metrics"
	rpprof "runtime/pprof"
	"runtime/trace"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"filescale/internal/db"
	"filescale/internal/metrics"
	"filescale/internal/shard"
	"filescale/internal/store"
)

// TestLoad records serve and store CPU and memory for 1, 10, 100 and 1000
// uploads at once, then as many downloads. The serve and each of the 12
// stores run as their own process, so the client isn't counted.
//
//	FILESCALE_LOAD=1 go test -run TestLoad -v -timeout 60m ./internal/serve
func TestLoad(t *testing.T) {
	if os.Getenv("FILESCALE_LOAD") == "" {
		t.Skip("set FILESCALE_LOAD=1, optionally FILESCALE_LOAD_SIZE=<bytes>")
	}
	size := 4 << 20
	if s := os.Getenv("FILESCALE_LOAD_SIZE"); s != "" {
		size, _ = strconv.Atoi(s)
	}
	levels := []int{1, 10, 100, 1000}
	if s := os.Getenv("FILESCALE_LOAD_LEVELS"); s != "" {
		levels = nil
		for _, f := range strings.Split(s, ",") {
			n, _ := strconv.Atoi(f)
			levels = append(levels, n)
		}
	}
	dir := t.TempDir()
	logs, err := os.MkdirTemp("", "filescale-load-logs")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("process logs in %s", logs)
	bases := []string{dir} // FILESCALE_LOAD_DIRS=/mnt/a,/mnt/b: stores take turns on these disks
	if s := os.Getenv("FILESCALE_LOAD_DIRS"); s != "" {
		bases = nil
		for _, b := range strings.Split(s, ",") {
			d, err := os.MkdirTemp(b, "filescale-load-")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { os.RemoveAll(d) })
			bases = append(bases, d)
		}
	}
	var stores []*proc
	var list []string // addr|store_id per disk
	if s := os.Getenv("FILESCALE_LOAD_REMOTE"); s != "" {
		// stores already running on other machines (TestLoadHelper with FILESCALE_LOAD_BIND), one entry per disk
		seen := map[string]bool{}
		for _, e := range strings.Split(s, ",") {
			list = append(list, e)
			addr, _, _ := strings.Cut(e, "|")
			if !seen[addr] {
				seen[addr] = true
				stores = append(stores, &proc{addr: addr})
			}
		}
	} else {
		for i := range 12 {
			name := fmt.Sprintf("store%d", i)
			p := startProc(t, logs, name, "store", "FILESCALE_LOAD_DIR="+filepath.Join(bases[i%len(bases)], name))
			stores = append(stores, p)
			for _, id := range strings.Split(p.id, ",") {
				list = append(list, p.addr+"|"+id)
			}
		}
	}
	cpuDir := ""
	if os.Getenv("FILESCALE_LOAD_PROFILE") != "" {
		cpuDir = logs
	}
	env := []string{"FILESCALE_LOAD_STORES=" + strings.Join(list, ","), "FILESCALE_LOAD_CPU=" + cpuDir}
	if g := os.Getenv("FILESCALE_LOAD_GOGC"); g != "" { // only the serve, not the client
		env = append(env, "GOGC="+g)
	}
	sv := startProc(t, logs, "serve", "serve", env...)
	// Windows desktop caps a listen backlog at 200 (Linux: 4096), retry like a real client would
	dial := func(ctx context.Context, network, addr string) (net.Conn, error) {
		for i := 1; ; i++ {
			c, err := (&net.Dialer{}).DialContext(ctx, network, addr)
			if err == nil || i == 20 || ctx.Err() != nil {
				return c, err
			}
			time.Sleep(time.Duration(i) * 50 * time.Millisecond)
		}
	}
	client := &http.Client{Transport: &http.Transport{DialContext: dial, MaxIdleConnsPerHost: 4096}}
	data := random(size)
	want := crc32.ChecksumIEEE(data)

	t.Logf("%.1f MiB files, %d CPUs shared by the serve, %d stores and the client (stores may be remote), metadata in memory (no Cassandra)", float64(size)/(1<<20), runtime.NumCPU(), len(stores))
	t.Logf("%5s %-5s %8s %8s | %-23s | %-44s | %-18s | %s", "", "", "", "", "latency per op", "serve", "stores", "")
	t.Logf("%5s %-5s %8s %8s | %7s %7s %7s | %7s %6s %5s %8s %8s %6s | %7s %10s | %s",
		"n", "op", "time", "MB/s", "p50", "p99", "max", "CPU", "ms/op", "cores", "peak", "live", "/op", "CPU", "peak", "failed")
	for _, n := range levels {
		for _, op := range []string{"write", "read"} {
			if op == "read" && os.Getenv("FILESCALE_LOAD_COLD") != "" {
				for _, p := range stores { // reads come from the disks, not the page cache
					p.get(t, "/__evict")
				}
			}
			cassandra := os.Getenv("FILESCALE_LOAD_CASSANDRA") != ""
			if cassandra {
				sv.get(t, "/__db").Body.Close() // starts its timings over
			}
			all := append([]*proc{sv}, stores...)
			before := make([]stats, len(all))
			for i, p := range all {
				before[i] = p.stats(t, fmt.Sprintf("%d-%s", n, op))
			}
			start := time.Now()
			failed, err, lat := load(client, sv.addr, n, op, data, want)
			wall := time.Since(start)
			slices.Sort(lat)
			pct := func(p float64) time.Duration {
				return lat[min(len(lat)-1, int(p*float64(len(lat))))].Round(time.Millisecond)
			}
			var st [2]stats // serve, stores summed
			for i, p := range all {
				s := p.stats(t, "")
				j := min(i, 1)
				st[j].CPU += s.CPU - before[i].CPU
				st[j].Peak += s.Peak
				st[j].Live += s.Live
			}
			perOp := (st[0].Peak - before[0].Mem) / int64(n)
			t.Logf("%5d %-5s %8v %8.0f | %7v %7v %7v | %6.2fs %6.1f %5.2f %5.0f MiB %5.0f MiB %3.0f KiB | %6.2fs %6.0f MiB | %d",
				n, op, wall.Round(time.Millisecond), float64(n*size)/wall.Seconds()/1e6, pct(0.5), pct(0.99), lat[len(lat)-1].Round(time.Millisecond),
				st[0].CPU.Seconds(), float64(st[0].CPU.Milliseconds())/float64(n), st[0].CPU.Seconds()/wall.Seconds(),
				mib(st[0].Peak), mib(st[0].Live), float64(perOp)/1024,
				st[1].CPU.Seconds(), mib(st[1].Peak), failed)
			if err != nil {
				t.Logf("      first failure: %v", err)
			}
			if cassandra {
				resp := sv.get(t, "/__db")
				b, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				t.Logf("      cassandra: %s", b)
			}
			if os.Getenv("FILESCALE_LOAD_PROFILE") != "" {
				saveProfile(t, sv, filepath.Join(logs, fmt.Sprintf("serve-%d-%s.pprof", n, op)))
			}
		}
	}
	if os.Getenv("FILESCALE_LOAD_METRICS") != "" { // the serve's /metrics after all phases
		resp := sv.get(t, "/metrics")
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		for _, line := range strings.Split(string(b), "\n") {
			if line != "" && !strings.HasPrefix(line, "#") {
				t.Logf("      %s", line)
			}
		}
	}
}

func mib(b int64) float64 { return float64(b) / (1 << 20) }

// evictCache drops only the test's shard files from the page cache (FADV_DONTNEED), the
// rest of the machine's cache stays.
func evictCache(dirs []string) {
	for _, d := range dirs {
		filepath.WalkDir(d, func(p string, e fs.DirEntry, err error) error {
			if err == nil && e.Type().IsRegular() {
				if f, err := os.Open(p); err == nil {
					unix.Fadvise(int(f.Fd()), 0, 0, unix.FADV_DONTNEED)
					f.Close()
				}
			}
			return nil
		})
	}
}

// saveProfile saves a process's allocation profile so far, for go tool pprof -sample_index=alloc_space.
func saveProfile(t *testing.T, p *proc, path string) {
	resp := p.get(t, "/debug/pprof/allocs")
	defer resp.Body.Close()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	io.Copy(f, resp.Body)
	t.Logf("      profile: %s", path)
}

// load runs n uploads or downloads at once, counts the failures and times each.
func load(c *http.Client, addr string, n int, op string, data []byte, want uint32) (int, error, []time.Duration) {
	var wg sync.WaitGroup
	var mu sync.Mutex
	failed, first := 0, error(nil)
	lat := make([]time.Duration, n)
	for i := range n {
		wg.Go(func() {
			start := time.Now()
			err := loadOne(c, fmt.Sprintf("http://%s/load/%d/%d", addr, n, i), op, data, want)
			lat[i] = time.Since(start)
			if err != nil {
				mu.Lock()
				failed++
				if first == nil {
					first = fmt.Errorf("after %v: %w", lat[i].Round(time.Millisecond), err)
				}
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	return failed, first, lat
}

func loadOne(c *http.Client, url, op string, data []byte, want uint32) error {
	method, body := http.MethodGet, io.Reader(nil)
	if op == "write" {
		method, body = http.MethodPut, bytes.NewReader(data)
	}
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return err
	}
	if token := os.Getenv("FILESCALE_LOAD_TOKEN"); token != "" { // the helpers' gate, the serve itself is open
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if op == "write" && resp.StatusCode != http.StatusCreated || op == "read" && resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 300))
		return fmt.Errorf("%s %s: %s %s", method, url, resp.Status, bytes.TrimSpace(msg))
	}
	h := crc32.NewIEEE()
	n, err := io.Copy(h, resp.Body)
	switch {
	case err != nil:
		return err
	case op == "read" && (n != int64(len(data)) || h.Sum32() != want):
		return fmt.Errorf("GET %s: %d bytes, wrong data", url, n)
	}
	return nil
}

type stats struct {
	CPU  time.Duration // process CPU time, user + system
	Mem  int64         // Go memory not returned to the OS, about the RSS
	Peak int64         // highest Mem since the last reset
	Live int64         // highest live heap after a GC since the last reset
}

type proc struct{ addr, id string }

// stats reads a process's counters, resetting them first for a new phase when phase isn't "".
func (p *proc) stats(t *testing.T, phase string) stats {
	path := "/__stats"
	if phase != "" {
		path += "?reset=" + phase
	}
	resp := p.get(t, path)
	defer resp.Body.Close()
	var s stats
	if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
		t.Fatal(err)
	}
	return s
}

// get calls a helper's own endpoint, with FILESCALE_LOAD_TOKEN when set.
func (p *proc) get(t *testing.T, path string) *http.Response {
	req, _ := http.NewRequest(http.MethodGet, "http://"+p.addr+path, nil)
	if token := os.Getenv("FILESCALE_LOAD_TOKEN"); token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("%s%s: %s", p.addr, path, resp.Status)
	}
	return resp
}

// startProc runs this test binary as a store or serve, see TestLoadHelper.
func startProc(t *testing.T, dir, name, role string, env ...string) *proc {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestLoadHelper$")
	cmd.Env = append(append(os.Environ(), env...), "FILESCALE_LOAD_HELPER="+role)
	logf, err := os.Create(filepath.Join(dir, name+".log"))
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = logf
	stdin, _ := cmd.StdinPipe()
	stdout, _ := cmd.StdoutPipe()
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		stdin.Close() // the helper exits when its stdin closes
		cmd.Wait()
		logf.Close()
	})
	sc := bufio.NewScanner(stdout)
	for sc.Scan() {
		if f := strings.Fields(sc.Text()); len(f) == 3 && f[0] == "READY" {
			go io.Copy(io.Discard, stdout)
			return &proc{addr: f[1], id: f[2]}
		}
	}
	t.Fatalf("%s didn't start, see %s", name, logf.Name())
	return nil
}

// TestLoadHelper is a store or serve process for TestLoad.
func TestLoadHelper(t *testing.T) {
	role := os.Getenv("FILESCALE_LOAD_HELPER")
	if role == "" {
		t.Skip("run by TestLoad")
	}
	// across machines: FILESCALE_LOAD_BIND=<public ip>:0, a FILESCALE_LOAD_TOKEN, FILESCALE_LOAD_STAY=1
	token := os.Getenv("FILESCALE_LOAD_TOKEN")
	dirs := strings.Split(os.Getenv("FILESCALE_LOAD_DIR"), ",") // a store gets one disk per dir
	var h http.Handler
	id := "-"
	mux := http.NewServeMux()
	switch role {
	case "store":
		// a restarted helper keeps its disks: give them the stores rows the serve's metadata has
		m := db.NewMem()
		for _, d := range dirs {
			if b, err := os.ReadFile(filepath.Join(d, "store_id")); err == nil {
				if sid, err := db.ParseID(strings.TrimSpace(string(b))); err == nil {
					m.Heartbeat(context.Background(), db.Store{ID: sid, LastSeen: time.Now()})
				}
			}
		}
		st, err := store.New(store.Config{Dirs: dirs, Token: token, DB: m, Client: shard.NewClient(token)})
		if err != nil {
			t.Fatal(err)
		}
		var ids []string
		for _, i := range st.IDs() {
			ids = append(ids, i.String())
		}
		h, id = st, strings.Join(ids, ",")
		mux.Handle("/metrics", metrics.Handler(st.Metrics))
	case "serve":
		sv := loadServe(t, mux, os.Getenv("FILESCALE_LOAD_STORES"), token)
		h = sv
		mux.Handle("/metrics", metrics.Handler(sv.Metrics))
	}
	bind := os.Getenv("FILESCALE_LOAD_BIND")
	if bind == "" {
		bind = "127.0.0.1:0"
	}
	ln, err := shard.Listen(bind)
	if err != nil {
		t.Fatal(err)
	}
	var peak, live atomic.Int64
	reset := func() {
		runtime.GC()
		debug.FreeOSMemory()
		m, l := memNow()
		peak.Store(m)
		live.Store(l)
	}
	reset()
	go func() {
		for {
			m, l := memNow()
			if m > peak.Load() {
				peak.Store(m)
			}
			if l > live.Load() {
				live.Store(l)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()
	var cpu, tr *os.File // CPU profile and execution trace of the current phase
	mux.HandleFunc("/__stats", func(w http.ResponseWriter, r *http.Request) {
		if cpu != nil {
			rpprof.StopCPUProfile()
			cpu.Close()
			cpu = nil
		}
		if tr != nil {
			trace.Stop()
			tr.Close()
			tr = nil
		}
		if phase := r.URL.Query().Get("reset"); phase != "" {
			reset()
			if dir := os.Getenv("FILESCALE_LOAD_CPU"); dir != "" {
				if f, err := os.Create(filepath.Join(dir, "cpu-"+phase+".pprof")); err == nil && rpprof.StartCPUProfile(f) == nil {
					cpu = f
				}
				// go tool trace -pprof=net|sync|syscall shows where the phase waited
				if f, err := os.Create(filepath.Join(dir, "trace-"+phase+".out")); err == nil && trace.Start(f) == nil {
					tr = f
				}
			}
		}
		m, _ := memNow()
		json.NewEncoder(w).Encode(stats{CPU: cpuTime(), Mem: m, Peak: peak.Load(), Live: live.Load()})
	})
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/__evict", func(w http.ResponseWriter, r *http.Request) { evictCache(dirs) })
	mux.Handle("/", h)
	var handler http.Handler = mux
	if token != "" { // a helper listening on a public address answers nothing without it
		handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Authorization") != "Bearer "+token {
				http.Error(w, "bad token", http.StatusUnauthorized)
				return
			}
			mux.ServeHTTP(w, r)
		})
	}
	srv := &http.Server{Handler: handler, ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 120 * time.Second}
	go srv.Serve(ln)
	fmt.Printf("READY %s %s\n", ln.Addr(), id)
	if os.Getenv("FILESCALE_LOAD_STAY") != "" {
		select {} // started by hand on another machine, stopped with kill
	}
	io.Copy(io.Discard, os.Stdin)
}

// loadServe is a serve over the given stores ("addr|store_id,..."), 8 + FILESCALE_LOAD_PARITY
// (default 4). Metadata in memory, or in FILESCALE_LOAD_CASSANDRA=host,... (keyspace
// FILESCALE_LOAD_KEYSPACE). Adds /__down?addrs=a,b: the stores memberlist would call dead,
// none = all alive, and /__db: Cassandra call latencies since the last /__db.
func loadServe(t *testing.T, mux *http.ServeMux, list, token string) *Serve {
	ctx := context.Background()
	var m db.DB = db.NewMem()
	if hosts := os.Getenv("FILESCALE_LOAD_CASSANDRA"); hosts != "" {
		c, err := db.Connect(strings.Split(hosts, ","), cmp.Or(os.Getenv("FILESCALE_LOAD_KEYSPACE"), "filescale_load"), 1)
		if err != nil {
			t.Fatal(err)
		}
		td := &timedDB{DB: c, took: map[string][]time.Duration{}}
		mux.HandleFunc("/__db", func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, td.report()) })
		m = td
	}
	all := map[string]bool{}
	var rows []db.Store
	for _, s := range strings.Split(list, ",") {
		addr, id, _ := strings.Cut(s, "|")
		uid, _ := db.ParseID(id)
		all[addr] = true
		rows = append(rows, db.Store{ID: uid, Addr: addr, Free: 1 << 50})
	}
	var alive atomic.Pointer[map[string]bool]
	alive.Store(&all)
	parity := 4
	if s := os.Getenv("FILESCALE_LOAD_PARITY"); s != "" {
		parity, _ = strconv.Atoi(s)
	}
	c := shard.NewClient(token)
	c.HTTP.Transport.(*http.Transport).MaxIdleConnsPerHost = 4096 // 1000 uploads hold 1000 connections per store
	sv := New(Config{DB: m, Client: c, DataShards: 8, ParityShards: parity, SmallFileSize: 32 << 10, SlowRead: 500 * time.Millisecond,
		Alive: func() map[string]bool { return *alive.Load() }, Serves: func() ([]string, string) { return []string{"me"}, "me" }})
	beat := func() {
		for _, r := range rows {
			r.LastSeen = time.Now()
			m.Heartbeat(ctx, r)
		}
		sv.Reload(ctx)
	}
	beat()
	go func() {
		for range time.Tick(30 * time.Second) {
			beat()
		}
	}()
	mux.HandleFunc("/__down", func(w http.ResponseWriter, r *http.Request) {
		m := maps.Clone(all)
		for _, a := range strings.Split(r.URL.Query().Get("addrs"), ",") {
			delete(m, a)
		}
		alive.Store(&m)
	})
	return sv
}

// timedDB times the metadata calls uploads and downloads make.
type timedDB struct {
	db.DB
	mu   sync.Mutex
	took map[string][]time.Duration
}

func (t *timedDB) time(op string, start time.Time) {
	d := time.Since(start)
	t.mu.Lock()
	t.took[op] = append(t.took[op], d)
	t.mu.Unlock()
}

func (t *timedDB) File(ctx context.Context, name string) (*db.File, error) {
	defer t.time("File", time.Now())
	return t.DB.File(ctx, name)
}

func (t *timedDB) PutFile(ctx context.Context, f *db.File) error {
	defer t.time("PutFile", time.Now())
	return t.DB.PutFile(ctx, f)
}

func (t *timedDB) PutShard(ctx context.Context, s db.Shard) error {
	defer t.time("PutShard", time.Now())
	return t.DB.PutShard(ctx, s)
}

func (t *timedDB) DeleteShard(ctx context.Context, store, file db.UUID) error {
	defer t.time("DeleteShard", time.Now())
	return t.DB.DeleteShard(ctx, store, file)
}

// report gives each call's count and latency since the last report, then starts over.
func (t *timedDB) report() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	ms := func(d time.Duration) string { return strconv.FormatFloat(float64(d.Microseconds())/1000, 'f', 2, 64) }
	var out []string
	for _, op := range slices.Sorted(maps.Keys(t.took)) {
		d := t.took[op]
		slices.Sort(d)
		out = append(out, fmt.Sprintf("%s %d: p50 %s p99 %s max %s ms", op, len(d), ms(d[len(d)/2]), ms(d[len(d)*99/100]), ms(d[len(d)-1])))
	}
	clear(t.took)
	return strings.Join(out, ", ")
}

func memNow() (mem, live int64) {
	s := []rmetrics.Sample{{Name: "/memory/classes/total:bytes"}, {Name: "/memory/classes/heap/released:bytes"}, {Name: "/gc/heap/live:bytes"}}
	rmetrics.Read(s)
	return int64(s[0].Value.Uint64() - s[1].Value.Uint64()), int64(s[2].Value.Uint64())
}

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"fornaxian.tech/zerodown"
	"github.com/hashicorp/memberlist"

	"filescale/internal/cluster"
	"filescale/internal/db"
	"filescale/internal/erasure"
	"filescale/internal/metrics"
	"filescale/internal/serve"
	"filescale/internal/shard"
	"filescale/internal/store"
)

type commonOpts struct {
	ip          string
	gossipPort  int
	peers       []string
	cassandra   []string
	keyspace    string
	replication int
	secret      string
	metricsPort int
	drainWait   time.Duration
}

type serveOpts struct {
	port          int
	dataShards    int
	parityShards  int
	smallFileSize int
	slowRead      time.Duration
	repairAfter   time.Duration
	token         string
	tokenFor      string
	only          string
	allowInline   bool
}

type storeOpts struct {
	port         int
	dirs         []string
	minFreeSpace int
}

func commonFlags(fs *flag.FlagSet) *commonOpts {
	o := &commonOpts{cassandra: []string{"127.0.0.1"}}
	fs.StringVar(&o.ip, "ip", "", "address to bind and advertise (default: all interfaces, advertise the first private IP)")
	fs.IntVar(&o.gossipPort, "gossipPort", 7946, "memberlist gossip port")
	listVar(fs, &o.peers, "peers", "comma-separated memberlist peers to join (host:port)")
	listVar(fs, &o.cassandra, "cassandra", "comma-separated Cassandra hosts (default 127.0.0.1)")
	fs.StringVar(&o.keyspace, "keyspace", "filescale", "Cassandra keyspace")
	fs.IntVar(&o.replication, "replication", 1, "Cassandra keyspace replication factor, only used when creating the keyspace (1 for a single node)")
	fs.StringVar(&o.secret, "secret", "", "shared cluster secret: encrypts gossip and authenticates store requests (empty = $FILESCALE_SECRET, else off)")
	fs.IntVar(&o.metricsPort, "metricsPort", 0, "port for Prometheus metrics at /metrics, 0 = off; no auth, keep it on the private network")
	fs.DurationVar(&o.drainWait, "drainWait", 30*time.Second, "how long a shutdown waits for requests in flight")
	return o
}

func serveFlags(fs *flag.FlagSet) *serveOpts {
	o := &serveOpts{}
	fs.IntVar(&o.port, "port", 8080, "HTTP port for client upload/download")
	fs.IntVar(&o.dataShards, "dataShards", 8, "data shards per stripe for new uploads (1 = replicate every file)")
	fs.IntVar(&o.parityShards, "parityShards", 4, "parity shards per stripe for new uploads; replicated files get parity+1 copies")
	fs.IntVar(&o.smallFileSize, "smallFileSize", 0, "files under this many bytes are replicated instead of erasure coded (default dataShards/parityShards × 512 KiB, 1 MiB for 8+4)")
	fs.DurationVar(&o.slowRead, "slowRead", 500*time.Millisecond, "read a shard from parity when a store takes longer than this for a block (~50ms for NVMe)")
	fs.DurationVar(&o.repairAfter, "repairAfter", 24*time.Hour, "rebuild a store's shards elsewhere once it has been down this long")
	fs.StringVar(&o.token, "token", "", "bearer token required on client requests (empty = $FILESCALE_TOKEN, else open)")
	kindVar(fs, &o.tokenFor, "tokenFor", `require -token only for "upload" (PUT, DELETE) or "download" (GET, HEAD) requests (default both)`)
	kindVar(fs, &o.only, "only", `serve only "upload" (PUT, DELETE) or "download" (GET, HEAD) requests (default both)`)
	fs.BoolVar(&o.allowInline, "allowInline", false, "accept ?response-content-disposition=inline (else only attachment): an uploaded html file then renders, use only behind a front that serves inline anyway")
	return o
}

func storeFlags(fs *flag.FlagSet) *storeOpts {
	o := &storeOpts{dirs: []string{"./data"}}
	fs.IntVar(&o.port, "port", 8081, "HTTP port for shard traffic")
	listVar(fs, &o.dirs, "dir", "comma-separated directories to keep shards in, one store_id each (default ./data)")
	fs.IntVar(&o.minFreeSpace, "minFreeSpace", 5, "percent of each disk kept free: below it new upload shards are refused, repairs may use it")
	return o
}

func listVar(fs *flag.FlagSet, p *[]string, name, usage string) {
	fs.Func(name, usage, func(s string) error {
		if s == "" {
			*p = nil
			return nil
		}
		*p = strings.Split(s, ",")
		return nil
	})
}

// kindVar defines a flag that takes "upload" or "download".
func kindVar(fs *flag.FlagSet, p *string, name, usage string) {
	fs.Func(name, usage, func(s string) error {
		if s != "upload" && s != "download" {
			return errors.New(`want "upload" or "download"`)
		}
		*p = s
		return nil
	})
}

// fromEnv fills an empty flag from an environment variable, so a deploy can keep the secret
// and token off the command line (ps, /proc/*/cmdline). Not flag defaults: -h prints those.
func fromEnv(p *string, name string) {
	if *p == "" {
		*p = os.Getenv(name)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `usage: filescale <command> [flags]

commands:
  serve    client-facing HTTP server for upload/download
  store    shard storage server

run "filescale <command> -h" for its flags
`)
	os.Exit(2)
}

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	fs := flag.NewFlagSet(os.Args[1], flag.ExitOnError)
	c := commonFlags(fs)
	switch os.Args[1] {
	case "serve":
		o := serveFlags(fs)
		fs.Parse(os.Args[2:])
		fromEnv(&c.secret, "FILESCALE_SECRET")
		fromEnv(&o.token, "FILESCALE_TOKEN")
		if c.drainWait < 0 {
			log.Fatalf("-drainWait %v: want >= 0", c.drainWait)
		}
		if _, err := erasure.New(o.dataShards, o.parityShards); err != nil {
			log.Fatalf("-dataShards %d -parityShards %d: %v", o.dataShards, o.parityShards, err)
		}
		if o.smallFileSize == 0 {
			o.smallFileSize = o.dataShards * (512 << 10) / max(o.parityShards, 1)
		}
		if supervise(c, o.port) {
			return
		}
		runServe(c, o)
	case "store":
		o := storeFlags(fs)
		fs.Parse(os.Args[2:])
		fromEnv(&c.secret, "FILESCALE_SECRET")
		if c.drainWait < 0 {
			log.Fatalf("-drainWait %v: want >= 0", c.drainWait)
		}
		if o.minFreeSpace < 0 || o.minFreeSpace > 100 {
			log.Fatalf("-minFreeSpace %d: want 0-100", o.minFreeSpace)
		}
		if supervise(c, o.port) {
			return
		}
		runStore(c, o)
	default:
		usage()
	}
}

func supervise(c *commonOpts, port int) bool {
	if zerodown.IsParent() {
		api, err := openFile(shard.Listen(net.JoinHostPort(c.ip, strconv.Itoa(port))))
		if err != nil {
			log.Fatal(err)
		}
		files := []*os.File{api}
		vars := []string{"FILESCALE_API_FD=3", "FILESCALE_METRICS_FD="}
		if c.metricsPort != 0 {
			m, err := openFile(net.Listen("tcp", net.JoinHostPort(c.ip, strconv.Itoa(c.metricsPort))))
			if err != nil {
				api.Close()
				log.Fatal(err)
			}
			files = append(files, m)
			vars[1] = "FILESCALE_METRICS_FD=4"
		}
		zerodown.ExtraFiles = files
		zerodown.ExtraVariables = vars
		defer func() {
			for _, f := range files {
				f.Close()
			}
		}()
	}
	return zerodown.Init()
}

func openFile(ln net.Listener, err error) (*os.File, error) {
	if err != nil {
		return nil, err
	}
	defer ln.Close()
	return ln.(*net.TCPListener).File()
}

func inheritedListener(env string) (net.Listener, error) {
	s := os.Getenv(env)
	if s == "" {
		return nil, nil
	}
	fd, err := strconv.Atoi(s)
	if err != nil {
		return nil, fmt.Errorf("%s=%q: %w", env, s, err)
	}
	f := os.NewFile(uintptr(fd), env)
	defer f.Close()
	return net.FileListener(f)
}

func apiListener(c *commonOpts, port int) (net.Listener, int) {
	ln, err := inheritedListener("FILESCALE_API_FD")
	if err != nil {
		log.Fatal(err)
	}
	if ln == nil {
		ln, err = shard.Listen(net.JoinHostPort(c.ip, strconv.Itoa(port)))
		if err != nil {
			log.Fatal(err)
		}
	}
	return ln, ln.Addr().(*net.TCPAddr).Port
}

func logCommon(cmd string, c *commonOpts) {
	shown := *c
	if shown.secret != "" {
		shown.secret = "set"
	}
	log.Printf("filescale %s: %+v", cmd, shown)
}

func runServe(c *commonOpts, o *serveOpts) {
	ln, port := apiListener(c, o.port)
	o.port = port
	logCommon("serve", c)
	shown := *o
	if shown.token == "" {
		log.Print("no -token set: client API is open")
	} else {
		shown.token = "set"
	}
	log.Printf("serve: %+v", shown)

	d := connect(c)
	defer d.Close()
	bg, stopBg := context.WithCancel(context.Background())
	client, rtt := newClient(c.secret)
	ml := join(bg, c, cluster.Meta{Role: "serve", Port: o.port, DataShards: o.dataShards, ParityShards: o.parityShards, SmallFileSize: int64(o.smallFileSize)}, rtt)
	sv := serve.New(serve.Config{
		DB: d, Client: client,
		DataShards: o.dataShards, ParityShards: o.parityShards, SmallFileSize: int64(o.smallFileSize),
		SlowRead: o.slowRead, Token: o.token, TokenFor: o.tokenFor, Only: o.only, AllowInline: o.allowInline,
		Alive:  func() map[string]bool { return cluster.AliveStoreAddrs(ml) },
		Serves: func() ([]string, string) { return cluster.Addrs(ml, "serve"), cluster.Addr(ml, o.port) },
	})
	if err := sv.Reload(bg); err != nil {
		log.Printf("stores: %v", err)
	}
	go sv.Run(bg, o.repairAfter)
	metricsSrv := serveMetrics(c, sv.Metrics)
	listen(c, ln, sv, ml, stopBg, metricsSrv)
}

func runStore(c *commonOpts, o *storeOpts) {
	ln, port := apiListener(c, o.port)
	o.port = port
	logCommon("store", c)
	log.Printf("store: %+v", *o)

	d := connect(c)
	defer d.Close()
	client, rtt := newClient(c.secret)
	st, err := store.New(store.Config{Dirs: o.dirs, MinFree: int64(o.minFreeSpace), Token: cluster.APIToken(c.secret),
		DB: d, Client: client})
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("store_ids: %v", st.IDs())
	bg, stopBg := context.WithCancel(context.Background())
	ml := join(bg, c, cluster.Meta{Role: "store", Port: o.port}, rtt)
	go st.Run(bg, cluster.Addr(ml, o.port))
	metricsSrv := serveMetrics(c, st.Metrics)
	listen(c, ln, st, ml, stopBg, metricsSrv)
}

func newClient(secret string) (*shard.Client, *cluster.RTT) {
	rtt := cluster.NewRTT()
	client := shard.NewClient(cluster.APIToken(secret))
	client.RTT = rtt.Of
	return client, rtt
}

func join(ctx context.Context, c *commonOpts, meta cluster.Meta, rtt *cluster.RTT) *memberlist.Memberlist {
	ml, err := cluster.New(c.ip, c.gossipPort, c.secret, meta, rtt)
	if err != nil {
		log.Fatal(err)
	}
	cluster.JoinOnce(ml, c.peers, 5*time.Second)
	go cluster.Join(ctx, ml, c.peers)
	return ml
}

func connect(c *commonOpts) *db.Cassandra {
	d, err := db.Connect(c.cassandra, c.keyspace, c.replication)
	if err != nil {
		log.Fatal(err)
	}
	return d
}

func listen(c *commonOpts, ln net.Listener, h http.Handler, ml *memberlist.Memberlist, stopBg context.CancelFunc, metricsSrv *http.Server) {
	srv := &http.Server{
		Handler:           h,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	go func() {
		if err := srv.Serve(ln); !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}()
	log.Printf("listening on %s, %d cluster members", ln.Addr(), ml.NumMembers())
	if zerodown.IsChild() {
		zerodown.StartupFinished()
	}
	<-ctx.Done()
	stop() // a second signal kills right away
	if stopBg != nil {
		stopBg()
	}
	if metricsSrv != nil {
		mctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = metricsSrv.Shutdown(mctx)
		cancel()
	}
	log.Printf("shutting down, waiting up to %v for requests in flight", c.drainWait)
	cluster.Leave(ml, 5*time.Second)
	_ = ml.Shutdown()
	dctx, cancel := context.WithTimeout(context.Background(), c.drainWait)
	defer cancel()
	if err := srv.Shutdown(dctx); err != nil {
		log.Printf("shutdown: %v", err)
	}
}

func serveMetrics(c *commonOpts, write func(*metrics.Writer)) *http.Server {
	if c.metricsPort == 0 {
		return nil
	}
	ln, err := inheritedListener("FILESCALE_METRICS_FD")
	if err != nil {
		log.Fatal(err)
	}
	if ln == nil {
		ln, err = net.Listen("tcp", net.JoinHostPort(c.ip, strconv.Itoa(c.metricsPort)))
		if err != nil {
			log.Fatal(err)
		}
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", metrics.Handler(write))
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		if err := srv.Serve(ln); !errors.Is(err, http.ErrServerClosed) {
			log.Printf("metrics: %v", err)
		}
	}()
	log.Printf("metrics on %s/metrics", ln.Addr())
	return srv
}

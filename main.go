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
		runServe(c, o)
	case "store":
		o := storeFlags(fs)
		fs.Parse(os.Args[2:])
		fromEnv(&c.secret, "FILESCALE_SECRET")
		runStore(c, o)
	default:
		usage()
	}
}

func logCommon(cmd string, c *commonOpts) {
	shown := *c
	if shown.secret != "" {
		shown.secret = "set"
	}
	log.Printf("filescale %s: %+v", cmd, shown)
}

func runServe(c *commonOpts, o *serveOpts) {
	if _, err := erasure.New(o.dataShards, o.parityShards); err != nil {
		log.Fatalf("-dataShards %d -parityShards %d: %v", o.dataShards, o.parityShards, err)
	}
	if o.smallFileSize == 0 {
		o.smallFileSize = o.dataShards * (512 << 10) / max(o.parityShards, 1)
	}
	logCommon("serve", c)
	shown := *o
	if shown.token == "" {
		log.Print("no -token set: client API is open")
	} else {
		shown.token = "set"
	}
	log.Printf("serve: %+v", shown)

	ml, client := join(c, cluster.Meta{Role: "serve", Port: o.port, DataShards: o.dataShards, ParityShards: o.parityShards, SmallFileSize: int64(o.smallFileSize)})
	d := connect(c)
	sv := serve.New(serve.Config{
		DB: d, Client: client,
		DataShards: o.dataShards, ParityShards: o.parityShards, SmallFileSize: int64(o.smallFileSize),
		SlowRead: o.slowRead, Token: o.token, TokenFor: o.tokenFor, Only: o.only, AllowInline: o.allowInline,
		Alive: func() map[string]bool {
			alive := map[string]bool{}
			for _, a := range cluster.Addrs(ml, "store") {
				alive[a] = true
			}
			return alive
		},
		Serves: func() ([]string, string) { return cluster.Addrs(ml, "serve"), cluster.Addr(ml, o.port) },
	})
	bg, stopBg := context.WithCancel(context.Background())
	go sv.Run(bg, o.repairAfter)
	serveMetrics(c, sv.Metrics)
	listen(c, o.port, sv, ml, stopBg)
}

func runStore(c *commonOpts, o *storeOpts) {
	if o.minFreeSpace < 0 || o.minFreeSpace > 100 {
		log.Fatalf("-minFreeSpace %d: want 0-100", o.minFreeSpace)
	}
	logCommon("store", c)
	log.Printf("store: %+v", *o)

	ml, client := join(c, cluster.Meta{Role: "store", Port: o.port})
	d := connect(c)
	st, err := store.New(store.Config{Dirs: o.dirs, MinFree: int64(o.minFreeSpace), Token: cluster.APIToken(c.secret),
		DB: d, Client: client})
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("store_ids: %v", st.IDs())
	bg, stopBg := context.WithCancel(context.Background())
	go st.Run(bg, cluster.Addr(ml, o.port))
	serveMetrics(c, st.Metrics)
	listen(c, o.port, st, ml, stopBg)
}

// join starts memberlist and a store client whose waits stretch with each store's round trip.
func join(c *commonOpts, meta cluster.Meta) (*memberlist.Memberlist, *shard.Client) {
	rtt := cluster.NewRTT()
	ml, err := cluster.New(c.ip, c.gossipPort, c.secret, meta, rtt)
	if err != nil {
		log.Fatal(err)
	}
	go cluster.Join(ml, c.peers)
	client := shard.NewClient(cluster.APIToken(c.secret))
	client.RTT = rtt.Of
	return ml, client
}

func connect(c *commonOpts) *db.Cassandra {
	d, err := db.Connect(c.cassandra, c.keyspace, c.replication)
	if err != nil {
		log.Fatal(err)
	}
	return d
}

func listen(c *commonOpts, port int, h http.Handler, ml *memberlist.Memberlist, stopBg context.CancelFunc) {
	srv := &http.Server{
		Addr:              net.JoinHostPort(c.ip, strconv.Itoa(port)),
		Handler:           h,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	ln, err := shard.Listen(srv.Addr)
	if err != nil {
		log.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	go func() {
		if err := srv.Serve(ln); !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}()
	log.Printf("listening on %s, %d cluster members", srv.Addr, ml.NumMembers())
	<-ctx.Done()
	stop() // a second signal kills right away
	if stopBg != nil {
		stopBg()
	}
	log.Printf("shutting down, waiting up to %v for requests in flight", drainWait)
	ml.Leave(5 * time.Second) // peers stop sending here now instead of in ~30s
	dctx, cancel := context.WithTimeout(context.Background(), drainWait)
	defer cancel()
	if err := srv.Shutdown(dctx); err != nil {
		log.Printf("shutdown: %v", err)
	}
}

// serveMetrics starts the -metricsPort listener, if set.
func serveMetrics(c *commonOpts, write func(*metrics.Writer)) {
	if c.metricsPort == 0 {
		return
	}
	ln, err := net.Listen("tcp", net.JoinHostPort(c.ip, strconv.Itoa(c.metricsPort)))
	if err != nil {
		log.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", metrics.Handler(write))
	go (&http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}).Serve(ln)
	log.Printf("metrics on %s/metrics", ln.Addr())
}

// drainWait is how long a shutdown waits for requests in flight, a var so tests can shrink it.
var drainWait = 30 * time.Second

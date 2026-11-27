package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"filescale/internal/cluster"
	"filescale/internal/metrics"
	"filescale/internal/store"
)

// -metricsPort serves the process metrics and the role's own at /metrics.
func TestMetricsPort(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	c := commonFlags(fs)
	if err := fs.Parse(nil); err != nil || c.metricsPort != 0 {
		t.Fatalf("default -metricsPort %d, %v: want off", c.metricsPort, err)
	}
	port := freePort(t)
	if err := fs.Parse([]string{"-ip=127.0.0.1", fmt.Sprint("-metricsPort=", port)}); err != nil {
		t.Fatal(err)
	}
	srv := serveMetrics(c, func(m *metrics.Writer) { m.Counter("filescale_test_total", "Test.", 7) })
	t.Cleanup(func() { srv.Close() })
	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/metrics", port))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(b), "\ngo_goroutines ") || !strings.Contains(string(b), "\nfilescale_test_total 7\n") {
		t.Errorf("%s\n%s", resp.Status, b)
	}
}

func TestMetricsPortAuth(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	c := commonFlags(fs)
	port := freePort(t)
	if err := fs.Parse([]string{"-ip=127.0.0.1", fmt.Sprint("-metricsPort=", port), "-metricsAuth=prom:s:ecret"}); err != nil {
		t.Fatal(err)
	}
	srv := serveMetrics(c, func(m *metrics.Writer) { m.Counter("filescale_test_total", "Test.", 7) })
	t.Cleanup(func() { srv.Close() })

	fetch := func(path, user, pass string) (int, string, string) {
		req, err := http.NewRequest("GET", fmt.Sprintf("http://127.0.0.1:%d%s", port, path), nil)
		if err != nil {
			t.Fatal(err)
		}
		if user != "" || pass != "" {
			req.SetBasicAuth(user, pass)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, resp.Header.Get("WWW-Authenticate"), string(b)
	}

	code, hdr, body := fetch("/metrics", "", "")
	if code != http.StatusUnauthorized || hdr != `Basic realm="metrics"` || strings.Contains(body, "filescale_test_total") {
		t.Errorf("unauth /metrics: %d %q\n%s", code, hdr, body)
	}
	if code, _, _ := fetch("/other", "", ""); code != http.StatusUnauthorized {
		t.Errorf("unauth /other: %d, want 401", code)
	}
	if code, _, _ := fetch("/metrics", "prom", "wrong"); code != http.StatusUnauthorized {
		t.Errorf("wrong pass /metrics: %d, want 401", code)
	}
	code, _, body = fetch("/metrics", "prom", "s:ecret")
	if code != http.StatusOK || !strings.Contains(body, "\ngo_goroutines ") || !strings.Contains(body, "\nfilescale_test_total 7\n") {
		t.Errorf("valid /metrics: %d\n%s", code, body)
	}
	if code, _, _ := fetch("/other", "prom", "s:ecret"); code != http.StatusNotFound {
		t.Errorf("valid /other: %d, want 404", code)
	}
}

func TestDrainWaitFlag(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	c := commonFlags(fs)
	if err := fs.Parse(nil); err != nil || c.drainWait != 30*time.Second {
		t.Fatalf("default -drainWait = %v, %v: want 30s", c.drainWait, err)
	}
	if err := fs.Parse([]string{"-drainWait=2m"}); err != nil || c.drainWait != 2*time.Minute {
		t.Fatalf("custom -drainWait = %v, %v: want 2m", c.drainWait, err)
	}
}

func TestScrubFlags(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	o := storeFlags(fs)
	if err := fs.Parse(nil); err != nil || o.scrubPass != store.DefaultScrubPass || o.scrubGap != store.DefaultScrubGap {
		t.Fatalf("default scrub flags = (%v, %v), %v", o.scrubPass, o.scrubGap, err)
	}
	if err := fs.Parse([]string{"-scrubPass=168h", "-scrubGap=0"}); err != nil || o.scrubPass != 168*time.Hour || o.scrubGap != 0 {
		t.Fatalf("custom scrub flags = (%v, %v), %v", o.scrubPass, o.scrubGap, err)
	}
}

func TestNoRepairFlag(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	o := serveFlags(fs)
	if err := fs.Parse(nil); err != nil || o.noRepair {
		t.Fatalf("default -noRepair = %v, %v: want false", o.noRepair, err)
	}
	if err := fs.Parse([]string{"-noRepair"}); err != nil || !o.noRepair {
		t.Fatalf("-noRepair = %v, %v: want true", o.noRepair, err)
	}
}

// SIGTERM lets a request in flight finish before listen returns; one that outlasts
// drainWait is cut off.
func TestListenDrains(t *testing.T) {
	for i, finishes := range []bool{true, false} {
		wait := 5 * time.Second
		if !finishes {
			wait = 300 * time.Millisecond
		}
		ml, err := cluster.New("127.0.0.1", 17960+i, "", cluster.Meta{Role: "serve"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { ml.Shutdown() })
		c := &commonOpts{ip: "127.0.0.1", drainWait: wait}
		ln, port := apiListener(c, 0)

		started, release := make(chan struct{}), make(chan struct{})
		t.Cleanup(func() { close(release) })
		h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			close(started)
			<-release
			io.WriteString(w, "done")
		})
		returned := make(chan struct{})
		go func() {
			listen(c, ln, h, ml, nil, nil)
			close(returned)
		}()
		got := make(chan error, 1)
		go func() { got <- get(port) }()
		select {
		case <-started: // handlers run after signal.NotifyContext, a SIGTERM no longer kills the test
		case <-time.After(10 * time.Second):
			t.Fatal("request never reached the handler")
		}
		syscall.Kill(os.Getpid(), syscall.SIGTERM)

		if !finishes {
			select {
			case <-returned:
			case <-time.After(3 * time.Second):
				t.Fatal("listen waited past drainWait")
			}
			continue
		}
		select {
		case <-returned:
			t.Fatal("listen returned with a request in flight")
		case <-time.After(300 * time.Millisecond):
		}
		release <- struct{}{} // the handler takes it, cleanup closes the channel later
		if err := <-got; err != nil {
			t.Errorf("request in flight: %v", err)
		}
		select {
		case <-returned:
		case <-time.After(3 * time.Second):
			t.Fatal("listen didn't return after the last request")
		}
	}
}

func TestZerodownHelper(t *testing.T) {
	if os.Getenv("FILESCALE_ZD_HELPER") == "" {
		return
	}
	port, _ := strconv.Atoi(os.Getenv("FILESCALE_ZD_PORT"))
	mport, _ := strconv.Atoi(os.Getenv("FILESCALE_ZD_METRICS"))
	gport, _ := strconv.Atoi(os.Getenv("FILESCALE_ZD_GOSSIP"))
	peer := os.Getenv("FILESCALE_ZD_PEER")
	gate := os.Getenv("FILESCALE_ZD_GATE")
	c := &commonOpts{
		ip:          "127.0.0.1",
		gossipPort:  gport,
		peers:       []string{peer},
		metricsPort: mport,
		drainWait:   5 * time.Second,
	}
	if code, parent := supervise(c, port); parent {
		os.Exit(code)
	}
	ln, boundPort := apiListener(c, port)
	bg, stopBg := context.WithCancel(context.Background())
	ml := join(bg, c, cluster.Meta{Role: "store", Port: boundPort}, nil)
	pid := strconv.Itoa(os.Getpid())
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Pid", pid)
		if r.URL.Path == "/slow" {
			_ = os.WriteFile(gate+".started", []byte(pid), 0o644)
			for {
				if _, err := os.Stat(gate + ".release"); err == nil {
					break
				}
				time.Sleep(20 * time.Millisecond)
			}
		}
		io.WriteString(w, pid)
	})
	msrv := serveMetrics(c, func(m *metrics.Writer) { m.Gauge("filescale_child_pid", "Child PID.", float64(os.Getpid())) })
	listen(c, ln, h, ml, stopBg, msrv)
	os.Exit(0)
}

func TestZerodownReloadAndDrain(t *testing.T) {
	peerPort := freePort(t)
	peer, err := cluster.New("127.0.0.1", peerPort, "", cluster.Meta{Role: "serve", Port: 8080}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { peer.Shutdown() })

	apiPort := freePort(t)
	metricsPort := freePort(t)
	gossipPort := freePort(t)
	gate := filepath.Join(t.TempDir(), "gate")

	cmd := exec.Command(os.Args[0], "-test.run=^TestZerodownHelper$")
	cmd.Env = append(os.Environ(),
		"FILESCALE_ZD_HELPER=1",
		fmt.Sprintf("FILESCALE_ZD_PORT=%d", apiPort),
		fmt.Sprintf("FILESCALE_ZD_METRICS=%d", metricsPort),
		fmt.Sprintf("FILESCALE_ZD_GOSSIP=%d", gossipPort),
		fmt.Sprintf("FILESCALE_ZD_PEER=127.0.0.1:%d", peerPort),
		"FILESCALE_ZD_GATE="+gate,
	)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()
	t.Cleanup(func() {
		select {
		case <-exited:
		default:
			_ = cmd.Process.Kill()
		}
	})

	client := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}
	fetch := func(path string) (string, error) {
		resp, err := client.Get(fmt.Sprintf("http://127.0.0.1:%d%s", apiPort, path))
		if err != nil {
			return "", err
		}
		defer resp.Body.Close()
		b, err := io.ReadAll(resp.Body)
		return string(b), err
	}

	var pid1 string
	for start := time.Now(); ; time.Sleep(30 * time.Millisecond) {
		if body, err := fetch("/fast"); err == nil {
			pid1 = body
			break
		}
		if time.Since(start) > 10*time.Second {
			t.Fatal("child 1 never came up")
		}
	}
	storeAddr := fmt.Sprintf("127.0.0.1:%d", apiPort)
	if !cluster.AliveStoreAddrs(peer)[storeAddr] {
		t.Fatalf("peer missing %s after startup", storeAddr)
	}

	slowDone := make(chan string, 1)
	slowErr := make(chan error, 1)
	go func() {
		body, err := fetch("/slow")
		if err != nil {
			slowErr <- err
			return
		}
		slowDone <- body
	}()
	for start := time.Now(); ; time.Sleep(20 * time.Millisecond) {
		if _, err := os.Stat(gate + ".started"); err == nil {
			break
		}
		if time.Since(start) > 5*time.Second {
			t.Fatal("slow request never reached child 1")
		}
	}

	if err := cmd.Process.Signal(syscall.SIGHUP); err != nil {
		t.Fatal(err)
	}

	var pid2 string
	for start := time.Now(); ; time.Sleep(30 * time.Millisecond) {
		body, err := fetch("/fast")
		if err != nil {
			t.Fatalf("request failed during reload: %v", err)
		}
		if body != pid1 {
			pid2 = body
			break
		}
		if time.Since(start) > 10*time.Second {
			t.Fatal("child 2 never took over after SIGHUP")
		}
	}

	for start := time.Now(); ; time.Sleep(30 * time.Millisecond) {
		mresp, err := client.Get(fmt.Sprintf("http://127.0.0.1:%d/metrics", metricsPort))
		if err != nil {
			t.Fatalf("metrics failed during reload: %v", err)
		}
		mb, _ := io.ReadAll(mresp.Body)
		mresp.Body.Close()
		if strings.Contains(string(mb), "filescale_child_pid "+pid2+"\n") {
			break
		}
		if time.Since(start) > 5*time.Second {
			t.Fatalf("metrics never switched to child 2 (%s):\n%s", pid2, mb)
		}
	}
	if !cluster.AliveStoreAddrs(peer)[storeAddr] {
		t.Fatalf("peer lost %s during reload", storeAddr)
	}

	if err := os.WriteFile(gate+".release", nil, 0o644); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-slowErr:
		t.Fatalf("slow request failed during reload: %v", err)
	case body := <-slowDone:
		if body != pid1 {
			t.Errorf("slow request served by %s, want %s", body, pid1)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("slow request never finished")
	}

	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-exited:
		if err != nil {
			t.Fatalf("parent exit after SIGTERM: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("parent never exited after SIGTERM")
	}
	for start := time.Now(); cluster.AliveStoreAddrs(peer)[storeAddr]; time.Sleep(20 * time.Millisecond) {
		if time.Since(start) > 3*time.Second {
			t.Fatal("peer still sees store alive after SIGTERM")
		}
	}
}

func TestCLIValidationBeforeSupervise(t *testing.T) {
	bin := filepath.Join(t.TempDir(), "filescale")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	for _, args := range [][]string{
		{"serve", "-h"},
		{"store", "-h"},
		{"serve", "-drainWait", "-1s"},
		{"serve", "-dataShards", "0"},
		{"serve", "-metricsAuth", "nocolon"},
		{"serve", "-metricsAuth", ":pass"},
		{"serve", "-metricsAuth", "user:"},
		{"store", "-minFreeSpace", "101"},
		{"store", "-scrubPass", "0"},
		{"store", "-scrubPass", "-1s"},
		{"store", "-scrubGap", "-1s"},
		{"store", "-metricsAuth", "nocolon"},
	} {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		out, err := exec.CommandContext(ctx, bin, args...).CombinedOutput()
		hung := ctx.Err() != nil
		cancel()
		if hung {
			t.Fatalf("%v hung (spawned supervisor?):\n%s", args, out)
		}
		if args[1] != "-h" && err == nil {
			t.Errorf("%v: want non-zero exit", args)
		}
		if args[1] == "-metricsAuth" && strings.Contains(string(out), args[2]) {
			t.Errorf("%v: output leaked secret:\n%s", args, out)
		}
	}

	for _, args := range [][]string{{"serve"}, {"store"}, {"serve", "-h"}} {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		cmd := exec.CommandContext(ctx, bin, args...)
		cmd.Env = append(os.Environ(), "FILESCALE_METRICS_AUTH=badval")
		out, err := cmd.CombinedOutput()
		hung := ctx.Err() != nil
		cancel()
		if hung {
			t.Fatalf("%v (env) hung (spawned supervisor?):\n%s", args, out)
		}
		if len(args) > 1 && args[1] == "-h" {
			if err != nil {
				t.Errorf("%v (env): want exit 0, got %v\n%s", args, err, out)
			}
		} else if err == nil {
			t.Errorf("%v (env): want non-zero exit", args)
		}
		if strings.Contains(string(out), "badval") {
			t.Errorf("%v (env): output leaked secret:\n%s", args, out)
		}
	}
}

func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	return port
}

// get fetches / from port, retrying until it listens, and wants "done".
func get(port int) error {
	var resp *http.Response
	var err error
	for range 100 {
		if resp, err = http.Get(fmt.Sprintf("http://127.0.0.1:%d/", port)); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err == nil && string(b) != "done" {
		err = fmt.Errorf("body %q", b)
	}
	return err
}

func TestListFlags(t *testing.T) {
	tests := []struct {
		args      []string
		peers     []string
		cassandra []string
		dirs      []string
	}{
		{nil, nil, []string{"127.0.0.1"}, []string{"./data"}},
		{[]string{"-peers="}, nil, []string{"127.0.0.1"}, []string{"./data"}},
		{
			[]string{"-peers=a:7946,b:7946", "-cassandra=c1,c2,c3", "-dir=/d1,/d2"},
			[]string{"a:7946", "b:7946"}, []string{"c1", "c2", "c3"}, []string{"/d1", "/d2"},
		},
	}
	for _, tt := range tests {
		fs := flag.NewFlagSet("test", flag.ContinueOnError)
		common := commonFlags(fs)
		store := storeFlags(fs)
		if err := fs.Parse(tt.args); err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(common.peers, tt.peers) {
			t.Errorf("%v: peers = %v, want %v", tt.args, common.peers, tt.peers)
		}
		if !slices.Equal(common.cassandra, tt.cassandra) {
			t.Errorf("%v: cassandra = %v, want %v", tt.args, common.cassandra, tt.cassandra)
		}
		if !slices.Equal(store.dirs, tt.dirs) {
			t.Errorf("%v: dirs = %v, want %v", tt.args, store.dirs, tt.dirs)
		}
	}
}

func TestKindFlags(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	serve := serveFlags(fs)
	if err := fs.Parse([]string{"-only=download", "-tokenFor=upload"}); err != nil {
		t.Fatal(err)
	}
	if serve.only != "download" || serve.tokenFor != "upload" {
		t.Errorf("only = %q, tokenFor = %q", serve.only, serve.tokenFor)
	}

	fs = flag.NewFlagSet("test", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	serveFlags(fs)
	if err := fs.Parse([]string{"-only=uploads"}); err == nil {
		t.Error("-only=uploads: want an error")
	}
}

// An empty -secret, -token, or -metricsAuth falls back to its environment variable, a flag wins over it.
func TestFromEnv(t *testing.T) {
	t.Setenv("FILESCALE_SECRET", "env-secret")
	t.Setenv("FILESCALE_TOKEN", "")
	t.Setenv("FILESCALE_METRICS_AUTH", "env:pass")
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	common := commonFlags(fs)
	serve := serveFlags(fs)
	if err := fs.Parse([]string{"-token=flag-token"}); err != nil {
		t.Fatal(err)
	}
	fromEnv(&common.secret, "FILESCALE_SECRET")
	fromEnv(&common.metricsAuth, "FILESCALE_METRICS_AUTH")
	fromEnv(&serve.token, "FILESCALE_TOKEN")
	if common.secret != "env-secret" || common.metricsAuth != "env:pass" || serve.token != "flag-token" {
		t.Errorf("secret = %q, metricsAuth = %q, token = %q", common.secret, common.metricsAuth, serve.token)
	}

	t.Setenv("FILESCALE_TOKEN", "env-token")
	fs = flag.NewFlagSet("test", flag.ContinueOnError)
	common = commonFlags(fs)
	serve = serveFlags(fs)
	if err := fs.Parse([]string{"-secret=flag-secret", "-metricsAuth=flag:pass"}); err != nil {
		t.Fatal(err)
	}
	fromEnv(&common.secret, "FILESCALE_SECRET")
	fromEnv(&common.metricsAuth, "FILESCALE_METRICS_AUTH")
	fromEnv(&serve.token, "FILESCALE_TOKEN")
	if common.secret != "flag-secret" || common.metricsAuth != "flag:pass" || serve.token != "env-token" {
		t.Errorf("secret = %q, metricsAuth = %q, token = %q", common.secret, common.metricsAuth, serve.token)
	}
}

func TestInitCommon(t *testing.T) {
	t.Setenv("FILESCALE_SECRET", "")
	t.Setenv("FILESCALE_METRICS_AUTH", "prom:secret")
	c := &commonOpts{}
	if err := initCommon(c); err != nil {
		t.Fatalf("valid env auth with metricsPort=0: %v", err)
	}
	if c.metricsAuth != "prom:secret" {
		t.Errorf("metricsAuth = %q, want prom:secret", c.metricsAuth)
	}

	c = &commonOpts{metricsAuth: "flag:pass"}
	if err := initCommon(c); err != nil {
		t.Fatalf("flag overrides env: %v", err)
	}
	if c.metricsAuth != "flag:pass" {
		t.Errorf("metricsAuth = %q, want flag:pass", c.metricsAuth)
	}

	t.Setenv("FILESCALE_METRICS_AUTH", "badsecret")
	if err := initCommon(&commonOpts{}); err == nil {
		t.Fatal("malformed env auth: want error")
	} else if strings.Contains(err.Error(), "badsecret") {
		t.Errorf("error leaked secret: %v", err)
	}

	var buf bytes.Buffer
	origOut := log.Writer()
	log.SetOutput(&buf)
	t.Cleanup(func() { log.SetOutput(origOut) })
	logCommon("serve", &commonOpts{secret: "s3cr3t", metricsAuth: "prom:p4ss"})
	out := buf.String()
	for _, leak := range []string{"s3cr3t", "p4ss", "prom:"} {
		if strings.Contains(out, leak) {
			t.Errorf("logCommon leaked %q in %q", leak, out)
		}
	}
	if !strings.Contains(out, "secret:set") || !strings.Contains(out, "metricsAuth:set") {
		t.Errorf("logCommon missing redacted fields: %q", out)
	}
}

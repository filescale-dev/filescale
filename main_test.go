package main

import (
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"filescale/internal/cluster"
	"filescale/internal/metrics"
)

// -metricsPort serves the process metrics and the role's own at /metrics.
func TestMetricsPort(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	c := commonFlags(fs)
	if err := fs.Parse(nil); err != nil || c.metricsPort != 0 {
		t.Fatalf("default -metricsPort %d, %v: want off", c.metricsPort, err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	if err := fs.Parse([]string{"-ip=127.0.0.1", fmt.Sprint("-metricsPort=", port)}); err != nil {
		t.Fatal(err)
	}
	serveMetrics(c, func(m *metrics.Writer) { m.Counter("filescale_test_total", "Test.", 7) })
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

// SIGTERM lets a request in flight finish before listen returns; one that outlasts
// drainWait is cut off.
func TestListenDrains(t *testing.T) {
	old := drainWait
	t.Cleanup(func() { drainWait = old })
	for i, finishes := range []bool{true, false} {
		drainWait = 5 * time.Second
		if !finishes {
			drainWait = 300 * time.Millisecond
		}
		ml, err := cluster.New("127.0.0.1", 17960+i, "", cluster.Meta{Role: "serve"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { ml.Shutdown() })
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		port := ln.Addr().(*net.TCPAddr).Port
		ln.Close()

		started, release := make(chan struct{}), make(chan struct{})
		t.Cleanup(func() { close(release) })
		h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			close(started)
			<-release
			io.WriteString(w, "done")
		})
		returned := make(chan struct{})
		go func() {
			listen(&commonOpts{ip: "127.0.0.1"}, port, h, ml, nil)
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

// An empty -secret or -token falls back to its environment variable, a flag wins over it.
func TestFromEnv(t *testing.T) {
	t.Setenv("FILESCALE_SECRET", "env-secret")
	t.Setenv("FILESCALE_TOKEN", "")
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	common := commonFlags(fs)
	serve := serveFlags(fs)
	if err := fs.Parse([]string{"-token=flag-token"}); err != nil {
		t.Fatal(err)
	}
	fromEnv(&common.secret, "FILESCALE_SECRET")
	fromEnv(&serve.token, "FILESCALE_TOKEN")
	if common.secret != "env-secret" || serve.token != "flag-token" {
		t.Errorf("secret = %q, token = %q", common.secret, serve.token)
	}

	t.Setenv("FILESCALE_TOKEN", "env-token")
	fs = flag.NewFlagSet("test", flag.ContinueOnError)
	common = commonFlags(fs)
	serve = serveFlags(fs)
	if err := fs.Parse([]string{"-secret=flag-secret"}); err != nil {
		t.Fatal(err)
	}
	fromEnv(&common.secret, "FILESCALE_SECRET")
	fromEnv(&serve.token, "FILESCALE_TOKEN")
	if common.secret != "flag-secret" || serve.token != "env-token" {
		t.Errorf("secret = %q, token = %q", common.secret, serve.token)
	}
}

//go:build unix

package zerodown

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestHelper(t *testing.T) {
	dir := os.Getenv("ZD_TEST_DIR")
	if dir == "" {
		return
	}
	if IsParent() {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		f, err := ln.(*net.TCPListener).File()
		ln.Close()
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		if err := os.WriteFile(filepath.Join(dir, "addr"), []byte(ln.Addr().String()), 0o644); err != nil {
			t.Fatal(err)
		}
		timeout := 10 * time.Second
		if s := os.Getenv("ZD_TIMEOUT"); s != "" {
			timeout, _ = time.ParseDuration(s)
		}
		os.Exit(Run(Options{
			Args:           []string{"-test.run=^TestHelper$"},
			ExtraFiles:     []*os.File{f},
			StartupTimeout: timeout,
		}))
	}

	pid := os.Getpid()
	_ = os.WriteFile(filepath.Join(dir, fmt.Sprintf("started.%d", pid)), nil, 0o644)
	modeBytes, _ := os.ReadFile(filepath.Join(dir, "mode"))
	mode := strings.TrimSpace(string(modeBytes))
	if codeStr, ok := strings.CutPrefix(mode, "crash:"); ok {
		code, _ := strconv.Atoi(codeStr)
		_ = os.WriteFile(filepath.Join(dir, fmt.Sprintf("crashed.%d", pid)), nil, 0o644)
		os.Exit(code)
	}
	if mode == "hang" {
		select {}
	}

	fd := os.NewFile(3, "listener")
	ln, err := net.FileListener(fd)
	fd.Close()
	if err != nil {
		os.Exit(99)
	}
	ino := exeInode()
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/crash":
			os.Exit(2)
		case "/exit0":
			os.Exit(0)
		default:
			fmt.Fprintf(w, "%d %d", pid, ino)
		}
	})}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	go func() {
		if err := srv.Serve(ln); !errors.Is(err, http.ErrServerClosed) {
			os.Exit(98)
		}
	}()
	Ready()
	<-ctx.Done()
	_ = srv.Close()
	os.Exit(0)
}

func exeInode() uint64 {
	fi, err := os.Stat("/proc/self/exe")
	if err != nil {
		return 0
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return 0
	}
	return st.Ino
}

type proc struct {
	t      *testing.T
	dir    string
	cmd    *exec.Cmd
	exited chan error
}

func startParent(t *testing.T, exe, mode string, timeout time.Duration) *proc {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "mode"), []byte(mode), 0o644); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(exe, "-test.run=^TestHelper$")
	cmd.Env = append(os.Environ(), "ZD_TEST_DIR="+dir)
	if timeout > 0 {
		cmd.Env = append(cmd.Env, "ZD_TIMEOUT="+timeout.String())
	}
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
			<-exited
		}
	})
	return &proc{t: t, dir: dir, cmd: cmd, exited: exited}
}

func (p *proc) setMode(mode string) {
	p.t.Helper()
	if err := os.WriteFile(filepath.Join(p.dir, "mode"), []byte(mode), 0o644); err != nil {
		p.t.Fatal(err)
	}
}

func (p *proc) addr() string {
	p.t.Helper()
	for start := time.Now(); ; time.Sleep(10 * time.Millisecond) {
		if b, err := os.ReadFile(filepath.Join(p.dir, "addr")); err == nil && len(b) > 0 {
			return strings.TrimSpace(string(b))
		}
		if time.Since(start) > 5*time.Second {
			p.t.Fatal("parent never wrote addr")
		}
	}
}

func (p *proc) get(path string) (string, error) {
	c := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}
	resp, err := c.Get("http://" + p.addr() + path)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	return string(b), err
}

func (p *proc) waitServing() (pid int, ino uint64) {
	p.t.Helper()
	for start := time.Now(); ; time.Sleep(20 * time.Millisecond) {
		if body, err := p.get("/"); err == nil {
			if _, err := fmt.Sscan(body, &pid, &ino); err == nil && pid > 0 {
				return pid, ino
			}
		}
		if time.Since(start) > 5*time.Second {
			p.t.Fatal("child never started serving")
		}
	}
}

func (p *proc) waitExit(wantCode int) {
	p.t.Helper()
	select {
	case err := <-p.exited:
		p.exited <- err // leave result for Cleanup
		code := 0
		if err != nil {
			var ee *exec.ExitError
			if !errors.As(err, &ee) {
				p.t.Fatalf("unexpected wait error: %v", err)
			}
			code = ee.ExitCode()
		}
		if code != wantCode {
			p.t.Fatalf("parent exit code = %d, want %d (err=%v)", code, wantCode, err)
		}
	case <-time.After(5 * time.Second):
		p.t.Fatalf("parent did not exit (want code %d)", wantCode)
	}
}

func (p *proc) waitMarker(prefix string, skip int) int {
	p.t.Helper()
	for start := time.Now(); ; time.Sleep(20 * time.Millisecond) {
		entries, _ := os.ReadDir(p.dir)
		for _, e := range entries {
			if s, ok := strings.CutPrefix(e.Name(), prefix); ok {
				if pid, err := strconv.Atoi(s); err == nil && pid != skip {
					return pid
				}
			}
		}
		if time.Since(start) > 5*time.Second {
			p.t.Fatalf("marker %q never appeared", prefix)
		}
	}
}

func TestChildCrashPropagatesExitCode(t *testing.T) {
	p := startParent(t, os.Args[0], "ok", 0)
	p.waitServing()
	_, _ = p.get("/crash")
	p.waitExit(2)
}

func TestChildUnexpectedZeroExitReturnsNonZero(t *testing.T) {
	p := startParent(t, os.Args[0], "ok", 0)
	p.waitServing()
	_, _ = p.get("/exit0")
	p.waitExit(1)
}

func TestInitialStartupCrash(t *testing.T) {
	p := startParent(t, os.Args[0], "crash:3", 0)
	p.waitExit(3)
}

func TestReloadCrashKeepsLiveChild(t *testing.T) {
	p := startParent(t, os.Args[0], "ok", 0)
	pid1, _ := p.waitServing()

	p.setMode("crash:4")
	if err := p.cmd.Process.Signal(syscall.SIGHUP); err != nil {
		t.Fatal(err)
	}
	p.waitMarker("crashed.", pid1)

	got, _ := p.waitServing()
	if got != pid1 {
		t.Fatalf("serving pid = %d after failed reload, want %d", got, pid1)
	}

	// A subsequent good reload still works.
	p.setMode("ok")
	if err := p.cmd.Process.Signal(syscall.SIGHUP); err != nil {
		t.Fatal(err)
	}
	for start := time.Now(); ; time.Sleep(20 * time.Millisecond) {
		got, _ := p.waitServing()
		if got != pid1 {
			break
		}
		if time.Since(start) > 5*time.Second {
			t.Fatal("good reload never took over after failed reload")
		}
	}
	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	p.waitExit(0)
}

func TestReloadTimeoutKeepsLiveChild(t *testing.T) {
	p := startParent(t, os.Args[0], "ok", 300*time.Millisecond)
	pid1, _ := p.waitServing()

	p.setMode("hang")
	if err := p.cmd.Process.Signal(syscall.SIGHUP); err != nil {
		t.Fatal(err)
	}
	pid2 := p.waitMarker("started.", pid1)
	for start := time.Now(); ; time.Sleep(20 * time.Millisecond) {
		if err := syscall.Kill(pid2, 0); err != nil {
			break
		}
		if time.Since(start) > 5*time.Second {
			t.Fatalf("timed-out child %d still alive", pid2)
		}
	}

	got, _ := p.waitServing()
	if got != pid1 {
		t.Fatalf("serving pid = %d after reload timeout, want %d", got, pid1)
	}
	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	p.waitExit(0)
}

func TestReloadQueuedDuringStartup(t *testing.T) {
	p := startParent(t, os.Args[0], "ok", 500*time.Millisecond)
	pid1, _ := p.waitServing()

	p.setMode("hang")
	if err := p.cmd.Process.Signal(syscall.SIGHUP); err != nil {
		t.Fatal(err)
	}
	p.waitMarker("started.", pid1)
	p.setMode("ok")
	if err := p.cmd.Process.Signal(syscall.SIGHUP); err != nil {
		t.Fatal(err)
	}
	for start := time.Now(); ; time.Sleep(20 * time.Millisecond) {
		if got, _ := p.waitServing(); got != pid1 {
			break
		}
		if time.Since(start) > 5*time.Second {
			t.Fatal("queued reload never took over")
		}
	}
	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	p.waitExit(0)
}

func TestStopDuringReload(t *testing.T) {
	p := startParent(t, os.Args[0], "ok", 10*time.Second)
	pid1, _ := p.waitServing()

	p.setMode("hang")
	if err := p.cmd.Process.Signal(syscall.SIGHUP); err != nil {
		t.Fatal(err)
	}
	pid2 := p.waitMarker("started.", pid1)

	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	p.waitExit(0)
	for _, pid := range []int{pid1, pid2} {
		if err := syscall.Kill(pid, 0); err == nil {
			t.Errorf("child %d still alive after parent exit", pid)
		}
	}
}

func TestReloadAfterBinaryReplaced(t *testing.T) {
	binDir := t.TempDir()
	bin := filepath.Join(binDir, "app")
	copyFile(t, os.Args[0], bin)

	p := startParent(t, bin, "ok", 0)
	pid1, ino1 := p.waitServing()

	// Replace the binary in place via rename (unlinks old inode, turning /proc/self/exe into " (deleted)" on Linux).
	tmp := bin + ".new"
	copyFile(t, os.Args[0], tmp)
	if err := os.Rename(tmp, bin); err != nil {
		t.Fatal(err)
	}

	if err := p.cmd.Process.Signal(syscall.SIGHUP); err != nil {
		t.Fatal(err)
	}
	var pid2 int
	var ino2 uint64
	for start := time.Now(); ; time.Sleep(20 * time.Millisecond) {
		pid2, ino2 = p.waitServing()
		if pid2 != pid1 {
			break
		}
		if time.Since(start) > 5*time.Second {
			t.Fatal("replacement binary never took over after SIGHUP")
		}
	}
	if ino1 != 0 && ino2 == ino1 {
		t.Errorf("child 2 ran old inode %d, want replaced binary", ino2)
	}
	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	p.waitExit(0)
}

func copyFile(t *testing.T, src, dst string) {
	t.Helper()
	in, err := os.Open(src)
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o755)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		t.Fatal(err)
	}
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}
}

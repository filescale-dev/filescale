//go:build unix

package zerodown

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const parentEnv = "PD_PARENT_PROCESS"

// Options configures the supervisor process.
type Options struct {
	Args           []string // defaults to os.Args[1:]
	ExtraFiles     []*os.File
	ExtraEnv       []string
	StartupTimeout time.Duration // defaults to 10m
}

type child struct {
	proc *os.Process
	done chan exit
}

type exit struct {
	state *os.ProcessState
	err   error
}

func parentPID() int {
	pid, err := strconv.Atoi(os.Getenv(parentEnv))
	if err != nil || pid <= 0 {
		return 0
	}
	return pid
}

func IsParent() bool { return parentPID() == 0 }
func IsChild() bool  { return parentPID() > 0 }

// Ready tells the supervising parent that the child is serving traffic.
func Ready() {
	pid := parentPID()
	if pid == 0 || os.Getppid() != pid {
		return
	}
	if err := syscall.Kill(pid, syscall.SIGUSR1); err != nil {
		log.Printf("zerodown: signal parent %d: %v", pid, err)
	}
}

// Run supervises child processes across SIGHUP reloads and returns the exit code.
func Run(opts Options) int {
	if opts.StartupTimeout <= 0 {
		opts.StartupTimeout = 10 * time.Minute
	}
	args := opts.Args
	if args == nil {
		args = os.Args[1:]
	}
	exe, err := resolveExe()
	if err != nil {
		log.Printf("zerodown: %v", err)
		return 1
	}
	// Dup ExtraFiles while blocking so exec's Fd() never flips the shared socket's O_NONBLOCK.
	files := make([]*os.File, len(opts.ExtraFiles))
	for i, f := range opts.ExtraFiles {
		if files[i], err = passable(f); err != nil {
			log.Printf("zerodown: %v", err)
			return 1
		}
		defer files[i].Close()
	}

	ready := make(chan os.Signal, 1)
	signal.Notify(ready, syscall.SIGUSR1)
	defer signal.Stop(ready)

	sigs := make(chan os.Signal, 4)
	signal.Notify(sigs, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	defer signal.Stop(sigs)

	var wg sync.WaitGroup
	start := func() (*child, error) {
		drainReady(ready)
		cmd := exec.Command(exe, args...)
		cmd.Env = childEnv(opts.ExtraEnv)
		cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, os.Stdout, os.Stderr
		cmd.ExtraFiles = files
		if err := cmd.Start(); err != nil {
			return nil, err
		}
		c := &child{proc: cmd.Process, done: make(chan exit, 1)}
		wg.Go(func() {
			st, err := c.proc.Wait()
			log.Printf("zerodown: child %d exited (%v, %v)", c.proc.Pid, st, err)
			c.done <- exit{st, err}
		})
		return c, nil
	}

	// Wait for next to signal Ready while still watching stop signals and live.
	pending := false // SIGHUP arrived mid-reload
	waitReady := func(next, live *child) (ok, stopped bool, ex exit, liveEx *exit) {
		t := time.NewTimer(opts.StartupTimeout)
		defer t.Stop()
		var liveDone <-chan exit
		if live != nil {
			liveDone = live.done
		}
		for {
			select {
			case <-ready:
				return true, false, exit{}, liveEx
			case ex = <-next.done:
				return false, false, ex, liveEx
			case <-t.C:
				log.Printf("zerodown: child %d timed out after %v", next.proc.Pid, opts.StartupTimeout)
				killChild(next)
				return false, false, exit{}, liveEx
			case le := <-liveDone:
				liveDone = nil
				liveEx = &le
			case sig := <-sigs:
				if sig == syscall.SIGHUP {
					log.Printf("zerodown: reload requested while child %d is starting, queued", next.proc.Pid)
					pending = true
					continue
				}
				stopChild(next, syscall.SIGTERM)
				if live != nil && liveEx == nil {
					stopChild(live, syscall.SIGTERM)
				}
				return false, true, exit{}, liveEx
			}
		}
	}

	live, err := start()
	if err != nil {
		log.Printf("zerodown: start child: %v", err)
		return 1
	}
	log.Printf("zerodown: started child %d", live.proc.Pid)
	ok, stopped, ex, _ := waitReady(live, nil)
	if stopped {
		wg.Wait()
		return 0
	}
	if !ok {
		wg.Wait()
		return crashCode(ex)
	}

	for {
		if !pending {
			select {
			case ex := <-live.done:
				wg.Wait()
				return crashCode(ex)
			case sig := <-sigs:
				if sig != syscall.SIGHUP {
					stopChild(live, syscall.SIGTERM)
					wg.Wait()
					return 0
				}
			}
		}
		pending = false
		log.Printf("zerodown: reloading %s", exe)
		next, err := start()
		if err != nil {
			log.Printf("zerodown: reload start failed, keeping child %d: %v", live.proc.Pid, err)
			continue
		}
		log.Printf("zerodown: started replacement child %d", next.proc.Pid)
		ok, stopped, ex, liveEx := waitReady(next, live)
		if stopped {
			wg.Wait()
			return 0
		}
		if ok {
			if liveEx == nil {
				stopChild(live, syscall.SIGTERM)
			}
			live = next
			continue
		}
		if liveEx != nil {
			wg.Wait()
			if ex.state != nil {
				return crashCode(ex)
			}
			return crashCode(*liveEx)
		}
		log.Printf("zerodown: reload failed, keeping child %d", live.proc.Pid)
	}
}

// passable dups f while blocking so os.File.Fd() never clears O_NONBLOCK later.
func passable(f *os.File) (*os.File, error) {
	rc, err := f.SyscallConn()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", f.Name(), err)
	}
	nfd, derr := -1, error(nil)
	if err := rc.Control(func(fd uintptr) { nfd, derr = syscall.Dup(int(fd)) }); err != nil {
		return nil, fmt.Errorf("%s: %w", f.Name(), err)
	}
	if derr != nil {
		return nil, fmt.Errorf("dup %s: %w", f.Name(), derr)
	}
	syscall.CloseOnExec(nfd)
	if err := syscall.SetNonblock(nfd, false); err != nil {
		syscall.Close(nfd)
		return nil, fmt.Errorf("%s: %w", f.Name(), err)
	}
	return os.NewFile(uintptr(nfd), f.Name()), nil
}

func killChild(c *child) {
	stopChild(c, syscall.SIGTERM)
	select {
	case <-c.done:
	case <-time.After(2 * time.Second):
		_ = c.proc.Kill()
		<-c.done
	}
}

func stopChild(c *child, sig os.Signal) {
	if c == nil {
		return
	}
	if err := c.proc.Signal(sig); err != nil {
		log.Printf("zerodown: signal child %d: %v", c.proc.Pid, err)
	}
}

func drainReady(ch <-chan os.Signal) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}

// crashCode turns an unsolicited child exit into a non-zero exit code for systemd.
func crashCode(ex exit) int {
	if ex.state == nil {
		return 1
	}
	if ws, ok := ex.state.Sys().(syscall.WaitStatus); ok && ws.Signaled() {
		return 128 + int(ws.Signal())
	}
	if code := ex.state.ExitCode(); code > 0 {
		return code
	}
	return 1
}

// resolveExe captures the binary path at startup before an in-place deploy unlinks it.
func resolveExe() (string, error) {
	if len(os.Args) > 0 && os.Args[0] != "" {
		if p, err := exec.LookPath(os.Args[0]); err == nil {
			if abs, err := filepath.Abs(p); err == nil {
				return abs, nil
			}
		}
	}
	exe, err := os.Executable()
	if err != nil {
		return "", err
	}
	return strings.TrimSuffix(exe, " (deleted)"), nil
}

func childEnv(extra []string) []string {
	prefix := parentEnv + "="
	base := os.Environ()
	env := make([]string, 0, len(base)+len(extra)+1)
	for _, kv := range base {
		if !strings.HasPrefix(kv, prefix) {
			env = append(env, kv)
		}
	}
	env = append(env, prefix+strconv.Itoa(os.Getpid()))
	return append(env, extra...)
}

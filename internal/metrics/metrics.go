// Package metrics writes the Prometheus text format, served on -metricsPort.
package metrics

import (
	"cmp"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"os"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
)

// Writer writes samples, a family's samples one after another.
type Writer struct {
	w    io.Writer
	last string
}

func NewWriter(w io.Writer) *Writer { return &Writer{w: w} }

// Counter writes one sample, labels are name, value pairs.
func (m *Writer) Counter(name, help string, v float64, labels ...string) {
	m.sample("counter", name, help, v, labels)
}

func (m *Writer) Gauge(name, help string, v float64, labels ...string) {
	m.sample("gauge", name, help, v, labels)
}

var escape = strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)

func (m *Writer) sample(kind, name, help string, v float64, labels []string) {
	if name != m.last {
		fmt.Fprintf(m.w, "# HELP %s %s\n# TYPE %s %s\n", name, help, name, kind)
		m.last = name
	}
	var b strings.Builder
	b.WriteString(name)
	for i := 0; i+1 < len(labels); i += 2 {
		sep := ","
		if i == 0 {
			sep = "{"
		}
		fmt.Fprintf(&b, `%s%s="%s"`, sep, labels[i], escape.Replace(labels[i+1]))
	}
	if len(labels) > 1 {
		b.WriteByte('}')
	}
	fmt.Fprintf(m.w, "%s %s\n", b.String(), strconv.FormatFloat(v, 'f', -1, 64))
}

// Handler serves the process metrics and then write's.
func Handler(write func(*Writer)) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		m := NewWriter(w)
		process(m)
		write(m)
	})
}

// ParseBasicAuth splits a "user:pass" credential on the first colon.
func ParseBasicAuth(s string) (user, pass string, err error) {
	user, pass, ok := strings.Cut(s, ":")
	if !ok || user == "" || pass == "" {
		return "", "", errors.New("want user:pass")
	}
	if strings.IndexFunc(s, func(r rune) bool { return r < 0x20 || r == 0x7f }) >= 0 {
		return "", "", errors.New("control characters not allowed")
	}
	return user, pass, nil
}

// BasicAuth protects next with HTTP Basic authentication.
func BasicAuth(next http.Handler, user, pass string) http.Handler {
	valid := user != "" && pass != ""
	wantUser := sha256.Sum256([]byte(user))
	wantPass := sha256.Sum256([]byte(pass))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u, p, ok := r.BasicAuth()
		gotUser := sha256.Sum256([]byte(u))
		gotPass := sha256.Sum256([]byte(p))
		if !valid || !ok ||
			subtle.ConstantTimeCompare(gotUser[:], wantUser[:])&subtle.ConstantTimeCompare(gotPass[:], wantPass[:]) != 1 {
			w.Header().Set("WWW-Authenticate", `Basic realm="metrics"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// process writes the usual process metrics, so stock dashboards work.
func process(m *Writer) {
	var ru syscall.Rusage
	if syscall.Getrusage(syscall.RUSAGE_SELF, &ru) == nil {
		m.Counter("process_cpu_seconds_total", "User and system CPU time.", float64(ru.Utime.Nano()+ru.Stime.Nano())/1e9)
	}
	if b, err := os.ReadFile("/proc/self/statm"); err == nil {
		if f := strings.Fields(string(b)); len(f) > 1 {
			pages, _ := strconv.ParseInt(f[1], 10, 64)
			m.Gauge("process_resident_memory_bytes", "Resident memory.", float64(pages*int64(os.Getpagesize())))
		}
	}
	if fds, err := os.ReadDir("/proc/self/fd"); err == nil {
		m.Gauge("process_open_fds", "Open file descriptors.", float64(len(fds)))
	}
	m.Gauge("go_goroutines", "Goroutines.", float64(runtime.NumGoroutine()))
}

// Status records a response's code and counts its body bytes into Sent.
type Status struct {
	http.ResponseWriter
	Code int
	Sent *atomic.Int64
}

func (s *Status) WriteHeader(code int) {
	if s.Code == 0 && code >= 200 {
		s.Code = code
	}
	s.ResponseWriter.WriteHeader(code)
}

func (s *Status) Write(b []byte) (int, error) {
	if s.Code == 0 {
		s.Code = http.StatusOK
	}
	n, err := s.ResponseWriter.Write(b)
	s.Sent.Add(int64(n))
	return n, err
}

// ReadFrom keeps net/http's own copy path for http.ServeContent.
func (s *Status) ReadFrom(r io.Reader) (int64, error) {
	if s.Code == 0 {
		s.Code = http.StatusOK
	}
	n, err := io.Copy(s.ResponseWriter, r)
	s.Sent.Add(n)
	return n, err
}

// Unwrap is for http.NewResponseController.
func (s *Status) Unwrap() http.ResponseWriter { return s.ResponseWriter }

// CountBody returns a shallow copy of r whose body counts the bytes read into n.
// A copy, not r.Body replaced in place: net/http checks its own request's Body type to see
// that an Expect: 100-continue body was never asked for. With a wrapper there, an early answer
// (401, 507) first waits to read the body the client won't send until 100 Continue: both hang.
func CountBody(r *http.Request, n *atomic.Int64) *http.Request {
	r = r.WithContext(r.Context())
	r.Body = Body{ReadCloser: r.Body, N: n}
	return r
}

// Body counts the bytes read from a request body into N.
type Body struct {
	io.ReadCloser
	N *atomic.Int64
}

func (b Body) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	b.N.Add(int64(n))
	return n, err
}

// Requests counts responses by method and code.
type Requests struct {
	mu sync.Mutex
	n  map[request]int64
}

type request struct {
	method string
	code   int
}

func (q *Requests) Add(method string, code int) {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodPut, http.MethodDelete:
	default:
		method = "other" // clients can't make up label values
	}
	if code == 0 {
		code = http.StatusOK // nothing written, net/http sends a 200
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.n == nil {
		q.n = map[request]int64{}
	}
	q.n[request{method, code}]++
}

func (q *Requests) Write(m *Writer) {
	q.mu.Lock()
	n := maps.Clone(q.n)
	q.mu.Unlock()
	for _, k := range slices.SortedFunc(maps.Keys(n), func(a, b request) int {
		return cmp.Or(strings.Compare(a.method, b.method), a.code-b.code)
	}) {
		m.Counter("filescale_requests_total", "Requests by method and response code.", float64(n[k]),
			"method", k.method, "code", strconv.Itoa(k.code))
	}
}

package metrics

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestWriter(t *testing.T) {
	var b strings.Builder
	m := NewWriter(&b)
	m.Counter("a_total", "A.", 3, "dir", `/x "y"\`, "store", "s1")
	m.Counter("a_total", "A.", 1.5, "dir", "z", "store", "s2")
	m.Gauge("b", "B.", 1e10)
	want := `# HELP a_total A.
# TYPE a_total counter
a_total{dir="/x \"y\"\\",store="s1"} 3
a_total{dir="z",store="s2"} 1.5
# HELP b B.
# TYPE b gauge
b 10000000000
`
	if b.String() != want {
		t.Errorf("got\n%s\nwant\n%s", b.String(), want)
	}
}

func TestRequests(t *testing.T) {
	var q Requests
	q.Add("PATCH", 405)
	q.Add("GET", 0) // nothing written
	q.Add("GET", 404)
	q.Add("GET", 404)
	var b strings.Builder
	q.Write(NewWriter(&b))
	for _, line := range []string{
		`filescale_requests_total{method="GET",code="200"} 1`,
		`filescale_requests_total{method="GET",code="404"} 2`,
		`filescale_requests_total{method="other",code="405"} 1`,
	} {
		if !strings.Contains(b.String(), line+"\n") {
			t.Errorf("missing %s in\n%s", line, b.String())
		}
	}
}

func TestStatus(t *testing.T) {
	var n atomic.Int64
	s := &Status{ResponseWriter: httptest.NewRecorder(), Sent: &n}
	io.Copy(s, strings.NewReader("hello")) // ServeContent's path, through ReadFrom
	s.Write([]byte("!"))
	if s.Code != http.StatusOK || n.Load() != 6 {
		t.Errorf("code %d, %d bytes", s.Code, n.Load())
	}
	s = &Status{ResponseWriter: httptest.NewRecorder(), Sent: &n}
	http.Error(s, "nope", http.StatusNotFound)
	if s.Code != http.StatusNotFound {
		t.Errorf("code %d", s.Code)
	}
	if http.NewResponseController(s).Flush() != nil {
		t.Error("ResponseController can't reach the real writer")
	}
}

func TestHandler(t *testing.T) {
	w := httptest.NewRecorder()
	Handler(func(m *Writer) { m.Gauge("x", "X.", 1) }).ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
	for _, s := range []string{"process_cpu_seconds_total ", "process_resident_memory_bytes ", "process_open_fds ", "go_goroutines ", "\nx 1\n"} {
		if !strings.Contains(w.Body.String(), s) {
			t.Errorf("missing %q in\n%s", s, w.Body.String())
		}
	}
}

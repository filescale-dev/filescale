package metrics

import (
	"encoding/base64"
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

func TestParseBasicAuth(t *testing.T) {
	for _, tt := range []struct {
		in, user, pass string
	}{
		{"prom:secret", "prom", "secret"},
		{"prom:a:b:c", "prom", "a:b:c"},
		{"prom:pass word", "prom", "pass word"},
	} {
		u, p, err := ParseBasicAuth(tt.in)
		if err != nil || u != tt.user || p != tt.pass {
			t.Errorf("ParseBasicAuth(%q) = (%q, %q, %v), want (%q, %q, nil)", tt.in, u, p, err, tt.user, tt.pass)
		}
	}
	for _, in := range []string{"", "nocolon", ":pass", "user:", ":", "user:pass\n", "user:pass\r", "user\x00:pass", "user:pass\x7f"} {
		if u, p, err := ParseBasicAuth(in); err == nil {
			t.Errorf("ParseBasicAuth(%q) = (%q, %q, nil), want error", in, u, p)
		}
	}
}

func TestBasicAuth(t *testing.T) {
	inner := Handler(func(m *Writer) { m.Gauge("x", "X.", 1) })
	var called bool
	h := BasicAuth(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		inner.ServeHTTP(w, r)
	}), "prom", "s:ecret")

	for _, tc := range []struct {
		name string
		req  func() *http.Request
	}{
		{"missing", func() *http.Request { return httptest.NewRequest("GET", "/metrics", nil) }},
		{"bearer", func() *http.Request {
			r := httptest.NewRequest("GET", "/metrics", nil)
			r.Header.Set("Authorization", "Bearer prom:s:ecret")
			return r
		}},
		{"bad base64", func() *http.Request {
			r := httptest.NewRequest("GET", "/metrics", nil)
			r.Header.Set("Authorization", "Basic !!!")
			return r
		}},
		{"wrong user", func() *http.Request {
			r := httptest.NewRequest("GET", "/metrics", nil)
			r.SetBasicAuth("other", "s:ecret")
			return r
		}},
		{"wrong pass", func() *http.Request {
			r := httptest.NewRequest("GET", "/metrics", nil)
			r.SetBasicAuth("prom", "wrong")
			return r
		}},
		{"post unauthenticated", func() *http.Request { return httptest.NewRequest("POST", "/metrics", nil) }},
	} {
		called = false
		w := httptest.NewRecorder()
		h.ServeHTTP(w, tc.req())
		if w.Code != http.StatusUnauthorized {
			t.Errorf("%s: code = %d, want 401", tc.name, w.Code)
		}
		if got := w.Header().Get("WWW-Authenticate"); got != `Basic realm="metrics"` {
			t.Errorf("%s: WWW-Authenticate = %q", tc.name, got)
		}
		if called || strings.Contains(w.Body.String(), "\nx 1\n") {
			t.Errorf("%s: protected handler ran", tc.name)
		}
	}

	for _, tc := range []struct {
		name string
		req  func() *http.Request
	}{
		{"valid", func() *http.Request {
			r := httptest.NewRequest("GET", "/metrics", nil)
			r.SetBasicAuth("prom", "s:ecret")
			return r
		}},
		{"lowercase scheme", func() *http.Request {
			r := httptest.NewRequest("GET", "/metrics", nil)
			r.Header.Set("Authorization", "basic "+base64.StdEncoding.EncodeToString([]byte("prom:s:ecret")))
			return r
		}},
	} {
		called = false
		w := httptest.NewRecorder()
		h.ServeHTTP(w, tc.req())
		if w.Code != http.StatusOK || !called || !strings.Contains(w.Body.String(), "\nx 1\n") {
			t.Errorf("%s: code = %d, called = %v, body =\n%s", tc.name, w.Code, called, w.Body.String())
		}
	}

	for _, creds := range [][2]string{{"", ""}, {"prom", ""}, {"", "secret"}} {
		called = false
		empty := BasicAuth(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			called = true
		}), creds[0], creds[1])
		r := httptest.NewRequest("GET", "/metrics", nil)
		r.SetBasicAuth(creds[0], creds[1])
		w := httptest.NewRecorder()
		empty.ServeHTTP(w, r)
		if w.Code != http.StatusUnauthorized || called {
			t.Errorf("empty creds %q: code = %d, called = %v, want 401", creds, w.Code, called)
		}
	}
}

package cluster

import (
	"bytes"
	"encoding/json"
	"log"
	"net"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/memberlist"
)

func TestJoin(t *testing.T) {
	node(t, 17946, "s3cret", Meta{Role: "store", Port: 8081})
	b := node(t, 17947, "s3cret", Meta{Role: "serve", Port: 8080})
	if _, err := b.Join([]string{"127.0.0.1:17946"}); err != nil {
		t.Fatal(err)
	}
	if n := b.NumMembers(); n != 2 {
		t.Fatalf("members = %d, want 2", n)
	}
	if got := Addrs(b, "store"); !slices.Equal(got, []string{"127.0.0.1:8081"}) {
		t.Errorf("store addrs = %v", got)
	}
	if got := Addrs(b, "serve"); !slices.Equal(got, []string{Addr(b, 8080)}) {
		t.Errorf("serve addrs = %v, want %s", got, Addr(b, 8080))
	}
}

// Join keeps trying while alone: a peer that comes up later is joined within its 5s.
func TestJoinRetries(t *testing.T) {
	a := node(t, 17952, "", Meta{Role: "serve"})
	go Join(a, []string{"127.0.0.1:17953"}) // not up yet
	time.Sleep(time.Second)
	node(t, 17953, "", Meta{Role: "store"})
	for start := time.Now(); a.NumMembers() < 2; time.Sleep(100 * time.Millisecond) {
		if time.Since(start) > 8*time.Second {
			t.Fatal("never joined the peer that came up late")
		}
	}
}

func TestJoinWrongSecret(t *testing.T) {
	node(t, 17948, "s3cret", Meta{Role: "store"})
	b := node(t, 17949, "other", Meta{Role: "store"})
	if _, err := b.Join([]string{"127.0.0.1:17948"}); err == nil || b.NumMembers() != 1 {
		t.Fatalf("join with wrong secret: err = %v, members = %d", err, b.NumMembers())
	}
}

func TestAPIToken(t *testing.T) {
	if APIToken("") != "" {
		t.Error("empty secret should mean no token")
	}
	if a, b := APIToken("x"), APIToken("y"); a == b || len(a) != 64 {
		t.Errorf("tokens %q, %q", a, b)
	}
}

func node(t *testing.T, port int, secret string, meta Meta) *memberlist.Memberlist {
	ml, err := New("127.0.0.1", port, secret, meta, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ml.Shutdown() })
	return ml
}

func TestRTT(t *testing.T) {
	r := NewRTT()
	n := &memberlist.Node{Addr: net.ParseIP("10.0.0.7")}
	if r.Of("10.0.0.7:8081") != 0 {
		t.Error("unprobed host should be 0")
	}
	r.NotifyPingComplete(n, 80*time.Millisecond, nil)
	if got := r.Of("10.0.0.7:8081"); got != 80*time.Millisecond {
		t.Errorf("first probe: %v", got)
	}
	r.NotifyPingComplete(n, 160*time.Millisecond, nil) // one slow probe moves it 1/8 of the way
	if got := r.Of("10.0.0.7"); got != 90*time.Millisecond {
		t.Errorf("smoothed: %v, want 90ms", got)
	}
}

func TestRTTFromProbes(t *testing.T) {
	if testing.Short() {
		t.Skip("waits for a WAN probe interval")
	}
	node(t, 17950, "", Meta{Role: "store"})
	rtt := NewRTT()
	b, err := New("127.0.0.1", 17951, "", Meta{Role: "serve"}, rtt)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Shutdown() })
	if _, err := b.Join([]string{"127.0.0.1:17950"}); err != nil {
		t.Fatal(err)
	}
	for range 150 {
		rtt.mu.Lock()
		_, ok := rtt.m["127.0.0.1"] // loopback round trips are below the Windows clock tick, so 0s
		rtt.mu.Unlock()
		if ok {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Error("no round trip learned from memberlist's probes within 15s")
}

// A serve joining next to one with different shard settings warns.
func TestServeSettingsWarning(t *testing.T) {
	var buf bytes.Buffer
	log.SetOutput(&buf)
	defer log.SetOutput(os.Stderr)
	e := events(Meta{Role: "serve", DataShards: 8, ParityShards: 4, SmallFileSize: 1 << 20})
	meta, err := json.Marshal(Meta{Role: "serve", DataShards: 10, ParityShards: 4, SmallFileSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	e.NotifyJoin(&memberlist.Node{Name: "other", Meta: meta})
	if !strings.Contains(buf.String(), "uses 10+4") {
		t.Errorf("no warning: %q", buf.String())
	}
	buf.Reset()
	meta, _ = json.Marshal(Meta{Role: "serve", DataShards: 8, ParityShards: 4, SmallFileSize: 1 << 20})
	e.NotifyJoin(&memberlist.Node{Name: "other", Meta: meta})
	if buf.Len() != 0 {
		t.Errorf("matching settings warned: %q", buf.String())
	}
}

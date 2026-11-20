package cluster

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/hashicorp/memberlist"
)

// Meta is gossiped with every node so others find its HTTP port and settings.
type Meta struct {
	Role          string `json:"role"`
	Port          int    `json:"port"`
	DataShards    int    `json:"dataShards,omitempty"`
	ParityShards  int    `json:"parityShards,omitempty"`
	SmallFileSize int64  `json:"smallFileSize,omitempty"`
}

// New joins nothing yet, see Join. rtt, if set, learns each node's round trip from memberlist's probes.
func New(ip string, port int, secret string, meta Meta, rtt *RTT) (*memberlist.Memberlist, error) {
	host, err := os.Hostname()
	if err != nil {
		return nil, err
	}
	// WAN timeouts everywhere: a far DC isn't flagged dead, and nothing time-critical
	// waits on memberlist (repair goes by last_seen, a dead store fails a request fast)
	cfg := memberlist.DefaultWANConfig()
	if rtt != nil {
		cfg.Ping = rtt
	}
	cfg.Name = fmt.Sprintf("%s:%d", host, port)
	cfg.BindPort = port
	cfg.AdvertisePort = port
	if ip != "" {
		cfg.BindAddr = ip
		cfg.AdvertiseAddr = ip
	}
	if secret == "" {
		log.Print("no -secret set: gossip and store API are unauthenticated")
	} else {
		key := sha256.Sum256([]byte("gossip" + secret))
		cfg.SecretKey = key[:]
	}
	b, err := json.Marshal(meta)
	if err != nil {
		return nil, err
	}
	cfg.Delegate = delegate(b)
	cfg.Events = events(meta)
	cfg.LogOutput = quiet{}
	return memberlist.Create(cfg)
}

// Join joins peers every 5s while alone, then every minute: memberlist stops contacting
// a node it marked dead after 60s, so a longer network cut would stay split until a restart.
func Join(ml *memberlist.Memberlist, peers []string) {
	for len(peers) > 0 {
		before := ml.NumMembers()
		if _, err := ml.Join(peers); err != nil {
			log.Printf("join %v: %v", peers, err)
		}
		n := ml.NumMembers()
		if n > before {
			log.Printf("joined %v: %d → %d members", peers, before, n)
		}
		if n > 1 {
			time.Sleep(time.Minute)
		} else {
			time.Sleep(5 * time.Second)
		}
	}
}

// APIToken is the bearer token for serve→store and store→store requests, "" when off.
func APIToken(secret string) string {
	if secret == "" {
		return ""
	}
	h := sha256.Sum256([]byte("api" + secret))
	return hex.EncodeToString(h[:])
}

// Addr is this node's advertised HTTP address.
func Addr(ml *memberlist.Memberlist, port int) string {
	return net.JoinHostPort(ml.LocalNode().Addr.String(), strconv.Itoa(port))
}

// Addrs lists the HTTP addresses of live members with the given role.
func Addrs(ml *memberlist.Memberlist, role string) []string {
	var out []string
	for _, n := range ml.Members() {
		var m Meta
		if json.Unmarshal(n.Meta, &m) == nil && m.Role == role {
			out = append(out, net.JoinHostPort(n.Addr.String(), strconv.Itoa(m.Port)))
		}
	}
	return out
}

// RTT is a smoothed round trip per host (TCP's 7/8 + 1/8), from memberlist's probes, so no extra traffic.
type RTT struct {
	mu sync.Mutex
	m  map[string]time.Duration
}

func NewRTT() *RTT { return &RTT{m: map[string]time.Duration{}} }

// Of is the round trip to addr (host or host:port), 0 if not probed yet.
func (r *RTT) Of(addr string) time.Duration {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.m[host]
}

func (r *RTT) NotifyPingComplete(n *memberlist.Node, rtt time.Duration, payload []byte) {
	host := n.Addr.String()
	r.mu.Lock()
	defer r.mu.Unlock()
	if old, ok := r.m[host]; ok {
		rtt = old - old/8 + rtt/8
	}
	r.m[host] = rtt
}

func (*RTT) AckPayload() []byte { return nil }

type delegate []byte

func (d delegate) NodeMeta(limit int) []byte                { return d }
func (delegate) NotifyMsg([]byte)                           {}
func (delegate) GetBroadcasts(overhead, limit int) [][]byte { return nil }
func (delegate) LocalState(join bool) []byte                { return nil }
func (delegate) MergeRemoteState(buf []byte, join bool)     {}

type events Meta

func (e events) NotifyJoin(n *memberlist.Node) {
	var m Meta
	if e.Role != "serve" || json.Unmarshal(n.Meta, &m) != nil || m.Role != "serve" {
		return
	}
	if m.DataShards != e.DataShards || m.ParityShards != e.ParityShards || m.SmallFileSize != e.SmallFileSize {
		log.Printf("serve %s uses %d+%d, small files under %d, this one %d+%d, under %d",
			n.Name, m.DataShards, m.ParityShards, m.SmallFileSize, e.DataShards, e.ParityShards, e.SmallFileSize)
	}
}

func (events) NotifyLeave(*memberlist.Node)  {}
func (events) NotifyUpdate(*memberlist.Node) {}

// quiet drops memberlist's debug lines.
type quiet struct{}

func (quiet) Write(p []byte) (int, error) {
	if !bytes.Contains(p, []byte("[DEBUG]")) {
		os.Stderr.Write(p)
	}
	return len(p), nil
}

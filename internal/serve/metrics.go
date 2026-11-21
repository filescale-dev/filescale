package serve

import (
	"maps"
	"sync"
	"sync/atomic"
	"time"

	"filescale/internal/db"
	"filescale/internal/metrics"
)

// counters are the numbers behind Metrics.
type counters struct {
	requests                metrics.Requests
	uploading, downloading  atomic.Int64 // requests in flight
	received, sent          atomic.Int64 // body bytes
	repaired, repairErrors  atomic.Int64
	repairEnded, repairTook atomic.Int64 // the last pass: unix nanos, nanos
	repairFailed            atomic.Int64 // its failed buckets
	cut                     atomic.Int64 // downloads cut off after the headers
	storesMu                sync.Mutex
	slow, refused           map[db.UUID]int64
}

// inc counts one event against a store.
func (s *counters) inc(m *map[db.UUID]int64, id db.UUID) {
	s.storesMu.Lock()
	defer s.storesMu.Unlock()
	if *m == nil {
		*m = map[db.UUID]int64{}
	}
	(*m)[id]++
}

// Metrics writes the serve's metrics for -metricsPort.
func (s *Serve) Metrics(m *metrics.Writer) {
	s.requests.Write(m)
	help := "Requests in flight: upload is PUT and DELETE, download GET and HEAD."
	m.Gauge("filescale_inflight", help, float64(s.uploading.Load()), "op", "upload")
	m.Gauge("filescale_inflight", help, float64(s.downloading.Load()), "op", "download")
	m.Counter("filescale_received_bytes_total", "Request body bytes read.", float64(s.received.Load()))
	m.Counter("filescale_sent_bytes_total", "Response body bytes sent.", float64(s.sent.Load()))
	m.Counter("filescale_parity_stripes_total", "Download stripes rebuilt from parity.", float64(s.rebuilt.Load()))
	m.Counter("filescale_download_cut_total", "Downloads cut off after the headers: a stripe had fewer than data_shards readable shards.", float64(s.cut.Load()))

	s.storesMu.Lock()
	slow, refused := maps.Clone(s.slow), maps.Clone(s.refused)
	s.storesMu.Unlock()
	for id, n := range slow {
		m.Counter("filescale_slow_drops_total", "Downloads that dropped the store as slow and read parity instead.", float64(n), "store", id.String())
	}
	for id, n := range refused {
		m.Counter("filescale_put_refused_total", "Shard PUTs the store failed before any data: refused, an error, or no 100 Continue in time.", float64(n), "store", id.String())
	}

	// the stores table as of the last reload, the same on every serve
	stores, alive := s.stores.Load().list, s.Alive()
	each := func(write func(string, string, float64, ...string), name, help string, v func(db.Store) float64) {
		for _, st := range stores {
			write(name, help, v(st), "store", st.ID.String(), "addr", st.Addr)
		}
	}
	each(m.Gauge, "filescale_store_up", "1 if uploads can pick the store: alive in memberlist, not in maintenance, seen in the last 3 minutes.",
		func(st db.Store) float64 { return one(usable(st, alive)) })
	each(m.Gauge, "filescale_store_maintenance", "1 if the store is in maintenance.",
		func(st db.Store) float64 { return one(st.State == "maintenance") })
	each(m.Gauge, "filescale_store_free_bytes", "Bytes left for uploads, from the store's last heartbeat.",
		func(st db.Store) float64 { return float64(st.Free) })
	each(m.Gauge, "filescale_store_last_seen_timestamp_seconds", "The store's last heartbeat.",
		func(st db.Store) float64 { return float64(st.LastSeen.Unix()) })
	m.Gauge("filescale_stores_reloaded_timestamp_seconds", "Last time the stores table was read from Cassandra.", seconds(s.reloaded.Load()))

	m.Counter("filescale_repaired_shards_total", "Shards rebuilt onto another store by repair.", float64(s.repaired.Load()))
	m.Counter("filescale_repair_errors_total", "Files repair failed on, retried in the next pass.", float64(s.repairErrors.Load()))
	m.Gauge("filescale_repair_last_pass_timestamp_seconds", "End of this serve's last repair pass, 0 = none yet.", seconds(s.repairEnded.Load()))
	m.Gauge("filescale_repair_pass_duration_seconds", "Length of that pass.", seconds(s.repairTook.Load()))
	m.Gauge("filescale_repair_failed_buckets", "Buckets that pass couldn't read.", float64(s.repairFailed.Load()))
}

func one(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

func seconds(nanos int64) float64 { return float64(nanos) / float64(time.Second) }

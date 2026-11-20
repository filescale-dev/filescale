package store

import (
	"time"

	"filescale/internal/metrics"
)

// Metrics writes the store's metrics for -metricsPort.
func (s *Store) Metrics(m *metrics.Writer) {
	s.requests.Write(m)
	m.Counter("filescale_received_bytes_total", "Request body bytes read.", float64(s.received.Load()))
	m.Counter("filescale_sent_bytes_total", "Response body bytes sent.", float64(s.sent.Load()))
	each := func(write func(string, string, float64, ...string), name, help string, v func(*disk) float64) {
		for _, d := range s.list {
			write(name, help, v(d), "store", d.id.String(), "dir", d.dir)
		}
	}
	each(m.Gauge, "filescale_disk_free_bytes", "Free bytes on the disk.",
		func(d *disk) float64 { free, _, _ := diskSpace(d.dir); return float64(free) })
	each(m.Gauge, "filescale_disk_size_bytes", "Size of the disk.",
		func(d *disk) float64 { _, total, _ := diskSpace(d.dir); return float64(total) })
	each(m.Gauge, "filescale_disk_failed", "1 if the disk failed its last probe: it refuses shards and stops heartbeating.",
		func(d *disk) float64 { return one(d.failed.Load()) })
	each(m.Gauge, "filescale_disk_inflight_bytes", "Bytes of shard PUTs in progress.",
		func(d *disk) float64 { return float64(d.inflight.Load()) })
	each(m.Counter, "filescale_scrub_rebuilt_blocks_total", "Bad or missing blocks scrub rebuilt from the other shards.",
		func(d *disk) float64 { return float64(d.blocks.Load()) })
	each(m.Counter, "filescale_scrub_missing_shards_total", "Shard files scrub found missing, rebuilt whole.",
		func(d *disk) float64 { return float64(d.missing.Load()) })
	each(m.Counter, "filescale_scrub_orphan_rows_total", "Index rows of shards no file uses, dropped by scrub.",
		func(d *disk) float64 { return float64(d.orphans.Load()) })
	each(m.Counter, "filescale_cleanup_removed_total", "Shard files without an index row, removed by cleanup.",
		func(d *disk) float64 { return float64(d.removed.Load()) })
	each(m.Gauge, "filescale_scrub_last_pass_timestamp_seconds", "End of the last whole scrub pass, 0 = none yet.",
		func(d *disk) float64 { return float64(d.scrubbed.Load()) / float64(time.Second) })
	each(m.Gauge, "filescale_cleanup_last_pass_timestamp_seconds", "End of the last cleanup pass, 0 = none yet.",
		func(d *disk) float64 { return float64(d.cleaned.Load()) / float64(time.Second) })
}

func one(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

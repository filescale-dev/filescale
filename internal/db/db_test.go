package db

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

func TestMem(t *testing.T) { testDB(t, NewMem()) }

func TestCassandra(t *testing.T) {
	c := connectTestCassandra(t)
	testDB(t, c)
}

func connectTestCassandra(t *testing.T) *Cassandra {
	t.Helper()
	hosts := os.Getenv("FILESCALE_CASSANDRA")
	if hosts == "" {
		t.Skip("set FILESCALE_CASSANDRA=host1,host2 to run against Cassandra")
	}
	c, err := Connect(strings.Split(hosts, ","), "filescale_test", 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

// Concurrent writes to one name never merge stores or mix rows.
func TestCassandraFilesConcurrency(t *testing.T) {
	c := connectTestCassandra(t)
	ctx := context.Background()
	uuids := func(n int) []UUID {
		s := make([]UUID, n)
		for i := range s {
			s[i] = newID(t)
		}
		return s
	}

	t.Run("concurrent_put_file", func(t *testing.T) {
		name := "repro/concurrent-put-" + newID(t).String()
		t.Cleanup(func() { c.DeleteFile(ctx, name) })
		const workers = 32
		const rounds = 50
		for round := range rounds {
			wantByID := make(map[UUID][]UUID, workers)
			files := make([]*File, workers)
			for i := range workers {
				id := newID(t)
				st := uuids(5)
				wantByID[id] = st
				files[i] = &File{Name: name, ID: id, Size: 76224, BLAKE3: make([]byte, 32), ContentType: "application/octet-stream",
					DataShards: 1, ParityShards: 4, Stores: st}
			}
			start := make(chan struct{})
			var wg sync.WaitGroup
			for _, f := range files {
				wg.Go(func() {
					<-start
					if err := c.PutFile(ctx, f); err != nil {
						t.Errorf("round %d PutFile: %v", round, err)
					}
				})
			}
			close(start)
			wg.Wait()
			got, err := c.File(ctx, name)
			if err != nil {
				t.Fatal(err)
			}
			if got == nil {
				t.Fatalf("round %d: File returned nil", round)
			}
			if err := got.CheckLayout(); err != nil {
				t.Fatalf("round %d: CheckLayout failed: %v (stores=%v)", round, err, got.Stores)
			}
			if want, ok := wantByID[got.ID]; !ok || !slices.Equal(got.Stores, want) {
				t.Fatalf("round %d: mixed row fields for id %s: got stores %v, want %v", round, got.ID, got.Stores, want)
			}
		}
	})

	t.Run("repair_racing_put_file", func(t *testing.T) {
		name := "repro/repair-vs-put-" + newID(t).String()
		t.Cleanup(func() { c.DeleteFile(ctx, name) })
		for round := range 50 {
			f1 := &File{Name: name, ID: newID(t), Size: 76224, BLAKE3: make([]byte, 32), ContentType: "application/octet-stream",
				DataShards: 1, ParityShards: 4, Stores: uuids(5)}
			if err := c.PutFile(ctx, f1); err != nil {
				t.Fatal(err)
			}
			f2 := &File{Name: name, ID: newID(t), Size: 76224, BLAKE3: make([]byte, 32), ContentType: "application/octet-stream",
				DataShards: 1, ParityShards: 4, Stores: uuids(5)}
			rep := newID(t)
			start := make(chan struct{})
			var wg sync.WaitGroup
			wg.Go(func() {
				<-start
				if err := c.PutFile(ctx, f2); err != nil {
					t.Errorf("round %d PutFile: %v", round, err)
				}
			})
			wg.Go(func() {
				<-start
				if _, err := c.SetShardStore(ctx, name, f1.ID, 2, rep); err != nil {
					t.Errorf("round %d SetShardStore: %v", round, err)
				}
			})
			close(start)
			wg.Wait()
			got, err := c.File(ctx, name)
			if err != nil {
				t.Fatal(err)
			}
			if got == nil {
				t.Fatalf("round %d: File returned nil", round)
			}
			if err := got.CheckLayout(); err != nil {
				t.Fatalf("round %d: CheckLayout failed: %v (stores=%v)", round, err, got.Stores)
			}
			if got.ID == f2.ID && !slices.Equal(got.Stores, f2.Stores) {
				t.Fatalf("round %d: f2 stores corrupted by repair: got %v, want %v", round, got.Stores, f2.Stores)
			}
		}
	})

	t.Run("delete_racing_put_and_repair", func(t *testing.T) {
		name := "repro/delete-race-" + newID(t).String()
		t.Cleanup(func() { c.DeleteFile(ctx, name) })
		for round := range 50 {
			f1 := &File{Name: name, ID: newID(t), Size: 76224, BLAKE3: make([]byte, 32), ContentType: "application/octet-stream",
				DataShards: 1, ParityShards: 4, Stores: uuids(5)}
			if err := c.PutFile(ctx, f1); err != nil {
				t.Fatal(err)
			}
			f2 := &File{Name: name, ID: newID(t), Size: 76224, BLAKE3: make([]byte, 32), ContentType: "application/octet-stream",
				DataShards: 1, ParityShards: 4, Stores: uuids(5)}
			start := make(chan struct{})
			var wg sync.WaitGroup
			wg.Go(func() {
				<-start
				if err := c.DeleteFile(ctx, name); err != nil {
					t.Errorf("round %d DeleteFile: %v", round, err)
				}
			})
			wg.Go(func() {
				<-start
				if err := c.PutFile(ctx, f2); err != nil {
					t.Errorf("round %d PutFile: %v", round, err)
				}
			})
			wg.Go(func() {
				<-start
				if _, err := c.SetShardStore(ctx, name, f1.ID, 2, newID(t)); err != nil {
					t.Errorf("round %d SetShardStore: %v", round, err)
				}
			})
			close(start)
			wg.Wait()
			got, err := c.File(ctx, name)
			if err != nil {
				t.Fatal(err)
			}
			if got != nil {
				if err := got.CheckLayout(); err != nil {
					t.Fatalf("round %d: CheckLayout failed after delete race: %v (row=%+v)", round, err, got)
				}
			}
		}
	})

	t.Run("overwrite_resets_all_fields", func(t *testing.T) {
		name := "repro/overwrite-" + newID(t).String()
		t.Cleanup(func() { c.DeleteFile(ctx, name) })
		p0, p1 := newID(t), newID(t)
		multi := &File{Name: name, ID: newID(t), Size: 15, BLAKE3: []byte{1, 2}, ContentType: "video/mp4",
			DataShards: 1, ParityShards: 4, Stores: uuids(10), Parts: []UUID{p0, p1}, PartSize: 10}
		if err := c.PutFile(ctx, multi); err != nil {
			t.Fatal(err)
		}
		single := &File{Name: name, ID: newID(t), Size: 76224, BLAKE3: []byte{3, 4}, ContentType: "text/plain",
			DataShards: 1, ParityShards: 4, Stores: uuids(5)}
		if err := c.PutFile(ctx, single); err != nil {
			t.Fatal(err)
		}
		got, err := c.File(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		if err := got.CheckLayout(); err != nil {
			t.Fatalf("after multi->single overwrite: %v (%+v)", err, got)
		}
		if got.ID != single.ID || !slices.Equal(got.Stores, single.Stores) || len(got.Parts) != 0 || got.PartSize != 0 {
			t.Fatalf("after multi->single overwrite = %+v, want %+v", got, single)
		}
		empty := &File{Name: name, ID: newID(t), Size: 0, BLAKE3: make([]byte, 32), ContentType: "application/octet-stream",
			DataShards: 1, ParityShards: 4}
		if err := c.PutFile(ctx, empty); err != nil {
			t.Fatal(err)
		}
		got, err = c.File(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		if err := got.CheckLayout(); err != nil {
			t.Fatalf("after single->empty overwrite: %v (%+v)", err, got)
		}
		if got.ID != empty.ID || len(got.Stores) != 0 || got.Size != 0 {
			t.Fatalf("after single->empty overwrite = %+v, want %+v", got, empty)
		}
	})
}

// testDB checks the behavior serve and store rely on, for any DB.
func testDB(t *testing.T, d DB) {
	ctx := context.Background()
	id, s1, s2 := newID(t), newID(t), newID(t)
	name := "dir/" + id.String()
	if f, err := d.File(ctx, name); err != nil || f != nil {
		t.Fatalf("missing file: %v, %v", f, err)
	}

	want := &File{Name: name, ID: id, Size: 10, BLAKE3: []byte{1, 2}, ContentType: "text/plain",
		DataShards: 8, ParityShards: 4, Stores: []UUID{s1, s2}}
	if err := d.PutFile(ctx, want); err != nil {
		t.Fatal(err)
	}
	got, err := d.File(ctx, name)
	if err != nil || got == nil || got.ID != id || got.Size != 10 || string(got.BLAKE3) != "\x01\x02" ||
		got.ContentType != "text/plain" || got.DataShards != 8 || got.ParityShards != 4 || !slices.Equal(got.Stores, want.Stores) {
		t.Fatalf("File = %+v, %v", got, err)
	}
	if got, err := d.File(All(ctx), name); err != nil || got == nil || got.ID != id {
		t.Fatalf("File at ALL = %+v, %v", got, err)
	}

	empty := &File{Name: name + "-empty", ID: newID(t), BLAKE3: make([]byte, 32), DataShards: 1, ParityShards: 4}
	if err := d.PutFile(ctx, empty); err != nil {
		t.Fatal(err)
	}
	if got, err := d.File(ctx, empty.Name); err != nil || got == nil || got.Size != 0 || len(got.Stores) != 0 || got.Parts != nil {
		t.Errorf("empty file = %+v, %v", got, err) // only its files row, no stores
	}
	d.DeleteFile(ctx, empty.Name)

	if ok, err := d.SetShardStore(ctx, name, id, 1, s1); !ok || err != nil {
		t.Errorf("SetShardStore: %v, %v", ok, err)
	}
	if ok, err := d.SetShardStore(ctx, name, id, 1, s1); !ok || err != nil {
		t.Errorf("SetShardStore to the same store: %v, %v", ok, err) // two repairs may pick one store
	}
	if ok, err := d.SetShardStore(ctx, name, newID(t), 0, s2); ok || err != nil {
		t.Errorf("SetShardStore with another file_id: %v, %v, want false", ok, err)
	}
	if got, _ := d.File(ctx, name); !slices.Equal(got.Stores, []UUID{s1, s1}) {
		t.Errorf("stores after SetShardStore = %v", got.Stores)
	}

	if err := d.PutShard(ctx, Shard{StoreID: s1, FileID: id, Index: 3, Name: name}); err != nil {
		t.Fatal(err)
	}
	rows, err := d.Shards(ctx, s1, Bucket(id))
	if err != nil || len(rows) != 1 || rows[0].FileID != id || rows[0].Index != 3 || rows[0].Name != name ||
		time.Since(rows[0].Written).Abs() > time.Hour {
		t.Fatalf("Shards = %+v, %v", rows, err)
	}
	if rows, err := d.Shards(All(ctx), s1, Bucket(id)); err != nil || len(rows) != 1 || rows[0].FileID != id {
		t.Fatalf("Shards at ALL = %+v, %v", rows, err)
	}
	if rows, _ := d.Shards(ctx, s2, Bucket(id)); len(rows) != 0 {
		t.Errorf("other store has rows: %+v", rows)
	}
	if err := d.DeleteShard(ctx, s1, id); err != nil {
		t.Fatal(err)
	}
	if rows, _ := d.Shards(ctx, s1, Bucket(id)); len(rows) != 0 {
		t.Errorf("rows after DeleteShard: %+v", rows)
	}

	if err := d.DeleteFile(ctx, name); err != nil {
		t.Fatal(err)
	}
	if f, _ := d.File(ctx, name); f != nil {
		t.Errorf("file after DeleteFile: %+v", f)
	}

	// a file in two parts of one shard each, repair moves part 1's
	p0, p1 := newID(t), newID(t)
	parts := &File{Name: name, ID: id, Size: 15, DataShards: 1, ParityShards: 0, Stores: []UUID{s1, s2}, Parts: []UUID{p0, p1}, PartSize: 10}
	if err := d.PutFile(ctx, parts); err != nil {
		t.Fatal(err)
	}
	if ok, err := d.SetShardStore(ctx, name, id, 1, s1); !ok || err != nil {
		t.Errorf("SetShardStore on part 1: %v, %v", ok, err)
	}
	got, err = d.File(ctx, name)
	if err != nil || !slices.Equal(got.Parts, parts.Parts) || got.PartSize != 10 || got.Part(1).ID != p1 || got.Part(1).Size != 5 ||
		!slices.Equal(got.Part(1).Stores, []UUID{s1}) || got.PartOf(p1) != 1 || got.PartOf(id) != -1 {
		t.Fatalf("File with parts = %+v, %v", got, err)
	}
	d.DeleteFile(ctx, name)

	upload := newID(t)
	if rows, err := d.UploadParts(ctx, name, upload); err != nil || len(rows) != 0 {
		t.Fatalf("UploadParts of a new upload = %+v, %v", rows, err)
	}
	for _, i := range []int{1, 0} {
		p := UploadPart{Name: name, UploadID: upload, Index: i, ID: newID(t), Size: 10, Total: 25, HashState: []byte{byte(i)},
			ContentType: "video/mp4", DataShards: 8, ParityShards: 4, Stores: []UUID{s1, s2}}
		if err := d.PutPart(ctx, p); err != nil {
			t.Fatal(err)
		}
	}
	ups, err := d.UploadParts(ctx, name, upload)
	if err != nil || len(ups) != 2 || ups[0].Index != 0 || ups[1].Index != 1 || ups[1].HashState[0] != 1 || ups[0].Total != 25 ||
		ups[0].ContentType != "video/mp4" || !slices.Equal(ups[0].Stores, []UUID{s1, s2}) || time.Since(ups[0].Written).Abs() > time.Hour {
		t.Fatalf("UploadParts = %+v, %v", ups, err)
	}
	if c, ok := d.(*Cassandra); ok {
		var ttl int
		if err := c.s.Query(`SELECT TTL(size) FROM uploads WHERE name = ? AND upload_id = ? AND part = 0`, name, upload).Scan(&ttl); err != nil || ttl < 82000 || ttl > 82800 {
			t.Errorf("uploads TTL = %d, %v", ttl, err)
		}
	}

	now := time.Now().Truncate(time.Millisecond)
	if err := d.Heartbeat(ctx, Store{ID: s1, Addr: "10.0.0.1:8081", Free: 5, LastSeen: now}); err != nil {
		t.Fatal(err)
	}
	stores, err := d.Stores(ctx)
	i := slices.IndexFunc(stores, func(s Store) bool { return s.ID == s1 })
	if err != nil || i < 0 || stores[i].Addr != "10.0.0.1:8081" || stores[i].Free != 5 || !stores[i].LastSeen.Equal(now) || stores[i].State != "" {
		t.Fatalf("Stores = %+v, %v", stores, err)
	}
	if c, ok := d.(*Cassandra); ok {
		if err := c.s.Query(`UPDATE stores SET state = 'readonly' WHERE store_id = ?`, s1).ExecContext(ctx); err != nil {
			t.Fatal(err)
		}
		if err := d.Heartbeat(ctx, Store{ID: s1, Addr: "10.0.0.1:8081", Free: 9, LastSeen: now}); err != nil {
			t.Fatal(err)
		}
		stores, err = d.Stores(ctx)
		i = slices.IndexFunc(stores, func(s Store) bool { return s.ID == s1 })
		if err != nil || i < 0 || stores[i].State != "readonly" || stores[i].Free != 9 {
			t.Fatalf("Stores after readonly heartbeat = %+v, %v", stores, err)
		}
	}
	if err := d.DeleteStore(ctx, s1); err != nil {
		t.Fatal(err)
	}
	stores, _ = d.Stores(ctx)
	if slices.ContainsFunc(stores, func(s Store) bool { return s.ID == s1 }) {
		t.Error("store after DeleteStore")
	}
}

func TestBucket(t *testing.T) {
	id, _ := ParseID("abcd0000-0000-4000-8000-000000000000")
	if b := Bucket(id); b != 0xabcd {
		t.Errorf("bucket = %x, want abcd (the prefix dir ab/cd)", b)
	}
}

func TestCheckLayout(t *testing.T) {
	uuids := func(n int) []UUID {
		s := make([]UUID, n)
		for i := range s {
			s[i] = newID(t)
		}
		return s
	}
	valid := []struct {
		name string
		f    File
	}{
		{"empty", File{Size: 0, DataShards: 1, ParityShards: 4}},
		{"empty non-nil slices", File{Size: 0, DataShards: 1, ParityShards: 4, Stores: []UUID{}, Parts: []UUID{}}},
		{"single 1+4", File{Size: 100, DataShards: 1, ParityShards: 4, Stores: uuids(5)}},
		{"single 8+4", File{Size: 1 << 20, DataShards: 8, ParityShards: 4, Stores: uuids(12)}},
		{"single empty parts slice", File{Size: 100, DataShards: 1, ParityShards: 0, Stores: uuids(1), Parts: []UUID{}}},
		{"multi exact", File{Size: 20, PartSize: 10, DataShards: 1, ParityShards: 4, Parts: uuids(2), Stores: uuids(10)}},
		{"multi partial tail", File{Size: 15, PartSize: 10, DataShards: 8, ParityShards: 4, Parts: uuids(2), Stores: uuids(24)}},
	}
	for _, tc := range valid {
		if err := tc.f.CheckLayout(); err != nil {
			t.Errorf("%s: got %v, want nil", tc.name, err)
		}
	}

	invalid := []struct {
		name string
		f    File
	}{
		{"negative size", File{Size: -1, DataShards: 1, ParityShards: 4}},
		{"zero data shards", File{Size: 10, DataShards: 0, ParityShards: 4, Stores: uuids(4)}},
		{"negative data shards", File{Size: 10, DataShards: -1, ParityShards: 4}},
		{"negative parity", File{Size: 10, DataShards: 1, ParityShards: -1}},
		{"empty with stores", File{Size: 0, DataShards: 1, ParityShards: 4, Stores: uuids(5)}},
		{"empty with parts", File{Size: 0, DataShards: 1, ParityShards: 4, Parts: uuids(1)}},
		{"empty with part size", File{Size: 0, DataShards: 1, ParityShards: 4, PartSize: 10}},
		{"non-empty no stores", File{Size: 10, DataShards: 1, ParityShards: 4}},
		{"doubled stores 1+4", File{Size: 76 << 10, DataShards: 1, ParityShards: 4, Stores: uuids(10)}},
		{"doubled stores 8+4", File{Size: 1 << 20, DataShards: 8, ParityShards: 4, Stores: uuids(24)}},
		{"truncated stores", File{Size: 100, DataShards: 1, ParityShards: 4, Stores: uuids(3)}},
		{"single with part size", File{Size: 100, DataShards: 1, ParityShards: 4, Stores: uuids(5), PartSize: 10}},
		{"multi zero part size", File{Size: 15, DataShards: 1, ParityShards: 4, Parts: uuids(2), Stores: uuids(10)}},
		{"multi negative part size", File{Size: 15, PartSize: -10, DataShards: 1, ParityShards: 4, Parts: uuids(2), Stores: uuids(10)}},
		{"multi too few parts", File{Size: 25, PartSize: 10, DataShards: 1, ParityShards: 4, Parts: uuids(2), Stores: uuids(10)}},
		{"multi too many parts", File{Size: 10, PartSize: 10, DataShards: 1, ParityShards: 4, Parts: uuids(2), Stores: uuids(10)}},
		{"multi short stores", File{Size: 15, PartSize: 10, DataShards: 1, ParityShards: 4, Parts: uuids(2), Stores: uuids(9)}},
		{"multi doubled stores", File{Size: 15, PartSize: 10, DataShards: 1, ParityShards: 4, Parts: uuids(2), Stores: uuids(20)}},
	}
	for _, tc := range invalid {
		if err := tc.f.CheckLayout(); err == nil {
			t.Errorf("%s: got nil, want error", tc.name)
		}
	}
}

func TestMemSetShardStoreBounds(t *testing.T) {
	m := NewMem()
	ctx := context.Background()
	id, s1 := newID(t), newID(t)
	m.PutFile(ctx, &File{Name: "f", ID: id, Size: 10, DataShards: 1, Stores: []UUID{s1}})
	for _, shard := range []int{-1, 1, 5} {
		if ok, err := m.SetShardStore(ctx, "f", id, shard, s1); ok || err == nil {
			t.Errorf("shard %d: ok=%v err=%v, want error", shard, ok, err)
		}
	}
}

// Mem hides upload rows past UploadTTL, like Cassandra's TTL on the uploads table.
func TestMemTTLOfUploads(t *testing.T) {
	m := NewMem()
	ctx := context.Background()
	m.Clock = func() time.Time { return time.Now().Add(-25 * time.Hour) }
	up := newID(t)
	m.PutPart(ctx, UploadPart{Name: "t", UploadID: up, Index: 0, ID: newID(t), Total: 10})
	m.Clock = nil
	m.PutPart(ctx, UploadPart{Name: "t", UploadID: up, Index: 1, ID: newID(t), Total: 10})
	if rows, err := m.UploadParts(ctx, "t", up); err != nil || len(rows) != 1 || rows[0].Index != 1 {
		t.Fatalf("rows = %+v, %v", rows, err)
	}
}

func TestMemHeartbeatPreservesState(t *testing.T) {
	m := NewMem()
	ctx := context.Background()
	id := newID(t)
	if err := m.Heartbeat(ctx, Store{ID: id, Addr: "10.0.0.1:8081", Free: 10, LastSeen: time.Now()}); err != nil {
		t.Fatal(err)
	}
	for _, state := range []string{"readonly", "maintenance", "up"} {
		m.SetState(id, state)
		if err := m.Heartbeat(ctx, Store{ID: id, Addr: "10.0.0.1:8081", Free: 20, LastSeen: time.Now()}); err != nil {
			t.Fatal(err)
		}
		stores, err := m.Stores(ctx)
		if err != nil || len(stores) != 1 || stores[0].State != state || stores[0].Free != 20 {
			t.Fatalf("state %q after heartbeat: %+v, %v", state, stores, err)
		}
	}
}

func TestAmbiguous(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want bool
	}{
		{&gocql.RequestErrWriteTimeout{WriteType: "CAS"}, true},
		{fmt.Errorf("wrapped: %w", &gocql.RequestErrWriteTimeout{WriteType: "CAS"}), true},
		{&gocql.RequestErrCASWriteUnknown{}, true},
		{&gocql.RequestErrWriteTimeout{WriteType: "SIMPLE"}, false},
		{errors.New("boom"), false},
		{context.Canceled, false},
	} {
		if got := ambiguous(tc.err); got != tc.want {
			t.Errorf("ambiguous(%v) = %v, want %v", tc.err, got, tc.want)
		}
	}
}

func TestBackoffCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := backoff(ctx, lwtAttempts); !errors.Is(err, context.Canceled) {
		t.Fatalf("backoff on a canceled ctx = %v, want Canceled", err)
	}
}

func newID(t *testing.T) UUID {
	id, err := NewID()
	if err != nil {
		t.Fatal(err)
	}
	return id
}

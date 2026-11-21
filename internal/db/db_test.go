package db

import (
	"context"
	"os"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestMem(t *testing.T) { testDB(t, NewMem()) }

func TestCassandra(t *testing.T) {
	hosts := os.Getenv("FILESCALE_CASSANDRA")
	if hosts == "" {
		t.Skip("set FILESCALE_CASSANDRA=host1,host2 to run against Cassandra")
	}
	c, err := Connect(strings.Split(hosts, ","), "filescale_test", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	testDB(t, c)
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

func newID(t *testing.T) UUID {
	id, err := NewID()
	if err != nil {
		t.Fatal(err)
	}
	return id
}

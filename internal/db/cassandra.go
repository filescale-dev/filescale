package db

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"time"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

var schema = []string{ // Cassandra schema for the filescale database
	`CREATE KEYSPACE IF NOT EXISTS %[1]s WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': %[2]d}`,
	`CREATE TABLE IF NOT EXISTS %[1]s.files (name text PRIMARY KEY, file_id uuid, size bigint, blake3 blob, content_type text, data_shards int, parity_shards int, stores list<uuid>, parts frozen<list<uuid>>, part_size bigint)`,
	`CREATE TABLE IF NOT EXISTS %[1]s.shards_by_store (store_id uuid, bucket int, file_id uuid, shard_index int, name text, PRIMARY KEY ((store_id, bucket), file_id))`,
	`CREATE TABLE IF NOT EXISTS %[1]s.stores (store_id uuid PRIMARY KEY, addr text, state text, free bigint, last_seen timestamp)`,
	// TTL = UploadTTL, only ever expired, never deleted, so tombstones can go after an hour
	`CREATE TABLE IF NOT EXISTS %[1]s.uploads (name text, upload_id uuid, part int, part_id uuid, size bigint, total bigint, hash_state blob, content_type text, data_shards int, parity_shards int, stores frozen<list<uuid>>, PRIMARY KEY ((name, upload_id), part)) WITH default_time_to_live = 82800 AND gc_grace_seconds = 3600`,
}

type Cassandra struct{ s *gocql.Session }

func Connect(hosts []string, keyspace string, replication int) (*Cassandra, error) {
	c := gocql.NewCluster(hosts...)
	s := connect(c)
	err := createSchema(s, keyspace, replication)
	s.Close()
	if err != nil {
		return nil, err
	}
	c.Keyspace = keyspace
	c.Consistency = gocql.LocalQuorum
	c.SerialConsistency = gocql.LocalSerial
	return &Cassandra{connect(c)}, nil
}

func (c *Cassandra) Close() { c.s.Close() }

func connect(c *gocql.ClusterConfig) *gocql.Session {
	for wait := time.Second; ; wait = min(wait*2, 30*time.Second) {
		s, err := c.CreateSession()
		if err == nil {
			return s
		}
		log.Printf("cassandra %v: %v, retrying in %v", c.Hosts, err, wait)
		time.Sleep(wait)
	}
}

func createSchema(s *gocql.Session, keyspace string, replication int) error {
	for _, stmt := range schema {
		if err := s.Query(fmt.Sprintf(stmt, keyspace, replication)).Exec(); err != nil {
			return err
		}
	}
	// IF NOT EXISTS keeps an older files table, fail here instead of on every GET
	if err := s.Query(fmt.Sprintf(`SELECT parts, blake3 FROM %s.files LIMIT 1`, keyspace)).Exec(); err != nil {
		return fmt.Errorf("keyspace %s is from an older filescale, recreate it: %w", keyspace, err)
	}
	return nil
}

// read is a query at ALL under db.All, else LOCAL_QUORUM.
func (c *Cassandra) read(ctx context.Context, stmt string, args ...any) *gocql.Query {
	q := c.s.Query(stmt, args...)
	if IsAll(ctx) {
		q = q.Consistency(gocql.All)
	}
	return q
}

func (c *Cassandra) File(ctx context.Context, name string) (*File, error) {
	f := File{Name: name}
	err := c.read(ctx, `SELECT file_id, size, blake3, content_type, data_shards, parity_shards, stores, parts, part_size FROM files WHERE name = ?`, name).
		ScanContext(ctx, &f.ID, &f.Size, &f.BLAKE3, &f.ContentType, &f.DataShards, &f.ParityShards, &f.Stores, &f.Parts, &f.PartSize)
	if errors.Is(err, gocql.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &f, nil
}

// PutFile is an LWT: plain writes at one timestamp merge their stores lists.
func (c *Cassandra) PutFile(ctx context.Context, f *File) error {
	for attempt := 0; ; attempt++ {
		ok, err := c.s.Query(`INSERT INTO files (name, file_id, size, blake3, content_type, data_shards, parity_shards, stores, parts, part_size) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS`,
			f.Name, f.ID, f.Size, f.BLAKE3, f.ContentType, f.DataShards, f.ParityShards, f.Stores, f.Parts, f.PartSize).
			MapScanCASContext(ctx, map[string]any{})
		if err == nil && !ok {
			ok, err = c.s.Query(`UPDATE files SET file_id = ?, size = ?, blake3 = ?, content_type = ?, data_shards = ?, parity_shards = ?, stores = ?, parts = ?, part_size = ? WHERE name = ? IF EXISTS`,
				f.ID, f.Size, f.BLAKE3, f.ContentType, f.DataShards, f.ParityShards, f.Stores, f.Parts, f.PartSize, f.Name).
				MapScanCASContext(ctx, map[string]any{})
		}
		if err != nil {
			if ambiguous(err) {
				if id, found, rerr := c.serialFileID(ctx, f.Name); rerr == nil && found && id == f.ID {
					return nil
				}
			}
			return err
		}
		if ok {
			return nil
		}
		if attempt+1 == lwtAttempts {
			return fmt.Errorf("files row %s kept changing", f.Name)
		}
		if err := backoff(ctx, attempt); err != nil {
			return err
		}
	}
}

func (c *Cassandra) PutPart(ctx context.Context, p UploadPart) error {
	return c.s.Query(`INSERT INTO uploads (name, upload_id, part, part_id, size, total, hash_state, content_type, data_shards, parity_shards, stores) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		p.Name, p.UploadID, p.Index, p.ID, p.Size, p.Total, p.HashState, p.ContentType, p.DataShards, p.ParityShards, p.Stores).ExecContext(ctx)
}

func (c *Cassandra) UploadParts(ctx context.Context, name string, upload UUID) ([]UploadPart, error) {
	iter := c.s.Query(`SELECT part, part_id, size, total, hash_state, content_type, data_shards, parity_shards, stores, WRITETIME(part_id) FROM uploads WHERE name = ? AND upload_id = ?`,
		name, upload).IterContext(ctx)
	var out []UploadPart
	for {
		p := UploadPart{Name: name, UploadID: upload}
		var written int64
		if !iter.Scan(&p.Index, &p.ID, &p.Size, &p.Total, &p.HashState, &p.ContentType, &p.DataShards, &p.ParityShards, &p.Stores, &written) {
			break
		}
		p.Written = time.UnixMicro(written)
		out = append(out, p)
	}
	return out, iter.Close()
}

// DeleteFile is never retried: a resend could delete a newer upload.
func (c *Cassandra) DeleteFile(ctx context.Context, name string) error {
	_, err := c.s.Query(`DELETE FROM files WHERE name = ? IF EXISTS`, name).MapScanCASContext(ctx, map[string]any{})
	if err != nil && ambiguous(err) {
		if _, found, rerr := c.serialFileID(ctx, name); rerr == nil && !found {
			return nil
		}
	}
	return err
}

func (c *Cassandra) SetShardStore(ctx context.Context, name string, fileID UUID, shard int, store UUID) (bool, error) {
	for attempt := 0; ; attempt++ {
		var cur UUID
		ok, err := c.s.Query(`UPDATE files SET stores[?] = ? WHERE name = ? IF file_id = ?`, shard, store, name, fileID).ScanCASContext(ctx, &cur)
		if err == nil || !ambiguous(err) || attempt+1 == lwtAttempts {
			return ok, err
		}
		if err := backoff(ctx, attempt); err != nil {
			return false, err
		}
	}
}

func (c *Cassandra) serialFileID(ctx context.Context, name string) (id UUID, found bool, err error) {
	err = c.s.Query(`SELECT file_id FROM files WHERE name = ?`, name).Consistency(gocql.LocalSerial).ScanContext(ctx, &id)
	if errors.Is(err, gocql.ErrNotFound) {
		return id, false, nil
	}
	return id, err == nil, err
}

const lwtAttempts = 8

func ambiguous(err error) bool {
	var wt *gocql.RequestErrWriteTimeout
	if errors.As(err, &wt) {
		return wt.WriteType == "CAS"
	}
	var unk *gocql.RequestErrCASWriteUnknown
	return errors.As(err, &unk)
}

func backoff(ctx context.Context, attempt int) error {
	d := min(5*time.Millisecond<<attempt, 200*time.Millisecond)
	t := time.NewTimer(d/2 + rand.N(d))
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func (c *Cassandra) PutShard(ctx context.Context, s Shard) error {
	return c.s.Query(`INSERT INTO shards_by_store (store_id, bucket, file_id, shard_index, name) VALUES (?, ?, ?, ?, ?)`,
		s.StoreID, Bucket(s.FileID), s.FileID, s.Index, s.Name).ExecContext(ctx)
}

func (c *Cassandra) DeleteShard(ctx context.Context, storeID, fileID UUID) error {
	return c.s.Query(`DELETE FROM shards_by_store WHERE store_id = ? AND bucket = ? AND file_id = ?`,
		storeID, Bucket(fileID), fileID).ExecContext(ctx)
}

func (c *Cassandra) Shards(ctx context.Context, storeID UUID, bucket int) ([]Shard, error) {
	iter := c.read(ctx, `SELECT file_id, shard_index, name, WRITETIME(name) FROM shards_by_store WHERE store_id = ? AND bucket = ?`,
		storeID, bucket).IterContext(ctx)
	var out []Shard
	s := Shard{StoreID: storeID}
	var written int64
	for iter.Scan(&s.FileID, &s.Index, &s.Name, &written) {
		s.Written = time.UnixMicro(written)
		out = append(out, s)
	}
	return out, iter.Close()
}

func (c *Cassandra) Stores(ctx context.Context) ([]Store, error) {
	iter := c.s.Query(`SELECT store_id, addr, state, free, last_seen FROM stores`).IterContext(ctx)
	var out []Store
	var s Store
	for iter.Scan(&s.ID, &s.Addr, &s.State, &s.Free, &s.LastSeen) {
		out = append(out, s)
	}
	return out, iter.Close()
}

func (c *Cassandra) Heartbeat(ctx context.Context, s Store) error {
	return c.s.Query(`UPDATE stores SET addr = ?, free = ?, last_seen = ? WHERE store_id = ?`,
		s.Addr, s.Free, s.LastSeen, s.ID).ExecContext(ctx)
}

func (c *Cassandra) DeleteStore(ctx context.Context, id UUID) error {
	return c.s.Query(`DELETE FROM stores WHERE store_id = ?`, id).ExecContext(ctx)
}

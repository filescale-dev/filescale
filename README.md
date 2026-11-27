# filescale

filescale is an opinionated erasure code-first file store using HTTP. Here are all the features (or caveats) one should know:

- **Erasure-coded**: filescale uses [Reed-Solomon erasure coding](https://www.backblaze.com/blog/reed-solomon/), with 8+4 as the default ratio (this can be changed, see the [flags](#flags)). **However**, small files are replicated instead. Existing shards are never rebalanced; they only move when repaired.
- **Resumable uploads**: Resuming is at part boundaries (64 MiB, or file size / 64 for larger files), and unfinished uploads expire after 23h. See the [client API](#client-api) for implementation.
- **Durable writes**: An upload only succeeds after every data and parity shard has been written to its store. If a server dies mid-upload, the request fails and you can retry the same PUT (resumable uploads keep their finished parts).
- **Self-healing**: If a store stays down for `-repairAfter` (24h), its shards are rebuilt on other stores. Stores also scrub their shards over `-scrubPass` (672h / 28d) with `-scrubGap` (24h) between passes and remove deleted or abandoned data after 24h.
- **Stateless serves**: Any serve can upload, resume, download, or repair. Put them behind DNS round robin or a load balancer for more bandwidth.
- **Flat names**: The URL path is the key. There is no listing or versioning. `PUT` overwrites, `DELETE` removes, and downloads are attachments by default.
- **Plain HTTP**: No TLS or user accounts, only an optional bearer token on the client API and optional HTTP Basic auth on `-metricsPort`. Keep serves, stores, and Cassandra on a private network or put them behind a TLS proxy.

Heavy inspiration from Pixeldrain's [pixelstore](https://www.patreon.com/pixeldrain/posts/erasure-coding-72493411) for design choices and implementation.

## Contents

- [Acknowledgements](#acknowledgements)
- [Requirements](#requirements)
- [Test it out](#test-it-out)
- [Modes](#modes)
- [Flags](#flags)
- [Build](#build)
- [Client API](#client-api)
  - [`PUT /{name}`](#put-name)
  - [`GET /{name}`](#get-name)
  - [`HEAD /{name}`](#head-name)
  - [`DELETE /{name}`](#delete-name)
  - [`PUT /{name}?upload=<uuid>`](#put-nameuploaduuid)
  - [`HEAD /{name}?upload=<uuid>`](#head-nameuploaduuid)
  - [`GET /`](#get-)
- [Deployment recommendations and info](#deployment-recommendations-and-info)
- [Operations](#operations)
- [Metrics](#metrics)
- [Tests](#tests)
- [Code](#code)

## Acknowledgements
This project was written with heavy AI use, and validated by a human.
- Pixeldrain's [pixelstore](https://www.patreon.com/pixeldrain/posts/erasure-coding-72493411) (Not associated with this project) - design choices and implementation
- [klauspost/reedsolomon](https://github.com/klauspost/reedsolomon) - underlying library used for erasure coding
- [hashicorp/memberlist](https://github.com/hashicorp/memberlist) - peer discovery
- [Fornaxian/zerodown](https://github.com/Fornaxian/zerodown) - inspiration for zero-downtime rolling restarts

## Requirements
- Linux, Go 1.25+ to build
- Cassandra 4.0+
- At least data+parity store servers

## Test it out
Assuming an 8+4 shard configuration, 12 stores on different ports can count as 12 "servers" on a single host.

```sh
docker run -d --name cassandra -p 9042:9042 cassandra:5.0

# start the serve first so it creates the schema
./filescale serve -ip 127.0.0.1 &
until curl -sf http://127.0.0.1:8080/; do sleep 2; done

for i in $(seq 1 12); do
  ./filescale store -ip 127.0.0.1 -port $((8100+i)) -gossipPort $((7950+i)) \
    -dir /tmp/filescale/$i -peers 127.0.0.1:7946 &
done
```

The serve picks up new stores within 30s. Then:
```sh
curl -T cat.jpg http://127.0.0.1:8080/photos/cat.jpg
curl -o cat.jpg http://127.0.0.1:8080/photos/cat.jpg
curl -X DELETE http://127.0.0.1:8080/photos/cat.jpg
```

## Modes
- **serve**: file gateway that encodes uploads, decodes downloads, and repairs shards of offline stores.
- **store**: keeps shards on user defined directories, scrubs shards and cleans up deleted ones.

## Flags

Run `filescale <command> -h` to see all flags.

| Both | Default | Description |
|---|---|---|
| `-ip` | first private IP | address to bind and advertise |
| `-gossipPort` | 7946 | memberlist port |
| `-peers` | | comma-separated memberlist seeds (`host:port,...`) |
| `-cassandra` | 127.0.0.1 | Cassandra contact points |
| `-keyspace` | filescale | Cassandra keyspace |
| `-replication` | 1 | keyspace replication factor (applied at creation only) |
| `-secret` | `$FILESCALE_SECRET` (off if unset) | shared cluster secret |
| `-metricsPort` | off | port for Prometheus `/metrics` |
| `-metricsAuth` | `$FILESCALE_METRICS_AUTH` (no auth if unset) | HTTP Basic auth (`user:pass`) for `-metricsPort` |
| `-drainWait` | 30s | how long shutdown waits for in-flight requests |

| serve | Default | Description |
|---|---|---|
| `-port` | 8080 | client HTTP port |
| `-dataShards`, `-parityShards` | 8, 4 | shard ratio for new uploads (existing files keep their original ratio) |
| `-smallFileSize` | d/p × 512 KiB | files smaller than this are replicated instead of erasure-coded |
| `-slowRead` | 500ms | how long to wait on a slow store before reading parity (~50ms for NVMe) |
| `-repairAfter` | 24h | rebuild a store's shards after it has been down this long |
| `-noRepair` | off | disable background repairs on this serve (other serves handle its buckets) |
| `-token` | `$FILESCALE_TOKEN` (no auth if unset) | bearer token for clients |
| `-tokenFor` | both | require `-token` for `upload` or `download` only |
| `-only` | both | serve only `upload` or `download` requests |
| `-allowInline` | off | allow `?response-content-disposition=inline...` (only use behind a proxy that already serves inline content) |

| store | Default | Description |
|---|---|---|
| `-port` | 8081 | shard traffic port |
| `-dir` | ./data | comma-separated shard directories (one `store_id` each) |
| `-minFreeSpace` | 5 | percent of each disk reserved from uploads (repairs can still use it) |
| `-scrubPass` | 672h | duration of one scrub pass (28 days) |
| `-scrubGap` | 24h | pause between scrub passes |

## Build

```sh
go build -o filescale .
```

## Client API
### `PUT /{name}`

```http
PUT /{name}
Content-Length: <bytes>

<file contents>

201 Created
ETag: "<blake3 hex>"
```

Uploads a file, overwriting any existing one. `Content-Length` is required. An optional
`X-Blake3: <hex>` header is verified against the uploaded data (returns 400 on mismatch).

### `GET /{name}`

```http
GET /{name}

200 OK
Content-Type: <type>
Content-Length: <bytes>
Content-Disposition: attachment
ETag: "<blake3 hex>"
```

Returns the file. Supports `Range`, `If-None-Match`, and `If-Range`.
`?response-content-disposition=attachment; filename=...` sets the download filename
(`inline` is also allowed with `-allowInline`).

### `HEAD /{name}`

```http
HEAD /{name}

200 OK
Content-Type: <type>
Content-Length: <bytes>
Content-Disposition: attachment
ETag: "<blake3 hex>"
```

Returns the same headers as `GET` without contacting any store.

### `DELETE /{name}`

```http
DELETE /{name}

204 No Content
```

Deletes the file. Returns 404 if it doesn't exist.

### `PUT /{name}?upload=<uuid>`

```http
PUT /{name}?upload=<uuid>
Content-Length: <bytes>

<file contents>

201 Created
ETag: "<blake3 hex>"
```

Resumable upload using a client-generated UUID. Send the whole file initially, or resume an
interrupted upload starting at `Upload-Offset`:

```http
PUT /{name}?upload=<uuid>
Content-Range: bytes <offset>-<size-1>/<size>
Content-Length: <size-offset>

<file contents, sliced>

201 Created
ETag: "<blake3 hex>"
```

### `HEAD /{name}?upload=<uuid>`

```http
HEAD /{name}?upload=<uuid>

200 OK
Upload-Offset: <bytes>
Upload-Length: <bytes>
```

`Upload-Offset` is how many bytes have been stored so far. Returns 404 if no upload exists for that UUID.

### `GET /`

```http
GET /

200 OK

ok
```

Health check for load balancers. Returns 503 when the serve can't reach Cassandra.

Uploads are idempotent, so on any failure you can retry the same PUT. Downloads default to
`Content-Disposition: attachment` with `X-Content-Type-Options: nosniff` so uploaded HTML
files won't render in the browser (unless `-allowInline` is enabled). There is no directory listing.

Resuming an interrupted upload:

```sh
id=$(uuidgen)
curl -T big.iso "http://serve:8080/big.iso?upload=$id"     # interrupted
off=$(curl -sI "http://serve:8080/big.iso?upload=$id" | awk -F': ' 'tolower($1)=="upload-offset" {print $2+0}')
size=$(stat -c %s big.iso)
tail -c +$((off+1)) big.iso > rest
curl -T rest -H "Content-Range: bytes $off-$((size-1))/$size" "http://serve:8080/big.iso?upload=$id"
```

Files up to 64 MiB fit in a single part and are re-uploaded from the start. Unfinished uploads expire after 23h.

## Deployment recommendations and info

- **Flags**: Every node gets the same `-peers` and `-secret`.
  - Ports to open:
    - 7946 TCP and UDP on every serve and store (memberlist gossip).
    - 8081 (store `-port`) from serves (shard reads, writes, and repairs).
    - 8080 (serve `-port`) from clients or the load balancer.
    - `-metricsPort`, if set, from Prometheus.
    - 9042 to Cassandra from every node (or whatever port is in `-cassandra`).
  - `-peers` is only a seed list (`host:port,...`). Gossip discovers the rest, so startup order doesn't matter.
  - The secret encrypts gossip and authenticates requests between serves and stores. A node without it can't join the cluster, and shard requests without it are rejected.
  - `FILESCALE_SECRET`, `FILESCALE_TOKEN`, and `FILESCALE_METRICS_AUTH` can be set in the environment instead of passing `-secret`, `-token`, and `-metricsAuth`.
- **Cassandra**: Every node gets `-cassandra host1,host2,...`.
  - Use `-replication 3` on a real cluster (3+ servers)
    - this flag is used on first start only!
  - On a brand new cluster, start one node first and let it create the database schema.
  - If Cassandra is unreachable, the node retries with backoff (1s up to 30s) instead of exiting.
- **Stores**: Run one `filescale store` per server and give it every disk: `-dir /mnt/d1,/mnt/d2,...`.
  - Each directory has its own `store_id`, i.e. its own repository of shards and its own entry in repair.
    - Don't leave a disk out, and don't put a shard directory under another one.
  - Use `-minFreeSpace` (default 5%) to bound free space. (Free space is also needed for repairs, do not set to 0%)
- **Serves**: Serves are stateless, so any serve can upload, resume, download, and repair any file.
  - Deploy as much serves as you want, and feel free to put them behind a load balancer (`/` can be used as a healthcheck, see [`GET /`](#get-))
  - `-token` protects the whole client API.
  - `-tokenFor upload` leaves downloads open (authenticated write, public read).
  - `-only upload|download` restricts a serve to uploads only or downloads only.
  - `-noRepair` disables background repairs on a serve (its buckets are split across the remaining serves; keep at least one serve running repairs).
- **Plain HTTP**: There is no TLS. `-token` and `-metricsAuth` stop unauthenticated requests, but traffic is still unencrypted.
  - Keep serves, stores, and metrics on a private network. `-metricsPort` has no auth unless `-metricsAuth` is set, and its labels include store addresses and disk paths.
- **Store disks**: Use XFS (or ext4) mounted with `noatime`.
  - Set `read_ahead_kb` to 1-4 MiB on HDDs (the default is 128 KiB).
  - Keep `vm.dirty_bytes` low enough that dirty pages flush within a minute under sustained writes.
  - Use `mq-deadline` (default on most HDDs) so writeback doesn't starve reads, or try `bfq` if read latency degrades during uploads.
- **Upgrades (rolling restarts)**: Upgrade stores first, then serves.
  - Atomically replace the binary (`install -m 755 filescale.new /usr/local/bin/filescale` or `mv`; never `cp` over a running binary) and send `SIGHUP` to the parent (`kill -HUP <pid>` or `systemctl reload`).
  - Changing flags requires a full restart.
  - Under systemd, use `Restart=on-failure`, `KillMode=control-group`, `KillSignal=SIGINT`, and `ExecReload=/bin/kill -HUP $MAINPID` (the parent exits non-zero if the live child crashes so systemd restarts it).
- **Shutdown**: `SIGTERM` or `SIGINT` (Ctrl-C) leaves the memberlist immediately so peers stop routing traffic to the node.
  - In-flight requests have up to `-drainWait` (default 30s) to finish.
  - Sending a second signal to the child exits immediately.

## Operations

Stores are managed in the `stores` table:

```sql
-- maintenance: no uploads and no repair, even if offline past -repairAfter
UPDATE stores SET state = 'maintenance' WHERE store_id IN (...);
UPDATE stores SET state = 'up' WHERE store_id IN (...);

-- read-only: no uploads or repair writes; still serves reads, and still repairs if it goes down
UPDATE stores SET state = 'readonly' WHERE store_id IN (...);
UPDATE stores SET state = 'up' WHERE store_id IN (...);

-- decommission: stop the store (or drop its -dir), then skip the -repairAfter wait
UPDATE stores SET last_seen = '2000-01-01' WHERE store_id = ...;
```

Serves pick up `stores` changes within 30s (upgrade all serves before using `'readonly'`).
Decommission one server at a time; a store is done once its row is gone from the `stores` table.
Each directory's `store_id` is in the startup log and in `<dir>/store_id`.

If a disk has shards but no row in `stores` (wrong `-keyspace` or `-cassandra`, or a decommissioned disk), it is logged and skipped at startup. Clear the directory to reuse it.

## Metrics

`-metricsPort` serves `/metrics` in the Prometheus text format (version 0.0.4). Set `-metricsAuth user:pass` (or `FILESCALE_METRICS_AUTH`) to require HTTP Basic auth.
Every node also exports `process_cpu_seconds_total`, `process_resident_memory_bytes`,
`process_open_fds`, and `go_goroutines`.

Recommended alerts:
- 5xx responses in `filescale_requests_total`
- `filescale_store_up` == 0
- `filescale_disk_failed` == 1
- Rising `filescale_slow_drops_total` or `filescale_put_refused_total` on a store
- `filescale_stores_reloaded_timestamp_seconds` older than 2 minutes
- No repair pass in 2 hours
- No scrub pass in 5 weeks (or longer than `-scrubPass` + `-scrubGap`)

### serve and store

| Metric | Type | Labels | Description |
|---|---|---|---|
| `filescale_requests_total` | counter | `method`, `code` | Requests by method and status code (methods other than GET, HEAD, PUT, or DELETE are grouped as `other`) |
| `filescale_received_bytes_total` | counter | | Request body bytes read |
| `filescale_sent_bytes_total` | counter | | Response body bytes sent |

### serve

| Metric | Type | Labels | Description |
|---|---|---|---|
| `filescale_inflight` | gauge | `op` (`upload`, `download`) | In-flight requests (`upload` is PUT/DELETE, `download` is GET/HEAD) |
| `filescale_parity_stripes_total` | counter | | Download stripes rebuilt from parity |
| `filescale_download_cut_total` | counter | | Downloads aborted after headers because a stripe had fewer than `data_shards` readable shards |
| `filescale_slow_drops_total` | counter | `store` | Downloads that dropped a slow store and read parity instead |
| `filescale_put_refused_total` | counter | `store` | Shard PUTs that failed before sending data (refused, error, or `100 Continue` timeout) |
| `filescale_store_up` | gauge | `store`, `addr` | 1 if the store is alive in memberlist, not in maintenance, and seen in the last 3 minutes |
| `filescale_store_maintenance` | gauge | `store`, `addr` | 1 if the store is in maintenance |
| `filescale_store_readonly` | gauge | `store`, `addr` | 1 if the store is read-only (no uploads or repair writes) |
| `filescale_store_free_bytes` | gauge | `store`, `addr` | Free bytes available for uploads (from the store's last heartbeat) |
| `filescale_store_last_seen_timestamp_seconds` | gauge | `store`, `addr` | Timestamp of the store's last heartbeat |
| `filescale_stores_reloaded_timestamp_seconds` | gauge | | Timestamp of the last `stores` table reload from Cassandra |
| `filescale_repaired_shards_total` | counter | | Shards rebuilt onto another store by repair (omitted with `-noRepair`) |
| `filescale_repair_errors_total` | counter | | Files that failed during repair and will be retried on the next pass (omitted with `-noRepair`) |
| `filescale_repair_last_pass_timestamp_seconds` | gauge | | End timestamp of this serve's last repair pass (0 if none, omitted with `-noRepair`) |
| `filescale_repair_pass_duration_seconds` | gauge | | Duration of the last repair pass (omitted with `-noRepair`) |
| `filescale_repair_failed_buckets` | gauge | | Buckets the last repair pass couldn't read (omitted with `-noRepair`) |

### store

| Metric | Type | Labels | Description |
|---|---|---|---|
| `filescale_disk_free_bytes` | gauge | `store`, `dir` | Free bytes on the disk |
| `filescale_disk_size_bytes` | gauge | `store`, `dir` | Total size of the disk |
| `filescale_disk_failed` | gauge | `store`, `dir` | 1 if the disk failed its last probe (refuses shards and stops heartbeating) |
| `filescale_disk_inflight_bytes` | gauge | `store`, `dir` | Bytes of shard PUTs in progress |
| `filescale_scrub_rebuilt_blocks_total` | counter | `store`, `dir` | Corrupt or missing blocks rebuilt from other shards during scrub |
| `filescale_scrub_missing_shards_total` | counter | `store`, `dir` | Missing shard files rebuilt in full during scrub |
| `filescale_scrub_orphan_rows_total` | counter | `store`, `dir` | Index rows for unreferenced shards removed during scrub |
| `filescale_cleanup_removed_total` | counter | `store`, `dir` | Shard files with no index row removed during cleanup |
| `filescale_scrub_last_pass_timestamp_seconds` | gauge | `store`, `dir` | End timestamp of the last full scrub pass (0 if none) |
| `filescale_cleanup_last_pass_timestamp_seconds` | gauge | `store`, `dir` | End timestamp of the last cleanup pass (0 if none) |

## Tests

On Linux:

```sh
go test -short ./...
FILESCALE_CASSANDRA=host go test ./internal/db
FILESCALE_LOAD=1 go test -run TestLoad -v ./internal/serve       # 1 serve + 12 stores as processes
FILESCALE_REPAIR=1 go test -run TestRepairTime -v ./internal/serve  # see repair_test.go for env vars
FILESCALE_SOAK=http://serve:8080 FILESCALE_SOAK_HOURS=48 \
  go test -run '^TestSoak$' -v -timeout 50h ./internal/serve     # against a running cluster
```

The load tests can use a lot of memory (5-10x more with `-race`). Run them with a memory limit,
e.g. `systemd-run --scope -p MemoryMax=8G ...`.

## Code

| Path | Description |
|---|---|
| `main.go` | flags, startup, shutdown |
| `internal/serve` | client API, upload, download, resume, repair |
| `internal/store` | store API, disks, heartbeat, scrub, cleanup |
| `internal/shard` | store client, block stream, shard reader |
| `internal/erasure` | Reed-Solomon and block layout |
| `internal/db` | Cassandra schema, queries, and an in-memory implementation for tests |
| `internal/cluster` | memberlist, node metadata, round-trip tracking |
| `internal/b3` | saving BLAKE3 state for resumed uploads |
| `internal/metrics` | Prometheus text format |
| `internal/zerodown` | parent supervisor for `SIGHUP` rolling restarts |

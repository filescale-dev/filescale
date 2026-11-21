# filescale

filescale is an opinionated erasure code-first file store using HTTP. Here are all the features (or caveats) one should know:

- **Erasure-coded**: filescale uses [Reed-Solomon erasure coding]((https://www.backblaze.com/blog/reed-solomon/)), with 8+4 as the default ratio (this can be changed, see the [flags](#flags)). **However**, small files are replicated instead. Existing shards are never rebalanced; they only move when repaired.
- **Resumable uploads**: Resuming is at part boundaries (64 MiB, or file size / 64 for larger files), and unfinished uploads expire after 23h. See the [client API](#client-api) for implementation.
- **Durable writes**: An upload is acknowledged only once every data and parity shard has landed on its store. If a server dies mid-upload, the request fails and the same PUT can simply be retried (a resumable upload keeps its finished parts).
- **Self-healing**: Shards of a store that has been down for `-repairAfter` (24h) are rebuilt on other stores. Stores also scrub their shards on a roughly monthly cycle and remove deleted or abandoned data after 24h.
- **Stateless serves**: Any serve can upload, resume, download or repair. Add them behind DNS round robin or a load balancer for more bandwidth.
- **Flat names**: The URL path is the key. There is no listing or versioning: `PUT` overwrites, `DELETE` removes, and downloads are attachments by default.
- **Plain HTTP**: no TLS and no user accounts, just an optional bearer token on the client API. Keep serves, stores and Cassandra on a private network or front them with TLS.

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
- [Fornaxian/zerodown](https://github.com/Fornaxian/zerodown) - zero-downtime rolling restarts

## Requirements
- Linux, Go 1.25+ to build
- Cassandra 4.0+
- At least data+parity store servers.

## Test it out
Assuming an 8+4 shard configuration, 12 stores on different ports can count as 12 "servers" on a single host.

```sh
docker run -d --name cassandra -p 9042:9042 cassandra:5.0

# start the serve alone first, it creates the schema
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

`filescale <command> -h` lists them.

| Both | Default | |
|---|---|---|
| `-ip` | first private IP | address to bind and advertise |
| `-gossipPort` | 7946 | memberlist port |
| `-peers` | | memberlist seeds, `host:port,...` |
| `-cassandra` | 127.0.0.1 | Cassandra contact points |
| `-keyspace` | filescale | |
| `-replication` | 1 | keyspace replication factor, at creation only |
| `-secret` | `$FILESCALE_SECRET`, else off | cluster secret |
| `-metricsPort` | off | Prometheus `/metrics`, no auth |
| `-drainWait` | 30s | how long shutdown waits for requests in flight |

| serve | Default | |
|---|---|---|
| `-port` | 8080 | client HTTP port |
| `-dataShards`, `-parityShards` | 8, 4 | for new uploads, each file keeps its own |
| `-smallFileSize` | d/p × 512 KiB | files under this are copied instead of coded |
| `-slowRead` | 500ms | how long a lagging store may hold up a read, ~50ms for NVMe |
| `-repairAfter` | 24h | rebuild a store's shards once it's been down this long |
| `-token` | `$FILESCALE_TOKEN`, else open | bearer token for clients |
| `-tokenFor` | both | `upload` or `download` only |
| `-only` | both | serve only `upload` or `download` requests |
| `-allowInline` | off | accept `?response-content-disposition=inline...`, only behind a front that already serves inline |

| store | Default | |
|---|---|---|
| `-port` | 8081 | shard traffic port |
| `-dir` | ./data | shard directories, one `store_id` each |
| `-minFreeSpace` | 5 | percent of each disk kept free, repairs may still use it |

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
`X-Blake3: <hex>` header is checked against the file, 400 on mismatch.

### `GET /{name}`

```http
GET /{name}

200 OK
Content-Type: <type>
Content-Length: <bytes>
Content-Disposition: attachment
ETag: "<blake3 hex>"
```

Returns the file. `Range`, `If-None-Match` and `If-Range` work.
`?response-content-disposition=attachment; filename=...` sets the download name
(`inline` too with `-allowInline`).

### `HEAD /{name}`

```http
HEAD /{name}

200 OK
Content-Type: <type>
Content-Length: <bytes>
Content-Disposition: attachment
ETag: "<blake3 hex>"
```

Same headers as `GET`, no store is contacted.

### `DELETE /{name}`

```http
DELETE /{name}

204 No Content
```

404 if the name doesn't exist.

### `PUT /{name}?upload=<uuid>`

```http
PUT /{name}?upload=<uuid>
Content-Length: <bytes>

<file contents>

201 Created
ETag: "<blake3 hex>"
```

Resumable upload; the client makes the UUID. Send the whole file, or resume a cut-off one
with the bytes from `Upload-Offset`:

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

`Upload-Offset` is how much is stored; 404 if nothing.

### `GET /`

```http
GET /

200 OK

ok
```

Health check for load balancers, 503 when the serve can't reach Cassandra.

Uploads are idempotent: on any failure, send the same PUT again. Downloads are always
`Content-Disposition: attachment` with `nosniff`, so an uploaded HTML file never renders on your domain.
There's no listing.

Resuming a cut-off upload:

```sh
id=$(uuidgen)
curl -T big.iso "http://serve:8080/big.iso?upload=$id"     # cut off
off=$(curl -sI "http://serve:8080/big.iso?upload=$id" | awk -F': ' 'tolower($1)=="upload-offset" {print $2+0}')
size=$(stat -c %s big.iso)
tail -c +$((off+1)) big.iso > rest
curl -T rest -H "Content-Range: bytes $off-$((size-1))/$size" "http://serve:8080/big.iso?upload=$id"
```

Files up to 64 MiB are one part and just get sent again. Unfinished uploads expire after 23h.

## Deployment recommendations and info

- **Flags**: Every node gets the same `-peers` and the same `-secret`.
  - Ports to open:
    - 7946 TCP and UDP on every node: memberlist gossip, serves and stores alike.
    - 8081 (store `-port`) from serves: shard reads, writes and repairs.
    - 8080 (serve `-port`) from clients or the load balancer.
    - `-metricsPort`, if set, from Prometheus.
    - 9042 to Cassandra from every node (or the port in `-cassandra`).
  - `-peers` is only a seed list (`host:port,...`); gossip finds the rest, so start order doesn't matter.
  - The secret authenticates serve→store requests. A node without it can neither join
    nor have its shards read or written.
  - `FILESCALE_SECRET` / `FILESCALE_TOKEN` in the environment can stand in for an empty `-secret` / `-token`.
- **Cassandra**: Every node gets `-cassandra host1,host2,...`
  - Use `-replication 3` on a real cluster (3+ servers)
    - this flag is used on first start only!
  - On a brand new cluster, start one node first and let it create the database schema.
  - If Cassandra is unreachable the node retries with backoff
    (1s up to 30s) instead of exiting.
- **Stores**: Run one `filescale store` per server and give it every disk: `-dir /mnt/d1,/mnt/d2,...`.
  - Each directory has its own `store_id`, i.e. its own repository of shards and its own entry in repair.
    - Don't leave a disk out, and don't put a shard directory under another one. 
  - Use `-minFreeSpace` (default 5%) to bound free space. (Free space is also needed for repairs, do not set to 0%)
- **Serves**: A serve holds no state: any serve can upload, resume, download and repair any file.
  - Deploy as much serves as you want, and feel free to put them behind a load balancer (`/` can be used as a healthcheck, see [`GET /`](#get-))
  - `-token` protects the whole client API
  - `-tokenFor upload` leaves downloads open (authenticated write, public read)
  - `-only upload|download` enforces uploads only or downloads only.
- **Plain HTTP**: There is no TLS: the token blocks unauthenticated use, not someone sniffing the network.
  - Keep serves, stores and metrics on a private network. `-metricsPort` has no auth, and its labels include store addresses and disk paths.
- **Store disks**: Use XFS (or ext4) mounted with `noatime`.
  - Use `read_ahead_kb` set to 1-4 MiB on HDDs (the default is 128 KiB)
  - Keep `vm.dirty_bytes` low enough that unflushed data stays under a minute old under sustained writes
  - Use `mq-deadline` (the usual HDD default) as it keeps writeback from starving reads; try `bfq` if read latency still suffers while uploads run.
- **Upgrades (rolling restarts).** Upgrade stores first, then serves. filescale uses [`zerodown`](https://github.com/Fornaxian/zerodown), see the below on how to upgrade without traffic loss.
  - Atomically replace the binary (`install -m 755 filescale.new /usr/local/bin/filescale` or `mv`; never `cp` over a running binary) and send `SIGHUP` to the parent (`kill -HUP <pid>` or `systemctl reload`).
  - Changing flags requires a full restart.
  - Under systemd, use `KillMode=control-group`, `KillSignal=SIGINT`, and `ExecReload=/bin/kill -HUP $MAINPID`.
- **Shutdown.** `SIGTERM` or `SIGINT` (Ctrl-C) leaves the memberlist at once, so peers stop sending to this node immediately.
  - filescale lets requests in flight finish for up to `-drainWait` (default 30s).
  - A second signal to the child exits at once.

## Operations

Stores are managed in the `stores` table:

```sql
-- maintenance: no uploads, no repair, however long it lasts
UPDATE stores SET state = 'maintenance' WHERE store_id IN (...);
UPDATE stores SET state = 'up' WHERE store_id IN (...);

-- decommission: stop the store (or drop its -dir), then skip the -repairAfter wait
UPDATE stores SET last_seen = '2000-01-01' WHERE store_id = ...;
```

A decommissioned store is done when its `stores` row is gone, one server at a time.
The `store_id`s of a store are in its startup log and in `<dir>/store_id`.

A disk with shards but no `stores` row (wrong `-keyspace` or `-cassandra`, or a decommissioned disk) is skipped at startup and logged. Empty it to reuse it.

## Metrics

`-metricsPort` serves `/metrics` in the Prometheus text format (version 0.0.4, no auth).
Every node also exports `process_cpu_seconds_total`, `process_resident_memory_bytes`,
`process_open_fds` and `go_goroutines`.

Alerts worth setting up:
- 5xx in `filescale_requests_total`
- `filescale_store_up` = 0
- `filescale_disk_failed` = 1,
- A store's `filescale_slow_drops_total` or `filescale_put_refused_total` climbing
- `filescale_stores_reloaded_timestamp_seconds` is over 2 minutes old
- no repair pass in 2 hours
- no scrub pass in 5 weeks.

### serve and store

| Metric | Type | Labels | |
|---|---|---|---|
| `filescale_requests_total` | counter | `method`, `code` | Requests by method and response code; method is `other` when it is not GET, HEAD, PUT or DELETE |
| `filescale_received_bytes_total` | counter | | Request body bytes read |
| `filescale_sent_bytes_total` | counter | | Response body bytes sent |

### serve

| Metric | Type | Labels | |
|---|---|---|---|
| `filescale_inflight` | gauge | `op` (`upload`, `download`) | Requests in flight; upload is PUT and DELETE, download GET and HEAD |
| `filescale_parity_stripes_total` | counter | | Download stripes rebuilt from parity |
| `filescale_download_cut_total` | counter | | Downloads cut off after the headers: a stripe had fewer than `data_shards` readable shards |
| `filescale_slow_drops_total` | counter | `store` | Downloads that dropped a slow store and read parity instead |
| `filescale_put_refused_total` | counter | `store` | Shard PUTs a store failed before any data: refused, an error, or no `100 Continue` in time |
| `filescale_store_up` | gauge | `store`, `addr` | 1 if uploads can pick the store: alive in memberlist, not in maintenance, seen in the last 3 minutes |
| `filescale_store_maintenance` | gauge | `store`, `addr` | 1 if the store is in maintenance |
| `filescale_store_free_bytes` | gauge | `store`, `addr` | Bytes left for uploads, from the store's last heartbeat |
| `filescale_store_last_seen_timestamp_seconds` | gauge | `store`, `addr` | The store's last heartbeat |
| `filescale_stores_reloaded_timestamp_seconds` | gauge | | Last time the stores table was read from Cassandra |
| `filescale_repaired_shards_total` | counter | | Shards rebuilt onto another store by repair |
| `filescale_repair_errors_total` | counter | | Files repair failed on, retried in the next pass |
| `filescale_repair_last_pass_timestamp_seconds` | gauge | | End of this serve's last repair pass, 0 = none yet |
| `filescale_repair_pass_duration_seconds` | gauge | | Length of that pass |
| `filescale_repair_failed_buckets` | gauge | | Buckets that pass couldn't read |

### store

| Metric | Type | Labels | |
|---|---|---|---|
| `filescale_disk_free_bytes` | gauge | `store`, `dir` | Free bytes on the disk |
| `filescale_disk_size_bytes` | gauge | `store`, `dir` | Size of the disk |
| `filescale_disk_failed` | gauge | `store`, `dir` | 1 if the disk failed its last probe: it refuses shards and stops heartbeating |
| `filescale_disk_inflight_bytes` | gauge | `store`, `dir` | Bytes of shard PUTs in progress |
| `filescale_scrub_rebuilt_blocks_total` | counter | `store`, `dir` | Bad or missing blocks scrub rebuilt from the other shards |
| `filescale_scrub_missing_shards_total` | counter | `store`, `dir` | Shard files scrub found missing, rebuilt whole |
| `filescale_scrub_orphan_rows_total` | counter | `store`, `dir` | Index rows of shards no file uses, dropped by scrub |
| `filescale_cleanup_removed_total` | counter | `store`, `dir` | Shard files without an index row, removed by cleanup |
| `filescale_scrub_last_pass_timestamp_seconds` | gauge | `store`, `dir` | End of the last whole scrub pass, 0 = none yet |
| `filescale_cleanup_last_pass_timestamp_seconds` | gauge | `store`, `dir` | End of the last cleanup pass, 0 = none yet |

## Tests

On Linux:

```sh
go test -short ./...
FILESCALE_CASSANDRA=host go test ./internal/db
FILESCALE_LOAD=1 go test -run TestLoad -v ./internal/serve       # 1 serve + 12 stores as processes
FILESCALE_REPAIR=1 go test -run TestRepairTime -v ./internal/serve  # see repair_test.go for its env
FILESCALE_SOAK=http://serve:8080 FILESCALE_SOAK_HOURS=48 \
  go test -run '^TestSoak$' -v -timeout 50h ./internal/serve     # against a running cluster
```

The load tests can use a lot of memory, `-race` 5-10x more. Run them under a cap,
e.g. `systemd-run --scope -p MemoryMax=8G ...`.

## Code

| | |
|---|---|
| `main.go` | flags, startup, shutdown |
| `internal/serve` | client API, upload, download, resume, repair |
| `internal/store` | store API, disks, heartbeat, scrub, cleanup |
| `internal/shard` | store client, block stream, shard reader |
| `internal/erasure` | Reed-Solomon and block layout |
| `internal/db` | Cassandra schema and queries, an in-memory version for tests |
| `internal/cluster` | memberlist, node metadata, round-trip tracking |
| `internal/b3` | saving BLAKE3 state for resumed uploads |
| `internal/metrics` | Prometheus text format |
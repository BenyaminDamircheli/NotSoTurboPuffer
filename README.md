# NotSoTurboPuffer

A vector database that stores all of its data, including its indexes, on S3-compatible object storage. Compute nodes hold only caches. You can stop the server, start it on another machine, and it continues after one cold query.

I built this to understand the tradeoffs behind [turbopuffer](https://turbopuffer.com/architecture)'s architecture and to see if I could build something with that architecture that is fast and cheap.

## How it works

Writes go to a write-ahead log. A background compactor folds the log into two indexes. Queries merge both.

```
write:   upsert/delete/patch -> batcher -> WAL file on S3 -> metadata CAS
query:   metadata -> replay recent WAL (exact) + probe ANN index -> merge by id, newest wins
compact: WAL batch -> SPFresh centroid index + inverted index -> one metadata CAS swap
```

These decisions shape the design:

**Safety comes from S3 preconditions, not from locks.** The engine writes every WAL file with `if-none-match`. It replaces the per-namespace `metadata.json` only with `if-match` on its ETag. A writer that loses the race re-reads the remote state, re-applies its delta, and retries. There is no clock-sync assumption anywhere.

**One writer per namespace, by construction.** Concurrent writers on one namespace produce WAL collisions, so all writes for a namespace pass through a single in-process batcher. A request blocks until its batch is durable on S3. The response contains the real WAL key, not a fake ack.

**The store is a stack of three layers.** `CachingStore<CountingStore<S3Store>>`: a write-through disk cache on top, a round-trip counter under it, raw S3 at the bottom. The order matters. The counter sits below the cache, so a cache hit never counts as a round trip. This makes "a cold query costs N round trips" a testable claim instead of a guess.

**The ANN index is SPFresh-style.** The compactor clusters vectors into postings and stores each posting as one S3 object. A query loads centroids, probes the `search_fanout` nearest postings, and scans those. Updates use delta operations and posting splits with LIRE reassignment instead of full rebuilds.

**Deletes and patches survive compaction.** This is where most of the subtle bugs lived. Compaction deletes WAL files, so their tombstones must apply to both indexes first. Otherwise deleted documents reappear in results. The engine keeps patches that target indexed rows as "pending patches". The query path overlays them on index results immediately. The next compaction merges them into both indexes. The benchmark suite checks both behaviors end-to-end on every run.

## Layout

```
crates/NotSoTurboPuffer/   engine: WAL, replay, SPFresh index, inverted index,
                           compactor, store stack, client API
crates/server/             axum HTTP server: handlers/ + models/, no storage logic
benchmarks/                Python benchmark + correctness suite, writes plots
```

## API

| Method | Path | Does |
|---|---|---|
| GET | `/health` | liveness |
| GET | `/v1/namespaces` | list namespaces |
| PUT | `/v1/namespaces/{ns}` | create namespace |
| GET | `/v1/namespaces/{ns}` | metadata (row counts, index state) |
| POST | `/v1/namespaces/{ns}/upsert` | write rows `{id, vector, attributes}` |
| POST | `/v1/namespaces/{ns}/delete` | write delete tombstones `{ids}` |
| POST | `/v1/namespaces/{ns}/patch` | update attributes `{rows: [{id, attributes}]}` |
| POST | `/v1/namespaces/{ns}/query` | vector search `{vector, top_k, filters?}` |
| GET/PUT | `/v1/namespaces/{ns}/schema` | which attributes get indexed for filtering |
| POST | `/v1/namespaces/{ns}/compact?force=true` | queue a compaction (202, fire-and-forget) |

Document ids can be JSON strings or unsigned integers. Attribute filters are exact-match on indexed attributes.

## Running it

You need an S3-compatible endpoint. MinIO, Tigris, and AWS all work.

```bash
export S3_ENDPOINT=https://...      # your endpoint
export S3_REGION=auto
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export S3_BUCKET=my-bucket          # optional, see below

cargo run -p server                 # reads config.toml from the repo root
```

`S3_BUCKET` switches the storage layout. When it is not set, every namespace gets its own bucket. This is convenient for local MinIO. When it is set, all namespaces live in that one bucket as key prefixes. Providers that scope credentials to a single bucket (Railway, for example) need this mode.

Tunables live in `config.toml`: batching windows, compaction thresholds, cache paths, request limits. The `PORT`, `S3_*`, and `LOCAL_CACHE_PATH` env vars override the file.

To deploy on Railway: create a bucket, put its credentials in the service variables, and deploy the repo. The provided `Dockerfile` builds the server. Railway injects `PORT`.

## Benchmarks

```bash
cd benchmarks
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python bench.py --base-url http://localhost:3000
```

The script does four things:

1. Inserts seeded random unit vectors and measures insert throughput.
2. Measures query latency before compaction (exact WAL scan).
3. Forces a compaction and measures cold and warm query latency on the ANN path.
4. Runs six correctness checks against the live server.

The correctness checks confirm that filters return only matching documents, that a deleted document stays gone across a further compaction cycle, and that a patch on a compacted document is visible immediately, survives the next compaction, and becomes filterable. The script exits non-zero if any check fails, so it also works as an integration test. Results land in `results/results.json` plus plots of insert throughput and the latency distributions.

Numbers from a deployment on Railway (sjc) with a Tigris bucket, 2,000 vectors at 128 dims, measured from a laptop on the US west coast:

| | |
|---|---|
| insert throughput | 333 vectors/sec (batches of 250) |
| warm query, over the network | p50 131 ms, p95 224 ms |
| warm query, server colocated with client | p50 3.3 ms |
| true cold query (fresh container, empty caches) | 398 ms, 3.0x the warm median |

The network numbers mostly measure the distance to the datacenter. The server-side warm path runs in single-digit milliseconds. The cold query pays about 270 ms of S3 round trips to load metadata, the index file, and postings. The server then serves those objects from cache until a restart clears it.

## What it costs

The architecture exists because of one price gap: RAM against object storage. Railway prices memory at $0.00000386 per GB-second, which is about $10 per GB-month. Railway buckets cost $0.015 per GB-month and AWS S3 Standard costs $0.023. Object storage is roughly 400 to 650 times cheaper per byte. A memory-resident vector database pays RAM prices for every vector. This design pays RAM prices only for caches.

Prices as of August 2026, from [railway.com/pricing](https://railway.com/pricing), the [Railway buckets docs](https://docs.railway.com/storage-buckets), and [AWS S3 pricing](https://aws.amazon.com/s3/pricing/):

| | Railway buckets | AWS S3 Standard |
|---|---|---|
| storage | $0.015 / GB-month | $0.023 / GB-month |
| reads | free, unlimited | $0.0004 per 1,000 |
| writes | free, unlimited | $0.005 per 1,000 |
| egress | free | $0.09 / GB |

On S3, requests cost money, and the store stack's round-trip counter measures exactly what each operation pays. A cold query costs about 6 GETs: metadata, the index file, and one GET per probed posting. A warm query costs 0. A write batch costs 3 requests (one WAL PUT plus the metadata read and CAS write), amortized across every row in the batch. At S3 rates, one million all-cold queries cost $2.40 in GET fees. With a 95% cache hit rate the same load costs about $0.12. On Railway buckets all requests are free, so only storage is billed.

Storage math: vectors cost dims x 4 bytes, and postings plus indexes roughly double that. One million 768-dim vectors is about 6 GB stored, or $0.09 per month on a Railway bucket. One billion is about 6 TB, or $92 per month. Holding those 6 TB in RAM at $10 per GB-month costs about $61,000 per month.

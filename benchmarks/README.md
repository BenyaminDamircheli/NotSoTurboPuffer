# Benchmarks

Python benchmark suite for the NotSoTurboPuffer HTTP server.

## What it does

1. Generates random unit vectors (seeded, reproducible).
2. Upserts them in batches and measures throughput.
3. Runs single-vector queries and measures latency percentiles.
4. Computes recall@k against an exact brute-force ground truth.
5. Writes `results/results.json` and three plots:
   - `insert_throughput.png` — vectors/sec per batch
   - `query_latency_hist.png` — latency histogram with p50/p95/p99
   - `query_latency_timeline.png` — latency per request over time

## Setup

```bash
cd benchmarks
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

## Run

Against a local server (`cargo run -p server` from the repo root):

```bash
python bench.py
```

Against a deployed instance:

```bash
python bench.py --base-url https://<your-domain> --vectors 2000 --queries 100
```

Useful flags: `--dims`, `--batch-size`, `--top-k`, `--namespace`, `--seed`.

## Notes

- Vectors are unit-normalized; the server default metric is cosine distance.
- Recall is computed on the same dataset the server holds, so unindexed
  (WAL-only) data reads exact and recall stays at 1.0. After compaction
  builds the ANN index, recall reflects the index quality.
- The filtered-query check needs the inverted index, which exists only
  after a compaction cycle; the script reports it as unavailable otherwise.

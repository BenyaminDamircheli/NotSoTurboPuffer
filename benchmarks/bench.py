#!/usr/bin/env python3
"""End-to-end benchmark for NotSoTurboPuffer.

Phases:
  1. Insert random unit vectors and measure throughput.
  2. Query pre-compaction (exact WAL scan) and measure latency + recall.
  3. Force compaction and wait for the ANN + inverted indexes.
  4. Measure the first post-compaction query (index load) as the cold sample,
     then warm query latency + ANN recall.
  5. Post-compaction correctness checks:
     - filtered queries return only documents in the requested category,
     - deleted documents stay gone after another compaction (no resurrection),
     - patches on compacted documents show up immediately and survive the
       next compaction.

Writes results/results.json and plots.

Usage:
    python bench.py --base-url http://localhost:3000
    python bench.py --base-url https://<railway-domain> --vectors 2000
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import numpy as np

from client import PufferClient
from plots import plot_insert_throughput, plot_latency_histogram, plot_latency_timeline

CATEGORIES = [f"cat-{i}" for i in range(5)]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://localhost:3000")
    parser.add_argument("--namespace", default=f"bench-{int(time.time())}")
    parser.add_argument("--dims", type=int, default=128)
    parser.add_argument("--vectors", type=int, default=5000)
    parser.add_argument("--batch-size", type=int, default=250)
    parser.add_argument("--queries", type=int, default=200)
    parser.add_argument("--top-k", type=int, default=10)
    parser.add_argument("--warmup", type=int, default=5, help="queries excluded from stats")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--compaction-timeout", type=int, default=300)
    parser.add_argument("--skip-compaction", action="store_true")
    parser.add_argument("--output-dir", type=Path, default=Path("results"))
    return parser.parse_args()


def category_of(index: int) -> str:
    return CATEGORIES[index % len(CATEGORIES)]


def generate_unit_vectors(count: int, dims: int, rng: np.random.Generator) -> np.ndarray:
    vectors = rng.standard_normal((count, dims)).astype(np.float32)
    norms = np.linalg.norm(vectors, axis=1, keepdims=True)
    return vectors / norms


def build_rows(vectors: np.ndarray, start_index: int) -> list[dict]:
    return [
        {
            "id": f"vec-{start_index + offset}",
            "vector": vector.tolist(),
            "attributes": {
                "category": category_of(start_index + offset),
                "shard": (start_index + offset) % 10,
            },
        }
        for offset, vector in enumerate(vectors)
    ]


def run_inserts(
    client: PufferClient, namespace: str, dataset: np.ndarray, batch_size: int
) -> list[float]:
    """Upserts the dataset in batches. Returns per-batch latencies in seconds."""
    latencies = []
    for start in range(0, len(dataset), batch_size):
        batch = dataset[start : start + batch_size]
        rows = build_rows(batch, start)

        started = time.perf_counter()
        client.upsert(namespace, rows)
        latencies.append(time.perf_counter() - started)

        done = start + len(batch)
        print(f"  upserted {done}/{len(dataset)} ({latencies[-1] * 1000:.0f} ms)")
    return latencies


def run_queries(
    client: PufferClient,
    namespace: str,
    queries: np.ndarray,
    top_k: int,
    filters: dict[str, str] | None = None,
) -> tuple[list[float], list[list[str]]]:
    """Runs one query per row. Returns latencies in ms and result-id lists."""
    latencies_ms = []
    result_ids = []
    for i, query in enumerate(queries):
        started = time.perf_counter()
        response = client.query(namespace, query.tolist(), top_k, filters)
        latencies_ms.append((time.perf_counter() - started) * 1000)
        result_ids.append([r["id"] for r in response["results"]])

        if (i + 1) % 50 == 0:
            print(f"  ran {i + 1}/{len(queries)} queries")
    return latencies_ms, result_ids


def brute_force_ids(dataset: np.ndarray, queries: np.ndarray, top_k: int) -> list[set[str]]:
    """Exact cosine-distance top-k per query, as ground truth for recall."""
    # Vectors are unit-normalized, so cosine distance = 1 - dot product.
    distances = 1.0 - queries @ dataset.T
    top_indices = np.argsort(distances, axis=1)[:, :top_k]
    return [{f"vec-{index}" for index in row} for row in top_indices]


def recall_at_k(server_ids: list[list[str]], truth_ids: list[set[str]], top_k: int) -> float:
    hits = sum(len(set(ids) & truth) for ids, truth in zip(server_ids, truth_ids))
    return hits / (len(truth_ids) * top_k)


def percentiles(latencies_ms: list[float]) -> dict[str, float]:
    p50, p95, p99 = np.percentile(np.array(latencies_ms), [50, 95, 99])
    return {
        "p50_ms": round(float(p50), 2),
        "p95_ms": round(float(p95), 2),
        "p99_ms": round(float(p99), 2),
        "mean_ms": round(float(np.mean(latencies_ms)), 2),
        "max_ms": round(float(np.max(latencies_ms)), 2),
    }


def compact_and_wait(client: PufferClient, namespace: str, timeout_secs: int) -> dict:
    """Forces compaction and polls until every WAL file is folded into the
    indexes. Returns the final metadata. Raises on timeout."""
    client.compact(namespace, force=True)
    deadline = time.time() + timeout_secs

    while time.time() < deadline:
        metadata = client.metadata(namespace)
        if metadata["pending_wal_files"] == 0 and metadata["index_status"] == "up-to-date":
            return metadata
        time.sleep(2)

    raise TimeoutError(f"compaction did not finish within {timeout_secs}s")


def check(name: str, passed: bool, detail: str, failures: list[str]) -> None:
    print(f"  [{'PASS' if passed else 'FAIL'}] {name}: {detail}")
    if not passed:
        failures.append(f"{name}: {detail}")


def run_correctness_checks(
    client: PufferClient,
    namespace: str,
    dataset: np.ndarray,
    top_k: int,
    compaction_timeout: int,
    failures: list[str],
) -> dict[str, str]:
    """Post-compaction correctness: filters, deletes, and patches must behave
    across compaction cycles, not just inside the WAL window."""
    outcomes: dict[str, str] = {}

    # --- Filters: every result must be in the requested category. ---
    filtered = client.query(namespace, dataset[0].tolist(), top_k, {"category": "cat-1"})
    ids = [r["id"] for r in filtered["results"]]
    wrong = [i for i in ids if category_of(int(i.split("-")[1])) != "cat-1"]
    check(
        "filter-correctness",
        len(ids) > 0 and not wrong,
        f"{len(ids)} results, {len(wrong)} outside cat-1",
        failures,
    )
    outcomes["filtered_query"] = f"{len(ids)} results, {len(wrong)} outside category"

    # --- Delete: a deleted document stays gone, including after the next
    # compaction folds the tombstone away (the resurrection case). ---
    victim = "vec-0"
    client.delete(namespace, [victim])
    result = client.query(namespace, dataset[0].tolist(), top_k)
    pre_ids = [r["id"] for r in result["results"]]
    check("delete-masks-doc", victim not in pre_ids, f"{victim} absent pre-compaction", failures)

    # --- Patch: patch a compacted document; it must show immediately. ---
    patched_doc = "vec-1"
    client.patch(namespace, [{"id": patched_doc, "attributes": {"category": "patched-cat"}}])
    result = client.query(namespace, dataset[1].tolist(), top_k)
    patched_row = next((r for r in result["results"] if r["id"] == patched_doc), None)
    check(
        "patch-visible-immediately",
        patched_row is not None and patched_row["attributes"].get("category") == "patched-cat",
        f"{patched_doc} category={patched_row['attributes'].get('category') if patched_row else 'MISSING'}",
        failures,
    )

    # --- Compact again: delete and patch must survive the cycle. ---
    print("  compacting again to fold delete + patch into the indexes...")
    compact_and_wait(client, namespace, compaction_timeout)

    result = client.query(namespace, dataset[0].tolist(), top_k)
    post_ids = [r["id"] for r in result["results"]]
    check(
        "delete-survives-compaction",
        victim not in post_ids,
        f"{victim} absent post-compaction (no resurrection)",
        failures,
    )

    result = client.query(namespace, dataset[1].tolist(), top_k)
    patched_row = next((r for r in result["results"] if r["id"] == patched_doc), None)
    check(
        "patch-survives-compaction",
        patched_row is not None and patched_row["attributes"].get("category") == "patched-cat",
        f"{patched_doc} category={patched_row['attributes'].get('category') if patched_row else 'MISSING'}",
        failures,
    )

    filtered = client.query(namespace, dataset[1].tolist(), top_k, {"category": "patched-cat"})
    filtered_ids = [r["id"] for r in filtered["results"]]
    check(
        "patched-doc-filterable",
        filtered_ids == [patched_doc],
        f"filter category=patched-cat returned {filtered_ids}",
        failures,
    )

    return outcomes


def main() -> None:
    args = parse_args()
    rng = np.random.default_rng(args.seed)
    client = PufferClient(args.base_url, timeout_secs=120.0)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    failures: list[str] = []

    print(f"Target: {args.base_url}, namespace: {args.namespace}")
    client.health()
    client.create_namespace(args.namespace)

    print(f"Generating {args.vectors} random unit vectors ({args.dims} dims)...")
    dataset = generate_unit_vectors(args.vectors, args.dims, rng)
    query_vectors = generate_unit_vectors(args.queries, args.dims, rng)
    truth = brute_force_ids(dataset, query_vectors, args.top_k)

    print("Phase 1: inserts...")
    insert_latencies = run_inserts(client, args.namespace, dataset, args.batch_size)
    insert_seconds = sum(insert_latencies)

    print("Phase 2: pre-compaction queries (exact WAL scan)...")
    wal_latencies, wal_ids = run_queries(
        client, args.namespace, query_vectors[: min(50, args.queries)], args.top_k
    )
    wal_recall = recall_at_k(wal_ids, truth[: len(wal_ids)], args.top_k)

    results: dict = {
        "base_url": args.base_url,
        "namespace": args.namespace,
        "dims": args.dims,
        "vectors": args.vectors,
        "batch_size": args.batch_size,
        "top_k": args.top_k,
        "insert_total_secs": round(insert_seconds, 2),
        "insert_vectors_per_sec": round(args.vectors / insert_seconds, 1),
        "insert_batch_ms": percentiles([latency * 1000 for latency in insert_latencies]),
        "wal_path": {
            "queries": len(wal_latencies),
            "latency_ms": percentiles(wal_latencies[args.warmup :]),
            "recall_at_k": round(wal_recall, 4),
        },
    }

    if args.skip_compaction:
        print("Skipping compaction phases (--skip-compaction).")
    else:
        print("Phase 3: forcing compaction and waiting for indexes...")
        compaction_started = time.perf_counter()
        metadata = compact_and_wait(client, args.namespace, args.compaction_timeout)
        compaction_secs = time.perf_counter() - compaction_started
        print(
            f"  compacted in {compaction_secs:.1f}s "
            f"(indexed_row_count={metadata['indexed_row_count']})"
        )

        print("Phase 4: cold query (index load), then warm queries...")
        cold_started = time.perf_counter()
        client.query(args.namespace, query_vectors[0].tolist(), args.top_k)
        cold_ms = (time.perf_counter() - cold_started) * 1000
        print(f"  cold first query: {cold_ms:.0f} ms")

        ann_latencies, ann_ids = run_queries(
            client, args.namespace, query_vectors, args.top_k
        )
        ann_recall = recall_at_k(ann_ids, truth, args.top_k)

        print("Phase 5: post-compaction correctness checks...")
        outcomes = run_correctness_checks(
            client,
            args.namespace,
            dataset,
            args.top_k,
            args.compaction_timeout,
            failures,
        )

        results["compaction"] = {
            "duration_secs": round(compaction_secs, 1),
            "indexed_row_count": metadata["indexed_row_count"],
        }
        results["ann_path"] = {
            "cold_first_query_ms": round(cold_ms, 1),
            "queries": len(ann_latencies),
            "latency_ms": percentiles(ann_latencies[args.warmup :]),
            "recall_at_k": round(ann_recall, 4),
        }
        results["correctness"] = outcomes

    results["failures"] = failures
    results["metadata"] = client.metadata(args.namespace)

    results_path = args.output_dir / "results.json"
    results_path.write_text(json.dumps(results, indent=2))

    plot_insert_throughput(
        insert_latencies, args.batch_size, args.output_dir / "insert_throughput.png"
    )
    plot_latency_histogram(
        wal_latencies[args.warmup :],
        "Query latency (WAL path, pre-compaction)",
        args.output_dir / "query_latency_wal.png",
    )
    if "ann_path" in results:
        plot_latency_histogram(
            ann_latencies[args.warmup :],
            "Query latency (ANN path, post-compaction)",
            args.output_dir / "query_latency_ann.png",
        )
        plot_latency_timeline(
            ann_latencies,
            "ANN query latency over time (first point ~= cold)",
            args.output_dir / "query_latency_timeline.png",
        )

    print("\n=== Results ===")
    print(f"Insert:   {results['insert_vectors_per_sec']} vectors/sec")
    wal = results["wal_path"]
    print(f"WAL path: p50={wal['latency_ms']['p50_ms']}ms "
          f"p95={wal['latency_ms']['p95_ms']}ms recall={wal['recall_at_k']}")
    if "ann_path" in results:
        ann = results["ann_path"]
        print(f"ANN path: cold={ann['cold_first_query_ms']}ms "
              f"p50={ann['latency_ms']['p50_ms']}ms "
              f"p95={ann['latency_ms']['p95_ms']}ms recall={ann['recall_at_k']}")
    if failures:
        print(f"CORRECTNESS FAILURES ({len(failures)}):")
        for failure in failures:
            print(f"  - {failure}")
    else:
        print("Correctness: all checks passed")
    print(f"Wrote {results_path} and plots to {args.output_dir}/")

    raise SystemExit(1 if failures else 0)


if __name__ == "__main__":
    main()

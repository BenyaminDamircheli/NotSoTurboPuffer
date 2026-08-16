"""Plot helpers for benchmark results. Each function writes one PNG."""

from __future__ import annotations

from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np


def plot_insert_throughput(batch_latencies_secs: list[float], batch_size: int, path: Path) -> None:
    throughput = [batch_size / latency for latency in batch_latencies_secs]

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(range(1, len(throughput) + 1), throughput, marker="o", linewidth=1)
    ax.set_xlabel("Batch number")
    ax.set_ylabel("Vectors / second")
    ax.set_title(f"Insert throughput per batch (batch size {batch_size})")
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)


def plot_latency_histogram(latencies_ms: list[float], title: str, path: Path) -> None:
    latencies = np.array(latencies_ms)
    p50, p95, p99 = np.percentile(latencies, [50, 95, 99])

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.hist(latencies, bins=40, alpha=0.75, edgecolor="black")
    for value, label, color in [(p50, "p50", "green"), (p95, "p95", "orange"), (p99, "p99", "red")]:
        ax.axvline(value, color=color, linestyle="--", label=f"{label} = {value:.1f} ms")
    ax.set_xlabel("Latency (ms)")
    ax.set_ylabel("Count")
    ax.set_title(title)
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)


def plot_latency_timeline(latencies_ms: list[float], title: str, path: Path) -> None:
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(range(1, len(latencies_ms) + 1), latencies_ms, marker=".", linewidth=0.8, alpha=0.8)
    ax.set_xlabel("Request number")
    ax.set_ylabel("Latency (ms)")
    ax.set_title(title)
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)

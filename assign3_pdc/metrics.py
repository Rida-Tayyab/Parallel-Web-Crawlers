"""Crawl performance structures and sampled worker metrics."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict, dataclass
from typing import Iterable

import pandas as pd


@dataclass
class CrawlMetrics:
    """One run of a crawling strategy."""

    strategy: str
    pages_fetched: int
    unique_urls_discovered: int
    errors: int
    duration_sec: float
    throughput_pages_per_sec: float = 0.0

    def __post_init__(self) -> None:
        if self.duration_sec > 0 and self.throughput_pages_per_sec == 0.0:
            object.__setattr__(
                self, "throughput_pages_per_sec", self.pages_fetched / self.duration_sec
            )

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def metrics_to_dataframe(rows: Iterable[CrawlMetrics]) -> pd.DataFrame:
    return pd.DataFrame([m.to_dict() for m in rows])


class MetricsCollector:
    """
    Tracks ``(timestamp, worker_id, pages_done)`` samples (e.g. from a crawler
    scheduler) and derives simple throughput/utilization aggregates.
    """

    def __init__(self, total_pages: int = 100) -> None:
        self._rows: list[tuple[float, int, int]] = []
        self.total_pages = max(1, int(total_pages))

    def record(self, timestamp: float, worker_id: int, pages_done: int) -> None:
        """Append one observation triple."""
        self._rows.append((float(timestamp), int(worker_id), int(pages_done)))

    def _sorted_rows(self) -> list[tuple[float, int, int]]:
        return sorted(self._rows, key=lambda item: item[0])

    def pages_per_second(self, window: float = 5.0) -> float:
        """
        Aggregate pages/sec over ``window`` ending at the latest timestamp.

        ``pages_done`` is treated as a monotonic cumulative counter per worker;
        deltas are summed across workers versus each worker's value just before
        the window anchor.
        """
        rows = self._sorted_rows()
        if not rows:
            return 0.0

        t_anchor = rows[-1][0] - window

        baseline: defaultdict[int, int] = defaultdict(int)
        last_by_worker: defaultdict[int, int] = defaultdict(int)

        for t, w, p in rows:
            pw = int(p)
            if t < t_anchor:
                baseline[w] = max(baseline[w], pw)
            last_by_worker[w] = pw

        total_delta = 0
        for w, hi in last_by_worker.items():
            total_delta += max(0, hi - baseline[w])

        subset = [(t, w, p) for t, w, p in rows if t >= t_anchor]
        if len(subset) >= 2:
            denom = max(subset[-1][0] - subset[0][0], 1e-6)
        else:
            denom = max(window, 1e-6)

        return float(total_delta) / denom

    def worker_utilization(self, worker_id: int) -> float:
        """``latest pages_done`` / ``total_pages``."""
        newest: int | None = None
        for _t, w, p in reversed(self._sorted_rows()):
            if w == worker_id:
                newest = p
                break
        if newest is None:
            return 0.0
        return min(1.0, max(0.0, newest / float(self.total_pages)))

    def to_dataframe(self) -> pd.DataFrame:
        return pd.DataFrame(
            self._sorted_rows(),
            columns=["timestamp", "worker_id", "pages_done"],
        )

    def save_csv(self, path: str = "metrics.csv") -> None:
        """Export snapshots to CSV (``.to_dataframe`` without auxiliary columns)."""
        df = pd.DataFrame(
            self._sorted_rows(),
            columns=["timestamp", "worker_id", "pages_done"],
        )
        df.to_csv(path, index=False)


def build_example_benchmark_csv(path: str = "benchmark_runs.csv") -> None:
    """
    Ship a deterministic toy table covering three threaded-style engines across
    ``num_workers`` so ``visualize`` can run immediately before real benchmarks exist.
    """
    rows = [
        # crawler, num_workers, execution_time_sec, pages_per_second (100-page crawl target)
        ("threaded", 1, 58.5, 1.71),
        ("threaded", 2, 31.2, 3.21),
        ("threaded", 4, 18.9, 5.29),
        ("threaded", 8, 14.1, 7.09),
        ("multiprocessing", 1, 61.3, 1.63),
        ("multiprocessing", 2, 34.8, 2.87),
        ("multiprocessing", 4, 20.7, 4.83),
        ("multiprocessing", 8, 15.9, 6.29),
        ("ray", 1, 55.9, 1.79),
        ("ray", 2, 29.9, 3.34),
        ("ray", 4, 17.8, 5.62),
        ("ray", 8, 13.8, 7.25),
        ("sequential", 1, 52.8, 1.89),
    ]
    pd.DataFrame(
        rows,
        columns=["crawler", "num_workers", "execution_time_sec", "pages_per_second"],
    ).to_csv(path, index=False)

"""Work-stealing frontier, adaptive batch sizing, and load monitoring."""

from __future__ import annotations

import csv
import itertools
import statistics
import threading
import time
from collections import deque
from pathlib import Path
from queue import Empty, PriorityQueue
from typing import Iterable


class WorkStealingQueue:
    """
    Frontier backed by ``queue.PriorityQueue`` plus per-worker assignment lists.

    Smaller *priority* values are serviced first (``PriorityQueue`` min-heap rule).
    """

    def __init__(self, num_workers: int, *, base_batch_size: int = 5) -> None:
        if num_workers < 1:
            raise ValueError("num_workers must be positive")
        self._num_workers = num_workers
        self._pq: PriorityQueue = PriorityQueue()
        self._seq = itertools.count()
        self._lock = threading.Lock()
        self._assignments: dict[int, list[str]] = {i: [] for i in range(num_workers)}
        self._pages_done: dict[int, int] = {i: 0 for i in range(num_workers)}
        self._base_batch_size = max(1, base_batch_size)
        self._last_batch_hint = self._base_batch_size

    def pending(self) -> int:
        """Approximate unfinished work units (PQ + in-flight assignments)."""
        with self._lock:
            return self._pq.qsize() + sum(len(lst) for lst in self._assignments.values())

    def queue_depth(self) -> int:
        with self._lock:
            return self._pq.qsize()

    def _adapt_from_depth(self, depth: int, starter: int) -> int:
        b = max(1, starter)
        if depth > 100:
            b *= 2
        if depth < 10:
            b = max(1, b // 2)
        return max(1, min(b, 256))

    def effective_batch_size(self, *, starter: int | None = None) -> int:
        """Adaptive batch size derived from PQ depth."""
        with self._lock:
            depth = self._pq.qsize()
            seed = self._base_batch_size if starter is None else starter
            b = self._adapt_from_depth(depth, seed)
            self._last_batch_hint = b
            return self._last_batch_hint

    @property
    def last_logged_batch_size(self) -> int:
        return max(1, self._last_batch_hint)

    def push(self, url: str, priority: int = 0) -> None:
        token = next(self._seq)
        self._pq.put((priority, token, url))

    def pop_batch(self, worker_id: int, batch_size: int = 5) -> list[str]:
        taken: list[str] = []
        with self._lock:
            depth = self._pq.qsize()
            want = self._adapt_from_depth(depth, batch_size)
            self._last_batch_hint = want

            while len(taken) < want:
                try:
                    _pri, _tok, url = self._pq.get_nowait()
                except Empty:
                    break
                taken.append(url)
                self._assignments.setdefault(worker_id, []).append(url)

        return taken

    def complete(self, worker_id: int, url: str, *, count_stat: bool = True) -> None:
        """Remove an assignment slot; optionally increment the monitor counter."""
        with self._lock:
            bucket = self._assignments.setdefault(worker_id, [])
            try:
                bucket.remove(url)
            except ValueError:
                pass
            if count_stat:
                self._pages_done[worker_id] = self._pages_done.get(worker_id, 0) + 1

    def snapshot_counts(self) -> dict[int, int]:
        """Per-worker completions observed at ``complete`` time."""
        with self._lock:
            return dict(self._pages_done)

    def steal(self, idle_worker_id: int) -> int:
        """
        Move URLs from the busiest worker (longest assignment list) to *idle_worker_id*.

        Returns the number of stolen URLs (0 when nothing moves).
        """
        with self._lock:
            best_id: int | None = None
            best_len = -1
            for wid, urls in self._assignments.items():
                if wid == idle_worker_id:
                    continue
                ln = len(urls)
                if ln > best_len:
                    best_len = ln
                    best_id = wid

            if best_id is None or best_len <= 1:
                return 0

            donor = self._assignments.setdefault(best_id, [])
            thief = self._assignments.setdefault(idle_worker_id, [])
            steal_n = max(1, len(donor) // 2)
            chunk = donor[-steal_n:]
            del donor[-steal_n:]
            thief.extend(chunk)
            return len(chunk)


class LoadBalancerMonitor(threading.Thread):
    """
    Periodic sampler that logs CSV metrics and optionally triggers steals when
    load imbalance crosses ``stddev > 0.3 * mean`` on per-worker completions.
    """

    def __init__(
        self,
        frontier: WorkStealingQueue,
        *,
        worker_ids: Iterable[int],
        metrics_path: str | Path = "metrics.csv",
        interval_sec: float = 0.5,
    ) -> None:
        super().__init__(daemon=True)
        self._frontier = frontier
        self._worker_ids = list(worker_ids)
        self._metrics_path = Path(metrics_path)
        self._interval_sec = interval_sec
        self._halt = threading.Event()

    def halt(self) -> None:
        self._halt.set()

    def run(self) -> None:
        write_header = not self._metrics_path.exists()
        with self._metrics_path.open("a", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            if write_header:
                writer.writerow(
                    ["timestamp", "worker_id", "pages_done", "queue_size", "batch_size"]
                )

            while not self._halt.is_set():
                time.sleep(self._interval_sec)

                counts = self._frontier.snapshot_counts()
                qs = self._frontier.queue_depth()
                batch_hint = self._frontier.effective_batch_size()
                stamp = time.time()

                vals = [counts.get(wid, 0) for wid in self._worker_ids]
                mean_v = statistics.mean(vals) if vals else 0.0
                std_v = statistics.pstdev(vals) if len(vals) > 1 else 0.0

                if vals and mean_v > 0 and std_v > 0.3 * mean_v:
                    idle_candidate = min(self._worker_ids, key=lambda w: counts.get(w, 0))
                    stolen = self._frontier.steal(idle_candidate)
                    _ = stolen  # could log if needed

                for wid in self._worker_ids:
                    writer.writerow(
                        [
                            stamp,
                            wid,
                            counts.get(wid, 0),
                            qs,
                            batch_hint,
                        ]
                    )

                handle.flush()


class LoadBalancer:
    """
    Round-robin sharding helpers kept for unrelated experiments/demo code.
    """

    def __init__(self, num_workers: int) -> None:
        if num_workers < 1:
            raise ValueError("num_workers must be >= 1")
        self._num_workers = num_workers
        self._rr = 0

    @property
    def num_workers(self) -> int:
        return self._num_workers

    def assign_shard_index(self) -> int:
        idx = self._rr % self._num_workers
        self._rr += 1
        return idx

    def split_urls(self, urls: Iterable[str]) -> list[list[str]]:
        buckets: list[deque[str]] = [deque() for _ in range(self._num_workers)]
        for i, url in enumerate(urls):
            buckets[i % self._num_workers].append(url)
        return [list(b) for b in buckets]

    def next_worker_id(self) -> int:
        return self.assign_shard_index()

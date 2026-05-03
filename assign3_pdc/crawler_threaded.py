"""Multi-threaded BFS crawler with a work-stealing frontier."""

from __future__ import annotations

import threading
import time
from typing import Any
from urllib.parse import urlparse

from crawler_sequential import fetch_page
from load_balancer import LoadBalancerMonitor, WorkStealingQueue
from utils import is_valid_url, normalize_url


def _is_live_page(page: dict[str, Any]) -> bool:
    if page.get("error") is not None:
        return False
    code = page.get("status_code")
    return isinstance(code, int) and code < 400


_enqueue_guard = threading.Lock()
_seen_urls: set[str] = set()


def _reset_frontier_tracking() -> None:
    with _enqueue_guard:
        _seen_urls.clear()


def _enqueue_discovered(
    frontier: WorkStealingQueue,
    *,
    url: str,
    stop_event: threading.Event,
) -> None:
    if stop_event.is_set():
        return
    with _enqueue_guard:
        if stop_event.is_set():
            return
        if url in _seen_urls:
            return
        _seen_urls.add(url)
    frontier.push(url)


class CrawlerWorker(threading.Thread):
    """Batched dequeue via ``WorkStealingQueue``, then ``fetch_page`` + fan-out."""

    def __init__(
        self,
        worker_id: int,
        *,
        seed_host: str,
        frontier: WorkStealingQueue,
        results: list[dict[str, Any]],
        state_lock: threading.Lock,
        worker_counts: dict[int, int],
        stop_event: threading.Event,
        max_pages: int,
    ) -> None:
        super().__init__(daemon=True)
        self._worker_id = worker_id
        self._seed_host = seed_host
        self._frontier = frontier
        self._results = results
        self._state_lock = state_lock
        self._worker_counts = worker_counts
        # Do not name this ``_stop``: ``threading.Thread`` reserves ``_stop`` for ``join()``.
        self._stop_evt = stop_event
        self._max_pages = max_pages

    def run(self) -> None:
        while True:
            if self._stop_evt.is_set() and self._frontier.pending() == 0:
                break

            urls = self._frontier.pop_batch(self._worker_id)
            if not urls:
                if self._stop_evt.is_set():
                    if self._frontier.pending() == 0:
                        break
                time.sleep(0.05)
                continue

            for url in urls:
                tally_stat = False
                try:
                    if self._stop_evt.is_set():
                        continue

                    with self._state_lock:
                        if len(self._results) >= self._max_pages:
                            self._stop_evt.set()
                            continue

                    page = fetch_page(url)

                    if self._stop_evt.is_set():
                        continue

                    if _is_live_page(page):
                        tally_stat = True
                        with self._state_lock:
                            if len(self._results) < self._max_pages:
                                self._results.append(page)
                                self._worker_counts[self._worker_id] += 1
                            if len(self._results) >= self._max_pages:
                                self._stop_evt.set()

                    if self._stop_evt.is_set():
                        continue

                    if _is_live_page(page):
                        for raw_href in page.get("links", []):
                            if self._stop_evt.is_set():
                                break
                            target = normalize_url(url, raw_href)
                            if not target or not is_valid_url(target):
                                continue
                            if urlparse(target).netloc != self._seed_host:
                                continue
                            _enqueue_discovered(
                                self._frontier,
                                url=target,
                                stop_event=self._stop_evt,
                            )
                finally:
                    self._frontier.complete(self._worker_id, url, count_stat=tally_stat)


def crawl_threaded(
    seed_url: str,
    num_workers: int = 8,
    max_pages: int = 100,
    *,
    metrics_csv: str | None = "metrics.csv",
) -> list[dict[str, Any]]:
    """
    Threads pull batched URLs from ``WorkStealingQueue`` while
    ``LoadBalancerMonitor`` records metrics and optionally steers steals.
    """
    _reset_frontier_tracking()
    normalized_seed = normalize_url(seed_url, seed_url) or seed_url.rstrip("/")
    host = urlparse(normalized_seed).netloc

    frontier = WorkStealingQueue(num_workers)

    stop_event = threading.Event()

    results: list[dict[str, Any]] = []
    state_lock = threading.Lock()
    worker_counts: dict[int, int] = {i: 0 for i in range(num_workers)}

    _enqueue_discovered(frontier, url=normalized_seed, stop_event=stop_event)

    monitor = (
        LoadBalancerMonitor(
            frontier,
            worker_ids=list(range(num_workers)),
            metrics_path=metrics_csv,
        )
        if metrics_csv
        else None
    )
    if monitor:
        monitor.start()

    workers = [
        CrawlerWorker(
            worker_id=i,
            seed_host=host,
            frontier=frontier,
            results=results,
            state_lock=state_lock,
            worker_counts=worker_counts,
            stop_event=stop_event,
            max_pages=max_pages,
        )
        for i in range(num_workers)
    ]

    for thread in workers:
        thread.start()

    deadline = time.perf_counter() + max(600.0, num_workers * 120.0)
    idle_cycles = 0
    try:
        while True:
            with state_lock:
                result_len = len(results)
            pend = frontier.pending()

            if result_len >= max_pages:
                stop_event.set()

            if pend == 0:
                idle_cycles += 1
            else:
                idle_cycles = 0

            if stop_event.is_set() and pend == 0:
                break

            if pend == 0 and idle_cycles >= 40:
                stop_event.set()
                break

            if time.perf_counter() > deadline:
                stop_event.set()
                break

            alive = any(t.is_alive() for t in workers)
            if pend == 0 and not alive:
                stop_event.set()
                break

            time.sleep(0.05)
    finally:
        stop_event.set()

    for thread in workers:
        thread.join(timeout=max(120.0, num_workers * 15.0))

    if monitor:
        monitor.halt()
        monitor.join(timeout=2.0)

    crawl_threaded.last_worker_counts = dict(worker_counts)  # type: ignore[attr-defined]
    return results


if __name__ == "__main__":
    benchmark_seed = "https://books.toscrape.com"
    worker_suite = [1, 2, 4, 8]

    print(f"benchmark seed={benchmark_seed!r}, max_pages=100")
    print()
    hdr = f"{'workers':>8} | {'seconds':>10} | {'pages':>5}"
    print(hdr)
    print("-" * len(hdr))

    for num in worker_suite:
        t0 = time.perf_counter()
        pages = crawl_threaded(benchmark_seed, num_workers=num, max_pages=100)
        secs = time.perf_counter() - t0
        print(f"{num:>8} | {secs:>10.2f} | {len(pages):>5}")

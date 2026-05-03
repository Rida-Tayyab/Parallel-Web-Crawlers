"""Multiprocessing web crawler with ``Manager``-backed shared structures."""

from __future__ import annotations

import multiprocessing as mp
import queue
import time
from typing import Any
from urllib.parse import urlparse

from crawler_sequential import fetch_page
from utils import is_valid_url, normalize_url


def _is_live_page(page: dict[str, Any]) -> bool:
    if page.get("error") is not None:
        return False
    code = page.get("status_code")
    return isinstance(code, int) and code < 400


def worker_process(
    worker_id: int,
    url_queue: Any,
    visited: Any,
    results: Any,
    max_pages: int,
    stop_event: Any,
    results_lock: Any,
    visit_lock: Any,
    seed_host: str,
) -> None:
    """
    Module-level worker for pickling under ``spawn``.

    The first six parameters match the coursework spec; ``results_lock``,
    ``visit_lock``, and ``seed_host`` are extra picklable arguments needed to
    coordinate without ``Process(initializer=…)`` (unsupported in some spawn
    setups).
    """
    _ = worker_id
    while True:
        if stop_event.is_set():
            break
        try:
            url = url_queue.get(timeout=1.0)
        except queue.Empty:
            if stop_event.is_set():
                break
            continue

        if stop_event.is_set():
            continue

        with results_lock:
            capped = len(results) >= max_pages
            if capped:
                stop_event.set()

        if capped:
            continue

        page = fetch_page(url)

        if stop_event.is_set():
            continue

        if _is_live_page(page):
            with results_lock:
                if len(results) < max_pages:
                    results.append(dict(page))
                if len(results) >= max_pages:
                    stop_event.set()

        if stop_event.is_set():
            continue

        if not _is_live_page(page):
            continue

        for raw_href in page.get("links", []):
            if stop_event.is_set():
                break
            target = normalize_url(url, raw_href)
            if not target or not is_valid_url(target):
                continue
            if urlparse(target).netloc != seed_host:
                continue
            enqueue_url_locked(url_queue, visited, visit_lock, stop_event, target)


def enqueue_url_locked(
    url_queue: Any,
    visited: Any,
    visit_lock: Any,
    stop_event: Any,
    url: str,
) -> None:
    if stop_event.is_set():
        return
    with visit_lock:
        if stop_event.is_set():
            return
        if url in visited:
            return
        visited[url] = True
    url_queue.put(url)


def _watcher_idle_stop(
    stop_event: Any,
    results: Any,
    url_queue: Any,
    max_pages: int,
) -> None:
    """
    Parent auxiliary process: raises ``stop_event`` when capped or when the
    crawler idles long enough with an apparently empty frontier.
    """
    stall_ticks = 0
    last_sig = (-1, 0)

    while not stop_event.is_set():
        try:
            n_results = len(results)
        except (OSError, ValueError):
            break

        if n_results >= max_pages:
            stop_event.set()
            break

        try:
            qsize_fn = getattr(url_queue, "qsize", None)
            qsz = qsize_fn() if callable(qsize_fn) else -1
        except (AssertionError, NotImplementedError, OSError, ValueError):
            qsz = -1

        idle_queue = qsz == 0
        sig = (n_results, qsz)

        if sig == last_sig and idle_queue:
            stall_ticks += 1
        else:
            stall_ticks = 0
            last_sig = sig

        if stall_ticks >= 120:
            stop_event.set()
            break

        time.sleep(0.1)


def crawl_multiprocessing(
    seed_url: str,
    num_workers: int = 8,
    max_pages: int = 100,
) -> list[dict[str, Any]]:
    """
    Provision shared ``Queue`` / ``list`` / ``dict`` objects through a Manager,
    spawn ``num_workers`` crawl processes, plus a lightweight watchdog for the
    “empty-but-not-finished yet” frontier case.
    """
    ctx = mp.get_context("spawn")
    normalized_seed = normalize_url(seed_url, seed_url) or seed_url.rstrip("/")
    seed_host = urlparse(normalized_seed).netloc

    with mp.Manager() as manager:
        url_queue = manager.Queue()
        results = manager.list()
        visited = manager.dict()
        stop_event = manager.Event()

        results_lock = manager.Lock()
        visit_lock = manager.Lock()

        visited[normalized_seed] = True
        url_queue.put(normalized_seed)

        workers: list[mp.Process] = []
        for wid in range(num_workers):
            proc = ctx.Process(
                target=worker_process,
                args=(
                    wid,
                    url_queue,
                    visited,
                    results,
                    max_pages,
                    stop_event,
                    results_lock,
                    visit_lock,
                    seed_host,
                ),
            )
            proc.start()
            workers.append(proc)

        watchdog = ctx.Process(
            target=_watcher_idle_stop,
            args=(stop_event, results, url_queue, max_pages),
        )
        watchdog.start()

        for proc in workers:
            proc.join()

        stop_event.set()
        watchdog.join(timeout=5)

        frozen = list(results)

    return frozen


if __name__ == "__main__":
    seed = "https://books.toscrape.com"

    hdr = f"{'workers':>8} | {'seconds':>10} | {'pages':>5}"
    print(hdr)
    print("-" * len(hdr))

    for worker_count in [1, 2, 4, 8]:
        t0 = time.perf_counter()
        collected = crawl_multiprocessing(seed, num_workers=worker_count, max_pages=100)
        elapsed = time.perf_counter() - t0
        print(f"{worker_count:>8} | {elapsed:>10.2f} | {len(collected):>5}")

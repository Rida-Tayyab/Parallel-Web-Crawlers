"""Ray actor-based distributed web crawler."""

from __future__ import annotations

import threading
import time
from typing import Any
from urllib.parse import urlparse

from crawler_sequential import fetch_page
from utils import is_valid_url, normalize_url

try:
    import ray
except ImportError:
    ray = None  # type: ignore[misc, assignment]


def _page_ok(page: dict[str, Any]) -> bool:
    if page.get("error") is not None:
        return False
    code = page.get("status_code")
    return isinstance(code, int) and code < 400


if ray is not None:

    @ray.remote
    class MasterActor:
        """Centralized frontier, deduplication set, aggregates, and a page counter."""

        def __init__(self, seed_url: str, max_pages: int, seed_netloc: str) -> None:
            self._max_pages = max_pages
            self._seed_netloc = seed_netloc

            sane = normalize_url(seed_url, seed_url) or seed_url.rstrip("/")
            # Spec: deque semantics implemented with a Python list (`pop(0)`, `append`).
            self._queue: list[str] = [sane]
            self._visited: set[str] = {sane}

            self._results: list[dict[str, Any]] = []
            self._counter: int = 0

        def get_batch(self, n: int = 5) -> list[str]:
            if self._counter >= self._max_pages:
                return []
            out: list[str] = []
            while len(out) < n and self._queue:
                out.append(self._queue.pop(0))
            return out

        def add_results(self, new_links: list[str], page_data: dict[str, Any]) -> None:
            if _page_ok(page_data) and self._counter < self._max_pages:
                self._results.append(dict(page_data))
                self._counter += 1

            if self._counter >= self._max_pages:
                return
            if not _page_ok(page_data):
                return

            for raw in new_links:
                if not isinstance(raw, str):
                    continue
                target = normalize_url(page_data["url"], raw)
                if not target or not is_valid_url(target):
                    continue
                if urlparse(target).netloc != self._seed_netloc:
                    continue
                if target in self._visited:
                    continue
                if self._counter >= self._max_pages:
                    break
                self._visited.add(target)
                self._queue.append(target)

        def get_stats(self) -> tuple[int, int]:
            return len(self._queue), len(self._results)

        def capped(self) -> bool:
            return self._counter >= self._max_pages

        def export_results(self) -> list[dict[str, Any]]:
            return list(self._results)

    @ray.remote
    class WorkerActor:
        """Performs batched network I/O + HTML parsing (via ``fetch_page``)."""

        def crawl_batch(
            self, urls: list[str]
        ) -> tuple[list[dict[str, Any]], list[list[str]]]:
            page_data_list: list[dict[str, Any]] = []
            new_links_list: list[list[str]] = []

            for url in urls:
                page = fetch_page(url)
                page_data_list.append(page)

                anchors: list[str] = []
                links = page.get("links")
                if isinstance(links, list):
                    anchors = [h for h in links if isinstance(h, str)]
                new_links_list.append(anchors)

            return page_data_list, new_links_list


else:
    MasterActor = None  # type: ignore[misc, assignment]
    WorkerActor = None  # type: ignore[misc, assignment]


def _safe_get(ref: Any, *, context: str) -> Any | None:
    try:
        return ray.get(ref)
    except Exception as exc:  # noqa: BLE001
        print(f"[crawl_ray] ray.get failed ({context}): {exc!r}")
        return None


def _drain_pending(master: Any, pending: list[Any], *, context: str) -> list[Any]:
    """Best-effort flush of in-flight ``crawl_batch`` handles."""
    remaining = list(pending)
    while remaining:
        done, remaining = ray.wait(remaining, num_returns=min(4, len(remaining)), timeout=5.0)
        if not done:
            break
        for ref in done:
            payload = _safe_get(ref, context=f"{context} crawl_batch")
            if payload is None:
                continue
            page_data_list, new_links_list = payload
            for page, raw_links in zip(page_data_list, new_links_list):
                _safe_get(
                    master.add_results.remote(raw_links, page),
                    context=f"{context} add_results",
                )
    return remaining


def crawl_ray(seed_url: str, num_workers: int = 8, max_pages: int = 100) -> list[dict[str, Any]]:
    """
    ``ray.init`` + one ``MasterActor`` + ``num_workers`` ``WorkerActor`` instances.

    The driver loop asynchronously issues ``get_batch``/``crawl_batch`` work while a
    lightweight monitor prints pages/sec about every five seconds.
    """
    if ray is None:
        raise ImportError("ray is not installed — install dependencies from requirements.txt.")

    ray.init(ignore_reinit_error=True)

    sane_seed = normalize_url(seed_url, seed_url) or seed_url.rstrip("/")
    seed_netloc = urlparse(sane_seed).netloc

    master = MasterActor.remote(seed_url, max_pages, seed_netloc)
    workers = [WorkerActor.remote() for _ in range(max(1, num_workers))]

    pending: list[Any] = []
    crawl_start = time.perf_counter()

    stop_monitor = threading.Event()

    def _monitor_speed() -> None:
        last_t = crawl_start
        last_r = 0
        while not stop_monitor.is_set():
            time.sleep(5.0)
            stats = _safe_get(master.get_stats.remote(), context="monitor get_stats")
            if stats is None:
                continue
            qsz, rcount = stats
            now = time.perf_counter()
            overall = rcount / max(now - crawl_start, 1e-6)
            window = max(now - last_t, 1e-6)
            delta = max(rcount - last_r, 0)
            window_rate = delta / window
            print(
                f"[speed] ~{overall:.2f} pages/s overall | ~{window_rate:.2f} pages/s (last ~5s) | "
                f"results={rcount} queue={qsz}"
            )
            last_t = now
            last_r = rcount

    monitor_thread = threading.Thread(target=_monitor_speed, daemon=True)
    monitor_thread.start()

    results_out: list[dict[str, Any]] = []

    try:
        while True:
            capped = _safe_get(master.capped.remote(), context="capped probe")
            if capped is True:
                break

            stats = _safe_get(master.get_stats.remote(), context="main get_stats")
            if stats is None:
                time.sleep(0.05)
                continue
            queue_len, result_len = stats

            if result_len >= max_pages:
                break

            filled = False
            while len(pending) < len(workers) and result_len < max_pages:
                urls = _safe_get(master.get_batch.remote(5), context="get_batch")
                if not urls:
                    break
                worker = workers[len(pending) % len(workers)]
                pending.append(worker.crawl_batch.remote(urls))
                filled = True

                fresh = _safe_get(master.get_stats.remote(), context="refresh stats")
                if fresh is None:
                    break
                queue_len, result_len = fresh
                if result_len >= max_pages:
                    break

            if pending:
                done, pending = ray.wait(pending, num_returns=1, timeout=2.0)
                if done:
                    for ref in done:
                        payload = _safe_get(ref, context="driver crawl_batch")
                        if payload is None:
                            continue
                        page_data_list, new_links_list = payload
                        for page, raw_links in zip(page_data_list, new_links_list):
                            _safe_get(
                                master.add_results.remote(raw_links, page),
                                context="driver add_results",
                            )

                    tail = _safe_get(master.get_stats.remote(), context="post-batch stats")
                    if tail is not None and tail[1] >= max_pages:
                        break
                    continue

            # No finished tasks this round.
            probe = _safe_get(master.get_stats.remote(), context="idle stats")
            if probe is None:
                time.sleep(0.05)
                continue

            queue_len, result_len = probe
            if result_len >= max_pages:
                break
            if queue_len == 0 and not pending and not filled:
                break

            time.sleep(0.02)
    finally:
        pending = _drain_pending(master, pending, context="finalize")
        if pending:
            print(f"[crawl_ray] warning: abandoning {len(pending)} in-flight refs after timeouts.")

        stop_monitor.set()
        monitor_thread.join(timeout=2.0)

        exported = _safe_get(master.export_results.remote(), context="export_results")
        if isinstance(exported, list):
            results_out = exported[:max_pages]

        try:
            ray.shutdown(shutdown_ray=True)
        except Exception:
            pass

    return results_out


if __name__ == "__main__":
    demo_seed = "https://books.toscrape.com"
    t0 = time.perf_counter()
    pages = crawl_ray(demo_seed, num_workers=8, max_pages=50)
    wall = time.perf_counter() - t0
    print(f"fetched={len(pages)} pages in {wall:.2f}s")

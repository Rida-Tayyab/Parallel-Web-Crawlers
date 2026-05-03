"""CLI entry point for PDC crawler assignment (sequential, threaded, multiprocessing, Ray, benchmark)."""

from __future__ import annotations

import argparse
import logging
import multiprocessing as mp
import sys
import time
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from crawler_multiprocessing import crawl_multiprocessing
from crawler_sequential import DEAD_URLS_LOG, crawl_sequential
from crawler_threaded import crawl_threaded
from visualize import generate_all_plots

LOG = logging.getLogger(__name__)

_BENCH_WORKERS = [1, 2, 4, 8, 12, 16]
_BENCH_MAX_PAGES = 200


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )


def run_sequential(url: str, workers: int, max_pages: int) -> None:
    _ = workers
    t0 = time.perf_counter()
    pages = crawl_sequential(url, max_pages=max_pages, delay=0.0)
    elapsed = time.perf_counter() - t0
    LOG.info(
        "sequential finished: pages=%s elapsed=%.2fs (%.3f pages/s)",
        len(pages),
        elapsed,
        len(pages) / max(elapsed, 1e-9),
    )


def run_threaded(url: str, workers: int, max_pages: int) -> None:
    t0 = time.perf_counter()
    pages = crawl_threaded(
        url, num_workers=workers, max_pages=max_pages, metrics_csv=None
    )
    elapsed = time.perf_counter() - t0
    LOG.info(
        "threaded finished (%s workers): pages=%s elapsed=%.2fs",
        workers,
        len(pages),
        elapsed,
    )


def run_multiprocessing(url: str, workers: int, max_pages: int) -> None:
    t0 = time.perf_counter()
    pages = crawl_multiprocessing(url, num_workers=workers, max_pages=max_pages)
    elapsed = time.perf_counter() - t0
    LOG.info(
        "multiprocessing finished (%s workers): pages=%s elapsed=%.2fs",
        workers,
        len(pages),
        elapsed,
    )


def run_ray(url: str, workers: int, max_pages: int) -> None:
    try:
        from crawler_ray import crawl_ray
    except ImportError as exc:  # pragma: no cover
        LOG.error("Ray crawler unavailable: %s", exc)
        raise SystemExit(1) from exc

    t0 = time.perf_counter()
    pages = crawl_ray(url, num_workers=workers, max_pages=max_pages)
    elapsed = time.perf_counter() - t0
    LOG.info(
        "Ray finished (%s workers): pages=%s elapsed=%.2fs",
        workers,
        len(pages),
        elapsed,
    )


def _timed_run(
    label: str,
    num_workers: int,
    fn: Callable[..., list[dict[str, Any]]],
) -> dict[str, Any]:
    t0 = time.perf_counter()
    pages = fn()
    elapsed = max(time.perf_counter() - t0, 1e-9)
    n = len(pages)
    LOG.info("[%s workers=%s] %s pages in %.2fs (%.3f p/s)", label, num_workers, n, elapsed, n / elapsed)
    return {
        "crawler": label,
        "num_workers": int(num_workers),
        "execution_time_sec": round(elapsed, 3),
        "pages_per_second": round(n / elapsed, 3),
        "pages_fetched": int(n),
    }


def run_benchmark(url: str) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []

    rows.append(
        _timed_run(
            "sequential",
            1,
            lambda: crawl_sequential(url, max_pages=_BENCH_MAX_PAGES, delay=0.0),
        )
    )

    for w in _BENCH_WORKERS:
        rows.append(
            _timed_run(
                "threaded",
                w,
                lambda ww=w: crawl_threaded(url, num_workers=ww, max_pages=_BENCH_MAX_PAGES, metrics_csv=None),
            )
        )

    for w in _BENCH_WORKERS:
        rows.append(
            _timed_run(
                "multiprocessing",
                w,
                lambda ww=w: crawl_multiprocessing(url, num_workers=ww, max_pages=_BENCH_MAX_PAGES),
            )
        )

    ray_ok = True
    try:
        from crawler_ray import crawl_ray
    except ImportError:
        ray_ok = False
        crawl_ray = None  # type: ignore[misc, assignment]

    if ray_ok and crawl_ray is not None:
        for w in _BENCH_WORKERS:
            rows.append(
                _timed_run(
                    "ray",
                    w,
                    lambda ww=w: crawl_ray(url, num_workers=ww, max_pages=_BENCH_MAX_PAGES),
                )
            )
    else:
        LOG.warning("Skipping Ray timings (Ray not installed/importable).")

    df = pd.DataFrame(rows)
    out_cols = ["crawler", "num_workers", "execution_time_sec", "pages_per_second"]
    out_csv = Path("benchmark_runs.csv")
    df[out_cols].to_csv(out_csv, index=False)
    LOG.info("Wrote %s (%s rows).", out_csv.resolve(), len(df[out_cols]))

    hm_pages = min(120, _BENCH_MAX_PAGES)
    LOG.info("Collecting threaded metrics snapshot for visualize (workers=8, max_pages=%s).", hm_pages)
    crawl_threaded(url, num_workers=8, max_pages=hm_pages, metrics_csv="metrics.csv")

    generate_all_plots(
        metrics_path="metrics.csv",
        benchmark_path=out_csv,
        output_dir="plot_out",
    )

    print()
    disp = df.copy()
    if "pages_fetched" in disp.columns:
        disp = disp.drop(columns=["pages_fetched"])
    print(disp.sort_values(["crawler", "num_workers"]).to_string(index=False))
    print()

    return df


def run_all_tests() -> None:
    """Smoke tests: crawlers fetch enough live pages on books.toscrape.com and dead URLs are logged."""

    configure_logging()

    ok_url = "https://books.toscrape.com"
    min_needed = 10
    crawl_depth = 30

    for name, fn in (
        ("sequential", lambda: crawl_sequential(ok_url, max_pages=crawl_depth, delay=0.0)),
        ("threaded", lambda: crawl_threaded(ok_url, num_workers=4, max_pages=crawl_depth, metrics_csv=None)),
        (
            "multiprocessing",
            lambda: crawl_multiprocessing(ok_url, num_workers=4, max_pages=crawl_depth),
        ),
    ):
        LOG.info("Test run: %s", name)
        items = fn()
        assert (
            len(items) >= min_needed
        ), f"{name}: expected ≥{min_needed} successes, got {len(items)!r}"

    try:
        from crawler_ray import crawl_ray
    except ImportError:
        LOG.warning("Ray skipped in tests (import failed).")
    else:
        items = crawl_ray(ok_url, num_workers=4, max_pages=crawl_depth)
        assert len(items) >= min_needed, f"ray: expected ≥{min_needed}, got {len(items)}"

    log_path = Path(DEAD_URLS_LOG)
    prior = log_path.read_text() if log_path.exists() else ""

    bad_seed = "https://books.toscrape.com/catalogue/not-a-real-slug-xxxxx/index.html"
    crawl_sequential(bad_seed, max_pages=3, delay=0.0)
    appended = log_path.read_text()[len(prior) :]
    combined = prior + appended
    assert log_path.exists(), f"{DEAD_URLS_LOG} was not created"
    assert appended.strip(), (
        "expected dead_urls.log append after crawling a bogus catalogue slug; got no new rows"
    )
    assert ("404" in combined) or ("status_code=" in combined) or ("error=" in combined), (
        f"dead URL section missing typical markers; appended={appended[:400]!r}"
    )

    LOG.info("All tests passed.")


_MODES = {
    "sequential": run_sequential,
    "threaded": run_threaded,
    "multiprocessing": run_multiprocessing,
    "ray": run_ray,
}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Parallel / distributed crawler driver.")
    p.add_argument(
        "--mode",
        choices=list(_MODES) + ["benchmark"],
        default=None,
        help="Which crawler engine to execute or 'benchmark' for the full sweep.",
    )
    p.add_argument(
        "--url",
        default="https://books.toscrape.com",
        help="Seed URL.",
    )
    p.add_argument(
        "--workers",
        type=int,
        default=8,
        help="Parallel worker count where applicable.",
    )
    p.add_argument(
        "--max-pages",
        type=int,
        default=100,
        help="Successful page cap.",
    )
    p.add_argument(
        "--run-tests",
        action="store_true",
        help="Validate crawlers (+ dead URL logging) and exit.",
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    configure_logging()

    if args.run_tests:
        run_all_tests()
        return

    if args.mode is None:
        raise SystemExit("error: specify --mode or pass --run-tests")

    if args.mode == "benchmark":
        run_benchmark(args.url)
        return

    handler = _MODES[args.mode]
    handler(args.url, args.workers, args.max_pages)


if __name__ == "__main__":
    mp.freeze_support()
    main()

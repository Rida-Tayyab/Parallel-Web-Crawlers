"""Sequential BFS web crawler with rate limiting and progress tracking."""

from __future__ import annotations

import time
from collections import deque
from typing import Any
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

from utils import RateLimiter, is_valid_url, normalize_url

DEAD_URLS_LOG = "dead_urls.log"


def fetch_page(url: str, timeout: int | float = 5) -> dict[str, Any]:
    """
    Fetch *url*, parse HTML with lxml, and return a structured result dict.

    On network/parse failures, ``error`` is set and remaining fields reflect
    best-effort extraction.
    """
    out: dict[str, Any] = {
        "url": url,
        "title": "No Title",
        "links": [],
        "status_code": None,
        "error": None,
    }
    try:
        resp = requests.get(url, timeout=timeout)
        out["status_code"] = resp.status_code
        soup = BeautifulSoup(resp.content, "lxml")

        tag = soup.find("title")
        if tag:
            title_text = tag.get_text(strip=True)
            if title_text:
                out["title"] = title_text

        links: list[str] = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href is None:
                continue
            stripped = href.strip()
            if stripped:
                links.append(stripped)
        out["links"] = links
    except Exception as exc:
        out["error"] = str(exc)

    return out


def crawl_sequential(seed_url: str, max_pages: int = 50, delay: float = 0.5) -> list[dict[str, Any]]:
    """
    Breadth-first crawl from *seed_url* until *max_pages* successful responses
    or the frontier empties.

    Dead URLs are appended to ``dead_urls.log``. Returns one ``fetch_page`` dict
    per successfully fetched HTML page eligible for crawling.
    """
    limiter = RateLimiter(delay)
    seed_norm = normalize_url(seed_url, seed_url) or seed_url.rstrip("/")
    host = urlparse(seed_norm).netloc

    frontier: deque[str] = deque([seed_norm])
    in_frontier: set[str] = {seed_norm}
    visited: set[str] = set()
    results: list[dict[str, Any]] = []

    with open(DEAD_URLS_LOG, "a", encoding="utf-8") as dead_log:
        dead_log.write(f"# crawl start seed={seed_norm}\n")

        with tqdm(total=max_pages, desc="Crawl", unit="page") as bar:
            while frontier and len(results) < max_pages:
                url = frontier.popleft()
                in_frontier.discard(url)
                if url in visited:
                    continue
                visited.add(url)

                limiter.wait()
                page = fetch_page(url)

                dead = False
                if page["error"] is not None:
                    dead_log.write(f"{url}\terror={page['error']}\n")
                    dead = True
                elif isinstance(page["status_code"], int) and page["status_code"] >= 400:
                    dead_log.write(f"{url}\tstatus_code={page['status_code']}\n")
                    dead = True

                if dead:
                    continue

                results.append(page)
                bar.update(1)

                for href in page["links"]:
                    nxt = normalize_url(url, href)
                    if not nxt or not is_valid_url(nxt):
                        continue
                    if urlparse(nxt).netloc != host:
                        continue
                    if nxt not in visited and nxt not in in_frontier:
                        frontier.append(nxt)
                        in_frontier.add(nxt)

    return results


if __name__ == "__main__":
    seed = "https://books.toscrape.com"
    t0 = time.perf_counter()
    pages_data = crawl_sequential(seed, max_pages=50, delay=0.5)
    elapsed = time.perf_counter() - t0
    print(f"Total pages crawled: {len(pages_data)}")
    print(f"Total elapsed time: {elapsed:.2f}s")

"""Shared URL helpers and rate limiting for crawlers."""

from __future__ import annotations

import time
from urllib.parse import urldefrag, urljoin, urlparse

# Paths ending with these are treated as direct file downloads, not HTML pages.
_BLOCKED_SUFFIXES = (
    ".pdf",
    ".jpg",
    ".jpeg",
    ".png",
    ".gif",
    ".webp",
    ".svg",
    ".ico",
    ".bmp",
    ".zip",
    ".tar",
    ".gz",
    ".tgz",
    ".rar",
    ".7z",
    ".exe",
    ".dmg",
    ".deb",
    ".rpm",
    ".mp3",
    ".mp4",
    ".mov",
    ".avi",
    ".mkv",
    ".doc",
    ".docx",
    ".xls",
    ".xlsx",
    ".ppt",
    ".pptx",
    ".csv",
    ".json",
    ".xml",
    ".rss",
    ".atom",
)


def normalize_url(base_url: str, href: str | None) -> str | None:
    """Resolve *href* against *base_url* and strip the fragment (#...)."""
    if not href:
        return None
    joined = urljoin(base_url, href.strip())
    without_fragment, _ = urldefrag(joined)
    return without_fragment or None


def is_valid_url(url: str | None) -> bool:
    """Accept only http/https and reject obvious non-HTML asset links."""
    if not url:
        return False
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        return False
    path = parsed.path.lower()
    for suffix in _BLOCKED_SUFFIXES:
        if path.endswith(suffix):
            return False
    return True


class RateLimiter:
    """Enforce a minimum delay between consecutive network calls."""

    def __init__(self, min_interval_sec: float = 0.0) -> None:
        self._min_interval = max(0.0, float(min_interval_sec))
        self._last_call_end: float | None = None

    def wait(self) -> None:
        if self._min_interval <= 0:
            return
        now = time.monotonic()
        if self._last_call_end is not None:
            elapsed = now - self._last_call_end
            if elapsed < self._min_interval:
                time.sleep(self._min_interval - elapsed)
        self._last_call_end = time.monotonic()

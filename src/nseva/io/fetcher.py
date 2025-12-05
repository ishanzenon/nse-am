"""Raw source fetch utilities (FuData Implementation Plan §6.1)."""

from __future__ import annotations

import time
from collections.abc import Mapping
from pathlib import Path
from typing import Optional

import requests

from nseva.util.hashing import sha256sum
from nseva.util.retry import retry

BROWSER_HEADERS: Mapping[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;"
        "q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
}

def fetch_file(
    url: str,
    dest: Path,
    *,
    timeout_seconds: float | None = None,
    retries: int = 0,
    rate_limit_seconds: float | None = None,
    expected_hash: str | None = None,
    manifest_extension: str = ".sha256",
    headers: Mapping[str, str] | None = None,
) -> Path:
    """Download `url` to `dest`, honoring polite access policies.

    Full behavior (rate limiting, hashing, retries) will be implemented in FUT-004.
    """
    if expected_hash:
        expected_hash = expected_hash.lower()

    if not file_needs_refresh(dest, expected_hash=expected_hash, manifest_extension=manifest_extension):
        return dest

    dest.parent.mkdir(parents=True, exist_ok=True)

    def _download() -> Path:
        if rate_limit_seconds:
            time.sleep(rate_limit_seconds)

        merged_headers = dict(BROWSER_HEADERS)
        if headers:
            merged_headers.update(headers)

        response = requests.get(
            url,
            stream=True,
            timeout=timeout_seconds,
            headers=merged_headers,
        )
        if response.status_code == 404:
            raise FileNotFoundError(f"Source not found at {url}")
        response.raise_for_status()

        tmp_path = dest.with_suffix(dest.suffix + ".download")
        with tmp_path.open("wb") as handle:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    handle.write(chunk)
        tmp_path.replace(dest)

        digest = sha256sum(dest)
        manifest_path = dest.with_suffix(dest.suffix + manifest_extension)
        manifest_path.write_text(digest, encoding="utf-8")

        if expected_hash and digest != expected_hash:
            raise ValueError(f"Downloaded hash mismatch for {dest.name}")

        return dest

    attempt_count = max(1, retries)
    backoff = rate_limit_seconds if rate_limit_seconds is not None else 1.0
    return retry(_download, attempts=attempt_count, backoff_seconds=backoff)


def file_needs_refresh(
    dest: Path, *, expected_hash: Optional[str] = None, manifest_extension: str = ".sha256"
) -> bool:
    """Decide whether a download is needed based on presence and hash manifest."""
    manifest = dest.with_suffix(dest.suffix + manifest_extension)

    if not dest.exists():
        return True

    stored_hash: str | None = None
    if manifest.exists():
        stored_hash = manifest.read_text(encoding="utf-8").strip().lower()

    if expected_hash:
        return stored_hash != expected_hash.lower()

    return stored_hash is None


__all__ = ["fetch_file", "file_needs_refresh"]

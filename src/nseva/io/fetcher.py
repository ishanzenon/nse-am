"""Raw source fetch utilities (FuData Implementation Plan §6.1)."""

from __future__ import annotations

from pathlib import Path
from typing import Optional


def fetch_file(
    url: str,
    dest: Path,
    *,
    timeout_seconds: float | None = None,
    retries: int = 0,
    rate_limit_seconds: float | None = None,
    expected_hash: str | None = None,
) -> Path:
    """Download `url` to `dest`, honoring polite access policies.

    Full behavior (rate limiting, hashing, retries) will be implemented in FUT-004.
    """
    raise NotImplementedError("Fetcher logic pending FUT-004.")


def file_needs_refresh(dest: Path, *, expected_hash: Optional[str] = None) -> bool:
    """Placeholder allowing the pipeline to decide whether to download again."""
    raise NotImplementedError("Hash-based cache checks land in FUT-004.")


__all__ = ["fetch_file", "file_needs_refresh"]

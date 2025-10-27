"""Retry helpers for polite network access (Implementation Plan §11)."""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import TypeVar

T = TypeVar("T")


def retry(operation: Callable[[], T], *, attempts: int, backoff_seconds: float) -> T:
    """Execute `operation` with retry semantics."""
    if attempts < 1:
        raise ValueError("attempts must be >= 1")

    last_error: Exception | None = None
    for attempt in range(attempts):
        try:
            return operation()
        except Exception as exc:  # noqa: BLE001 - propagate last error after retries
            last_error = exc
            if attempt == attempts - 1:
                break
            delay = backoff_seconds * (2**attempt)
            time.sleep(delay)
    assert last_error is not None  # for type checkers
    raise last_error


__all__ = ["retry"]

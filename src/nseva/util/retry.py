"""Retry helpers for polite network access (Implementation Plan §11)."""

from __future__ import annotations

from collections.abc import Callable
from typing import TypeVar

T = TypeVar("T")


def retry(operation: Callable[[], T], *, attempts: int, backoff_seconds: float) -> T:
    """Execute `operation` with retry semantics."""
    raise NotImplementedError("Retry helper arrives in FUT-004.")


__all__ = ["retry"]

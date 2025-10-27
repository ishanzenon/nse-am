"""Monthly futures summary builder (Implementation Plan §6.7)."""

from __future__ import annotations

from datetime import date
from typing import Any


def build_futures_summary(symbol: str, expiry: date, *, context: Any) -> Any:
    """Compute W1/W3 summaries and audit fields for `symbol` and `expiry`."""
    raise NotImplementedError("Gold summary builder arrives in FUT-009.")


__all__ = ["build_futures_summary"]

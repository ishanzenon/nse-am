"""Per-date futures feature builder (Implementation Plan §6.6)."""

from __future__ import annotations

from datetime import date
from typing import Any


def build_futures_day(symbol: str, trade_date: date, *, context: Any) -> Any:
    """Produce the gold.futures_day dataset for `symbol` on `trade_date`."""
    raise NotImplementedError("Gold day builder arrives in FUT-008.")


__all__ = ["build_futures_day"]

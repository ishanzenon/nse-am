"""Future hook: rollover analytics scaffolding (Implementation Plan §15)."""

from __future__ import annotations

from datetime import date
from typing import Any


def compute_rollover_metrics(symbol: str, expiry: date, *, context: Any) -> Any:
    """Placeholder for future rollover analytics."""
    raise NotImplementedError("Rollover analytics are out of scope for FuData v1.")


__all__ = ["compute_rollover_metrics"]

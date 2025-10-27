"""Trading calendar utilities (Implementation Plan §6.4)."""

from __future__ import annotations

from datetime import date


def is_trading_day(day: date) -> bool:
    """Return True if `day` is recognised as a trading day.

    Actual calendar logic will be implemented in FUT-007 using observed data.
    """
    raise NotImplementedError("Calendar logic arrives in FUT-007.")


def next_trading_day_after(day: date) -> date:
    """Return the next trading day following `day`."""
    raise NotImplementedError("Calendar navigation arrives in FUT-007.")


__all__ = ["is_trading_day", "next_trading_day_after"]

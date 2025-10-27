"""Expiry chain derivation utilities (Implementation Plan §6.5)."""

from __future__ import annotations

from datetime import date
from typing import Iterable, Sequence


def derive_expiries(symbol: str) -> Sequence[date]:
    """Return expiries observed for `symbol`."""
    raise NotImplementedError("Expiry derivation arrives in FUT-007.")


def windows_for(symbol: str, expiry: date) -> tuple[date, date, date]:
    """Return (W1.start, W3.start, E0) window boundaries for `symbol`."""
    raise NotImplementedError("Window logic arrives in FUT-007.")


def impacted_expiries_for(symbol: str, trade_date: date) -> Iterable[date]:
    """Yield expiries whose windows include `trade_date`."""
    raise NotImplementedError("Impacted expiry detection arrives in FUT-007.")


__all__ = ["derive_expiries", "windows_for", "impacted_expiries_for"]

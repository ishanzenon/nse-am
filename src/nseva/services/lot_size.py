"""Future hook: Permitted lot-size service (Implementation Plan §15)."""

from __future__ import annotations

from datetime import date


def get_lot_size(symbol: str, on_date: date) -> int:
    """Return the lot size for `symbol` on `on_date`.

    In v1 this will defer to bhavcopy-derived values; the permitted lot-size
    upgrade will plug in here once enabled.
    """
    raise NotImplementedError("Lot-size sourcing will be implemented in FUT-005/FUT-006.")


__all__ = ["get_lot_size"]

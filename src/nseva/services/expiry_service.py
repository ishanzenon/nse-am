"""Expiry chain derivation utilities (Implementation Plan §6.5)."""

from __future__ import annotations

import logging
from datetime import date, timedelta
from pathlib import Path
from typing import Iterable, Sequence

import pandas as pd
from dateutil.relativedelta import relativedelta

from nseva.services.calendar import next_trading_day_after
from nseva.util.paths import data_root_from_config

try:
    from nseva.config import load_config
except Exception:  # pragma: no cover - optional for early import phases
    load_config = None  # type: ignore[assignment]

LOGGER = logging.getLogger(__name__)


def _data_root(storage_root: str | Path | None) -> Path:
    if storage_root is not None:
        return data_root_from_config(storage_root)
    if load_config is None:
        return data_root_from_config("./data")
    cfg = load_config()
    return data_root_from_config(cfg.runtime.storage_root)


def derive_expiries(symbol: str, *, storage_root: str | Path | None = None) -> Sequence[date]:
    """Return expiries observed for `symbol` from silver FUTSTK partitions."""

    root = _data_root(storage_root)
    table_root = root / "silver" / "fo_bhavcopy_day"
    if not table_root.exists():
        return []

    expiries: set[date] = set()
    for partition in table_root.glob("date=*"):
        parquet_file = partition / "data.parquet"
        if not parquet_file.exists():
            continue
        frame = pd.read_parquet(parquet_file, columns=["symbol", "expiry_date"])
        matches = frame[frame["symbol"] == symbol]
        if not matches.empty:
            expiries.update(pd.to_datetime(matches["expiry_date"]).dt.date)

    return tuple(sorted(expiries))


def windows_for(
    symbol: str, expiry: date, *, storage_root: str | Path | None = None
) -> tuple[date, date, date]:
    """Return (W1.start, W3.start, E0) window boundaries for `symbol`."""

    expiries = list(derive_expiries(symbol, storage_root=storage_root))
    if expiry not in expiries:
        raise ValueError(f"Expiry {expiry} not observed for symbol {symbol}.")

    idx = expiries.index(expiry)

    def fallback(idx_offset: int, months_back: int) -> date:
        fallback_expiry = expiry - relativedelta(months=months_back)
        LOGGER.warning(
            "Expiry offset %s missing for %s @ %s; using fallback %s",
            idx_offset,
            symbol,
            expiry,
            fallback_expiry,
        )
        return fallback_expiry

    prev1 = expiries[idx - 1] if idx - 1 >= 0 else fallback(1, 1)
    prev3 = expiries[idx - 3] if idx - 3 >= 0 else fallback(3, 3)

    w1_start = next_trading_day_after(prev1, storage_root=storage_root)
    w3_start = next_trading_day_after(prev3, storage_root=storage_root)

    if w3_start > w1_start:
        pass  # expected
    else:
        LOGGER.warning("Overlap start not before primary start for %s @ %s", symbol, expiry)

    return (w1_start, w3_start, expiry)


def impacted_expiries_for(
    symbol: str, trade_date: date, *, storage_root: str | Path | None = None
) -> Iterable[date]:
    """Yield expiries whose windows include `trade_date`."""

    expiries = derive_expiries(symbol, storage_root=storage_root)
    for expiry in expiries:
        w1_start, w3_start, e0 = windows_for(symbol, expiry, storage_root=storage_root)
        if w3_start <= trade_date <= e0:
            yield expiry


__all__ = ["derive_expiries", "windows_for", "impacted_expiries_for"]

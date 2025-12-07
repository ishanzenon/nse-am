"""Trading calendar utilities (Implementation Plan §6.4)."""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

from nseva.io.cache import cache_path_for
from nseva.util.paths import data_root_from_config

try:
    from nseva.config import load_config
except Exception:  # pragma: no cover - optional to avoid import cycles during early boot
    load_config = None  # type: ignore[assignment]


def _data_root(storage_root: str | Path | None) -> Path:
    if storage_root is not None:
        return data_root_from_config(storage_root)
    if load_config is None:
        return data_root_from_config("./data")
    cfg = load_config()
    return data_root_from_config(cfg.runtime.storage_root)


def is_trading_day(day: date, *, storage_root: str | Path | None = None) -> bool:
    """Return True if `day` is recognised as a trading day.

    Uses the presence of raw UDiFF downloads or silver partitions as the
    observed trading calendar.
    """
    root = _data_root(storage_root)
    date_str = day.isoformat()

    # Raw UDiFF zip presence
    raw_path = cache_path_for("fo_udiff", trade_date=date_str, root=root).with_suffix(".zip")
    if raw_path.exists():
        return True

    # Silver partition presence
    silver_part = root / "silver" / "fo_bhavcopy_day" / f"date={date_str}"
    return silver_part.exists() and any(silver_part.iterdir())


def next_trading_day_after(day: date, *, storage_root: str | Path | None = None) -> date:
    """Return the next trading day following `day`."""

    current = day
    for _ in range(366):  # guard against runaway loops
        current = current + timedelta(days=1)
        if is_trading_day(current, storage_root=storage_root):
            return current
    raise ValueError(f"No trading day found within a year after {day.isoformat()}.")


__all__ = ["is_trading_day", "next_trading_day_after"]

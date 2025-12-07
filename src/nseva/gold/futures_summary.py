"""Monthly futures summary builder (Implementation Plan §6.7)."""

from __future__ import annotations

from datetime import date
from math import floor
from pathlib import Path
from typing import Any, Iterable, Optional

import pandas as pd

from nseva.services.expiry_service import windows_for
from nseva.util.paths import data_root_from_config

try:
    from nseva.config import load_config
except Exception:  # pragma: no cover - allow early imports
    load_config = None  # type: ignore[assignment]


def build_futures_summary(
    symbol: str, expiry: date, *, storage_root: str | Path | None = None
) -> pd.DataFrame:
    """Compute W1/W3 summaries and audit fields for `symbol` and `expiry`."""

    root = _data_root(storage_root)
    cfg = load_config() if load_config else None
    summary_scope = (
        cfg.futures.windows.primary.summary_scope if cfg else "W1"
    )  # default W1 per design

    primary_start, overlap_start, end_date = windows_for(symbol, expiry, storage_root=root)
    scope_start = primary_start if summary_scope == "W1" else overlap_start

    gold_df = _load_gold_days(root, symbol, scope_start, end_date)
    if gold_df.empty:
        return pd.DataFrame(columns=_summary_columns())

    max_oi_contracts = int(gold_df["oi_contracts"].max())

    latest_mwpl_row = (
        gold_df.dropna(subset=["mwpl_shares", "lot_size_shares"])
        .sort_values("trade_date")
        .tail(1)
    )
    if latest_mwpl_row.empty:
        max_permitted = pd.NA
        threshold_90 = pd.NA
        mwpl_used = pd.NA
        lot_used = pd.NA
        as_of_date = pd.NaT
    else:
        mwpl_used = int(latest_mwpl_row["mwpl_shares"].iloc[0])
        lot_used = int(latest_mwpl_row["lot_size_shares"].iloc[0])
        as_of_date = latest_mwpl_row["trade_date"].iloc[0]
        max_permitted = floor(mwpl_used / lot_used) if lot_used else pd.NA
        threshold_90 = floor(0.9 * max_permitted) if max_permitted is not pd.NA else pd.NA

    summary = pd.DataFrame(
        [
            {
                "symbol": symbol,
                "expiry_date": expiry,
                "primary_start_date": primary_start,
                "overlap_start_date": overlap_start,
                "end_date": end_date,
                "summary_scope": summary_scope,
                "max_permitted_contracts": max_permitted,
                "threshold_90pct": threshold_90,
                "max_oi_contracts": max_oi_contracts,
                "mwpl_shares_used": mwpl_used,
                "lot_size_used": lot_used,
                "as_of_trade_date": as_of_date,
            }
        ]
    )

    dest = (
        root
        / "gold"
        / "futures_month_summary"
        / f"{symbol}_{expiry.isoformat()}.parquet"
    )
    dest.parent.mkdir(parents=True, exist_ok=True)
    summary.to_parquet(dest, index=False)
    return summary


def _load_gold_days(
    root: Path, symbol: str, start_date: date, end_date: date
) -> pd.DataFrame:
    """Load gold futures_day rows for `symbol` within the date window."""

    gold_root = root / "gold" / "futures_day"
    frames: list[pd.DataFrame] = []

    if not gold_root.exists():
        return pd.DataFrame()

    for partition in gold_root.glob("date=*"):
        date_str = partition.name.split("=", 1)[1]
        trade_date = date.fromisoformat(date_str)
        if trade_date < start_date or trade_date > end_date:
            continue
        file_path = partition / f"{symbol}.parquet"
        if not file_path.exists():
            continue
        df = pd.read_parquet(file_path)
        frames.append(df)

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    combined["trade_date"] = pd.to_datetime(combined["trade_date"]).dt.date
    return combined


def _data_root(storage_root: Optional[Path | str] = None) -> Path:
    if storage_root is not None:
        return data_root_from_config(storage_root)
    if load_config is None:  # pragma: no cover
        return data_root_from_config("./data")
    cfg = load_config()
    return data_root_from_config(cfg.runtime.storage_root)


def _summary_columns() -> list[str]:
    return [
        "symbol",
        "expiry_date",
        "primary_start_date",
        "overlap_start_date",
        "end_date",
        "summary_scope",
        "max_permitted_contracts",
        "threshold_90pct",
        "max_oi_contracts",
        "mwpl_shares_used",
        "lot_size_used",
        "as_of_trade_date",
    ]


__all__ = ["build_futures_summary"]

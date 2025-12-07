"""Per-date futures feature builder (Implementation Plan §6.6)."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any, Optional

import pandas as pd

from nseva.util.paths import data_root_from_config

try:
    from nseva.config import load_config
except Exception:  # pragma: no cover - permit early imports
    load_config = None  # type: ignore[assignment]


def build_futures_day(symbol: str, trade_date: date, *, storage_root: str | Path | None = None) -> pd.DataFrame:
    """Produce the gold.futures_day dataset for `symbol` on `trade_date`."""

    root = _data_root(storage_root)
    date_str = trade_date.isoformat()
    fo_path = root / "silver" / "fo_bhavcopy_day" / f"date={date_str}" / "data.parquet"
    mwpl_path = root / "silver" / "mwpl_combined_day" / f"date={date_str}" / "data.parquet"

    if not fo_path.exists():
        raise FileNotFoundError(f"Silver FUTSTK partition missing for {date_str}")

    fo_df = pd.read_parquet(fo_path)
    fo_df = fo_df[fo_df["symbol"] == symbol]
    if fo_df.empty:
        return pd.DataFrame(columns=_gold_columns())

    if mwpl_path.exists():
        mwpl_df = pd.read_parquet(mwpl_path)
        mwpl_df = mwpl_df[mwpl_df["symbol"] == symbol]
    else:
        mwpl_df = pd.DataFrame(columns=["trade_date", "symbol", "mwpl_shares", "combined_oi_shares"])

    gold_df = _compute_gold(fo_df, mwpl_df)

    gold_path = root / "gold" / "futures_day" / f"date={date_str}" / f"{symbol}.parquet"
    gold_path.parent.mkdir(parents=True, exist_ok=True)
    gold_df.to_parquet(gold_path, index=False)
    return gold_df


def _compute_gold(fo_df: pd.DataFrame, mwpl_df: pd.DataFrame) -> pd.DataFrame:
    """Join FUTSTK rows with MWPL and derive gold columns."""

    merged = fo_df.merge(
        mwpl_df[["trade_date", "symbol", "mwpl_shares", "combined_oi_shares"]],
        on=["trade_date", "symbol"],
        how="left",
        suffixes=("", "_mwpl"),
    )
    merged["oi_contracts"] = merged["open_interest_contracts"]
    merged["oi_shares"] = merged["oi_contracts"] * merged["lot_size_shares"]

    return merged[
        [
            "trade_date",
            "symbol",
            "expiry_date",
            "open",
            "high",
            "low",
            "close",
            "settle_price",
            "contracts",
            "value_lakhs",
            "oi_contracts",
            "lot_size_shares",
            "oi_shares",
            "mwpl_shares",
            "combined_oi_shares",
        ]
    ]


def _gold_columns() -> list[str]:
    return [
        "trade_date",
        "symbol",
        "expiry_date",
        "open",
        "high",
        "low",
        "close",
        "settle_price",
        "contracts",
        "value_lakhs",
        "oi_contracts",
        "lot_size_shares",
        "oi_shares",
        "mwpl_shares",
        "combined_oi_shares",
    ]


def _data_root(storage_root: Optional[Path | str] = None) -> Path:
    if storage_root is not None:
        return data_root_from_config(storage_root)
    if load_config is None:  # pragma: no cover
        return data_root_from_config("./data")
    cfg = load_config()
    return data_root_from_config(cfg.runtime.storage_root)


__all__ = ["build_futures_day"]

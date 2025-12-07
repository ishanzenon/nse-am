"""Combined OI / MWPL parser (Implementation Plan §6.2)."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path

import pandas as pd

REQUIRED_COLUMNS = ["trade_date", "symbol", "mwpl_shares", "combined_oi_shares"]


def mwpl_to_silver(
    path: Path, *, column_aliases: Mapping[str, Sequence[str]] | None = None
) -> pd.DataFrame:
    """Normalize a combined OI / MWPL file into the silver schema.

    Applies config-driven alias mapping, enforces required columns, coerces
    numeric fields to integers, and validates non-negative values.
    """

    if not path.exists():
        raise FileNotFoundError(path)

    df = _read_any(path)

    alias_map: dict[str, Sequence[str]] = {k: v for k, v in (column_aliases or {}).items()}
    rename_map = _build_rename_map(df.columns, alias_map)
    df = df.rename(columns=rename_map)

    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns after alias mapping: {missing}")

    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    df["symbol"] = df["symbol"].astype(str).str.strip()

    for col in ["mwpl_shares", "combined_oi_shares"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
        if df[col].isna().any():
            raise ValueError(f"Null values found in required column '{col}'.")
        if (df[col] < 0).any():
            raise ValueError(f"Negative values found in column '{col}'.")

    return df[REQUIRED_COLUMNS]


def _build_rename_map(
    observed_columns: Sequence[str], alias_map: Mapping[str, Sequence[str]]
) -> dict[str, str]:
    """Create a rename map from observed -> canonical using aliases."""

    alias_lookup: dict[str, str] = {canonical.upper(): canonical for canonical in REQUIRED_COLUMNS}
    for canonical, aliases in alias_map.items():
        for candidate in (canonical, *aliases):
            alias_lookup[candidate.strip().upper()] = canonical

    rename: dict[str, str] = {}
    for col in observed_columns:
        key = alias_lookup.get(str(col).strip().upper())
        if key:
            rename[col] = key
    return rename


def _read_any(path: Path) -> pd.DataFrame:
    """Read CSV or Excel into a DataFrame."""

    suffix = path.suffix.lower()
    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(path)
    return pd.read_csv(path, compression="infer")


__all__ = ["mwpl_to_silver", "REQUIRED_COLUMNS"]

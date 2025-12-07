"""UDiFF bhavcopy parser (Implementation Plan §6.2)."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path

import pandas as pd

FUTSTK_INSTRUMENT = "FUTSTK"

REQUIRED_COLUMNS = [
    "trade_date",
    "instrument",
    "symbol",
    "expiry_date",
    "open",
    "high",
    "low",
    "close",
    "settle_price",
    "contracts",
    "value_lakhs",
    "open_interest_contracts",
    "lot_size_shares",
]

OPTIONAL_COLUMNS = ["change_in_oi_contracts"]


def udiff_to_silver_fo(
    path: Path, *, column_aliases: Mapping[str, Sequence[str]] | None = None
) -> pd.DataFrame:
    """Convert a UDiFF FUTSTK file into the silver schema.

    Applies config-driven column alias mapping, filters to FUTSTK rows,
    enforces required schema, and returns a pandas DataFrame with canonical
    column names and basic type normalization.
    """

    if not path.exists():
        raise FileNotFoundError(path)

    alias_map: dict[str, Sequence[str]] = {k: v for k, v in (column_aliases or {}).items()}
    df = pd.read_csv(path, compression="zip")

    columns_upper = {col.upper(): col for col in df.columns}
    if "TCKRSYMB" in columns_upper and "FININSTRMID" in columns_upper:
        df = df.drop(columns=[columns_upper["FININSTRMID"]])
    if "FININSTRMACTLXPRYDT" in columns_upper and "XPRYDT" in columns_upper:
        df = df.drop(columns=[columns_upper["XPRYDT"]])
    if "TRADDT" in columns_upper and "BIZDT" in columns_upper:
        df = df.drop(columns=[columns_upper["BIZDT"]])

    rename_map = _build_rename_map(df.columns, alias_map)
    df = df.rename(columns=rename_map)
    df = df.loc[:, ~df.columns.duplicated()]

    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns after alias mapping: {missing}")

    instrument_map = {
        "STF": FUTSTK_INSTRUMENT,
        "STO": "OPTSTK",
        "IDF": "FUTIDX",
        "IDO": "OPTIDX",
    }
    df["instrument"] = df["instrument"].astype(str).str.upper().replace(instrument_map)

    df = df[df["instrument"].str.upper() == FUTSTK_INSTRUMENT].copy()
    if df.empty:
        return df[REQUIRED_COLUMNS + OPTIONAL_COLUMNS]

    df["instrument"] = FUTSTK_INSTRUMENT
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    df["expiry_date"] = pd.to_datetime(df["expiry_date"]).dt.date
    for col in ["symbol"]:
        df[col] = df[col].astype(str).str.strip()

    float_cols = ["open", "high", "low", "close", "settle_price", "value_lakhs"]
    int_cols = ["contracts", "open_interest_contracts", "lot_size_shares"]
    optional_int_cols = ["change_in_oi_contracts"]

    for col in float_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    for col in int_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    for col in optional_int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
        else:
            df[col] = pd.Series([pd.NA] * len(df), dtype="Int64")

    _validate_numeric(df, int_cols)
    _validate_numeric(df, float_cols, allow_zero=True, allow_negative=False)
    if (df["lot_size_shares"] <= 0).any():
        raise ValueError("lot_size_shares must be > 0.")

    ordered_cols = REQUIRED_COLUMNS + OPTIONAL_COLUMNS
    return df[ordered_cols]


def _build_rename_map(
    observed_columns: Sequence[str], alias_map: Mapping[str, Sequence[str]]
) -> dict[str, str]:
    """Create a rename map from observed -> canonical using aliases."""

    alias_lookup: dict[str, str] = {
        canonical.upper(): canonical for canonical in REQUIRED_COLUMNS + OPTIONAL_COLUMNS
    }
    for canonical, aliases in alias_map.items():
        for candidate in (canonical, *aliases):
            alias_lookup[candidate.strip().upper()] = canonical

    rename: dict[str, str] = {}
    for col in observed_columns:
        key = alias_lookup.get(col.strip().upper())
        if key:
            rename[col] = key
    return rename


def _validate_numeric(df: pd.DataFrame, cols: Sequence[str], *, allow_zero: bool = True, allow_negative: bool = False) -> None:
    """Validate numeric columns for nulls and sign constraints."""

    for col in cols:
        if df[col].isna().any():
            raise ValueError(f"Null values found in required column '{col}'.")
        if not allow_negative and (df[col] < 0).any():
            raise ValueError(f"Negative values found in column '{col}'.")
        if not allow_zero and (df[col] == 0).any():
            raise ValueError(f"Zero values found in column '{col}' when prohibited.")


__all__ = ["udiff_to_silver_fo", "FUTSTK_INSTRUMENT", "REQUIRED_COLUMNS", "OPTIONAL_COLUMNS"]

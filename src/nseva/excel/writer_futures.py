"""FuData Excel rendering (Implementation Plan §6.8 & §9)."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Iterable, Mapping, Sequence

import pandas as pd
from openpyxl import load_workbook
from openpyxl.worksheet.worksheet import Worksheet

from nseva.gold.futures_summary import build_futures_summary
from nseva.services.expiry_service import windows_for
from nseva.util.paths import data_root_from_config

try:
    from nseva.config import load_config
except Exception:  # pragma: no cover - allow early imports
    load_config = None  # type: ignore[assignment]


def render_futures_workbook(
    symbol: str,
    expiries: Sequence[date],
    *,
    template_path: Path,
    output_path: Path,
    storage_root: Path | str | None = None,
) -> None:
    """Render/update the FuData workbook for `symbol`."""

    cfg = load_config() if load_config else None
    root = data_root_from_config(storage_root or (cfg.runtime.storage_root if cfg else "./data"))
    wb = load_workbook(template_path)
    ws = wb.active

    _clear_sheet(ws)

    start_row = 1
    start_col = 1
    spacing_rows = cfg.futures.table_spacing_rows if cfg else 2

    for expiry in sorted(expiries):
        _render_block(ws, symbol, expiry, start_row, start_col, root)
        start_col += 20  # assume template width; adjust if needed

    output_path.parent.mkdir(parents=True, exist_ok=True)
    wb.save(output_path)


def _render_block(ws: Worksheet, symbol: str, expiry: date, row: int, col: int, root: Path) -> None:
    """Render a single monthly block."""

    primary_start, overlap_start, _ = windows_for(symbol, expiry, storage_root=root)
    summary_df = build_futures_summary(symbol, expiry, storage_root=root)
    gold_df = _load_gold_for_window(symbol, overlap_start, expiry, root=root)

    ws.cell(row=row + 0, column=col + 1, value="Start Dt:")
    ws.cell(row=row + 0, column=col + 2, value=primary_start)
    ws.cell(row=row + 1, column=col + 1, value="Overlap Start Dt:")
    ws.cell(row=row + 1, column=col + 2, value=overlap_start)
    ws.cell(row=row + 0, column=col + 4, value="Expiry:")
    ws.cell(row=row + 0, column=col + 5, value=expiry)

    if not summary_df.empty:
        ws.cell(row=row + 0, column=col + 7, value="Max. Contracts")
        ws.cell(
            row=row + 0,
            column=col + 8,
            value=_safe_excel_number(summary_df["max_permitted_contracts"].iloc[0]),
        )
        ws.cell(row=row + 1, column=col + 7, value="90% of Max. OI Contracts")
        ws.cell(
            row=row + 1,
            column=col + 8,
            value=_safe_excel_number(summary_df["threshold_90pct"].iloc[0]),
        )
        ws.cell(row=row + 2, column=col + 7, value="Max. OI Contracts month")
        ws.cell(
            row=row + 2,
            column=col + 8,
            value=_safe_excel_number(summary_df["max_oi_contracts"].iloc[0]),
        )

    headers = [
        "Date",
        "Expiry",
        "Open",
        "High",
        "Low",
        "Close",
        "Settle Price",
        "No. of contracts",
        "Turnover in lacs",
        "Open Int",
        "OI Contracts",
        "Change in OI",
    ]
    for offset, header in enumerate(headers):
        ws.cell(row=row + 4, column=col + offset, value=header)

    if not gold_df.empty:
        gold_df = gold_df.sort_values("trade_date")
        for idx, (_, rec) in enumerate(gold_df.iterrows(), start=5):
            ws.cell(row=row + idx, column=col + 0, value=rec["trade_date"])
            ws.cell(row=row + idx, column=col + 1, value=rec["expiry_date"])
            ws.cell(row=row + idx, column=col + 2, value=rec["open"])
            ws.cell(row=row + idx, column=col + 3, value=rec["high"])
            ws.cell(row=row + idx, column=col + 4, value=rec["low"])
            ws.cell(row=row + idx, column=col + 5, value=rec["close"])
            ws.cell(row=row + idx, column=col + 6, value=rec["settle_price"])
            ws.cell(row=row + idx, column=col + 7, value=rec["contracts"])
            ws.cell(row=row + idx, column=col + 8, value=rec["value_lakhs"])
            ws.cell(row=row + idx, column=col + 9, value=rec["oi_contracts"])
            ws.cell(row=row + idx, column=col + 10, value=rec["oi_contracts"])
            ws.cell(row=row + idx, column=col + 11, value=rec.get("change_in_oi_contracts"))


def _load_gold_for_window(symbol: str, start: date, end: date, *, root: Path) -> pd.DataFrame:
    """Load gold futures_day rows for a window."""

    gold_root = root / "gold" / "futures_day"
    frames: list[pd.DataFrame] = []
    for partition in gold_root.glob("date=*"):
        date_str = partition.name.split("=", 1)[1]
        trade_date = date.fromisoformat(date_str)
        if trade_date < start or trade_date > end:
            continue
        path = partition / f"{symbol}.parquet"
        if path.exists():
            df = pd.read_parquet(path)
            frames.append(df)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _clear_sheet(ws: Worksheet) -> None:
    """Clear sheet contents."""

    ws.delete_rows(1, ws.max_row)


def _safe_excel_number(value: object) -> object:
    """Openpyxl cannot write pandas.NA/NaT directly."""

    if value is pd.NA:
        return None
    return value


__all__ = ["render_futures_workbook"]

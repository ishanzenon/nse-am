"""Command-line entry points for the FuData pipeline (FUT-012)."""

from __future__ import annotations

from datetime import date, timedelta
import time
import pandas as pd
from pathlib import Path
from typing import Optional

import typer

from nseva.config import load_config
from nseva.excel.writer_futures import render_futures_workbook
from nseva.gold.futures_day import build_futures_day
from nseva.gold.futures_summary import build_futures_summary
from nseva.io.cache import cache_path_for
from nseva.io.fetcher import fetch_file
from nseva.parse.mwpl import mwpl_to_silver
from nseva.parse.udiff import udiff_to_silver_fo
from nseva.silver.writer import write_partition
from nseva.util.logging import configure_logging
from nseva.util.manifest import write_manifest
from nseva.util.paths import data_root_from_config
import logging

app = typer.Typer(add_completion=False, help="NSE FuData pipeline CLI")


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def _month_shift(day: date, months_back: int) -> date:
    """Return the same day shifted back by `months_back` months, clamped to month end if needed."""

    year = day.year
    month = day.month - months_back
    while month <= 0:
        month += 12
        year -= 1
    # clamp day to last day of target month
    target = date(year, month, 1)
    next_month = (target.replace(day=28) + timedelta(days=4)).replace(day=1)
    last_day = (next_month - timedelta(days=1)).day
    return date(year, month, min(day.day, last_day))


def _ingest_udiff_dates(
    cfg,
    root: Path,
    dates: list[date],
    *,
    symbol: Optional[str],
    prefetch_months: int,
    logger: logging.Logger,
) -> set[str]:
    udiff_src = cfg.futures.sources.udiff_fo
    dates_to_fetch = set(dates)
    if prefetch_months > 0:
        for d in dates:
            for offset in range(1, prefetch_months + 1):
                dates_to_fetch.add(_month_shift(d, offset))

    seen: set[str] = set()
    for target_date in sorted(dates_to_fetch):
        url = udiff_src.url_pattern.format(
            YYYY=f"{target_date.year:04d}", MM=f"{target_date.month:02d}", DD=f"{target_date.day:02d}"
        )
        dest = cache_path_for("fo_udiff", trade_date=target_date.isoformat(), root=root).with_suffix(".zip")
        if dest.exists():
            logger.info("Skipping download, raw exists for %s", target_date)
        else:
            logger.info("Fetching UDiFF %s -> %s", url, dest)
            try:
                fetch_file(url, dest, retries=udiff_src.retries, timeout_seconds=udiff_src.timeout_seconds)
            except FileNotFoundError:
                logger.warning("UDiFF missing for %s", target_date)
                continue

        df = udiff_to_silver_fo(dest, column_aliases=udiff_src.column_aliases)
        if symbol:
            df = df[df["symbol"] == symbol]
        write_partition("fo_bhavcopy_day", target_date.isoformat(), df, root=root)
        logger.info("Wrote silver fo_bhavcopy_day for %s rows=%s", target_date.isoformat(), len(df))
        seen.add(target_date.isoformat())
    return seen


@app.command()
def ingest_udiff(
    trade_date: str = typer.Argument(..., help="Trade date YYYY-MM-DD"),
    symbol: Optional[str] = typer.Option(None, help="Optional symbol filter"),
    prefetch_months: int = typer.Option(0, help="If >0, prefetch this many prior months of UDiFF before trade_date"),
) -> None:
    """Download and parse UDiFF FUTSTK for a date into silver."""

    logger = configure_logging()
    cfg = load_config()
    root = data_root_from_config(cfg.runtime.storage_root)
    d = _parse_date(trade_date)

    seen = _ingest_udiff_dates(cfg, root, [d], symbol=symbol, prefetch_months=prefetch_months, logger=logger)
    write_manifest({"step": "ingest_udiff", "dates": sorted(seen)}, root=root)


@app.command()
def ingest_mwpl(
    trade_date: str = typer.Argument(..., help="Trade date YYYY-MM-DD"),
    path: Optional[Path] = typer.Option(None, help="Path to local MWPL file (CSV/Excel)"),
) -> None:
    """Parse MWPL combined OI for a date into silver."""

    logger = configure_logging()
    cfg = load_config()
    root = data_root_from_config(cfg.runtime.storage_root)
    mwpl_src = cfg.futures.sources.mwpl_combined
    d = _parse_date(trade_date)

    if path is None:
        typer.echo("Provide --path to a local MWPL file (download not wired).")
        raise typer.Exit(code=1)

    df = mwpl_to_silver(path, column_aliases=mwpl_src.column_aliases)
    write_partition("mwpl_combined_day", d.isoformat(), df, root=root)
    logger.info("Wrote silver mwpl_combined_day for %s rows=%s", d.isoformat(), len(df))
    write_manifest({"step": "ingest_mwpl", "date": d.isoformat(), "rows": len(df)}, root=root)


@app.command()
def build_gold(
    trade_date: str = typer.Argument(..., help="Trade date YYYY-MM-DD"),
    symbol: str = typer.Option(..., "--symbol", "-s", help="Symbol"),
) -> None:
    """Build gold futures_day and summaries for a symbol/date."""

    logger = configure_logging()
    cfg = load_config()
    root = data_root_from_config(cfg.runtime.storage_root)
    d = _parse_date(trade_date)

    gold_df = build_futures_day(symbol, d, storage_root=root)
    logger.info("Built gold futures_day rows=%s for %s %s", len(gold_df), symbol, d.isoformat())

    expiries = gold_df["expiry_date"].unique()
    for expiry in expiries:
        build_futures_summary(symbol, expiry, storage_root=root)
        logger.info("Updated summary for %s %s", symbol, expiry)

    write_manifest({"step": "build_gold", "date": d.isoformat(), "symbol": symbol}, root=root)


@app.command()
def export_excel(
    symbol: str = typer.Argument(..., help="Symbol"),
    expiries: Optional[str] = typer.Option(None, help="Comma-separated expiries YYYY-MM-DD"),
    template: Path = typer.Option(Path("templates/futures_template.xlsx"), help="Template path"),
) -> None:
    """Render FuData Excel for a symbol."""

    cfg = load_config()
    root = data_root_from_config(cfg.runtime.storage_root)
    if expiries:
        expiry_list = [date.fromisoformat(item.strip()) for item in expiries.split(",") if item.strip()]
    else:
        summary_root = root / "gold" / "futures_month_summary"
        expiry_list = []
        for file in summary_root.glob(f"{symbol}_*.parquet"):
            expiry_str = file.name.split("_", 1)[1].replace(".parquet", "")
            expiry_list.append(date.fromisoformat(expiry_str))

    if not expiry_list:
        raise typer.Exit("No expiries found to export.")

    output = root / "excel" / symbol / "FuData.xlsx"
    render_futures_workbook(
        symbol,
        expiry_list,
        template_path=template,
        output_path=output,
        storage_root=root,
    )
    typer.echo(f"Wrote {output}")


@app.command()
def run(
    from_date: str = typer.Option(..., "--from", help="Start date YYYY-MM-DD"),
    to_date: str = typer.Option(..., "--to", help="End date YYYY-MM-DD"),
    symbols: Optional[str] = typer.Option(None, help="Comma-separated symbols (default from config)"),
    prefetch_months: int = typer.Option(0, help="Prefetch this many prior months for expiry discovery"),
    sleep_between_days: float = typer.Option(0.0, help="Sleep seconds between days for politeness"),
) -> None:
    """Backfill over a date range: ingest UDiFF, build gold, and export Excel."""

    logger = configure_logging()
    cfg = load_config()
    root = data_root_from_config(cfg.runtime.storage_root)
    symbol_list = symbols.split(",") if symbols else cfg.runtime.symbols
    start = _parse_date(from_date)
    end = _parse_date(to_date)

    day = start
    while day <= end:
        # ingest UDiFF for this day (with optional prefetch)
        seen = _ingest_udiff_dates(cfg, root, [day], symbol=None, prefetch_months=prefetch_months, logger=logger)
        for sym in symbol_list:
            build_futures_day(sym, day, storage_root=root)
        # build impacted summaries for each symbol by inspecting gold for this date
        for sym in symbol_list:
            gold_path = root / "gold" / "futures_day" / f"date={day.isoformat()}" / f"{sym}.parquet"
            if gold_path.exists():
                df = pd.read_parquet(gold_path)
                for expiry in df["expiry_date"].unique():
                    build_futures_summary(sym, expiry, storage_root=root)
        day += timedelta(days=1)
        if sleep_between_days > 0:
            time.sleep(sleep_between_days)

    # Export Excel for each symbol with all summaries found
    for sym in symbol_list:
        summary_root = root / "gold" / "futures_month_summary"
        expiry_list = []
        for file in summary_root.glob(f"{sym}_*.parquet"):
            expiry_str = file.name.split("_", 1)[1].replace(".parquet", "")
            expiry_list.append(date.fromisoformat(expiry_str))
        if not expiry_list:
            logger.warning("No summaries found for %s; skipping Excel", sym)
            continue
        output = root / "excel" / sym / "FuData.xlsx"
        render_futures_workbook(sym, expiry_list, template_path=Path("templates/futures_template.xlsx"), output_path=output, storage_root=root)
        logger.info("Wrote %s", output)

    write_manifest({"step": "run", "from": start.isoformat(), "to": end.isoformat(), "symbols": symbol_list}, root=root)


def main() -> None:
    app()


__all__ = ["main", "app"]

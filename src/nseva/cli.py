"""Command-line entry points for the FuData pipeline (FUT-012)."""

from __future__ import annotations

from datetime import date
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

app = typer.Typer(add_completion=False, help="NSE FuData pipeline CLI")


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


@app.command()
def ingest_udiff(
    trade_date: str = typer.Argument(..., help="Trade date YYYY-MM-DD"),
    symbol: Optional[str] = typer.Option(None, help="Optional symbol filter"),
) -> None:
    """Download and parse UDiFF FUTSTK for a date into silver."""

    logger = configure_logging()
    cfg = load_config()
    root = data_root_from_config(cfg.runtime.storage_root)
    udiff_src = cfg.futures.sources.udiff_fo
    d = _parse_date(trade_date)

    url = udiff_src.url_pattern.format(YYYY=f"{d.year:04d}", MM=f"{d.month:02d}", DD=f"{d.day:02d}")
    dest = cache_path_for("fo_udiff", trade_date=d.isoformat(), root=root).with_suffix(".zip")
    logger.info("Fetching UDiFF %s -> %s", url, dest)
    fetch_file(url, dest, retries=udiff_src.retries, timeout_seconds=udiff_src.timeout_seconds)

    df = udiff_to_silver_fo(dest, column_aliases=udiff_src.column_aliases)
    if symbol:
        df = df[df["symbol"] == symbol]
    write_partition("fo_bhavcopy_day", d.isoformat(), df, root=root)
    logger.info("Wrote silver fo_bhavcopy_day for %s rows=%s", d.isoformat(), len(df))

    write_manifest({"step": "ingest_udiff", "date": d.isoformat(), "rows": len(df)}, root=root)


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


def main() -> None:
    app()


__all__ = ["main", "app"]

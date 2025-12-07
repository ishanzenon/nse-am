from __future__ import annotations

import zipfile
from datetime import date
from pathlib import Path
from typing import Iterable

import pandas as pd
from typer.testing import CliRunner

from nseva import cli
from nseva.config import load_config
from nseva.io.cache import cache_path_for
from nseva.silver.writer import write_partition

SYMBOL = "TESTSYM"


def make_udiff_row(
    trade_date: str,
    symbol: str,
    expiry: str,
    *,
    oi_contracts: int,
    contracts: int,
    value_lakhs: float,
    lot_size: int = 10,
    open_price: float = 100.0,
    high: float | None = None,
    low: float | None = None,
    close: float | None = None,
    settle: float | None = None,
    change_in_oi: int = 0,
) -> dict[str, object]:
    """Build a FUTSTK row using UDiFF-style headers."""

    return {
        "TRD_DT": trade_date,
        "INSTRUMENT": "FUTSTK",
        "SYMBOL": symbol,
        "EXPIRY_DT": expiry,
        "OPEN": open_price,
        "HIGH": high if high is not None else open_price,
        "LOW": low if low is not None else open_price,
        "CLOSE": close if close is not None else open_price,
        "SETTLE_PR": settle if settle is not None else open_price,
        "CONTRACTS": contracts,
        "VAL_IN_LAKH": value_lakhs,
        "OPENINT": oi_contracts,
        "CHG_IN_OI": change_in_oi,
        "LOT_SIZE": lot_size,
    }


def seed_udiff_zips(root: Path, rows: Iterable[dict[str, object]]) -> None:
    """Write grouped UDiFF rows into per-day zip files under raw/fo_udiff."""

    grouped: dict[str, list[dict[str, object]]] = {}
    for row in rows:
        trade_date = str(row["TRD_DT"])
        grouped.setdefault(trade_date, []).append(row)

    for trade_date, members in grouped.items():
        dest = cache_path_for("fo_udiff", trade_date=trade_date, root=root).with_suffix(".zip")
        dest.parent.mkdir(parents=True, exist_ok=True)
        csv_bytes = pd.DataFrame(members).to_csv(index=False).encode("utf-8")
        with zipfile.ZipFile(dest, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("bhav.csv", csv_bytes)


def mark_trading_day(root: Path, trade_date: str) -> None:
    """Create an empty raw UDiFF placeholder to mark a trading day."""

    dest = cache_path_for("fo_udiff", trade_date=trade_date, root=root).with_suffix(".zip")
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text("placeholder", encoding="utf-8")


def seed_mwpl_partition(
    root: Path,
    trade_date: str,
    symbol: str,
    *,
    mwpl_shares: int,
    combined_oi_shares: int,
) -> Path:
    """Write a mwpl_combined_day silver partition."""

    frame = pd.DataFrame(
        [
            {
                "trade_date": pd.to_datetime(trade_date).date(),
                "symbol": symbol,
                "mwpl_shares": mwpl_shares,
                "combined_oi_shares": combined_oi_shares,
            }
        ]
    )
    return write_partition("mwpl_combined_day", trade_date, frame, root=root)


def run_sample_pipeline(monkeypatch, tmp_path: Path) -> tuple[Path, dict[str, object]]:
    """Seed synthetic data, run the CLI backfill, and return the data root and expectations."""

    root = tmp_path / "data"
    cfg = load_config(overrides={"runtime": {"storage_root": str(root), "symbols": [SYMBOL]}})
    monkeypatch.setattr(cli, "load_config", lambda: cfg)

    expiry_chain = ["2024-01-25", "2024-02-22", "2024-03-28", "2024-04-25"]
    run_dates = [
        ("2024-03-29", 150),
        ("2024-03-30", 160),
        ("2024-03-31", 170),
        ("2024-04-01", 180),
        ("2024-04-02", 190),
        ("2024-04-03", 220),
        ("2024-04-04", 200),
        ("2024-04-05", 210),
    ]

    udiff_rows = []
    for trade_date, oi in run_dates:
        udiff_rows.append(
            make_udiff_row(
                trade_date,
                SYMBOL,
                "2024-04-25",
                oi_contracts=oi,
                contracts=10,
                value_lakhs=1.5,
                change_in_oi=1,
            )
        )

    # seed earlier expiries so windows_for can resolve E-1/E-3
    for expiry in expiry_chain[:-1]:
        udiff_rows.append(
            make_udiff_row(
                "2024-03-29",
                SYMBOL,
                expiry,
                oi_contracts=5,
                contracts=2,
                value_lakhs=0.5,
            )
        )

    seed_udiff_zips(root, udiff_rows)
    mark_trading_day(root, "2024-01-26")

    seed_mwpl_partition(root, "2024-04-02", SYMBOL, mwpl_shares=1000, combined_oi_shares=400)
    seed_mwpl_partition(root, "2024-04-04", SYMBOL, mwpl_shares=1200, combined_oi_shares=450)

    runner = CliRunner()
    result = runner.invoke(
        cli.app,
        ["run", "2024-03-29", "2024-04-05", "--symbols", SYMBOL, "--prefetch-months", "0"],
    )
    if result.exit_code != 0:
        raise AssertionError(result.output)

    expectations = {
        "expiry": date(2024, 4, 25),
        "primary_start": date(2024, 3, 29),
        "overlap_start": date(2024, 1, 26),
        "max_oi_contracts": 220,
        "max_permitted": 120,
        "threshold_90pct": 108,
        "as_of": date(2024, 4, 4),
        "first_trade_date": date(2024, 3, 29),
        "oi_by_date": dict(run_dates),
    }
    return root, expectations

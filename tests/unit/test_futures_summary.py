from __future__ import annotations

import unittest
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd

from nseva.gold.futures_summary import build_futures_summary
from nseva.io.cache import cache_path_for
from nseva.silver.writer import write_partition


def _silver_row(trade_date: str, symbol: str, expiry: str) -> dict:
    return {
        "trade_date": pd.to_datetime(trade_date).date(),
        "instrument": "FUTSTK",
        "symbol": symbol,
        "expiry_date": pd.to_datetime(expiry).date(),
        "open": 1.0,
        "high": 1.0,
        "low": 1.0,
        "close": 1.0,
        "settle_price": 1.0,
        "contracts": 1,
        "value_lakhs": 1.0,
        "open_interest_contracts": 1,
        "lot_size_shares": 10,
        "change_in_oi_contracts": 0,
    }


class FuturesSummaryTests(unittest.TestCase):
    def test_builds_summary_with_latest_mwpl(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            symbol = "ABC"
            # Seed expiries for window calc
            expiries = [
                ("2024-01-02", "2024-01-25"),
                ("2024-02-05", "2024-02-22"),
                ("2024-03-01", "2024-03-28"),
                ("2024-04-01", "2024-04-25"),
            ]
            for trade_dt, expiry in expiries:
                write_partition(
                    "fo_bhavcopy_day",
                    trade_dt,
                    pd.DataFrame([_silver_row(trade_dt, symbol, expiry)]),
                    root=root,
                )

            # Mark trading days for W1/W3 starts
            for day in ["2024-01-26", "2024-03-29", "2024-04-05"]:
                marker = cache_path_for("fo_udiff", trade_date=day, root=root).with_suffix(".zip")
                marker.parent.mkdir(parents=True, exist_ok=True)
                marker.write_text("x", encoding="utf-8")

            # Gold day data in scope (W1 start = 2024-03-29, W3 start = 2024-01-26)
            day_frames = [
                pd.DataFrame(
                    [
                        {
                            "trade_date": pd.to_datetime("2024-03-29").date(),
                            "symbol": symbol,
                            "expiry_date": pd.to_datetime("2024-04-25").date(),
                            "open": 1,
                            "high": 1,
                            "low": 1,
                            "close": 1,
                            "settle_price": 1,
                            "contracts": 10,
                            "value_lakhs": 1,
                            "oi_contracts": 100,
                            "lot_size_shares": 10,
                            "oi_shares": 1000,
                            "mwpl_shares": 900,
                            "combined_oi_shares": 400,
                        }
                    ]
                ),
                pd.DataFrame(
                    [
                        {
                            "trade_date": pd.to_datetime("2024-04-05").date(),
                            "symbol": symbol,
                            "expiry_date": pd.to_datetime("2024-04-25").date(),
                            "open": 1,
                            "high": 1,
                            "low": 1,
                            "close": 1,
                            "settle_price": 1,
                            "contracts": 20,
                            "value_lakhs": 2,
                            "oi_contracts": 200,
                            "lot_size_shares": 10,
                            "oi_shares": 2000,
                            "mwpl_shares": 1000,
                            "combined_oi_shares": 450,
                        }
                    ]
                ),
            ]
            for frame in day_frames:
                trade_dt = frame["trade_date"].iloc[0]
                dest = root / "gold" / "futures_day" / f"date={trade_dt.isoformat()}"
                dest.mkdir(parents=True, exist_ok=True)
                frame.to_parquet(dest / f"{symbol}.parquet", index=False)

            summary = build_futures_summary(symbol, date(2024, 4, 25), storage_root=root)
            self.assertEqual(len(summary), 1)
            row = summary.iloc[0]
            self.assertEqual(row["max_oi_contracts"], 200)
            self.assertEqual(row["mwpl_shares_used"], 1000)
            self.assertEqual(row["lot_size_used"], 10)
            self.assertEqual(row["max_permitted_contracts"], 100)
            self.assertEqual(row["threshold_90pct"], 90)
            self.assertEqual(row["as_of_trade_date"], pd.to_datetime("2024-04-05").date())

    def test_empty_when_no_gold(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            symbol = "ABC"
            expiry = date(2024, 1, 25)
            write_partition(
                "fo_bhavcopy_day",
                "2024-01-02",
                pd.DataFrame([_silver_row("2024-01-02", symbol, expiry.isoformat())]),
                root=root,
            )
            marker = cache_path_for("fo_udiff", trade_date="2024-01-03", root=root).with_suffix(".zip")
            marker.parent.mkdir(parents=True, exist_ok=True)
            marker.write_text("x", encoding="utf-8")

            summary = build_futures_summary(symbol, expiry, storage_root=root)
            self.assertTrue(summary.empty)


if __name__ == "__main__":
    unittest.main()

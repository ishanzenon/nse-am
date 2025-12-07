from __future__ import annotations

import unittest
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd

from nseva.gold.futures_day import build_futures_day
from nseva.silver.writer import write_partition


def _make_fo_partition(root: Path, trade_date: str, symbol: str, expiry: str) -> None:
    frame = pd.DataFrame(
        [
            {
                "trade_date": pd.to_datetime(trade_date).date(),
                "instrument": "FUTSTK",
                "symbol": symbol,
                "expiry_date": pd.to_datetime(expiry).date(),
                "open": 10.0,
                "high": 11.0,
                "low": 9.5,
                "close": 10.5,
                "settle_price": 10.25,
                "contracts": 5,
                "value_lakhs": 1.23,
                "open_interest_contracts": 100,
                "lot_size_shares": 15,
                "change_in_oi_contracts": 2,
            }
        ]
    )
    write_partition("fo_bhavcopy_day", trade_date, frame, root=root)


def _make_mwpl_partition(root: Path, trade_date: str, symbol: str) -> None:
    frame = pd.DataFrame(
        [
            {
                "trade_date": pd.to_datetime(trade_date).date(),
                "symbol": symbol,
                "mwpl_shares": 1000,
                "combined_oi_shares": 500,
            }
        ]
    )
    write_partition("mwpl_combined_day", trade_date, frame, root=root)


class FuturesDayGoldTests(unittest.TestCase):
    def test_builds_gold_with_mwpl_join(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            trade_date = date(2024, 1, 2)
            symbol = "ABC"
            _make_fo_partition(root, trade_date.isoformat(), symbol, "2024-01-25")
            _make_mwpl_partition(root, trade_date.isoformat(), symbol)

            gold = build_futures_day(symbol, trade_date, storage_root=root)

            self.assertEqual(len(gold), 1)
            row = gold.iloc[0]
            self.assertEqual(row["oi_contracts"], 100)
            self.assertEqual(row["oi_shares"], 1500)
            self.assertEqual(row["mwpl_shares"], 1000)
            self.assertEqual(row["combined_oi_shares"], 500)

            gold_file = root / "gold" / "futures_day" / f"date={trade_date.isoformat()}" / f"{symbol}.parquet"
            self.assertTrue(gold_file.exists())

    def test_missing_mwpl_still_builds(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            trade_date = date(2024, 1, 2)
            symbol = "ABC"
            _make_fo_partition(root, trade_date.isoformat(), symbol, "2024-01-25")

            gold = build_futures_day(symbol, trade_date, storage_root=root)

            self.assertTrue(pd.isna(gold.iloc[0]["mwpl_shares"]))
            self.assertTrue(pd.isna(gold.iloc[0]["combined_oi_shares"]))


if __name__ == "__main__":
    unittest.main()

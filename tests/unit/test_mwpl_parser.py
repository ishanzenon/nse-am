from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd

from nseva.parse.mwpl import REQUIRED_COLUMNS, mwpl_to_silver
from nseva.silver.writer import write_partition


DEFAULT_ALIASES = {
    "trade_date": ["TRD_DT", "DATE"],
    "symbol": ["SYMBOL", "SECURITY"],
    "mwpl_shares": ["MWPL_SHARES", "MWPL", "MKT_WIDE_POS_LIMIT"],
    "combined_oi_shares": ["COMBINED_OI_SHARES", "COMBINED_OI", "TOTAL_OI"],
}


class MwplParserTests(unittest.TestCase):
    def test_mwpl_parses_aliases(self) -> None:
        raw = pd.DataFrame(
            [
                {
                    "TRD_DT": "2024-01-02",
                    "SECURITY": "ABC",
                    "MWPL": 1000,
                    "TOTAL_OI": 500,
                }
            ]
        )

        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "mwpl.csv"
            raw.to_csv(path, index=False)
            parsed = mwpl_to_silver(path, column_aliases=DEFAULT_ALIASES)

        self.assertEqual(parsed.columns.tolist(), REQUIRED_COLUMNS)
        self.assertEqual(len(parsed), 1)
        row = parsed.iloc[0]
        self.assertEqual(row["trade_date"].isoformat(), "2024-01-02")
        self.assertEqual(row["symbol"], "ABC")
        self.assertEqual(row["mwpl_shares"], 1000)
        self.assertEqual(row["combined_oi_shares"], 500)

    def test_mwpl_missing_required_raises(self) -> None:
        raw = pd.DataFrame(
            [
                {
                    "TRD_DT": "2024-01-02",
                    "SECURITY": "ABC",
                    "MWPL": 1000,
                }
            ]
        )

        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "mwpl.csv"
            raw.to_csv(path, index=False)
            with self.assertRaises(ValueError):
                mwpl_to_silver(path, column_aliases=DEFAULT_ALIASES)

    def test_mwpl_negative_values_raise(self) -> None:
        raw = pd.DataFrame(
            [
                {
                    "TRD_DT": "2024-01-02",
                    "SECURITY": "ABC",
                    "MWPL": -1,
                    "TOTAL_OI": 10,
                }
            ]
        )

        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "mwpl.csv"
            raw.to_csv(path, index=False)
            with self.assertRaises(ValueError):
                mwpl_to_silver(path, column_aliases=DEFAULT_ALIASES)

    def test_mwpl_partition_write(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "trade_date": pd.to_datetime("2024-01-02").date(),
                    "symbol": "ABC",
                    "mwpl_shares": 100,
                    "combined_oi_shares": 50,
                }
            ]
        )

        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            dest = write_partition("mwpl_combined_day", "2024-01-02", frame, root=root)
            reloaded = pd.read_parquet(dest)

            self.assertEqual(
                dest,
                root / "silver" / "mwpl_combined_day" / "date=2024-01-02" / "data.parquet",
            )
            self.assertEqual(reloaded["mwpl_shares"].iloc[0], 100)


if __name__ == "__main__":
    unittest.main()

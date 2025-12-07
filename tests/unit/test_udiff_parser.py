from __future__ import annotations

import unittest
import zipfile
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd

from nseva.parse.udiff import REQUIRED_COLUMNS, OPTIONAL_COLUMNS, udiff_to_silver_fo
from nseva.silver.writer import write_partition


DEFAULT_ALIASES = {
    "trade_date": ["TRD_DT"],
    "instrument": ["INSTRUMENT"],
    "symbol": ["SYMBOL"],
    "expiry_date": ["EXPIRY_DT"],
    "open": ["OPEN"],
    "high": ["HIGH"],
    "low": ["LOW"],
    "close": ["CLOSE"],
    "settle_price": ["SETTLE_PR"],
    "contracts": ["CONTRACTS"],
    "value_lakhs": ["VAL_IN_LAKH"],
    "open_interest_contracts": ["OPENINT"],
    "change_in_oi_contracts": ["CHG_IN_OI"],
    "lot_size_shares": ["LOT_SIZE"],
}


class UdiffoParserTests(unittest.TestCase):
    def _write_zip(self, path: Path, frame: pd.DataFrame) -> Path:
        csv_bytes = frame.to_csv(index=False).encode("utf-8")
        with zipfile.ZipFile(path, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("bhav.csv", csv_bytes)
        return path

    def test_udiff_parses_aliases_and_filters(self) -> None:
        raw = pd.DataFrame(
            [
                {
                    "TRD_DT": "2024-01-02",
                    "INSTRUMENT": "FUTSTK",
                    "SYMBOL": "ABC",
                    "EXPIRY_DT": "2024-01-25",
                    "OPEN": 10.0,
                    "HIGH": 11.0,
                    "LOW": 9.5,
                    "CLOSE": 10.5,
                    "SETTLE_PR": 10.25,
                    "CONTRACTS": 5,
                    "VAL_IN_LAKH": 1.23,
                    "OPENINT": 100,
                    "CHG_IN_OI": 2,
                    "LOT_SIZE": 15,
                },
                {
                    "TRD_DT": "2024-01-02",
                    "INSTRUMENT": "OPTSTK",
                    "SYMBOL": "ABC",
                    "EXPIRY_DT": "2024-01-25",
                    "OPEN": 0,
                    "HIGH": 0,
                    "LOW": 0,
                    "CLOSE": 0,
                    "SETTLE_PR": 0,
                    "CONTRACTS": 0,
                    "VAL_IN_LAKH": 0,
                    "OPENINT": 0,
                    "CHG_IN_OI": 0,
                    "LOT_SIZE": 15,
                },
            ]
        )

        with TemporaryDirectory() as tmpdir:
            zip_path = self._write_zip(Path(tmpdir) / "bhav.zip", raw)
            parsed = udiff_to_silver_fo(zip_path, column_aliases=DEFAULT_ALIASES)

        self.assertEqual(parsed.columns.tolist(), REQUIRED_COLUMNS + OPTIONAL_COLUMNS)
        self.assertEqual(len(parsed), 1)
        row = parsed.iloc[0]
        self.assertEqual(str(row["instrument"]), "FUTSTK")
        self.assertEqual(row["trade_date"].isoformat(), "2024-01-02")
        self.assertEqual(row["expiry_date"].isoformat(), "2024-01-25")
        self.assertEqual(row["contracts"], 5)
        self.assertEqual(row["open_interest_contracts"], 100)
        self.assertEqual(row["lot_size_shares"], 15)
        self.assertEqual(row["change_in_oi_contracts"], 2)

    def test_udiff_missing_required_raises(self) -> None:
        raw = pd.DataFrame(
            [
                {
                    "TRD_DT": "2024-01-02",
                    "INSTRUMENT": "FUTSTK",
                    "SYMBOL": "ABC",
                    "EXPIRY_DT": "2024-01-25",
                    "OPEN": 10.0,
                    "HIGH": 11.0,
                    "LOW": 9.5,
                    "CLOSE": 10.5,
                    "SETTLE_PR": 10.25,
                    "VAL_IN_LAKH": 1.23,
                    "OPENINT": 100,
                    "CHG_IN_OI": 2,
                    "LOT_SIZE": 15,
                }
            ]
        )

        with TemporaryDirectory() as tmpdir:
            zip_path = self._write_zip(Path(tmpdir) / "bhav.zip", raw)
            with self.assertRaises(ValueError):
                udiff_to_silver_fo(zip_path, column_aliases=DEFAULT_ALIASES)

    def test_udiff_rejects_zero_lot_size(self) -> None:
        raw = pd.DataFrame(
            [
                {
                    "TRD_DT": "2024-01-02",
                    "INSTRUMENT": "FUTSTK",
                    "SYMBOL": "ABC",
                    "EXPIRY_DT": "2024-01-25",
                    "OPEN": 10.0,
                    "HIGH": 11.0,
                    "LOW": 9.5,
                    "CLOSE": 10.5,
                    "SETTLE_PR": 10.25,
                    "CONTRACTS": 5,
                    "VAL_IN_LAKH": 1.23,
                    "OPENINT": 100,
                    "CHG_IN_OI": 2,
                    "LOT_SIZE": 0,
                }
            ]
        )

        with TemporaryDirectory() as tmpdir:
            zip_path = self._write_zip(Path(tmpdir) / "bhav.zip", raw)
            with self.assertRaises(ValueError):
                udiff_to_silver_fo(zip_path, column_aliases=DEFAULT_ALIASES)

    def test_write_partition_round_trip(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "trade_date": pd.to_datetime("2024-01-02").date(),
                    "instrument": "FUTSTK",
                    "symbol": "ABC",
                    "expiry_date": pd.to_datetime("2024-01-25").date(),
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

        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            dest = write_partition("fo_bhavcopy_day", "2024-01-02", frame, root=root)
            reloaded = pd.read_parquet(dest)
            self.assertTrue(dest.exists())
            self.assertEqual(
                dest,
                root
                / "silver"
                / "fo_bhavcopy_day"
                / "date=2024-01-02"
                / "data.parquet",
            )
            self.assertEqual(reloaded["instrument"].iloc[0], "FUTSTK")
            self.assertEqual(reloaded["contracts"].iloc[0], 5)


if __name__ == "__main__":
    unittest.main()

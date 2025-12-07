from __future__ import annotations

import unittest
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd

from nseva.io.cache import cache_path_for
from nseva.services.calendar import is_trading_day, next_trading_day_after
from nseva.services.expiry_service import derive_expiries, impacted_expiries_for, windows_for
from nseva.silver.writer import write_partition


def _make_silver_day(root: Path, trade_date: str, symbol: str, expiry: str) -> Path:
    frame = pd.DataFrame(
        [
            {
                "trade_date": pd.to_datetime(trade_date).date(),
                "instrument": "FUTSTK",
                "symbol": symbol,
                "expiry_date": pd.to_datetime(expiry).date(),
                "open": 1.0,
                "high": 2.0,
                "low": 0.5,
                "close": 1.5,
                "settle_price": 1.4,
                "contracts": 10,
                "value_lakhs": 1.0,
                "open_interest_contracts": 100,
                "lot_size_shares": 15,
                "change_in_oi_contracts": 1,
            }
        ]
    )
    return write_partition("fo_bhavcopy_day", trade_date, frame, root=root)


class CalendarExpiryTests(unittest.TestCase):
    def test_trading_day_detection_and_navigation(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            day1 = date(2024, 1, 2)
            day2 = date(2024, 1, 4)
            raw1 = cache_path_for("fo_udiff", trade_date=day1.isoformat(), root=root).with_suffix(
                ".zip"
            )
            raw1.parent.mkdir(parents=True, exist_ok=True)
            raw1.write_text("x", encoding="utf-8")

            # silver-only trading day
            _make_silver_day(root, day2.isoformat(), "ABC", "2024-01-25")

            self.assertTrue(is_trading_day(day1, storage_root=root))
            self.assertTrue(is_trading_day(day2, storage_root=root))
            self.assertFalse(is_trading_day(date(2024, 1, 3), storage_root=root))

            self.assertEqual(next_trading_day_after(day1, storage_root=root), day2)

    def test_windows_and_impacted_expiries(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            symbol = "ABC"
            # expiries: Jan, Feb, Mar, Apr
            _make_silver_day(root, "2024-01-26", symbol, "2024-01-25")
            _make_silver_day(root, "2024-02-23", symbol, "2024-02-22")
            _make_silver_day(root, "2024-03-29", symbol, "2024-03-28")
            _make_silver_day(root, "2024-04-05", symbol, "2024-04-25")

            expiries = derive_expiries(symbol, storage_root=root)
            self.assertEqual(
                expiries,
                (
                    date(2024, 1, 25),
                    date(2024, 2, 22),
                    date(2024, 3, 28),
                    date(2024, 4, 25),
                ),
            )

            w1_start, w3_start, e0 = windows_for(symbol, date(2024, 4, 25), storage_root=root)
            self.assertEqual(e0, date(2024, 4, 25))
            self.assertEqual(w1_start, date(2024, 3, 29))
            self.assertEqual(w3_start, date(2024, 1, 26))

            impacted = list(impacted_expiries_for(symbol, date(2024, 4, 5), storage_root=root))
            self.assertEqual(impacted, [date(2024, 4, 25)])


if __name__ == "__main__":
    unittest.main()

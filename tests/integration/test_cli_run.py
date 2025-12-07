from __future__ import annotations

import pandas as pd

from tests.helpers import SYMBOL, run_sample_pipeline


def test_cli_run_builds_gold_and_summary(monkeypatch, tmp_path) -> None:
    root, expected = run_sample_pipeline(monkeypatch, tmp_path)

    gold_frames: list[pd.DataFrame] = []
    for trade_date in sorted(expected["oi_by_date"]):
        gold_path = root / "gold" / "futures_day" / f"date={trade_date}" / f"{SYMBOL}.parquet"
        assert gold_path.exists()
        gold_frames.append(pd.read_parquet(gold_path))

    combined = pd.concat(gold_frames, ignore_index=True)
    assert combined["oi_contracts"].max() == expected["max_oi_contracts"]
    assert combined["oi_shares"].max() == expected["max_oi_contracts"] * combined["lot_size_shares"].iloc[0]

    summary_path = (
        root / "gold" / "futures_month_summary" / f"{SYMBOL}_{expected['expiry'].isoformat()}.parquet"
    )
    summary = pd.read_parquet(summary_path)
    assert len(summary) == 1
    row = summary.iloc[0]
    assert pd.to_datetime(row["primary_start_date"]).date() == expected["primary_start"]
    assert pd.to_datetime(row["overlap_start_date"]).date() == expected["overlap_start"]
    assert row["max_oi_contracts"] == expected["max_oi_contracts"]
    assert row["max_permitted_contracts"] == expected["max_permitted"]
    assert row["threshold_90pct"] == expected["threshold_90pct"]
    assert pd.to_datetime(row["as_of_trade_date"]).date() == expected["as_of"]
    assert row["mwpl_shares_used"] == 1200
    assert row["lot_size_used"] == 10

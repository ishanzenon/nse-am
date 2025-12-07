## NSE FuData pipeline

Config-first pipeline that ingests daily NSE UDiFF F&O bhavcopy (FUTSTK) and MWPL/combined OI, builds silver/gold Parquet tables, and renders the FuData Excel sheet from the provided template.

### Setup
- Python 3.13; dependencies are pinned in `pyproject.toml` / `uv.lock`.
- Create a virtualenv (`uv sync` or `python -m venv .venv && .venv/bin/pip install -e .[dev]`).
- Run commands from the repo root; data is stored under `./data` by default (override via config).

### Data layout
- `data/raw/fo_udiff/YYYY/MM/DD.zip` — downloaded UDiFF; `.sha256` manifest alongside.
- `data/raw/mwpl/` — MWPL downloads (manual feed for now).
- `data/silver/fo_bhavcopy_day/date=YYYY-MM-DD/data.parquet` — normalized FUTSTK.
- `data/silver/mwpl_combined_day/date=YYYY-MM-DD/data.parquet` — normalized MWPL.
- `data/gold/futures_day/date=YYYY-MM-DD/{SYMBOL}.parquet` — per-day gold features.
- `data/gold/futures_month_summary/{SYMBOL}_YYYY-MM-DD.parquet` — W1/W3 summaries per expiry.
- `data/excel/{SYMBOL}/FuData.xlsx` — rendered workbook.
- `data/logs/run_manifests/run_*.json` — per-command manifest snapshots.

### CLI commands (Typer)
- `python -m nseva.cli ingest-udiff 2024-04-05 --symbols ABC --prefetch-months 4`  
  Parses UDiFF for the day into silver. `--prefetch-months` (opt-in, default 0) also ingests that many prior months if missing.
- `python -m nseva.cli ingest-mwpl 2024-04-05 --path /path/to/mwpl.csv`  
  MWPL ingestion is local-file only; download/discovery is not wired yet.
- `python -m nseva.cli build-gold 2024-04-05 --symbol ABC`  
  Builds gold.futures_day and any impacted summaries for the symbol/date.
- `python -m nseva.cli export-excel ABC`  
  Renders `data/excel/ABC/FuData.xlsx` using `templates/futures_template.xlsx`.
- `python -m nseva.cli run 2024-04-01 2024-04-05 --symbols ABC,XYZ --prefetch-months 4 --sleep-between-days 0.0`  
  End-to-end backfill over a date range: ingest UDiFF (plus optional history), build gold, recompute summaries, and export Excel per symbol. Idempotent: re-running a day overwrites that day’s partitions.

### Logging and manifests
- Logging is configured to stdout (and file if provided); MWPL/window fallbacks surface as WARN.
- Each command writes a manifest JSON under `data/logs/run_manifests/` with the step, symbols, and dates processed.

### Testing
- Run `PYTHONPATH=$PWD/src pytest` to execute unit, integration, and acceptance suites (fixtures are synthetic; no network needed).

### Done criteria (v1)
- Backfill and single-day runs produce silver/gold Parquet aligned to the design schemas.
- Expiry service derives W1/W3 from observed expiries (with heuristic warning fallback).
- Summary metrics use the latest available MWPL for the window; idempotent writes on rerun.
- FuData.xlsx matches the template layout (headers, dual-window metadata, summaries, W3 daily table) with display rounding from config.
- Logging/manifests capture runs and data anomalies; README documents how to operate the pipeline.

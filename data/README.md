# Data Workspace Layout

This directory hosts runtime artifacts produced by the FuData pipeline:

- `raw/` — source downloads (UDiFF bhavcopy zips, MWPL files, quarantine copies)
- `silver/` — normalized daily parquet partitions (`fo_bhavcopy_day`, `mwpl_combined_day`)
- `gold/` — per-date and per-expiry feature tables
- `excel/` — rendered FuData workbooks per symbol
- `logs/` — structured run logs and manifests

The contents are ignored by git; only this README and `.gitkeep` files remain tracked to preserve layout.

# Setup (Windows) — Simple, repeatable

This is the “how to run commands” file.
It does NOT explain the full project spec (that is in design/SPEC.md).

## Where to run commands
Run commands from the repo root folder:
C:\Users\pcash\OneDrive\Backtest

You can confirm you are in the right folder because you can see:
- config/
- design/
- tools/

## Step 1 — Confirm you are using the right Python
Run:
- pybt -V

Success looks like:
- It prints a Python version (your backtest environment).

If pybt is not found:
- Use this instead:
  C:\Users\pcash\anaconda3\envs\backtest\python.exe --version

## Step 2 — Install required packages (if needed)
Run:
- pybt -m pip install -r requirements.txt

If you get a network error, install only what you need (example):
- pybt -m pip install openpyxl duckdb databento databento-dbn

## Step 3 — Create the Excel config workbook (only needed once)
Run:
- pybt tools/make_run_config_xlsx.py

This creates:
- config/run_config.xlsx

## Step 4 — Verify the workbook headers
Run:
- pybt tools/verify_run_config_xlsx.py

## Step 5 — Bootstrap SSD folders + DuckDB registry (only needed once per machine)
Run:
- pybt tools/bootstrap_foundation.py

This creates the folder layout under:
- E:\BacktestData\

And creates the DuckDB registry at:
- E:\BacktestData\duckdb\research.duckdb

## Step 6 — Export config snapshot (do this after any Excel edits)
IMPORTANT:
- Close Excel before exporting (Excel can lock the file).

Run:
- pybt tools/export_config_snapshot.py

This writes:
- config/exports/config_snapshot_latest.json
<CONTENT_END>

# Daily Workflow (Beginner Friendly)

Run these steps in order. Use the `pybt` command (your preferred Python alias).

## 1) Update daily XLSX inputs (append-only)
What you do: add new rows to your data or notes without deleting old rows.
Why it matters: keeps history intact and auditable.
What you should see: the file grows with new rows, no old rows removed.

## 2) Edit the control plane Excel file
File: `config/run_config.xlsx`
What you do: update settings in the workbook (Excel control plane [source-of-truth settings]).
Why it matters: all configuration must flow through Excel.
What you should see: your changes saved in the workbook.

## 3) Export a config snapshot
Command:
```powershell
pybt tools/export_config_snapshot.py
```
What this does: creates `config/exports/config_snapshot_latest.json` (config snapshot JSON [saved copy]).
Why it matters: Python reads the snapshot, not the live Excel file.
What you should see: a new timestamped JSON file and an updated `config_snapshot_latest.json`.

## 4) Run ingest jobs (coming later)
Command (placeholder):
```powershell
pybt tools/ingest_<source>.py
```
What this will do: bring new raw data into `E:\BacktestData\raw`.
Why it will matter: the system needs fresh data to work on.
What you should see: new files under `E:\BacktestData\raw`.

## 5) Build derived data (coming later)
Command (placeholder):
```powershell
pybt tools/build_derived.py
```
What this will do: create derived folders like `bars_1m`, `cvd_1m`, `footprint_base_1m`, `big_trade_events`.
Why it will matter: derived data feeds charts and backtests.
What you should see: updated files under `E:\BacktestData\derived`.

## 6) Run backtests (coming later)
Command (placeholder):
```powershell
pybt tools/run_backtests.py
```
What this will do: run strategies and record results.
Why it will matter: backtests show performance and risk.
What you should see: new outputs under `E:\BacktestData\runs`.

## 7) Generate the context pack
Command:
```powershell
pybt tools/make_context_pack.py
```
What this does: creates `context_pack.md` (context pack [handoff summary]).
Why it matters: it captures the current project state for ChatGPT/Codex.
What you should see: the file printed in the terminal and `context_pack.md` in the repo root.

## Before Committing (Quality Check)
Command:
```powershell
pybt tools/check_instruction_headers.py
```
What this does: checks every code file and notebook for the required Instruction Header.
Why it matters: prevents unclear files from being committed.
What you should see: `OK: All files contain the Instruction Header.`

# Setup (Very Simple English)

This guide is for Windows. It uses the `pybt` command (your preferred Python alias).
Always run commands from the repo root [project folder].

`pybt` is a shortcut (PowerShell alias) that points to the project’s conda Python:
`C:\Users\pcash\anaconda3\envs\backtest\python.exe`.
PowerShell aliases do not persist forever; if `pybt` is not recognized, either
re-create the alias or use the full Python path.
Fallback version check:
`C:\Users\pcash\anaconda3\envs\backtest\python.exe -V`

## Step 1: Open the right terminal
Command:
```powershell
pybt -V
```
What this does: confirms the `pybt` Python is available by printing its version.
Why this is needed: all scripts must use the correct Python environment.
Success looks like: you see a Python version number and no errors.

## Step 2: Install required packages
Command:
```powershell
pybt -m pip install openpyxl duckdb
```
What this does: installs packages for Excel [openpyxl] and DuckDB [duckdb].
Why this is needed: scripts cannot read Excel or create the registry without them.
Success looks like: the command finishes and prints `Successfully installed ...`.

## Step 3: Create the Excel config workbook
Command:
```powershell
pybt tools/make_run_config_xlsx.py
```
What this does: creates `config/run_config.xlsx` (the Excel control plane [source-of-truth settings]).
Why this is needed: all configuration is entered in Excel first.
Success looks like: a message `Created workbook: ...config\\run_config.xlsx` and the file exists.

## Step 4: Verify the Excel workbook
Command:
```powershell
pybt tools/verify_run_config_xlsx.py
```
What this does: checks every sheet name and header.
Why this is needed: prevents silent mistakes in the control plane.
Success looks like: `Workbook verification passed.`

## Step 5: Bootstrap SSD data root + DuckDB registry
Command:
```powershell
pybt tools/bootstrap_foundation.py
```
What this does:
- Creates the SSD data root [fast storage] at `E:\BacktestData`.
- Creates the DuckDB registry [database file] at `E:\BacktestData\duckdb\research.duckdb`.
- Creates derived folders: `bars_1m`, `cvd_1m`, `footprint_base_1m`, `big_trade_events`.
- Verifies the workbook and exports a config snapshot.
Why this is needed: the project needs a standard data layout and a registry database.
Success looks like: `Foundation bootstrap complete.` and a list of created paths.

## Step 6: Export the config snapshot JSON
Command:
```powershell
pybt tools/export_config_snapshot.py
```
What this does: writes a JSON snapshot [saved copy] of the Excel control plane.
Why this is needed: Python reads the snapshot, not the live Excel file.
Success looks like: a timestamped file plus `config/exports/config_snapshot_latest.json`.

## Step 7: Generate the context pack
Command:
```powershell
pybt tools/make_context_pack.py
```
What this does: creates `context_pack.md` (a full project summary for ChatGPT/Codex).
Why this is needed: it captures the latest docs, config snapshot, and key code.
Success looks like: the file prints to the terminal and `context_pack.md` exists.

## Enable Git Hooks (optional, local)
Command:
```powershell
git config core.hooksPath .githooks
```
What this does: tells Git to use the repo’s local hook scripts.
Why this is needed: it runs checks before commits.
Success looks like: the command runs with no errors.

---

## Plain-English Explanations
- Repo root [project folder]: the top folder of this repo, where `README.md` lives.
- Excel control plane [source-of-truth settings]: `config/run_config.xlsx`.
- Config snapshot JSON [saved copy]: `config/exports/config_snapshot_latest.json`.
- DuckDB registry [database file]: `E:\BacktestData\duckdb\research.duckdb`.
- SSD data root [fast storage]: `E:\BacktestData`.
- Derived folders [generated data]: `bars_1m`, `cvd_1m`, `footprint_base_1m`, `big_trade_events`.
- Context pack [handoff summary]: `context_pack.md`.

## Common Problems & Fixes
- Problem: Wrong Python environment (terminal vs Jupyter). Fix: close Jupyter, open the terminal, run `pybt --version`, then rerun commands.
- Problem: Module not found. Fix: run `pybt -m pip install openpyxl duckdb` again.
- Problem: `E:` drive missing. Fix: connect the SSD and confirm `E:\BacktestData` exists, then rerun bootstrap.
- Problem: Unicode/binary issues in context pack. Fix: avoid binary files in the repo root; regenerate with `pybt tools/make_context_pack.py`.

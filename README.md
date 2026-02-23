# Backtest Foundation

## Project Goal
Build a reliable, auditable research stack for ingesting market data, deriving features, and running backtests and optimization studies with strict reproducibility.

## Workflow Order
1. Bootstrap storage + registry.
2. Ingest raw sources to canonical storage.
3. Build derived tables + chart data API + thin notebooks.
4. PnL/execution engine.
5. Feature system + caching.
6. Optimization + constraints + robustness.

## Control Plane
Excel is the control plane; Python reads exported JSON snapshots. The workbook is the source of truth.

## Storage Roots
- `CODE_ROOT`: repository root.
- `DATA_ROOT`: SSD data root (default `E:\BacktestData`).

## Notebook Policy
No core logic in notebooks. Notebooks are for thin, reproducible analysis and visualization only.

## How to Run (Phase 0)
Use the `pybt` command (preferred Python alias):
1. `pybt tools/make_run_config_xlsx.py`
2. `pybt tools/bootstrap_foundation.py`
3. `pybt tools/make_context_pack.py`

## Quality Checks
Run this before committing:
`pybt tools/check_instruction_headers.py`

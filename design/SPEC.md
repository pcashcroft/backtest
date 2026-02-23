# SPEC

## Plain English Summary
This project builds a careful research stack for market data, features, backtests, and optimization. Excel is the main control file, and Python reads a saved JSON snapshot. Data lives on an SSD with a standard folder layout. Notebooks are for viewing, not for core logic.

Preface: Authoritative spec captured from chat; update via doc update protocol.

## PROJECT GOAL
Build a robust, reproducible research stack for market data ingestion, feature engineering, backtesting, and optimization with auditable inputs and outputs.

## NON-NEGOTIABLE RULES
- Excel is the control plane. Python reads exported JSON snapshots.
- No core logic in notebooks; notebooks are thin, reproducible analysis only.
- SSD-backed `DATA_ROOT` is the default storage root.
- All config changes flow through the workbook and snapshot export.

## DATA SOURCES
- Market data ingestion into raw storage, with canonicalized datasets in structured tables.
- Dataset definitions and metadata live in the workbook.

## CHARTING
- Chart data comes from derived tables and a thin chart data API.
- Notebooks visualize; they do not implement core logic.

## BACKTEST + OPTIMIZATION
- Backtest engine consumes canonical data and feature caches.
- Optimization studies run on top of backtests with constraints and robustness profiles.

## STORAGE ARCHITECTURE
- `CODE_ROOT`: repository root.
- `DATA_ROOT`: SSD root (default `E:\BacktestData`).
- DuckDB registry at `E:\BacktestData\duckdb\research.duckdb`.
- Derived and feature cache folders under `DATA_ROOT`.

## WORKFLOW ORDER
1. Bootstrap storage + registry.
2. Ingest raw sources to canonical storage.
3. Build derived tables + chart data API + thin notebooks.
4. PnL/execution engine.
5. Feature system + caching.
6. Optimization + constraints + robustness.

## EXCEL WORKBOOK SCHEMA (SHEET LIST)
RUNBOOK, PATHS, DATASETS, INSTRUMENTS, FEATURE_LIBRARY, ENGINEERED_FEATURES, FEATURE_SETS, SIGNAL_TEMPLATES, STRATEGIES, BACKTESTS, OPTIMIZATION_STUDIES, OPTIMIZATION_PARAMS, CONSTRAINT_SETS, CONSTRAINTS, METRICS, ROBUSTNESS_PROFILES, ROBUSTNESS, REPORTING

## PHASE 0 VS PHASE 1+
- Phase 0: repository scaffolding, workbook skeleton, config export, storage bootstrap, DuckDB registry.
- Phase 1+: ingestion, derived tables, engine, features, optimization, reporting.

## CODEX BRIDGE POLICY
- Maintain a context pack with current docs, config snapshot, and key code.
- Use the context pack as the authoritative handoff state.

## CURRENT STATUS
Phase 0 foundation scaffolding and bootstrap utilities.

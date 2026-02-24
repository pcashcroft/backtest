# Progress

Last updated: 2026-02-24 (Session 5)

## Completed

### Foundation (Phase 0 - Infrastructure)
- SSD folder structure bootstrapped (`E:\BacktestData\` with canonical/, derived/, duckdb/, etc.)
- DuckDB registry created with tables: `registry_datasets`, `registry_instruments`, `manifest_derived_tables`, `manifest_feature_cache`, `runs`, `run_metrics`, `meta_schema_version`
- Config system: Excel workbook → JSON snapshot → Python reads snapshot (17 sheet schemas)
- INSTRUMENTS sheet made config-driven (volume_col, units columns added)
- Pre-commit hook enforcing instruction headers on all .py files

### Ingestion (Phase 0 - Data)
- ES trades ingested from Databento DBN → partitioned parquet (FULL + RTH sessions, 312 dates, 2025-02-23 to 2026-02-22)
- ES OHLCV 1s ingested from Databento DBN → partitioned parquet (FULL + RTH sessions, 3113 dates since 2016-02-22)
- Daily series ingested from consolidated.xlsm → canonical parquet by year (2009-2026)
- Macro instruments ingested from Macro_Instruments.xlsx → daily_series canonical parquet

### Derived Data
- `bars_1m` built for ES FULL + RTH: 3,113 FULL dates + 2,578 RTH dates (2016-02-23 to 2026-02-22)
  - Output: `E:\BacktestData\derived\bars_1m\ES\{FULL|RTH}\{date}\part-0.parquet`
  - Schema: bar_time (timestamp[us, tz=UTC]), symbol, open, high, low, close (float64), volume (int64), tick_count (int32)
  - DuckDB manifest updated: ES_BARS_1M_FULL (2016-02-23 → 2026-02-23), ES_BARS_1M_RTH (2016-02-23 → 2026-02-21)
  - Architecture: pre-compute 1m only; higher intervals (5m/10m/30m/1h/1d) aggregated on-the-fly via DuckDB
  - Instrument-agnostic: new instruments added via DATASETS sheet row only

- `footprint_base_1m` built for ES FULL + RTH: 312 FULL dates + 257 RTH dates (2025-02-24 to 2026-02-22)
  - Output: `E:\BacktestData\derived\footprint_base_1m\ES\{FULL|RTH}\{date}\part-0.parquet`
  - Schema: bar_time (timestamp UTC), symbol, price (float64), buy_volume (int64), sell_volume (int64), trade_count (int32)
  - Rows: 3,590,204 FULL + 1,704,180 RTH; spread symbols excluded
  - Higher intervals aggregated on-the-fly (sum buy/sell per price level)

- `cvd_1m` built for ES FULL + RTH: 312 FULL dates + 257 RTH dates (2025-02-24 to 2026-02-22)
  - Output: `E:\BacktestData\derived\cvd_1m\ES\{FULL|RTH}\{date}\part-0.parquet`
  - Schema: bar_time (timestamp UTC), symbol, buy_volume (int64), sell_volume (int64), delta (int64), trade_count (int32)
  - Rows: 476,687 FULL + 154,182 RTH; chart layer computes cumsum(delta) for the CVD line

- `footprint_proxy_1m` + `cvd_proxy_1m` built for ES FULL + RTH: 3,113 FULL + 2,578 RTH dates (2016-02-22 to 2026-02-22)
  - Built from 1s OHLCV using BVC: buy_frac=(close-low)/(high-low), fallback 0.5 for doji bars
  - Footprint price assignment: doji bars → single price 50/50; non-doji → buy at high, sell at low
  - Same schema as real tables for chart/backtest interoperability; `trade_count` = number of 1s bars
  - Output: `E:\BacktestData\derived\footprint_proxy_1m\ES\` and `cvd_proxy_1m\ES\`
  - footprint_proxy_1m rows: 23,679,891 FULL + 10,803,863 RTH
  - cvd_proxy_1m rows: 4,896,455 FULL + 1,618,721 RTH
  - DuckDB manifest: ES_FOOTPRINT_PROXY_1M_FULL/RTH, ES_CVD_PROXY_1M_FULL/RTH (2016-02-23 → 2026-02-23)

### Big Trade Events (Session 4)
- `big_trade_events` computed **on-the-fly** (no pre-saved parquet) — threshold experimentation friendly
- Architecture decision: compute at chart/backtest request time; Phase 4 feature cache handles caching for optimisation runs
- Three threshold methods, all configured via dedicated DATASETS columns (threshold_method, threshold_min_size, threshold_pct, threshold_z, threshold_window_days):
  - `fixed_count` — filter WHERE size >= min_size
  - `rolling_pct` — percentile_cont(pct) over lookback window (pct + window_days as columns)
  - `z_score` — mean + z_threshold * stddev over lookback window (z_threshold + window_days)
- Separate threshold configs for real vs proxy: `ES_BIG_TRADES` (min_size=50), `ES_BIG_TRADES_PROXY` (min_size=100)
- Changing threshold = edit dedicated DATASETS columns in Excel → re-export snapshot → immediate effect, no rebuild
- Output schema: ts_event (UTC), symbol, price (float64), size (int64), side ('B'/'S'/'N')
  - Real: side from canonical trade side Int16 (2→'B', 1→'S', 0→'N')
  - Proxy: BVC buy_frac>0.5→'B' at high, <0.5→'S' at low; doji skipped (ambiguous direction)
- Same real_then_proxy / real_only / proxy_only / both source modes as footprint/CVD
- Verified: real RTH 2026-01-02..03 → 799 events; proxy RTH 2022-01-03..05 → 7,586 events
- New INSTRUMENTS columns: `big_trades_dataset_id`, `big_trades_proxy_dataset_id`, `big_trades_source_mode`
- ES defaults: big_trades_dataset_id=ES_BIG_TRADES, big_trades_proxy_dataset_id=ES_BIG_TRADES_PROXY, big_trades_source_mode=real_then_proxy
- New module: `src/platform/data/big_trades.py` — `get_big_trades(instrument_id, session, start_date, end_date, snapshot)`
- New admin scripts: `migrate_run_config_add_big_trades_cols.py`, `add_big_trades_config.py`

### Tools (23 scripts in 5 subdirectories + 1 library module)

**`tools/ingest/`** — Bring raw data in
- `ingest_trades_databento.py` - Trade-level Databento DBN → partitioned parquet (instrument-agnostic, `--instrument-id`)
- `ingest_ohlcv_1s_databento.py` - 1s OHLCV Databento DBN → partitioned parquet (instrument-agnostic, `--instrument-id`)
- `ingest_daily_consolidated.py` - Consolidated daily series → canonical parquet
- `ingest_daily_macro_instruments.py` - Macro instrument daily data → canonical parquet

**`tools/build/`** — Derive new data from canonical
- `build_derived_bars_1m.py` - Build 1m OHLCV bars from 1s canonical (incremental, instrument-agnostic)
- `build_derived_trade_metrics.py` - Build footprint_base_1m + cvd_1m from trades (incremental, dispatches on metric_type)
- `build_derived_trade_metrics_proxy.py` - Build footprint_proxy_1m + cvd_proxy_1m from 1s OHLCV using BVC

**`tools/admin/`** — Excel config management + migrations
- `make_run_config_xlsx.py` - Create blank workbook from schema
- `export_config_snapshot.py` - Excel → JSON snapshot export
- `add_bars_1m_config.py` - Add BARS_1M to DATASETS + update INSTRUMENTS (`--instrument-id`)
- `add_trade_metrics_config.py` - Add FOOTPRINT_1M + CVD_1M to DATASETS (`--instrument-id`)
- `add_trade_metrics_proxy_config.py` - Add FOOTPRINT_PROXY_1M + CVD_PROXY_1M to DATASETS (`--instrument-id`)
- `add_big_trades_config.py` - Add BIG_TRADES + BIG_TRADES_PROXY to DATASETS + update INSTRUMENTS (`--instrument-id`, `--min-size`, `--proxy-min-size`)
- `migrate_run_config_add_instruments_cols.py` - Schema migration (volume_col, units)
- `migrate_run_config_add_metric_source_cols.py` - Migration: 5 metric-source columns + defaults
- `migrate_run_config_add_big_trades_cols.py` - Migration: 3 big-trade columns + defaults
- `migrate_run_config_add_threshold_cols.py` - Migration: 5 threshold columns to DATASETS + defaults
- `update_instruments_from_macro_workbook.py` - Sync INSTRUMENTS from macro workbook

**`src/platform/data/`** — On-the-fly data computation library
- `big_trades.py` - `get_big_trades()`: on-the-fly big trade events (real + proxy, 3 threshold methods)

**`tools/verify/`** — Checks, debugging, profiling
- `verify_run_config_xlsx.py` - Validate workbook structure
- `verify_duckdb_registry.py` - Check DuckDB tables
- `check_instruction_headers.py` - Enforce documentation standard (pre-commit hook)
- `profile_databento_dbn.py` - Inspect DBN file stats

**`tools/setup/`** — One-time bootstrap + utility
- `bootstrap_foundation.py` - SSD setup + DuckDB init
- `update_design_folder_layout.py` - Auto-generate FOLDER_LAYOUT.md

## In Progress

*(nothing)*

## Next Up (Priority Order)

1. **Interactive Jupyter charts** - Candle charts, footprint overlay, CVD (real + proxy), big trade bubbles (B/S/N colours), session selector; metric_source_mode controls which datasets to load
2. **PnL/execution engine** - Daily + intraday backtests with realistic execution
3. **PnL/execution engine** - Daily + intraday backtests with realistic execution
4. **Feature system + caching** - Feature library, engineered features, cache keyed by spec hash
5. **Optimization + robustness** - IS/OOS, walk-forward, bootstrap, placebo, parameter sensitivity

## Session Log

### Session 1 (2026-02-24) - Migration to Claude Code + Derived Bars
- Installed GitHub CLI (gh v2.87.3)
- Cleaned up main branch: removed 8 stale design/instruction files from ChatGPT workflow
- Added design/FOLDER_LAYOUT.md and tools/update_design_folder_layout.py
- Created this PROGRESS.md as dedicated progress tracker (separate from SPEC.md)
- Established workflow: Claude Code for coding/git, Cursor for IDE, Jupyter for charts only
- Code cleanup: config module exports, removed empty placeholder dirs, consistency fixes
- Deleted make_context_pack.py (unnecessary with Claude Code direct file access)
- Built `bars_1m` for ES: 3,113 FULL dates + 2,578 RTH dates from 1s canonical data
- Created `add_bars_1m_config.py` (generalized DATASETS/INSTRUMENTS setup, `--instrument-id` CLI arg) and `build_derived_bars_1m.py` (incremental builder)
- Architecture decision: pre-compute 1m only, aggregate higher intervals on-the-fly via DuckDB
- DuckDB manifest updated for ES_BARS_1M_FULL and ES_BARS_1M_RTH
- Built footprint_base_1m and cvd_1m for ES (312 FULL + 257 RTH dates each)
- Created `add_trade_metrics_config.py` + `build_derived_trade_metrics.py` (instrument-agnostic, metric_type dispatch)

### Session 2 (2026-02-24) - Proxy Trade Metrics + Metric Source Config
- Confirmed 10 years of 1s OHLCV coverage: 3,113 FULL dates, 2,578 RTH dates (2016-02-22 to 2026-02-22)
- Designed metric source configuration: `metric_source_mode` column (real_only / proxy_only / real_then_proxy / proxy_then_real / both) controls chart and backtest data resolution per instrument
- Added 5 metric-source columns to INSTRUMENTS schema (schema.py + workbook migration):
  `footprint_dataset_id`, `footprint_proxy_dataset_id`, `cvd_dataset_id`, `cvd_proxy_dataset_id`, `metric_source_mode`
- Folded `volume_col` and `units` into schema.py (were previously special-cased in make_run_config_xlsx.py)
- Created `migrate_run_config_add_metric_source_cols.py` - migrates existing workbook + sets ES defaults
- ES INSTRUMENTS defaults set: footprint_dataset_id=ES_FOOTPRINT_1M, cvd_dataset_id=ES_CVD_1M, metric_source_mode=real_then_proxy
- Created `add_trade_metrics_proxy_config.py` + `build_derived_trade_metrics_proxy.py`
  - BVC method: buy_frac=(close-low)/(high-low), fallback 0.5 for doji bars
  - Footprint proxy: doji at single price 50/50; non-doji buy at high, sell at low
  - Schema-compatible with real tables (same column names) for interchangeable chart/backtest use
- ES proxy build completed: footprint_proxy_1m + cvd_proxy_1m, 3,113 FULL + 2,578 RTH dates

### Session 5 (2026-02-24) - Package rename + header hygiene
- Renamed `src/backtest/` → `src/platform/` (better reflects scope: config, data, future charts + engine)
- Added INSTRUCTION HEADER docstrings (plain English) to the 5 config module files that were missing them:
  `config/__init__.py`, `schema.py`, `excel_io.py`, `export_snapshot.py`, `load_snapshot.py`
- Updated all import references in tools and docstrings (no functional changes)
- Deleted stale `src/backtest/__pycache__/` left over from git mv
- Fixed `migrate_run_config_add_threshold_cols.py` missing from tools list in PROGRESS.md
- Fixed stale `src/backtest/` references in Session 4 notes and tools list

### Session 4 (2026-02-24) - Big Trade Events
- Architecture decision: compute on-the-fly (no pre-saved parquet); rebuild-free threshold experimentation
- Three threshold methods: `fixed_count`, `rolling_pct` (percentile + window_days), `z_score` (z_threshold + window_days)
- Threshold config stored in DATASETS.notes per dataset → change method/params in Excel, re-export, immediate effect
- Created `src/platform/data/` package + `big_trades.py`:
  - `get_big_trades(instrument_id, session, start_date, end_date, snapshot)` → DataFrame
  - Dispatches on source_mode (real_only/proxy_only/real_then_proxy/proxy_then_real/both)
  - Real path: queries canonical trades parquet, decodes side Int16 (2→'B', 1→'S', 0→'N')
  - Proxy path: BVC from 1s OHLCV, buy_frac>0.5→'B' at high, <0.5→'S' at low, doji skipped
  - Rolling window methods: extend load window backward by window_days; filter output to requested dates
- Added 3 INSTRUMENTS columns to schema.py + workbook: `big_trades_dataset_id`, `big_trades_proxy_dataset_id`, `big_trades_source_mode`
- Created `tools/admin/migrate_run_config_add_big_trades_cols.py` + `add_big_trades_config.py`
- ES config: ES_BIG_TRADES (real, min_size=50), ES_BIG_TRADES_PROXY (proxy, min_size=100), source_mode=real_then_proxy
- Verified: real 799 events/2 days, proxy 7,586/3 days, blend correctly separates by coverage
- check_instruction_headers.py OK

### Session 3 (2026-02-24) - Tools Reorganisation + Ingest Generalisation
- Removed 2 obsolete PowerShell scripts (`push_to_github.ps1`, `end_thread_handover.ps1`) — Claude Code handles git
- Generalised `ingest_es_trades_databento.py` → `ingest_trades_databento.py`: now instrument-agnostic via `--instrument-id` CLI; reads source glob from DATASETS config instead of hardcoded paths
- Generalised `ingest_ohlcv_1s_databento.py`: now instrument-agnostic via `--instrument-id` CLI; derives DATASET_ID and CANONICAL_ROOT from instrument_id
- Reorganised flat `tools/` (21 scripts) into 5 subdirectories: `ingest/`, `build/`, `admin/`, `verify/`, `setup/`
- Updated 7 hardcoded subprocess path references across 6 files + pre-commit hook
- Updated all 21 INSTRUCTION HEADER "How to run" paths and cross-references
- Fixed `_repo_root()` / `parents[]` depth in 6 files (now `parents[2]` for subdirectory nesting)
- All verification passed: `check_instruction_headers.py` OK, `export_config_snapshot.py` OK, both ingest `--help` OK

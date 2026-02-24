# Progress

Last updated: 2026-02-24 (Session 2)

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

- `footprint_proxy_1m` + `cvd_proxy_1m` building for ES FULL + RTH: 3,113 FULL + 2,578 RTH dates (2016-02-22 to 2026-02-22)
  - Built from 1s OHLCV using BVC: buy_frac=(close-low)/(high-low), fallback 0.5 for doji bars
  - Footprint price assignment: doji bars → single price 50/50; non-doji → buy at high, sell at low
  - Same schema as real tables for chart/backtest interoperability; `trade_count` = number of 1s bars
  - Output: `E:\BacktestData\derived\footprint_proxy_1m\ES\` and `cvd_proxy_1m\ES\`
  - Build in progress (started 2026-02-24)

### Tools
- `bootstrap_foundation.py` - SSD setup + DuckDB init
- `ingest_es_trades_databento.py` - ES trade-level data (incremental support)
- `ingest_ohlcv_1s_databento.py` - ES 1s OHLCV bars
- `ingest_daily_consolidated.py` - Consolidated daily series
- `ingest_daily_macro_instruments.py` - Macro instrument daily data
- `export_config_snapshot.py` - Excel → JSON export
- `make_run_config_xlsx.py` - Create blank workbook from schema
- `verify_run_config_xlsx.py` - Validate workbook structure
- `verify_duckdb_registry.py` - Check DuckDB tables
- `check_instruction_headers.py` - Enforce documentation standard
- `profile_databento_dbn.py` - Inspect DBN file stats
- `update_design_folder_layout.py` - Auto-generate FOLDER_LAYOUT.md
- `update_instruments_from_macro_workbook.py` - Sync INSTRUMENTS from workbook
- `migrate_run_config_add_instruments_cols.py` - Schema migration
- `add_bars_1m_config.py` - Add any instrument's BARS_1M row to DATASETS + update INSTRUMENTS (CLI: `--instrument-id ES`)
- `build_derived_bars_1m.py` - Build 1m OHLCV bars from 1s canonical (incremental, instrument-agnostic)
- `add_trade_metrics_config.py` - Add any instrument's FOOTPRINT_1M + CVD_1M rows to DATASETS (CLI: `--instrument-id ES`)
- `build_derived_trade_metrics.py` - Build footprint_base_1m and cvd_1m from trades (incremental, instrument-agnostic, dispatches on metric_type)
- `migrate_run_config_add_metric_source_cols.py` - One-time migration: adds 5 metric-source columns to INSTRUMENTS sheet + sets defaults
- `add_trade_metrics_proxy_config.py` - Add any instrument's FOOTPRINT_PROXY_1M + CVD_PROXY_1M rows to DATASETS + update INSTRUMENTS (CLI: `--instrument-id ES`)
- `build_derived_trade_metrics_proxy.py` - Build footprint_proxy_1m and cvd_proxy_1m from 1s OHLCV using BVC (incremental, instrument-agnostic)

## In Progress

- `footprint_proxy_1m` + `cvd_proxy_1m` build running (3,113 FULL + 2,578 RTH dates)

## Next Up (Priority Order)

1. **Big trade events** - Build `big_trade_events` from ES trades
2. **Interactive Jupyter charts** - Candle charts, footprint overlay, CVD (real + proxy), big trade bubbles, session selector; metric_source_mode controls which datasets to load
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
- ES proxy build started: footprint_proxy_1m + cvd_proxy_1m, 3,113 FULL + 2,578 RTH dates

# Progress

Last updated: 2026-02-24

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
- `make_context_pack.py` - Generate handover docs
- `update_design_folder_layout.py` - Auto-generate FOLDER_LAYOUT.md
- `update_instruments_from_macro_workbook.py` - Sync INSTRUMENTS from workbook
- `migrate_run_config_add_instruments_cols.py` - Schema migration

## In Progress

- Migration from ChatGPT/Codex workflow to Claude Code (this session)

## Next Up (Priority Order)

1. **Derived OHLCV bars** - Build 1m/5m/10m/30m/1h/1d bars from 1s canonical data (FULL + RTH)
2. **Derived trade metrics** - Build footprint_base_1m, cvd_1m from ES trades
3. **Big trade events** - Build big_trade_events from ES trades
4. **Interactive Jupyter charts** - Candle charts, footprint overlay, CVD, big trade bubbles, session selector
5. **PnL/execution engine** - Daily + intraday backtests with realistic execution
6. **Feature system + caching** - Feature library, engineered features, cache keyed by spec hash
7. **Optimization + robustness** - IS/OOS, walk-forward, bootstrap, placebo, parameter sensitivity

## Session Log

### Session 1 (2026-02-24) - Migration to Claude Code
- Installed GitHub CLI (gh v2.87.3)
- Cleaned up main branch: removed 8 stale design/instruction files from ChatGPT workflow
- Added design/FOLDER_LAYOUT.md and tools/update_design_folder_layout.py
- Fixed BOM in make_context_pack.py
- Created this PROGRESS.md as dedicated progress tracker (separate from SPEC.md)
- Established workflow: Claude Code for coding/git, Cursor for IDE, Jupyter for charts only
- Code cleanup: config module exports, removed empty placeholder dirs, consistency fixes

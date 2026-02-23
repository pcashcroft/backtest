# ROADMAP

## Phase 0 (foundation) — DONE (current repo)
- Repo scaffolding + docs + config tooling
- Excel workbook skeleton (config/run_config.xlsx)
- Config snapshot export/load
- SSD folder bootstrap under E:\BacktestData
- DuckDB registry created at E:\BacktestData\duckdb\research.duckdb
- Daily consolidated ingest to canonical Parquet (series-first long format)
- ES trades DBN ingest to canonical Parquet partitioned by session/date

## Phase 1 (data expansion + derived + charts) — NEXT
- Download more ES trade-level data (expand range)
- Re-run canonical ingest incrementally
- Build derived intraday tables (FULL + RTH):
  - bars_1m
  - cvd_1m
  - footprint_base_1m
  - big_trade_events
- Chart-data API layer
- Thin Jupyter chart notebooks (candles + footprint + CVD + bubbles)

## Phase 2 (engine + features) — AFTER
- Backtest engine (daily + intraday)
- Execution models (slippage, spread, commissions)
- Feature system + caching + timing enforcement

## Phase 3 (optimization + robustness + reporting) — AFTER
- Optimization studies (IS/OOS, constraints)
- Robustness suite (walk-forward, bootstrap, placebo, sensitivity)
- Reporting artifacts / leaderboards / Pareto plots
<CONTENT_END>

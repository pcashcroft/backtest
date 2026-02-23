# SPEC

This file is the authoritative “what we are building” document.

**Design-authority rule (non-negotiable):**
- ChatGPT writes *all* design docs (SPEC/WORKFLOW/ROADMAP/DECISIONS/PROGRESS + any design rules).
- Codex only applies that text into files and writes/edits code.
- If chat and repo ever disagree, the repo design docs (after handover) are the source of truth for future threads.

## Where to find what (tight + non-overlapping)
- `design/SPEC.md` = the full requirements (what + rules)
- `design/WORKFLOW.md` = the build order and how components connect
- `design/ROADMAP.md` = phases and sequencing (high-level)
- `design/DECISIONS.md` = everything we agreed (including “do this later”)
- `design/PROGRESS.md` = what’s done + what’s next (status)
- `instructions/*.md` = only “how to run commands” (no repeating the spec)
- `tools/*.py` = runnable scripts (each must contain a clear INSTRUCTION HEADER)
- `context_pack.md` = generated snapshot pasted into a new chat thread (never manually edited)

---

# AUTHORITATIVE PROJECT SPEC (FULL — from the first chat prompt)

We are continuing the SAME “Backtest” project. Treat this message as the authoritative context and do not ask me to repeat decisions already captured here unless truly necessary.

PROJECT GOAL
Build a modular, Excel-configured, high-performance research platform for:

Daily research across many proxies/indicators (CTA flows, VIX, xover spreads, Nasdaq proxy, 10y treasury proxy, etc.)

Intraday / trade-level research + charting + backtesting for E-mini ES using Databento

Realistic execution options, optimization, and overfitting/robustness tests

A clear workflow: ingest/store → charts → PnL/execution engine → feature engineering/caching → optimization + robustness

NON-NEGOTIABLE RULES (ENGINEERING + WORKFLOW)
2.1 Architecture / Modularity (very important)

Design must be as modular and low-coupling as possible.

Modules should depend on stable contracts/interfaces, not on each other’s implementations wherever possible.

Avoid deep import chains (A imports B imports C). Use thin “orchestrator” scripts to wire components together.

Prefer pure functions (input → output), minimize hidden global state.

Shared types/contracts go in one “common/contracts” area; other modules only import those contracts + config loader + stdlib.

2.2 Code vs notebooks

All business logic in .py modules.

Notebooks are thin runners + visualization consumers only (no core logic in notebooks).

2.3 Configuration

Excel is the single control plane: everything configured in Excel (including DATA_ROOT).

Python reads an exported JSON config snapshot (not the live open workbook).

All decisions (paths, features, gates, scaling, execution, optimization flags, constraints, robustness tests) must be spreadsheet-controlled.

2.4 Storage locations / portability

Code lives in OneDrive: C:\Users\pcash\OneDrive\Backtest

Data lives on SSD E: (speed prioritized): E:\BacktestData

Must be portable to other machines: use Excel PATH profiles so path changes require Excel edits only (not code edits).

Prioritise speed over disk space.

2.5 Performance

Use vectorization where possible.

Heavy aggregation should be pushed to DuckDB over partitioned Parquet, not Python loops over ticks.

Caching must exist (feature and derived table caches keyed by stable spec hashes and as_of_date).

2.6 Lookahead bias prevention

CTA flows are known after the close → default availability_lag_days = 1 for CTA-derived features.

Engine must enforce feature availability timing (decisions at time t may only use features available at/before that decision time).

DATA SOURCES / DATA REQUIREMENTS
3.1 Databento (intraday ES)

Using Databento for ES trades (trade-level, very detailed). Start with ~1 year, expand later to ~7 years.

Trades include aggressor-side flag (buyer vs seller aggressor) → enough to build CVD and footprint (bid/ask volume at price inferred from aggressor side).

Keep option open to later download order book / quotes to simulate more realistic fills.

Ingest BOTH sessions: RTH and FULL. Derive charts for BOTH.

3.2 Daily series (xlsx now, API later)

Daily datasets initially in xlsx (append-only), may move to API later with daily updates.

Must support multiple daily proxies/indicators, not hardcoded to CTA/SPX:
examples: CTA flows, VIX, xover spreads, volumes, Nasdaq proxy, 10y treasury proxy, SPX proxy, etc.

The same “expandability” requirement applies to any instrument/series mentioned.

CHARTING REQUIREMENTS (PHASE 0)
4.1 Start inside Jupyter

Interactive charts in Jupyter initially.

Must be easy to migrate later (so separate a “chart data API layer” from the UI layer).

4.2 Flexibility required

Candle charts with user-defined time horizons (e.g., 2m/5m/10m/any).

Volume footprint period must be definable separately from candle interval (e.g., 10m candles but 1m footprint buckets inside).

Big trade bubbles overlay; bubble definition adjustable interactively (e.g., size threshold in contracts, percentile, etc.).

Show/overlay CVD, footprint, simple volume.

Session selector: RTH vs FULL.

Few seconds per interaction is acceptable.

4.3 Footprint buckets

Base footprint bucket = 1 minute, with ability to aggregate to longer periods.

BACKTEST + OPTIMIZATION REQUIREMENTS
5.1 Instruments / series philosophy

Everything should be “series-first”: proxies/indicators should be first-class series, not only features.

Any series can be used as:

feature input

gating input

scaling input

charted series

potentially tradeable later

5.2 Phase 0 must support BOTH daily and intraday backtests

Daily backtests on proxies (e.g., SPX) and daily indicators (CTA, VIX etc.)

Intraday backtests on ES derived bars/metrics

5.3 Positioning

Long/short/flat allowed.

Discrete positions (-1, 0, +1) in Phase 0 baseline; scaling exists as a mode.

5.4 Gating (regime filters)

Gating can use ANY feature or engineered feature.

Must support multiple gates and logic (AND/OR/NOT).

Default: gating blocks entries only (exits always allowed).

Optional per-gate mode: block entries AND exits.

5.5 Scaling / vol control

Scaling based on features (most likely vol) to create vol-control strategies.

Default scaling rebalance = daily.

Provide override: rebalance scaling only when signal changes.

Scaling parameters must be spreadsheet-driven and optimizable.

5.6 Execution realism

Execution must be realistic and flexible.

Must support:

execution time (close/next open/next close/custom intraday)

execution price models (mid/bid-ask side-aware/spread-adjusted etc.)

commissions (must be parameters in the spreadsheet)

slippage/spread models (ticks/cents/%/bps; and potentially derived from quotes later)

All execution parameters are configured in Excel.

5.7 Optimization

Optimize on IS only.

OOS evaluation separately (default OOS = last X months, configurable).

Objective default: maximize total PnL over IS.

Constraints: none to many, configured in Excel; examples: Sharpe, MaxDD, turnover, min trades etc.

Allow selection of objective metric (PnL default) and secondary metric for Pareto plots.

“X to optimize” approach:

a parameter is only optimized if optimize_flag = “X”

everything else stays fixed from Excel config

Pareto line/frontier plotting option (at least for analysis; multi-objective optimization not required initially).

5.8 Robustness / overfitting tests (ALL available in Phase 0)
All must be implemented as configurable tests via Excel toggles/profiles:

IS vs OOS gap diagnostics

Walk-forward folds

Purge/embargo around split boundary

Bootstrap (block bootstrap preferred)

Placebo tests: signal shuffle and lag sweep (e.g., -10..+10)

Parameter stability / sensitivity plots (1D curves and 2D heatmaps)

Regime/subperiod robustness

Multiple-testing / selection-bias diagnostics (at least reporting; deeper later)

STORAGE ARCHITECTURE (SPEED-FIRST)
6.1 SSD layout concept
DATA_ROOT = E:\BacktestData (configured in Excel)

duckdb\research.duckdb

raw\ (immutable, append-only)

canonical\ (standardized)

derived\ (bars, footprint_base, cvd, events)

features_cache\

runs\ (run artifacts)

logs\

6.2 DuckDB role
DuckDB stores:

dataset/series registry and manifests (coverage, ingests, schemas)

derived table manifests (what built, params, coverage)

feature specs/hashes/dependencies metadata

run metadata and metrics

optimization trials summary + feasibility flags

robustness test outputs summary

Parquet stores heavy time series; DuckDB queries parquet for speed.

6.3 Derived tables required (intraday)
Derived from trades (RTH and FULL separately):

bars_1m

footprint_base_1m

cvd_1m

big_trade_events (or efficient event querying path)

WORKFLOW ORDER (MUST FOLLOW)
Build in this exact order:

Ingest + storage foundation (raw/canonical/registry)

Derived tables + flexible charts (validate data quality)

PnL/execution engine (daily + intraday, realistic execution options)

Feature system + caching

Optimization + constraints + robustness suite

EXCEL WORKBOOK SCHEMA (SHEETS + HEADERS MUST EXIST)
We will create config/run_config.xlsx with these sheets and columns (exact headers):

RUNBOOK:
StepNo, Task, CommandOrAction, Notes

PATHS:
Profile, IsActive, CODE_ROOT, DATA_ROOT, DUCKDB_FILE, RAW_DIR, CANONICAL_DIR, FEATURE_CACHE_DIR, RUNS_DIR, LOG_DIR, EXPORT_CONFIG_DIR, Notes

DATASETS:
dataset_id, dataset_type, source_type, source_path_or_id, update_mode, date_col, timestamp_tz, bar_frequency, known_time_rule, default_availability_lag_days, canonical_table_name, canonical_partition_keys, notes

INSTRUMENTS (series registry – includes instruments and indicators/proxies):
instrument_id, instrument_name, instrument_type, prices_dataset_id, open_col, high_col, low_col, close_col, return_model, tick_size, multiplier, currency, calendar, default_execution_time, default_execution_price_model, notes

FEATURE_LIBRARY:
feature_id, feature_name, dataset_id, instrument_scope, input_col, transform, params_json, lag_days, availability_lag_days, missing_policy, winsorize, normalize, tags, enabled, notes

ENGINEERED_FEATURES:
eng_feature_id, eng_feature_name, input_feature_ids, combine_transform, params_json, lag_days, availability_lag_days, missing_policy, tags, enabled, notes

FEATURE_SETS:
feature_set_id, description, include_tags, exclude_tags, include_feature_ids, exclude_feature_ids, max_features, enabled, notes

SIGNAL_TEMPLATES:
template_id, template_name, allowed_feature_inputs, supports_long_short, supports_flat, description, default_params_json, enabled, notes

STRATEGIES:
strategy_id, strategy_name, instrument_id, feature_set_id, signal_template_id, primary_feature_id, secondary_feature_id, feature_combo_rule, combo_weights_json, position_mode, position_values, enabled, notes

BACKTESTS:
backtest_id, strategy_id, start_date, end_date, oos_last_x_months, rebalance_rule, execution_time, execution_price_model, spread_model_type, spread_value, slippage_model_type, slippage_value, allow_flip_same_day, min_hold_days, cooldown_days, as_of_date, enabled, notes

OPTIMIZATION_STUDIES:
study_id, backtest_id, method, objective_primary_metric, objective_secondary_metric, objective_direction, optimize_on, n_trials, seed, save_top_n_artifacts, constraint_set_id, robustness_profile_id, enabled, notes

OPTIMIZATION_PARAMS:
study_id, param_scope, target_id, param_name, optimize_flag, param_type, low, high, step, choices, constraint_group, constraint_rule, default_value_if_not_optimized, notes

CONSTRAINT_SETS:
constraint_set_id, description, enabled, notes

CONSTRAINTS:
constraint_set_id, metric_id, scope, operator, threshold, enabled, severity, notes

METRICS:
metric_id, description, enabled, annualization_factor, risk_free_rate, notes

ROBUSTNESS_PROFILES:
robustness_profile_id, description, enabled, notes

ROBUSTNESS:
robustness_profile_id, test_id, enabled, param_json, notes

REPORTING:
report_id, target_type, target_id, generate_tearsheet, save_equity_curve, save_positions, save_signals, save_feature_debug, leaderboard_top_n, pareto_plot, output_formats, enabled, notes

(Feature combos are Phase 1+, but fields exist; in Phase 0 keep them unused.)

PHASE 0 vs PHASE 1+ (ROADMAP)
Phase 0 includes:

Ingest daily + intraday (Databento trades) with registry/manifests

Derived tables (bars_1m, footprint_base_1m, cvd_1m, big trades) for RTH and FULL

Interactive Jupyter charts with interval controls, footprint controls, bubble controls, session controls

Backtest engine supporting daily + intraday with realistic execution options

Gating + scaling + constraints + robustness framework (all available/configurable)

Optimization with optimize_flag controls; objective PnL IS; constraints optional; Pareto plots

Phase 1+ adds breadth and depth:

Feature combinations (linear combos, confirmations)

Additional signal templates; debounce/persistence/cooldown enhancements

More advanced execution using quotes/order book when added

Additional microstructure features and liquidity metrics

Multi-instrument portfolio construction and weighting

More formal multiple-testing adjustments (deflated Sharpe / SPA / reality check)

Optional migration of chart UI from Jupyter to local web app using same chart-data API

FILE UPDATE PROCESS TO CHATGPT (CODEx BRIDGE)
GitHub/SharePoint direct access is NOT reliable for ChatGPT in-chat. Therefore:

10.1 Codex Bridge policy

Codex has local access to the repo folder.

Whenever ChatGPT needs “latest files,” we generate a context snapshot using Codex and paste it into chat.

10.2 Context pack (“context_pack.md”) standard
When requested, Codex should create/overwrite a file context_pack.md in repo root that includes:

Repo metadata: timestamp, branch, HEAD commit, last 5 commits

Folder tree (2–3 levels, excluding .git and caches)

Key docs full text:

design/SPEC.md, design/WORKFLOW.md, design/ROADMAP.md

instructions/00_setup.md and 01_daily_workflow.md

Config:

latest exported JSON config from config/exports/

if none exists, list workbook sheet names + headers (not the whole binary)

SSD data layout (REQUIRED):
- Print the active PATHS values from the snapshot (at minimum):
  DATA_ROOT, RAW_DIR, CANONICAL_DIR, DUCKDB_FILE.
- Print shallow filesystem trees (depth ~2) for:
  DATA_ROOT, RAW_DIR, CANONICAL_DIR.
- Must not crash if the SSD path is missing or cannot be listed; print a clear message instead.

Code focus:

list changed files in last 5 commits

include full file if <300 lines else top 120 + bottom 60
Then Codex prints the entire context_pack.md content to terminal. The user pastes it into chat.

10.3 Doc Update Protocol (no manual writing)

When design changes are agreed in chat:

ChatGPT generates updated doc contents (SPEC/WORKFLOW/ROADMAP + instructions)

User pastes an instruction into Codex to:
a) archive current docs into archive/<timestamp>/...
b) overwrite current docs with new versions
c) optionally git commit + push
User does not manually write docs.

CURRENT STATUS

SSD is mounted as E: drive.

Project repo exists at C:\Users\pcash\OneDrive\Backtest but currently essentially empty (we are still finalizing spec/design before generating files and code).

Codex CLI is installed and working locally.
<CONTENT_END>

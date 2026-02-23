# WORKFLOW

## Build/Run Order
1. Ingest/store foundation.
2. Derived tables + chart data API + thin notebooks.
3. PnL/execution engine.
4. Feature system + caching.
5. Optimization + constraints + robustness.

## Commands We Will Implement
- `python tools/bootstrap_foundation.py`
- `python tools/make_run_config_xlsx.py`
- `python tools/verify_run_config_xlsx.py`
- `python tools/export_config_snapshot.py`
- `python tools/verify_duckdb_registry.py`
- `python tools/ingest_<source>.py` (placeholder)
- `python tools/build_derived.py` (placeholder)
- `python tools/run_backtests.py` (placeholder)
- `python tools/run_optimization.py` (placeholder)
- `python tools/make_context_pack.py`

# WORKFLOW

## Plain English Summary
Build the system in a strict order. Start with storage and config, then ingestion, then derived data and charts, then backtests, then features, then optimization.

## Build/Run Order
1. Ingest/store foundation.
2. Derived tables + chart data API + thin notebooks.
3. PnL/execution engine.
4. Feature system + caching.
5. Optimization + constraints + robustness.

## Commands We Will Implement
- `pybt tools/bootstrap_foundation.py`
- `pybt tools/make_run_config_xlsx.py`
- `pybt tools/verify_run_config_xlsx.py`
- `pybt tools/export_config_snapshot.py`
- `pybt tools/verify_duckdb_registry.py`
- `pybt tools/ingest_<source>.py` (placeholder)
- `pybt tools/build_derived.py` (placeholder)
- `pybt tools/run_backtests.py` (placeholder)
- `pybt tools/run_optimization.py` (placeholder)
- `pybt tools/make_context_pack.py`

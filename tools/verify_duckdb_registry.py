"""
INSTRUCTION HEADER
Purpose: Verify the DuckDB registry exists and contains required tables.
Inputs: Reads `E:\\BacktestData\\duckdb\\research.duckdb`.
Outputs: None (prints results, exits non-zero on failure).
How to run: `pybt tools/verify_duckdb_registry.py`
Also: `C:\\Users\\pcash\\anaconda3\\envs\\backtest\\python.exe tools\\verify_duckdb_registry.py`
Success looks like: `DuckDB registry verification passed.`
Common failures and fixes:
- Module not found (duckdb): run `pybt -m pip install duckdb`.
- Database missing: run `pybt tools/bootstrap_foundation.py`.
"""

from __future__ import annotations

from pathlib import Path
import sys

import duckdb


def main() -> int:
    """Validate that required registry tables exist."""
    db_path = Path("E:/BacktestData/duckdb/research.duckdb")
    if not db_path.exists():
        print(f"Missing DuckDB file: {db_path}")
        return 1

    required = {
        "meta_schema_version",
        "registry_datasets",
        "registry_instruments",
        "manifest_derived_tables",
        "manifest_feature_cache",
        "runs",
        "run_metrics",
    }

    con = duckdb.connect(str(db_path))
    tables = {
        row[0]
        for row in con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
        ).fetchall()
    }
    con.close()

    missing = sorted(required - tables)
    if missing:
        print("Missing tables:", ", ".join(missing))
        return 1

    print("DuckDB registry verification passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

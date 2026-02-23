from __future__ import annotations

from pathlib import Path
import sys

import duckdb


def main() -> int:
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

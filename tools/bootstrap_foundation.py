"""
INSTRUCTION HEADER

What this file does (plain English):
- Sets up the project “data area” on your SSD (creates folders on E:).
- Creates the DuckDB “registry” database file (a small database that stores metadata).
- Verifies the Excel config workbook exists and is correct.
- Exports the Excel config to a JSON snapshot so Python can read config reproducibly.

Technical summary:
- Data root (SSD): E:\\BacktestData
- DuckDB registry file: E:\\BacktestData\\duckdb\\research.duckdb
- Config workbook: <repo>\\config\\run_config.xlsx
- Config snapshot output: <repo>\\config\\exports\\config_snapshot_latest.json

Inputs:
- Uses paths under E:\\BacktestData (SSD).
- Reads the Excel workbook at config\\run_config.xlsx (the “control plane”).
- Calls: tools\\verify_run_config_xlsx.py and tools\\export_config_snapshot.py

Outputs:
- Creates folders under E:\\BacktestData\\... (raw, canonical, derived, features_cache, runs, logs).
- Creates/updates DuckDB tables inside research.duckdb.
- Writes a JSON config snapshot in config\\exports\\ (also updates config_snapshot_latest.json).

How to run:
- From repo root PowerShell:
  pybt tools/bootstrap_foundation.py
- If you don’t have the pybt alias:
  C:\\Users\\pcash\\anaconda3\\envs\\backtest\\python.exe tools\\bootstrap_foundation.py

What success looks like:
- Prints “Foundation bootstrap complete.”
- Prints the list of created/verified paths
- Prints the latest snapshot path

Common failures and fixes:
- “ModuleNotFoundError: duckdb” -> install it:
  pybt -m pip install duckdb
- “E:\\ drive missing” -> check SSD is mounted as E: and retry.
- “Workbook verification failed / workbook missing” ->
  run: pybt tools/make_run_config_xlsx.py then pybt tools/verify_run_config_xlsx.py
"""

from __future__ import annotations

from pathlib import Path
import subprocess
import sys
import datetime as dt

import duckdb


def _repo_root() -> Path:
    """Return the repository root folder based on this file location."""
    return Path(__file__).resolve().parents[1]


def _ensure_dirs(paths: list[Path]) -> None:
    """Create folders if they do not exist."""
    for p in paths:
        p.mkdir(parents=True, exist_ok=True)


def _init_duckdb(db_path: Path) -> None:
    """Create required DuckDB registry tables if they do not exist."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS meta_schema_version (
            schema_version INTEGER,
            applied_at TIMESTAMP
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS registry_datasets (
            dataset_id VARCHAR PRIMARY KEY,
            dataset_type VARCHAR,
            source_type VARCHAR,
            spec_json VARCHAR,
            updated_at TIMESTAMP
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS registry_instruments (
            instrument_id VARCHAR PRIMARY KEY,
            instrument_type VARCHAR,
            spec_json VARCHAR,
            updated_at TIMESTAMP
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS manifest_derived_tables (
            derived_id VARCHAR,
            table_name VARCHAR,
            spec_hash VARCHAR,
            session VARCHAR,
            coverage_start TIMESTAMP,
            coverage_end TIMESTAMP,
            parquet_path VARCHAR,
            created_at TIMESTAMP
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS manifest_feature_cache (
            feature_hash VARCHAR,
            feature_id VARCHAR,
            as_of_date DATE,
            parquet_path VARCHAR,
            created_at TIMESTAMP
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS runs (
            run_id VARCHAR PRIMARY KEY,
            run_type VARCHAR,
            spec_hash VARCHAR,
            started_at TIMESTAMP,
            ended_at TIMESTAMP,
            status VARCHAR,
            notes VARCHAR
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS run_metrics (
            run_id VARCHAR,
            metric_id VARCHAR,
            value DOUBLE,
            created_at TIMESTAMP
        )
        """
    )

    existing = con.execute("SELECT COUNT(*) FROM meta_schema_version").fetchone()[0]
    if existing == 0:
        con.execute(
            "INSERT INTO meta_schema_version VALUES (?, ?)",
            [1, dt.datetime.now()],
        )
    con.close()


def _run(cmd: list[str]) -> None:
    """Run a subprocess command and raise on failure."""
    subprocess.run(cmd, check=True)


def main() -> int:
    """Create SSD folder layout, init DuckDB, and export config snapshot."""
    data_root = Path("E:/BacktestData")
    db_path = data_root / "duckdb" / "research.duckdb"

    dirs = [
        data_root,
        data_root / "duckdb",
        data_root / "raw",
        data_root / "canonical",
        data_root / "derived",
        data_root / "derived" / "bars_1m",
        data_root / "derived" / "footprint_base_1m",
        data_root / "derived" / "cvd_1m",
        data_root / "derived" / "big_trade_events",
        data_root / "features_cache",
        data_root / "runs",
        data_root / "logs",
    ]
    _ensure_dirs(dirs)
    _init_duckdb(db_path)

    repo_root = _repo_root()
    _run([sys.executable, str(repo_root / "tools" / "verify_run_config_xlsx.py")])
    _run([sys.executable, str(repo_root / "tools" / "export_config_snapshot.py")])

    latest_snapshot = repo_root / "config" / "exports" / "config_snapshot_latest.json"

    print("Foundation bootstrap complete.")
    print("Created/verified paths:")
    for p in dirs:
        print(f"  {p}")
    print(f"Latest snapshot: {latest_snapshot}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

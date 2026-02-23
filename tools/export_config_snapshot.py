"""
INSTRUCTION HEADER
Purpose: Export the Excel control plane to JSON snapshots.
Inputs: Reads `config/run_config.xlsx`.
Outputs: Writes `config/exports/config_snapshot_<YYYYMMDD_HHMMSS>.json`
and `config/exports/config_snapshot_latest.json`.
How to run: `pybt tools/export_config_snapshot.py`
Also: `C:\\Users\\pcash\\anaconda3\\envs\\backtest\\python.exe tools\\export_config_snapshot.py`
Success looks like: printed paths for the timestamped snapshot and latest snapshot.
Common failures and fixes:
- Module not found (openpyxl or orjson): run `pybt -m pip install openpyxl orjson`.
- Missing workbook: run `pybt tools/make_run_config_xlsx.py`.
"""

from __future__ import annotations

from pathlib import Path
import datetime as dt
import sys


def _repo_root() -> Path:
    """Return the repository root folder based on this file location."""
    return Path(__file__).resolve().parents[1]


def main() -> int:
    """Export snapshots using the schema-aware exporter."""
    repo_root = _repo_root()
    sys.path.insert(0, str(repo_root / "src"))
    from backtest.config.export_snapshot import export_snapshot

    xlsx_path = repo_root / "config" / "run_config.xlsx"
    exports_dir = repo_root / "config" / "exports"
    exports_dir.mkdir(parents=True, exist_ok=True)

    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    latest_path = exports_dir / "config_snapshot_latest.json"
    stamped_path = exports_dir / f"config_snapshot_{ts}.json"

    export_snapshot(xlsx_path, stamped_path)
    export_snapshot(xlsx_path, latest_path)

    print(f"Wrote snapshot: {stamped_path}")
    print(f"Wrote latest : {latest_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

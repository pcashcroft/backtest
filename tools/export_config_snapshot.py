from __future__ import annotations

from pathlib import Path
import datetime as dt
import sys


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def main() -> int:
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

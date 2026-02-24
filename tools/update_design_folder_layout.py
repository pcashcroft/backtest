"""
INSTRUCTION HEADER

What this file does (plain English):
- Writes design/FOLDER_LAYOUT.md so the repo contains an always-up-to-date reference for:
  - CODE_ROOT and SSD paths (from config snapshot when available)
  - Repo folder layout (depth-limited)
  - SSD folder layout (depth-limited) for DATA_ROOT, RAW_DIR, CANONICAL_DIR
- This file is designed for handovers so a new thread can pick up with no other knowledge.

Where to run:
- Run from repo root: C:/Users/pcash/OneDrive/Backtest

Inputs:
- config/exports/config_snapshot_latest.json (optional; if missing we still write a file)

Outputs:
- design/FOLDER_LAYOUT.md

How to run:
C:/Users/pcash/anaconda3/envs/backtest/python.exe tools/update_design_folder_layout.py

What success looks like:
- Prints the output path and the active PATHS values it found (if snapshot exists)
- design/FOLDER_LAYOUT.md is written/overwritten

Common failures + fixes:
- If E:/ drive is missing or blocked, the file still writes but shows "(missing)" or "(cannot list)" lines.
"""

from __future__ import annotations

import datetime as dt
import json
import re
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
SNAPSHOT_PATH = REPO_ROOT / "config" / "exports" / "config_snapshot_latest.json"
OUT_PATH = REPO_ROOT / "design" / "FOLDER_LAYOUT.md"


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="replace")


def _load_snapshot(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        return json.loads(_read_text(path))
    except Exception:
        return None


def _pick_active_paths_row(snapshot: dict[str, Any]) -> dict[str, Any] | None:
    rows = snapshot.get("sheets", {}).get("PATHS", [])
    if not isinstance(rows, list):
        return None

    def is_true(v: Any) -> bool:
        if isinstance(v, bool):
            return v
        if v is None:
            return False
        s = str(v).strip().lower()
        return s in ("true", "1", "yes", "y", "x")

    for r in rows:
        if isinstance(r, dict) and is_true(r.get("IsActive")):
            return r

    if rows and isinstance(rows[0], dict):
        return rows[0]
    return None


def _safe_iterdir(path: Path):
    try:
        return sorted(path.iterdir(), key=lambda p: (p.is_file(), p.name.lower()))
    except Exception as exc:
        return exc


def _tree(root: Path, depth: int) -> list[str]:
    root_str = str(root)
    if not root.exists():
        return [f"(missing) {root_str}"]

    lines: list[str] = []

    def walk(p: Path, d: int) -> None:
        if d > depth:
            return
        entries = _safe_iterdir(p)
        if isinstance(entries, Exception):
            lines.append(f"(cannot list) {str(p)} ({entries})")
            return

        for e in entries:
            if e.name.startswith("~$") or e.name == "Thumbs.db":
                continue
            indent = "  " * (d - 1)
            lines.append(f"{indent}{str(e)}")
            if e.is_dir():
                walk(e, d + 1)

    walk(root, 1)
    return lines


def _repo_tree(depth: int = 3) -> list[str]:
    excluded = {
        ".git",
        "__pycache__",
        ".venv",
        "venv",
        ".ipynb_checkpoints",
        ".pytest_cache",
        ".mypy_cache",
        ".ruff_cache",
        ".DS_Store",
    }

    lines: list[str] = []

    def walk(p: Path, d: int) -> None:
        if d > depth:
            return
        entries = _safe_iterdir(p)
        if isinstance(entries, Exception):
            lines.append(f"(cannot list) {str(p)} ({entries})")
            return

        for e in entries:
            if e.name in excluded or e.name.startswith("~$") or e.name == "Thumbs.db":
                continue
            rel = e.relative_to(REPO_ROOT).as_posix()
            indent = "  " * (d - 1)
            lines.append(f"{indent}{rel}")
            if e.is_dir():
                walk(e, d + 1)

    walk(REPO_ROOT, 1)
    return lines


def _list_date_dirs(session_dir: Path) -> list[str]:
    if not session_dir.exists():
        return []
    entries = _safe_iterdir(session_dir)
    if isinstance(entries, Exception):
        return []
    dates: list[str] = []
    for e in entries:
        if not e.is_dir():
            continue
        name = e.name
        if name.startswith("date="):
            name = name.split("date=", 1)[1]
        if re.match(r"^\d{4}-\d{2}-\d{2}$", name):
            dates.append(name)
    return sorted(set(dates))


def _date_partitions(base: Path, session: str) -> dict[str, Any]:
    hive_dir = base / f"session={session}"
    plain_dir = base / session
    hive_dates = _list_date_dirs(hive_dir)
    plain_dates = _list_date_dirs(plain_dir)
    return {
        "exists_hive": hive_dir.exists(),
        "exists_plain": plain_dir.exists(),
        "hive_dates": hive_dates,
        "plain_dates": plain_dates,
    }


def _list_year_dirs(base: Path) -> dict[str, Any]:
    hive_years: list[int] = []
    plain_years: list[int] = []

    if base.exists():
        entries = _safe_iterdir(base)
        if not isinstance(entries, Exception):
            for e in entries:
                if not e.is_dir():
                    continue
                name = e.name
                if name.startswith("year="):
                    val = name.split("year=", 1)[1]
                    if re.match(r"^\d{4}$", val):
                        hive_years.append(int(val))
                elif re.match(r"^\d{4}$", name):
                    plain_years.append(int(name))

    return {
        "exists_hive": len(hive_years) > 0,
        "exists_plain": len(plain_years) > 0,
        "hive_years": sorted(set(hive_years)),
        "plain_years": sorted(set(plain_years)),
    }


def _format_range(items: list[str]) -> str:
    if not items:
        return "none"
    return f"{items[0]} .. {items[-1]}"


def _format_years(items: list[int]) -> str:
    if not items:
        return "none"
    return f"{items[0]} .. {items[-1]}"


def main() -> int:
    snapshot = _load_snapshot(SNAPSHOT_PATH)
    paths_row = _pick_active_paths_row(snapshot) if snapshot else None

    code_root = (paths_row.get("CODE_ROOT") if paths_row else None) or str(REPO_ROOT)
    data_root = (paths_row.get("DATA_ROOT") if paths_row else None) or "E:/BacktestData"
    raw_dir = (paths_row.get("RAW_DIR") if paths_row else None) or f"{data_root}/raw"
    canonical_dir = (paths_row.get("CANONICAL_DIR") if paths_row else None) or f"{data_root}/canonical"
    duckdb_file = (paths_row.get("DUCKDB_FILE") if paths_row else None) or f"{data_root}/duckdb/research.duckdb"

    code_root_p = Path(str(code_root))
    data_root_p = Path(str(data_root))
    raw_dir_p = Path(str(raw_dir))
    canonical_dir_p = Path(str(canonical_dir))

    now = dt.datetime.now().isoformat(timespec="seconds")

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    parts: list[str] = []
    parts.append("# Folder layout")
    parts.append("")
    parts.append("This file is kept up to date for handovers.")
    parts.append("It is auto-generated by `tools/update_design_folder_layout.py`.")
    parts.append("")
    parts.append(f"Last generated: {now}")
    parts.append("")
    parts.append("## Key roots")
    parts.append("")
    parts.append("```text")
    parts.append(f"CODE_ROOT: {code_root}")
    parts.append(f"DATA_ROOT: {data_root}")
    parts.append(f"RAW_DIR: {raw_dir}")
    parts.append(f"CANONICAL_DIR: {canonical_dir}")
    parts.append(f"DUCKDB_FILE: {duckdb_file}")
    parts.append("```")
    parts.append("")

    parts.append("## Coverage summary (canonical datasets)")
    parts.append("```text")
    es_trades_base = canonical_dir_p / "es_trades"
    es_ohlcv_base = canonical_dir_p / "es_ohlcv_1s"
    daily_series_base = canonical_dir_p / "daily_series"

    for label, base in [("ES trades", es_trades_base), ("ES OHLCV 1s", es_ohlcv_base)]:
        for session in ["FULL", "RTH"]:
            info = _date_partitions(base, session)
            hive_dates = info["hive_dates"]
            plain_dates = info["plain_dates"]
            parts.append(
                f"{label} {session}: "
                f"layout hive={info['exists_hive']} plain={info['exists_plain']}; "
                f"hive_dates={len(hive_dates)} range={_format_range(hive_dates)}; "
                f"plain_dates={len(plain_dates)} range={_format_range(plain_dates)}"
            )

    years = _list_year_dirs(daily_series_base)
    parts.append(
        "daily_series years: "
        f"layout hive={years['exists_hive']} plain={years['exists_plain']}; "
        f"hive_years={len(years['hive_years'])} range={_format_years(years['hive_years'])}; "
        f"plain_years={len(years['plain_years'])} range={_format_years(years['plain_years'])}"
    )
    parts.append("```")
    parts.append("")

    parts.append("## Repo tree (depth=3)")
    parts.append("")
    parts.append("```text")
    parts.extend(_repo_tree(depth=3))
    parts.append("```")
    parts.append("")

    parts.append("## SSD trees (depth=2)")
    parts.append("")
    parts.append("### DATA_ROOT")
    parts.append("```text")
    parts.extend(_tree(data_root_p, depth=2))
    parts.append("```")
    parts.append("")
    parts.append("### RAW_DIR")
    parts.append("```text")
    parts.extend(_tree(raw_dir_p, depth=2))
    parts.append("```")
    parts.append("")
    parts.append("### CANONICAL_DIR")
    parts.append("```text")
    parts.extend(_tree(canonical_dir_p, depth=2))
    parts.append("```")
    parts.append("")

    OUT_PATH.write_text("\n".join(parts) + "\n", encoding="utf-8", errors="replace")

    print(f"Wrote: {OUT_PATH}")
    if paths_row:
        print("Active PATHS row values used:")
        print(f"  CODE_ROOT: {code_root}")
        print(f"  DATA_ROOT: {data_root}")
        print(f"  RAW_DIR: {raw_dir}")
        print(f"  CANONICAL_DIR: {canonical_dir}")
        print(f"  DUCKDB_FILE: {duckdb_file}")
    else:
        print("No snapshot PATHS row found; used defaults for DATA_ROOT/RAW_DIR/CANONICAL_DIR.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""
INSTRUCTION HEADER

What this file does (plain English):
- Loads the JSON config snapshot from disk and returns it as a Python dict.
- Validates that all expected sheets (as defined in HEADERS) are present in the
  snapshot before returning, so callers get a clear error if the snapshot is
  stale or incomplete rather than a cryptic KeyError later.
- Uses orjson for speed if available, falls back to stdlib json automatically.
- Main export: load_snapshot(json_path) -> dict

Where it runs: Imported by data modules (e.g. big_trades.py) and Jupyter
  notebooks that need the config at runtime. Never run directly as a script.
Inputs:  json_path — path to config_snapshot_latest.json (or any snapshot file).
Outputs: Dict with keys: exported_at, source_xlsx, schema, sheets.
Common failures + fixes:
  - "Snapshot missing sheet": re-export via tools/admin/export_config_snapshot.py.
  - File not found: run export_config_snapshot.py first to generate the snapshot.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .schema import HEADERS


def _json_loads(raw: bytes) -> Any:
    try:
        import orjson
        return orjson.loads(raw)
    except ImportError:
        import json
        return json.loads(raw)


def load_snapshot(json_path: str | Path) -> dict[str, Any]:
    json_path = Path(json_path)
    data = _json_loads(json_path.read_bytes())

    if "sheets" not in data or not isinstance(data["sheets"], dict):
        raise ValueError("Snapshot missing 'sheets' dict.")

    for sheet_name in HEADERS.keys():
        if sheet_name not in data["sheets"]:
            raise ValueError(f"Snapshot missing sheet: {sheet_name}")

    return data

"""
INSTRUCTION HEADER

What this file does (plain English):
- Library function that reads run_config.xlsx and writes all sheet data to a
  JSON snapshot file on disk.
- The snapshot is what downstream code actually reads at runtime — data loaders,
  the backtest engine, and Jupyter notebooks all call load_snapshot() on this
  file rather than opening the live Excel workbook directly.
- Stamps the output with an exported_at timestamp and the source xlsx path for
  traceability.
- Main export: export_snapshot(xlsx_path, output_path) -> Path

Where it runs: Called by tools/admin/export_config_snapshot.py (the CLI
  wrapper that re-exports after every config change). Never run directly.
Inputs:  xlsx_path — path to run_config.xlsx.
         output_path — destination for the JSON snapshot file.
Outputs: JSON file written to output_path; returns the output Path.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable
import datetime as dt

from .excel_io import read_sheets_as_records
from .schema import HEADERS


def _get_json_dumps() -> Callable[[Any], bytes]:
    try:
        import orjson

        return lambda obj: orjson.dumps(obj, option=orjson.OPT_INDENT_2)
    except Exception:
        import json

        return lambda obj: json.dumps(obj, indent=2).encode("utf-8")


def export_snapshot(xlsx_path: str | Path, output_path: str | Path) -> Path:
    xlsx_path = Path(xlsx_path)
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    payload = {
        "exported_at": dt.datetime.now().isoformat(timespec="seconds"),
        "source_xlsx": str(xlsx_path),
        "schema": HEADERS,
        "sheets": read_sheets_as_records(xlsx_path),
    }

    dumps = _get_json_dumps()
    output_path.write_bytes(dumps(payload))
    return output_path

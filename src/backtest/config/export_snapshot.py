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

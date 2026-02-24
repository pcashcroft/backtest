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

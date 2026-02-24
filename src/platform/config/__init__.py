"""
INSTRUCTION HEADER

What this file does (plain English):
- Exports the public API of the platform.config subpackage so callers only
  need a single import line, e.g. `from platform.config import load_snapshot`.
- Re-exports four things:
    HEADERS              — dict mapping each Excel sheet name to its column list
    load_snapshot        — load the JSON config snapshot into a Python dict
    export_snapshot      — read run_config.xlsx and write it out as a JSON snapshot
    read_sheets_as_records — read every sheet of the xlsx into a list of dicts

Where it runs: Imported by tools and data modules. Never run directly.
"""

from .schema import HEADERS
from .load_snapshot import load_snapshot
from .export_snapshot import export_snapshot
from .excel_io import read_sheets_as_records

__all__ = ["HEADERS", "load_snapshot", "export_snapshot", "read_sheets_as_records"]

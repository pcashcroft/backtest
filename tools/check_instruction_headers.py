"""
INSTRUCTION HEADER
What this file does: Checks that files contain the required Instruction Header marker.
Where it runs: Terminal [command line] on Windows.
Inputs: Files in `src/`, `tools/`, `tests/`, and `notebooks/`.
Outputs: Prints failures and exits non-zero if any are missing.
How to run: `pybt tools/check_instruction_headers.py`
What success looks like: Prints `OK` and exits with code 0.
Common failures + fixes: Add an Instruction Header to missing files.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path


MARKER = "INSTRUCTION HEADER"
ROOTS = ["src", "tools", "tests", "notebooks"]
IGNORE_DIRS = {".ipynb_checkpoints", "archive", "config", "__pycache__"}
IGNORE_FILES = {"context_pack.md"}


def _is_ignored_dir(path: Path) -> bool:
    return path.name in IGNORE_DIRS


def _is_ignored_file(path: Path) -> bool:
    return path.name in IGNORE_FILES


def _find_py_issue(path: Path) -> str | None:
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except Exception as exc:
        return f"cannot read file: {exc}"

    head = lines[:40]
    head_text = "\n".join(head)
    if MARKER not in head_text:
        return "missing marker in top 40 lines"

    first_line = ""
    for line in head:
        stripped = line.strip()
        if stripped == "" or stripped.startswith("#!"):
            continue
        if stripped.lower().startswith("# -*- coding:"):
            continue
        first_line = stripped
        break

    if not (first_line.startswith('"""') or first_line.startswith("'''")):
        return "missing top-of-file module docstring"

    return None


def _find_ipynb_issue(path: Path) -> str | None:
    try:
        data = json.loads(path.read_text(encoding="utf-8", errors="replace"))
    except Exception as exc:
        return f"cannot read notebook: {exc}"

    cells = data.get("cells", [])
    if not cells:
        return "notebook has no cells"

    first = cells[0]
    if first.get("cell_type") != "markdown":
        return "first cell is not markdown"

    source = first.get("source", [])
    if isinstance(source, list):
        text = "".join(source)
    else:
        text = str(source)

    if MARKER not in text:
        return "missing marker in first markdown cell"

    return None


def main() -> int:
    repo_root = Path(__file__).resolve().parents[1]
    failures: list[str] = []

    for root in ROOTS:
        start = repo_root / root
        if not start.exists():
            continue

        for dirpath, dirnames, filenames in os.walk(start):
            dirnames[:] = [d for d in dirnames if not _is_ignored_dir(Path(d))]

            for fname in filenames:
                path = Path(dirpath) / fname
                rel = path.relative_to(repo_root)

                if _is_ignored_file(path):
                    continue
                if "archive" in rel.parts:
                    continue
                if "config" in rel.parts and "exports" in rel.parts:
                    continue

                if path.suffix == ".py":
                    issue = _find_py_issue(path)
                elif path.suffix == ".ipynb":
                    issue = _find_ipynb_issue(path)
                else:
                    continue

                if issue:
                    failures.append(f"{rel.as_posix()}: {issue}")

    if failures:
        print("Instruction Header check failed:")
        for item in failures:
            print(f"- {item}")
        return 1

    print("OK: All files contain the Instruction Header.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

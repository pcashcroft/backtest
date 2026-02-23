"""
INSTRUCTION HEADER

What this script does (plain English):
- Creates context_pack.md in the repo root.
- context_pack.md is what you paste into a NEW chat thread so the new thread
  has the full project memory (spec + decisions + progress + config snapshot + key code).

Where to run:
- Run from repo root: C:\\Users\\pcash\\OneDrive\\Backtest

How to run:
- pybt tools/make_context_pack.py
  (or use your conda python path)

What success looks like:
- It prints the contents of context_pack.md to the terminal.
- It also writes/overwrites context_pack.md in the repo root.

Notes:
- This script must never crash due to encoding/binary files.
- It will not print binary file contents (xlsx, dbn, parquet, etc.). It will only note they exist.
"""

from __future__ import annotations



from pathlib import Path
import datetime as dt
import subprocess
import sys
import json


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _run_git(args: list[str]) -> str:
    try:
        out = subprocess.check_output(["git", *args], stderr=subprocess.STDOUT)
        return out.decode("utf-8", errors="replace").strip()
    except Exception:
        return ""


def _folder_tree(root: Path, max_depth: int = 3) -> list[str]:
    excluded = {
        ".git",
        "__pycache__",
        ".venv",
        "venv",
        ".ipynb_checkpoints",
        ".pytest_cache",
        ".mypy_cache",
        ".ruff_cache",
        ".ruff_cache",
        ".DS_Store",
    }
    lines: list[str] = []
    root = root.resolve()

    def walk(path: Path, depth: int) -> None:
        if depth > max_depth:
            return
        entries = sorted(path.iterdir(), key=lambda p: (p.is_file(), p.name.lower()))
        for entry in entries:
            if entry.name in excluded:
                continue
            rel = entry.relative_to(root)
            indent = "  " * (depth - 1)
            lines.append(f"{indent}{rel.as_posix()}")
            if entry.is_dir():
                walk(entry, depth + 1)

    walk(root, 1)
    return lines


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="replace")


_BINARY_SUFFIXES = {
    ".xlsx", ".xlsm", ".xls", ".dbn", ".zst", ".parquet", ".png", ".jpg", ".jpeg", ".gif",
    ".pdf", ".zip", ".whl", ".exe", ".dll",
}


def _include_file(path: Path) -> str:
    if not path.exists() or not path.is_file():
        return f"(missing) {path.as_posix()}"

    if path.suffix.lower() in _BINARY_SUFFIXES:
        size = path.stat().st_size
        return f"(binary file omitted) {path.name}  size={size} bytes"

    text = _read_text(path)
    lines = text.splitlines()
    if len(lines) < 300:
        return "\n".join(lines)
    top = "\n".join(lines[:120])
    bottom = "\n".join(lines[-60:])
    return f"{top}\n...\n{bottom}"


def _git_changed_files_last_commits(n: int = 5) -> list[Path]:
    out = _run_git(["log", f"-n{n}", "--name-only", "--pretty=format:"])
    if not out:
        return []
    files: list[str] = []
    for line in out.splitlines():
        line = line.strip()
        if line:
            files.append(line)

    uniq: list[Path] = []
    seen: set[str] = set()
    for f in files:
        if f not in seen:
            seen.add(f)
            uniq.append(Path(f))
    return uniq


def _load_snapshot_safe(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8", errors="replace"))
    except Exception:
        return {}


def _is_truthy(val: object) -> bool:
    if isinstance(val, bool):
        return val
    if isinstance(val, (int, float)):
        return val != 0
    if isinstance(val, str):
        return val.strip().lower() in {"1", "true", "yes", "y"}
    return False


def _select_active_paths_row(snapshot: dict) -> dict:
    sheets = snapshot.get("sheets", {})
    rows = sheets.get("PATHS", [])
    for row in rows:
        if _is_truthy(row.get("IsActive")):
            return row
    return {}


def _safe_tree(root: Path, max_depth: int = 2, max_entries: int = 200) -> list[str]:
    lines: list[str] = []
    try:
        if not root.exists():
            return [f"(missing) {root}"]
    except Exception as exc:
        return [f"(cannot access) {root} ({exc})"]

    def walk(path: Path, depth: int) -> None:
        if len(lines) >= max_entries:
            return
        if depth > max_depth:
            return
        try:
            entries = sorted(path.iterdir(), key=lambda p: (p.is_file(), p.name.lower()))
        except Exception as exc:
            lines.append(f"{'  ' * (depth - 1)}(cannot list) {path} ({exc})")
            return
        for entry in entries:
            if len(lines) >= max_entries:
                return
            rel = entry
            indent = "  " * (depth - 1)
            lines.append(f"{indent}{rel}")
            if entry.is_dir():
                walk(entry, depth + 1)

    walk(root, 1)
    if len(lines) >= max_entries:
        lines.append("(truncated)")
    return lines


def main() -> int:
    repo_root = _repo_root()
    ts = dt.datetime.now().isoformat(timespec="seconds")
    branch = _run_git(["rev-parse", "--abbrev-ref", "HEAD"]) or "(no git)"
    head = _run_git(["rev-parse", "HEAD"]) or "(no git)"
    last5 = _run_git(["log", "-n5", "--oneline"]) or "(no git)"

    # Key docs included every time (this is the memory for new threads)
    key_docs = [
        repo_root / "design" / "SPEC.md",
        repo_root / "design" / "WORKFLOW.md",
        repo_root / "design" / "ROADMAP.md",
        repo_root / "design" / "DOC_UPDATE_PROTOCOL.md",
        repo_root / "design" / "DECISIONS.md",
        repo_root / "design" / "PROGRESS.md",
        repo_root / "instructions" / "00_setup.md",
        repo_root / "instructions" / "01_daily_workflow.md",
        repo_root / "instructions" / "STYLE_GUIDE.md",
    ]

    parts: list[str] = []
    parts.append("# Context Pack")
    parts.append("")
    parts.append("## Repo Metadata")
    parts.append(f"- Timestamp: {ts}")
    parts.append(f"- Branch: {branch}")
    parts.append(f"- HEAD: {head}")
    parts.append("### Last 5 Commits")
    parts.append("```text")
    parts.append(last5)
    parts.append("```")
    parts.append("")

    # Handover Summary (so new thread sees the important stuff immediately)
    decisions = repo_root / "design" / "DECISIONS.md"
    progress = repo_root / "design" / "PROGRESS.md"
    if decisions.exists() or progress.exists():
        parts.append("## Handover Summary")
        if progress.exists():
            parts.append("### Progress (full file below in Key Docs)")
            parts.append("```text")
            parts.append(_include_file(progress))
            parts.append("```")
        if decisions.exists():
            parts.append("### Recent decisions (full file below in Key Docs)")
            parts.append("```text")
            # show only the bottom ~60 lines so it’s not too repetitive
            txt = _read_text(decisions).splitlines()
            tail = "\n".join(txt[-60:]) if len(txt) > 60 else "\n".join(txt)
            parts.append(tail)
            parts.append("```")
        parts.append("")

    parts.append("## Folder Tree (2-3 levels)")
    parts.append("```text")
    parts.extend(_folder_tree(repo_root, max_depth=3))
    parts.append("```")
    parts.append("")

    parts.append("## Key Docs")
    for doc in key_docs:
        parts.append(f"### {doc.relative_to(repo_root).as_posix()}")
        parts.append("```text")
        parts.append(_include_file(doc))
        parts.append("```")
        parts.append("")

    parts.append("## Config")
    latest_snapshot = repo_root / "config" / "exports" / "config_snapshot_latest.json"
    if latest_snapshot.exists():
        parts.append("### config/exports/config_snapshot_latest.json")
        parts.append("```text")
        parts.append(_include_file(latest_snapshot))
        parts.append("```")
    else:
        parts.append("(no config snapshot found)")
    parts.append("")

    # SSD Data Layout
    snapshot = _load_snapshot_safe(latest_snapshot) if latest_snapshot.exists() else {}
    paths_row = _select_active_paths_row(snapshot)
    data_root = paths_row.get("DATA_ROOT", "E:/BacktestData") if paths_row else "E:/BacktestData"
    raw_dir = paths_row.get("RAW_DIR", "E:/BacktestData/raw") if paths_row else "E:/BacktestData/raw"
    canonical_dir = paths_row.get("CANONICAL_DIR", "E:/BacktestData/canonical") if paths_row else "E:/BacktestData/canonical"
    duckdb_file = paths_row.get("DUCKDB_FILE", "E:/BacktestData/duckdb/research.duckdb") if paths_row else "E:/BacktestData/duckdb/research.duckdb"

    parts.append("## SSD Data Layout (from snapshot + filesystem)")
    parts.append("### SSD Paths (active PATHS row)")
    parts.append("```text")
    parts.append(f"DATA_ROOT: {data_root}")
    parts.append(f"RAW_DIR: {raw_dir}")
    parts.append(f"CANONICAL_DIR: {canonical_dir}")
    parts.append(f"DUCKDB_FILE: {duckdb_file}")
    parts.append("```")
    parts.append("")

    for label, root in [("DATA_ROOT", data_root), ("RAW_DIR", raw_dir), ("CANONICAL_DIR", canonical_dir)]:
        parts.append(f"### {label} tree (depth=2)")
        parts.append("```text")
        try:
            lines = _safe_tree(Path(root), max_depth=2, max_entries=200)
        except Exception as exc:
            lines = [f"(cannot list) {root} ({exc})"]
        parts.extend(lines)
        parts.append("```")
        parts.append("")

    parts.append("## Code Focus (changed files in last 5 commits)")
    changed = _git_changed_files_last_commits(5)
    if not changed:
        parts.append("(no git history or no changed files)")
    else:
        for path in changed:
            full_path = repo_root / path
            parts.append(f"### {path.as_posix()}")
            parts.append("```text")
            parts.append(_include_file(full_path))
            parts.append("```")
    parts.append("")

    output_path = repo_root / "context_pack.md"
    output_path.write_text("\n".join(parts), encoding="utf-8", errors="replace")
    content = output_path.read_text(encoding="utf-8", errors="replace")
    if content.startswith("\ufeff"):
        content = content.lstrip("\ufeff")
    sys.stdout.buffer.write(content.encode("utf-8", errors="replace"))
    sys.stdout.buffer.write(b"\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

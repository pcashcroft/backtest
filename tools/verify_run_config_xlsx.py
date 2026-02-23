from __future__ import annotations

from pathlib import Path
import sys

from openpyxl import load_workbook


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _load_headers() -> dict[str, list[str]]:
    repo_root = _repo_root()
    sys.path.insert(0, str(repo_root / "src"))
    from backtest.config.schema import HEADERS

    return HEADERS


def main() -> int:
    headers = _load_headers()
    repo_root = _repo_root()
    xlsx_path = repo_root / "config" / "run_config.xlsx"

    if not xlsx_path.exists():
        print(f"Missing workbook: {xlsx_path}")
        return 1

    wb = load_workbook(xlsx_path)
    expected_sheets = list(headers.keys())
    actual_sheets = wb.sheetnames

    missing = [s for s in expected_sheets if s not in actual_sheets]
    extra = [s for s in actual_sheets if s not in expected_sheets]

    failed = False
    if missing:
        print("Missing sheets:", ", ".join(missing))
        failed = True
    if extra:
        print("Unexpected sheets:", ", ".join(extra))
        failed = True

    for sheet_name, expected_cols in headers.items():
        if sheet_name not in wb.sheetnames:
            continue
        ws = wb[sheet_name]
        actual_cols = [c.value for c in ws[1]]
        if actual_cols != expected_cols:
            failed = True
            print(f"Header mismatch in {sheet_name}:")
            print("  Expected:", expected_cols)
            print("  Actual  :", actual_cols)

    if failed:
        return 1

    print("Workbook verification passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

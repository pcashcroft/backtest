"""
INSTRUCTION HEADER
Purpose: Profile Databento DBN files to inspect columns, symbols, and timestamps.
Inputs: DBN files under a root folder (via --root and --glob).
Outputs: Console report only (no files written).
How to run:
  C:\\Users\\pcash\\anaconda3\\envs\\backtest\\python.exe tools\\verify\\profile_databento_dbn.py --root "E:\\BacktestData\\raw\\Emini_1s_ohlcv"
Success looks like: Prints per-file stats and overall totals.
Common failures and fixes:
- Module not found (databento): install databento in the backtest env.
- No files matched: verify --root/--glob.
"""

from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path
import re

import pandas as pd


DATE_RE = re.compile(r"(20\d{6})")


def _infer_date_from_name(path: Path) -> str | None:
    match = DATE_RE.search(path.name)
    if not match:
        return None
    value = match.group(1)
    return f"{value[:4]}-{value[4:6]}-{value[6:]}"


def _format_mb(num_bytes: int) -> str:
    return f"{num_bytes / (1024 * 1024):.2f} MB"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Profile Databento DBN files.")
    parser.add_argument("--root", required=True, help="Root folder containing DBN files.")
    parser.add_argument("--glob", default="*.dbn", help="Glob pattern for DBN files.")
    parser.add_argument("--max-files", type=int, default=50, help="Maximum files to process.")
    parser.add_argument("--max-unique-symbols", type=int, default=80, help="Max symbols to print.")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    root = Path(args.root)
    if not root.exists():
        raise FileNotFoundError(f"Root not found: {root}")

    files = sorted(root.glob(args.glob))
    if not files:
        raise FileNotFoundError(f"No files matched {args.glob} under {root}")

    files = files[: args.max_files]

    total_rows = 0
    total_loaded = 0
    all_columns: set[str] = set()
    all_symbols: set[str] = set()

    print(f"Matched files: {len(files)}")

    for path in files:
        stat = path.stat()
        mtime = datetime.fromtimestamp(stat.st_mtime)
        inferred = _infer_date_from_name(path)
        print("\n---")
        print(f"File: {path}")
        print(f"Size: {_format_mb(stat.st_size)}")
        print(f"Modified: {mtime}")
        print(f"Inferred date: {inferred}")

        try:
            from databento import DBNStore

            df = DBNStore.from_file(path).to_df()
        except Exception as exc:  # noqa: BLE001
            print(f"Load failed: {exc}")
            continue

        if df.index.name == "ts_recv":
            df = df.reset_index()
        else:
            df = df.reset_index()

        total_loaded += 1
        total_rows += len(df)
        all_columns.update(df.columns.astype(str).tolist())

        print(f"Rows: {len(df)}")
        print(f"Columns: {list(df.columns)}")
        print("Dtypes:")
        print(df.dtypes)

        if "ts_event" in df.columns:
            print(f"ts_event min: {df['ts_event'].min()}")
            print(f"ts_event max: {df['ts_event'].max()}")
        if "ts_recv" in df.columns:
            print(f"ts_recv min: {df['ts_recv'].min()}")
            print(f"ts_recv max: {df['ts_recv'].max()}")

        if "symbol" in df.columns:
            symbols = sorted(set(df["symbol"].dropna().astype(str).tolist()))
            all_symbols.update(symbols)
            print(f"Unique symbols: {len(symbols)}")
            print(f"First symbols: {symbols[: args.max_unique_symbols]}")

        if "price" in df.columns and "size" in df.columns:
            price_min = pd.to_numeric(df["price"], errors="coerce").min()
            price_max = pd.to_numeric(df["price"], errors="coerce").max()
            size_sum = pd.to_numeric(df["size"], errors="coerce").sum()
            print(f"price min/max: {price_min} / {price_max}")
            print(f"size sum: {size_sum}")

    print("\n===")
    print(f"Total files matched: {len(files)}")
    print(f"Total files loaded: {total_loaded}")
    print(f"Total rows loaded: {total_rows}")
    print(f"Union of columns: {sorted(all_columns)}")
    print(f"Total unique symbols: {len(all_symbols)}")
    print(f"First symbols: {sorted(all_symbols)[: args.max_unique_symbols]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

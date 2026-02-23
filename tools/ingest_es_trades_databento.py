"""
INSTRUCTION HEADER
What this file does: Canonicalizes Databento ES trades DBN files into partitioned parquet (FULL and RTH sessions).
Where it runs: Terminal [command line].
Inputs: `config/exports/config_snapshot_latest.json`, DBN files in `E:/BacktestData/raw/Emini_trade_data`.
Outputs: Parquet dataset under `E:/BacktestData/canonical/es_trades`, plus DuckDB registry/manifest entries.
How to run: `pybt tools/ingest_es_trades_databento.py`
Also: `C:/Users/pcash/anaconda3/envs/backtest/python.exe tools/ingest_es_trades_databento.py`
What success looks like: prints files processed, row counts for FULL/RTH, min/max ts_event, output path, and manifest rows.
Common failures + fixes: databento missing -> install package; DBN files missing -> check raw folder and glob;
DuckDB missing -> install duckdb; permission issues -> ensure canonical root is writable.
Incremental mode: use `--incremental` to skip dates already ingested per session/spec hash.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
from pathlib import Path
from typing import Any, Iterable

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import argparse
import re


SNAPSHOT_PATH = Path("config/exports/config_snapshot_latest.json")
DATASET_ID = "DB_ES_TRADES"
RAW_ROOT = Path("E:/BacktestData/raw/Emini_trade_data")
RAW_GLOB = "glbx-mdp3-*.trades.dbn"
CANONICAL_ROOT = Path("E:/BacktestData/canonical/es_trades")
DUCKDB_PATH = Path("E:/BacktestData/duckdb/research.duckdb")
TZ_NY = "America/New_York"

REQUIRED_COLS = ["ts_event", "ts_recv", "symbol", "price", "size", "side", "sequence", "flags"]


def _load_snapshot(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Missing config snapshot: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _find_dataset_row(snapshot: dict[str, Any], dataset_id: str) -> dict[str, Any]:
    datasets = snapshot.get("sheets", {}).get("DATASETS", [])
    for row in datasets:
        if row.get("dataset_id") == dataset_id:
            return row
    raise ValueError(f"Dataset not found: {dataset_id}")


def _extract_date_from_name(path: Path) -> str | None:
    name = path.name
    m = re.search(r"(20\d{2})[-_]?(\d{2})[-_]?(\d{2})", name)
    if not m:
        return None
    return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"


def _sort_files_by_date(paths: list[Path]) -> list[Path]:
    def key(p: Path):
        date_str = _extract_date_from_name(p)
        mtime = p.stat().st_mtime
        return (date_str or "0000-00-00", mtime, p.name)

    return sorted(paths, key=key)


def _ensure_writable_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    test_path = path / ".write_test.tmp"
    try:
        test_path.write_text("ok", encoding="utf-8")
    except Exception as exc:
        raise PermissionError(f"Canonical root not writable: {path} ({exc})")
    finally:
        if test_path.exists():
            test_path.unlink()


def _load_dbn(path: Path) -> pd.DataFrame:
    try:
        import databento  # type: ignore
    except Exception as exc:
        raise ImportError(f"databento package is required: {exc}")

    try:
        store = databento.DBNStore.from_file(str(path))
        df = store.to_df()
    except Exception as exc:
        raise RuntimeError(f"Failed to read DBN file: {path} ({exc})")

    if df.index.name != "ts_recv":
        df.index.name = "ts_recv"
    df = df.reset_index()
    return df


def _validate_and_cast(df: pd.DataFrame) -> pd.DataFrame:
    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns in DBN data: {missing}. Columns seen: {list(df.columns)}")

    df = df[REQUIRED_COLS].copy()

    df["ts_event"] = pd.to_datetime(df["ts_event"], utc=True, errors="raise")
    df["ts_recv"] = pd.to_datetime(df["ts_recv"], utc=True, errors="raise")
    df["price"] = df["price"].astype("float64")
    df["size"] = df["size"].astype("Int64")
    side_series = df["side"]
    try:
        df["side"] = pd.to_numeric(side_series, errors="raise").astype("Int16")
    except Exception:
        mapping = {"A": 1, "B": 2, "N": 0}
        mapped = side_series.map(mapping)
        if mapped.isna().any():
            sample = side_series.dropna().astype(str).unique()[:5]
            raise ValueError(
                "side column must be numeric or in {A,B,N} for int16. "
                f"Sample values: {sample}."
            )
        df["side"] = mapped.astype("Int16")
    df["sequence"] = df["sequence"].astype("int64")
    df["flags"] = df["flags"].astype("int64")
    df["symbol"] = df["symbol"].astype("string")

    return df


def _add_session_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["date"] = df["ts_event"].dt.date
    df["session_FULL"] = "FULL"

    local = df["ts_event"].dt.tz_convert(TZ_NY)
    weekday = local.dt.weekday < 5
    t = local.dt.time
    rth = weekday & (t >= dt.time(9, 30)) & (t < dt.time(16, 0))
    df["session_RTH"] = rth
    return df


def _write_partitioned(df: pd.DataFrame, session: str, output_root: Path) -> int:
    if session == "FULL":
        out = df.copy()
    elif session == "RTH":
        out = df[df["session_RTH"]].copy()
    else:
        raise ValueError(f"Unknown session: {session}")

    if out.empty:
        return 0

    out["session"] = session
    out = out.drop(columns=["session_FULL", "session_RTH"])
    out["date"] = pd.to_datetime(out["date"]).dt.date

    table = pa.Table.from_pandas(out, preserve_index=False)
    partitioning = ds.partitioning(
        schema=pa.schema([("session", pa.string()), ("date", pa.date32())]),
        flavor="hive",
    )
    ds.write_dataset(
        table,
        base_dir=str(output_root),
        format="parquet",
        partitioning=partitioning,
        existing_data_behavior="overwrite_or_ignore",
    )
    return len(out)


def _spec_hash(dataset_id: str, columns: Iterable[str], rth_rule: str, session: str) -> str:
    payload = f"{dataset_id}|{','.join(columns)}|{rth_rule}|{session}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _upsert_registry(dataset_id: str, raw_glob: str, canonical_root: str, columns: list[str]) -> None:
    spec_json = json.dumps(
        {
            "raw_glob": raw_glob,
            "canonical_root": canonical_root,
            "columns_kept": columns,
            "rth_rule": "09:30-16:00 America/New_York, weekdays only",
            "timezone": "UTC",
        }
    )

    now = dt.datetime.now()
    con = duckdb.connect(str(DUCKDB_PATH))
    con.execute(
        """
        INSERT OR REPLACE INTO registry_datasets
        (dataset_id, dataset_type, source_type, spec_json, updated_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        [dataset_id, "intraday_trades", "databento", spec_json, now],
    )
    con.close()


def _insert_manifest(
    session: str,
    coverage_start: dt.datetime | None,
    coverage_end: dt.datetime | None,
    columns: list[str],
) -> tuple:
    if coverage_start is None or coverage_end is None:
        raise ValueError(f"No coverage for session {session}; no rows to record.")

    now = dt.datetime.now()
    spec_hash = _spec_hash(DATASET_ID, columns, "09:30-16:00 America/New_York, weekdays only", session)

    con = duckdb.connect(str(DUCKDB_PATH))
    con.execute(
        """
        INSERT INTO manifest_derived_tables
        (derived_id, table_name, spec_hash, session, coverage_start, coverage_end, parquet_path, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            f"canonical_es_trades_{session}",
            "es_trades",
            spec_hash,
            session,
            coverage_start,
            coverage_end,
            str(CANONICAL_ROOT),
            now,
        ],
    )
    row = con.execute(
        """
        SELECT * FROM manifest_derived_tables
        WHERE derived_id=?
        ORDER BY created_at DESC
        LIMIT 1
        """,
        [f"canonical_es_trades_{session}"],
    ).fetchone()
    con.close()
    return row


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest Databento ES trades DBN files.")
    parser.add_argument("--max-files", type=int, default=None, help="Process only the most recent N files.")
    parser.add_argument(
        "--only-session",
        choices=["FULL", "RTH", "BOTH"],
        default="BOTH",
        help="Write only FULL, only RTH, or both sessions.",
    )
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="Skip files whose dates are already ingested per session/spec hash.",
    )
    parser.add_argument(
        "--rth-weekdays-only",
        action="store_true",
        default=True,
        help="When selecting RTH candidates, keep only weekday dates (Mon-Fri).",
    )
    parser.add_argument(
        "--rth-include-date",
        type=str,
        default=None,
        help="Force-include this YYYY-MM-DD date in RTH selection.",
    )
    return parser.parse_args()


def _partition_dates(session: str) -> set[str]:
    dates: set[str] = set()
    for p in CANONICAL_ROOT.glob(f"session={session}/date=*"):
        if p.is_dir():
            dates.add(p.name.split("=", 1)[1])
    if dates:
        return dates
    for p in (CANONICAL_ROOT / session).glob("*"):
        if p.is_dir():
            dates.add(p.name)
    return dates


def _manifest_rows_for_session(con: duckdb.DuckDBPyConnection, session: str, spec_hash: str) -> list[tuple]:
    return con.execute(
        """
        SELECT * FROM manifest_derived_tables
        WHERE derived_id=? AND spec_hash=?
        """,
        [f"canonical_es_trades_{session}", spec_hash],
    ).fetchall()


def main() -> int:
    args = _parse_args()
    snapshot = _load_snapshot(SNAPSHOT_PATH)
    _ = _find_dataset_row(snapshot, DATASET_ID)

    if not RAW_ROOT.exists():
        raise FileNotFoundError(f"Raw folder not found: {RAW_ROOT}")

    files = _sort_files_by_date(list(RAW_ROOT.glob(RAW_GLOB)))
    if not files:
        raise FileNotFoundError(f"No DBN files found for glob: {RAW_ROOT / RAW_GLOB}")

    skipped_full: list[Path] = []
    skipped_rth: list[Path] = []
    selected_dates_full: set[str] = set()
    selected_dates_rth: set[str] = set()
    if args.incremental:
        spec_full = _spec_hash(DATASET_ID, REQUIRED_COLS, "09:30-16:00 America/New_York, weekdays only", "FULL")
        spec_rth = _spec_hash(DATASET_ID, REQUIRED_COLS, "09:30-16:00 America/New_York, weekdays only", "RTH")
        con = duckdb.connect(str(DUCKDB_PATH))
        rows_full = _manifest_rows_for_session(con, "FULL", spec_full) if args.only_session in ("FULL", "BOTH") else []
        rows_rth = _manifest_rows_for_session(con, "RTH", spec_rth) if args.only_session in ("RTH", "BOTH") else []
        con.close()

        dates_full = _partition_dates("FULL") if rows_full else set()
        dates_rth = _partition_dates("RTH") if rows_rth else set()

        if rows_full and not dates_full:
            raise RuntimeError("Manifest shows FULL ingested but no FULL partition folders found.")
        if rows_rth and not dates_rth:
            raise RuntimeError("Manifest shows RTH ingested but no RTH partition folders found.")

        by_date: dict[str, Path] = {}
        for f in files:
            file_date = _extract_date_from_name(f)
            if not file_date:
                raise ValueError(f"Cannot parse date from filename: {f.name}")
            by_date[file_date] = f

        if args.only_session in ("FULL", "BOTH"):
            full_candidates = [d for d in by_date.keys() if d not in dates_full]
            full_candidates.sort()
            if args.max_files is not None:
                full_candidates = full_candidates[-args.max_files :]
            selected_dates_full = set(full_candidates)
        if args.only_session in ("RTH", "BOTH"):
            rth_candidates = [d for d in by_date.keys() if d not in dates_rth]
            dropped_weekend = 0
            if args.rth_weekdays_only:
                filtered: list[str] = []
                for d in rth_candidates:
                    try:
                        dd = dt.date.fromisoformat(d)
                    except Exception:
                        raise ValueError(f"Invalid date in filename for RTH selection: {d}")
                    if dd.weekday() < 5:
                        filtered.append(d)
                    else:
                        dropped_weekend += 1
                rth_candidates = filtered
            rth_candidates.sort()
            if args.max_files is not None:
                rth_candidates = rth_candidates[-args.max_files :]
            selected_dates_rth = set(rth_candidates)

            if args.rth_include_date:
                try:
                    dt.date.fromisoformat(args.rth_include_date)
                except Exception:
                    raise ValueError(f"Invalid --rth-include-date: {args.rth_include_date}")
                if args.rth_include_date not in by_date:
                    raise FileNotFoundError(
                        f"--rth-include-date requested but file not found: glbx-mdp3-{args.rth_include_date.replace('-', '')}.trades.dbn"
                    )
                selected_dates_rth.add(args.rth_include_date)

        for d, f in by_date.items():
            if d in dates_full and args.only_session in ("FULL", "BOTH"):
                skipped_full.append(f)
            if d in dates_rth and args.only_session in ("RTH", "BOTH"):
                skipped_rth.append(f)

        files = sorted({by_date[d] for d in (selected_dates_full | selected_dates_rth)}, key=lambda p: p.name)

        total_found = len(by_date)
        print(f"Total files found: {total_found}")
        print(f"FULL candidates: {len([d for d in by_date.keys() if d not in dates_full])}")
        print(f"FULL selected dates: {sorted(selected_dates_full)}")
        print(f"RTH candidates: {len([d for d in by_date.keys() if d not in dates_rth])}")
        if args.rth_weekdays_only:
            print(f"RTH weekend dates dropped: {dropped_weekend}")
        print(f"RTH selected dates: {sorted(selected_dates_rth)}")
        print(f"Files skipped FULL: {len(set(skipped_full))}")
        print(f"Files skipped RTH: {len(set(skipped_rth))}")

    if not args.incremental and args.max_files is not None:
        if args.max_files <= 0:
            raise ValueError("--max-files must be a positive integer.")
        files = files[-args.max_files :]

    print("Files to process:")
    for f in files:
        print(f"  {f}")

    _ensure_writable_dir(CANONICAL_ROOT)

    total_full = 0
    total_rth = 0
    min_full = None
    max_full = None
    min_rth = None
    max_rth = None

    for path in files:
        file_date = _extract_date_from_name(path)
        if not file_date:
            raise ValueError(f"Cannot parse date from filename: {path.name}")

        df = _load_dbn(path)
        df = _validate_and_cast(df)
        df = _add_session_columns(df)

        full_rows = 0
        rth_rows = 0
        if args.only_session in ("FULL", "BOTH"):
            if args.incremental and selected_dates_full and file_date not in selected_dates_full:
                full_rows = 0
            else:
                full_rows = _write_partitioned(df, "FULL", CANONICAL_ROOT)
        if args.only_session in ("RTH", "BOTH"):
            if args.incremental and selected_dates_rth and file_date not in selected_dates_rth:
                rth_rows = 0
            else:
                rth_rows = _write_partitioned(df, "RTH", CANONICAL_ROOT)

        if full_rows > 0:
            dmin = df["ts_event"].min().to_pydatetime()
            dmax = df["ts_event"].max().to_pydatetime()
            min_full = dmin if min_full is None else min(min_full, dmin)
            max_full = dmax if max_full is None else max(max_full, dmax)
            total_full += full_rows

        if rth_rows > 0:
            rth_df = df[df["session_RTH"]]
            rmin = rth_df["ts_event"].min().to_pydatetime()
            rmax = rth_df["ts_event"].max().to_pydatetime()
            min_rth = rmin if min_rth is None else min(min_rth, rmin)
            max_rth = rmax if max_rth is None else max(max_rth, rmax)
            total_rth += rth_rows

    _upsert_registry(DATASET_ID, str(RAW_ROOT / RAW_GLOB), str(CANONICAL_ROOT), REQUIRED_COLS)

    manifest_full = None
    manifest_rth = None
    if args.only_session in ("FULL", "BOTH"):
        if total_full == 0:
            raise ValueError("No FULL rows written; refusing to insert manifest.")
        manifest_full = _insert_manifest("FULL", min_full, max_full, REQUIRED_COLS)
    if args.only_session in ("RTH", "BOTH"):
        if total_rth == 0:
            print("No RTH rows written; skipping RTH manifest insert.")
        else:
            manifest_rth = _insert_manifest("RTH", min_rth, max_rth, REQUIRED_COLS)

    print(f"Files processed: {len(files)}")
    print(f"Rows written FULL: {total_full}")
    print(f"Rows written RTH: {total_rth}")
    print(f"FULL min/max ts_event: {min_full} / {max_full}")
    print(f"RTH min/max ts_event: {min_rth} / {max_rth}")
    print(f"Output canonical root: {CANONICAL_ROOT}")
    print(f"Manifest FULL: {manifest_full}")
    print(f"Manifest RTH: {manifest_rth}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

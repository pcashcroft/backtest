"""
INSTRUCTION HEADER

What this file does (plain English):
- Ingests Databento 1-second OHLCV DBN files into canonical parquet partitions on SSD.
- Writes BOTH sessions:
  - FULL: all rows
  - RTH: 09:30?16:00 America/New_York, weekdays only
- Symbol filtering is controlled by config (DATASETS.notes for DB_ES_OHLCV_1S), and spreads are excluded by default.

Where to run it:
- Run from repo root: C:/Users/pcash/OneDrive/Backtest

Inputs:
- config/exports/config_snapshot_latest.json
- DBN files matched by DATASETS.dataset_id=DB_ES_OHLCV_1S source_path_or_id glob

Outputs:
- Canonical parquet under: E:/BacktestData/canonical/es_ohlcv_1s
  partitioned by session and NY date:
  session=FULL/date=YYYY-MM-DD/...
  session=RTH/date=YYYY-MM-DD/...

How to run:
C:/Users/pcash/anaconda3/envs/backtest/python.exe tools/ingest_ohlcv_1s_databento.py

What success looks like:
- It prints matched files, rows loaded, symbol filtering stats, unique NY dates, and a per-day progress bar while writing.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import re
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds


SNAPSHOT_PATH = Path("config/exports/config_snapshot_latest.json")
DATA_ROOT = Path("E:/BacktestData")
CANONICAL_ROOT = DATA_ROOT / "canonical" / "es_ohlcv_1s"
DUCKDB_PATH = DATA_ROOT / "duckdb" / "research.duckdb"
DATASET_ID = "DB_ES_OHLCV_1S"

KEEP_COLS = ["ts_event", "symbol", "open", "high", "low", "close", "volume"]


def _load_snapshot(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Missing config snapshot: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _find_dataset_row(snapshot: dict[str, Any], dataset_id: str) -> dict[str, Any]:
    rows = snapshot.get("sheets", {}).get("DATASETS", []) or []
    for r in rows:
        if r.get("dataset_id") == dataset_id:
            return r
    raise ValueError(f"Dataset not found in snapshot: {dataset_id}")


def _parse_notes(notes: str) -> dict[str, str]:
    """
    Parse simple KEY: VALUE lines from the DATASETS.notes field.
    Unknown lines are ignored.
    """
    out: dict[str, str] = {}
    for raw in (notes or "").splitlines():
        line = raw.strip()
        if not line:
            continue
        if ":" not in line:
            continue
        k, v = line.split(":", 1)
        out[k.strip()] = v.strip()
    return out


def _expand_files(glob_path: str) -> list[Path]:
    p = Path(glob_path)
    root = p.parent
    pat = p.name
    if not root.exists():
        raise FileNotFoundError(f"DBN glob parent folder does not exist: {root}")
    files = sorted(root.glob(pat))
    if not files:
        raise FileNotFoundError(f"No DBN files matched: {glob_path}")
    return files


def _load_dbn_to_df(path: Path) -> pd.DataFrame:
    """
    Load DBN file to pandas DataFrame.
    NOTE: For very large DBNs, this can take time; caller prints before calling this.
    """
    try:
        import databento  # type: ignore
    except Exception as exc:
        raise ImportError(f"databento package is required: {exc}")

    store = databento.DBNStore.from_file(str(path))
    df = store.to_df()

    # Normalize to plain columns
    df = df.reset_index()
    return df


def _apply_symbol_filters(df: pd.DataFrame, include_regex: str | None, exclude_contains: str | None) -> pd.DataFrame:
    if "symbol" not in df.columns:
        raise ValueError("Missing 'symbol' column (required for filtering).")

    before_rows = len(df)
    before_syms = df["symbol"].astype(str).nunique(dropna=True)

    out = df
    if include_regex:
        pattern = re.compile(include_regex)
        out = out[out["symbol"].astype(str).str.match(pattern)]
    if exclude_contains:
        out = out[~out["symbol"].astype(str).str.contains(exclude_contains)]

    after_rows = len(out)
    after_syms = out["symbol"].astype(str).nunique(dropna=True)

    print(f"Symbol filtering: rows {before_rows} -> {after_rows}", flush=True)
    print(f"Symbol filtering: unique symbols {before_syms} -> {after_syms}", flush=True)
    return out


def _compute_date_and_rth(
    df: pd.DataFrame,
    rth_start: str,
    rth_end: str,
    rth_tz: str,
) -> tuple[pd.Series, pd.Series]:
    """
    Returns:
      date_series: NY-normalized date (python date)
      is_rth: boolean mask
    """
    if "ts_event" not in df.columns:
        raise ValueError("Missing 'ts_event' column.")

    ts = pd.to_datetime(df["ts_event"], utc=True, errors="raise")
    ts_ny = ts.dt.tz_convert(rth_tz)

    date_series = ts_ny.dt.normalize().dt.date

    is_weekday = ts_ny.dt.weekday < 5
    start_t = pd.to_datetime(rth_start).time()
    end_t = pd.to_datetime(rth_end).time()
    in_window = (ts_ny.dt.time >= start_t) & (ts_ny.dt.time < end_t)
    is_rth = is_weekday & in_window

    return date_series, is_rth


def _partition_dir(root: Path, session: str, date_value: dt.date) -> Path:
    return root / f"session={session}" / f"date={date_value.isoformat()}"


def _write_partition(df_part: pd.DataFrame, root: Path, session: str) -> int:
    """
    Write one partition chunk using hive partitioning on [session, date].
    We include 'session' and 'date' columns in the table so partitioning works reliably.
    """
    if df_part.empty:
        return 0

    out = df_part.copy()
    out["session"] = session

    table = pa.Table.from_pandas(out, preserve_index=False)

    ds.write_dataset(
        table,
        base_dir=str(root),
        format="parquet",
        partitioning=["session", "date"],
        existing_data_behavior="overwrite_or_ignore",
    )
    return len(out)


def _spec_hash(dataset_id: str, include_regex: str, exclude_contains: str, rth_start: str, rth_end: str, rth_tz: str) -> str:
    payload = f"{dataset_id}|{include_regex}|{exclude_contains}|{rth_start}|{rth_end}|{rth_tz}|v2_day_partition"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _upsert_registry(dataset_id: str, spec_json: str) -> None:
    now = dt.datetime.now()
    con = duckdb.connect(str(DUCKDB_PATH))
    con.execute(
        """
        INSERT OR REPLACE INTO registry_datasets
        (dataset_id, dataset_type, source_type, spec_json, updated_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        [dataset_id, "intraday_ohlcv_1s", "dbn", spec_json, now],
    )
    con.close()


def _insert_manifest(session: str, spec_hash: str, start: dt.datetime, end: dt.datetime) -> tuple[Any, ...]:
    now = dt.datetime.now()
    con = duckdb.connect(str(DUCKDB_PATH))
    con.execute(
        """
        INSERT INTO manifest_derived_tables
        (derived_id, table_name, spec_hash, session, coverage_start, coverage_end, parquet_path, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            "canonical_es_ohlcv_1s",
            "es_ohlcv_1s",
            spec_hash,
            session,
            start,
            end,
            str(CANONICAL_ROOT),
            now,
        ],
    )
    row = con.execute(
        """
        SELECT * FROM manifest_derived_tables
        WHERE derived_id='canonical_es_ohlcv_1s' AND session=?
        ORDER BY created_at DESC
        LIMIT 1
        """,
        [session],
    ).fetchone()
    con.close()
    return row


def _progress_iter(dates: list[dt.date]):
    """
    Returns an iterator over dates with progress reporting.
    Uses tqdm if available, otherwise prints a heartbeat line per date.
    """
    try:
        from tqdm import tqdm  # type: ignore

        return tqdm(dates, desc="Writing OHLCV 1s by date", unit="day")
    except Exception:
        total = len(dates)

        class _Fallback:
            def __iter__(self):
                for i, d in enumerate(dates, start=1):
                    print(f"Processing {i}/{total}: {d.isoformat()}", flush=True)
                    yield d

        return _Fallback()


def main() -> int:
    snapshot = _load_snapshot(SNAPSHOT_PATH)
    dataset = _find_dataset_row(snapshot, DATASET_ID)

    source_glob = dataset.get("source_path_or_id")
    if not source_glob:
        raise ValueError("Missing source_path_or_id for DB_ES_OHLCV_1S in snapshot.")

    notes_kv = _parse_notes(dataset.get("notes") or "")
    include_regex = notes_kv.get("symbol_include_regex", "")
    exclude_contains = notes_kv.get("symbol_exclude_contains", "")
    rth_start = notes_kv.get("rth_start", "09:30")
    rth_end = notes_kv.get("rth_end", "16:00")
    rth_tz = notes_kv.get("rth_tz", "America/New_York")

    files = _expand_files(source_glob)
    print(f"Matched files: {len(files)}", flush=True)
    for f in files:
        print(f"  {f}", flush=True)

    CANONICAL_ROOT.mkdir(parents=True, exist_ok=True)

    total_full_written = 0
    total_rth_written = 0

    full_min: pd.Timestamp | None = None
    full_max: pd.Timestamp | None = None
    rth_min: pd.Timestamp | None = None
    rth_max: pd.Timestamp | None = None

    symbols_after: set[str] = set()

    for path in files:
        print(f"---", flush=True)
        print(f"Loading DBN (this may take time): {path}", flush=True)

        df_raw = _load_dbn_to_df(path)
        print(f"Rows loaded: {len(df_raw)}", flush=True)

        missing = [c for c in KEEP_COLS if c not in df_raw.columns]
        if missing:
            raise ValueError(f"DBN file missing required columns {missing}. Columns seen: {list(df_raw.columns)}")

        df = df_raw[KEEP_COLS].copy()

        # Ensure ts_event is UTC
        df["ts_event"] = pd.to_datetime(df["ts_event"], utc=True, errors="raise")

        # Symbol filtering (config-driven)
        df = _apply_symbol_filters(df, include_regex or None, exclude_contains or None)
        if df.empty:
            raise ValueError("No rows after symbol filtering; refusing to proceed.")

        symbols_after.update(df["symbol"].astype(str).unique().tolist())

        # Compute partition date and RTH flag
        df["date"], df["is_rth"] = _compute_date_and_rth(df, rth_start, rth_end, rth_tz)

        # Coverage (FULL)
        fmin = df["ts_event"].min()
        fmax = df["ts_event"].max()
        full_min = fmin if full_min is None else min(full_min, fmin)
        full_max = fmax if full_max is None else max(full_max, fmax)

        # Coverage (RTH)
        if df["is_rth"].any():
            rmin = df.loc[df["is_rth"], "ts_event"].min()
            rmax = df.loc[df["is_rth"], "ts_event"].max()
            rth_min = rmin if rth_min is None else min(rth_min, rmin)
            rth_max = rmax if rth_max is None else max(rth_max, rmax)

        unique_dates = sorted(df["date"].unique().tolist())
        print(f"Unique NY dates in this file: {len(unique_dates)}", flush=True)
        print(f"First date: {unique_dates[0]}  Last date: {unique_dates[-1]}", flush=True)

        iterator = _progress_iter(unique_dates)

        for d in iterator:
            # FULL
            full_dir = _partition_dir(CANONICAL_ROOT, "FULL", d)
            if full_dir.exists():
                try:
                    from tqdm import tqdm  # type: ignore
                    tqdm.write(f"SKIP FULL date={d}")
                except Exception:
                    print(f"SKIP FULL date={d}", flush=True)
            else:
                df_day = df[df["date"] == d].drop(columns=["is_rth"])
                total_full_written += _write_partition(df_day, CANONICAL_ROOT, "FULL")

            # RTH
            rth_dir = _partition_dir(CANONICAL_ROOT, "RTH", d)
            if rth_dir.exists():
                try:
                    from tqdm import tqdm  # type: ignore
                    tqdm.write(f"SKIP RTH date={d}")
                except Exception:
                    print(f"SKIP RTH date={d}", flush=True)
            else:
                df_day = df[df["date"] == d]
                df_day_rth = df_day[df_day["is_rth"]].drop(columns=["is_rth"])
                if not df_day_rth.empty:
                    total_rth_written += _write_partition(df_day_rth, CANONICAL_ROOT, "RTH")

    if full_min is None or full_max is None:
        raise ValueError("No FULL coverage computed; refusing to insert manifest.")

    spec_json = json.dumps(
        {
            "source_path_or_id": source_glob,
            "symbol_include_regex": include_regex,
            "symbol_exclude_contains": exclude_contains,
            "rth_start": rth_start,
            "rth_end": rth_end,
            "rth_tz": rth_tz,
            "canonical_root": str(CANONICAL_ROOT),
            "keep_cols": KEEP_COLS,
            "writer_mode": "day_partition",
        }
    )
    spec_hash = _spec_hash(DATASET_ID, include_regex, exclude_contains, rth_start, rth_end, rth_tz)

    _upsert_registry(DATASET_ID, spec_json)
    manifest_full = _insert_manifest("FULL", spec_hash, full_min.to_pydatetime(), full_max.to_pydatetime())

    manifest_rth = None
    if rth_min is not None and rth_max is not None:
        manifest_rth = _insert_manifest("RTH", spec_hash, rth_min.to_pydatetime(), rth_max.to_pydatetime())

    sym_sorted = sorted(symbols_after)

    print("", flush=True)
    print("DONE", flush=True)
    print(f"Rows written FULL (new): {total_full_written}", flush=True)
    print(f"Rows written RTH (new): {total_rth_written}", flush=True)
    print(f"FULL min/max ts_event: {full_min} / {full_max}", flush=True)
    print(f"RTH min/max ts_event: {rth_min} / {rth_max}", flush=True)
    print(f"Unique symbols after filtering: {len(sym_sorted)}", flush=True)
    print(f"First symbols: {sym_sorted[:30]}", flush=True)
    print(f"Output canonical root: {CANONICAL_ROOT}", flush=True)
    print(f"Manifest FULL: {manifest_full}", flush=True)
    print(f"Manifest RTH: {manifest_rth}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

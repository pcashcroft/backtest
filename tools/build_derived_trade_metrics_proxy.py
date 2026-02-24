"""
INSTRUCTION HEADER

What this script does (plain English):
- Builds 1-minute footprint_proxy_1m and cvd_proxy_1m parquet files from canonical
  1-second OHLCV data on SSD, using Bulk Volume Classification (BVC).
- Reads all DATASETS rows with dataset_type='derived_trade_metrics_proxy' from the
  config snapshot and processes each one (instrument-agnostic).
- metric_type in the DATASETS notes field controls what is built:
    metric_type: footprint  ->  footprint_proxy_1m  (buy/sell volume per price level per minute)
    metric_type: cvd        ->  cvd_proxy_1m        (buy/sell volume + delta per minute)
- Supports incremental mode: skips dates already built.
- Updates the DuckDB manifest (manifest_derived_tables) with coverage after each session.

BVC Method (Bulk Volume Classification):
  buy_frac  = (close - low) / (high - low)   for non-doji bars
  buy_frac  = 0.5                             for doji bars (high == low)
  sell_frac = 1 - buy_frac

Footprint proxy price assignment:
  Doji bars (high == low):
    single row: price=high, buy=round(vol*0.5), sell=vol-round(vol*0.5)
  Non-doji bars:
    row 1: price=high, buy=round(vol*buy_frac), sell=0
    row 2: price=low,  buy=0,                   sell=vol-round(vol*buy_frac)
  These are then aggregated (summed) per (minute, symbol, price).

Schema compatibility:
  Proxy tables use the same column names as the real tables so the chart layer
  and feature system can consume both via the metric_source_mode config.
  trade_count in proxy tables = number of 1s bars contributing to each row.

Where to run:
- Run from repo root: C:\\Users\\pcash\\OneDrive\\Backtest

Inputs:
- config/exports/config_snapshot_latest.json
- E:\\BacktestData\\canonical\\es_ohlcv_1s\\{FULL|RTH}\\{date}\\part-*.parquet

Outputs:
  footprint_proxy_1m:
    E:\\BacktestData\\derived\\footprint_proxy_1m\\ES\\{FULL|RTH}\\{date}\\part-0.parquet
    Schema: bar_time (timestamp UTC), symbol (str), price (float64),
            buy_volume (int64), sell_volume (int64), trade_count (int32)

  cvd_proxy_1m:
    E:\\BacktestData\\derived\\cvd_proxy_1m\\ES\\{FULL|RTH}\\{date}\\part-0.parquet
    Schema: bar_time (timestamp UTC), symbol (str),
            buy_volume (int64), sell_volume (int64), delta (int64), trade_count (int32)

How to run:
  # Normal incremental run (skips already-built dates):
  C:\\Users\\pcash\\anaconda3\\envs\\backtest\\python.exe tools\\build_derived_trade_metrics_proxy.py

  # Force-rebuild all dates:
  C:\\Users\\pcash\\anaconda3\\envs\\backtest\\python.exe tools\\build_derived_trade_metrics_proxy.py --force-rebuild

  # Only process one specific dataset:
  C:\\Users\\pcash\\anaconda3\\envs\\backtest\\python.exe tools\\build_derived_trade_metrics_proxy.py --dataset-id ES_FOOTPRINT_PROXY_1M

What success looks like:
- Prints per-session progress and "DONE" at the end.
- Files exist at: E:\\BacktestData\\derived\\footprint_proxy_1m\\ES\\FULL\\2016-02-22\\part-0.parquet
- DuckDB manifest has rows for ES_FOOTPRINT_PROXY_1M_FULL, ES_FOOTPRINT_PROXY_1M_RTH,
  ES_CVD_PROXY_1M_FULL, ES_CVD_PROXY_1M_RTH.

Common failures + fixes:
- "No derived_trade_metrics_proxy rows found":
    run tools/add_trade_metrics_proxy_config.py first.
- "Source canonical root not found":
    run ingest_ohlcv_1s_databento.py first.
- DuckDB permission error: close any other process using research.duckdb.
- pyarrow missing: pip install pyarrow in the backtest conda env.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import sys
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq


SNAPSHOT_PATH = Path("config/exports/config_snapshot_latest.json")
SESSIONS = ("FULL", "RTH")


# ---------------------------------------------------------------------------
# Config helpers  (same pattern as other build scripts)
# ---------------------------------------------------------------------------

def _load_snapshot(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(
            f"Config snapshot missing: {path}\n"
            "Run tools/export_config_snapshot.py first."
        )
    try:
        import orjson
        return orjson.loads(path.read_bytes())
    except ImportError:
        return json.loads(path.read_text(encoding="utf-8"))


def _active_paths(snapshot: dict[str, Any]) -> dict[str, Any]:
    rows = snapshot.get("sheets", {}).get("PATHS", []) or []
    for r in rows:
        val = r.get("IsActive")
        if isinstance(val, bool) and val:
            return r
        if isinstance(val, (int, float)) and val:
            return r
        if isinstance(val, str) and val.strip().lower() in {"1", "true", "yes", "y"}:
            return r
    raise ValueError("No active PATHS row in config snapshot.")


def _find_dataset_rows(snapshot: dict[str, Any], dataset_type: str) -> list[dict[str, Any]]:
    return [
        r for r in snapshot.get("sheets", {}).get("DATASETS", []) or []
        if r.get("dataset_type") == dataset_type
    ]


def _find_dataset_by_id(snapshot: dict[str, Any], dataset_id: str) -> dict[str, Any]:
    for r in snapshot.get("sheets", {}).get("DATASETS", []) or []:
        if r.get("dataset_id") == dataset_id:
            return r
    raise ValueError(f"Dataset not found in snapshot: {dataset_id!r}")


def _parse_notes(notes: str | None) -> dict[str, str]:
    """Parse KEY: VALUE lines from a DATASETS notes field."""
    out: dict[str, str] = {}
    for raw in (notes or "").splitlines():
        line = raw.strip()
        if line and ":" in line:
            k, v = line.split(":", 1)
            out[k.strip()] = v.strip()
    return out


# ---------------------------------------------------------------------------
# Filesystem helpers
# ---------------------------------------------------------------------------

def _list_dates(root: Path, session: str) -> list[str]:
    """List YYYY-MM-DD subdirs under root/session/, sorted ascending."""
    session_dir = root / session
    if not session_dir.exists():
        return []
    return sorted(
        d for d in os.listdir(session_dir)
        if len(d) == 10 and (session_dir / d).is_dir()
    )


def _built_dates(root: Path, session: str) -> set[str]:
    """Return set of dates already written under root/session/."""
    session_dir = root / session
    if not session_dir.exists():
        return set()
    return {d for d in os.listdir(session_dir) if (session_dir / d).is_dir()}


# ---------------------------------------------------------------------------
# BVC aggregations
# ---------------------------------------------------------------------------

def _build_footprint_proxy(
    parquet_files: list[Path],
    con: duckdb.DuckDBPyConnection,
) -> pa.Table:
    """
    Build footprint_proxy_1m from 1s OHLCV data using BVC.

    Price assignment:
      - Doji bars (high == low): single price level, 50/50 buy/sell split.
      - Non-doji bars: buy volume assigned to high price, sell to low price.

    Aggregated per (minute, symbol, price) by summing across the session.

    Columns: bar_time, symbol, price, buy_volume, sell_volume, trade_count
    """
    file_list = (
        "["
        + ", ".join(f"'{str(p).replace(chr(92), '/')}'" for p in parquet_files)
        + "]"
    )

    sql = f"""
    WITH bvc AS (
        SELECT
            date_trunc('minute', bar_time)                         AS bar_time,
            symbol,
            high,
            low,
            CAST(volume AS BIGINT)                                 AS volume,
            CASE
                WHEN high = low
                    THEN 0.5
                ELSE CAST((close - low) AS DOUBLE)
                   / CAST((high  - low) AS DOUBLE)
            END                                                    AS buy_frac
        FROM read_parquet({file_list})
        WHERE volume > 0
    ),
    price_rows AS (
        -- Doji: single price level (high = low), 50/50 split
        SELECT
            bar_time,
            symbol,
            high                                          AS price,
            CAST(ROUND(volume * buy_frac) AS BIGINT)     AS buy_volume,
            CAST(volume - ROUND(volume * buy_frac) AS BIGINT) AS sell_volume
        FROM bvc
        WHERE high = low

        UNION ALL

        -- Non-doji: buy volume attributed to the high price
        SELECT
            bar_time,
            symbol,
            high                                          AS price,
            CAST(ROUND(volume * buy_frac) AS BIGINT)     AS buy_volume,
            CAST(0 AS BIGINT)                             AS sell_volume
        FROM bvc
        WHERE high != low

        UNION ALL

        -- Non-doji: sell volume attributed to the low price
        SELECT
            bar_time,
            symbol,
            low                                           AS price,
            CAST(0 AS BIGINT)                             AS buy_volume,
            CAST(volume - ROUND(volume * buy_frac) AS BIGINT) AS sell_volume
        FROM bvc
        WHERE high != low
    )
    SELECT
        bar_time,
        symbol,
        price,
        CAST(SUM(buy_volume)  AS BIGINT)  AS buy_volume,
        CAST(SUM(sell_volume) AS BIGINT)  AS sell_volume,
        CAST(COUNT(*)         AS INTEGER) AS trade_count
    FROM price_rows
    GROUP BY bar_time, symbol, price
    ORDER BY bar_time, symbol, price
    """
    table = con.execute(sql).fetch_arrow_table()
    return _cast_bar_time_utc(table)


def _build_cvd_proxy(
    parquet_files: list[Path],
    con: duckdb.DuckDBPyConnection,
) -> pa.Table:
    """
    Build cvd_proxy_1m from 1s OHLCV data using BVC.

    buy_frac  = (close - low) / (high - low)  for non-doji bars
    buy_frac  = 0.5                            for doji bars
    sell_frac = 1 - buy_frac

    Aggregated per (minute, symbol).
    delta = buy_volume - sell_volume (chart layer computes cumsum for CVD line).

    Columns: bar_time, symbol, buy_volume, sell_volume, delta, trade_count
    """
    file_list = (
        "["
        + ", ".join(f"'{str(p).replace(chr(92), '/')}'" for p in parquet_files)
        + "]"
    )

    sql = f"""
    WITH bvc AS (
        SELECT
            date_trunc('minute', bar_time)                         AS bar_time,
            symbol,
            CAST(volume AS BIGINT)                                 AS volume,
            CASE
                WHEN high = low
                    THEN 0.5
                ELSE CAST((close - low) AS DOUBLE)
                   / CAST((high  - low) AS DOUBLE)
            END                                                    AS buy_frac
        FROM read_parquet({file_list})
        WHERE volume > 0
    )
    SELECT
        bar_time,
        symbol,
        CAST(SUM(ROUND(volume * buy_frac)) AS BIGINT)                  AS buy_volume,
        CAST(SUM(volume) - SUM(ROUND(volume * buy_frac)) AS BIGINT)    AS sell_volume,
        CAST(
              SUM(ROUND(volume * buy_frac))
            - (SUM(volume) - SUM(ROUND(volume * buy_frac)))
          AS BIGINT
        )                                                               AS delta,
        CAST(COUNT(*) AS INTEGER)                                       AS trade_count
    FROM bvc
    GROUP BY bar_time, symbol
    ORDER BY bar_time, symbol
    """
    table = con.execute(sql).fetch_arrow_table()
    return _cast_bar_time_utc(table)


def _cast_bar_time_utc(table: pa.Table) -> pa.Table:
    """Cast bar_time column to timestamp[us, tz=UTC], avoiding pytz."""
    import pyarrow.compute as pc
    utc_type = pa.timestamp("us", tz="UTC")
    bar_time_utc = pc.cast(table.column("bar_time"), utc_type)
    return table.set_column(
        table.schema.get_field_index("bar_time"), "bar_time", bar_time_utc
    )


# ---------------------------------------------------------------------------
# DuckDB manifest
# ---------------------------------------------------------------------------

def _upsert_manifest(
    reg_con: duckdb.DuckDBPyConnection,
    derived_id: str,
    table_name: str,
    spec_hash: str,
    session: str,
    coverage_start: dt.datetime,
    coverage_end: dt.datetime,
    parquet_path: str,
) -> None:
    reg_con.execute(
        "DELETE FROM manifest_derived_tables WHERE derived_id = ? AND session = ?",
        [derived_id, session],
    )
    reg_con.execute(
        """
        INSERT INTO manifest_derived_tables
            (derived_id, table_name, spec_hash, session,
             coverage_start, coverage_end, parquet_path, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            derived_id, table_name, spec_hash, session,
            coverage_start, coverage_end, parquet_path,
            dt.datetime.now(),
        ],
    )


def _query_full_coverage(
    output_root: Path,
    session: str,
    agg_con: duckdb.DuckDBPyConnection,
) -> tuple[dt.datetime | None, dt.datetime | None]:
    """
    Query min/max bar_time across all built dates for a session.
    Casts to plain TIMESTAMP to avoid the pytz requirement.
    """
    glob = str(output_root / session / "*" / "part-0.parquet").replace("\\", "/")
    try:
        row = agg_con.execute(
            f"SELECT min(bar_time::TIMESTAMP), max(bar_time::TIMESTAMP) "
            f"FROM read_parquet('{glob}')"
        ).fetchone()
        if row and row[0] is not None:
            ts_start = row[0] if isinstance(row[0], dt.datetime) else row[0].as_py()
            ts_end   = row[1] if isinstance(row[1], dt.datetime) else row[1].as_py()
            if hasattr(ts_start, "tzinfo") and ts_start.tzinfo is not None:
                ts_start = ts_start.replace(tzinfo=None)
            if hasattr(ts_end, "tzinfo") and ts_end.tzinfo is not None:
                ts_end = ts_end.replace(tzinfo=None)
            return ts_start, ts_end
    except Exception as exc:
        print(f"    WARNING: could not query coverage for manifest: {exc}", flush=True)
    return None, None


# ---------------------------------------------------------------------------
# Progress helper
# ---------------------------------------------------------------------------

def _progress_iter(items: list[str], desc: str):
    try:
        from tqdm import tqdm
        return tqdm(items, desc=desc, unit="day")
    except ImportError:
        total = len(items)

        class _Fallback:
            def __iter__(self_):
                for i, item in enumerate(items, 1):
                    print(f"  [{desc}] {i}/{total}: {item}", flush=True)
                    yield item

        return _Fallback()


# ---------------------------------------------------------------------------
# Spec hash
# ---------------------------------------------------------------------------

def _spec_hash(
    derived_id: str, source_dataset_id: str, instrument_id: str, metric_type: str
) -> str:
    payload = f"{derived_id}|{source_dataset_id}|{instrument_id}|{metric_type}|bvc_1m|v1"
    return hashlib.sha256(payload.encode()).hexdigest()


# ---------------------------------------------------------------------------
# Core per-dataset processor
# ---------------------------------------------------------------------------

def _process_proxy_dataset(
    snapshot: dict[str, Any],
    dataset_row: dict[str, Any],
    paths_row: dict[str, Any],
    force_rebuild: bool,
    agg_con: duckdb.DuckDBPyConnection,
    reg_con: duckdb.DuckDBPyConnection,
) -> None:
    derived_id    = dataset_row["dataset_id"]
    source_id     = (dataset_row.get("source_path_or_id") or "").strip()
    table_name    = (dataset_row.get("canonical_table_name") or "").strip()
    notes         = _parse_notes(dataset_row.get("notes"))
    instrument_id = notes.get("instrument_id", "").strip()
    metric_type   = notes.get("metric_type", "").strip().lower()

    if not instrument_id:
        raise ValueError(f"{derived_id}: missing 'instrument_id' in DATASETS notes.")
    if not source_id:
        raise ValueError(f"{derived_id}: missing source_path_or_id.")
    if metric_type not in ("footprint", "cvd"):
        raise ValueError(
            f"{derived_id}: unsupported metric_type={metric_type!r}. "
            "Expected 'footprint' or 'cvd'."
        )

    source_row    = _find_dataset_by_id(snapshot, source_id)
    canonical_dir = Path(paths_row["CANONICAL_DIR"])
    data_root     = Path(paths_row["DATA_ROOT"])

    source_root  = canonical_dir / source_row["canonical_table_name"]
    output_root  = data_root / "derived" / table_name / instrument_id
    output_root.mkdir(parents=True, exist_ok=True)

    spec_h = _spec_hash(derived_id, source_id, instrument_id, metric_type)

    print(f"\n{'='*60}", flush=True)
    print(f"  Dataset    : {derived_id}  (instrument={instrument_id})", flush=True)
    print(f"  Metric type: {metric_type} (BVC proxy from 1s OHLCV)", flush=True)
    print(f"  Source     : {source_root}", flush=True)
    print(f"  Output     : {output_root}", flush=True)
    print(f"{'='*60}", flush=True)

    for session in SESSIONS:
        source_dates = _list_dates(source_root, session)

        if not source_dates:
            print(f"  [{session}] No source dates found - skipping.", flush=True)
            continue

        already_built = set() if force_rebuild else _built_dates(output_root, session)
        to_build      = [d for d in source_dates if d not in already_built]

        print(
            f"\n  [{session}] source={len(source_dates)}  built={len(already_built)}"
            f"  to_build={len(to_build)}",
            flush=True,
        )

        if not to_build:
            print(
                f"  [{session}] All dates already built "
                f"(use --force-rebuild to reprocess).",
                flush=True,
            )
        else:
            rows_written = 0

            for date_str in _progress_iter(to_build, f"{derived_id}/{session}"):
                source_date_dir = source_root / session / date_str
                parquet_files   = sorted(source_date_dir.glob("part-*.parquet"))

                if not parquet_files:
                    print(
                        f"    WARNING: no parquet files in {source_date_dir}",
                        flush=True,
                    )
                    continue

                if metric_type == "footprint":
                    table = _build_footprint_proxy(parquet_files, agg_con)
                else:
                    table = _build_cvd_proxy(parquet_files, agg_con)

                if table.num_rows == 0:
                    print(
                        f"    WARNING: 0 rows after aggregation for {date_str} - skipping.",
                        flush=True,
                    )
                    continue

                out_dir = output_root / session / date_str
                out_dir.mkdir(parents=True, exist_ok=True)
                pq.write_table(table, out_dir / "part-0.parquet", compression="snappy")
                rows_written += table.num_rows

            print(f"  [{session}] Rows written (new): {rows_written}", flush=True)

        # Update manifest with full coverage across ALL built dates.
        ts_start, ts_end = _query_full_coverage(output_root, session, agg_con)
        if ts_start is not None:
            manifest_id = f"{derived_id}_{session}"
            _upsert_manifest(
                reg_con,
                derived_id=manifest_id,
                table_name=table_name,
                spec_hash=spec_h,
                session=session,
                coverage_start=ts_start,
                coverage_end=ts_end,
                parquet_path=str(output_root / session),
            )
            print(f"  [{session}] Manifest updated: {ts_start} -> {ts_end}", flush=True)
        else:
            print(f"  [{session}] No output found - manifest not updated.", flush=True)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Build 1-minute footprint_proxy_1m and cvd_proxy_1m "
            "from canonical 1s OHLCV data using BVC."
        )
    )
    parser.add_argument(
        "--force-rebuild",
        action="store_true",
        help="Re-process all dates, overwriting any existing output.",
    )
    parser.add_argument(
        "--dataset-id",
        default=None,
        metavar="ID",
        help=(
            "Only process this specific dataset_id "
            "(default: all derived_trade_metrics_proxy rows)."
        ),
    )
    args = parser.parse_args()

    snapshot  = _load_snapshot(SNAPSHOT_PATH)
    paths_row = _active_paths(snapshot)

    derived_rows = _find_dataset_rows(snapshot, "derived_trade_metrics_proxy")
    if not derived_rows:
        print(
            "ERROR: No DATASETS rows with "
            "dataset_type='derived_trade_metrics_proxy' found.\n"
            "Run tools/add_trade_metrics_proxy_config.py first.",
            file=sys.stderr,
        )
        return 1

    if args.dataset_id:
        derived_rows = [r for r in derived_rows if r.get("dataset_id") == args.dataset_id]
        if not derived_rows:
            print(
                f"ERROR: No derived_trade_metrics_proxy row found for "
                f"dataset_id={args.dataset_id!r}.",
                file=sys.stderr,
            )
            return 1

    print(
        f"Proxy metric datasets to process: {[r['dataset_id'] for r in derived_rows]}",
        flush=True,
    )

    agg_con = duckdb.connect(":memory:")

    duckdb_path = Path(paths_row["DUCKDB_FILE"])
    reg_con = duckdb.connect(str(duckdb_path))

    try:
        for row in derived_rows:
            _process_proxy_dataset(
                snapshot, row, paths_row, args.force_rebuild, agg_con, reg_con
            )
    finally:
        reg_con.close()
        agg_con.close()

    print("\nDONE", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

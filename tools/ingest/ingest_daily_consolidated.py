"""
INSTRUCTION HEADER
What this file does: Ingests the DAILY_CONSOLIDATED_XLSM workbook into a canonical long-format parquet dataset.
Where it runs: Terminal [command line].
Inputs: `config/exports/config_snapshot_latest.json`, source XLSM path from that snapshot, DuckDB at
`E:/BacktestData/duckdb/research.duckdb`.
Outputs: Parquet dataset under `E:/BacktestData/canonical/daily_series` partitioned by `year`, plus registry updates in DuckDB.
How to run: `pybt tools/ingest/ingest_daily_consolidated.py`
Also: `C:/Users/pcash/anaconda3/envs/backtest/python.exe tools/ingest/ingest_daily_consolidated.py`
What success looks like: prints ingested row count, min/max date, output path, and the inserted DuckDB manifest row.
Common failures + fixes: snapshot missing -> run `pybt tools/admin/export_config_snapshot.py`; source file missing -> fix path in DATASETS;
openpyxl missing -> `pybt -m pip install openpyxl`; duckdb missing -> `pybt -m pip install duckdb`.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds


SNAPSHOT_PATH = Path("config/exports/config_snapshot_latest.json")
DATA_ROOT = Path("E:/BacktestData")
CANONICAL_ROOT = DATA_ROOT / "canonical" / "daily_series"
DUCKDB_PATH = DATA_ROOT / "duckdb" / "research.duckdb"
DATASET_ID = "DAILY_CONSOLIDATED_XLSM"


def _load_snapshot(path: Path) -> dict[str, Any]:
    """Load the JSON config snapshot from disk."""
    if not path.exists():
        raise FileNotFoundError(f"Missing config snapshot: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _find_dataset_row(snapshot: dict[str, Any], dataset_id: str) -> dict[str, Any]:
    """Return the DATASETS row matching dataset_id."""
    datasets = snapshot.get("sheets", {}).get("DATASETS", [])
    for row in datasets:
        if row.get("dataset_id") == dataset_id:
            return row
    raise ValueError(f"Dataset not found in snapshot: {dataset_id}")


def _parse_ingest_columns(notes: str) -> list[str]:
    """Extract ingest columns from notes text."""
    marker = "Ingest columns:"
    if marker not in notes:
        raise ValueError("Notes missing 'Ingest columns:' section.")
    tail = notes.split(marker, 1)[1]
    if "Source file:" in tail:
        tail = tail.split("Source file:", 1)[0]
    tail = tail.replace("\n", " ")
    cols = [c.strip() for c in tail.split(",")]
    cleaned = [c.rstrip(" .;") for c in cols]
    return [c for c in cleaned if c]


def _select_sheet_name(xlsx_path: str) -> str | int:
    """Return 'Data' if present; otherwise return first sheet index 0."""
    xl = pd.ExcelFile(xlsx_path, engine="openpyxl")
    if "Data" in xl.sheet_names:
        return "Data"
    return 0


def _read_source_df(xlsx_path: str, date_col: str, ingest_cols: list[str]) -> pd.DataFrame:
    """Read source XLSM and return a cleaned wide dataframe."""
    sheet = _select_sheet_name(xlsx_path)
    cols = [date_col, *ingest_cols]
    seen = set()
    usecols = [c for c in cols if not (c in seen or seen.add(c))]

    df = pd.read_excel(
        xlsx_path,
        sheet_name=sheet,
        engine="openpyxl",
        usecols=usecols,
    )
    if date_col not in df.columns:
        raise ValueError(f"Missing date column in sheet: {date_col}")

    df[date_col] = pd.to_datetime(df[date_col], errors="raise").dt.normalize()
    df = df.rename(columns={date_col: "date"})
    return df


def _to_long(df: pd.DataFrame, ingest_cols: list[str]) -> pd.DataFrame:
    """Reshape wide data to long (date, series_id, value) and drop NaNs."""
    missing = [c for c in ingest_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing ingest columns in source: {missing}")

    long_df = df.melt(id_vars=["date"], value_vars=ingest_cols, var_name="series_id", value_name="value")
    long_df = long_df.dropna(subset=["value"])
    if long_df.empty:
        raise ValueError("No non-null rows after melt; nothing to ingest.")
    return long_df


def _write_parquet(long_df: pd.DataFrame, output_root: Path) -> None:
    """Write long dataframe as partitioned parquet dataset."""
    output_root.mkdir(parents=True, exist_ok=True)
    long_df = long_df.copy()
    long_df["year"] = long_df["date"].dt.year
    table = pa.Table.from_pandas(long_df, preserve_index=False)
    ds.write_dataset(
        table,
        base_dir=str(output_root),
        format="parquet",
        partitioning=["year"],
        existing_data_behavior="overwrite_or_ignore",
    )


def _spec_hash(dataset_id: str, ingest_cols: list[str]) -> str:
    """Stable hash from dataset_id and ingest column list."""
    payload = f"{dataset_id}|{','.join(ingest_cols)}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _upsert_registry(
    dataset_id: str,
    source_path: str,
    ingest_cols: list[str],
    date_col: str,
    tz: str,
    known_time_rule: str,
    coverage_start: dt.datetime,
    coverage_end: dt.datetime,
    output_root: Path,
) -> dict[str, Any]:
    """Update registry_datasets and insert a manifest_derived_tables row."""
    spec_json = json.dumps(
        {
            "source_path": source_path,
            "ingested_columns": ingest_cols,
            "date_col": date_col,
            "tz": tz,
            "known_time_rule": known_time_rule,
        }
    )

    spec_hash = _spec_hash(dataset_id, ingest_cols)
    now = dt.datetime.now()

    con = duckdb.connect(str(DUCKDB_PATH))
    con.execute(
        """
        INSERT OR REPLACE INTO registry_datasets
        (dataset_id, dataset_type, source_type, spec_json, updated_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        [dataset_id, "daily_series_wide", "xlsm", spec_json, now],
    )
    con.execute(
        """
        INSERT INTO manifest_derived_tables
        (derived_id, table_name, spec_hash, session, coverage_start, coverage_end, parquet_path, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            "canonical_daily_series",
            "daily_series",
            spec_hash,
            "DAILY",
            coverage_start,
            coverage_end,
            str(output_root),
            now,
        ],
    )
    row = con.execute(
        """
        SELECT * FROM manifest_derived_tables
        WHERE derived_id='canonical_daily_series'
        ORDER BY created_at DESC
        LIMIT 1
        """
    ).fetchone()
    con.close()

    return {
        "manifest_row": row,
        "spec_hash": spec_hash,
    }


def main() -> int:
    snapshot = _load_snapshot(SNAPSHOT_PATH)
    dataset = _find_dataset_row(snapshot, DATASET_ID)

    notes = dataset.get("notes") or ""
    ingest_cols = _parse_ingest_columns(notes)
    source_path = dataset.get("source_path_or_id")
    if not source_path:
        raise ValueError("Missing source_path_or_id in dataset row.")

    date_col = dataset.get("date_col") or "date"
    tz = dataset.get("timestamp_tz") or ""
    known_time_rule = dataset.get("known_time_rule") or ""

    df = _read_source_df(source_path, date_col, ingest_cols)
    long_df = _to_long(df, ingest_cols)

    _write_parquet(long_df, CANONICAL_ROOT)

    coverage_start = long_df["date"].min().to_pydatetime()
    coverage_end = long_df["date"].max().to_pydatetime()

    registry_info = _upsert_registry(
        dataset_id=DATASET_ID,
        source_path=source_path,
        ingest_cols=ingest_cols,
        date_col="date",
        tz=tz,
        known_time_rule=known_time_rule,
        coverage_start=coverage_start,
        coverage_end=coverage_end,
        output_root=CANONICAL_ROOT,
    )

    print(f"Rows ingested: {len(long_df)}")
    print(f"Min date: {coverage_start.date()}")
    print(f"Max date: {coverage_end.date()}")
    print(f"Output parquet root: {CANONICAL_ROOT}")
    print(f"Manifest row: {registry_info['manifest_row']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

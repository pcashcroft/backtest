"""
INSTRUCTION HEADER
What this file does: Ingests Macro_Instruments.xlsx into canonical long-format daily_series parquet.
Where it runs: Terminal (repo root).
Inputs: `config/exports/config_snapshot_latest.json`, source XLSX path from that snapshot, DuckDB at
`E:/BacktestData/duckdb/research.duckdb`.
Outputs: Parquet dataset under `E:/BacktestData/canonical/daily_series` partitioned by `year`, plus registry updates in DuckDB.
How to run: `C:/Users/pcash/anaconda3/envs/backtest/python.exe tools/ingest_daily_macro_instruments.py`
What success looks like: Prints sheet list, row counts, min/max date, output path, series checks, and inserted manifest row.
Common failures + fixes: snapshot missing -> run `tools/export_config_snapshot.py`; source file missing -> fix path in DATASETS;
openpyxl missing -> install packages; duckdb missing -> install duckdb.
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


SNAPSHOT_PATH = Path("config/exports/config_snapshot_latest.json")
DATA_ROOT = Path("E:/BacktestData")
CANONICAL_ROOT = DATA_ROOT / "canonical" / "daily_series"
DUCKDB_PATH = DATA_ROOT / "duckdb" / "research.duckdb"
DATASET_ID = "DAILY_MACRO_INSTRUMENTS_XLSX"
MAPPING_RULE_VERSION = "v1"


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


def _parse_notes_kv(notes: str) -> dict[str, str]:
    """Parse key: value pairs from notes (one per line)."""
    parsed: dict[str, str] = {}
    for raw_line in notes.splitlines():
        line = raw_line.strip()
        if not line or ":" not in line:
            continue
        key, value = line.split(":", 1)
        parsed[key.strip()] = value.strip()
    return parsed


def _parse_required_series_ids(value: str) -> list[str]:
    """Parse comma-separated series ids."""
    if not value:
        return []
    items = [v.strip() for v in value.split(",")]
    return [v for v in items if v]


def _load_workbook_sheets(xlsx_path: str) -> dict[str, pd.DataFrame]:
    """Load all sheets from the workbook into dataframes."""
    return pd.read_excel(xlsx_path, sheet_name=None, engine="openpyxl")


def _clean_dates(df: pd.DataFrame, sheet_name: str) -> pd.DataFrame:
    """Normalize date column and remove metadata/non-date rows."""
    if df.empty:
        raise ValueError(f"Sheet '{sheet_name}' is empty.")

    date_col = df.columns[0]
    df = df.rename(columns={date_col: "date"})

    stripped = df["date"].astype(str).str.strip()
    df = df[~stripped.eq("DATES")].copy()

    parsed = pd.to_datetime(df["date"], errors="coerce")
    df = df[~parsed.isna()].copy()
    df["date"] = parsed[~parsed.isna()].dt.normalize()
    return df


def _map_columns(sheet_name: str, data_cols: list[str]) -> dict[str, str]:
    """Map data columns to canonical field names."""
    if len(data_cols) == 4:
        fields = ["open", "high", "low", "close"]
    elif len(data_cols) == 5:
        fields = ["open", "high", "low", "close", "volume"]
    elif len(data_cols) == 1:
        fields = ["last"]
    else:
        raise ValueError(
            f"Unsupported column count in sheet '{sheet_name}': {len(data_cols)} columns {data_cols}"
        )
    return dict(zip(data_cols, fields, strict=True))


def _coerce_numeric(df: pd.DataFrame, cols: Iterable[str]) -> pd.DataFrame:
    """Coerce mapped columns to numeric."""
    for col in cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def _sheet_to_long(sheet_name: str, df: pd.DataFrame, prefix_mode: str) -> pd.DataFrame:
    """Process a single sheet into long format."""
    df = _clean_dates(df, sheet_name)
    data_cols = [c for c in df.columns if c != "date"]
    mapping = _map_columns(sheet_name, data_cols)

    df = _coerce_numeric(df, mapping.keys())
    df = df.dropna(subset=list(mapping.keys()), how="all")

    if df.empty:
        raise ValueError(f"No usable rows after cleaning sheet '{sheet_name}'.")

    if prefix_mode == "sheet_name":
        prefix = sheet_name
    elif prefix_mode == "first_data_col":
        prefix = data_cols[0]
    else:
        raise ValueError(f"Unrecognized series_id_prefix_mode: {prefix_mode}")

    long_frames = []
    for source_col, field in mapping.items():
        tmp = df[["date", source_col]].copy()
        tmp = tmp.rename(columns={source_col: "value"})
        tmp["series_id"] = f"{prefix}|{field}"
        tmp = tmp.dropna(subset=["value"])
        long_frames.append(tmp)

    long_df = pd.concat(long_frames, ignore_index=True)
    if long_df.empty:
        raise ValueError(f"No long rows produced for sheet '{sheet_name}'.")
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


def _spec_hash(dataset_id: str, sheet_names: list[str], mapping_rule_version: str) -> str:
    """Stable hash from dataset_id, sheet names, and mapping rule version."""
    payload = f"{dataset_id}|{','.join(sorted(sheet_names))}|{mapping_rule_version}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _upsert_registry(
    dataset_id: str,
    source_path: str,
    sheet_names: list[str],
    mapping_rule_version: str,
    timestamp_tz: str,
    known_time_rule: str,
    coverage_start: dt.datetime,
    coverage_end: dt.datetime,
    output_root: Path,
) -> dict[str, Any]:
    """Update registry_datasets and insert a manifest_derived_tables row."""
    spec_json = json.dumps(
        {
            "source_path": source_path,
            "sheet_names": sheet_names,
            "mapping_rule_version": mapping_rule_version,
            "timestamp_tz": timestamp_tz,
            "known_time_rule": known_time_rule,
        }
    )

    spec_hash = _spec_hash(dataset_id, sheet_names, mapping_rule_version)
    now = dt.datetime.now()

    con = duckdb.connect(str(DUCKDB_PATH))
    con.execute(
        """
        INSERT OR REPLACE INTO registry_datasets
        (dataset_id, dataset_type, source_type, spec_json, updated_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        [dataset_id, "daily_series_wide", "xlsx", spec_json, now],
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


def _verify_series_ids(long_df: pd.DataFrame, required: list[str]) -> None:
    """Verify required series_id values exist."""
    if not required:
        print("No required_series_ids specified; skipping required-series validation.")
        return
    present = set(long_df["series_id"].unique())
    missing = [s for s in required if s not in present]
    if missing:
        raise ValueError(f"Missing required series_id values: {missing}")
    print("Required-series validation passed.")


def _print_top_series(long_df: pd.DataFrame, top_n: int = 10) -> None:
    """Print top series_id by row count."""
    counts = long_df["series_id"].value_counts().head(top_n)
    print("Top series_id by row count:")
    for series_id, count in counts.items():
        print(f"  {series_id}: {count}")


def main() -> int:
    snapshot = _load_snapshot(SNAPSHOT_PATH)
    dataset = _find_dataset_row(snapshot, DATASET_ID)

    source_path = dataset.get("source_path_or_id")
    if not source_path:
        raise ValueError("Missing source_path_or_id in dataset row.")
    if not Path(source_path).exists():
        raise FileNotFoundError(f"Source file not found: {source_path}")

    timestamp_tz = dataset.get("timestamp_tz") or ""
    known_time_rule = dataset.get("known_time_rule") or ""
    notes = dataset.get("notes") or ""
    notes_kv = _parse_notes_kv(notes)
    prefix_mode = notes_kv.get("series_id_prefix_mode", "sheet_name")
    required_series = _parse_required_series_ids(notes_kv.get("required_series_ids", ""))

    sheets = _load_workbook_sheets(source_path)
    if not sheets:
        raise ValueError("No sheets found in workbook.")

    long_frames = []
    for sheet_name, df in sheets.items():
        long_frames.append(_sheet_to_long(sheet_name, df, prefix_mode))

    long_df = pd.concat(long_frames, ignore_index=True)
    if long_df.empty:
        raise ValueError("No long rows produced across all sheets; nothing to ingest.")

    print(f"series_id_prefix_mode: {prefix_mode}")
    if required_series:
        print(f"required_series_ids: {required_series}")
    _verify_series_ids(long_df, required_series)

    _write_parquet(long_df, CANONICAL_ROOT)

    coverage_start = long_df["date"].min().to_pydatetime()
    coverage_end = long_df["date"].max().to_pydatetime()

    registry_info = _upsert_registry(
        dataset_id=DATASET_ID,
        source_path=source_path,
        sheet_names=list(sheets.keys()),
        mapping_rule_version=MAPPING_RULE_VERSION,
        timestamp_tz=timestamp_tz,
        known_time_rule=known_time_rule,
        coverage_start=coverage_start,
        coverage_end=coverage_end,
        output_root=CANONICAL_ROOT,
    )

    unique_series = long_df["series_id"].nunique()
    min_date = coverage_start.date()
    max_date = coverage_end.date()

    print(f"Sheets processed ({len(sheets)}): {list(sheets.keys())}")
    print(f"Total long rows written: {len(long_df)}")
    print(f"Unique series_id count: {unique_series}")
    print(f"Min date: {min_date}")
    print(f"Max date: {max_date}")
    print(f"Output parquet root: {CANONICAL_ROOT}")
    print(f"Manifest row: {registry_info['manifest_row']}")
    _print_top_series(long_df, top_n=10)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

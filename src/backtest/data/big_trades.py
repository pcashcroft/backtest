"""
INSTRUCTION HEADER

What this module does (plain English):
- Computes big-trade events on-the-fly from canonical parquet (no pre-computed output files).
- Supports real tick-data trades and BVC-derived proxy from 1s OHLCV.
- Three threshold methods: fixed_count, rolling_pct, z_score — each configured via dedicated
  DATASETS columns (threshold_method, threshold_min_size, threshold_pct, threshold_z,
  threshold_window_days).
- Five source modes: real_only, proxy_only, real_then_proxy, proxy_then_real, both.

How to use (Jupyter / notebook):
  from backtest.data.big_trades import get_big_trades
  df = get_big_trades("ES", "RTH", "2025-12-01", "2025-12-31")

Big trade events — on-the-fly computation from canonical parquet.

Public API:

    from backtest.data.big_trades import get_big_trades

    df = get_big_trades(
        instrument_id="ES",
        session="RTH",                # "FULL" or "RTH"
        start_date="2025-12-01",
        end_date="2025-12-31",
        snapshot=snapshot,            # dict from load_snapshot()
    )

    # df columns: ts_event, symbol, price, size, side
    #   ts_event : datetime[us, UTC]
    #   symbol   : str   (e.g. "ESH5")
    #   price    : float
    #   size     : int   (contracts)
    #   side     : str   "B"=buy/green  "S"=sell/red  "N"=neutral/grey

Threshold methods (set via DATASETS columns for each dataset):

    fixed_count  → filter WHERE size >= threshold_min_size

    rolling_pct  → threshold = percentile(threshold_pct) over a lookback window
                   threshold_window_days calendar days before start_date are loaded
                   to compute the distribution; only events within [start_date,
                   end_date] are returned.

    z_score      → threshold = mean + threshold_z * stddev over the lookback window
                   same window_days approach as rolling_pct

Source mode (set in INSTRUMENTS.big_trades_source_mode):

    real_only        → only real trades (big_trades_dataset_id)
    proxy_only       → only proxy data  (big_trades_proxy_dataset_id)
    real_then_proxy  → real trades where available (date coverage check);
                       proxy for all other dates
    proxy_then_real  → proxy first, real overrides where available
    both             → union of real + proxy (duplicates possible; use for comparison)
"""

from __future__ import annotations

import datetime as dt
import json
import os
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SESSIONS = ("FULL", "RTH")
SNAPSHOT_PATH = Path("config/exports/config_snapshot_latest.json")


# ---------------------------------------------------------------------------
# Snapshot helpers (self-contained so this module has no src imports needed
# for standalone use in Jupyter; can also call load_snapshot from config)
# ---------------------------------------------------------------------------

def _load_snapshot_if_needed(snapshot: dict[str, Any] | None) -> dict[str, Any]:
    if snapshot is not None:
        return snapshot
    if not SNAPSHOT_PATH.exists():
        raise FileNotFoundError(
            f"Config snapshot not found: {SNAPSHOT_PATH}. "
            "Run tools/admin/export_config_snapshot.py first."
        )
    try:
        import orjson
        return orjson.loads(SNAPSHOT_PATH.read_bytes())
    except ImportError:
        return json.loads(SNAPSHOT_PATH.read_text(encoding="utf-8"))


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


def _find_dataset_by_id(snapshot: dict[str, Any], dataset_id: str) -> dict[str, Any]:
    for r in snapshot.get("sheets", {}).get("DATASETS", []) or []:
        if r.get("dataset_id") == dataset_id:
            return r
    raise ValueError(f"Dataset not found in snapshot: {dataset_id!r}")


def _find_instrument(snapshot: dict[str, Any], instrument_id: str) -> dict[str, Any]:
    for r in snapshot.get("sheets", {}).get("INSTRUMENTS", []) or []:
        if r.get("instrument_id") == instrument_id:
            return r
    raise ValueError(f"Instrument not found in snapshot: {instrument_id!r}")


def _parse_notes(notes: str | None) -> dict[str, str]:
    """Parse KEY: VALUE lines from a DATASETS notes field (legacy fallback)."""
    out: dict[str, str] = {}
    for raw in (notes or "").splitlines():
        line = raw.strip()
        if line and ":" in line:
            k, v = line.split(":", 1)
            out[k.strip()] = v.strip()
    return out


def _get_threshold_config(ds: dict[str, Any]) -> dict[str, Any]:
    """
    Read threshold configuration from a DATASETS row.

    Reads from dedicated columns (threshold_method, threshold_min_size,
    threshold_pct, threshold_z, threshold_window_days) first; falls back
    to parsing the notes field for backward compatibility.

    Returns a dict with keys:
      threshold_method    : str  ("fixed_count", "rolling_pct", "z_score")
      threshold_min_size  : int  (for fixed_count)
      threshold_pct       : float (for rolling_pct, e.g. 99.0)
      threshold_z         : float (for z_score, e.g. 2.5)
      threshold_window_days: int (for rolling_pct + z_score)
    """
    # Primary: dedicated columns
    method = ds.get("threshold_method")

    # Fallback: parse notes for backward compat with old rows
    notes = _parse_notes(ds.get("notes"))

    if not method:
        method = notes.get("threshold_method", "fixed_count")

    def _col_or_notes(col_key: str, notes_key: str, cast, default):
        v = ds.get(col_key)
        if v is not None and str(v).strip() != "":
            try:
                return cast(v)
            except (TypeError, ValueError):
                pass
        v_notes = notes.get(notes_key)
        if v_notes is not None:
            try:
                return cast(v_notes)
            except (TypeError, ValueError):
                pass
        return default

    return {
        "threshold_method":      str(method).strip(),
        "threshold_min_size":    _col_or_notes("threshold_min_size",  "min_size",    int,   50),
        "threshold_pct":         _col_or_notes("threshold_pct",       "pct",         float, 99.0),
        "threshold_z":           _col_or_notes("threshold_z",         "z_threshold", float, 2.5),
        "threshold_window_days": _col_or_notes("threshold_window_days","window_days", int,   63),
    }


# ---------------------------------------------------------------------------
# Filesystem helpers
# ---------------------------------------------------------------------------

def _canonical_root(data_root: Path, table_name: str, instrument_id: str) -> Path:
    """
    Map canonical_table_name to the on-disk root.

    Trades canonical: {data_root}/canonical/{instrument_id.lower()}_trades/
    1s OHLCV canonical: {data_root}/canonical/{instrument_id.lower()}_ohlcv_1s/

    We determine this by inspecting the source DATASETS row's canonical_table_name
    and dataset_type to pick the right subfolder name.
    """
    _TABLE_DIR = {
        "es_trades":       f"{instrument_id.lower()}_trades",
        "nq_trades":       f"{instrument_id.lower()}_trades",
        "es_ohlcv_1s":     f"{instrument_id.lower()}_ohlcv_1s",
        "nq_ohlcv_1s":     f"{instrument_id.lower()}_ohlcv_1s",
    }
    # Guess the canonical dir from the table name pattern
    if table_name.endswith("_trades") or "trades" in table_name:
        dir_name = f"{instrument_id.lower()}_trades"
    elif "ohlcv" in table_name or "1s" in table_name:
        dir_name = f"{instrument_id.lower()}_ohlcv_1s"
    else:
        dir_name = table_name
    return data_root / "canonical" / dir_name


def _list_available_dates(root: Path, session: str) -> list[str]:
    """Return sorted list of YYYY-MM-DD date dirs under root/session/."""
    session_dir = root / session
    if not session_dir.exists():
        return []
    return sorted(
        d for d in os.listdir(session_dir)
        if len(d) == 10 and (session_dir / d).is_dir()
    )


def _parquet_files_for_dates(
    root: Path,
    session: str,
    dates: list[str],
) -> list[Path]:
    """Collect part-*.parquet files for the given dates."""
    files: list[Path] = []
    session_dir = root / session
    for d in dates:
        date_dir = session_dir / d
        if date_dir.exists():
            files.extend(sorted(date_dir.glob("part-*.parquet")))
    return files


def _dates_in_range(
    available: list[str],
    start_date: str,
    end_date: str,
) -> list[str]:
    return [d for d in available if start_date <= d <= end_date]


def _lookback_start(start_date: str, window_days: int) -> str:
    """Extend start_date backward by window_days calendar days."""
    d = dt.date.fromisoformat(start_date) - dt.timedelta(days=window_days)
    return d.isoformat()


# ---------------------------------------------------------------------------
# DuckDB helpers
# ---------------------------------------------------------------------------

def _file_list_sql(files: list[Path]) -> str:
    """Format a list of paths as a DuckDB array literal."""
    return (
        "["
        + ", ".join(f"'{str(p).replace(chr(92), '/')}'" for p in files)
        + "]"
    )


# ---------------------------------------------------------------------------
# Real trades computation
# ---------------------------------------------------------------------------

def _compute_real(
    data_root: Path,
    source_dataset: dict[str, Any],
    instrument_id: str,
    session: str,
    start_date: str,
    end_date: str,
    threshold_config: dict[str, Any],
    con: duckdb.DuckDBPyConnection,
) -> pd.DataFrame:
    """
    Query canonical trades parquet and return big-trade events.

    Side encoding in canonical: Int16  2→'B', 1→'S', 0→'N'
    """
    canon_root = _canonical_root(
        data_root,
        source_dataset.get("canonical_table_name", ""),
        instrument_id,
    )

    threshold_method = threshold_config["threshold_method"]
    window_days = threshold_config["threshold_window_days"]

    # Determine date range to load (extended for rolling methods)
    available = _list_available_dates(canon_root, session)
    if not available:
        return _empty_df()

    if threshold_method == "fixed_count":
        load_dates = _dates_in_range(available, start_date, end_date)
    else:
        lb_start = _lookback_start(start_date, window_days)
        load_dates = _dates_in_range(available, lb_start, end_date)

    if not load_dates:
        return _empty_df()

    files = _parquet_files_for_dates(canon_root, session, load_dates)
    if not files:
        return _empty_df()

    fl = _file_list_sql(files)

    if threshold_method == "fixed_count":
        min_size = threshold_config["threshold_min_size"]
        sql = f"""
            SELECT
                ts_event,
                symbol,
                CAST(price AS DOUBLE)  AS price,
                CAST(size  AS BIGINT)  AS size,
                CASE WHEN CAST(side AS INTEGER) = 2 THEN 'B'
                     WHEN CAST(side AS INTEGER) = 1 THEN 'S'
                     ELSE 'N' END      AS side
            FROM read_parquet({fl})
            WHERE CAST(size AS BIGINT) >= {min_size}
              AND symbol NOT LIKE '%-%'
            ORDER BY ts_event
        """

    elif threshold_method == "rolling_pct":
        pct = threshold_config["threshold_pct"]
        sql = f"""
            WITH base AS (
                SELECT
                    ts_event,
                    symbol,
                    CAST(price AS DOUBLE)  AS price,
                    CAST(size  AS BIGINT)  AS size,
                    CASE WHEN CAST(side AS INTEGER) = 2 THEN 'B'
                         WHEN CAST(side AS INTEGER) = 1 THEN 'S'
                         ELSE 'N' END      AS side
                FROM read_parquet({fl})
                WHERE symbol NOT LIKE '%-%'
            ),
            threshold AS (
                SELECT PERCENTILE_CONT({pct / 100.0}) WITHIN GROUP (ORDER BY size) AS cutoff
                FROM base
                WHERE ts_event < TIMESTAMPTZ '{start_date} 00:00:00+00'
            )
            SELECT b.ts_event, b.symbol, b.price, b.size, b.side
            FROM base b, threshold t
            WHERE b.ts_event >= TIMESTAMPTZ '{start_date} 00:00:00+00'
              AND b.ts_event <= TIMESTAMPTZ '{end_date} 23:59:59+00'
              AND b.size >= t.cutoff
            ORDER BY b.ts_event
        """

    elif threshold_method == "z_score":
        z_threshold = threshold_config["threshold_z"]
        sql = f"""
            WITH base AS (
                SELECT
                    ts_event,
                    symbol,
                    CAST(price AS DOUBLE)  AS price,
                    CAST(size  AS BIGINT)  AS size,
                    CASE WHEN CAST(side AS INTEGER) = 2 THEN 'B'
                         WHEN CAST(side AS INTEGER) = 1 THEN 'S'
                         ELSE 'N' END      AS side
                FROM read_parquet({fl})
                WHERE symbol NOT LIKE '%-%'
            ),
            stats AS (
                SELECT
                    AVG(CAST(size AS DOUBLE))    AS mu,
                    STDDEV(CAST(size AS DOUBLE)) AS sigma
                FROM base
                WHERE ts_event < TIMESTAMPTZ '{start_date} 00:00:00+00'
            )
            SELECT b.ts_event, b.symbol, b.price, b.size, b.side
            FROM base b, stats s
            WHERE b.ts_event >= TIMESTAMPTZ '{start_date} 00:00:00+00'
              AND b.ts_event <= TIMESTAMPTZ '{end_date} 23:59:59+00'
              AND CAST(b.size AS DOUBLE) >= s.mu + {z_threshold} * s.sigma
            ORDER BY b.ts_event
        """

    else:
        raise ValueError(
            f"Unknown threshold_method {threshold_method!r}. "
            "Expected: fixed_count, rolling_pct, z_score."
        )

    return con.execute(sql).df()


# ---------------------------------------------------------------------------
# Proxy (BVC from 1s OHLCV) computation
# ---------------------------------------------------------------------------

def _compute_proxy(
    data_root: Path,
    source_dataset: dict[str, Any],
    instrument_id: str,
    session: str,
    start_date: str,
    end_date: str,
    threshold_config: dict[str, Any],
    con: duckdb.DuckDBPyConnection,
) -> pd.DataFrame:
    """
    Derive big-trade events from 1s OHLCV using BVC.

    buy_frac = (close - low) / (high - low) for non-doji bars
             = 0.5 for doji bars (high == low) → SKIPPED (ambiguous direction)

    Buy event:  price=high, size=round(volume * buy_frac)   when buy_frac > 0.5
    Sell event: price=low,  size=round(volume * sell_frac)  when buy_frac < 0.5
    """
    canon_root = _canonical_root(
        data_root,
        source_dataset.get("canonical_table_name", ""),
        instrument_id,
    )

    threshold_method = threshold_config["threshold_method"]
    window_days = threshold_config["threshold_window_days"]

    available = _list_available_dates(canon_root, session)
    if not available:
        return _empty_df()

    if threshold_method == "fixed_count":
        load_dates = _dates_in_range(available, start_date, end_date)
    else:
        lb_start = _lookback_start(start_date, window_days)
        load_dates = _dates_in_range(available, lb_start, end_date)

    if not load_dates:
        return _empty_df()

    files = _parquet_files_for_dates(canon_root, session, load_dates)
    if not files:
        return _empty_df()

    fl = _file_list_sql(files)

    if threshold_method == "fixed_count":
        min_size = threshold_config["threshold_min_size"]
        sql = f"""
            WITH bvc AS (
                SELECT
                    ts_event,
                    symbol,
                    CAST(high   AS DOUBLE)  AS high,
                    CAST(low    AS DOUBLE)  AS low,
                    CAST(volume AS BIGINT)  AS volume,
                    CASE WHEN high = low THEN 0.5
                         ELSE CAST((close - low) AS DOUBLE) / CAST((high - low) AS DOUBLE)
                    END AS buy_frac
                FROM read_parquet({fl})
                WHERE volume > 0
            ),
            events AS (
                -- Buy events (proxy: price = high)
                SELECT ts_event, symbol, high AS price,
                       CAST(ROUND(volume * buy_frac)       AS BIGINT) AS size,
                       'B' AS side
                FROM bvc
                WHERE buy_frac > 0.5
                UNION ALL
                -- Sell events (proxy: price = low)
                SELECT ts_event, symbol, low  AS price,
                       CAST(ROUND(volume * (1.0 - buy_frac)) AS BIGINT) AS size,
                       'S' AS side
                FROM bvc
                WHERE buy_frac < 0.5
                -- Doji bars (buy_frac = 0.5) are SKIPPED — direction ambiguous
            )
            SELECT ts_event, symbol, price, size, side
            FROM events
            WHERE size >= {min_size}
            ORDER BY ts_event
        """

    elif threshold_method == "rolling_pct":
        pct = threshold_config["threshold_pct"]
        sql = f"""
            WITH bvc AS (
                SELECT
                    ts_event,
                    symbol,
                    CAST(high   AS DOUBLE)  AS high,
                    CAST(low    AS DOUBLE)  AS low,
                    CAST(volume AS BIGINT)  AS volume,
                    CASE WHEN high = low THEN 0.5
                         ELSE CAST((close - low) AS DOUBLE) / CAST((high - low) AS DOUBLE)
                    END AS buy_frac
                FROM read_parquet({fl})
                WHERE volume > 0
            ),
            events AS (
                SELECT ts_event, symbol, high AS price,
                       CAST(ROUND(volume * buy_frac)         AS BIGINT) AS size,
                       'B' AS side
                FROM bvc WHERE buy_frac > 0.5
                UNION ALL
                SELECT ts_event, symbol, low  AS price,
                       CAST(ROUND(volume * (1.0 - buy_frac)) AS BIGINT) AS size,
                       'S' AS side
                FROM bvc WHERE buy_frac < 0.5
            ),
            threshold AS (
                SELECT PERCENTILE_CONT({pct / 100.0}) WITHIN GROUP (ORDER BY size) AS cutoff
                FROM events
                WHERE ts_event < TIMESTAMPTZ '{start_date} 00:00:00+00'
            )
            SELECT e.ts_event, e.symbol, e.price, e.size, e.side
            FROM events e, threshold t
            WHERE e.ts_event >= TIMESTAMPTZ '{start_date} 00:00:00+00'
              AND e.ts_event <= TIMESTAMPTZ '{end_date} 23:59:59+00'
              AND e.size >= t.cutoff
            ORDER BY e.ts_event
        """

    elif threshold_method == "z_score":
        z_threshold = threshold_config["threshold_z"]
        sql = f"""
            WITH bvc AS (
                SELECT
                    ts_event,
                    symbol,
                    CAST(high   AS DOUBLE)  AS high,
                    CAST(low    AS DOUBLE)  AS low,
                    CAST(volume AS BIGINT)  AS volume,
                    CASE WHEN high = low THEN 0.5
                         ELSE CAST((close - low) AS DOUBLE) / CAST((high - low) AS DOUBLE)
                    END AS buy_frac
                FROM read_parquet({fl})
                WHERE volume > 0
            ),
            events AS (
                SELECT ts_event, symbol, high AS price,
                       CAST(ROUND(volume * buy_frac)         AS BIGINT) AS size,
                       'B' AS side
                FROM bvc WHERE buy_frac > 0.5
                UNION ALL
                SELECT ts_event, symbol, low  AS price,
                       CAST(ROUND(volume * (1.0 - buy_frac)) AS BIGINT) AS size,
                       'S' AS side
                FROM bvc WHERE buy_frac < 0.5
            ),
            stats AS (
                SELECT
                    AVG(CAST(size AS DOUBLE))    AS mu,
                    STDDEV(CAST(size AS DOUBLE)) AS sigma
                FROM events
                WHERE ts_event < TIMESTAMPTZ '{start_date} 00:00:00+00'
            )
            SELECT e.ts_event, e.symbol, e.price, e.size, e.side
            FROM events e, stats s
            WHERE e.ts_event >= TIMESTAMPTZ '{start_date} 00:00:00+00'
              AND e.ts_event <= TIMESTAMPTZ '{end_date} 23:59:59+00'
              AND CAST(e.size AS DOUBLE) >= s.mu + {z_threshold} * s.sigma
            ORDER BY e.ts_event
        """

    else:
        raise ValueError(
            f"Unknown threshold_method {threshold_method!r}. "
            "Expected: fixed_count, rolling_pct, z_score."
        )

    return con.execute(sql).df()


# ---------------------------------------------------------------------------
# Empty result helper
# ---------------------------------------------------------------------------

def _empty_df() -> pd.DataFrame:
    return pd.DataFrame(
        columns=["ts_event", "symbol", "price", "size", "side"]
    ).astype({
        "ts_event": "datetime64[us, UTC]",
        "symbol":   "object",
        "price":    "float64",
        "size":     "int64",
        "side":     "object",
    })


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_big_trades(
    instrument_id: str,
    session: str,
    start_date: str,
    end_date: str,
    snapshot: dict[str, Any] | None = None,
) -> pd.DataFrame:
    """
    Return big-trade events for the requested instrument/session/date window.

    Parameters
    ----------
    instrument_id : str
        As in INSTRUMENTS sheet (e.g. "ES", "NQ").
    session : str
        "FULL" or "RTH".
    start_date : str
        Inclusive start date, "YYYY-MM-DD".
    end_date : str
        Inclusive end date, "YYYY-MM-DD".
    snapshot : dict, optional
        Loaded config snapshot dict. If None, loads from
        config/exports/config_snapshot_latest.json.

    Returns
    -------
    pd.DataFrame
        Columns: ts_event, symbol, price, size, side
        Sorted by ts_event ascending.
        source_mode 'both' may return overlapping events from real + proxy.
    """
    snapshot = _load_snapshot_if_needed(snapshot)
    paths = _active_paths(snapshot)
    data_root = Path(paths["DATA_ROOT"])

    instrument = _find_instrument(snapshot, instrument_id)
    source_mode = instrument.get("big_trades_source_mode") or "real_then_proxy"

    bt_dataset_id  = instrument.get("big_trades_dataset_id")
    btp_dataset_id = instrument.get("big_trades_proxy_dataset_id")

    con = duckdb.connect()  # in-memory; no file needed

    real_df  = pd.DataFrame()
    proxy_df = pd.DataFrame()

    def _get_real() -> pd.DataFrame:
        if not bt_dataset_id:
            return _empty_df()
        ds = _find_dataset_by_id(snapshot, bt_dataset_id)
        src_ds = _find_dataset_by_id(snapshot, ds["source_path_or_id"])
        return _compute_real(
            data_root, src_ds, instrument_id, session,
            start_date, end_date, _get_threshold_config(ds), con,
        )

    def _get_proxy() -> pd.DataFrame:
        if not btp_dataset_id:
            return _empty_df()
        ds = _find_dataset_by_id(snapshot, btp_dataset_id)
        src_ds = _find_dataset_by_id(snapshot, ds["source_path_or_id"])
        return _compute_proxy(
            data_root, src_ds, instrument_id, session,
            start_date, end_date, _get_threshold_config(ds), con,
        )

    if source_mode == "real_only":
        result = _get_real()

    elif source_mode == "proxy_only":
        result = _get_proxy()

    elif source_mode == "real_then_proxy":
        # Real data where available (coverage check), proxy for the rest
        real_df = _get_real()
        if real_df.empty:
            result = _get_proxy()
        else:
            real_dates = set(
                pd.to_datetime(real_df["ts_event"]).dt.date.astype(str)
            )
            proxy_df = _get_proxy()
            if not proxy_df.empty:
                proxy_only = proxy_df[
                    ~pd.to_datetime(proxy_df["ts_event"]).dt.date.astype(str).isin(real_dates)
                ]
                result = pd.concat([real_df, proxy_only], ignore_index=True).sort_values(
                    "ts_event"
                ).reset_index(drop=True)
            else:
                result = real_df

    elif source_mode == "proxy_then_real":
        # Proxy as base; real overrides where it has coverage
        proxy_df = _get_proxy()
        real_df  = _get_real()
        if proxy_df.empty:
            result = real_df
        elif real_df.empty:
            result = proxy_df
        else:
            real_dates = set(
                pd.to_datetime(real_df["ts_event"]).dt.date.astype(str)
            )
            proxy_only = proxy_df[
                ~pd.to_datetime(proxy_df["ts_event"]).dt.date.astype(str).isin(real_dates)
            ]
            result = pd.concat([proxy_only, real_df], ignore_index=True).sort_values(
                "ts_event"
            ).reset_index(drop=True)

    elif source_mode == "both":
        real_df  = _get_real()
        proxy_df = _get_proxy()
        result = pd.concat([real_df, proxy_df], ignore_index=True).sort_values(
            "ts_event"
        ).reset_index(drop=True)

    else:
        raise ValueError(
            f"Unknown big_trades_source_mode {source_mode!r}. "
            "Expected: real_only, proxy_only, real_then_proxy, proxy_then_real, both."
        )

    con.close()

    if result.empty:
        return _empty_df()

    # Ensure correct dtypes
    result["price"] = result["price"].astype("float64")
    result["size"]  = result["size"].astype("int64")
    result["side"]  = result["side"].astype("object")

    return result.reset_index(drop=True)

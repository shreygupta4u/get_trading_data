"""
delta_utils.py
==============
Delta Lake utilities using the `deltalake` Python package (delta-rs).

NO Java required — delta-rs is a pure Python / Rust implementation.
Install with:  pip install deltalake

Delta table layout
------------------
    data/delta/
        candles/               <- daily OHLCV, partitioned by symbol
        weekly_technicals/     <- weekly OHLCV + SMA-33w, partitioned by symbol
"""

import logging
from datetime import date as date_type
from functools import reduce
from pathlib import Path

import pandas as pd

import config

log = logging.getLogger("nasdaq_candles")

# ── Delta table paths ─────────────────────────────────────────────────────────
CANDLES_DELTA_PATH    = str(config.DELTA_DIR / "candles")
TECHNICALS_DELTA_PATH = str(config.DELTA_DIR / "weekly_technicals")


# ── Table helpers ─────────────────────────────────────────────────────────────

def is_delta_table(path: str) -> bool:
    """Return True if a valid Delta table exists at *path*."""
    try:
        from deltalake import DeltaTable
        return DeltaTable.is_deltatable(path)
    except Exception:
        return False


# ── Candles table ─────────────────────────────────────────────────────────────

def upsert_candles(df: pd.DataFrame) -> None:
    """
    Merge a pandas OHLCV DataFrame into the Delta candles table.

    - New (symbol, date) rows are inserted.
    - Existing rows are left unchanged (insert-only merge).
    - Creates the table on the first call.

    The table is partitioned by 'symbol', so each symbol's data lives in
    its own Parquet file(s) under  data/delta/candles/symbol=XXX/ .
    """
    from deltalake import DeltaTable, write_deltalake

    if df is None or df.empty:
        return

    config.DELTA_DIR.mkdir(parents=True, exist_ok=True)

    df = _normalise_candles_schema(df)

    if is_delta_table(CANDLES_DELTA_PATH):
        (
            DeltaTable(CANDLES_DELTA_PATH)
            .merge(
                source=df,
                predicate="t.symbol = s.symbol AND t.date = s.date",
                source_alias="s",
                target_alias="t",
            )
            .when_not_matched_insert_all()
            .execute()
        )
    else:
        write_deltalake(
            CANDLES_DELTA_PATH,
            df,
            mode="overwrite",
            partition_by=["symbol"],
        )

    log.info("  Delta upsert: %d rows / %d symbol(s)",
             len(df), df["symbol"].nunique())


def read_candles(
    symbols: list[str] | None = None,
    start: str | None = None,
    end: str | None = None,
) -> pd.DataFrame:
    """
    Read the candles Delta table into a pandas DataFrame.

    Parameters
    ----------
    symbols : list of ticker strings, or None for all
    start   : 'YYYY-MM-DD' lower bound on date (inclusive)
    end     : 'YYYY-MM-DD' upper bound on date (inclusive)
    """
    import pyarrow.dataset as ds
    from deltalake import DeltaTable

    if not is_delta_table(CANDLES_DELTA_PATH):
        log.warning("Candles Delta table does not exist at %s", CANDLES_DELTA_PATH)
        return pd.DataFrame()

    dt = DeltaTable(CANDLES_DELTA_PATH)

    filters = []
    if symbols:
        filters.append(ds.field("symbol").isin([s.upper() for s in symbols]))
    if start:
        filters.append(ds.field("date") >= date_type.fromisoformat(start))
    if end:
        filters.append(ds.field("date") <= date_type.fromisoformat(end))

    filter_expr = reduce(lambda a, b: a & b, filters) if filters else None
    df = dt.to_pyarrow_dataset().to_table(filter=filter_expr).to_pandas()

    if df.empty:
        return df

    df["date"]   = pd.to_datetime(df["date"])
    df["symbol"] = df["symbol"].astype(str)
    return df.sort_values(["symbol", "date"]).reset_index(drop=True)


# ── Weekly technicals table ───────────────────────────────────────────────────

def upsert_weekly_technicals(df: pd.DataFrame) -> None:
    """
    Merge a weekly technicals DataFrame into the Delta weekly_technicals table.
    Existing rows are *updated* (recalculates SMA when more history is added);
    new rows are inserted.
    """
    from deltalake import DeltaTable, write_deltalake

    if df is None or df.empty:
        return

    config.DELTA_DIR.mkdir(parents=True, exist_ok=True)

    df = df.copy()
    df["week_start"] = pd.to_datetime(df["week_start"]).dt.date
    df["symbol"]     = df["symbol"].astype(str)

    # Ensure numeric types are correct
    for col in ["Open", "High", "Low", "Close", "sma_33w"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype(float)
    if "Volume" in df.columns:
        df["Volume"] = pd.to_numeric(df["Volume"], errors="coerce").fillna(0).astype("int64")
    if "trading_days" in df.columns:
        df["trading_days"] = df["trading_days"].astype("int32")

    if is_delta_table(TECHNICALS_DELTA_PATH):
        (
            DeltaTable(TECHNICALS_DELTA_PATH)
            .merge(
                source=df,
                predicate="t.symbol = s.symbol AND t.week_start = s.week_start",
                source_alias="s",
                target_alias="t",
            )
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .execute()
        )
    else:
        write_deltalake(
            TECHNICALS_DELTA_PATH,
            df,
            mode="overwrite",
            partition_by=["symbol"],
        )

    log.info("  Delta upsert (technicals): %d rows / %d symbol(s)",
             len(df), df["symbol"].nunique())


def read_weekly_technicals(
    symbols: list[str] | None = None,
    start: str | None = None,
    end: str | None = None,
) -> pd.DataFrame:
    """Read the weekly_technicals Delta table into a pandas DataFrame."""
    import pyarrow.dataset as ds
    from deltalake import DeltaTable

    if not is_delta_table(TECHNICALS_DELTA_PATH):
        log.warning("Weekly technicals table does not exist at %s", TECHNICALS_DELTA_PATH)
        return pd.DataFrame()

    dt = DeltaTable(TECHNICALS_DELTA_PATH)

    filters = []
    if symbols:
        filters.append(ds.field("symbol").isin([s.upper() for s in symbols]))
    if start:
        filters.append(ds.field("week_start") >= date_type.fromisoformat(start))
    if end:
        filters.append(ds.field("week_start") <= date_type.fromisoformat(end))

    filter_expr = reduce(lambda a, b: a & b, filters) if filters else None
    df = dt.to_pyarrow_dataset().to_table(filter=filter_expr).to_pandas()

    if df.empty:
        return df

    df["week_start"] = pd.to_datetime(df["week_start"])
    df["symbol"]     = df["symbol"].astype(str)
    return df.sort_values(["symbol", "week_start"]).reset_index(drop=True)


# ── One-time Parquet migration ────────────────────────────────────────────────

def migrate_parquet_to_delta() -> bool:
    """
    One-time migration: read all per-symbol Parquet files from data/symbols/
    and write them into the Delta candles table.

    - Existing Parquet files are PRESERVED and never deleted.
    - Safe to call multiple times — skips automatically if the Delta table
      already exists.

    Returns True if migration was performed, False if skipped.
    """
    from deltalake import write_deltalake

    if is_delta_table(CANDLES_DELTA_PATH):
        log.info("Delta candles table already exists — Parquet migration skipped.")
        return False

    parquet_files = sorted(config.SYMBOLS_DATA_DIR.glob("*.parquet"))
    if not parquet_files:
        log.info("No legacy Parquet files found in %s — nothing to migrate.",
                 config.SYMBOLS_DATA_DIR)
        return False

    log.info(
        "Migrating %d Parquet file(s) to Delta at %s ...",
        len(parquet_files), CANDLES_DELTA_PATH,
    )
    config.DELTA_DIR.mkdir(parents=True, exist_ok=True)

    dfs: list[pd.DataFrame] = []
    for f in parquet_files:
        try:
            dfs.append(pd.read_parquet(f))
        except Exception as exc:
            log.warning("  Could not read %s: %s", f.name, exc)

    if not dfs:
        log.error("No Parquet files could be read — migration aborted.")
        return False

    combined = pd.concat(dfs, ignore_index=True)
    combined = _normalise_candles_schema(combined)
    combined = combined.drop_duplicates(subset=["symbol", "date"])
    combined = combined.sort_values(["symbol", "date"]).reset_index(drop=True)

    write_deltalake(
        CANDLES_DELTA_PATH,
        combined,
        mode="overwrite",
        partition_by=["symbol"],
    )

    log.info(
        "Migration complete: %d rows from %d files written to Delta.",
        len(combined), len(parquet_files),
    )
    return True


# ── Internal helpers ──────────────────────────────────────────────────────────

def _normalise_candles_schema(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure consistent column types and order for the candles Delta table.
    """
    df = df.copy()
    df["date"]   = pd.to_datetime(df["date"]).dt.date
    df["symbol"] = df["symbol"].astype(str)

    for col in ("Open", "High", "Low", "Close"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype(float)
    if "Volume" in df.columns:
        df["Volume"] = pd.to_numeric(df["Volume"], errors="coerce").fillna(0).astype("int64")

    # Canonical column order
    ordered = [c for c in ["date", "Open", "High", "Low", "Close", "Volume", "symbol"]
               if c in df.columns]
    extra   = [c for c in df.columns if c not in ordered]
    return df[ordered + extra]

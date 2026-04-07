"""
delta_utils.py
==============
Delta Lake utilities using the `deltalake` Python package (delta-rs).

NO Java required — pure Python / Rust implementation.
Install with:  pip install deltalake

Delta table layout
------------------
    data/delta/
        daily/      OHLCV + SMA-20/33, EMA-20/33  (partitioned by symbol)
        weekly/     OHLCV + SMA-20/33, EMA-20/33  (partitioned by symbol)

Schema
------
    daily   : date       | symbol | Open | High | Low | Close | Volume
                         | sma_20 | sma_33 | ema_20 | ema_33

    weekly  : week_start | symbol | Open | High | Low | Close | Volume
                         | trading_days | sma_20 | sma_33 | ema_20 | ema_33

Indicator columns are NaN until technical_analysis.py runs.

Period conventions
------------------
    period="daily"   date column = "date"
    period="weekly"  date column = "week_start"
"""

import logging
from datetime import date as date_type
from functools import reduce

import pandas as pd

import config

log = logging.getLogger("nasdaq_candles")

# ── Backward-compat constant (points to new daily table) ─────────────────────
CANDLES_DELTA_PATH = str(config.DELTA_PATHS["daily"])


# ── Internal helpers ──────────────────────────────────────────────────────────

def _table_path(period: str) -> str:
    try:
        return str(config.DELTA_PATHS[period])
    except KeyError:
        raise ValueError(f"Unknown period '{period}'. Valid: {list(config.DELTA_PATHS)}")


def _date_col(period: str) -> str:
    """Primary date column name for the given period."""
    return "date" if period == "daily" else "week_start"


def _merge_predicate(period: str) -> str:
    col = _date_col(period)
    return f"t.symbol = s.symbol AND t.{col} = s.{col}"


# ── Table existence check ─────────────────────────────────────────────────────

def is_delta_table(path: str) -> bool:
    """Return True if a valid Delta table exists at *path*."""
    try:
        from deltalake import DeltaTable
        return DeltaTable.is_deltatable(path)
    except Exception:
        return False


# ── Candle write (insert-only) ────────────────────────────────────────────────

def upsert_candles(df: pd.DataFrame, period: str = "daily") -> None:
    """
    Insert new candle rows into the combined period table.

    Only inserts rows whose (symbol, date/week_start) key does not yet exist.
    Existing rows are left untouched — this preserves previously computed
    indicator values.

    Indicator columns (sma_20, sma_33, ema_20, ema_33) are set to NaN on
    insert; they are filled in when technical_analysis.py runs.
    """
    from deltalake import DeltaTable, write_deltalake

    if df is None or df.empty:
        return

    config.DELTA_DIR.mkdir(parents=True, exist_ok=True)
    path = _table_path(period)
    df   = _normalise_schema(df, period)

    if is_delta_table(path):
        (
            DeltaTable(path)
            .merge(
                source=df,
                predicate=_merge_predicate(period),
                source_alias="s",
                target_alias="t",
            )
            .when_not_matched_insert_all()   # insert new rows only
            .execute()
        )
    else:
        write_deltalake(path, df, mode="overwrite", partition_by=["symbol"])

    log.info("  Delta insert [%s]: %d rows / %d symbol(s)",
             period, len(df), df["symbol"].nunique())


# ── Indicator write (update existing rows) ───────────────────────────────────

def upsert_technicals(df: pd.DataFrame, period: str) -> None:
    """
    Write indicator values back into the combined period table.

    Updates existing rows (fills in sma_20/33, ema_20/33) and inserts any
    rows that are somehow missing (e.g. first-ever run with no prior candles).

    The source DataFrame must contain ALL columns (OHLCV + indicators) because
    Delta's update-all replaces the entire matched row.  Since we always read
    candles from the same table before computing, OHLCV values are identical
    and safe to write back.
    """
    from deltalake import DeltaTable, write_deltalake

    if df is None or df.empty:
        return

    config.DELTA_DIR.mkdir(parents=True, exist_ok=True)
    path = _table_path(period)
    df   = _normalise_schema(df, period)

    if is_delta_table(path):
        (
            DeltaTable(path)
            .merge(
                source=df,
                predicate=_merge_predicate(period),
                source_alias="s",
                target_alias="t",
            )
            .when_matched_update_all()       # overwrite row (updates indicators)
            .when_not_matched_insert_all()   # insert if row somehow missing
            .execute()
        )
    else:
        write_deltalake(path, df, mode="overwrite", partition_by=["symbol"])

    log.info("  Delta update [%s technicals]: %d rows / %d symbol(s)",
             period, len(df), df["symbol"].nunique())


# ── Read ──────────────────────────────────────────────────────────────────────

def read_candles(
    symbols: list[str] | None = None,
    start:   str | None = None,
    end:     str | None = None,
    period:  str = "daily",
) -> pd.DataFrame:
    """
    Read the combined period table into a pandas DataFrame.
    Returns OHLCV columns plus any indicator columns already computed.

    Parameters
    ----------
    symbols : list of ticker strings, or None for all
    start   : 'YYYY-MM-DD' lower bound on date/week_start (inclusive)
    end     : 'YYYY-MM-DD' upper bound on date/week_start (inclusive)
    period  : "daily" or "weekly"
    """
    import pyarrow.dataset as ds
    from deltalake import DeltaTable

    path = _table_path(period)
    if not is_delta_table(path):
        log.warning("Delta table (%s) does not exist at %s", period, path)
        return pd.DataFrame()

    dt      = DeltaTable(path)
    dcol    = _date_col(period)
    filters = []

    if symbols:
        filters.append(ds.field("symbol").isin([s.upper() for s in symbols]))
    if start:
        filters.append(ds.field(dcol) >= date_type.fromisoformat(start))
    if end:
        filters.append(ds.field(dcol) <= date_type.fromisoformat(end))

    fexpr = reduce(lambda a, b: a & b, filters) if filters else None
    df    = dt.to_pyarrow_dataset().to_table(filter=fexpr).to_pandas()

    if df.empty:
        return df

    df[dcol]     = pd.to_datetime(df[dcol])
    df["symbol"] = df["symbol"].astype(str)
    return df.sort_values(["symbol", dcol]).reset_index(drop=True)


# ── One-time migration from legacy tables ─────────────────────────────────────

def migrate_to_combined_tables() -> bool:
    """
    One-time migration: merge the legacy separate candle and technicals tables
    into the new combined daily/weekly tables.

    Legacy tables (read-only, never deleted):
        data/delta/candles/            -> daily OHLCV
        data/delta/technicals_daily/   -> daily indicators   (may not exist)
        data/delta/candles_weekly/     -> weekly OHLCV       (may not exist)
        data/delta/technicals_weekly/  -> weekly indicators  (may not exist)

    New combined tables:
        data/delta/daily/              -> daily OHLCV + indicators
        data/delta/weekly/             -> weekly OHLCV + indicators

    Safe to call multiple times — skips if both combined tables already exist.
    Returns True if migration ran, False if skipped.
    """
    from deltalake import write_deltalake

    daily_done  = is_delta_table(_table_path("daily"))
    weekly_done = is_delta_table(_table_path("weekly"))

    if daily_done and weekly_done:
        log.info("Combined tables already exist — migration skipped.")
        return False

    config.DELTA_DIR.mkdir(parents=True, exist_ok=True)
    migrated = False

    # ── Daily ─────────────────────────────────────────────────────────────────
    if not daily_done:
        legacy_candles = str(config.DELTA_DIR / "candles")
        legacy_tech    = str(config.DELTA_DIR / "technicals_daily")

        if not is_delta_table(legacy_candles):
            log.warning("Legacy daily candles table not found — skipping daily migration.")
        else:
            log.info("Migrating daily table ...")
            from deltalake import DeltaTable
            candles_df = DeltaTable(legacy_candles).to_pyarrow_dataset().to_table().to_pandas()
            candles_df["date"]   = pd.to_datetime(candles_df["date"]).dt.date
            candles_df["symbol"] = candles_df["symbol"].astype(str)

            if is_delta_table(legacy_tech):
                log.info("  Joining with legacy daily technicals...")
                tech_df = DeltaTable(legacy_tech).to_pyarrow_dataset().to_table().to_pandas()
                tech_df["date"]   = pd.to_datetime(tech_df["date"]).dt.date
                tech_df["symbol"] = tech_df["symbol"].astype(str)
                candles_df = candles_df.merge(
                    tech_df[["date", "symbol", "sma_20", "sma_33", "ema_20", "ema_33"]],
                    on=["date", "symbol"],
                    how="left",
                )
            else:
                log.info("  No legacy daily technicals found — indicator columns will be NaN.")

            combined = _normalise_schema(candles_df, "daily")
            combined = combined.drop_duplicates(subset=["symbol", "date"])
            write_deltalake(_table_path("daily"), combined,
                            mode="overwrite", partition_by=["symbol"])
            log.info("  Daily migration complete: %d rows, %d symbols.",
                     len(combined), combined["symbol"].nunique())
            migrated = True

    # ── Weekly ────────────────────────────────────────────────────────────────
    if not weekly_done:
        legacy_weekly      = str(config.DELTA_DIR / "candles_weekly")
        legacy_weekly_tech = str(config.DELTA_DIR / "technicals_weekly")

        if not is_delta_table(legacy_weekly):
            log.warning("Legacy weekly candles table not found — skipping weekly migration.")
        else:
            log.info("Migrating weekly table ...")
            from deltalake import DeltaTable
            weekly_df = DeltaTable(legacy_weekly).to_pyarrow_dataset().to_table().to_pandas()
            weekly_df["week_start"] = pd.to_datetime(weekly_df["week_start"]).dt.date
            weekly_df["symbol"]     = weekly_df["symbol"].astype(str)

            if is_delta_table(legacy_weekly_tech):
                log.info("  Joining with legacy weekly technicals...")
                tech_df = DeltaTable(legacy_weekly_tech).to_pyarrow_dataset().to_table().to_pandas()
                tech_df["week_start"] = pd.to_datetime(tech_df["week_start"]).dt.date
                tech_df["symbol"]     = tech_df["symbol"].astype(str)
                weekly_df = weekly_df.merge(
                    tech_df[["week_start", "symbol", "sma_20", "sma_33", "ema_20", "ema_33"]],
                    on=["week_start", "symbol"],
                    how="left",
                )
            else:
                log.info("  No legacy weekly technicals found — indicator columns will be NaN.")

            combined = _normalise_schema(weekly_df, "weekly")
            combined = combined.drop_duplicates(subset=["symbol", "week_start"])
            write_deltalake(_table_path("weekly"), combined,
                            mode="overwrite", partition_by=["symbol"])
            log.info("  Weekly migration complete: %d rows, %d symbols.",
                     len(combined), combined["symbol"].nunique())
            migrated = True

    return migrated


# ── One-time legacy Parquet migration (daily candles only) ────────────────────

def migrate_parquet_to_delta() -> bool:
    """
    One-time migration: read per-symbol Parquet files from data/symbols/ and
    write them into the combined daily table.
    Skips automatically if the daily table already exists.
    """
    from deltalake import write_deltalake

    path = _table_path("daily")
    if is_delta_table(path):
        log.info("Daily Delta table already exists — Parquet migration skipped.")
        return False

    parquet_files = sorted(config.SYMBOLS_DATA_DIR.glob("*.parquet"))
    if not parquet_files:
        log.info("No legacy Parquet files found — nothing to migrate.")
        return False

    log.info("Migrating %d Parquet file(s) to Delta at %s ...",
             len(parquet_files), path)
    config.DELTA_DIR.mkdir(parents=True, exist_ok=True)

    dfs = []
    for f in parquet_files:
        try:
            dfs.append(pd.read_parquet(f))
        except Exception as exc:
            log.warning("  Could not read %s: %s", f.name, exc)

    if not dfs:
        log.error("No Parquet files could be read — migration aborted.")
        return False

    combined = pd.concat(dfs, ignore_index=True)
    combined = _normalise_schema(combined, "daily")
    combined = combined.drop_duplicates(subset=["symbol", "date"])
    combined = combined.sort_values(["symbol", "date"]).reset_index(drop=True)
    write_deltalake(path, combined, mode="overwrite", partition_by=["symbol"])

    log.info("Parquet migration complete: %d rows from %d files.",
             len(combined), len(parquet_files))
    return True


# ── Schema normaliser ─────────────────────────────────────────────────────────

# Canonical column order for each period
_DAILY_COLS  = ["date",       "symbol", "Open", "High", "Low", "Close",
                "Volume", "sma_20", "sma_33", "ema_20", "ema_33"]
_WEEKLY_COLS = ["week_start", "symbol", "Open", "High", "Low", "Close",
                "Volume", "trading_days", "sma_20", "sma_33", "ema_20", "ema_33"]


def _normalise_schema(df: pd.DataFrame, period: str) -> pd.DataFrame:
    """
    Ensure consistent column types and order for a combined period table.
    Adds any missing indicator columns as NaN so the schema is always complete.
    """
    df   = df.copy()
    dcol = _date_col(period)
    canonical = _DAILY_COLS if period == "daily" else _WEEKLY_COLS

    df[dcol]     = pd.to_datetime(df[dcol]).dt.date
    df["symbol"] = df["symbol"].astype(str)

    for col in ("Open", "High", "Low", "Close"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype(float)

    if "Volume" in df.columns:
        df["Volume"] = (
            pd.to_numeric(df["Volume"], errors="coerce").fillna(0).astype("int64")
        )
    if "trading_days" in df.columns:
        df["trading_days"] = df["trading_days"].astype("int32")

    # Always include indicator columns — NaN if not yet computed
    for col in ("sma_20", "sma_33", "ema_20", "ema_33"):
        if col not in df.columns:
            df[col] = float("nan")
        else:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype(float)

    # Canonical column order (extras appended at end)
    ordered = [c for c in canonical if c in df.columns]
    extra   = [c for c in df.columns if c not in ordered]
    return df[ordered + extra]

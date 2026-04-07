"""
technical_analysis.py
=====================
Calculate technical indicators from the Delta candles tables.

No Java / PySpark required — pure pandas.

Supported periods
-----------------
    daily   Reads data/delta/candles/         → writes data/delta/technicals_daily/
    weekly  Reads data/delta/candles_weekly/  → writes data/delta/technicals_weekly/

Indicators calculated (applied to the period's Close price)
-----------------------------------------------------------
    sma_20   20-period Simple Moving Average
    sma_33   33-period Simple Moving Average
    ema_20   20-period Exponential Moving Average  (standard recursive, adjust=False)
    ema_33   33-period Exponential Moving Average

    All indicators require min_periods equal to their window length; rows with
    insufficient history receive NaN (still written to Delta for completeness).

Incremental logic
-----------------
    A per-period technicals state file tracks the candles-state date at the
    time technicals were last computed for each symbol.  A symbol is SKIPPED
    if its technicals state matches its current candles state — meaning no
    new candle data has arrived since the last computation.

    When a symbol IS recomputed, the FULL history is read and all rows are
    upserted.  This ensures rolling windows are always correct even when
    historical data is backfilled.

Usage
-----
    python technical_analysis.py                          # all symbols, daily
    python technical_analysis.py --period weekly          # all symbols, weekly
    python technical_analysis.py --symbol AAPL MSFT       # specific symbols
    python technical_analysis.py --force                  # ignore state, recompute all
"""

import argparse
import logging
import sys

import pandas as pd

import config
import delta_utils
from fetch_nasdaq_candles import load_symbol_state, save_symbol_state, setup_logging

log = setup_logging()


# ── Core indicator engine ─────────────────────────────────────────────────────

def _compute_indicators(df: pd.DataFrame, period: str) -> pd.DataFrame:
    """
    Add sma_20, sma_33, ema_20, ema_33 columns to a single-symbol DataFrame
    that is already sorted by its date column.

    Parameters
    ----------
    df     : single-symbol candles DataFrame (must have 'Close' column)
    period : "daily" or "weekly" (determines the date column name)
    """
    close = df["Close"]

    df["sma_20"] = close.rolling(window=20, min_periods=20).mean().round(6)
    df["sma_33"] = close.rolling(window=33, min_periods=33).mean().round(6)
    df["ema_20"] = close.ewm(span=20, adjust=False, min_periods=20).mean().round(6)
    df["ema_33"] = close.ewm(span=33, adjust=False, min_periods=33).mean().round(6)

    return df


# ── Main entry point ──────────────────────────────────────────────────────────

def calculate_indicators(
    period:  str = "daily",
    symbols: list[str] | None = None,
    force:   bool = False,
) -> pd.DataFrame:
    """
    Compute SMA-20, SMA-33, EMA-20, EMA-33 for every symbol in the candles
    Delta table for *period* and upsert results into the technicals table.

    Parameters
    ----------
    period  : "daily" or "weekly"
    symbols : restrict to these tickers (None = all symbols in candles table)
    force   : recompute even for symbols whose state is already current

    Returns the combined technicals DataFrame (all processed symbols).
    """
    table_path = delta_utils._table_path(period)
    if not delta_utils.is_delta_table(table_path):
        log.error(
            "[%s technicals] Delta table not found at %s. "
            "Run fetch_nasdaq_candles.py --period %s first.",
            period, table_path, period,
        )
        return pd.DataFrame()

    dcol           = delta_utils._date_col(period)
    tech_state_key = f"technicals_{period}"

    # Load state files
    candles_state = load_symbol_state(period)
    tech_state    = load_symbol_state(tech_state_key)

    syms_upper = [s.upper() for s in symbols] if symbols else None

    # ── Determine which symbols need recomputation ────────────────────────────
    if syms_upper:
        candidates = syms_upper
    else:
        # All symbols in the candles table
        candidates = list(candles_state.keys())

    if not candidates:
        log.warning("[%s technicals] No symbols in candles state — nothing to do.", period)
        return pd.DataFrame()

    pending: list[str] = []
    skipped = 0
    for sym in candidates:
        candle_last = candles_state.get(sym)
        if candle_last is None:
            log.debug("  [%s] %s: not in candles state — skipping", period, sym)
            skipped += 1
            continue
        if not force and tech_state.get(sym) == candle_last:
            skipped += 1   # technicals already reflect the latest candle
            continue
        pending.append(sym)

    log.info(
        "[%s technicals] %d symbol(s) to compute, %d already current",
        period, len(pending), skipped,
    )

    if not pending:
        return pd.DataFrame()

    # ── Read full candle history for pending symbols ──────────────────────────
    log.info("[%s technicals] Reading candles from Delta...", period)
    df = delta_utils.read_candles(symbols=pending, period=period)

    if df.empty:
        log.error("[%s technicals] No candle data returned.", period)
        return pd.DataFrame()

    df[dcol]     = pd.to_datetime(df[dcol])
    df["symbol"] = df["symbol"].astype(str)

    # ── Compute indicators per symbol ─────────────────────────────────────────
    results: list[pd.DataFrame] = []

    for symbol, grp in df.groupby("symbol"):
        grp = grp.sort_values(dcol).reset_index(drop=True)
        grp = _compute_indicators(grp, period)

        # Keep only the date key + symbol + indicator columns
        keep = [dcol, "symbol", "sma_20", "sma_33", "ema_20", "ema_33"]
        results.append(grp[[c for c in keep if c in grp.columns]])

    if not results:
        return pd.DataFrame()

    result_df = pd.concat(results, ignore_index=True)

    # ── Upsert to Delta ───────────────────────────────────────────────────────
    log.info("[%s technicals] Upserting %d rows for %d symbol(s) to Delta...",
             period, len(result_df), result_df["symbol"].nunique())
    delta_utils.upsert_technicals(result_df, period=period)

    # ── Update technicals state ───────────────────────────────────────────────
    for sym in pending:
        if sym in candles_state:
            tech_state[sym] = candles_state[sym]
    save_symbol_state(tech_state, tech_state_key)

    non_null_sma33 = result_df["sma_33"].notna().sum()
    log.info(
        "[%s technicals] Done: %d rows, %d symbol(s), %d rows with valid SMA-33",
        period, len(result_df), result_df["symbol"].nunique(), non_null_sma33,
    )
    return result_df


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Calculate SMA-20/33 and EMA-20/33 from the Delta candles tables.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples
--------
  python technical_analysis.py                           # daily, all symbols
  python technical_analysis.py --period weekly           # weekly, all symbols
  python technical_analysis.py --symbol AAPL MSFT TSLA  # specific symbols
  python technical_analysis.py --period weekly --force   # recompute everything
        """,
    )
    parser.add_argument(
        "--period",
        choices=["daily", "weekly"],
        default="daily",
        help="Period to compute indicators for (default: daily).",
    )
    parser.add_argument(
        "--symbol", nargs="+", metavar="TICKER",
        help="Compute only for these symbols (default: all).",
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Recompute all symbols regardless of state.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    df = calculate_indicators(
        period=args.period,
        symbols=args.symbol,
        force=args.force,
    )
    sys.exit(0 if not df.empty else 1)


if __name__ == "__main__":
    main()

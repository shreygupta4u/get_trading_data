"""
technical_analysis.py
=====================
Calculate technical indicators from the Delta candles table using pandas.

No Java / PySpark required.

Indicators implemented
----------------------
Weekly SMA-33
    Resamples daily OHLCV to ISO weekly candles (Monday = week start),
    then computes a 33-week rolling Simple Moving Average of the weekly
    Close price.  Requires at least 33 weeks of history; the sma_33w
    column is NaN for earlier weeks.

Output Delta table
------------------
    data/delta/weekly_technicals/   (partitioned by symbol)

Schema
------
    symbol        string
    week_start    date      Monday of each ISO week
    Open          float64   First trading day's open price that week
    High          float64   Highest price of the week
    Low           float64   Lowest price of the week
    Close         float64   Last trading day's close price that week
    Volume        int64     Sum of daily volumes for the week
    trading_days  int32     Number of trading days in that week
    sma_33w       float64   33-week SMA of weekly Close (NaN until 33 weeks)

Usage
-----
    python technical_analysis.py                     # all symbols
    python technical_analysis.py --symbol AAPL MSFT  # specific symbols
    python technical_analysis.py --export             # also export to Excel
"""

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd

import config
import delta_utils

log = logging.getLogger("nasdaq_candles")


# ── Weekly SMA-33 ─────────────────────────────────────────────────────────────

def calculate_weekly_sma33(symbols: list[str] | None = None) -> pd.DataFrame:
    """
    Read daily candles from the Delta table, resample to ISO weekly candles,
    compute a 33-week SMA of Close, and upsert into the weekly_technicals
    Delta table.

    Returns the result as a pandas DataFrame (all symbols combined).
    """
    if not delta_utils.is_delta_table(delta_utils.CANDLES_DELTA_PATH):
        log.error(
            "Candles Delta table not found at %s. "
            "Run fetch_nasdaq_candles.py first.",
            delta_utils.CANDLES_DELTA_PATH,
        )
        return pd.DataFrame()

    syms_upper = [s.upper() for s in symbols] if symbols else None
    log.info("Loading daily candles from Delta table...")
    df = delta_utils.read_candles(symbols=syms_upper)

    if df.empty:
        log.error("No candle data found — cannot calculate technicals.")
        return pd.DataFrame()

    log.info(
        "Loaded %d rows across %d symbol(s). Calculating weekly SMA-33...",
        len(df), df["symbol"].nunique(),
    )

    df["date"] = pd.to_datetime(df["date"])

    results: list[pd.DataFrame] = []

    for symbol, grp in df.groupby("symbol"):
        grp = grp.sort_values("date").reset_index(drop=True)
        grp = grp.set_index("date")

        # ── ISO week start (Monday) for each trading day ──────────────────────
        # dayofweek: Mon=0 … Sun=6  →  subtract to snap back to Monday
        grp["week_start"] = (
            grp.index - pd.to_timedelta(grp.index.dayofweek, unit="D")
        ).date

        # ── Resample to weekly OHLCV ──────────────────────────────────────────
        weekly = (
            grp.groupby("week_start")
            .agg(
                Open        =("Open",   "first"),
                High        =("High",   "max"),
                Low         =("Low",    "min"),
                Close       =("Close",  "last"),
                Volume      =("Volume", "sum"),
                trading_days=("Close",  "count"),
            )
        )
        weekly.index = pd.to_datetime(weekly.index)   # keep as datetime for rolling

        # ── 33-week rolling SMA of weekly Close ───────────────────────────────
        # min_periods=33: row gets NaN until 33 full weeks are available
        weekly["sma_33w"] = (
            weekly["Close"]
            .rolling(window=33, min_periods=33)
            .mean()
            .round(6)
        )

        weekly["symbol"]     = symbol
        weekly.index.name    = "week_start"
        weekly               = weekly.reset_index()

        results.append(weekly)

    if not results:
        log.warning("No weekly data produced.")
        return pd.DataFrame()

    result_df = pd.concat(results, ignore_index=True)
    result_df["trading_days"] = result_df["trading_days"].astype("int32")

    # ── Write to Delta ────────────────────────────────────────────────────────
    log.info("Writing weekly_technicals to Delta table at %s ...",
             delta_utils.TECHNICALS_DELTA_PATH)
    delta_utils.upsert_weekly_technicals(result_df)

    sym_count  = result_df["symbol"].nunique()
    week_count = len(result_df)
    non_null   = result_df["sma_33w"].notna().sum()

    log.info(
        "Weekly SMA-33 complete: %d weeks across %d symbol(s)  "
        "(%d rows have a valid SMA-33w value)",
        week_count, sym_count, non_null,
    )
    return result_df


# ── Optional Excel export ─────────────────────────────────────────────────────

def export_technicals_to_excel(df: pd.DataFrame, output_path: Path) -> None:
    """Export the weekly_technicals DataFrame to an Excel workbook."""
    print(f"Exporting {len(df):,} weekly rows across {df['symbol'].nunique()} symbols...")
    with pd.ExcelWriter(output_path, engine="openpyxl",
                        datetime_format="YYYY-MM-DD") as writer:
        df.to_excel(writer, index=False, sheet_name="Weekly Technicals")

        for symbol, group in df.groupby("symbol"):
            sheet = str(symbol)[:31]
            group.reset_index(drop=True).to_excel(
                writer, index=False, sheet_name=sheet
            )

    size_kb = output_path.stat().st_size / 1024
    print(f"Saved {output_path}  ({size_kb:.0f} KB)")


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Calculate Weekly SMA-33 from the Delta candles table.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples
--------
  python technical_analysis.py                          # all symbols
  python technical_analysis.py --symbol AAPL MSFT TSLA  # specific symbols
  python technical_analysis.py --export                  # also save to Excel
  python technical_analysis.py --symbol AAPL --export --out aapl_weekly.xlsx
        """,
    )
    parser.add_argument(
        "--symbol", nargs="+", metavar="TICKER",
        help="Calculate only for these symbols (default: all).",
    )
    parser.add_argument(
        "--export", action="store_true",
        help="Also export the result to an Excel file.",
    )
    parser.add_argument(
        "--out", default="weekly_technicals.xlsx", metavar="FILE",
        help="Excel output file name (used with --export, default: weekly_technicals.xlsx).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    df = calculate_weekly_sma33(symbols=args.symbol)

    if df.empty:
        log.error("No data returned — cannot export.")
        sys.exit(1)

    if args.export:
        export_technicals_to_excel(df, Path(args.out))


if __name__ == "__main__":
    main()

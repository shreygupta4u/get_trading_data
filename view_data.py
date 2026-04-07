"""
view_data.py
============
Reads candle data and exports to an Excel workbook.

Data source priority
--------------------
1. Delta candles table  (data/delta/candles/)       <- primary
2. Legacy per-symbol Parquet files (data/symbols/)  <- fallback

Usage
-----
    python view_data.py                              # export everything
    python view_data.py --symbol AAPL MSFT           # filter symbols
    python view_data.py --start 2024-01-01           # from date
    python view_data.py --start 2024-01-01 --end 2024-12-31
    python view_data.py --out my_report.xlsx         # custom filename
    python view_data.py --include-technicals         # add Weekly SMA-33 sheet

Output
------
    Sheet "All Data"          every OHLCV row, sorted by symbol then date
    Sheet per symbol          e.g. "AAPL", "MSFT"
    Sheet "Weekly SMA-33"     (optional, with --include-technicals)
"""

import argparse
import sys
from datetime import date
from pathlib import Path

import pandas as pd

import config
import delta_utils


def load_candle_data(
    symbols: list[str] | None = None,
    start: date | None = None,
    end: date | None = None,
) -> pd.DataFrame:
    """
    Load OHLCV candle data.  Tries the Delta table first; falls back to the
    legacy per-symbol Parquet files if the Delta table is not available.
    """
    # ── Try Delta table first ─────────────────────────────────────────────────
    if delta_utils.is_delta_table(delta_utils.CANDLES_DELTA_PATH):
        print("Reading from Delta candles table...")
        start_str = str(start) if start else None
        end_str   = str(end)   if end   else None
        df = delta_utils.read_candles(symbols=symbols, start=start_str, end=end_str)
        if not df.empty:
            df["date"]   = pd.to_datetime(df["date"])
            df["symbol"] = df["symbol"].astype(str)
            return df.sort_values(["symbol", "date"]).reset_index(drop=True)

    # ── Fallback: legacy per-symbol Parquet files ─────────────────────────────
    if symbols:
        parquet_files = sorted(
            config.SYMBOLS_DATA_DIR / f"{s.upper()}.parquet"
            for s in symbols
            if (config.SYMBOLS_DATA_DIR / f"{s.upper()}.parquet").exists()
        )
    else:
        parquet_files = sorted(config.SYMBOLS_DATA_DIR.glob("*.parquet"))

    if not parquet_files:
        print("No data found in Delta table or Parquet files.")
        sys.exit(1)

    print(f"Reading {len(parquet_files)} legacy Parquet file(s)...")
    dfs = []
    for f in parquet_files:
        try:
            dfs.append(pd.read_parquet(f))
        except Exception as e:
            print(f"  Warning: could not read {f}: {e}")

    if not dfs:
        print("No data could be read.")
        sys.exit(1)

    df = pd.concat(dfs, ignore_index=True)
    df["date"]   = pd.to_datetime(df["date"]).dt.date
    df["symbol"] = df["symbol"].astype(str)

    if symbols:
        df = df[df["symbol"].isin([s.upper() for s in symbols])]
    if start:
        df = df[df["date"] >= start]
    if end:
        df = df[df["date"] <= end]

    df = df.sort_values(["symbol", "date"]).reset_index(drop=True)
    df["date"] = pd.to_datetime(df["date"])
    return df


def load_technicals(
    symbols: list[str] | None = None,
    start: date | None = None,
    end: date | None = None,
) -> pd.DataFrame:
    """Load weekly technicals (SMA-33w) from the Delta technicals table."""
    if not delta_utils.is_delta_table(delta_utils.TECHNICALS_DELTA_PATH):
        print("  Weekly technicals Delta table not found — run technical_analysis.py first.")
        return pd.DataFrame()

    start_str = str(start) if start else None
    end_str   = str(end)   if end   else None
    df = delta_utils.read_weekly_technicals(symbols=symbols, start=start_str, end=end_str)
    return df


def export_to_excel(
    df: pd.DataFrame,
    output_path: Path,
    technicals_df: pd.DataFrame | None = None,
) -> None:
    print(f"Exporting {len(df):,} candle rows across {df['symbol'].nunique()} symbols...")
    with pd.ExcelWriter(output_path, engine="openpyxl", datetime_format="YYYY-MM-DD") as writer:
        # All OHLCV data
        df.to_excel(writer, index=False, sheet_name="All Data")
        _autofit(writer.sheets["All Data"], df)

        # One sheet per symbol (OHLCV)
        for symbol, group in df.groupby("symbol"):
            sheet_name = str(symbol)[:31]
            group.reset_index(drop=True).to_excel(writer, index=False, sheet_name=sheet_name)
            _autofit(writer.sheets[sheet_name], group)

        # Weekly SMA-33 sheet (optional)
        if technicals_df is not None and not technicals_df.empty:
            technicals_df.to_excel(writer, index=False, sheet_name="Weekly SMA-33")
            _autofit(writer.sheets["Weekly SMA-33"], technicals_df)
            print(f"  Added 'Weekly SMA-33' sheet ({len(technicals_df):,} rows)")

    size_kb = output_path.stat().st_size / 1024
    print(f"Saved {output_path}  ({size_kb:.0f} KB)")


def _autofit(ws, df: pd.DataFrame) -> None:
    """Set column widths to fit content."""
    for i, col in enumerate(df.columns, start=1):
        max_len = max(len(str(col)), df[col].astype(str).str.len().max())
        ws.column_dimensions[ws.cell(1, i).column_letter].width = min(max_len + 2, 30)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export candle data (Delta or Parquet) to Excel."
    )
    parser.add_argument("--symbol", nargs="+", metavar="TICKER",
                        help="Filter to specific symbols")
    parser.add_argument("--start", metavar="YYYY-MM-DD",
                        help="Filter from this date (inclusive)")
    parser.add_argument("--end", metavar="YYYY-MM-DD",
                        help="Filter to this date (inclusive)")
    parser.add_argument("--out", default="trading_data.xlsx", metavar="FILE",
                        help="Output Excel file (default: trading_data.xlsx)")
    parser.add_argument("--include-technicals", action="store_true",
                        help="Add a 'Weekly SMA-33' sheet from the technicals Delta table")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    start = date.fromisoformat(args.start) if args.start else None
    end   = date.fromisoformat(args.end)   if args.end   else None

    df = load_candle_data(symbols=args.symbol, start=start, end=end)
    if df.empty:
        print("No data matched the filters.")
        sys.exit(1)

    tech_df = None
    if args.include_technicals:
        print("Loading weekly technicals...")
        tech_df = load_technicals(symbols=args.symbol, start=start, end=end)

    output_path = Path(args.out)
    export_to_excel(df, output_path, technicals_df=tech_df)


if __name__ == "__main__":
    main()

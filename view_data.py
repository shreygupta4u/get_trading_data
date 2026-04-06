"""
view_data.py
============
Reads all Parquet files under data/ and exports them to an Excel workbook.

Usage
-----
    python view_data.py                          # export everything → trading_data.xlsx
    python view_data.py --symbol AAPL MSFT       # only those symbols
    python view_data.py --start 2024-01-01       # from a start date
    python view_data.py --start 2024-01-01 --end 2024-12-31
    python view_data.py --out my_report.xlsx     # custom output file

Output
------
    Sheet "All Data"  — every row, sorted by symbol then date
    One sheet per symbol (e.g. "AAPL", "MSFT", …)
"""

import argparse
import sys
from datetime import date
from pathlib import Path

import pandas as pd

import config


def load_parquet_data(
    symbols: list[str] | None = None,
    start: date | None = None,
    end: date | None = None,
) -> pd.DataFrame:
    # Each symbol has its own parquet file under data/symbols/
    if symbols:
        parquet_files = sorted(
            config.SYMBOLS_DATA_DIR / f"{s.upper()}.parquet"
            for s in symbols
            if (config.SYMBOLS_DATA_DIR / f"{s.upper()}.parquet").exists()
        )
    else:
        parquet_files = sorted(config.SYMBOLS_DATA_DIR.glob("*.parquet"))

    if not parquet_files:
        print("No parquet files found in", config.SYMBOLS_DATA_DIR)
        sys.exit(1)

    print(f"Reading {len(parquet_files)} symbol file(s)...")
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
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["symbol"] = df["symbol"].astype(str)

    # Symbol filter already applied at file level above; normalise just in case
    if symbols:
        symbols_upper = [s.upper() for s in symbols]
        df = df[df["symbol"].isin(symbols_upper)]
    if start:
        df = df[df["date"] >= start]
    if end:
        df = df[df["date"] <= end]

    df = df.sort_values(["symbol", "date"]).reset_index(drop=True)
    # Convert date back to datetime for Excel compatibility
    df["date"] = pd.to_datetime(df["date"])
    return df


def export_to_excel(df: pd.DataFrame, output_path: Path) -> None:
    print(f"Exporting {len(df):,} rows across {df['symbol'].nunique()} symbols...")
    with pd.ExcelWriter(output_path, engine="openpyxl", datetime_format="YYYY-MM-DD") as writer:
        # All Data sheet
        df.to_excel(writer, index=False, sheet_name="All Data")
        _autofit(writer.sheets["All Data"], df)

        # One sheet per symbol
        for symbol, group in df.groupby("symbol"):
            sheet_name = str(symbol)[:31]  # Excel sheet name limit
            group = group.reset_index(drop=True)
            group.to_excel(writer, index=False, sheet_name=sheet_name)
            _autofit(writer.sheets[sheet_name], group)

    size_kb = output_path.stat().st_size / 1024
    print(f"Saved {output_path}  ({size_kb:.0f} KB)")


def _autofit(ws, df: pd.DataFrame) -> None:
    """Set column widths to fit content."""
    for i, col in enumerate(df.columns, start=1):
        max_len = max(len(str(col)), df[col].astype(str).str.len().max())
        ws.column_dimensions[ws.cell(1, i).column_letter].width = min(max_len + 2, 30)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export trading Parquet data to Excel.")
    parser.add_argument("--symbol", nargs="+", metavar="TICKER", help="Filter to specific symbols")
    parser.add_argument("--start", metavar="YYYY-MM-DD", help="Filter from this date (inclusive)")
    parser.add_argument("--end", metavar="YYYY-MM-DD", help="Filter to this date (inclusive)")
    parser.add_argument("--out", default="trading_data.xlsx", metavar="FILE", help="Output Excel file (default: trading_data.xlsx)")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    start = date.fromisoformat(args.start) if args.start else None
    end = date.fromisoformat(args.end) if args.end else None

    df = load_parquet_data(symbols=args.symbol, start=start, end=end)
    if df.empty:
        print("No data matched the filters.")
        sys.exit(1)

    output_path = Path(args.out)
    export_to_excel(df, output_path)


if __name__ == "__main__":
    main()

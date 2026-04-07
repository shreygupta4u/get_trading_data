"""
query.py
========
Interactive SQL shell for querying the Delta tables using DuckDB.
No Java, no Spark — runs entirely locally.

Usage
-----
    python query.py               # opens interactive SQL shell
    python query.py --sql "SELECT ..."   # run a single query and print result

Table names available in every query
-------------------------------------
    daily    daily candles + indicators  (date, symbol, OHLCV, sma_20/33, ema_20/33)
    weekly   weekly candles + indicators (week_start, symbol, OHLCV, sma_20/33, ema_20/33)

Example SQL
-----------
    -- Latest 10 rows for AAPL
    SELECT * FROM daily WHERE symbol = 'AAPL' ORDER BY date DESC LIMIT 10;

    -- All symbols with SMA-20 > SMA-33 (golden cross) as of last trading day
    SELECT symbol, date, Close, sma_20, sma_33
    FROM daily
    WHERE date = (SELECT MAX(date) FROM daily)
      AND sma_20 > sma_33
    ORDER BY symbol;

    -- Weekly summary for MAG7 since 2024
    SELECT symbol, week_start, Close, sma_20, sma_33
    FROM weekly
    WHERE symbol IN ('AAPL','MSFT','GOOGL','AMZN','META','NVDA','TSLA')
      AND week_start >= '2024-01-01'
    ORDER BY symbol, week_start;

    -- Row counts and date ranges per symbol
    SELECT symbol, COUNT(*) rows, MIN(date) first_date, MAX(date) last_date
    FROM daily
    GROUP BY symbol
    ORDER BY rows DESC
    LIMIT 20;

    -- Average volume by year for AAPL
    SELECT YEAR(date) yr, ROUND(AVG(Volume),0) avg_vol
    FROM daily
    WHERE symbol = 'AAPL'
    GROUP BY yr ORDER BY yr;
"""

import argparse
import sys

import duckdb
import pandas as pd

import config

pd.set_option("display.max_columns", 20)
pd.set_option("display.width", 200)
pd.set_option("display.float_format", "{:.4f}".format)


def _build_connection() -> duckdb.DuckDBPyConnection:
    """Open DuckDB, install Delta extension, register both tables as views."""
    con = duckdb.connect()
    con.execute("INSTALL delta; LOAD delta;")

    for period, path in config.DELTA_PATHS.items():
        path_str = str(path).replace("\\", "/")
        if path.exists():
            con.execute(
                f"CREATE OR REPLACE VIEW {period} AS "
                f"SELECT * FROM delta_scan('{path_str}')"
            )
        else:
            print(f"  Warning: {period} table not found at {path_str} — view not created.")

    return con


def run_query(sql: str, con: duckdb.DuckDBPyConnection) -> None:
    """Execute *sql* and print the result."""
    try:
        result = con.execute(sql).df()
        if result.empty:
            print("(no rows returned)")
        else:
            print(result.to_string(index=False))
            print(f"\n({len(result):,} row(s))")
    except Exception as exc:
        print(f"Error: {exc}")


def interactive_shell(con: duckdb.DuckDBPyConnection) -> None:
    """REPL: read SQL from stdin, execute, print results."""
    print("=" * 60)
    print("  DuckDB SQL shell  —  Delta tables loaded")
    print("  Tables: daily, weekly")
    print("  Type  \\help  for example queries")
    print("  Type  \\q  or Ctrl+C to exit")
    print("=" * 60)
    print()

    buffer = []
    while True:
        prompt = "sql> " if not buffer else "...> "
        try:
            line = input(prompt).strip()
        except (KeyboardInterrupt, EOFError):
            print("\nBye.")
            break

        if line in ("\\q", "\\quit", "exit", "quit"):
            break

        if line == "\\help":
            print(__doc__)
            continue

        if line == "\\tables":
            run_query("SHOW TABLES", con)
            continue

        if not line:
            continue

        buffer.append(line)
        joined = " ".join(buffer)

        # Execute when the statement ends with ;
        if joined.rstrip().endswith(";"):
            run_query(joined, con)
            buffer = []
            print()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="SQL shell for NASDAQ Delta tables via DuckDB.",
    )
    parser.add_argument(
        "--sql", metavar="QUERY",
        help='Run a single SQL statement and exit (wrap in double quotes).',
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    print("Loading Delta tables into DuckDB...")
    con = _build_connection()
    print("Ready.\n")

    if args.sql:
        run_query(args.sql, con)
    else:
        interactive_shell(con)


if __name__ == "__main__":
    main()

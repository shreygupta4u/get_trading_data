"""
fetch_nasdaq_candles.py
=======================
Downloads the daily OHLCV candle for every NASDAQ-listed equity and stores
the result as a date-partitioned Parquet file.

Usage
-----
    python fetch_nasdaq_candles.py                 # fetch today
    python fetch_nasdaq_candles.py --date 2024-12-31  # fetch a specific date
    python fetch_nasdaq_candles.py --date 2024-12-01 --date 2024-12-31  # range
    python fetch_nasdaq_candles.py --force         # re-fetch even if file exists

Output layout
-------------
    data/
    └── year=YYYY/
        └── month=MM/
            └── YYYYMMDD.parquet
"""

import argparse
import logging
import sys
import time
from datetime import date, datetime, timedelta
from logging.handlers import RotatingFileHandler
from pathlib import Path

import pandas as pd
import requests
import yfinance as yf

import config

# ── Logging setup ─────────────────────────────────────────────────────────────

def setup_logging() -> logging.Logger:
    config.LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = config.LOG_DIR / "fetch.log"

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=config.LOG_ROTATION_BYTES,
        backupCount=config.LOG_BACKUP_COUNT,
    )
    file_handler.setFormatter(fmt)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(fmt)

    logger = logging.getLogger("nasdaq_candles")
    logger.setLevel(getattr(logging, config.LOG_LEVEL))
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger


log = setup_logging()


# ── Symbol fetching ───────────────────────────────────────────────────────────

def fetch_nasdaq_symbols() -> list[str]:
    """
    Download the current NASDAQ listed-symbol directory.
    Returns a list of ticker strings, e.g. ['AAPL', 'MSFT', ...].

    The source file is pipe-delimited with a trailing summary line
    that starts with 'File Creation Time' — we drop that.
    """
    log.info("Fetching NASDAQ symbol list from %s", config.NASDAQ_SYMBOLS_URL)
    try:
        resp = requests.get(config.NASDAQ_SYMBOLS_URL, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as exc:
        log.error("Could not fetch symbol list: %s", exc)
        raise

    lines = resp.text.splitlines()
    # First line is the header; last line is a metadata footer
    data_lines = [l for l in lines[1:] if not l.startswith("File Creation Time")]

    symbols = []
    for line in data_lines:
        parts = line.split("|")
        if len(parts) < 2:
            continue
        symbol = parts[0].strip()
        # Skip test symbols, warrants, units, rights (contain $, ^, +, .)
        if any(c in symbol for c in ("$", "^", "+", ".")):
            continue
        if symbol:
            symbols.append(symbol)

    log.info("Found %d NASDAQ symbols", len(symbols))
    return symbols


# ── Data fetching ─────────────────────────────────────────────────────────────

def _fetch_batch(symbols: list[str], target_date: date) -> pd.DataFrame:
    """
    Download daily data for a list of symbols covering target_date.
    yfinance's batch download (space-separated tickers) is far more
    efficient than one-by-one calls.
    """
    start = target_date.strftime("%Y-%m-%d")
    # End date is exclusive in yfinance → add one day
    end = (target_date + timedelta(days=1)).strftime("%Y-%m-%d")

    tickers_str = " ".join(symbols)
    try:
        raw = yf.download(
            tickers=tickers_str,
            start=start,
            end=end,
            interval="1d",
            auto_adjust=True,       # adjusts for splits/dividends automatically
            progress=False,
            timeout=config.REQUEST_TIMEOUT,
            group_by="ticker",      # MultiIndex: (column, ticker)
        )
    except Exception as exc:
        log.warning("Batch download raised exception: %s", exc)
        return pd.DataFrame()

    if raw.empty:
        return pd.DataFrame()

    # Flatten MultiIndex columns → long format with a 'symbol' column
    rows = []
    for sym in symbols:
        try:
            if len(symbols) == 1:
                sym_df = raw  # single-ticker returns flat columns
            else:
                sym_df = raw[sym]  # slices the MultiIndex on the ticker level
        except KeyError:
            continue

        sym_df = sym_df.dropna(how="all")
        if sym_df.empty:
            continue

        sym_df = sym_df.rename(columns=str)[config.COLUMNS_TO_KEEP]
        sym_df["symbol"] = sym
        sym_df["date"] = sym_df.index.date
        rows.append(sym_df.reset_index(drop=True))

    if not rows:
        return pd.DataFrame()

    return pd.concat(rows, ignore_index=True)


def fetch_candles_for_date(
    symbols: list[str], target_date: date
) -> pd.DataFrame:
    """
    Iterate through symbols in batches, collecting OHLCV data.
    Retries failed batches up to RETRY_ATTEMPTS times.
    """
    all_frames: list[pd.DataFrame] = []
    total = len(symbols)
    batches = [
        symbols[i : i + config.BATCH_SIZE]
        for i in range(0, total, config.BATCH_SIZE)
    ]

    log.info(
        "Fetching candles for %s — %d symbols in %d batches",
        target_date,
        total,
        len(batches),
    )

    for idx, batch in enumerate(batches, start=1):
        attempt = 0
        success = False
        while attempt < config.RETRY_ATTEMPTS and not success:
            attempt += 1
            try:
                df = _fetch_batch(batch, target_date)
                all_frames.append(df)
                success = True
            except Exception as exc:
                log.warning(
                    "Batch %d/%d attempt %d failed: %s",
                    idx, len(batches), attempt, exc,
                )
                if attempt < config.RETRY_ATTEMPTS:
                    time.sleep(config.RETRY_DELAY_SECONDS)

        if not success:
            log.error("Batch %d/%d permanently failed — skipping.", idx, len(batches))

        if idx % 10 == 0 or idx == len(batches):
            log.info("  Progress: %d/%d batches complete", idx, len(batches))

        if idx < len(batches):
            time.sleep(config.BATCH_DELAY_SECONDS)

    if not all_frames:
        log.warning("No data returned for %s", target_date)
        return pd.DataFrame()

    combined = pd.concat(all_frames, ignore_index=True)
    combined = combined.drop_duplicates(subset=["symbol", "date"])
    log.info(
        "Retrieved %d rows across %d symbols for %s",
        len(combined),
        combined["symbol"].nunique(),
        target_date,
    )
    return combined


# ── Storage ───────────────────────────────────────────────────────────────────

def parquet_path(target_date: date) -> Path:
    """Return the partition path for a given date."""
    return (
        config.DATA_DIR
        / f"year={target_date.year}"
        / f"month={target_date.month:02d}"
        / f"{target_date.strftime('%Y%m%d')}.parquet"
    )


def save_to_parquet(df: pd.DataFrame, target_date: date) -> Path:
    """
    Persist the DataFrame as a Snappy-compressed Parquet file.
    Returns the path written.
    """
    path = parquet_path(target_date)
    path.parent.mkdir(parents=True, exist_ok=True)

    df["date"] = pd.to_datetime(df["date"])           # ensure proper dtype
    df["symbol"] = df["symbol"].astype("category")    # efficient string storage

    df.to_parquet(
        path,
        engine="pyarrow",
        compression=config.PARQUET_COMPRESSION,
        index=False,
    )
    size_kb = path.stat().st_size / 1024
    log.info("Saved %s  (%.1f KB)", path, size_kb)
    return path


# ── State tracking ────────────────────────────────────────────────────────────

def record_run(run_date: date) -> None:
    config.STATE_FILE.write_text(run_date.isoformat())


def last_run_date() -> date | None:
    if config.STATE_FILE.exists():
        try:
            return date.fromisoformat(config.STATE_FILE.read_text().strip())
        except ValueError:
            pass
    return None


# ── Main entry point ──────────────────────────────────────────────────────────

def is_trading_day(d: date) -> bool:
    """Rough filter: skip weekends. Does NOT account for market holidays."""
    return d.weekday() < 5  # 0=Mon … 4=Fri


def run_fetch(target_date: date, force: bool = False) -> bool:
    """
    Fetch and store candles for target_date.
    Returns True on success, False otherwise.
    """
    if not is_trading_day(target_date):
        log.info("%s is a weekend — skipping.", target_date)
        return True  # not an error

    out_path = parquet_path(target_date)
    if out_path.exists() and not force:
        log.info("Data already exists for %s at %s — skipping (use --force to overwrite).", target_date, out_path)
        return True

    try:
        symbols = fetch_nasdaq_symbols()
    except Exception:
        return False

    df = fetch_candles_for_date(symbols, target_date)
    if df.empty:
        log.warning("Empty result for %s — nothing written.", target_date)
        return False

    save_to_parquet(df, target_date)
    record_run(target_date)
    return True


def run_today(force: bool = False) -> bool:
    today = date.today()
    # Market closes at 4 PM; if running before 4 PM, warn but continue
    now = datetime.now()
    if now.hour < 16:
        log.warning(
            "It is %s — markets may not be closed yet. "
            "Fetched data could be incomplete.",
            now.strftime("%H:%M"),
        )
    return run_fetch(today, force=force)


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch NASDAQ daily candles and store as Parquet."
    )
    parser.add_argument(
        "--date",
        action="append",
        dest="dates",
        metavar="YYYY-MM-DD",
        help=(
            "Date(s) to fetch. Pass once for a single date, "
            "twice for a start/end range (inclusive). "
            "Defaults to today."
        ),
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-fetch even if the Parquet file already exists.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if not args.dates:
        # Default: today
        success = run_today(force=args.force)
        sys.exit(0 if success else 1)

    if len(args.dates) == 1:
        d = date.fromisoformat(args.dates[0])
        success = run_fetch(d, force=args.force)
        sys.exit(0 if success else 1)

    if len(args.dates) == 2:
        start = date.fromisoformat(args.dates[0])
        end = date.fromisoformat(args.dates[1])
        if start > end:
            log.error("Start date must be ≤ end date.")
            sys.exit(1)
        current = start
        results = []
        while current <= end:
            results.append(run_fetch(current, force=args.force))
            current += timedelta(days=1)
        sys.exit(0 if all(results) else 1)

    log.error("Pass at most two --date arguments (start and end).")
    sys.exit(1)


if __name__ == "__main__":
    main()

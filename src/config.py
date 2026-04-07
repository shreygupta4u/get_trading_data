"""
config.py — Central configuration for NASDAQ Candle Fetcher
Adjust these values to match your environment and preferences.
"""

import os
from datetime import date
from pathlib import Path

# ── Paths ────────────────────────────────────────────────────────────────────
BASE_DIR          = Path(__file__).parent.parent   # project root (one level above src/)
DATA_DIR          = BASE_DIR / "data"                    # Root for all data
SYMBOLS_DATA_DIR  = DATA_DIR / "symbols"                 # Legacy per-symbol parquet (kept, not deleted)
DELTA_DIR         = DATA_DIR / "delta"                   # Delta Lake tables (primary store)
LOG_DIR           = BASE_DIR / "logs"                    # Log files
SYMBOL_STATE_FILE = BASE_DIR / ".symbol_state.json"      # Backward-compat alias (daily state)

# ── Time Periods ──────────────────────────────────────────────────────────────
# Periods processed by the orchestrator, in execution order.
# "daily"  → fetches raw OHLCV from yfinance (source of truth).
# "weekly" → derives weekly OHLCV by resampling the daily Delta table.
# Daily must come before weekly because weekly depends on fresh daily data.
TIME_PERIODS: list[str] = ["daily", "weekly"]

# ── Per-period state files ────────────────────────────────────────────────────
# Each file tracks { "AAPL": "2026-04-04", ... } for that period.
#   daily              → last daily close date fetched per symbol
#   weekly             → last complete week_start (Monday) derived per symbol
#   technicals_daily   → candles-state date when daily indicators were last computed
#   technicals_weekly  → candles-state date when weekly indicators were last computed
SYMBOL_STATE_FILES: dict[str, Path] = {
    "daily":             BASE_DIR / ".symbol_state.json",
    "weekly":            BASE_DIR / ".symbol_state_weekly.json",
    "technicals_daily":  BASE_DIR / ".symbol_state_technicals_daily.json",
    "technicals_weekly": BASE_DIR / ".symbol_state_technicals_weekly.json",
}

# ── Per-period Delta table paths ─────────────────────────────────────────────
# One table per period — OHLCV candles AND technical indicators live together.
#
#   daily   schema: date | symbol | Open | High | Low | Close | Volume
#                          | sma_20 | sma_33 | ema_20 | ema_33
#
#   weekly  schema: week_start | symbol | Open | High | Low | Close | Volume
#                          | trading_days | sma_20 | sma_33 | ema_20 | ema_33
#
# Indicator columns are NaN until technical_analysis.py runs.
DELTA_PATHS: dict[str, Path] = {
    "daily":  DATA_DIR / "delta" / "daily",
    "weekly": DATA_DIR / "delta" / "weekly",
}

# ── NASDAQ Symbol Sources ─────────────────────────────────────────────────────
# Used only when SYMBOLS=[] and INDEXES=[].
# The screener JSON API is tried first; the pipe-delimited FTP file is the fallback.
NASDAQ_SCREENER_URL = (
    "https://api.nasdaq.com/api/screener/stocks?tableonly=true&download=true"
)
NASDAQ_SYMBOLS_URL = (                                        # FTP fallback
    "https://ftp.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt"
)

# ── Fetch Settings ────────────────────────────────────────────────────────────
BATCH_SIZE           = 100     # (legacy) symbols per yfinance batch call
BATCH_DELAY_SECONDS  = 2      # Pause between parallel batches (politeness delay)
MAX_WORKERS          = 4      # Parallel yfinance fetch threads per batch
                               # Increase for faster downloads; lower if rate-limited
REQUEST_TIMEOUT      = 30     # yfinance download timeout in seconds
RETRY_ATTEMPTS       = 3      # Retry each chunk/symbol this many times
RETRY_DELAY_SECONDS  = 10     # Wait between retries

# ── Schedule Settings ─────────────────────────────────────────────────────────
# Run daily at this time in 24-hour local time.
SCHEDULE_HOUR   = 16   # 4 PM
SCHEDULE_MINUTE = 15   # 4:15 PM  (gives markets time to publish final candle)

# If the machine was OFF during the scheduled time, run immediately on startup
# for any missed trading days within this look-back window (calendar days).
CATCHUP_DAYS = 7

# ── Data Settings ─────────────────────────────────────────────────────────────
# Parquet compression codec: "snappy" (fast), "gzip" (smaller), "zstd" (best)
PARQUET_COMPRESSION = "snappy"

# Columns stored in each parquet file.
# yfinance returns: Open, High, Low, Close, Volume, Dividends, Stock Splits
COLUMNS_TO_KEEP = ["Open", "High", "Low", "Close", "Volume"]

# ── Logging ───────────────────────────────────────────────────────────────────
LOG_LEVEL            = "INFO"          # DEBUG | INFO | WARNING | ERROR
LOG_ROTATION_BYTES   = 5 * 1024 * 1024  # Rotate after 5 MB
LOG_BACKUP_COUNT     = 5               # Keep 5 rotated log files

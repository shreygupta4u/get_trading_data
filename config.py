"""
config.py — Central configuration for NASDAQ Daily Candle Fetcher
Adjust these values to match your environment and preferences.
"""

import os
from datetime import date
from pathlib import Path

# ── Paths ────────────────────────────────────────────────────────────────────
BASE_DIR          = Path(__file__).parent
DATA_DIR          = BASE_DIR / "data"                    # Root for all data
SYMBOLS_DATA_DIR  = DATA_DIR / "symbols"                 # One parquet per symbol
LOG_DIR           = BASE_DIR / "logs"                    # Log files
SYMBOL_STATE_FILE = BASE_DIR / ".symbol_state.json"      # Per-symbol last-fetch dates

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
BATCH_SIZE          = 100      # Symbols to request per yfinance batch call
BATCH_DELAY_SECONDS = 2        # Pause between batches (be polite to the API)
MAX_WORKERS         = 1        # Parallel batch workers (keep at 1 to avoid bans)
REQUEST_TIMEOUT     = 30       # yfinance download timeout in seconds
RETRY_ATTEMPTS      = 3        # Retry failed batches this many times
RETRY_DELAY_SECONDS = 10       # Wait between retries

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

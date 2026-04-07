"""
fetch_nasdaq_candles.py
=======================
Downloads daily OHLCV candles and stores one Parquet file per symbol.

Behaviour
---------
- First time a symbol is seen   → fetches full history from IPO date via
                                  yfinance period='max', up to the last
                                  closed trading day.
- Subsequent runs               → incremental: fetches only from the day
                                  after the last saved candle for that symbol.
- New symbol added to config    → treated as first-time (full history),
                                  all other symbols stay incremental.

Usage
-----
    python fetch_nasdaq_candles.py                         # incremental update
    python fetch_nasdaq_candles.py --force                 # re-fetch all symbols from scratch
    python fetch_nasdaq_candles.py --date 2024-12-31       # specific end date
    python fetch_nasdaq_candles.py --date 2024-01-01 --date 2024-12-31  # explicit range

Output layout
-------------
    data/
    └── symbols/
        ├── AAPL.parquet
        ├── MSFT.parquet
        └── ...

State file
----------
    .symbol_state.json   →  { "AAPL": "2026-04-04", "MSFT": "2026-04-04", ... }
"""

import argparse
import json
import logging
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from logging.handlers import RotatingFileHandler
from pathlib import Path

import pandas as pd
import requests
import yfinance as yf
from pytickersymbols import PyTickerSymbols

import config
import config_symbols
import delta_utils


# ── Logging ───────────────────────────────────────────────────────────────────

def setup_logging() -> logging.Logger:
    config.LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = config.LOG_DIR / "fetch.log"

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    logger = logging.getLogger("nasdaq_candles")
    # Guard: only add handlers once (safe when imported by orchestrator)
    if not logger.handlers:
        file_handler = RotatingFileHandler(
            log_file, maxBytes=config.LOG_ROTATION_BYTES, backupCount=config.LOG_BACKUP_COUNT
        )
        file_handler.setFormatter(fmt)
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(fmt)
        logger.setLevel(getattr(logging, config.LOG_LEVEL))
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
    return logger


log = setup_logging()


# ── Symbol resolution ─────────────────────────────────────────────────────────

# Map the short names used in config_symbols.INDEXES to pytickersymbols index names.
# Run  PyTickerSymbols().get_all_indices()  to see the full list the library supports.
_INDEX_PTS_NAMES: dict[str, str] = {
    "SP500":      "S&P 500",
    "NASDAQ100":  "NASDAQ 100",
    "DOW30":      "DOW JONES",
    "DAX":        "DAX",
    "FTSE100":    "FTSE 100",
    "FTSE250":    "FTSE 250",
    "CAC40":      "CAC 40",
    "AEX":        "AEX",
    "BEL20":      "BEL 20",
    "IBEX35":     "IBEX 35",
    "SMI":        "SMI",
    "MDAX":       "MDAX",
    "TECDAX":     "TECDAX",
}

# Wikipedia fallback: used only when pytickersymbols cannot resolve an index.
_INDEX_WIKIPEDIA: dict[str, dict] = {
    "SP500": {
        "url": "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
        "col": "Symbol",
    },
    "NASDAQ100": {
        "url": "https://en.wikipedia.org/wiki/Nasdaq-100",
        "col": "Ticker",
    },
    "DOW30": {
        "url": "https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average",
        "col": "Symbol",
    },
}


def _fetch_index_constituents_pts(index_name: str) -> list[str]:
    """
    Resolve index constituents using the pytickersymbols library.
    Data is bundled inside the package — no internet required.
    Returns Yahoo Finance-compatible ticker strings, or [] on failure.
    """
    pts_name = _INDEX_PTS_NAMES.get(index_name.upper())
    if pts_name is None:
        log.warning(
            "Index '%s' not in pytickersymbols mapping. Supported short names: %s",
            index_name, ", ".join(_INDEX_PTS_NAMES),
        )
        return []

    try:
        pts = PyTickerSymbols()
        stocks = list(pts.get_stocks_by_index(pts_name))
        if not stocks:
            log.warning("pytickersymbols returned no stocks for '%s'.", pts_name)
            return []

        tickers: list[str] = []
        for stock in stocks:
            # A stock can have several Yahoo symbols (e.g. MMM.F for Frankfurt,
            # MMM for NYSE).  Prefer USD-denominated entries; take the first
            # non-empty Yahoo symbol otherwise; fall back to the generic 'symbol'.
            sym_entries = [
                e for e in stock.get("symbols", [])
                if isinstance(e, dict) and e.get("yahoo")
            ]
            usd_entries = [e for e in sym_entries if e.get("currency", "").upper() == "USD"]
            chosen = (usd_entries or sym_entries or [{}])[0]
            ticker = (
                str(chosen.get("yahoo", "") or stock.get("symbol", "")).strip()
            )
            if ticker:
                tickers.append(ticker)

        log.info(
            "pytickersymbols: %d constituents for %s ('%s')",
            len(tickers), index_name, pts_name,
        )
        return tickers

    except Exception as exc:
        log.warning("pytickersymbols failed for '%s': %s", index_name, exc)
        return []


def _fetch_index_constituents_wikipedia(index_name: str) -> list[str]:
    """Fallback: scrape index constituent tickers from Wikipedia."""
    meta = _INDEX_WIKIPEDIA.get(index_name.upper())
    if meta is None:
        log.error(
            "No Wikipedia fallback configured for index '%s'. "
            "Add it to _INDEX_WIKIPEDIA or install pytickersymbols.",
            index_name,
        )
        return []

    log.info("Fetching %s constituents from Wikipedia (fallback)...", index_name)
    try:
        tables = pd.read_html(meta["url"])
    except Exception as exc:
        log.error("Wikipedia fetch failed for %s: %s", index_name, exc)
        return []

    col = meta["col"]
    for table in tables:
        if col in table.columns:
            tickers = (
                table[col]
                .dropna()
                .astype(str)
                .str.strip()
                .str.replace(r"\s+", "", regex=True)
                .str.replace(".", "-", regex=False)  # BRK.B → BRK-B for yfinance
                .tolist()
            )
            tickers = [t for t in tickers if t and not t.startswith("^")]
            log.info("Wikipedia: %d constituents for %s", len(tickers), index_name)
            return tickers

    log.error("Could not find column '%s' in any Wikipedia table for %s.", col, index_name)
    return []


def _fetch_index_constituents(index_name: str) -> list[str]:
    """
    Resolve constituents for a named index.
    Tries pytickersymbols first (offline, bundled data), then falls back
    to scraping Wikipedia.
    """
    tickers = _fetch_index_constituents_pts(index_name)
    if tickers:
        return tickers
    log.info("Falling back to Wikipedia for index '%s'.", index_name)
    return _fetch_index_constituents_wikipedia(index_name)


def get_symbols() -> list[str]:
    """
    Build the final symbol list.

    Special values
    --------------
    SYMBOLS = ["all"]   → return the full NASDAQ listed-symbol directory
                          (INDEXES is ignored)
    INDEXES = ["none"]  → skip all index expansion (only SYMBOLS are used)

    Normal behaviour
    ----------------
    Start with explicit SYMBOLS, merge each index's constituents, de-duplicate
    (preserving order, explicit symbols first).
    If both lists are empty, fall back to the full NASDAQ directory.
    """
    symbols_cfg = [s.strip() for s in config_symbols.SYMBOLS if s.strip()]
    indexes_cfg = [i.strip() for i in config_symbols.INDEXES if i.strip()]

    # "all" in SYMBOLS → download every NASDAQ ticker, ignore everything else
    if any(s.upper() == "ALL" for s in symbols_cfg):
        log.info('SYMBOLS contains "all" — fetching full NASDAQ symbol list.')
        return _fetch_nasdaq_symbols()

    seen: set[str] = set()
    result: list[str] = []

    def _add(sym: str) -> None:
        s = sym.upper().strip()
        if s and s not in seen:
            seen.add(s)
            result.append(s)

    for s in symbols_cfg:
        _add(s)

    # "none" in INDEXES → skip index expansion entirely
    if not any(i.upper() == "NONE" for i in indexes_cfg):
        for index_name in indexes_cfg:
            for s in _fetch_index_constituents(index_name):
                _add(s)

    if result:
        log.info(
            "Symbol list: %d explicit + index constituents = %d total",
            len(symbols_cfg), len(result),
        )
        return result

    # Both lists empty → fall back to full NASDAQ directory
    return _fetch_nasdaq_symbols()


_SCREENER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nasdaq.com/",
}

_SYMBOL_JUNK_CHARS = frozenset("$^+.")


def _is_clean_symbol(sym: str) -> bool:
    return bool(sym) and not any(c in sym for c in _SYMBOL_JUNK_CHARS)


def _fetch_nasdaq_symbols_screener() -> list[str]:
    """
    Primary source: NASDAQ Screener JSON API.
    Returns all US-listed tickers visible in the NASDAQ stock screener.
    """
    log.info("Fetching symbol list from NASDAQ Screener API...")
    try:
        resp = requests.get(
            config.NASDAQ_SCREENER_URL, headers=_SCREENER_HEADERS, timeout=30
        )
        resp.raise_for_status()
        rows = resp.json().get("data", {}).get("rows") or []
        symbols = [
            r["symbol"].strip()
            for r in rows
            if r.get("symbol") and _is_clean_symbol(r["symbol"].strip())
        ]
        if symbols:
            log.info("NASDAQ Screener API: %d symbols", len(symbols))
        return symbols
    except Exception as exc:
        log.warning("NASDAQ Screener API failed: %s", exc)
        return []


def _fetch_nasdaq_symbols_ftp() -> list[str]:
    """
    Fallback source: NASDAQ FTP pipe-delimited listed-symbol file.
    """
    log.info("Fetching symbol list from NASDAQ FTP file...")
    try:
        resp = requests.get(config.NASDAQ_SYMBOLS_URL, timeout=30)
        resp.raise_for_status()
        lines = resp.text.splitlines()
        data_lines = [l for l in lines[1:] if not l.startswith("File Creation Time")]
        symbols = []
        for line in data_lines:
            parts = line.split("|")
            if len(parts) < 2:
                continue
            sym = parts[0].strip()
            if _is_clean_symbol(sym):
                symbols.append(sym)
        if symbols:
            log.info("NASDAQ FTP: %d symbols", len(symbols))
        return symbols
    except Exception as exc:
        log.warning("NASDAQ FTP failed: %s", exc)
        return []


def _fetch_nasdaq_symbols() -> list[str]:
    """
    Fetch all NASDAQ-listed symbols, trying sources in order:
      1. NASDAQ Screener JSON API  (primary — works from most networks)
      2. NASDAQ FTP pipe-delimited file  (fallback)
    Raises RuntimeError if every source fails.
    """
    for fetcher in (_fetch_nasdaq_symbols_screener, _fetch_nasdaq_symbols_ftp):
        symbols = fetcher()
        if symbols:
            return symbols

    raise RuntimeError(
        "Could not fetch the NASDAQ symbol list from any source. "
        "Check your internet connection, or explicitly set SYMBOLS / INDEXES "
        "in config_symbols.py to avoid needing the NASDAQ directory."
    )


# ── Per-symbol state ──────────────────────────────────────────────────────────

def load_symbol_state(period: str = "daily") -> dict[str, str]:
    """
    Load per-symbol last-fetch dates from the JSON state file for *period*.
    Returns a dict like {"AAPL": "2026-04-04", "MSFT": "2026-04-04"}.
    """
    path = config.SYMBOL_STATE_FILES.get(period, config.SYMBOL_STATE_FILE)
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            log.warning("Could not read state file %s (%s) — starting fresh.", path, exc)
    return {}


def save_symbol_state(state: dict[str, str], period: str = "daily") -> None:
    """Persist the state dict to disk (sorted for readability)."""
    path = config.SYMBOL_STATE_FILES.get(period, config.SYMBOL_STATE_FILE)
    path.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")


# ── Week helpers ──────────────────────────────────────────────────────────────

def _this_week_start() -> date:
    """Monday of the current ISO week."""
    today = date.today()
    return today - timedelta(days=today.weekday())


def _last_complete_week_start() -> date:
    """
    Monday of the last *fully completed* ISO week (ended last Sunday).
    If today is Wednesday 2026-04-08, returns Monday 2026-03-30.
    """
    return _this_week_start() - timedelta(weeks=1)


# ── Raw data normalisation ────────────────────────────────────────────────────

def _normalise_raw(raw: pd.DataFrame, symbol: str, end: date) -> pd.DataFrame:
    """
    Normalise a raw yfinance DataFrame into the canonical schema:
        date | Open | High | Low | Close | Volume | symbol
    Filters rows to <= end and drops NaN-only rows.
    """
    if raw is None or raw.empty:
        return pd.DataFrame()

    raw = raw.copy()
    raw.index = pd.to_datetime(raw.index)

    # Flatten MultiIndex columns that some yfinance versions return
    if isinstance(raw.columns, pd.MultiIndex):
        raw.columns = raw.columns.get_level_values(0)

    raw = raw.rename(columns=str).dropna(how="all")
    if raw.empty:
        return pd.DataFrame()

    # Keep only configured OHLCV columns that actually exist
    keep = [c for c in config.COLUMNS_TO_KEEP if c in raw.columns]
    if not keep:
        return pd.DataFrame()

    raw = raw[keep].copy()
    raw["symbol"] = symbol
    raw["date"] = raw.index.date

    # Exclude incomplete trading day (today before 4 PM is handled upstream,
    # but double-check here in case yfinance sneaks in a partial row)
    raw = raw[raw["date"] <= end]
    return raw.reset_index(drop=True)


# ── Data fetching ─────────────────────────────────────────────────────────────

DATE_CHUNK_YEARS = 3  # maximum years per incremental chunk

# If start <= this sentinel, use period="max" instead of chunked range fetch
_FULL_HISTORY_SENTINEL = date(1970, 1, 1)


def _date_chunks(start: date, end: date) -> list[tuple[date, date]]:
    """Split [start, end] into DATE_CHUNK_YEARS-year sub-ranges."""
    chunks: list[tuple[date, date]] = []
    chunk_start = start
    while chunk_start <= end:
        chunk_end = min(
            date(chunk_start.year + DATE_CHUNK_YEARS, chunk_start.month, chunk_start.day)
            - timedelta(days=1),
            end,
        )
        chunks.append((chunk_start, chunk_end))
        chunk_start = chunk_end + timedelta(days=1)
    return chunks


def _fetch_symbol_full_history(symbol: str, end: date) -> pd.DataFrame:
    """
    Use yfinance Ticker.history(period='max') to pull all available data
    for a symbol from its IPO date up to `end`.
    """
    log.info("  [full history] %s — fetching from IPO to %s", symbol, end)
    for attempt in range(1, config.RETRY_ATTEMPTS + 1):
        try:
            ticker = yf.Ticker(symbol)
            raw = ticker.history(period="max", auto_adjust=True, actions=False)
            df = _normalise_raw(raw, symbol, end)
            if df.empty:
                log.warning("  %s: no history data returned (possibly pre-IPO or delisted).", symbol)
            else:
                log.info(
                    "  %s: got %d rows  (%s → %s)",
                    symbol, len(df), df["date"].min(), df["date"].max(),
                )
            return df
        except Exception as exc:
            log.warning(
                "  %s full-history attempt %d/%d failed: %s",
                symbol, attempt, config.RETRY_ATTEMPTS, exc,
            )
            if attempt < config.RETRY_ATTEMPTS:
                time.sleep(config.RETRY_DELAY_SECONDS)

    log.error("  %s: all full-history attempts exhausted.", symbol)
    return pd.DataFrame()


def _fetch_symbol_incremental(symbol: str, start: date, end: date) -> pd.DataFrame:
    """
    Fetch daily OHLCV for a symbol over [start, end] in DATE_CHUNK_YEARS-year
    chunks, retrying each chunk on failure.
    """
    chunks = _date_chunks(start, end)
    log.info(
        "  [incremental] %s — %s → %s  (%d chunk(s))",
        symbol, start, end, len(chunks),
    )
    all_frames: list[pd.DataFrame] = []

    for chunk_start, chunk_end in chunks:
        yf_start = chunk_start.strftime("%Y-%m-%d")
        yf_end   = (chunk_end + timedelta(days=1)).strftime("%Y-%m-%d")  # exclusive

        for attempt in range(1, config.RETRY_ATTEMPTS + 1):
            try:
                raw = yf.download(
                    tickers=symbol,
                    start=yf_start,
                    end=yf_end,
                    interval="1d",
                    auto_adjust=True,
                    progress=False,
                    timeout=config.REQUEST_TIMEOUT,
                )
                df = _normalise_raw(raw, symbol, end)
                if not df.empty:
                    all_frames.append(df)
                break  # success (even if empty — no data for that window)
            except Exception as exc:
                log.warning(
                    "  %s chunk %s–%s attempt %d/%d failed: %s",
                    symbol, chunk_start, chunk_end, attempt, config.RETRY_ATTEMPTS, exc,
                )
                if attempt < config.RETRY_ATTEMPTS:
                    time.sleep(config.RETRY_DELAY_SECONDS)
                else:
                    log.error("  %s chunk %s–%s permanently failed — skipping.", symbol, chunk_start, chunk_end)

    if not all_frames:
        return pd.DataFrame()
    return pd.concat(all_frames, ignore_index=True)


def fetch_symbol(symbol: str, start: date, end: date) -> pd.DataFrame:
    """
    Route to the right fetcher:
      start <= _FULL_HISTORY_SENTINEL  →  full IPO history via period='max'
      otherwise                        →  chunked incremental range fetch
    """
    if start <= _FULL_HISTORY_SENTINEL:
        return _fetch_symbol_full_history(symbol, end)
    return _fetch_symbol_incremental(symbol, start, end)


# ── Storage ───────────────────────────────────────────────────────────────────

def _save_parquet_fallback(df: pd.DataFrame) -> None:
    """
    Pandas-based fallback: write per-symbol Parquet files to data/symbols/.
    Used when Java / Spark is not available.
    """
    config.SYMBOLS_DATA_DIR.mkdir(parents=True, exist_ok=True)
    for symbol, group in df.groupby("symbol"):
        path = config.SYMBOLS_DATA_DIR / f"{symbol}.parquet"
        group = group.copy()
        group["date"] = pd.to_datetime(group["date"])

        if path.exists():
            existing = pd.read_parquet(path)
            existing["date"] = pd.to_datetime(existing["date"])
            group = pd.concat([existing, group], ignore_index=True)

        group = (
            group
            .drop_duplicates(subset=["date"])
            .sort_values("date")
            .reset_index(drop=True)
        )
        group["symbol"] = group["symbol"].astype(str)
        group.to_parquet(path, engine="pyarrow",
                         compression=config.PARQUET_COMPRESSION, index=False)
        log.info("  Parquet saved: %s — %d rows", path.name, len(group))


def _add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute SMA-20, SMA-33, EMA-20, EMA-33 on the Close column of a
    single-symbol DataFrame that is already sorted by date.

    Called inline inside the parallel fetch worker so indicators are
    computed immediately after candle data arrives, not in a separate pass.

    Rolling windows require sufficient history:
      - For new symbols (first fetch = full IPO history): all windows compute.
      - For incremental fetches: we read the FULL existing history first,
        append the new rows, recompute on the combined series, then return
        only the new rows with fresh indicator values. This ensures the
        rolling window always has enough context even on small incremental
        fetches (e.g. 1-day update).
    """
    if df.empty:
        return df

    symbol = df["symbol"].iloc[0]
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)

    # Read existing history from Delta so rolling window has full context
    existing = delta_utils.read_candles(symbols=[symbol], period="daily")
    if not existing.empty:
        existing["date"] = pd.to_datetime(existing["date"])
        # Combine: existing base + new rows (new rows take precedence on same date)
        combined = pd.concat([existing, df], ignore_index=True)
        combined = combined.drop_duplicates(subset=["date"], keep="last")
        combined = combined.sort_values("date").reset_index(drop=True)
    else:
        combined = df

    close = combined["Close"]
    combined["sma_20"] = close.rolling(window=20, min_periods=20).mean().round(6)
    combined["sma_33"] = close.rolling(window=33, min_periods=33).mean().round(6)
    combined["ema_20"] = close.ewm(span=20, adjust=False, min_periods=20).mean().round(6)
    combined["ema_33"] = close.ewm(span=33, adjust=False, min_periods=33).mean().round(6)

    # Return only the new rows (with their freshly computed indicators)
    new_dates = set(df["date"])
    result = combined[combined["date"].isin(new_dates)].reset_index(drop=True)
    return result


_INDICATOR_COLS = {"sma_20", "sma_33", "ema_20", "ema_33"}

def save_symbol_data(df: pd.DataFrame, period: str = "daily") -> None:
    """
    Persist candle rows to the Delta table for *period*.

    - If the DataFrame contains indicator columns (sma_20/33, ema_20/33):
      uses update-mode merge so existing rows get their indicator values
      filled in and new rows are inserted with indicators already set.

    - If no indicator columns are present:
      uses insert-only merge (new rows only, existing rows untouched).
    """
    has_indicators = bool(_INDICATOR_COLS & set(df.columns))
    if has_indicators:
        delta_utils.upsert_technicals(df, period=period)
    else:
        delta_utils.upsert_candles(df, period=period)


# ── Date helpers ──────────────────────────────────────────────────────────────

def is_trading_day(d: date) -> bool:
    """Weekday check only (holidays are handled implicitly by yfinance)."""
    return d.weekday() < 5  # Mon=0 … Fri=4


def _safe_end_date(requested: date) -> date:
    """
    Cap the end date to yesterday when running before 4 PM so we never
    request an in-progress trading day.  yfinance will skip any
    weekend / holiday rows in a range fetch automatically.
    """
    now = datetime.now()
    today = now.date()
    if requested >= today and now.hour < 16:
        capped = today - timedelta(days=1)
        log.info(
            "It is %s (before 4 PM) — capping end date from %s to %s.",
            now.strftime("%H:%M"), requested, capped,
        )
        return capped
    return requested


# ── Main fetch engine ─────────────────────────────────────────────────────────

def run_all_symbols(
    period: str = "daily",
    force: bool = False,
    override_start: date | None = None,
    override_end: date | None = None,
) -> bool:
    """
    Fetch / derive candles for all configured symbols for the given *period*.

    period="daily"
        Fetches raw OHLCV from yfinance (parallel, incremental).
        Skips symbols already up to the safe end date.

    period="weekly"
        Derives weekly OHLCV by resampling the daily Delta table.
        Skips symbols whose weekly state >= last complete week Monday.
        No yfinance calls — pure resample of existing data.

    Returns True if at least one symbol was written successfully.
    """
    try:
        symbols = get_symbols()
    except Exception:
        return False

    if not symbols:
        log.error("Symbol list is empty — nothing to do.")
        return False

    # One-time migrations: legacy Parquet files → Delta, then separate
    # candle/technicals tables → combined daily/weekly tables
    if period == "daily":
        delta_utils.migrate_parquet_to_delta()
    delta_utils.migrate_to_combined_tables()

    if period == "weekly":
        return _run_all_weekly(symbols, force=force)
    else:
        return _run_all_daily(
            symbols,
            force=force,
            override_start=override_start,
            override_end=override_end,
        )


# ── Daily fetch (yfinance, parallel) ─────────────────────────────────────────

def _run_all_daily(
    symbols: list[str],
    force: bool = False,
    override_start: date | None = None,
    override_end: date | None = None,
) -> bool:
    end   = _safe_end_date(override_end or date.today())
    state = load_symbol_state("daily")
    state_lock  = threading.Lock()
    total       = len(symbols)
    skipped = failed = 0
    any_success = False

    log.info("[daily] Parallel fetch: %d symbol(s), %d workers, end=%s",
             total, config.MAX_WORKERS, end)

    # ── Build work list ───────────────────────────────────────────────────────
    pending: list[tuple[str, date]] = []
    for symbol in symbols:
        if override_start is not None:
            pending.append((symbol, override_start))
        elif force or symbol not in state:
            pending.append((symbol, date(1900, 1, 1)))   # sentinel → period='max'
        else:
            last = date.fromisoformat(state[symbol])
            nxt  = last + timedelta(days=1)
            if nxt > end:
                skipped += 1
            else:
                pending.append((symbol, nxt))

    log.info("[daily] %d to fetch, %d already up to date", len(pending), skipped)

    def _fetch_one(sym: str, start: date) -> tuple[str, pd.DataFrame | None, date | None]:
        reason = "full history" if start <= _FULL_HISTORY_SENTINEL else f"{start} to {end}"
        log.info("  [daily] %s  (%s)", sym, reason)
        try:
            df = fetch_symbol(sym, start, end)
            if df.empty:
                return sym, None, None
            # Compute indicators inline — uses full history from Delta for context
            df = _add_indicators(df)
            latest = pd.to_datetime(df["date"]).dt.date.max()
            return sym, df, latest
        except Exception as exc:
            log.error("  [daily error] %s: %s", sym, exc)
            return sym, None, None

    batch_size    = config.MAX_WORKERS
    total_batches = max(1, (len(pending) + batch_size - 1) // batch_size)

    try:
        for b_idx, b_start in enumerate(range(0, len(pending), batch_size), start=1):
            batch = pending[b_start : b_start + batch_size]
            pct   = (b_start / max(len(pending), 1)) * 100
            log.info("=== [daily] Batch %d/%d  (%.0f%%)  %s ===",
                     b_idx, total_batches, pct, [s for s, _ in batch])

            with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
                futures = {executor.submit(_fetch_one, sym, start): sym
                           for sym, start in batch}
                batch_results: list[tuple[str, pd.DataFrame, date]] = []
                for future in as_completed(futures):
                    sym, df, latest = future.result()
                    if df is not None:
                        batch_results.append((sym, df, latest))
                        log.info("  [daily done] %s  rows=%d  last=%s", sym, len(df), latest)
                    else:
                        failed += 1

            if batch_results:
                combined = pd.concat([r[1] for r in batch_results], ignore_index=True)
                save_symbol_data(combined, period="daily")
                with state_lock:
                    for sym, _, latest in batch_results:
                        state[sym] = latest.isoformat()
                    save_symbol_state(state, "daily")
                any_success = True
                log.info("  [daily] Batch %d/%d saved: %d symbol(s), %d rows",
                         b_idx, total_batches, len(batch_results), len(combined))

            if b_idx < total_batches:
                time.sleep(config.BATCH_DELAY_SECONDS)

    except KeyboardInterrupt:
        log.warning("[daily] Interrupted — state saved to last completed batch.")

    done = sum(1 for s in symbols if s in state)
    log.info("[daily] Done: %d/%d in state | skipped=%d failed=%d",
             done, total, skipped, failed)
    return any_success


# ── Weekly fetch (resample from daily Delta, no yfinance) ────────────────────

def _run_all_weekly(symbols: list[str], force: bool = False) -> bool:
    """
    Derive weekly OHLCV candles by resampling the daily Delta table.

    Skip logic (per symbol)
    -----------------------
    weekly_state[symbol] >= last_complete_week_start()
        → already have all complete weeks → skip

    Only COMPLETE weeks (week_start < this Monday) are written.
    The current in-progress week is intentionally excluded so it can be
    re-derived on the next run once more daily bars arrive.
    """
    if not delta_utils.is_delta_table(delta_utils.CANDLES_DELTA_PATH):
        log.error("[weekly] Daily Delta table not found — run daily fetch first.")
        return False

    daily_state  = load_symbol_state("daily")
    weekly_state = load_symbol_state("weekly")

    last_complete = _last_complete_week_start()   # last Monday whose week is done
    this_monday   = _this_week_start()            # current week start — excluded

    total   = len(symbols)
    skipped = failed = 0
    any_success = False

    # ── Determine which symbols need updating ─────────────────────────────────
    pending: list[str] = []
    for symbol in symbols:
        if symbol not in daily_state:
            log.debug("  [weekly] %s: no daily data yet — skipping", symbol)
            skipped += 1
            continue
        if not force and symbol in weekly_state:
            last_w = date.fromisoformat(weekly_state[symbol])
            if last_w >= last_complete:
                skipped += 1
                continue
        pending.append(symbol)

    log.info("[weekly] %d symbol(s) to derive, %d already up to date (last complete week: %s)",
             len(pending), skipped, last_complete)

    if not pending:
        return True

    # ── Batch-read daily candles for all pending symbols ──────────────────────
    # Read from earliest weekly state date - 1 week to catch any late arrivals
    min_weekly_date = None
    for sym in pending:
        if sym in weekly_state:
            d = date.fromisoformat(weekly_state[sym])
            if min_weekly_date is None or d < min_weekly_date:
                min_weekly_date = d

    read_from = str(min_weekly_date - timedelta(days=7)) if min_weekly_date else None

    log.info("[weekly] Reading daily candles from Delta (from %s)...",
             read_from or "beginning of history")
    daily_df = delta_utils.read_candles(symbols=pending, start=read_from, period="daily")

    if daily_df.empty:
        log.error("[weekly] No daily candles returned for pending symbols.")
        return False

    daily_df["date"] = pd.to_datetime(daily_df["date"])

    # ── Resample each symbol to weekly ───────────────────────────────────────
    results: list[pd.DataFrame] = []

    for symbol, grp in daily_df.groupby("symbol"):
        try:
            grp = grp.sort_values("date").set_index("date")

            # Snap each daily bar to its ISO week Monday
            grp["week_start"] = (
                grp.index - pd.to_timedelta(grp.index.dayofweek, unit="D")
            ).date

            # Only keep rows in COMPLETE weeks (week_start < this Monday)
            grp = grp[grp["week_start"] < this_monday]

            if symbol in weekly_state and not force:
                last_w = date.fromisoformat(weekly_state[symbol])
                grp = grp[grp["week_start"] > last_w]

            if grp.empty:
                continue

            weekly_grp = (
                grp.groupby("week_start")
                .agg(
                    Open        =("Open",  "first"),
                    High        =("High",  "max"),
                    Low         =("Low",   "min"),
                    Close       =("Close", "last"),
                    Volume      =("Volume","sum"),
                    trading_days=("Close", "count"),
                )
            )
            weekly_grp["symbol"]  = symbol
            weekly_grp.index.name = "week_start"
            weekly_grp = weekly_grp.reset_index()

            # ── Indicators on weekly Close ────────────────────────────────────
            # Combine with existing weekly history so rolling window has context
            existing_w = delta_utils.read_candles(symbols=[symbol], period="weekly")
            if not existing_w.empty:
                existing_w["week_start"] = pd.to_datetime(existing_w["week_start"])
                weekly_grp["week_start"] = pd.to_datetime(weekly_grp["week_start"])
                combined_w = pd.concat([existing_w, weekly_grp], ignore_index=True)
                combined_w = combined_w.drop_duplicates(subset=["week_start"], keep="last")
                combined_w = combined_w.sort_values("week_start").reset_index(drop=True)
            else:
                weekly_grp["week_start"] = pd.to_datetime(weekly_grp["week_start"])
                combined_w = weekly_grp.sort_values("week_start").reset_index(drop=True)

            close_w = combined_w["Close"]
            combined_w["sma_20"] = close_w.rolling(window=20, min_periods=20).mean().round(6)
            combined_w["sma_33"] = close_w.rolling(window=33, min_periods=33).mean().round(6)
            combined_w["ema_20"] = close_w.ewm(span=20, adjust=False, min_periods=20).mean().round(6)
            combined_w["ema_33"] = close_w.ewm(span=33, adjust=False, min_periods=33).mean().round(6)

            # Keep only newly derived weeks
            new_weeks = set(weekly_grp["week_start"])
            weekly_grp = combined_w[combined_w["week_start"].isin(new_weeks)].reset_index(drop=True)
            results.append(weekly_grp)

        except Exception as exc:
            log.error("  [weekly error] %s: %s", symbol, exc)
            failed += 1

    if not results:
        log.info("[weekly] No new weekly candles to write.")
        return True

    combined = pd.concat(results, ignore_index=True)
    save_symbol_data(combined, period="weekly")

    # Update weekly state to last_complete for each symbol written
    for r in results:
        sym        = r["symbol"].iloc[0]
        latest_wk  = r["week_start"].max()
        # Store the actual latest week written (not the sentinel)
        weekly_state[sym] = str(latest_wk) if isinstance(latest_wk, date) \
                            else pd.to_datetime(latest_wk).date().isoformat()

    save_symbol_state(weekly_state, "weekly")
    any_success = True

    log.info("[weekly] Done: %d symbol(s) written, %d rows, %d failed",
             len(results), len(combined), failed)
    return any_success


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch OHLCV candles per symbol and store in Delta Lake.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples
--------
  python fetch_nasdaq_candles.py                                # daily incremental
  python fetch_nasdaq_candles.py --period weekly                # derive weekly candles
  python fetch_nasdaq_candles.py --force                        # re-fetch all from IPO
  python fetch_nasdaq_candles.py --date 2025-01-01              # daily up to date
  python fetch_nasdaq_candles.py --date 2024-01-01 --date 2025-01-01  # fixed range
        """,
    )
    parser.add_argument(
        "--period",
        choices=["daily", "weekly"],
        default="daily",
        help="Time period to fetch/derive (default: daily).",
    )
    parser.add_argument(
        "--date",
        action="append",
        dest="dates",
        metavar="YYYY-MM-DD",
        help=(
            "Pass once to set an explicit end date. "
            "Pass twice (start then end) to override both bounds. "
            "Only applies to --period daily."
        ),
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Ignore saved state and re-fetch/re-derive all symbols from scratch.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    override_start: date | None = None
    override_end: date | None = None

    if args.dates:
        if args.period == "weekly":
            log.warning("--date is ignored for --period weekly.")
        elif len(args.dates) == 1:
            override_end = _safe_end_date(date.fromisoformat(args.dates[0]))
        elif len(args.dates) == 2:
            override_start = date.fromisoformat(args.dates[0])
            override_end   = _safe_end_date(date.fromisoformat(args.dates[1]))
            if override_start > override_end:
                log.error("Start date %s must be <= end date %s.", override_start, override_end)
                sys.exit(1)
        else:
            log.error("Pass at most two --date arguments (start end).")
            sys.exit(1)

    success = run_all_symbols(
        period=args.period,
        force=args.force,
        override_start=override_start,
        override_end=override_end,
    )
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()

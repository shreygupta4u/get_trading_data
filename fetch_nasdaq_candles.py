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
import time
from datetime import date, datetime, timedelta
from logging.handlers import RotatingFileHandler
from pathlib import Path

import pandas as pd
import requests
import yfinance as yf
from pytickersymbols import PyTickerSymbols

import config
import config_symbols


# ── Logging ───────────────────────────────────────────────────────────────────

def setup_logging() -> logging.Logger:
    config.LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = config.LOG_DIR / "fetch.log"

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    file_handler = RotatingFileHandler(
        log_file, maxBytes=config.LOG_ROTATION_BYTES, backupCount=config.LOG_BACKUP_COUNT
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
    Build the final symbol list from config_symbols.SYMBOLS + any INDEXES.
    De-duplicates while preserving order (explicit symbols come first).
    Falls back to the full NASDAQ directory if both lists are empty.
    """
    seen: set[str] = set()
    result: list[str] = []

    def _add(sym: str) -> None:
        s = sym.upper().strip()
        if s and s not in seen:
            seen.add(s)
            result.append(s)

    for s in config_symbols.SYMBOLS:
        _add(s)

    for index_name in config_symbols.INDEXES:
        for s in _fetch_index_constituents(index_name):
            _add(s)

    if result:
        log.info(
            "Symbol list: %d explicit symbol(s) + index constituents = %d total",
            len(config_symbols.SYMBOLS), len(result),
        )
        return result

    return _fetch_nasdaq_symbols()


def _fetch_nasdaq_symbols() -> list[str]:
    """Download full NASDAQ listed-symbol directory (fallback when SYMBOLS=[] and INDEXES=[])."""
    log.info("Fetching NASDAQ symbol list from %s", config.NASDAQ_SYMBOLS_URL)
    try:
        resp = requests.get(config.NASDAQ_SYMBOLS_URL, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as exc:
        log.error("Could not fetch symbol list: %s", exc)
        raise

    lines = resp.text.splitlines()
    data_lines = [l for l in lines[1:] if not l.startswith("File Creation Time")]
    symbols = []
    for line in data_lines:
        parts = line.split("|")
        if len(parts) < 2:
            continue
        symbol = parts[0].strip()
        if any(c in symbol for c in ("$", "^", "+", ".")):
            continue
        if symbol:
            symbols.append(symbol)

    log.info("Found %d NASDAQ symbols", len(symbols))
    return symbols


# ── Per-symbol state ──────────────────────────────────────────────────────────

def load_symbol_state() -> dict[str, str]:
    """
    Load per-symbol last-fetch dates from the JSON state file.
    Returns a dict like {"AAPL": "2026-04-04", "MSFT": "2026-04-04"}.
    """
    if config.SYMBOL_STATE_FILE.exists():
        try:
            return json.loads(config.SYMBOL_STATE_FILE.read_text(encoding="utf-8"))
        except Exception as exc:
            log.warning("Could not read state file (%s) — starting fresh.", exc)
    return {}


def save_symbol_state(state: dict[str, str]) -> None:
    """Persist the state dict to disk (sorted for readability)."""
    config.SYMBOL_STATE_FILE.write_text(
        json.dumps(state, indent=2, sort_keys=True), encoding="utf-8"
    )


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


# ── Per-symbol Parquet storage ────────────────────────────────────────────────

def symbol_parquet_path(symbol: str) -> Path:
    return config.SYMBOLS_DATA_DIR / f"{symbol}.parquet"


def save_symbol_data(df: pd.DataFrame, symbol: str) -> None:
    """
    Upsert new rows into the symbol's Parquet file.
    Reads the existing file (if any), merges, de-duplicates on date, then writes.
    """
    path = symbol_parquet_path(symbol)
    path.parent.mkdir(parents=True, exist_ok=True)

    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])

    if path.exists():
        existing = pd.read_parquet(path)
        existing["date"] = pd.to_datetime(existing["date"])
        combined = pd.concat([existing, df], ignore_index=True)
    else:
        combined = df

    combined = (
        combined
        .drop_duplicates(subset=["date"])
        .sort_values("date")
        .reset_index(drop=True)
    )
    combined["symbol"] = combined["symbol"].astype(str)

    combined.to_parquet(
        path,
        engine="pyarrow",
        compression=config.PARQUET_COMPRESSION,
        index=False,
    )
    size_kb = path.stat().st_size / 1024
    log.info("  Saved %s — %d rows total  (%.1f KB)", path.name, len(combined), size_kb)


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
    force: bool = False,
    override_start: date | None = None,
    override_end: date | None = None,
) -> bool:
    """
    Core loop — iterates over every configured symbol and fetches / updates it.

    Per-symbol logic
    ----------------
    New symbol (not in .symbol_state.json)
        → fetches full history from IPO via period='max'
    Known symbol
        → incremental fetch: last_saved_date + 1 day  →  end_date
    override_start / override_end
        → bypasses state; forces a specific date range for all symbols

    After each symbol, the state file is updated immediately so that a
    partial run can be resumed later without re-downloading already-saved data.
    """
    end = _safe_end_date(override_end or date.today())

    try:
        symbols = get_symbols()
    except Exception:
        return False

    if not symbols:
        log.error("Symbol list is empty — nothing to do.")
        return False

    state = load_symbol_state()
    any_success = False
    total = len(symbols)

    log.info("Starting fetch for %d symbol(s)  end_date=%s", total, end)

    for idx, symbol in enumerate(symbols, start=1):
        log.info("─── [%d/%d] %s ───", idx, total, symbol)

        # ── Determine start date for this symbol ──────────────────────────────
        if override_start is not None:
            start = override_start
            log.info("  Range override: %s → %s", start, end)

        elif force or symbol not in state:
            # First time or forced re-fetch → full history from IPO
            start = date(1900, 1, 1)   # sentinel triggers period='max' fetch
            reason = "forced re-fetch" if (force and symbol in state) else "new symbol"
            log.info("  %s — fetching full history from IPO → %s", reason, end)

        else:
            last_fetched = date.fromisoformat(state[symbol])
            start = last_fetched + timedelta(days=1)
            if start > end:
                log.info("  Already up to date (last candle: %s) — skipping.", last_fetched)
                continue
            log.info("  Incremental: last candle %s → fetching %s → %s", last_fetched, start, end)

        # ── Fetch ─────────────────────────────────────────────────────────────
        df = fetch_symbol(symbol, start, end)

        if df.empty:
            log.warning("  No data returned for %s — state unchanged.", symbol)
            if idx < total:
                time.sleep(config.BATCH_DELAY_SECONDS)
            continue

        # ── Save & update state immediately ───────────────────────────────────
        save_symbol_data(df, symbol)

        latest_date = pd.to_datetime(df["date"]).dt.date.max()
        state[symbol] = latest_date.isoformat()
        save_symbol_state(state)
        any_success = True

        log.info("  State updated: %s last_candle=%s", symbol, latest_date)

        if idx < total:
            time.sleep(config.BATCH_DELAY_SECONDS)

    log.info("Fetch complete — %d/%d symbols had new data.", sum(1 for s in symbols if s in state), total)
    return any_success


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch daily OHLCV candles per symbol and store as Parquet.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples
--------
  python fetch_nasdaq_candles.py                          # incremental update
  python fetch_nasdaq_candles.py --force                  # re-fetch all from IPO
  python fetch_nasdaq_candles.py --date 2025-01-01        # incremental up to date
  python fetch_nasdaq_candles.py --date 2024-01-01 --date 2025-01-01  # fixed range
        """,
    )
    parser.add_argument(
        "--date",
        action="append",
        dest="dates",
        metavar="YYYY-MM-DD",
        help=(
            "Pass once to set an explicit end date. "
            "Pass twice (start then end) to override both bounds for all symbols."
        ),
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Ignore saved state and re-fetch all symbols from their IPO date.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    override_start: date | None = None
    override_end: date | None = None

    if args.dates:
        if len(args.dates) == 1:
            override_end = _safe_end_date(date.fromisoformat(args.dates[0]))
        elif len(args.dates) == 2:
            override_start = date.fromisoformat(args.dates[0])
            override_end   = _safe_end_date(date.fromisoformat(args.dates[1]))
            if override_start > override_end:
                log.error("Start date %s must be ≤ end date %s.", override_start, override_end)
                sys.exit(1)
        else:
            log.error("Pass at most two --date arguments (start end).")
            sys.exit(1)

    success = run_all_symbols(
        force=args.force,
        override_start=override_start,
        override_end=override_end,
    )
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()

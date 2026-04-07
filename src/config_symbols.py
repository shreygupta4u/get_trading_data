"""
config_symbols.py — Symbol and index configuration
===================================================
Edit this file to control which securities are fetched.

SYMBOLS  — explicit list of tickers (e.g. MAG7 below).
INDEXES  — market indexes whose full constituent lists are automatically
           resolved and merged with SYMBOLS at run time.

Supported index short names  (resolved via pytickersymbols, Wikipedia as fallback)
---------------------------
    US indices
        "SP500"      — S&P 500             (~503 stocks)
        "NASDAQ100"  — NASDAQ 100          (~100 stocks)
        "DOW30"      — Dow Jones           (  30 stocks)
    European indices
        "DAX"        — DAX (Germany)       (  40 stocks)
        "FTSE100"    — FTSE 100 (UK)       ( 100 stocks)
        "FTSE250"    — FTSE 250 (UK)       ( 250 stocks)
        "CAC40"      — CAC 40 (France)     (  40 stocks)
        "AEX"        — AEX (Netherlands)   (  25 stocks)
        "BEL20"      — BEL 20 (Belgium)    (  20 stocks)
        "IBEX35"     — IBEX 35 (Spain)     (  35 stocks)
        "SMI"        — SMI (Switzerland)   (  20 stocks)
        "MDAX"       — MDAX (Germany)      (  50 stocks)
        "TECDAX"     — TecDAX (Germany)    (  30 stocks)

Special values
--------------
    SYMBOLS = ["all"]   — download every ticker listed on NASDAQ
                          (ignores INDEXES entirely)
    INDEXES = ["none"]  — skip all index expansion
                          (only the tickers in SYMBOLS are fetched)

Examples
--------
    SYMBOLS = ["AAPL", "TSLA"],  INDEXES = ["SP500", "NASDAQ100"]
    → AAPL + TSLA + all S&P 500 + NASDAQ 100 stocks (de-duplicated)

    SYMBOLS = ["all"],  INDEXES = [...]
    → every NASDAQ-listed ticker (INDEXES is ignored)

    SYMBOLS = ["AAPL", "MSFT"],  INDEXES = ["none"]
    → only AAPL and MSFT

    SYMBOLS = [],  INDEXES = []
    → falls back to the full NASDAQ listed-symbol directory (same as "all")
"""

# ── Individual Symbols ────────────────────────────────────────────────────────
# Use ["all"] to download every NASDAQ-listed ticker.
SYMBOLS: list[str] = [
    #"AAPL",   # Apple
    #"MSFT",   # Microsoft
    #"CRWD",   # Crowdstrike
    #"GOOGL",  # Alphabet
    # "AMZN",   # Amazon
    # "NVDA",   # NVIDIA
    # "META",   # Meta
    # "TSLA",   # Tesla
]

# ── Index Constituents ────────────────────────────────────────────────────────
# Use ["none"] to skip all index expansion.
INDEXES: list[str] = [
    #"SP500",
    # "NASDAQ100",
    # "DOW30",
]
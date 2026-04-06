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

Example
-------
    SYMBOLS = ["AAPL", "TSLA"]
    INDEXES = ["SP500", "NASDAQ100"]
    → fetches AAPL + TSLA + all S&P 500 + NASDAQ 100 stocks (de-duplicated)

Leave both lists empty to fall back to the full NASDAQ listed-symbol directory.
"""

# ── Individual Symbols ────────────────────────────────────────────────────────
SYMBOLS: list[str] = [
    "AAPL",   # Apple
    "MSFT",   # Microsoft
    "CRWD",     # Crowdstrike
    "GOOGL",  # Alphabet
    # "AMZN",   # Amazon
    # "NVDA",   # NVIDIA
    # "META",   # Meta
    # "TSLA",   # Tesla
]

# ── Index Constituents ────────────────────────────────────────────────────────
# Add index names here to include all their constituent stocks.
# Constituents are fetched live from Wikipedia at run time.
INDEXES: list[str] = [
    "SP500",
    # "NASDAQ100",
    # "DOW30",
]
"""
run.py
======
Interactive launcher for the FVB33 data pipeline.

Run from the project root:
    python run.py

Or pass an option number directly to skip the menu:
    python run.py 1     # full incremental run
    python run.py 5     # force weekly indicators + signals
"""

import sys
import os
import time

# ── Make sure src/ is on the path ─────────────────────────────────────────────
SRC = os.path.join(os.path.dirname(__file__), "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from fetch_nasdaq_candles import setup_logging
log = setup_logging()

MENU = """
╔══════════════════════════════════════════════════════════════╗
║              FVB33 Data Pipeline — Run Menu                  ║
╠══════════════════════════════════════════════════════════════╣
║  FULL PIPELINE                                               ║
║   1. Full incremental run   (fetch + indicators + signals)   ║
║   2. Full FORCE run         (re-fetch + recompute everything) ║
║                                                              ║
║  WEEKLY INDICATORS                                           ║
║   3. Weekly indicators — incremental                         ║
║   4. Weekly indicators — FORCE recompute all                 ║
║                                                              ║
║  SIGNALS                                                     ║
║   5. Signals — incremental  (skip up-to-date symbols)        ║
║   6. Signals — FORCE recompute all                           ║
║                                                              ║
║  COMBINED (indicators + signals)                             ║
║   7. Weekly indicators + signals — incremental               ║
║   8. Weekly indicators + signals — FORCE recompute all       ║
║      (use this after any indicator/signal logic change)      ║
║                                                              ║
║  PORTFOLIO BACKTEST (S&P 500 fund simulation)                ║
║  10. Portfolio backtest — incremental  (new weeks only)      ║
║  11. Portfolio backtest — FORCE full recompute               ║
║                                                              ║
║  MAINTENANCE                                                 ║
║   9. Vacuum Delta tables    (reclaim disk — run monthly)     ║
║                                                              ║
║   0. Exit                                                    ║
╚══════════════════════════════════════════════════════════════╝
"""


def run_full(force: bool = False) -> None:
    """Full orchestrator run: fetch + indicators + signals for all periods."""
    from orchestrator import run_orchestrator
    log.info("Starting full %s run...", "FORCE" if force else "incremental")
    run_orchestrator(force=force)


def run_weekly_indicators(force: bool = False) -> None:
    """Weekly indicators only (no candle fetch, no signals)."""
    from technical_analysis import calculate_indicators
    log.info("Weekly indicators — %s...", "FORCE recompute" if force else "incremental")
    t0 = time.time()
    result = calculate_indicators(period="weekly", force=force)
    elapsed = time.time() - t0
    if result.empty:
        log.info("Weekly indicators: no rows written (all up to date).")
    else:
        log.info("Weekly indicators done in %.1f s.", elapsed)


def run_signals(force: bool = False) -> None:
    """Signals only (reads existing weekly indicators, no re-fetch)."""
    from signals import calculate_signals
    log.info("Signals — %s...", "FORCE recompute" if force else "incremental")
    t0 = time.time()
    ok = calculate_signals(force=force)
    elapsed = time.time() - t0
    if ok:
        log.info("Signals done in %.1f s.", elapsed)
    else:
        log.info("Signals: nothing written (all symbols already current).")


def run_weekly_indicators_and_signals(force: bool = False) -> None:
    """Weekly indicators then signals in sequence."""
    run_weekly_indicators(force=force)
    run_signals(force=force)


def run_portfolio_backtest(force: bool = False) -> None:
    """S&P 500 portfolio simulation — incremental or full recompute."""
    from portfolio_backtest import run_portfolio_backtest as _run
    log.info("Portfolio backtest — %s...", "FORCE full recompute" if force else "incremental")
    _run(force=force)


def run_vacuum() -> None:
    """Remove stale parquet files from daily and weekly Delta tables."""
    import delta_utils
    for period in ("daily", "weekly"):
        log.info("Vacuuming %s table...", period)
        try:
            delta_utils.vacuum_table(period, retention_hours=0)
            log.info("%s vacuum complete.", period)
        except Exception as exc:
            log.warning("%s vacuum failed — %s", period, exc)


ACTIONS = {
    "1" : lambda: run_full(force=False),
    "2" : lambda: run_full(force=True),
    "3" : lambda: run_weekly_indicators(force=False),
    "4" : lambda: run_weekly_indicators(force=True),
    "5" : lambda: run_signals(force=False),
    "6" : lambda: run_signals(force=True),
    "7" : lambda: run_weekly_indicators_and_signals(force=False),
    "8" : lambda: run_weekly_indicators_and_signals(force=True),
    "9" : lambda: run_vacuum(),
    "10": lambda: run_portfolio_backtest(force=False),
    "11": lambda: run_portfolio_backtest(force=True),
    "0" : lambda: sys.exit(0),
}


def main() -> None:
    # Allow passing option number as CLI arg (e.g. python run.py 5)
    if len(sys.argv) > 1:
        choice = sys.argv[1].strip()
    else:
        print(MENU)
        choice = input("Select option: ").strip()

    action = ACTIONS.get(choice)
    if action is None:
        print(f"Invalid option: '{choice}'. Choose 0–8.")
        sys.exit(1)

    print()
    t0 = time.time()
    action()
    print(f"\nDone in {time.time() - t0:.1f} s")


if __name__ == "__main__":
    main()

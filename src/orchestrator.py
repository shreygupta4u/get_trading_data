"""
orchestrator.py
===============
Runs candle fetching AND technical analysis for every configured time period,
in the order defined by config.TIME_PERIODS.

Default order:  daily → weekly
    1. Fetch daily candles from yfinance  (incremental, parallel)
    2. Compute daily technicals           (SMA-20/33, EMA-20/33 on daily Close)
    3. Derive weekly candles from daily   (resample, incremental)
    4. Compute weekly technicals          (SMA-20/33, EMA-20/33 on weekly Close)

Why daily before weekly?
    Weekly candles are resampled from the daily Delta table.  Running daily
    first ensures that any new bars fetched today are included in this run's
    weekly derivation.

Incremental behaviour
---------------------
    All steps are incremental.  Each symbol is skipped unless new data
    exists since the last run.  A full restart can be forced with --force.

Usage
-----
    python orchestrator.py                     # run all periods, incremental
    python orchestrator.py --period daily      # run daily only
    python orchestrator.py --period weekly     # run weekly only
    python orchestrator.py --force             # ignore state, recompute all
    python orchestrator.py --dry-run           # show what would run, do nothing
"""

import argparse
import logging
import sys
import time
from datetime import datetime

import config
from fetch_nasdaq_candles import run_all_symbols, setup_logging
from technical_analysis import calculate_indicators

log = setup_logging()


# ── Core runner ───────────────────────────────────────────────────────────────

def run_orchestrator(
    periods:  list[str] | None = None,
    force:    bool = False,
    dry_run:  bool = False,
) -> bool:
    """
    Execute fetch + technicals for each period in order.

    Parameters
    ----------
    periods  : list of periods to run (default: config.TIME_PERIODS)
    force    : pass --force to every step (re-derive / re-fetch everything)
    dry_run  : log what would run but do not execute anything

    Returns True if every step for every period succeeded.
    """
    periods = periods or config.TIME_PERIODS
    overall_start = time.time()
    all_ok = True

    log.info("=" * 65)
    log.info("Orchestrator started  |  periods: %s  |  force=%s",
             periods, force)
    log.info("=" * 65)

    for period in periods:
        period_start = time.time()
        log.info("")
        log.info(">>> Period: %s", period.upper())

        # ── Step 1: Fetch candles ─────────────────────────────────────────────
        step = f"[{period}] fetch"
        log.info("--- %s ---", step)

        if dry_run:
            log.info("  DRY RUN: would call run_all_symbols(period='%s', force=%s)",
                     period, force)
            fetch_ok = True
        else:
            fetch_ok = run_all_symbols(period=period, force=force)
            if not fetch_ok:
                log.error(
                    "%s FAILED — skipping technicals for '%s' period.",
                    step, period,
                )
                all_ok = False
                continue   # Do not compute technicals if candle fetch failed

        # ── Step 2: Calculate technicals ─────────────────────────────────────
        step = f"[{period}] technicals"
        log.info("--- %s ---", step)

        if dry_run:
            log.info("  DRY RUN: would call calculate_indicators(period='%s', force=%s)",
                     period, force)
        else:
            result_df = calculate_indicators(period=period, force=force)
            if result_df.empty:
                # Empty can mean "nothing new to compute" (all skipped) — not an error
                log.info("  %s: no rows written (all symbols up to date or no data).", step)
            else:
                log.info("  %s: %d rows written.", step, len(result_df))

        elapsed = time.time() - period_start
        log.info("<<< Period %s complete in %.1f s", period.upper(), elapsed)

    total_elapsed = time.time() - overall_start
    status = "OK" if all_ok else "PARTIAL FAILURE"
    log.info("")
    log.info("=" * 65)
    log.info("Orchestrator finished  |  status: %s  |  total: %.1f s",
             status, total_elapsed)
    log.info("=" * 65)
    return all_ok


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Orchestrate NASDAQ candle fetch + technical analysis for all periods.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples
--------
  python orchestrator.py                    # full incremental run (daily + weekly)
  python orchestrator.py --period daily     # daily only
  python orchestrator.py --period weekly    # weekly only
  python orchestrator.py --force            # ignore all state, recompute everything
  python orchestrator.py --dry-run          # preview what would run
        """,
    )
    parser.add_argument(
        "--period",
        choices=["daily", "weekly"],
        default=None,
        metavar="PERIOD",
        help="Run only this period (default: all periods in config.TIME_PERIODS).",
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Ignore saved state and recompute / re-fetch everything.",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print what would run without executing anything.",
    )
    return parser.parse_args()


def main() -> None:
    args   = parse_args()
    periods = [args.period] if args.period else None
    ok     = run_orchestrator(periods=periods, force=args.force, dry_run=args.dry_run)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()

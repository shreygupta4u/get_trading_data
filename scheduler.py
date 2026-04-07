"""
scheduler.py
============
Runs the NASDAQ candle fetcher daily at the configured time (default 4:15 PM).
Also performs a catch-up check on startup so missed runs are recovered
automatically after downtime.

Usage
-----
    python scheduler.py                   # start the persistent scheduler
    python scheduler.py --catchup-only    # run catch-up now, then exit
    python scheduler.py --run-now         # fetch today immediately + schedule

Run as a background process
----------------------------
    Windows (PowerShell):
        Start-Process python -ArgumentList "scheduler.py" -WindowStyle Hidden

    Linux / macOS:
        nohup python scheduler.py > logs/scheduler_stdout.log 2>&1 &

Catch-up Logic
--------------
On each startup the scheduler checks the per-symbol state file.  If any
trading day within CATCHUP_DAYS is missing from the state, it fetches
immediately.  Also handles the case where the machine was off during the
scheduled window.
"""

import argparse
import logging
import sys
import time
from datetime import date, datetime, timedelta
from logging.handlers import RotatingFileHandler

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

import config
from fetch_nasdaq_candles import (
    is_trading_day,
    load_symbol_state,
    run_all_symbols,
    setup_logging,
)

# ── Logging ───────────────────────────────────────────────────────────────────
config.LOG_DIR.mkdir(parents=True, exist_ok=True)

log = logging.getLogger("nasdaq_scheduler")
log.setLevel(getattr(logging, config.LOG_LEVEL))

if not log.handlers:
    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    fh = RotatingFileHandler(
        config.LOG_DIR / "scheduler.log",
        maxBytes=config.LOG_ROTATION_BYTES,
        backupCount=config.LOG_BACKUP_COUNT,
    )
    fh.setFormatter(fmt)
    log.addHandler(fh)

    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    log.addHandler(ch)


# ── Catch-up helpers ──────────────────────────────────────────────────────────

def _has_data_for_date(d: date) -> bool:
    """
    Return True if at least one symbol in the state file has data on or
    after date *d*.  A symbol being present with a date >= d means the
    fetch for that day ran successfully.
    """
    state = load_symbol_state()
    if not state:
        return False
    return any(date.fromisoformat(v) >= d for v in state.values())


def missing_trading_days(lookback: int = config.CATCHUP_DAYS) -> list[date]:
    """
    Return a sorted list of trading days (Mon–Fri) in the past `lookback`
    calendar days that are missing from the symbol state.
    Excludes today (handled separately by maybe_run_today).
    """
    today = date.today()
    missing = []
    for offset in range(1, lookback + 1):      # start at yesterday
        d = today - timedelta(days=offset)
        if is_trading_day(d) and not _has_data_for_date(d):
            missing.append(d)
    missing.sort()
    return missing


def run_catchup() -> None:
    """Back-fill any missing trading days found within CATCHUP_DAYS."""
    missed = missing_trading_days()
    if not missed:
        log.info("Catch-up check: no missing days found.")
        return

    start = missed[0]
    end   = missed[-1]
    log.info(
        "Catch-up: fetching %d missed trading day(s)  (%s to %s)",
        len(missed), start, end,
    )
    run_all_symbols(override_start=start, override_end=end)


def maybe_run_today() -> None:
    """
    If today's data is missing and we are past the scheduled time, fetch now.
    Covers the case where the machine was off during the cron window.
    """
    today = date.today()
    now   = datetime.now()

    if not is_trading_day(today):
        return

    scheduled_time = now.replace(
        hour=config.SCHEDULE_HOUR,
        minute=config.SCHEDULE_MINUTE,
        second=0,
        microsecond=0,
    )

    if now >= scheduled_time and not _has_data_for_date(today):
        log.info(
            "Today (%s) data is missing and it is past %02d:%02d — fetching now.",
            today, config.SCHEDULE_HOUR, config.SCHEDULE_MINUTE,
        )
        run_all_symbols()


# ── Scheduled job ─────────────────────────────────────────────────────────────

def scheduled_job() -> None:
    """Entry point called by APScheduler at the configured daily time."""
    log.info("=== Scheduled job triggered ===")
    success = run_all_symbols()
    if success:
        log.info("Scheduled job finished successfully.")
    else:
        log.error("Scheduled job finished with errors — check fetch.log.")


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="NASDAQ candle scheduler.")
    p.add_argument(
        "--catchup-only",
        action="store_true",
        help="Run catch-up logic and exit without starting the scheduler.",
    )
    p.add_argument(
        "--run-now",
        action="store_true",
        help="Fetch today immediately, then start the recurring schedule.",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()

    log.info("=" * 60)
    log.info("NASDAQ Candle Scheduler starting up")
    log.info(
        "Scheduled daily run: %02d:%02d local time",
        config.SCHEDULE_HOUR, config.SCHEDULE_MINUTE,
    )
    log.info("Data directory: %s", config.DATA_DIR.resolve())

    # 1. Always check for missed days on startup
    run_catchup()
    maybe_run_today()

    if args.catchup_only:
        log.info("--catchup-only requested — exiting.")
        return

    if args.run_now:
        log.info("--run-now requested — fetching now...")
        run_all_symbols()

    # 2. Start APScheduler with a daily cron trigger
    scheduler = BlockingScheduler(timezone="local")
    scheduler.add_job(
        scheduled_job,
        trigger=CronTrigger(
            hour=config.SCHEDULE_HOUR,
            minute=config.SCHEDULE_MINUTE,
        ),
        id="daily_nasdaq_fetch",
        name="Fetch NASDAQ Daily Candles",
        max_instances=1,          # prevent overlap if a run takes too long
        coalesce=True,            # merge missed fires into one
        misfire_grace_time=3600,  # tolerate up to 1-hour late start
    )

    log.info(
        "Scheduler started. Next run at %02d:%02d. Press Ctrl+C to stop.",
        config.SCHEDULE_HOUR, config.SCHEDULE_MINUTE,
    )

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        log.info("Scheduler stopped by user.")


if __name__ == "__main__":
    main()

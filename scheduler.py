"""
scheduler.py — Nightly job runner.

In production the scheduler runs as a background thread inside the Flask
process (started by app.py).  This file can also be run directly for manual
or standalone use:

    python scheduler.py              # Run on schedule (11:00 PM nightly)
    python scheduler.py --run-now   # Run immediately (manual test / Railway one-off)
    python scheduler.py --date 2025-03-15  # Back-fill a specific date
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time

import schedule
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

import database as db
import scraper as sc
import emailer as em
import d1baseball


# ---------------------------------------------------------------------------
# Core job — importable by app.py's background thread
# ---------------------------------------------------------------------------

def run_nightly_job(target_date: str | None = None) -> None:
    """Scrape stats and send emails for target_date (defaults to yesterday)."""
    logger.info("=" * 60)
    logger.info("Nightly job starting. Target date: %s", target_date or "yesterday")
    logger.info("=" * 60)

    # 0 — Clear D1Baseball cache so season stats are fetched fresh each run
    d1baseball.clear_cache()

    # 1 — Scrape
    failures: list[dict] = []
    try:
        saved = sc.scrape_all_players()
        logger.info("Scraping done. %d game entries saved.", saved)
        # Collect players whose scrape_status is 'failed' after the job
        for player in db.get_all_players():
            if player.get("scrape_status") == "failed":
                failures.append({
                    "name": player["name"],
                    "school": player["school"],
                    "error": player.get("scrape_error") or "Unknown error",
                })
    except Exception as exc:
        logger.exception("Scraping failed with unexpected error: %s", exc)

    # 2 — Email agents
    try:
        sent = em.send_nightly_emails(target_date=target_date)
        logger.info("Email job done. %d email(s) sent.", sent)
    except Exception as exc:
        logger.exception("Email job failed with unexpected error: %s", exc)

    # 3 — Alert admin of any scrape failures
    if failures:
        logger.warning("%d player(s) had scrape errors — sending alert.", len(failures))
        try:
            em.send_scrape_error_alert(failures)
        except Exception as exc:
            logger.error("Could not send scrape error alert: %s", exc)

    logger.info("Nightly job finished.")


# ---------------------------------------------------------------------------
# Standalone entry point
# ---------------------------------------------------------------------------

def main() -> None:
    # Only configure logging when run directly (app.py sets it up otherwise)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    parser = argparse.ArgumentParser(
        description="NCAA Player Tracker — nightly scheduler"
    )
    parser.add_argument(
        "--run-now",
        action="store_true",
        help="Run the scrape+email job immediately instead of waiting for schedule",
    )
    parser.add_argument(
        "--date",
        metavar="YYYY-MM-DD",
        default=None,
        help="Manually specify the game date to report (default: yesterday)",
    )
    args = parser.parse_args()

    db.init_db()

    if args.run_now or args.date:
        logger.info("Manual run triggered.")
        run_nightly_job(target_date=args.date)
        return

    RUN_AT = os.environ.get("NIGHTLY_RUN_AT", "23:00")
    logger.info("Standalone scheduler started. Job will run nightly at %s UTC.", RUN_AT)
    schedule.every().day.at(RUN_AT).do(run_nightly_job)

    while True:
        schedule.run_pending()
        time.sleep(30)


if __name__ == "__main__":
    main()

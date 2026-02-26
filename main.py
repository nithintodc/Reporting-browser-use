#!/usr/bin/env python3
"""
Orchestrator: runs DoorDash workflow via browser-use (login, reports, download, campaign),
then runs analysis agents and combined report. No Playwright; browser-use drives the browser.
"""

import asyncio
import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv

from agents.doordash_agent import run_reports_then_analysis_then_campaign
from agents.marketing_agent import run as marketing_run
from agents.analysis_agent import run as analysis_run
from agents.google_pusher_agent import run as google_pusher_run
from agents.combined_report_agent import run as combined_report_run

# Load environment variables from .env
load_dotenv()

# Structured logging
LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
DOWNLOADS_ROOT = Path(__file__).resolve().parent / "downloads"


def _run_dir_for_email(email: str) -> Path:
    """downloads/{email_sanitized}-{timestamp} so data is clean per run."""
    safe = (email or "run").strip()
    for c in ("@", ".", " ", "/", "\\"):
        safe = safe.replace(c, "_")
    safe = safe[:50] if len(safe) > 50 else safe
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return DOWNLOADS_ROOT / f"{safe}-{timestamp}"

# Retry configuration
MAX_RETRIES = 3
RETRY_DELAY_SEC = 5


def setup_logging(level: int = logging.INFO) -> None:
    """Configure structured logging to stderr."""
    logging.basicConfig(
        level=level,
        format=LOG_FORMAT,
        datefmt=LOG_DATE_FORMAT,
        stream=sys.stderr,
        force=True,
    )


def get_required_env(name: str) -> str:
    """Return environment variable or exit with error."""
    value = os.getenv(name)
    if not value or not value.strip():
        logging.error("Missing required environment variable: %s. Copy .env.example to .env and fill values.", name)
        sys.exit(1)
    return value.strip()


def get_optional_env(name: str, default: str = "") -> str:
    """Return environment variable or default."""
    value = os.getenv(name)
    return value.strip() if value else default


def get_last_three_months_date_range():
    """
    Return (start_date, end_date) as MM/DD/YYYY for the 3 months previous to current month.
    Example: if today is Feb 2026 → start 11/01/2025, end 01/31/2026 (Nov, Dec, Jan).
    """
    today = datetime.now().date()
    first_this_month = today.replace(day=1)
    last_prev_month = first_this_month - timedelta(days=1)  # last day of previous month
    # First day of month 3 months before current month (e.g. Feb → Nov previous year)
    y, m = first_this_month.year, first_this_month.month
    m -= 3
    if m <= 0:
        m += 12
        y -= 1
    first_three_months_ago = datetime(y, m, 1).date()
    start_str = first_three_months_ago.strftime("%m/%d/%Y")
    end_str = last_prev_month.strftime("%m/%d/%Y")
    return start_str, end_str


async def _analysis_phase(
    marketing_path: Path | None,
    financial_path: Path | None,
    run_dir: Path,
    report_start_date: str,
    report_end_date: str,
) -> Path | None:
    """Run Financial + Marketing analysis and combined report (called while browser is paused). Returns combined_path for campaign combos."""
    logger = logging.getLogger("main")
    if not marketing_path and not financial_path:
        raise RuntimeError("DoorDash (browser-use) did not return any downloaded file path")

    financial_sheets = None
    marketing_sheets = None

    if marketing_path:
        logger.info("Marketing report: %s", marketing_path)
        try:
            result = marketing_run(
                Path(marketing_path),
                output_dir=run_dir,
                post_start_date=report_start_date,
                post_end_date=report_end_date,
                operator_name=get_optional_env("OPERATOR_NAME"),
                write_file=False,
            )
            if isinstance(result, list):
                marketing_sheets = result
                logger.info("MarketingAgent built %s sheets", len(marketing_sheets))
        except Exception as marketing_err:
            logger.warning("MarketingAgent failed (non-fatal): %s", marketing_err)

    if financial_path:
        logger.info("Financial report: %s", financial_path)
        dl_path = Path(financial_path)
        is_zip = dl_path.suffix.lower() == ".zip"
        if not is_zip and dl_path.is_file() and dl_path.stat().st_size >= 4:
            with open(dl_path, "rb") as f:
                is_zip = f.read(4) == b"PK\x03\x04"
        if is_zip:
            try:
                result = analysis_run(
                    dl_path,
                    output_dir=run_dir,
                    report_start_date=report_start_date,
                    report_end_date=report_end_date,
                    operator_name=get_optional_env("OPERATOR_NAME"),
                    write_file=False,
                )
                if isinstance(result, list):
                    financial_sheets = result
                    logger.info("AnalysisAgent built %s sheets", len(financial_sheets))
            except Exception as analysis_err:
                logger.warning("AnalysisAgent failed (non-fatal): %s", analysis_err)

    combined_path = None
    if financial_sheets or marketing_sheets:
        try:
            combined_path = combined_report_run(
                financial_sheets=financial_sheets,
                marketing_sheets=marketing_sheets,
                output_dir=run_dir,
            )
            if combined_path:
                logger.info("Combined report: %s", combined_path)
        except Exception as comb_err:
            logger.warning("Combined report failed (non-fatal): %s", comb_err)

    if combined_path:
        try:
            result = google_pusher_run(
                financial_xlsx_path=combined_path,
                marketing_xlsx_path=None,
                spreadsheet_title=f"DoorDash Reports {report_start_date} to {report_end_date}",
            )
            if result:
                logger.info("GooglePusherAgent: Pushed to %s", result.get("spreadsheet_url"))
        except Exception as push_err:
            logger.warning("GooglePusherAgent failed (non-fatal): %s", push_err)

    if combined_path:
        logger.info("Combined analysis written to %s (used for campaign combos from Day-Slot sheets).", combined_path)
    else:
        logger.warning("Combined analysis file was not created; campaign combos will be empty. Check financial/marketing report paths.")
    return combined_path


async def run_workflow() -> None:
    """Single browser session: login → reports → download → (browser stays open) → analysis → campaign (no second login) → close."""
    logger = logging.getLogger("main")

    doordash_email = get_required_env("DOORDASH_EMAIL")
    doordash_password = get_required_env("DOORDASH_PASSWORD")
    get_required_env("BROWSER_USE_API_KEY")

    run_dir = _run_dir_for_email(doordash_email)
    run_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Run directory: %s", run_dir)

    report_start_date, report_end_date = get_last_three_months_date_range()
    logger.info("Report date range (last 3 months): %s to %s", report_start_date, report_end_date)

    last_error: Exception | None = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info("Attempt %d/%d", attempt, MAX_RETRIES)

            async def analysis_callback(m_path, f_path):
                return await _analysis_phase(m_path, f_path, run_dir, report_start_date, report_end_date)

            await run_reports_then_analysis_then_campaign(
                download_dir=run_dir,
                email=doordash_email,
                password=doordash_password,
                start_date=report_start_date,
                end_date=report_end_date,
                analysis_callback=analysis_callback,
            )
            logger.info("Campaign creation completed.")
            return

        except Exception as e:
            last_error = e
            logger.warning("Attempt %d failed: %s", attempt, e, exc_info=True)
            if attempt < MAX_RETRIES:
                logger.info("Retrying in %s seconds...", RETRY_DELAY_SEC)
                await asyncio.sleep(RETRY_DELAY_SEC)
            else:
                break

    if last_error:
        logger.error("All retries exhausted: %s", last_error)
        sys.exit(1)


def main() -> None:
    """Entry point: setup logging and run async workflow."""
    setup_logging()
    asyncio.run(run_workflow())


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Orchestrator: starts browser, runs Gmail login, runs DoorDash workflow,
closes browser gracefully. Uses structured logging and retries.
"""

import asyncio
import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv

from agents.browser_manager import BrowserManager
from agents.gmail_agent import GmailAgent
from agents.doordash_agent import DoorDashAgent
from agents.marketing_agent import run as marketing_run
from agents.analysis_agent import run as analysis_run
from agents.google_pusher_agent import run as google_pusher_run
from agents.combined_report_agent import run as combined_report_run
from agents.marketing_campaign_agent import MarketingCampaignAgent

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


async def run_workflow() -> None:
    """Run full workflow: Gmail login (or use existing session), DoorDash marketing report download (last 3 months), marketing analysis, save to downloads."""
    logger = logging.getLogger("main")
    use_local_browser = os.getenv("USE_LOCAL_BROWSER", "").strip().lower() in ("1", "true", "yes")
    cdp_url = get_optional_env("LOCAL_BROWSER_CDP_URL", "http://localhost:9222")

    if use_local_browser:
        gmail_email = get_optional_env("GMAIL_EMAIL")
        gmail_password = get_optional_env("GMAIL_PASSWORD")
        doordash_email = get_required_env("DOORDASH_EMAIL")
        doordash_password = get_required_env("DOORDASH_PASSWORD")
        logger.info("Using local browser at %s (Gmail login skipped)", cdp_url)
    else:
        gmail_email = get_required_env("GMAIL_EMAIL")
        gmail_password = get_required_env("GMAIL_PASSWORD")
        doordash_email = get_required_env("DOORDASH_EMAIL")
        doordash_password = get_required_env("DOORDASH_PASSWORD")

    run_dir = _run_dir_for_email(gmail_email or doordash_email)
    run_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Run directory: %s", run_dir)

    browser = BrowserManager(
        headless=False,
        download_dir=run_dir,
        use_local_browser=use_local_browser,
        cdp_url=cdp_url if use_local_browser else None,
    )
    last_error: Exception | None = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info("Attempt %d/%d", attempt, MAX_RETRIES)
            await browser.start()

            if use_local_browser:
                # Local browser: open only DoorDash (no Gmail tab). 2FA must be entered manually if prompted.
                logger.info("Opening DoorDash merchant portal...")
                doordash_page = await browser.new_tab()
                async def get_otp_no_gmail():
                    return None  # No Gmail agent; enter 2FA manually if required
                get_otp = get_otp_no_gmail
            else:
                # Fresh browser: Gmail tab for OTP, then DoorDash
                logger.info("Opening Gmail tab (for OTP when needed)...")
                gmail_page = await browser.new_tab()
                gmail_agent = GmailAgent(
                    gmail_page,
                    gmail_email,
                    gmail_password,
                    skip_login=False,
                )
                await gmail_agent.login()
                logger.info("Opening DoorDash merchant portal...")
                doordash_page = await browser.new_tab()
                async def get_otp():
                    return await gmail_agent.get_otp_from_latest_doordash_email()
                get_otp = get_otp

            doordash_agent = DoorDashAgent(
                doordash_page,
                doordash_email,
                doordash_password,
                get_otp_callback=get_otp,
                download_dir=run_dir,
            )
            report_start_date, report_end_date = get_last_three_months_date_range()
            logger.info("Report date range (last 3 months): %s to %s", report_start_date, report_end_date)
            marketing_path, financial_path = await doordash_agent.run(
                start_date=report_start_date,
                end_date=report_end_date,
            )

            if not marketing_path and not financial_path:
                raise RuntimeError("DoorDash agent did not return any downloaded file path")

            # Run analyses in memory (no separate financial/marketing files); build one combined workbook
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

            # Write single combined analysis file (financial + marketing sheets)
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

            # Push combined report to Google Sheets
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

            # Run marketing campaign setup (new users campaign: discount %, min subtotal, Breakfast + Monday, TODC-test)
            try:
                discount_pct = int(get_optional_env("CAMPAIGN_DISCOUNT_PCT") or "20")
                min_subtotal = float(get_optional_env("CAMPAIGN_MIN_SUBTOTAL") or "20")
                campaign_name = get_optional_env("CAMPAIGN_NAME") or "TODC-test"
                campaign_agent = MarketingCampaignAgent(doordash_page)
                ok = await campaign_agent.run(
                    discount_pct=discount_pct,
                    min_subtotal=min_subtotal,
                    campaign_name=campaign_name,
                )
                if ok:
                    logger.info("MarketingCampaignAgent: Campaign setup completed")
                else:
                    logger.warning("MarketingCampaignAgent: Campaign setup did not complete (check UI/selectors)")
            except Exception as campaign_err:
                logger.warning("MarketingCampaignAgent failed (non-fatal): %s", campaign_err)

            await browser.close()
            return

        except Exception as e:
            last_error = e
            logger.warning("Attempt %d failed: %s", attempt, e, exc_info=True)
            try:
                await browser.close()
            except Exception:
                pass
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
    if os.getenv("MARKETING_CAMPAIGN_DEBUG", "").strip().lower() in ("1", "true", "yes"):
        logging.getLogger("agents.marketing_campaign_agent").setLevel(logging.DEBUG)
    asyncio.run(run_workflow())


if __name__ == "__main__":
    main()

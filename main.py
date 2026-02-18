#!/usr/bin/env python3
"""
Orchestrator: starts browser, runs Gmail login, runs DoorDash workflow,
closes browser gracefully. Uses structured logging and retries.
"""

import asyncio
import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

from agents.browser_manager import BrowserManager
from agents.gmail_agent import GmailAgent
from agents.doordash_agent import DoorDashAgent
from agents.report_storage_agent import ReportStorageAgent

# Load environment variables from .env
load_dotenv()

# Structured logging
LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
DOWNLOAD_DIR = Path(__file__).resolve().parent / "downloads"

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


async def run_workflow() -> None:
    """Run full workflow: Gmail login (or use existing session), DoorDash report download, storage."""
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

    browser = BrowserManager(
        headless=False,
        download_dir=DOWNLOAD_DIR,
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
                download_dir=DOWNLOAD_DIR,
            )
            downloaded_path = await doordash_agent.run(
                start_date="01/01/2026",
                end_date="01/31/2026",
            )

            if not downloaded_path:
                raise RuntimeError("DoorDash agent did not return a downloaded file path")

            storage_agent = ReportStorageAgent(DOWNLOAD_DIR)
            final_path = storage_agent.process(downloaded_path)
            logger.info("Workflow completed. Report: %s", final_path)
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
    asyncio.run(run_workflow())


if __name__ == "__main__":
    main()

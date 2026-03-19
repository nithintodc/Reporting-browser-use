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
from agents.slack_log_notifier import install_slack_log_notifier

# Load environment variables from .env
load_dotenv()

# Structured logging
LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
DOWNLOADS_ROOT = Path(__file__).resolve().parent / "downloads"
LOGS_DIR = Path(__file__).resolve().parent / "logs"


def _run_dir_for_email(email: str) -> Path:
    """downloads/{email_sanitized}-{timestamp} so data is clean per run."""
    safe = (email or "run").strip()
    for c in ("@", ".", " ", "/", "\\"):
        safe = safe.replace(c, "_")
    safe = safe[:50] if len(safe) > 50 else safe
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return DOWNLOADS_ROOT / f"{safe}-{timestamp}"

# Retry configuration — base delay doubles each attempt: 5s, 10s, 20s, ...
MAX_RETRIES = 3
RETRY_BASE_DELAY_SEC = 5


def setup_logging(level: int = logging.INFO) -> logging.FileHandler:
    """Configure structured logging to stderr AND a timestamped log file in logs/."""
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = LOGS_DIR / f"run_{timestamp}.log"

    # File handler — captures everything (DEBUG and above)
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(LOG_FORMAT, datefmt=LOG_DATE_FORMAT))

    # Console handler — INFO and above (same as before)
    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setLevel(level)
    console_handler.setFormatter(logging.Formatter(LOG_FORMAT, datefmt=LOG_DATE_FORMAT))

    # Configure root logger with both handlers
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    root.handlers.clear()
    root.addHandler(console_handler)
    root.addHandler(file_handler)

    # Also create/update a symlink for easy access: logs/latest.log -> run_XXXX.log
    latest_link = LOGS_DIR / "latest.log"
    try:
        if latest_link.is_symlink() or latest_link.exists():
            latest_link.unlink()
        latest_link.symlink_to(log_file.name)
    except OSError:
        pass  # symlinks may not work on all systems

    logging.info("Log file: %s", log_file)
    return file_handler


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


def _run_marketing(marketing_path: Path, run_dir: Path, report_start_date: str, report_end_date: str, operator_name: str):
    """Blocking marketing analysis — intended to be run via asyncio.to_thread."""
    try:
        result = marketing_run(
            marketing_path,
            output_dir=run_dir,
            post_start_date=report_start_date,
            post_end_date=report_end_date,
            operator_name=operator_name,
            write_file=False,
        )
        return result if isinstance(result, list) else None
    except Exception as e:
        logging.getLogger("main").warning("MarketingAgent failed (non-fatal): %s", e)
        return None


def _run_financial(financial_path: Path, run_dir: Path, report_start_date: str, report_end_date: str, operator_name: str):
    """Blocking financial analysis — intended to be run via asyncio.to_thread."""
    logger = logging.getLogger("main")
    dl_path = Path(financial_path)
    is_zip = dl_path.suffix.lower() == ".zip"
    if not is_zip and dl_path.is_file() and dl_path.stat().st_size >= 4:
        with open(dl_path, "rb") as f:
            is_zip = f.read(4) == b"PK\x03\x04"
    if not is_zip:
        logger.warning("FinancialAgent: file is not a ZIP, skipping: %s", dl_path)
        return None
    try:
        result = analysis_run(
            dl_path,
            output_dir=run_dir,
            report_start_date=report_start_date,
            report_end_date=report_end_date,
            operator_name=operator_name,
            write_file=False,
        )
        return result if isinstance(result, list) else None
    except Exception as e:
        logging.getLogger("main").warning("AnalysisAgent failed (non-fatal): %s", e)
        return None


async def _analysis_phase(
    marketing_path: Path | None,
    financial_path: Path | None,
    run_dir: Path,
    report_start_date: str,
    report_end_date: str,
) -> Path | None:
    """Run Financial + Marketing analysis in parallel, then build combined report. Returns combined_path."""
    logger = logging.getLogger("main")
    if not marketing_path and not financial_path:
        raise RuntimeError("DoorDash (browser-use) did not return any downloaded file path")

    operator_name = get_optional_env("OPERATOR_NAME")

    # Run both analyses concurrently — they are pure CPU/IO and don't share state
    marketing_task = (
        asyncio.to_thread(_run_marketing, Path(marketing_path), run_dir, report_start_date, report_end_date, operator_name)
        if marketing_path else asyncio.sleep(0, result=None)
    )
    financial_task = (
        asyncio.to_thread(_run_financial, Path(financial_path), run_dir, report_start_date, report_end_date, operator_name)
        if financial_path else asyncio.sleep(0, result=None)
    )

    if marketing_path:
        logger.info("Marketing report: %s", marketing_path)
    if financial_path:
        logger.info("Financial report: %s", financial_path)

    marketing_sheets, financial_sheets = await asyncio.gather(marketing_task, financial_task)

    if marketing_sheets:
        logger.info("MarketingAgent built %s sheets", len(marketing_sheets))
    if financial_sheets:
        logger.info("AnalysisAgent built %s sheets", len(financial_sheets))

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


def _sanitize_email(email: str) -> str:
    """Sanitize email for use in folder names."""
    safe = (email or "run").strip()
    for c in ("@", ".", " ", "/", "\\"):
        safe = safe.replace(c, "_")
    return safe[:50] if len(safe) > 50 else safe


def _find_latest_run_with_pending_campaigns(email: str) -> tuple[Path | None, Path | None]:
    """
    Find the latest existing run folder for this email that has a combined_analysis
    with Campaign Mappings containing non-Successful campaigns.
    Returns (run_dir, combined_path) or (None, None) if not found.
    """
    import re
    from agents.combined_report_agent import read_campaign_combos_from_mappings

    safe = _sanitize_email(email)

    for folder in sorted(DOWNLOADS_ROOT.glob(f"{safe}-*"), reverse=True):
        if not folder.is_dir():
            continue
        if not re.match(rf"^{re.escape(safe)}-\d{{8}}_\d{{6}}$", folder.name):
            continue
        combined_files = sorted(folder.glob("combined_analysis_*.xlsx"), reverse=True)
        if not combined_files:
            continue
        combined_path = combined_files[0]
        combos = read_campaign_combos_from_mappings(combined_path)
        if not combos:
            continue
        pending = [c for c in combos if c.get("status") != "Successful"]
        if pending:
            return folder, combined_path
    return None, None


async def run_workflow() -> None:
    """Single browser session: login → reports → download → (browser stays open) → analysis → campaign (no second login) → close."""
    logger = logging.getLogger("main")

    doordash_email = get_required_env("DOORDASH_EMAIL")
    doordash_password = get_required_env("DOORDASH_PASSWORD")
    get_required_env("BROWSER_USE_API_KEY")

    report_start_date, report_end_date = get_last_three_months_date_range()
    logger.info("Report date range (last 3 months): %s to %s", report_start_date, report_end_date)

    # --- Auto-detect campaigns-only mode ---
    # If an existing run folder has pending (non-Successful) campaigns,
    # reuse that folder and skip report download + analysis.
    # Set FORCE_FULL_RUN=true to override and always run the full pipeline.
    force_full = os.getenv("FORCE_FULL_RUN", "").strip().lower() in ("1", "true", "yes")
    campaigns_only_combined_path: Path | None = None
    run_dir: Path | None = None

    if not force_full:
        existing_dir, existing_combined = _find_latest_run_with_pending_campaigns(doordash_email)
        if existing_dir and existing_combined:
            from agents.combined_report_agent import read_campaign_combos_from_mappings
            all_combos = read_campaign_combos_from_mappings(existing_combined)
            total = len(all_combos)
            successful = sum(1 for c in all_combos if c.get("status") == "Successful")
            pending = total - successful
            run_dir = existing_dir
            campaigns_only_combined_path = existing_combined
            logger.info(
                "AUTO-RESUME: Found existing run with %d/%d campaigns pending in %s",
                pending, total, existing_dir.name,
            )
            logger.info("AUTO-RESUME: Using combined analysis %s", campaigns_only_combined_path)
            logger.info("AUTO-RESUME: Skipping report download + analysis, going straight to campaigns")
            logger.info("AUTO-RESUME: Set FORCE_FULL_RUN=true to override and run full pipeline")

    if run_dir is None:
        run_dir = _run_dir_for_email(doordash_email)
        run_dir.mkdir(parents=True, exist_ok=True)

    logger.info("Run directory: %s", run_dir)

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
                campaigns_only_combined_path=campaigns_only_combined_path,
            )
            logger.info("Campaign creation completed.")
            return

        except Exception as e:
            last_error = e
            logger.warning("Attempt %d failed: %s", attempt, e, exc_info=True)
            if attempt < MAX_RETRIES:
                delay = RETRY_BASE_DELAY_SEC * (2 ** (attempt - 1))  # 5s, 10s, 20s, ...
                logger.info("Retrying in %s seconds (attempt %d/%d)...", delay, attempt + 1, MAX_RETRIES)
                await asyncio.sleep(delay)
            else:
                break

    if last_error:
        logger.error("All retries exhausted: %s", last_error)
        sys.exit(1)


def main() -> None:
    """Entry point: setup logging and run async workflow."""
    setup_logging()
    install_slack_log_notifier()  # send Slack when terminal/log signals appear (e.g. "Login was successful")
    asyncio.run(run_workflow())


if __name__ == "__main__":
    main()

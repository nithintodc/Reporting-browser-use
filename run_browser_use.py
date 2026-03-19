#!/usr/bin/env python3
"""
Optional: Run DoorDash report download using browser-use (AI-driven control).
Uses an LLM to interpret the page and perform login, navigation, and download.

Set OPENAI_API_KEY in .env (recommended; works on restricted networks).
Alternatively set BROWSER_USE_API_KEY to use Browser Use Cloud (may fail with
"nodename nor servname provided, or not known" if DNS/network blocks it).
"""

import asyncio
import os
from datetime import datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


def _get_date_range() -> tuple[str, str]:
    """
    Return (start_date, end_date) as MM/DD/YYYY.
    Reads REPORT_START_DATE / REPORT_END_DATE from env if set,
    otherwise computes the last 3 full months automatically.
    """
    start = os.getenv("REPORT_START_DATE", "").strip()
    end = os.getenv("REPORT_END_DATE", "").strip()
    if start and end:
        return start, end
    today = datetime.now().date()
    first_this_month = today.replace(day=1)
    last_prev_month = first_this_month - timedelta(days=1)
    y, m = first_this_month.year, first_this_month.month
    m -= 3
    if m <= 0:
        m += 12
        y -= 1
    first_three_months_ago = datetime(y, m, 1).date()
    return first_three_months_ago.strftime("%m/%d/%Y"), last_prev_month.strftime("%m/%d/%Y")

DOWNLOAD_DIR = Path(__file__).resolve().parent / "downloads"
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)


def _get_llm():
    """Prefer OpenAI (works on most networks); fall back to Browser Use Cloud if no OpenAI key."""
    try:
        from browser_use import ChatBrowserUse, ChatOpenAI
    except ImportError:
        raise SystemExit("Install browser-use: pip install browser-use")

    # Prefer OpenAI so it works when Browser Use Cloud is unreachable (DNS/firewall)
    if os.getenv("OPENAI_API_KEY"):
        return ChatOpenAI(model="gpt-4o-mini")
    if os.getenv("BROWSER_USE_API_KEY"):
        return ChatBrowserUse()
    raise SystemExit(
        "Set OPENAI_API_KEY in .env (recommended) or BROWSER_USE_API_KEY for browser-use mode."
    )


def _get_browser():
    """
    Browser with download path set to project downloads/.
    Supports remote CDP (GCP headless Chrome), Multilogin, or local Chrome.
    """
    from browser_use import Browser

    downloads_path = str(DOWNLOAD_DIR.resolve())

    # Remote CDP (GCP headless Chrome, Browserless, etc.)
    cdp_url = os.environ.get("LOCAL_BROWSER_CDP_URL", "").strip()
    if cdp_url:
        return Browser(
            cdp_url=cdp_url,
            downloads_path=downloads_path,
            enable_default_extensions=False,
        )

    # Local Chrome with persistent profile (laptop fallback)
    raw_dir = os.environ.get("CHROME_USER_DATA_DIR", "").strip()
    if raw_dir:
        user_data_dir = str(Path(raw_dir).expanduser().resolve())
    else:
        user_data_dir = str(Path(__file__).resolve().parent / ".cursor" / "chrome-debug-profile")

    common = dict(
        user_data_dir=user_data_dir,
        downloads_path=downloads_path,
        enable_default_extensions=False,
    )
    if os.name == "posix" and Path("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome").exists():
        return Browser(executable_path="/Applications/Google Chrome.app/Contents/MacOS/Google Chrome", **common)
    return Browser(**common)


async def main():
    email = os.getenv("DOORDASH_EMAIL", "").strip()
    password = os.getenv("DOORDASH_PASSWORD", "").strip()
    if not email or not password:
        raise SystemExit("Set DOORDASH_EMAIL and DOORDASH_PASSWORD in .env")

    start_date, end_date = _get_date_range()

    task = (
        "Log in to the DoorDash Merchant Portal at https://merchant-portal.doordash.com/merchant/ "
        f"using this email: {email} and this password: {password}. "
        "Then open the Reports section, create a new report, select Financial report, "
        f"set the date range from {start_date} to {end_date}, create the report, "
        "wait for it to finish generating, then click Download. "
        f"Ensure the file is saved to this folder: {DOWNLOAD_DIR.resolve()}."
    )

    from browser_use import Agent

    llm = _get_llm()
    browser = _get_browser()
    agent = Agent(task=task, llm=llm, browser=browser)
    await agent.run()


if __name__ == "__main__":
    asyncio.run(main())

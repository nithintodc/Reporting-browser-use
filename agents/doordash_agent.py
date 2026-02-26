"""
DoorDash Merchant Portal automation using the browser-use framework.
Runs the full workflow: login, financial report, marketing report, download(s), and campaign creation.
Returns paths to downloaded report file(s) for use by analysis_agent and marketing_agent.
"""

import logging
import os
from pathlib import Path
from typing import Optional, Tuple

logger = logging.getLogger(__name__)


def get_task_description(
    email: str,
    password: str,
    start_date: str,
    end_date: str,
    store_search: str = "1864",
    store_name: str = "McDonald's (1864 - PINE LAKE ROAD)",
    campaign_name: str = "1864-Tue",
) -> str:
    """Build the agent task with credentials and date range."""
    if not password:
        raise ValueError("DOORDASH_PASSWORD is not set. Add it to your .env file (see .env.example).")

    return f"""
You are automating the DoorDash Merchant Portal. Complete the following steps in order.
Wait for the page to load after each action before proceeding. If a modal or overlay appears, interact with it as described.

=== STEP 0: Navigate and log in (DO THIS EXACT ORDER — two-step login) ===
The login has TWO steps. Do NOT enter the password in the email field. Do NOT click "Log In" until the password screen is visible.

1. Go to exactly this URL: https://merchant-portal.doordash.com/merchant/login
2. On the first screen you see: find the EMAIL input field (labeled "Email"). Enter ONLY the email address, exactly: {email}
3. Click the "Continue to Log In" button (the red button). Wait for the page to change.
4. On the NEXT screen (after Continue to Log In): find the PASSWORD input field. Enter ONLY the password there: {password}
5. Click the "Log In" button. Wait until the dashboard or main merchant view has loaded (you should remain on merchant-portal.doordash.com).

Important: The email field and password field appear on different steps. First screen = email + "Continue to Log In". Second screen = password + "Log In".

=== STEP 1: Generate Financial Report ===
5. On the dashboard, locate the LEFT SIDEBAR. Click on "Reports" in the sidebar.
6. On the Reports page, find and click the "Create report" button (typically in the top right area).
7. A modal will appear: "Choose a report type". Select the "Financial report" RADIO BUTTON (click it), then click "Next".
8. Under "Choose a time range", select "By date range".
9. Set Start date to: {start_date}
10. Set End date to: {end_date}
11. Click the "Create report" button at the bottom of the modal. Wait for the modal to close and the new report to appear in the reports list.

=== STEP 2: Generate Marketing Report ===
12. Click "Create report" again.
13. In "Choose a report type", select the "Marketing report" RADIO BUTTON, then click "Next".
14. Under "Channels": UNCHECK "Online Ordering". Leave "Marketplace" CHECKED.
15. Under "Choose a time range", select "By date range".
16. Set Start date to: {start_date} and End date to: {end_date}.
17. Click "Create report" at the bottom. Wait for the report to appear in the list.

=== STEP 3: Download the Financial Report ===
18. On the Reports page, find the recently created "Financials" (or "Financial") report in the table/list.
19. Find the DOWNLOAD icon (downward arrow) next to that specific report and click it to download the CSV. Wait for the download to complete if possible.

=== STEP 4: Download the Marketing Report ===
20. In the same Reports list, find the recently created "Marketing" report.
21. Find the DOWNLOAD icon next to that report and click it to download. Wait for the download to complete if possible.

=== STEP 5: Create a Marketing Campaign ===
22. In the LEFT SIDEBAR, click "Marketing", then click "Run a campaign".
23. Under campaign options, click "Discount for all customers".

24. Edit Stores:
    - Find "Stores" and click the EDIT (pencil) icon next to it.
    - In the search bar, type: {store_search}
    - Select "{store_name}" from the results.
    - Click "Save".

25. Edit Customer incentive:
    - Click the EDIT (pencil) icon next to "Customer incentive".
    - Select the "%" (percentage) option.
    - Type 15 in the percentage field.
    - Under "Minimum subtotal", choose "Custom" and enter 10 in the dollar amount field.
    - Click "Save".

26. Edit Scheduling:
    - Click the EDIT (pencil) icon next to "Scheduling".
    - Choose "Set a custom schedule".
    - UNCHECK all days EXCEPT "Tuesday" (Mon, Wed, Thu, Fri, Sat, Sun must be unchecked; only Tuesday checked).
    - Ensure Tuesday is checked for all timeslots.
    - Click "Save".

27. Edit Campaign name:
    - Click the EDIT (pencil) icon next to "Campaign name".
    - Delete the default text and type exactly: {campaign_name}
    - Click "Save".

28. Click the large "Create promotion" button at the bottom of the screen.

=== DONE ===
When all steps are complete, use the done action to finish. Summarize what was done: login, both reports created, both reports downloaded, and campaign "{campaign_name}" created.
"""


def _get_llm():
    """Use native Browser Use API (BROWSER_USE_API_KEY)."""
    try:
        from browser_use import ChatBrowserUse
    except ImportError:
        raise ImportError("Install browser-use: pip install browser-use")

    api_key = os.getenv("BROWSER_USE_API_KEY")
    if not api_key or not api_key.strip():
        raise ValueError(
            "BROWSER_USE_API_KEY is not set. Add it to your .env file for the Browser Use API."
        )
    return ChatBrowserUse()


def _get_browser(download_dir: Path):
    """Browser with download path set to the given directory."""
    from browser_use import Browser

    downloads_path = str(download_dir.resolve())
    common = dict(
        downloads_path=downloads_path,
        enable_default_extensions=False,
    )
    # Optional: use Chrome executable on macOS for consistent behavior
    if os.name == "posix":
        chrome = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
        if Path(chrome).exists():
            return Browser(executable_path=chrome, **common)
    return Browser(**common)


def _discover_downloads(download_dir: Path) -> Tuple[Optional[Path], Optional[Path]]:
    """
    Find the most recent financial and marketing report files in download_dir.
    Returns (marketing_path, financial_path). Financial is typically ZIP or CSV; marketing often ZIP.
    """
    download_dir = Path(download_dir)
    if not download_dir.is_dir():
        return (None, None)

    # Sort by mtime descending; accept .csv, .zip
    all_files = []
    for ext in ("*.csv", "*.zip", "*.xlsx"):
        for f in download_dir.glob(ext):
            if f.is_file():
                all_files.append((f.stat().st_mtime, f))
    all_files.sort(key=lambda x: x[0], reverse=True)

    financial_path: Optional[Path] = None
    marketing_path: Optional[Path] = None

    for _mtime, path in all_files:
        name_lower = path.name.lower()
        if "financial" in name_lower or "financials" in name_lower:
            if financial_path is None:
                financial_path = path
        elif "marketing" in name_lower:
            if marketing_path is None:
                marketing_path = path
        if financial_path and marketing_path:
            break

    # If we only have one file, assume financial (task downloads financial first, then marketing)
    if len(all_files) >= 1 and financial_path is None and marketing_path is None:
        financial_path = all_files[0][1]

    return (marketing_path, financial_path)


async def run(
    download_dir: Path,
    email: str,
    password: str,
    start_date: str,
    end_date: str,
    store_search: str = "1864",
    store_name: str = "McDonald's (1864 - PINE LAKE ROAD)",
    campaign_name: str = "1864-Tue",
) -> Tuple[Optional[Path], Optional[Path]]:
    """
    Run the DoorDash Merchant Portal automation via browser-use.
    Returns (marketing_download_path, financial_download_path) for use by analysis/marketing agents.
    """
    from browser_use import Agent

    download_dir = Path(download_dir)
    download_dir.mkdir(parents=True, exist_ok=True)

    task = get_task_description(
        email=email,
        password=password,
        start_date=start_date,
        end_date=end_date,
        store_search=store_search,
        store_name=store_name,
        campaign_name=campaign_name,
    )

    llm = _get_llm()
    browser = _get_browser(download_dir)
    agent = Agent(task=task, llm=llm, browser=browser)

    logger.info("DoorDash (browser-use): Starting agent run")
    history = await agent.run()
    if history and history.final_result:
        logger.info("DoorDash (browser-use): %s", history.final_result)
    else:
        logger.info("DoorDash (browser-use): Run completed.")

    marketing_path, financial_path = _discover_downloads(download_dir)
    if financial_path:
        logger.info("DoorDash (browser-use): Financial report at %s", financial_path)
    if marketing_path:
        logger.info("DoorDash (browser-use): Marketing report at %s", marketing_path)

    return (marketing_path, financial_path)

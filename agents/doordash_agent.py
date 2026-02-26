"""
DoorDash Merchant Portal automation using the browser-use framework.
Runs the full workflow: login, financial report, marketing report, download(s), and campaign creation.
Returns paths to downloaded report file(s) for use by analysis_agent and marketing_agent.
"""

import asyncio
import logging
import os
from pathlib import Path
from typing import Awaitable, Callable, Optional, Tuple

logger = logging.getLogger(__name__)


def get_task_description(
    email: str,
    password: str,
    start_date: str,
    end_date: str,
    store_search: str,
    store_name: str,
    campaign_name: str,
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
    - Choose "Set a custom schedule". A modal "Set custom schedule" will open with a grid of days and time slots.
    - To clear all selections efficiently: click the "Weekdays" button at the top (this deselects Mon–Fri), then click the "Weekends" button (this deselects Sat–Sun). Do NOT click each day cell one by one.
    - Then select only Tuesday: check all time slots for "Tue" (Early morning, Breakfast, Lunch, Afternoon, Dinner, Late night).
    - Click "Save" at the bottom of the modal. Wait 2 seconds.

27. Edit Campaign name:
    - Click the EDIT (pencil) icon next to "Campaign name". Delete the default text and type exactly: {campaign_name}. Wait 2 seconds. Click "Save".

28. Then click the large "Create promotion" button at the bottom of the screen.

=== DONE ===
When all steps are complete, use the done action to finish. Summarize what was done: login, both reports created, both reports downloaded, and campaign "{campaign_name}" created.
"""


def get_task_description_reports_only(
    email: str,
    password: str,
    start_date: str,
    end_date: str,
) -> str:
    """Task that ends after downloading both reports (no campaign). Used so we can run analysis before campaign."""
    if not password:
        raise ValueError("DOORDASH_PASSWORD is not set. Add it to your .env file (see .env.example).")
    return f"""
You are automating the DoorDash Merchant Portal. Complete the following steps in order. Stop after downloading both reports — do NOT create a campaign.

=== STEP 0: Navigate and log in (DO THIS EXACT ORDER — two-step login) ===
The login has TWO steps. Do NOT enter the password in the email field. Do NOT click "Log In" until the password screen is visible.

1. Go to exactly this URL: https://merchant-portal.doordash.com/merchant/login
2. On the first screen: find the EMAIL input field (labeled "Email"). Enter ONLY the email, exactly: {email}
3. Click the "Continue to Log In" button (the red button). Wait for the page to change.
4. On the NEXT screen: find the PASSWORD input field. Enter ONLY the password there: {password}
5. Click the "Log In" button. Wait until the dashboard has loaded.

=== STEP 1: Generate Financial Report ===
6. In the LEFT SIDEBAR, click "Reports". Click "Create report". Select "Financial report" RADIO BUTTON, click "Next".
7. Choose "By date range". Set Start date: {start_date}, End date: {end_date}. Click "Create report". Wait for the report to appear in the list.

=== STEP 2: Generate Marketing Report ===
8. Click "Create report". Select "Marketing report" RADIO BUTTON, click "Next". UNCHECK "Online Ordering", leave "Marketplace" CHECKED.
9. By date range: Start {start_date}, End {end_date}. Click "Create report". Wait for it to appear.

=== STEP 3: Download the Financial Report ===
10. Find the recently created "Financials" (or "Financial") report. Click the DOWNLOAD icon next to it. Wait for the download to complete.

=== STEP 4: Download the Marketing Report ===
11. Find the recently created "Marketing" report. Click the DOWNLOAD icon next to it. Wait for the download to complete.

=== DONE (stop here — no campaign) ===
When both reports are downloaded, use the done action to finish. Summarize: login, both reports created and downloaded.
"""


def get_task_description_campaign_only(
    email: str,
    password: str,
    store_search: str,
    store_name: str,
    campaign_name: str,
) -> str:
    """Task that does login then only campaign creation (reports already done)."""
    if not password:
        raise ValueError("DOORDASH_PASSWORD is not set. Add it to your .env file (see .env.example).")
    return f"""
You are automating the DoorDash Merchant Portal. You are already done with reports; now only create the marketing campaign. Complete the following in order.

=== STEP 0: Log in (two-step login) ===
1. Go to: https://merchant-portal.doordash.com/merchant/login
2. Enter ONLY the email in the Email field: {email}. Click "Continue to Log In". Wait for the next screen.
3. Enter ONLY the password in the Password field: {password}. Click "Log In". Wait for the dashboard.

=== STEP 1: Create Marketing Campaign ===
4. In the LEFT SIDEBAR, click "Marketing", then "Run a campaign". Click "Discount for all customers".

5. Edit Stores: click EDIT (pencil) next to "Stores". In search type: {store_search}. Select "{store_name}". Click "Save".

6. Edit Customer incentive: EDIT (pencil). Select "%", type 15. Minimum subtotal: "Custom", enter 10. Click "Save".

7. Edit Scheduling: EDIT (pencil). Choose "Set a custom schedule". In the "Set custom schedule" modal:
   - Click the "Weekdays" button at the top to deselect all weekday slots. Click the "Weekends" button to deselect all weekend slots. Do NOT click each day cell one by one.
   - Then select only Tuesday (Tue): check all time slots for Tue (Early morning through Late night).
   - Click "Save". Wait 2 seconds.

8. Edit Campaign name: click EDIT (pencil) next to "Campaign name". Delete the default text and type exactly: {campaign_name}. Wait 2 seconds. Click "Save".

9. Then click the large "Create promotion" button at the bottom.

=== DONE ===
When the campaign is created, use the done action to finish. Summarize: login and campaign "{campaign_name}" created.
"""


def get_task_description_campaign_already_logged_in(
    store_search: str,
    store_name: str,
    campaign_name: str,
) -> str:
    """Task for campaign creation when already logged in (same browser session). No login steps."""
    return f"""
You are already logged in to the DoorDash Merchant Portal and viewing the dashboard. Do NOT go to the login page or enter credentials. Start from the current page.

Create the marketing campaign:

1. In the LEFT SIDEBAR, click "Marketing", then "Run a campaign". Click "Discount for all customers".

2. Edit Stores: click EDIT (pencil) next to "Stores". In search type: {store_search}. Select "{store_name}". Click "Save".

3. Edit Customer incentive: EDIT (pencil). Select "%", type 15. Minimum subtotal: "Custom", enter 10. Click "Save".

4. Edit Scheduling: EDIT (pencil). Choose "Set a custom schedule". In the "Set custom schedule" modal:
   - Click the "Weekdays" button at the top to deselect all weekday slots. Click the "Weekends" button to deselect all weekend slots. Do NOT click each day cell one by one.
   - Then select only Tuesday (Tue): check all time slots for Tue (Early morning through Late night).
   - Click "Save". Wait 2 seconds.

5. Edit Campaign name: click EDIT (pencil) next to "Campaign name". Delete the default text and type exactly: {campaign_name}. Wait 2 seconds. Click "Save".

6. Then click the large "Create promotion" button at the bottom.

When the campaign is created, use the done action to finish. Summarize: campaign "{campaign_name}" created.
"""


def get_task_description_campaign_for_combo(combo: dict) -> str:
    """
    Build campaign task for one (store_id, day, slot, min_subtotal, campaign_name) from combined_analysis.
    For use when already logged in (same browser session). Combo dict has keys:
    store_id, day, slot, min_subtotal, campaign_name (e.g. TODC-{StoreID}-Monday-Breakfast).
    """
    store_id = str(combo.get("store_id", "")).strip()
    day = str(combo.get("day", "")).strip()
    slot = str(combo.get("slot", "")).strip()
    min_subtotal = combo.get("min_subtotal", 10)
    try:
        min_subtotal = int(round(float(min_subtotal)))
    except (TypeError, ValueError):
        min_subtotal = 10
    campaign_name = str(combo.get("campaign_name", f"TODC-{store_id}-{day}-{slot}")).strip()

    # Day short form for UI (e.g. Monday -> Mon, Tuesday -> Tue)
    day_short = day[:3] if len(day) >= 3 else day

    return f"""
You are already logged in to the DoorDash Merchant Portal. Do NOT go to login. Start from the current page.

Create this campaign (exactly one store, one day, one slot):

1. In the LEFT SIDEBAR, click "Marketing", then "Run a campaign". Click "Discount for all customers".

2. Edit Stores: click EDIT (pencil) next to "Stores". In the search bar type: {store_id}. Select the store that contains "{store_id}" (e.g. McDonald's ({store_id} - ...)). Click "Save".

3. Edit Customer incentive: click EDIT (pencil). Select the "%" (percentage) option. Type 15 in the percentage field. Under "Minimum subtotal", choose "Custom" and enter {min_subtotal} in the dollar amount field. For "Maximum discount amount", select the leftmost option ("Always lowest" or similar). Click "Save".

4. Edit Scheduling: click EDIT (pencil). Choose "Set a custom schedule". In the modal:
   - Click the "Weekdays" button to deselect all weekday slots. Click the "Weekends" button to deselect all weekend slots.
   - Then select ONLY the single combination: Day = {day} ({day_short}) and Slot = {slot}. In the grid, check only the cell where column {day_short} meets row {slot}.
   - Click "Save". Wait 2 seconds.

5. Edit Campaign name: click EDIT (pencil) next to "Campaign name". Delete the default text and type exactly: {campaign_name}. Wait 2 seconds. Click "Save".

6. Then click the large "Create promotion" button at the bottom.

When the campaign is created, use the done action to finish. Summarize: campaign "{campaign_name}" created.
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


def _get_browser(download_dir: Path, keep_alive: bool = False):
    """Browser with download path set to the given directory. keep_alive=True keeps browser open for reuse."""
    from browser_use import Browser

    downloads_path = str(download_dir.resolve())
    common = dict(
        downloads_path=downloads_path,
        enable_default_extensions=False,
        keep_alive=keep_alive,
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


async def _run_agent(download_dir: Path, task: str) -> None:
    """Run the browser-use agent with the given task (no download discovery)."""
    from browser_use import Agent

    download_dir = Path(download_dir)
    download_dir.mkdir(parents=True, exist_ok=True)
    llm = _get_llm()
    browser = _get_browser(download_dir)
    agent = Agent(task=task, llm=llm, browser=browser)
    history = await agent.run()
    if history and history.final_result:
        logger.info("DoorDash (browser-use): %s", history.final_result)
    else:
        logger.info("DoorDash (browser-use): Run completed.")


async def run_reports_only(
    download_dir: Path,
    email: str,
    password: str,
    start_date: str,
    end_date: str,
) -> Tuple[Optional[Path], Optional[Path]]:
    """
    Run only login + report creation + download. Stops before campaign.
    Returns (marketing_download_path, financial_download_path) for analysis agents.
    """
    download_dir = Path(download_dir)
    task = get_task_description_reports_only(
        email=email,
        password=password,
        start_date=start_date,
        end_date=end_date,
    )
    logger.info("DoorDash (browser-use): Starting reports-only run (login, reports, download)")
    await _run_agent(download_dir, task)
    marketing_path, financial_path = _discover_downloads(download_dir)
    if financial_path:
        logger.info("DoorDash (browser-use): Financial report at %s", financial_path)
    if marketing_path:
        logger.info("DoorDash (browser-use): Marketing report at %s", marketing_path)
    return (marketing_path, financial_path)


async def run_campaign_only(
    download_dir: Path,
    email: str,
    password: str,
    store_search: str,
    store_name: str,
    campaign_name: str,
) -> None:
    """
    Run only login + campaign creation. Use after reports are downloaded and analysis/combined report have run.
    """
    download_dir = Path(download_dir)
    task = get_task_description_campaign_only(
        email=email,
        password=password,
        store_search=store_search,
        store_name=store_name,
        campaign_name=campaign_name,
    )
    logger.info("DoorDash (browser-use): Starting campaign-only run")
    await _run_agent(download_dir, task)


async def run_reports_then_analysis_then_campaign(
    download_dir: Path,
    email: str,
    password: str,
    start_date: str,
    end_date: str,
    analysis_callback: Callable[[Optional[Path], Optional[Path]], Awaitable[Optional[Path]]],
) -> None:
    """
    Single browser session: login → reports → download → (browser stays open) →
    run analysis_callback(marketing_path, financial_path) → returns combined_path →
    for each (store, day, slot) combo from combined_analysis Day-Slot sheets, run campaign (no login again) → close browser.

    Store IDs come only from the logged-in account's combined_analysis sheets ("Day-Slot - {StoreID}"). No env store IDs.
    """
    from browser_use import Agent

    try:
        from agents.campaign_params import (
            get_all_campaign_combos_from_combined_analysis,
            ensure_campaigns_executed_csv,
            log_campaign_executed,
        )
    except ImportError:
        get_all_campaign_combos_from_combined_analysis = None
        ensure_campaigns_executed_csv = None
        log_campaign_executed = None

    download_dir = Path(download_dir)
    download_dir.mkdir(parents=True, exist_ok=True)

    reports_task = get_task_description_reports_only(
        email=email,
        password=password,
        start_date=start_date,
        end_date=end_date,
    )

    llm = _get_llm()
    browser = _get_browser(download_dir, keep_alive=True)
    agent = Agent(task=reports_task, llm=llm, browser=browser)

    logger.info("DoorDash (browser-use): Phase 1 — reports (login, create, download); browser will stay open.")
    await agent.run()

    marketing_path, financial_path = _discover_downloads(download_dir)
    if financial_path:
        logger.info("DoorDash (browser-use): Financial report at %s", financial_path)
    if marketing_path:
        logger.info("DoorDash (browser-use): Marketing report at %s", marketing_path)

    logger.info("DoorDash (browser-use): Pausing browser agent; running analysis callback.")
    combined_path = await analysis_callback(marketing_path, financial_path)

    if not combined_path or not Path(combined_path).is_file():
        logger.warning(
            "DoorDash (browser-use): No combined_analysis file returned. Set DOORDASH_* credentials and ensure financial/marketing analysis run; campaigns will use fallback env only if set."
        )

    combos = []
    if combined_path and Path(combined_path).is_file() and get_all_campaign_combos_from_combined_analysis:
        combos = get_all_campaign_combos_from_combined_analysis(Path(combined_path))
        logger.info("DoorDash (browser-use): Found %s campaign combos from Day-Slot sheets (store IDs from sheets).", len(combos))

    if hasattr(agent, "add_new_task"):
        if combos:
            if ensure_campaigns_executed_csv:
                ensure_campaigns_executed_csv(download_dir)
            logger.info("DoorDash (browser-use): Phase 2 — %s campaigns from combined_analysis (same session).", len(combos))
            for i, combo in enumerate(combos, 1):
                task = get_task_description_campaign_for_combo(combo)
                agent.add_new_task(task)
                try:
                    await agent.run()
                    status = "Completed"
                except Exception as e:
                    logger.warning("Campaign %s failed: %s", combo.get("campaign_name"), e)
                    status = "Failed"
                if log_campaign_executed:
                    log_campaign_executed(
                        download_dir,
                        store_id=str(combo.get("store_id", "")),
                        campaign_name=str(combo.get("campaign_name", "")),
                        pct_value=15,
                        min_subtotal=float(combo.get("min_subtotal", 10)),
                        max_discount="Always lowest",
                        status=status,
                    )
                logger.info("DoorDash (browser-use): Campaign %s/%s done: %s", i, len(combos), combo.get("campaign_name"))
        else:
            logger.warning(
                "DoorDash (browser-use): No campaign combos from combined_analysis. Store IDs come only from that file (Day-Slot - {StoreID} sheets). Skip campaigns until combined_analysis is created for this account."
            )
    else:
        logger.warning(
            "Agent.add_new_task not found. Store IDs come only from combined_analysis; cannot run campaigns without chaining. Skip campaign phase."
        )

    try:
        kill_fn = getattr(browser, "kill", None)
        if callable(kill_fn):
            result = kill_fn()
            if asyncio.iscoroutine(result):
                await result
        else:
            close_fn = getattr(browser, "close", None)
            if callable(close_fn):
                result = close_fn()
                if asyncio.iscoroutine(result):
                    await result
    except Exception as e:
        logger.debug("Browser close/kill: %s", e)


async def run(
    download_dir: Path,
    email: str,
    password: str,
    start_date: str,
    end_date: str,
    store_search: str = "",
    store_name: str = "",
    campaign_name: str = "",
) -> Tuple[Optional[Path], Optional[Path]]:
    """
    Run reports-only then return paths (convenience alias for run_reports_only).
    For full flow with analysis in between, use run_reports_only → analysis → run_campaign_only from main.
    """
    return await run_reports_only(
        download_dir=download_dir,
        email=email,
        password=password,
        start_date=start_date,
        end_date=end_date,
    )

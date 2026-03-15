"""
DoorDash Merchant Portal automation using the browser-use framework.
Runs the full workflow: login, financial report, marketing report, download(s), and campaign creation.
Returns paths to downloaded report file(s) for use by analysis_agent and marketing_agent.
"""

import asyncio
import logging
import os
import zipfile
from pathlib import Path
from typing import Awaitable, Callable, Optional, Tuple

# Timeouts (seconds) for each browser-use agent phase
AGENT_REPORTS_TIMEOUT = 900   # 15 min: login + create 2 reports + download both
AGENT_LOGIN_TIMEOUT = 180     # 3 min: re-login after browser restart
AGENT_RESET_TIMEOUT = 90      # 1.5 min: navigate to Marketing page between campaigns
AGENT_CAMPAIGN_TIMEOUT = 360  # 6 min: create one campaign end-to-end

# Campaigns per browser session before restart; override via env for tuning
MAX_CAMPAIGNS_PER_SESSION = int(os.getenv("MAX_CAMPAIGNS_PER_SESSION", "5"))

from agents.combined_report_agent import (
    append_campaign_mappings_to_workbook,
    read_campaign_mapping_statuses,
    update_campaign_mapping_status,
)
from agents.slack_agent import push_to_slack

logger = logging.getLogger(__name__)

# --- IN USE: Login → Report creation → Report download (Phase 1 of main flow) ---
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
3. Click the "Continue to Log In" button (the red button). WAIT UNTIL the page changes and you see the password screen.
4. On the NEXT screen: find the PASSWORD input field. Enter ONLY the password there: {password}
5. Click the "Log In" button. WAIT UNTIL the dashboard has fully loaded (you see sidebar navigation and main content).

=== STEP 1: Generate Financial Report ===
6. In the LEFT SIDEBAR, click "Reports". WAIT UNTIL the Reports page loads. Click "Create report". Select "Financial report" RADIO BUTTON, click "Next".
7. Choose "By date range". Set Start date: {start_date}, End date: {end_date}. Click "Create report". WAIT UNTIL the report appears in the list (it may take several seconds to generate).

=== STEP 2: Download the Financial Report IMMEDIATELY ===
8. The Financial report you just created should now be at the TOP of the reports list. Click the DOWNLOAD icon (arrow/download button) next to this TOPMOST "Financials" report row. WAIT UNTIL the download completes (file appears in downloads). Do NOT proceed until the financial report is fully downloaded.

=== STEP 3: Generate Marketing Report ===
9. Click "Create report". Select "Marketing report" RADIO BUTTON, click "Next".
10. IMPORTANT: You MUST UNCHECK "Online Ordering" checkbox. Make sure "Online Ordering" is UNCHECKED and "Marketplace" remains CHECKED.
11. By date range: Start {start_date}, End {end_date}. Click "Create report". WAIT UNTIL the report appears in the list.

=== STEP 4: Download the Marketing Report IMMEDIATELY ===
12. The Marketing report you just created should now be at the TOP of the reports list. Click the DOWNLOAD icon (arrow/download button) next to this TOPMOST "Marketing" report row. WAIT UNTIL the download completes (file appears in downloads).

=== DONE (stop here — no campaign) ===
When both reports are downloaded, use the done action to finish. Summarize: login, both reports created and downloaded.
"""

def _get_retry_download_task(missing_reports: list[str]) -> str:
    """Generate a task to retry downloading missing reports from the already-open Reports page."""
    parts = []
    for report_type in missing_reports:
        if report_type == "Financial":
            parts.append(
                '- Find the most recently created "Financials" (or "Financial") report row in the reports table. '
                'Click the DOWNLOAD icon (arrow/download button) next to it. '
                'WAIT UNTIL the download completes (file appears in downloads folder).'
            )
        elif report_type == "Marketing":
            parts.append(
                '- Find the most recently created "Marketing" report row in the reports table. '
                'Click the DOWNLOAD icon (arrow/download button) next to it. '
                'WAIT UNTIL the download completes (file appears in downloads folder).'
            )
    steps = "\n".join(parts)
    return f"""
You are on the DoorDash Merchant Portal. The Reports page should already be open.
If you are not on the Reports page, click "Reports" in the left sidebar and WAIT for it to load.

Download the following missing report(s):
{steps}

IMPORTANT: Make sure to wait for each download to fully complete before proceeding to the next.
When done, use the done action. Summarize which reports were downloaded.
"""


# --- IN USE: Campaign creation with subtotal + slot tags (Phase 2, per store per subtotal) ---
def get_task_description_campaign_for_subtotal_combo(combo: dict) -> str:
    store_id = str(combo.get("store_id", "")).strip()
    store_name = str(combo.get("store_name", "")).strip()
    min_subtotal = combo.get("min_subtotal", 10)
    try:
        min_subtotal = int(round(float(min_subtotal)))
    except (TypeError, ValueError):
        min_subtotal = 10
    slot_tags = combo.get("slot_tags") or []
    if not isinstance(slot_tags, (list, tuple)):
        slot_tags = []
    slot_tags = [int(t) for t in slot_tags if t is not None and str(t).strip() != ""]
    campaign_name = str(combo.get("campaign_name", f"TODC-{store_id}-${min_subtotal}")).strip() or f"TODC-{store_id}-${min_subtotal}"
    tags_str = ", ".join(str(t) for t in sorted(slot_tags))

    # Grid mapping: tag number → (row_name, col_name) for explicit instructions
    ALL_TAGS = set(range(1, 43))
    _GRID_ROWS = ["Early morning", "Breakfast", "Lunch", "Afternoon", "Dinner", "Late night"]
    _GRID_COLS = ["Mon", "Tue", "Wed", "Thur", "Fri", "Sat", "Sun"]
    def _tag_to_cell(t: int) -> str:
        row_idx = (t - 1) // 7
        col_idx = (t - 1) % 7
        return f"{_GRID_ROWS[row_idx]}-{_GRID_COLS[col_idx]}"

    selected_set = set(slot_tags)

    def _group_by_row(tag_set):
        """Group tags by row name for systematic processing."""
        rows = {}
        for t in sorted(tag_set):
            row_idx = (t - 1) // 7
            row_name = _GRID_ROWS[row_idx]
            col_name = _GRID_COLS[(t - 1) % 7]
            rows.setdefault(row_name, []).append((t, col_name))
        return rows

    # Always: deselect everything first, then select only the needed slots
    grouped = _group_by_row(selected_set)
    row_lines = []
    for row_name, cells in grouped.items():
        cols = ", ".join(f"{col} (tag {t})" for t, col in cells)
        row_lines.append(f"  - {row_name} row ({len(cells)} cells): {cols}")
    grouped_str = "\n".join(row_lines)
    schedule_instructions = f"""- CRITICAL: Do NOT click any individual grid cells yet.
- FIRST click "Weekdays" to DESELECT all weekday slots.
- THEN click "Weekends" to DESELECT all weekend slots.
- WAIT UNTIL all slots appear deselected (no checkmarks visible in any cell).
- VERIFY: Every cell in the grid is empty before proceeding. If any cells still have checkmarks, click "Weekdays" and "Weekends" again to toggle them off.
- Now SELECT (click to check) the following {len(selected_set)} cells, organized by row. Process ONE ROW AT A TIME:
{grouped_str}
- IMPORTANT: Click each cell exactly ONCE to select it. Do NOT click any cell more than once or it will toggle back off. Do NOT re-click cells you already selected.
- After selecting all {len(selected_set)} cells above, verify that exactly {len(selected_set)} cells are checked.
- COUNT the checked cells in each row to verify: {', '.join(f'{row}: {sum(1 for t in selected_set if (t-1)//7 == i)}' for i, row in enumerate(_GRID_ROWS) if sum(1 for t in selected_set if (t-1)//7 == i) > 0)}."""

    return f"""
ROLE: You are automating campaign creation on DoorDash Merchant Portal. You are already logged in.

HARD RULES (read before every action):
- Do NOT go to the login page.
- Do NOT create reports or download anything.
- Do NOT click "Get started" (that is for BOGO, not discount campaigns).
- Do NOT click "Create promotion" until step 7 explicitly says to.
- Do NOT click any button not mentioned in the steps below.
- If a modal does not open after clicking Edit, wait 3 seconds, scroll to make the section visible, then click Edit again ONCE. If it still fails, go to sidebar > Marketing > Run a campaign and restart from step 1.

CAMPAIGN: {campaign_name}
STORE ID: {store_id}
STORE NAME: {store_name if store_name else "N/A"}
MIN SUBTOTAL: ${min_subtotal}
SCHEDULE TAGS: {tags_str}

STEP 1 — Open campaign builder:
- Click "Marketing" in the left sidebar.
- WAIT UNTIL the Marketing page has fully loaded (look for page content to appear, not just a spinner).
- Click "Run a campaign".
- WAIT UNTIL you see campaign type cards on the page. If after 10 seconds you don't see them, scroll down or click "Run a campaign" again.
- VERIFY: You see campaign type cards on the page.
- Find "Discount for all customers" card. Click its "Select" button.
- WAIT UNTIL a right-side panel appears. VERIFY: A right-side panel is visible.
- Click "Customize your campaign" in that panel.
- WAIT UNTIL the campaign customization form loads.

STEP 2 — Select store:
- Click the Edit (pencil) icon next to "Stores".
- WAIT UNTIL the Store selection modal is fully open and interactive.
- VERIFY: Store selection modal is open. If not, wait a moment and click Edit again.
- Click "Select All" to deselect all stores.
- WAIT UNTIL all checkboxes are deselected.
- Type "{store_id}" in the search bar.
- WAIT UNTIL search results appear.
- If a store matching "{store_id}" appears in the results, select it.
- FALLBACK: If NO store appears for "{store_id}" (empty results or "no results found"), clear the search bar and type the store name "{store_name}" instead. WAIT UNTIL search results appear. Select the store matching "{store_name}".
- Select ONLY one store (the one matching the Store ID or Store Name above).
- Click "Save".
- WAIT UNTIL the modal closes and the store selection is saved.

STEP 3 — Set customer incentive:
- Scroll the right panel until "Customer incentive" heading is visible.
- Click the Edit (pencil) icon that is DIRECTLY next to "Customer incentive" text.
- WAIT UNTIL the incentive modal is fully loaded.
- VERIFY: Modal title says "Set customer incentive". If not, do NOT proceed — wait 3 seconds, then retry the click once. If still wrong, navigate to sidebar > Marketing > Run a campaign and restart from step 1.
- Click "15%" radio button.
- WAIT UNTIL the radio button is selected.
- Click "Custom" under Minimum subtotal.
- WAIT UNTIL the custom subtotal input field appears.
- Click DIRECTLY on the custom subtotal INPUT FIELD (not the "Custom" button) to focus it.
- Select all text in the field (triple-click or Ctrl+A / Cmd+A).
- Type: {min_subtotal}
- WAIT 2 seconds for the field to update.
- VERIFY the field now displays "{min_subtotal}" or "${min_subtotal}". READ the actual value shown in the input field.
- If the field is EMPTY or shows a DIFFERENT value (like "$25" which is the default), you MUST fix it:
  1. Click the input field again to focus it.
  2. Triple-click to select all existing text.
  3. Type {min_subtotal} again.
  4. WAIT 2 seconds and re-verify.
- Do NOT proceed until the field shows {min_subtotal} or ${min_subtotal}. The default value of $25 is WRONG unless the target is exactly $25.
- Find "Maximum discount amount" section (three buttons like $5, $7, $10 or similar).
- Click the LEFTMOST button (smallest value, typically $5 or $2).
- VERIFY: The leftmost button appears selected/highlighted.
- Click "Save".
- WAIT UNTIL the modal closes.

STEP 4 — Set schedule:
- Click the Edit (pencil) icon next to "Scheduling".
- WAIT UNTIL the Scheduling modal is fully open with a grid visible.
- VERIFY: Scheduling modal is open with a grid. If not, wait and retry.
- Click "Set a custom schedule".
- WAIT UNTIL the custom schedule grid is visible.
- Grid layout: 6 rows (Early morning, Breakfast, Lunch, Afternoon, Dinner, Late night) x 7 columns (Mon, Tue, Wed, Thur, Fri, Sat, Sun).
{schedule_instructions}
- Click "Save".
- WAIT UNTIL the modal closes and the schedule is saved.

STEP 5 — Verify incentive (MANDATORY safety check):
- Click Edit (pencil) next to "Customer incentive" again.
- WAIT UNTIL the modal opens.
- READ the current Minimum subtotal value displayed in the field. It MUST show {min_subtotal} or ${min_subtotal}.
- If it shows $25 (the default) or ANY value other than ${min_subtotal}, it is WRONG and you MUST fix it:
  1. Click "Custom" under Minimum subtotal.
  2. Click the input field to focus it.
  3. Triple-click to select all text, then type: {min_subtotal}
  4. WAIT 2 seconds and verify the field shows {min_subtotal} or ${min_subtotal}.
- Confirm the leftmost Maximum discount button is selected. If not, click it.
- Click "Save".
- WAIT UNTIL the modal closes.
- VERIFY: The campaign summary/panel should show "orders ${min_subtotal} or more" (NOT $25 unless target is $25). If it shows the wrong value, go back to STEP 5 and fix it.

STEP 6 — Set campaign name:
- Click Edit (pencil) next to "Campaign name".
- WAIT UNTIL the name editing field is visible and editable.
- Clear ALL existing text in the name field.
- Type exactly: {campaign_name}
- Click "Save".
- WAIT UNTIL the modal closes.
- VERIFY: The campaign name shows as "{campaign_name}".

STEP 7 — Create the promotion:
- ONLY now click "Create promotion" at the bottom.
- WAIT UNTIL you see confirmation that the campaign was created (a success message, toast, or redirect).

DONE: Use the done action. Summarize: campaign "{campaign_name}" created for store {store_id}.
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


def _peek_zip_type(path: Path) -> str:
    """
    Inspect ZIP contents to classify as 'financial', 'marketing', or ''.
    Used as fallback when filename has no recognizable keyword.
    """
    try:
        with zipfile.ZipFile(path, "r") as z:
            names_upper = " ".join(z.namelist()).upper()
        if "FINANCIAL_DETAILED" in names_upper or ("FINANCIAL" in names_upper and "MARKETING" not in names_upper):
            return "financial"
        if "MARKETING_PROMOTION" in names_upper or "MARKETING_SPONSORED" in names_upper or "MARKETING" in names_upper:
            return "marketing"
    except Exception:
        pass
    return ""


def _discover_downloads(download_dir: Path) -> Tuple[Optional[Path], Optional[Path]]:
    """
    Find the most recent financial and marketing report files in download_dir.
    Strategy:
      1. Filename keyword match ("financial", "marketing").
      2. If keywords fail, peek inside ZIPs to classify by content.
      3. Last resort: treat the most-recent file as financial.
    Returns (marketing_path, financial_path).
    """
    download_dir = Path(download_dir)
    if not download_dir.is_dir():
        return (None, None)

    all_files = []
    for ext in ("*.csv", "*.zip", "*.xlsx"):
        for f in download_dir.glob(ext):
            if f.is_file():
                all_files.append((f.stat().st_mtime, f))
    all_files.sort(key=lambda x: x[0], reverse=True)

    financial_path: Optional[Path] = None
    marketing_path: Optional[Path] = None

    # Pass 1: filename keywords (fast)
    unmatched = []
    for _mtime, path in all_files:
        name_lower = path.name.lower()
        if "financial" in name_lower or "financials" in name_lower:
            if financial_path is None:
                financial_path = path
        elif "marketing" in name_lower:
            if marketing_path is None:
                marketing_path = path
        else:
            unmatched.append(path)
        if financial_path and marketing_path:
            break

    # Pass 2: ZIP content inspection for files not matched by name
    if (financial_path is None or marketing_path is None) and unmatched:
        for path in unmatched:
            if path.suffix.lower() == ".zip":
                kind = _peek_zip_type(path)
                if kind == "financial" and financial_path is None:
                    financial_path = path
                    logger.info("DoorDash: classified %s as financial by content", path.name)
                elif kind == "marketing" and marketing_path is None:
                    marketing_path = path
                    logger.info("DoorDash: classified %s as marketing by content", path.name)
            if financial_path and marketing_path:
                break

    # Pass 3: last resort — treat most-recent unmatched file as financial
    # but never reuse a file already assigned to marketing_path
    if financial_path is None and all_files:
        for _mtime, candidate in all_files:
            if candidate != marketing_path:
                financial_path = candidate
                logger.warning("DoorDash: no filename/content match; treating %s as financial", financial_path.name)
                break
        if financial_path is None:
            logger.warning("DoorDash: only one file found and it is already assigned as marketing; no financial report available")

    return (marketing_path, financial_path)


async def _kill_browser(browser) -> None:
    """Gracefully kill/close browser; swallows all errors."""
    try:
        kill_fn = getattr(browser, "kill", None) or getattr(browser, "close", None)
        if callable(kill_fn):
            result = kill_fn()
            if asyncio.iscoroutine(result):
                await result
    except Exception as e:
        logger.debug("Browser close: %s", e)


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
    from browser_use import Agent

    download_dir = Path(download_dir)
    download_dir.mkdir(parents=True, exist_ok=True)
    task = get_task_description_reports_only(
        email=email,
        password=password,
        start_date=start_date,
        end_date=end_date,
    )
    logger.info("DoorDash (browser-use): Starting reports-only run (login, reports, download)")
    llm = _get_llm()
    browser = _get_browser(download_dir)
    agent = Agent(task=task, llm=llm, browser=browser)
    history = await asyncio.wait_for(agent.run(), timeout=AGENT_REPORTS_TIMEOUT)
    if history and history.final_result:
        logger.info("DoorDash (browser-use): %s", history.final_result)
    marketing_path, financial_path = _discover_downloads(download_dir)
    if financial_path:
        logger.info("DoorDash (browser-use): Financial report at %s", financial_path)
    if marketing_path:
        logger.info("DoorDash (browser-use): Marketing report at %s", marketing_path)
    return (marketing_path, financial_path)


# --- IN USE: Main flow — Login → Reports → Download → Analysis → Campaigns (subtotal+tags) for all stores/subtotals ---
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
            get_campaign_combos_from_slots_and_combined,
            ensure_campaigns_executed_csv,
            log_campaign_executed,
        )
    except ImportError:
        get_all_campaign_combos_from_combined_analysis = None
        get_campaign_combos_from_slots_and_combined = None
        ensure_campaigns_executed_csv = None
        log_campaign_executed = None

    download_dir = Path(download_dir)
    project_root = Path(__file__).resolve().parent.parent
    slots_csv_path = project_root / "slots.csv"
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
    try:
        await asyncio.wait_for(agent.run(), timeout=AGENT_REPORTS_TIMEOUT)
        push_to_slack(f"Login successful for {email}")
    except asyncio.TimeoutError:
        await _kill_browser(browser)
        push_to_slack(f"Phase 1 timed out after {AGENT_REPORTS_TIMEOUT}s for {email}")
        raise RuntimeError(f"Phase 1 (reports) timed out after {AGENT_REPORTS_TIMEOUT}s")
    except Exception as e:
        await _kill_browser(browser)
        push_to_slack(f"Login failed for {email}: {e}")
        raise e

    marketing_path, financial_path = _discover_downloads(download_dir)

    # --- Retry: if one report is missing, attempt to download just the missing one ---
    if not financial_path or not marketing_path:
        missing = []
        if not financial_path:
            missing.append("Financial")
        if not marketing_path:
            missing.append("Marketing")
        logger.warning("DoorDash (browser-use): Missing report(s) after Phase 1: %s. Retrying download.", ", ".join(missing))
        push_to_slack(f"Missing report(s): {', '.join(missing)}. Retrying download...")

        retry_task = _get_retry_download_task(missing)
        retry_agent = Agent(task=retry_task, llm=llm, browser=browser)
        try:
            await asyncio.wait_for(retry_agent.run(), timeout=300)  # 5 min retry
            marketing_path, financial_path = _discover_downloads(download_dir)
            logger.info("DoorDash (browser-use): After retry — financial=%s, marketing=%s", financial_path, marketing_path)
        except Exception as retry_err:
            logger.warning("DoorDash (browser-use): Retry download failed: %s", retry_err)

    if financial_path:
        logger.info("DoorDash (browser-use): Financial report at %s", financial_path)
        push_to_slack("Financials Report pulled")
    else:
        push_to_slack("Financials Report failed: file not found after retry")

    if marketing_path:
        logger.info("DoorDash (browser-use): Marketing report at %s", marketing_path)
        push_to_slack("Marketing report pulled")
    else:
        push_to_slack("Marketing report failed: file not found after retry")

    if financial_path and marketing_path:
        push_to_slack("Reports downloaded")

    logger.info("DoorDash (browser-use): Pausing browser agent; running analysis callback.")
    combined_path = await analysis_callback(marketing_path, financial_path)

    if not combined_path or not Path(combined_path).is_file():
        logger.warning(
            "DoorDash (browser-use): No combined_analysis file returned. Set DOORDASH_* credentials and ensure financial/marketing analysis run; campaigns will use fallback env only if set."
        )
    else:
        push_to_slack("Combined analysis formed")

    combos = []
    use_slots_csv = False
    if get_campaign_combos_from_slots_and_combined and slots_csv_path.is_file() and combined_path and Path(combined_path).is_file():
        combos = get_campaign_combos_from_slots_and_combined(slots_csv_path, Path(combined_path))
        if combos:
            use_slots_csv = True
            logger.info("DoorDash (browser-use): Found %s campaign combos from Day-Slot sheets + slots grid (one per min_subtotal per store).", len(combos))
    if not combos and combined_path and Path(combined_path).is_file() and get_all_campaign_combos_from_combined_analysis:
        combos = get_all_campaign_combos_from_combined_analysis(Path(combined_path))
        logger.info("DoorDash (browser-use): Found %s campaign combos from Day-Slot sheets (store IDs from sheets).", len(combos))

    # Push campaign mappings to combined analysis sheet (one row per combo)
    if combined_path and Path(combined_path).is_file() and combos:
        mappings = []
        for c in combos:
            # Support both legacy (day, slot) and subtotal-based (slot_tags) combo shape
            slot_tags = c.get("slot_tags")
            if slot_tags is None and c.get("day") and c.get("slot"):
                slot_tags = [f"{c.get('day', '')}-{c.get('slot', '')}"]
            mappings.append({
                "store_id": c.get("store_id", ""),
                "store_name": c.get("store_name", ""),
                "min_subtotal": c.get("min_subtotal", 10),
                "slot_tags": slot_tags or [],
                "campaign_name": c.get("campaign_name", ""),
            })
        append_campaign_mappings_to_workbook(Path(combined_path), mappings)

        # Skip campaigns already marked Successful in the sheet (resume after partial run)
        existing_statuses = read_campaign_mapping_statuses(Path(combined_path))
        already_done = {name for name, s in existing_statuses.items() if s == "Successful"}
        if already_done:
            before = len(combos)
            combos = [c for c in combos if c.get("campaign_name") not in already_done]
            logger.info(
                "DoorDash: skipping %d already-Successful campaign(s); %d remaining.",
                before - len(combos), len(combos),
            )
            push_to_slack(f"Resuming: {before - len(combos)} campaigns already done, {len(combos)} remaining")

    # Template for re-login after browser restart
    relogin_task = (
        f"Go to https://merchant-portal.doordash.com/merchant/login\n"
        f"Enter email: {email}, click 'Continue to Log In'.\n"
        f"On the next screen, enter password: {password}, click 'Log In'.\n"
        f"Wait for the dashboard to load. Use done action to finish."
    )

    # Navigation reset run before each campaign to dismiss any leftover UI and land on Marketing page
    reset_task = (
        "IMPORTANT: Before navigating, check if any modal, popup, dialog, or overlay is currently visible on the page. "
        "If so, close it by clicking 'X', 'Close', 'Cancel', or pressing Escape. "
        "Then navigate to the DoorDash Merchant Portal dashboard. "
        "In the LEFT SIDEBAR, click 'Marketing'. "
        "WAIT UNTIL the Marketing page has fully loaded (you see campaign-related content, not a loading spinner). "
        "If the page shows an error or doesn't load, try clicking 'Marketing' in the sidebar again. "
        "Confirm you see the Marketing page. Use the done action to finish."
    )

    if combos:
        if ensure_campaigns_executed_csv:
            ensure_campaigns_executed_csv(download_dir)
        logger.info(
            "DoorDash (browser-use): Phase 2 — %s campaigns from %s (fresh Agent context per campaign).",
            len(combos),
            "slots.csv" if use_slots_csv else "combined_analysis",
        )
        for i, combo in enumerate(combos, 1):
            # Restart browser every N campaigns to prevent browser memory growth
            if i > 1 and (i - 1) % MAX_CAMPAIGNS_PER_SESSION == 0:
                logger.info("Restarting browser after %d campaigns to prevent memory growth", i - 1)
                await _kill_browser(browser)
                browser = _get_browser(download_dir, keep_alive=True)
                relogin_ok = False
                for relogin_attempt in range(1, 3):  # 2 attempts to re-login
                    try:
                        login_agent = Agent(task=relogin_task, llm=llm, browser=browser)
                        await asyncio.wait_for(login_agent.run(), timeout=AGENT_LOGIN_TIMEOUT)
                        push_to_slack(f"Browser restarted after {i - 1} campaigns, re-logged in")
                        relogin_ok = True
                        break
                    except asyncio.TimeoutError:
                        logger.warning("Re-login attempt %d timed out", relogin_attempt)
                        await _kill_browser(browser)
                        browser = _get_browser(download_dir, keep_alive=True)
                    except Exception as e:
                        logger.warning("Re-login attempt %d failed: %s", relogin_attempt, e)
                        await _kill_browser(browser)
                        browser = _get_browser(download_dir, keep_alive=True)
                if not relogin_ok:
                    push_to_slack(f"Re-login failed after browser restart at campaign {i}; aborting remaining campaigns")
                    logger.error("Re-login failed after 2 attempts; stopping campaign loop to avoid running on dead browser")
                    await _kill_browser(browser)
                    return

            # Fresh Agent for navigation reset — clean LLM context, same browser session
            # Navigate to a fresh URL first to clear any stale DOM/JS state from previous campaign
            nav_to_marketing_task = (
                "Go to this URL: https://merchant-portal.doordash.com/merchant/marketing "
                "WAIT UNTIL the page has fully loaded. "
                "If any modal, popup, dialog, or overlay is visible, close it by clicking 'X', 'Close', 'Cancel', or pressing Escape. "
                "Confirm you see the Marketing page with campaign-related content. Use the done action to finish."
            )
            try:
                reset_agent = Agent(task=nav_to_marketing_task, llm=llm, browser=browser)
                await asyncio.wait_for(reset_agent.run(), timeout=AGENT_RESET_TIMEOUT)
            except asyncio.TimeoutError:
                logger.warning("Navigation reset timed out before campaign %s; attempting fallback reset", i)
                # Fallback: try the sidebar-click method
                try:
                    fallback_agent = Agent(task=reset_task, llm=llm, browser=browser)
                    await asyncio.wait_for(fallback_agent.run(), timeout=AGENT_RESET_TIMEOUT)
                except Exception:
                    logger.warning("Fallback navigation reset also failed before campaign %s", i)
            except Exception as e:
                logger.warning("Navigation reset failed before campaign %s: %s; continuing", i, e)

            campaign_name = str(combo.get("campaign_name", ""))
            store_id = str(combo.get("store_id", ""))
            min_subtotal = str(combo.get("min_subtotal", "10"))
            if combo.get("day") and combo.get("slot"):
                detail = f"store {store_id}, {combo.get('day')}, {combo.get('slot')}, min_subtotal ${min_subtotal}"
            else:
                detail = f"store {store_id}, min_subtotal ${min_subtotal}"
            push_to_slack(f"{campaign_name} setup in progress — {detail}")

            # Fresh Agent per campaign — LLM context never carries previous campaign history
            campaign_task = get_task_description_campaign_for_subtotal_combo(combo)
            status = "Failed"
            try:
                campaign_agent = Agent(task=campaign_task, llm=llm, browser=browser)
                history = await asyncio.wait_for(campaign_agent.run(), timeout=AGENT_CAMPAIGN_TIMEOUT)
                completed_ok = True
                if history is not None:
                    if hasattr(history, "is_successful") and callable(history.is_successful):
                        completed_ok = history.is_successful()
                    elif hasattr(history, "final_result"):
                        val = history.final_result
                        completed_ok = bool(val() if callable(val) else val) if val is not None else False
                if completed_ok:
                    status = "Successful"
                    push_to_slack(f"{campaign_name} — done")
                else:
                    status = "Failed"
                    push_to_slack(f"{campaign_name} — failed (agent stopped without completing)")
                    logger.warning("Campaign %s: agent stopped without completing", campaign_name)
            except asyncio.TimeoutError:
                status = "Failed"
                logger.warning("Campaign %s timed out after %ss", campaign_name, AGENT_CAMPAIGN_TIMEOUT)
                push_to_slack(f"{campaign_name} — timed out")
            except Exception as e:
                status = "Failed"
                logger.warning("Campaign %s failed: %s", campaign_name, e)
                push_to_slack(f"{campaign_name} — failed: {e}")

            # Write status live to Campaign Mappings sheet so reruns can skip Successful ones
            if combined_path and Path(combined_path).is_file():
                update_campaign_mapping_status(Path(combined_path), campaign_name, status)

            if log_campaign_executed:
                log_campaign_executed(
                    download_dir,
                    store_id=store_id,
                    campaign_name=campaign_name,
                    pct_value=15,
                    min_subtotal=float(combo.get("min_subtotal", 10)),
                    max_discount="Always lowest",
                    status=status,
                )
            logger.info("DoorDash (browser-use): Campaign %s/%s done: %s [%s]", i, len(combos), campaign_name, status)
            # Brief yield to let the event loop breathe; not a hard wait
            await asyncio.sleep(1)

    else:
        logger.warning(
            "DoorDash (browser-use): No campaign combos from combined_analysis. "
            "Store IDs come from Day-Slot - {StoreID} sheets. Skip campaigns until combined_analysis is created."
        )

    await _kill_browser(browser)

"""
DoorDash Merchant Portal automation using the browser-use framework.
Runs the full workflow: login, financial report, marketing report, download(s), and campaign creation.
Returns paths to downloaded report file(s) for use by analysis_agent and marketing_agent.
"""

import asyncio
import logging
import os
import re
import time
import zipfile
from pathlib import Path
from typing import Awaitable, Callable, Optional, Tuple

# Timeouts (seconds) for each browser-use agent phase
AGENT_REPORTS_TIMEOUT = 900   # 15 min: login + create 2 reports + download both
AGENT_LOGIN_TIMEOUT = 180     # 3 min: re-login after browser restart
AGENT_RESET_TIMEOUT = 90      # 1.5 min: navigate to Marketing page between campaigns
AGENT_CAMPAIGN_TIMEOUT = 720  # 12 min: create one campaign end-to-end (increased from 540 to handle many-slot campaigns)

# Campaigns per browser session before restart; override via env for tuning
MAX_CAMPAIGNS_PER_SESSION = int(os.getenv("MAX_CAMPAIGNS_PER_SESSION", "5"))

from agents.combined_report_agent import (
    append_campaign_mappings_to_workbook,
    copy_campaign_mappings_from_previous,
    read_campaign_combos_from_mappings,
    read_campaign_mapping_statuses,
    update_campaign_mapping_status,
)
from agents.slack_agent import push_to_slack

logger = logging.getLogger(__name__)


def _build_campaign_tools():
    """
    Build a Tools instance with a custom 'set_schedule_grid' action.
    Uses CDP Input.dispatchMouseEvent (native browser input pipeline) to click grid cells,
    which reliably triggers React's synthetic event system — unlike JS dispatchEvent which doesn't.
    """
    from browser_use import Tools
    from browser_use.agent.views import ActionResult

    tools = Tools()

    _GRID_ROW_NAMES = ["Early morning", "Breakfast", "Lunch", "Afternoon", "Dinner", "Late night"]
    _GRID_COL_NAMES = ["Mon", "Tue", "Wed", "Thur", "Fri", "Sat", "Sun"]

    def _tag_label(tag: int) -> str:
        """Human-readable label for a tag number, e.g. 8 → 'Breakfast/Mon'."""
        row_idx = (tag - 1) // 7
        col_idx = (tag - 1) % 7
        return f"{_GRID_ROW_NAMES[row_idx]}/{_GRID_COL_NAMES[col_idx]}"

    @tools.action(
        description=(
            "Set the DoorDash campaign schedule grid to exactly the desired state. "
            "Pass 'wanted_tags' as a comma-separated string of tag numbers (1-42) that should be CHECKED. "
            "Grid layout: 6 rows (Early morning, Breakfast, Lunch, Afternoon, Dinner, Late night) x 7 cols (Mon-Sun). "
            "Tag 1 = Mon/Early morning, Tag 2 = Tue/Early morning, ..., Tag 7 = Sun/Early morning, "
            "Tag 8 = Mon/Breakfast, ..., Tag 42 = Sun/Late night. "
            "This action detects current cell states and only toggles cells that need to change, then clicks Save. "
            "Example: wanted_tags='1,2,3,8,9' means check Mon/Tue/Wed Early morning + Mon/Tue Breakfast. "
            "IMPORTANT: Call this ONCE after the custom schedule grid is visible. Do NOT manually click any grid cells."
        ),
    )
    async def set_schedule_grid(wanted_tags: str, browser_session) -> ActionResult:
        """Detect current grid state, toggle only cells that need changing, using CDP native clicks."""
        try:
            wanted = set()
            for t in wanted_tags.split(","):
                t = t.strip()
                if t and t.isdigit():
                    wanted.add(int(t))

            logger.info("set_schedule_grid CALLED: wanted_tags=%s → wanted=%s", wanted_tags, sorted(wanted))
            logger.info("set_schedule_grid: wanted cells: %s",
                        ", ".join(f"{t}({_tag_label(t)})" for t in sorted(wanted)))

            cdp_session = await browser_session.get_or_create_cdp_session()
            cdp_client = cdp_session.cdp_client
            session_id = cdp_session.session_id

            # Step 1: Find grid cells, detect their checked state, and get viewport coordinates.
            # Checked cells have a visible checkmark (SVG path with non-zero opacity / colored fill).
            # Unchecked cells have a hidden/transparent SVG.
            js_find_grid = """
(function() {
    // Find Weekdays button to anchor in the schedule modal
    var weekdaysBtn = null;
    var allBtns = document.querySelectorAll('button');
    for (var i = 0; i < allBtns.length; i++) {
        if (allBtns[i].textContent.trim() === 'Weekdays') { weekdaysBtn = allBtns[i]; break; }
    }
    if (!weekdaysBtn) return JSON.stringify({error: 'No Weekdays button found on page'});

    // Walk up to find modal container
    var container = weekdaysBtn;
    for (var up = 0; up < 12; up++) {
        container = container.parentElement;
        if (!container) break;
    }
    if (!container) return JSON.stringify({error: 'Could not find modal container'});

    // Find grid rows — divs with exactly 7 children that each contain an SVG
    var rows = [];
    var candidateRows = container.querySelectorAll('div[class*="StyledInlineChildren"]');
    for (var r = 0; r < candidateRows.length; r++) {
        var row = candidateRows[r];
        if (row.children.length === 7) {
            var allHaveSvg = true;
            for (var c = 0; c < 7; c++) {
                if (!row.children[c].querySelector('svg')) { allHaveSvg = false; break; }
            }
            if (allHaveSvg) rows.push(row);
        }
    }
    // Fallback
    if (rows.length < 6) {
        rows = [];
        var allDivs = container.querySelectorAll('div');
        for (var d = 0; d < allDivs.length; d++) {
            var div = allDivs[d];
            if (div.children.length === 7) {
                var ok = true;
                for (var c2 = 0; c2 < 7; c2++) {
                    if (!div.children[c2].querySelector('svg')) { ok = false; break; }
                }
                if (ok) rows.push(div);
            }
        }
    }
    if (rows.length < 6) return JSON.stringify({error: 'Found ' + rows.length + ' grid rows, expected 6'});

    // Detect checked state for each cell.
    // Strategy: check background color of the cell div, or SVG checkmark visibility.
    // DoorDash selected cells have a teal/green background; unselected have white/light gray.
    function isChecked(cellDiv) {
        var style = window.getComputedStyle(cellDiv);
        var bg = style.backgroundColor;
        // Also check the first child div (sometimes the colored background is on inner div)
        var innerDiv = cellDiv.querySelector('div');
        var innerBg = innerDiv ? window.getComputedStyle(innerDiv).backgroundColor : '';

        // Check for teal/green-ish backgrounds (selected state)
        // Teal: rgb(0, 175, 169) or similar; selected cells are NOT white/transparent
        function isTealish(color) {
            if (!color || color === 'rgba(0, 0, 0, 0)' || color === 'transparent') return false;
            // Parse rgb values
            var m = color.match(/rgba?\\((\\d+),\\s*(\\d+),\\s*(\\d+)/);
            if (!m) return false;
            var r = parseInt(m[1]), g = parseInt(m[2]), b = parseInt(m[3]);
            // White or very light (unselected)
            if (r > 240 && g > 240 && b > 240) return false;
            // Near-white grays
            if (r > 220 && g > 220 && b > 220 && Math.abs(r-g) < 15 && Math.abs(g-b) < 15) return false;
            // Has significant color = selected
            return true;
        }

        if (isTealish(bg)) return true;
        if (isTealish(innerBg)) return true;

        // Fallback: check SVG path opacity
        var svg = cellDiv.querySelector('svg');
        if (svg) {
            var path = svg.querySelector('path');
            if (path) {
                var pathStyle = window.getComputedStyle(path);
                var stroke = pathStyle.stroke;
                var fill = pathStyle.fill;
                // If path has visible stroke/fill (not transparent/none)
                if (stroke && stroke !== 'none' && stroke !== 'rgba(0, 0, 0, 0)') return true;
                if (fill && fill !== 'none' && fill !== 'rgba(0, 0, 0, 0)') {
                    // Check it's not white
                    var fm = fill.match(/rgba?\\((\\d+),\\s*(\\d+),\\s*(\\d+)/);
                    if (fm) {
                        var fr = parseInt(fm[1]), fg = parseInt(fm[2]), fb = parseInt(fm[3]);
                        if (fr < 240 || fg < 240 || fb < 240) return true;
                    }
                }
            }
            // Check svg visibility/opacity
            var svgStyle = window.getComputedStyle(svg);
            if (svgStyle.opacity === '0' || svgStyle.visibility === 'hidden') return false;
        }

        // Final fallback: check aria-checked or data attributes
        if (cellDiv.getAttribute('aria-checked') === 'true') return true;
        if (cellDiv.getAttribute('aria-checked') === 'false') return false;

        // Default: assume checked (grid usually starts fully selected)
        return true;
    }

    var result = {cells: [], saveBtn: null};
    for (var ri = 0; ri < 6; ri++) {
        for (var ci = 0; ci < 7; ci++) {
            var cell = rows[ri].children[ci];
            var rect = cell.getBoundingClientRect();
            var checked = isChecked(cell);
            result.cells.push({
                tag: ri * 7 + ci + 1,
                x: Math.round(rect.left + rect.width / 2),
                y: Math.round(rect.top + rect.height / 2),
                checked: checked
            });
        }
    }

    // Find Save button
    var modalBtns = container.querySelectorAll('button');
    for (var j = 0; j < modalBtns.length; j++) {
        var btnTxt = modalBtns[j].textContent.trim();
        if (btnTxt === 'Save' || btnTxt === 'Save schedule') {
            var sRect = modalBtns[j].getBoundingClientRect();
            result.saveBtn = {x: Math.round(sRect.left + sRect.width / 2), y: Math.round(sRect.top + sRect.height / 2)};
            break;
        }
    }
    return JSON.stringify(result);
})()
"""
            # Get grid state + coordinates
            eval_result = await cdp_client.send.Runtime.evaluate(
                params={"expression": js_find_grid, "returnByValue": True, "awaitPromise": True},
                session_id=session_id,
            )
            if eval_result.get("exceptionDetails"):
                err = eval_result["exceptionDetails"].get("text", "Unknown JS error")
                logger.warning("set_schedule_grid JS error: %s", err)
                return ActionResult(error=f"JavaScript error finding grid: {err}")

            import json
            raw_val = eval_result.get("result", {}).get("value", "{}")
            grid_info = json.loads(raw_val) if isinstance(raw_val, str) else raw_val

            if "error" in grid_info:
                logger.warning("set_schedule_grid: %s", grid_info["error"])
                return ActionResult(error=grid_info["error"])

            cells = grid_info["cells"]  # list of {tag, x, y, checked}
            save_btn = grid_info.get("saveBtn")

            if len(cells) != 42:
                return ActionResult(error=f"Expected 42 cells, found {len(cells)}")

            # Log current grid state
            currently_checked = {c["tag"] for c in cells if c.get("checked")}
            currently_unchecked = {c["tag"] for c in cells if not c.get("checked")}
            logger.info("set_schedule_grid: CURRENT STATE: %d checked, %d unchecked",
                        len(currently_checked), len(currently_unchecked))
            logger.info("set_schedule_grid: currently checked tags: %s", sorted(currently_checked))
            logger.info("set_schedule_grid: currently unchecked tags: %s", sorted(currently_unchecked))

            # Determine which cells need to be TOGGLED:
            # - Cells that are checked but should NOT be → click to uncheck
            # - Cells that are unchecked but SHOULD be → click to check
            need_uncheck = currently_checked - wanted   # checked but unwanted
            need_check = wanted - currently_checked      # unchecked but wanted
            no_change = wanted & currently_checked       # already correct

            logger.info("set_schedule_grid: PLAN: %d to uncheck, %d to check, %d already correct",
                        len(need_uncheck), len(need_check), len(no_change))
            if need_uncheck:
                logger.info("set_schedule_grid: UNCHECK: %s",
                            ", ".join(f"{t}({_tag_label(t)})" for t in sorted(need_uncheck)))
            if need_check:
                logger.info("set_schedule_grid: CHECK: %s",
                            ", ".join(f"{t}({_tag_label(t)})" for t in sorted(need_check)))

            to_click = need_uncheck | need_check  # all cells that need toggling

            # Helper: click at viewport coordinates using CDP Input.dispatchMouseEvent
            async def cdp_click(x: int, y: int):
                await cdp_client.send.Input.dispatchMouseEvent(
                    params={"type": "mousePressed", "x": x, "y": y, "button": "left", "clickCount": 1},
                    session_id=session_id,
                )
                await cdp_client.send.Input.dispatchMouseEvent(
                    params={"type": "mouseReleased", "x": x, "y": y, "button": "left", "clickCount": 1},
                    session_id=session_id,
                )
                await asyncio.sleep(0.05)  # tiny delay for React to process

            # Click all cells that need toggling using coordinates from the initial scan.
            # We process row-by-row and scroll each row into view first, but we use
            # a scroll approach that does NOT rely on re-detecting grid rows (which
            # breaks after toggling cells since unchecked cells lose their SVG checkmarks).
            clicked = 0
            cell_lookup = {c["tag"]: c for c in cells}

            for row_idx in range(6):
                row_tags = {row_idx * 7 + col + 1 for col in range(7)}
                row_needs_click = row_tags & to_click
                if not row_needs_click:
                    continue

                # Scroll the first cell of this row into view using its initial coordinates.
                # We use window.scrollTo or elementFromPoint to ensure visibility.
                first_tag = min(row_needs_click)
                first_cell = cell_lookup[first_tag]
                js_scroll_to_y = f"""
(function() {{
    // Scroll the grid modal so that y={first_cell['y']} is visible
    var el = document.elementFromPoint({first_cell['x']}, {first_cell['y']});
    if (el) {{
        el.scrollIntoView({{behavior: 'instant', block: 'center'}});
        return JSON.stringify({{ok: true}});
    }}
    // Fallback: try scrolling the modal container
    var weekdaysBtn = null;
    var allBtns = document.querySelectorAll('button');
    for (var i = 0; i < allBtns.length; i++) {{
        if (allBtns[i].textContent.trim() === 'Weekdays') {{ weekdaysBtn = allBtns[i]; break; }}
    }}
    if (weekdaysBtn) {{
        var container = weekdaysBtn;
        for (var up = 0; up < 12; up++) {{ container = container.parentElement; if (!container) break; }}
        if (container && container.scrollTo) {{
            container.scrollTo(0, {max(0, first_cell['y'] - 300)});
        }}
    }}
    return JSON.stringify({{ok: true}});
}})()
"""
                try:
                    await cdp_client.send.Runtime.evaluate(
                        params={"expression": js_scroll_to_y, "returnByValue": True, "awaitPromise": True},
                        session_id=session_id,
                    )
                except Exception as scroll_err:
                    logger.debug("set_schedule_grid: scroll hint for row %d: %s", row_idx, scroll_err)

                await asyncio.sleep(0.15)

                # Re-read coordinates for THIS row's cells after scrolling
                # Use a simple JS that finds elements at the original x,y or nearby
                js_refresh_row = f"""
(function() {{
    var tags = {list(sorted(row_needs_click))};
    var origCoords = {json.dumps({{t: {{"x": cell_lookup[t]["x"], "y": cell_lookup[t]["y"]}} for t in sorted(row_needs_click)}})};
    var result = [];
    for (var i = 0; i < tags.length; i++) {{
        var tag = tags[i];
        var ox = origCoords[tag].x;
        var oy = origCoords[tag].y;
        var el = document.elementFromPoint(ox, oy);
        if (el) {{
            var rect = el.getBoundingClientRect();
            result.push({{tag: tag, x: Math.round(rect.left + rect.width/2), y: Math.round(rect.top + rect.height/2)}});
        }} else {{
            result.push({{tag: tag, x: ox, y: oy}});
        }}
    }}
    return JSON.stringify(result);
}})()
"""
                try:
                    refresh_result = await cdp_client.send.Runtime.evaluate(
                        params={"expression": js_refresh_row, "returnByValue": True, "awaitPromise": True},
                        session_id=session_id,
                    )
                    refresh_raw = refresh_result.get("result", {}).get("value", "[]")
                    refreshed = json.loads(refresh_raw) if isinstance(refresh_raw, str) else refresh_raw
                    # Update cell_lookup with fresh coords
                    for rc in refreshed:
                        if rc["tag"] in cell_lookup:
                            cell_lookup[rc["tag"]]["x"] = rc["x"]
                            cell_lookup[rc["tag"]]["y"] = rc["y"]
                except Exception as refresh_err:
                    logger.debug("set_schedule_grid: coord refresh for row %d: %s (using original)", row_idx, refresh_err)

                # Click each cell that needs toggling in this row
                for tag in sorted(row_needs_click):
                    c = cell_lookup[tag]
                    action_type = "UNCHECK" if tag in need_uncheck else "CHECK"
                    logger.info("set_schedule_grid: %s tag %d (%s) at (%d, %d)",
                                action_type, tag, _tag_label(tag), c["x"], c["y"])
                    await cdp_click(c["x"], c["y"])
                    clicked += 1

            # Verify: scroll Save into view, re-read full grid state
            await asyncio.sleep(0.3)  # let React settle

            # Scroll Save button into view and re-read entire grid
            js_verify_and_scroll_save = """
(function() {
    var weekdaysBtn = null;
    var allBtns = document.querySelectorAll('button');
    for (var i = 0; i < allBtns.length; i++) {
        if (allBtns[i].textContent.trim() === 'Weekdays') { weekdaysBtn = allBtns[i]; break; }
    }
    if (!weekdaysBtn) return JSON.stringify({error: 'No Weekdays button'});
    var container = weekdaysBtn;
    for (var up = 0; up < 12; up++) { container = container.parentElement; if (!container) break; }

    var rows = [];
    var candidateRows = container.querySelectorAll('div[class*="StyledInlineChildren"]');
    for (var r = 0; r < candidateRows.length; r++) {
        var row = candidateRows[r];
        if (row.children.length === 7) {
            var ok = true;
            for (var c = 0; c < 7; c++) { if (!row.children[c].querySelector('svg')) { ok = false; break; } }
            if (ok) rows.push(row);
        }
    }
    if (rows.length < 6) {
        rows = [];
        var allDivs = container.querySelectorAll('div');
        for (var d = 0; d < allDivs.length; d++) {
            var div = allDivs[d];
            if (div.children.length === 7) {
                var ok2 = true;
                for (var c2 = 0; c2 < 7; c2++) { if (!div.children[c2].querySelector('svg')) { ok2 = false; break; } }
                if (ok2) rows.push(div);
            }
        }
    }

    // Re-read checked state for all cells
    function isChecked(cellDiv) {
        var style = window.getComputedStyle(cellDiv);
        var bg = style.backgroundColor;
        var innerDiv = cellDiv.querySelector('div');
        var innerBg = innerDiv ? window.getComputedStyle(innerDiv).backgroundColor : '';
        function isTealish(color) {
            if (!color || color === 'rgba(0, 0, 0, 0)' || color === 'transparent') return false;
            var m = color.match(/rgba?\\((\\d+),\\s*(\\d+),\\s*(\\d+)/);
            if (!m) return false;
            var r = parseInt(m[1]), g = parseInt(m[2]), b = parseInt(m[3]);
            if (r > 240 && g > 240 && b > 240) return false;
            if (r > 220 && g > 220 && b > 220 && Math.abs(r-g) < 15 && Math.abs(g-b) < 15) return false;
            return true;
        }
        if (isTealish(bg)) return true;
        if (isTealish(innerBg)) return true;
        var svg = cellDiv.querySelector('svg');
        if (svg) {
            var path = svg.querySelector('path');
            if (path) {
                var pathStyle = window.getComputedStyle(path);
                var stroke = pathStyle.stroke;
                if (stroke && stroke !== 'none' && stroke !== 'rgba(0, 0, 0, 0)') return true;
            }
        }
        if (cellDiv.getAttribute('aria-checked') === 'true') return true;
        if (cellDiv.getAttribute('aria-checked') === 'false') return false;
        return true;
    }

    var result = {cells: [], saveBtn: null};
    if (rows.length >= 6) {
        for (var ri = 0; ri < 6; ri++) {
            for (var ci = 0; ci < 7; ci++) {
                var cell = rows[ri].children[ci];
                result.cells.push({tag: ri * 7 + ci + 1, checked: isChecked(cell)});
            }
        }
    }

    // Scroll Save button into view
    var modalBtns = container.querySelectorAll('button');
    for (var j = 0; j < modalBtns.length; j++) {
        var btnTxt = modalBtns[j].textContent.trim();
        if (btnTxt === 'Save' || btnTxt === 'Save schedule') {
            modalBtns[j].scrollIntoView({behavior: 'instant', block: 'center'});
            var sRect = modalBtns[j].getBoundingClientRect();
            result.saveBtn = {x: Math.round(sRect.left + sRect.width / 2), y: Math.round(sRect.top + sRect.height / 2)};
            break;
        }
    }
    return JSON.stringify(result);
})()
"""
            verify_result = await cdp_client.send.Runtime.evaluate(
                params={"expression": js_verify_and_scroll_save, "returnByValue": True, "awaitPromise": True},
                session_id=session_id,
            )
            verify_raw = verify_result.get("result", {}).get("value", "{}")
            verify_info = json.loads(verify_raw) if isinstance(verify_raw, str) else verify_raw

            if "cells" in verify_info and verify_info["cells"]:
                after_checked = {c["tag"] for c in verify_info["cells"] if c.get("checked")}
                logger.info("set_schedule_grid: AFTER clicks: checked=%s", sorted(after_checked))
                if after_checked == wanted:
                    logger.info("set_schedule_grid: VERIFIED — grid matches wanted state perfectly")
                else:
                    missing = wanted - after_checked
                    extra = after_checked - wanted
                    if missing:
                        logger.warning("set_schedule_grid: MISMATCH — still unchecked but wanted: %s",
                                       ", ".join(f"{t}({_tag_label(t)})" for t in sorted(missing)))
                    if extra:
                        logger.warning("set_schedule_grid: MISMATCH — still checked but unwanted: %s",
                                       ", ".join(f"{t}({_tag_label(t)})" for t in sorted(extra)))
                    # Attempt correction using original cell coordinates
                    corrections = missing | extra
                    if corrections and len(corrections) <= 15:
                        logger.info("set_schedule_grid: CORRECTING %d cells...", len(corrections))
                        for corr_tag in sorted(corrections):
                            if corr_tag in cell_lookup:
                                cc = cell_lookup[corr_tag]
                                logger.info("set_schedule_grid: CORRECT tag %d (%s) at (%d, %d)",
                                            corr_tag, _tag_label(corr_tag), cc["x"], cc["y"])
                                await cdp_click(cc["x"], cc["y"])
                                clicked += 1
                        await asyncio.sleep(0.2)

            # Click Save
            save_clicked = False
            # Re-read save button position (may have shifted)
            if verify_info and "saveBtn" in verify_info and verify_info["saveBtn"]:
                save_btn = verify_info["saveBtn"]
            if save_btn:
                await asyncio.sleep(0.2)
                logger.info("set_schedule_grid: clicking Save at (%d, %d)", save_btn["x"], save_btn["y"])
                await cdp_click(save_btn["x"], save_btn["y"])
                save_clicked = True
                clicked += 1

            actual_toggled = clicked - (1 if save_clicked else 0)  # exclude Save click
            expected_toggles = len(to_click)
            if save_clicked and actual_toggled >= expected_toggles:
                status_word = "SUCCESS"
            elif save_clicked and actual_toggled > 0:
                status_word = "PARTIAL"
            else:
                status_word = "ERROR"
            msg = f"{status_word}: {actual_toggled}/{expected_toggles} cells toggled ({len(need_uncheck)} to uncheck, {len(need_check)} to check), {clicked} CDP clicks"
            if not save_clicked:
                msg += ". Save button not found — click Save manually."
            if actual_toggled < expected_toggles:
                msg += f". WARNING: only {actual_toggled} of {expected_toggles} cells were clicked!"
            msg += f" wanted={len(wanted)}/42"
            logger.info("set_schedule_grid: %s", msg)
            if status_word == "ERROR":
                return ActionResult(error=msg)
            return ActionResult(extracted_content=msg)
        except Exception as e:
            logger.warning("set_schedule_grid failed: %s", e)
            return ActionResult(error=f"set_schedule_grid error: {e}")

    return tools

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

    selected_set = set(slot_tags)
    all_tags = set(range(1, 43))
    unselected_set = all_tags - selected_set

    _GRID_ROWS = ["Early morning", "Breakfast", "Lunch", "Afternoon", "Dinner", "Late night"]
    _GRID_COLS = ["Mon", "Tue", "Wed", "Thur", "Fri", "Sat", "Sun"]

    def _group_by_row(tag_set):
        rows = {}
        for t in sorted(tag_set):
            row_idx = (t - 1) // 7
            row_name = _GRID_ROWS[row_idx]
            col_name = _GRID_COLS[(t - 1) % 7]
            rows.setdefault(row_name, []).append((t, col_name))
        return rows

    # Build fallback manual instructions (used ONLY if set_schedule_grid fails twice)
    if len(selected_set) == 42:
        manual_fallback = "All 42 cells should already be selected. Just click Save."
    elif len(unselected_set) <= 20:
        grouped = _group_by_row(unselected_set)
        lines = []
        for row_name, cells in grouped.items():
            cols = ", ".join(col for _, col in cells)
            lines.append(f"  - {row_name} row: click {cols}")
        manual_fallback = f"DESELECT these {len(unselected_set)} cells (click each ONCE):\n" + "\n".join(lines) + "\n  Then click Save."
    else:
        grouped = _group_by_row(selected_set)
        lines = []
        for row_name, cells in grouped.items():
            cols = ", ".join(col for _, col in cells)
            lines.append(f"  - {row_name} row: click {cols}")
        manual_fallback = (
            f"Click Weekdays ONCE, then Weekends ONCE (grid now empty). "
            f"Then SELECT these {len(selected_set)} cells (click each ONCE):\n"
            + "\n".join(lines)
            + "\n  Then click Save. NEVER click Weekdays/Weekends again."
        )

    schedule_instructions = f"""- IMPORTANT: Use the set_schedule_grid action to configure the grid automatically.
- Call: set_schedule_grid(wanted_tags="{tags_str}")
- This action programmatically checks/unchecks the correct cells and clicks Save. It is 100% reliable.
- Do NOT manually click any grid cells, "Weekdays", or "Weekends" buttons. The action handles everything.
- If set_schedule_grid returns SUCCESS, proceed to STEP 4B immediately.
- If set_schedule_grid returns PARTIAL (Save not found), click "Save" manually once, then proceed to STEP 4B.
- If set_schedule_grid returns ERROR twice, do it manually: {manual_fallback}"""

    return f"""
ROLE: You are automating campaign creation on DoorDash Merchant Portal. You are already logged in.

RULES:
- Do NOT go to the login page or create/download reports.
- Do NOT click "Get started" (that is for BOGO, not discount campaigns).
- Do NOT click "Create promotion" until step 6 explicitly says to.
- If a modal fails to open after clicking Edit, wait 3s, scroll to make section visible, click Edit again ONCE.

CAMPAIGN: {campaign_name} | STORE: {store_id} ({store_name if store_name else "N/A"}) | SUBTOTAL: ${min_subtotal} | TAGS: {tags_str}

STEP 1 — Open campaign builder:
- Click "Marketing" in the left sidebar. Wait for page to load.
- Click "Run a campaign". Wait for campaign type cards.
- Find "Discount for all customers" card, click "Select".
- Click "Customize your campaign" in the right panel. Wait for form to load.

STEP 2 — Select store:
- Click Edit (pencil) next to "Stores". Wait for modal.
- Click "Select All" to deselect all stores.
- Search "{store_id}" in search bar. If found, select it. If NOT found, search "{store_name}" instead.
- Select ONLY the one matching store. Click "Save".

STEP 3 — Set customer incentive:
- Click Edit (pencil) next to "Customer incentive". Wait for modal.
- Click "15%" radio button.
- Click "Custom" under Minimum subtotal. Click the input field.
- Select all text (triple-click), type: {min_subtotal}
- Wait 2s. VERIFY field shows {min_subtotal} or ${min_subtotal}. If it shows $25 or wrong value, clear and retype.
- Click LEFTMOST button under "Maximum discount amount" (smallest value).
- Click "Save".

STEP 4 — Set schedule:
- Click Edit (pencil) next to "Scheduling". Wait for modal with grid.
- Click "Set a custom schedule". Wait for grid.
- Grid: 6 rows (Early morning, Breakfast, Lunch, Afternoon, Dinner, Late night) x 7 cols (Mon-Sun).
{schedule_instructions}

STEP 4B — Re-confirm Maximum discount amount (MANDATORY after schedule):
- Click Edit (pencil) next to "Customer incentive". Wait for modal to open.
- Look at the "Maximum discount amount" section. Click the LEFTMOST (smallest/lowest) value button.
- Click "Save".
- This step is REQUIRED because DoorDash may change available max discount options after the schedule is modified.

STEP 5 — Set campaign name:
- Click Edit (pencil) next to "Campaign name".
- Clear all text, type exactly: {campaign_name}
- Click "Save".

STEP 6 — Final verify and create (do NOT skip):
- BEFORE clicking "Create promotion", read the campaign summary panel:
  - Confirm it shows "${min_subtotal}" (not $25 unless target is $25). If wrong, click Edit next to "Customer incentive", fix it, Save.
  - Confirm campaign name shows "{campaign_name}".
- Click "Create promotion". Wait for success confirmation.
- IMPORTANT: If you see a message like "The details of this campaign are the same as one of your live campaigns" or any duplication warning, do NOT try to fix it. Just use the done action immediately and say: "{campaign_name}" DUPLICATE for store {store_id}.

DONE: Use done action. Say: "{campaign_name}" created for store {store_id}.
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
    """
    Browser with download path set to the given directory.
    keep_alive=True keeps browser open for reuse.

    Connection priority:
      1. LOCAL_BROWSER_CDP_URL — Remote headless Chrome via CDP (GCP/cloud deployment)
      2. Local Chrome executable (macOS laptop)
      3. Default browser-use browser
    """
    from browser_use import Browser

    downloads_path = str(download_dir.resolve())

    # --- Remote CDP (headless Chrome on GCP VM, Browserless, etc.) ---
    cdp_url = os.getenv("LOCAL_BROWSER_CDP_URL", "").strip()
    if cdp_url:
        logger.info("Connecting to remote Chrome via CDP: %s", cdp_url)
        return Browser(
            cdp_url=cdp_url,
            downloads_path=downloads_path,
            enable_default_extensions=False,
            keep_alive=keep_alive,
        )

    # --- Local Chrome executable (laptop/macOS) ---
    common = dict(
        downloads_path=downloads_path,
        enable_default_extensions=False,
        keep_alive=keep_alive,
    )
    if os.name == "posix":
        chrome = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
        if Path(chrome).exists():
            return Browser(executable_path=chrome, **common)

    # --- Default browser-use browser ---
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
    campaigns_only_combined_path: Optional[Path] = None,
) -> None:
    """
    Single browser session: login → reports → download → (browser stays open) →
    run analysis_callback(marketing_path, financial_path) → returns combined_path →
    for each (store, day, slot) combo from combined_analysis Day-Slot sheets, run campaign (no login again) → close browser.

    If campaigns_only_combined_path is provided, skip Phase 1 (reports) and analysis entirely.
    Just login and run campaigns from the existing combined analysis file.

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

    llm = _get_llm()
    browser = _get_browser(download_dir, keep_alive=True)

    # --- CAMPAIGNS-ONLY MODE: skip reports & analysis, just login and run campaigns ---
    if campaigns_only_combined_path and Path(campaigns_only_combined_path).is_file():
        combined_path = Path(campaigns_only_combined_path)
        logger.info("=" * 70)
        logger.info("CAMPAIGNS-ONLY MODE — skipping reports & analysis")
        logger.info("  Email: %s", email)
        logger.info("  Combined analysis: %s", combined_path)
        logger.info("  Download dir: %s", download_dir)
        logger.info("=" * 70)
        push_to_slack(
            f"*Campaigns-only mode* — skipping reports & analysis\n"
            f"Email: {email}\n"
            f"Using: {combined_path.name}"
        )

        # Login only
        login_task = (
            f"Go to https://merchant-portal.doordash.com/merchant/login\n"
            f"Enter email: {email}, click 'Continue to Log In'.\n"
            f"On the next screen, enter password: {password}, click 'Log In'.\n"
            f"Wait for the dashboard to load. Use done action to finish."
        )
        try:
            login_agent = Agent(task=login_task, llm=llm, browser=browser)
            await asyncio.wait_for(login_agent.run(), timeout=AGENT_LOGIN_TIMEOUT)
            logger.info("Login successful")
            push_to_slack(f"Login successful for {email}")
        except Exception as e:
            await _kill_browser(browser)
            push_to_slack(f"*FAILED* — Login failed for {email}: {e}")
            raise e

        # Read combos from Campaign Mappings, skip Successful ones
        all_combos = read_campaign_combos_from_mappings(combined_path)
        if not all_combos:
            logger.warning("No campaign mappings found in %s", combined_path)
            await _kill_browser(browser)
            return
        before = len(all_combos)
        combos = [c for c in all_combos if c.get("status") != "Successful"]
        skipped = before - len(combos)
        use_slots_csv = False
        logger.info(
            "DoorDash: %d campaign mappings; %d already Successful, %d remaining.",
            before, skipped, len(combos),
        )
        push_to_slack(f"Resuming: {skipped} Successful (skipped), {len(combos)} to run")

    else:
        # --- FULL MODE: Login → Reports → Analysis → Campaigns ---
        reports_task = get_task_description_reports_only(
            email=email,
            password=password,
            start_date=start_date,
            end_date=end_date,
        )

        agent = Agent(task=reports_task, llm=llm, browser=browser)

        logger.info("=" * 70)
        logger.info("PHASE 1: LOGIN + REPORTS (login, create financial & marketing, download)")
        logger.info("  Email: %s", email)
        logger.info("  Date range: %s to %s", start_date, end_date)
        logger.info("  Download dir: %s", download_dir)
        logger.info("=" * 70)
        push_to_slack(
            f"*Phase 1 started* — Login + Reports\n"
            f"Email: {email}\n"
            f"Date range: {start_date} to {end_date}"
        )
        phase1_start = time.time()
        try:
            await asyncio.wait_for(agent.run(), timeout=AGENT_REPORTS_TIMEOUT)
            phase1_elapsed = time.time() - phase1_start
            logger.info("Phase 1: Login + reports completed in %.0fs", phase1_elapsed)
            push_to_slack(f"Login successful for {email} ({phase1_elapsed:.0f}s)")
        except asyncio.TimeoutError:
            await _kill_browser(browser)
            push_to_slack(f"*FAILED* — Phase 1 timed out after {AGENT_REPORTS_TIMEOUT}s for {email}")
            raise RuntimeError(f"Phase 1 (reports) timed out after {AGENT_REPORTS_TIMEOUT}s")
        except Exception as e:
            await _kill_browser(browser)
            push_to_slack(f"*FAILED* — Login failed for {email}: {e}")
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

        # --- Find previous combined analysis from sibling run folders ---
        _all_combined: list[Path] = []
        _dir_name = download_dir.name
        _prefix_match = re.match(r"^(.+)-\d{8}_\d{6}$", _dir_name)
        if _prefix_match:
            _email_prefix = _prefix_match.group(1)
            for sibling in download_dir.parent.glob(f"{_email_prefix}-*"):
                if sibling == download_dir:
                    continue
                _all_combined.extend(sibling.glob("combined_analysis_*.xlsx"))
        _all_combined.extend(download_dir.glob("combined_analysis_*.xlsx"))
        old_combined_files = sorted(_all_combined, key=lambda f: f.parent.name, reverse=True)
        if old_combined_files:
            logger.info("Found previous combined analysis: %s", old_combined_files[0])

        logger.info("=" * 70)
        logger.info("ANALYSIS PHASE: Financial + Marketing analysis, combined report, Google Sheets")
        logger.info("=" * 70)
        push_to_slack("*Analysis phase started* — Processing downloaded reports...")
        analysis_start = time.time()
        combined_path = await analysis_callback(marketing_path, financial_path)
        analysis_elapsed = time.time() - analysis_start
        logger.info("Analysis phase completed in %.0fs", analysis_elapsed)

        if not combined_path or not Path(combined_path).is_file():
            logger.warning("No combined_analysis file returned — campaigns will have no slot data")
            push_to_slack("*Warning:* Combined analysis not created — check financial/marketing report paths")
        else:
            push_to_slack(f"Combined analysis created ({analysis_elapsed:.0f}s) — {combined_path}")

        # --- Copy Campaign Mappings from previous run if available, then run non-Successful ones ---
        combos = []
        use_slots_csv = False
        copied_from_previous = False

        if old_combined_files and combined_path and Path(combined_path).is_file():
            copied = copy_campaign_mappings_from_previous(old_combined_files[0], Path(combined_path))
            if copied:
                all_combos = read_campaign_combos_from_mappings(Path(combined_path))
                if all_combos:
                    copied_from_previous = True
                    before = len(all_combos)
                    combos = [c for c in all_combos if c.get("status") != "Successful"]
                    skipped = before - len(combos)
                    logger.info(
                        "DoorDash: Copied %d campaign mappings from previous run; %d already Successful, %d remaining.",
                        before, skipped, len(combos),
                    )
                    if skipped:
                        push_to_slack(f"Resuming: {skipped} campaigns already Successful, {len(combos)} remaining")

        # Fallback: build combos from Day-Slot sheets if no previous Campaign Mappings available
        if not copied_from_previous:
            if get_campaign_combos_from_slots_and_combined and slots_csv_path.is_file() and combined_path and Path(combined_path).is_file():
                combos = get_campaign_combos_from_slots_and_combined(slots_csv_path, Path(combined_path))
                if combos:
                    use_slots_csv = True
                    logger.info("DoorDash (browser-use): Found %s campaign combos from Day-Slot sheets + slots grid (one per min_subtotal per store).", len(combos))
            if not combos and combined_path and Path(combined_path).is_file() and get_all_campaign_combos_from_combined_analysis:
                combos = get_all_campaign_combos_from_combined_analysis(Path(combined_path))
                logger.info("DoorDash (browser-use): Found %s campaign combos from Day-Slot sheets (store IDs from sheets).", len(combos))

            # Push fresh campaign mappings to combined analysis sheet
            if combined_path and Path(combined_path).is_file() and combos:
                mappings = []
                for c in combos:
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

        total = len(combos)
        logger.info("=" * 70)
        logger.info("PHASE 2: CAMPAIGN CREATION — %s campaigns to create", total)
        logger.info("Source: %s | Browser restart every %s campaigns", "slots.csv" if use_slots_csv else "combined_analysis", MAX_CAMPAIGNS_PER_SESSION)
        logger.info("=" * 70)
        push_to_slack(
            f"*Phase 2 started* — {total} campaigns to create\n"
            f"Source: {'slots.csv' if use_slots_csv else 'combined_analysis'} | "
            f"Timeout: {AGENT_CAMPAIGN_TIMEOUT}s/campaign | Browser restart every {MAX_CAMPAIGNS_PER_SESSION}"
        )

        # Tracking stats
        phase2_start = time.time()
        stats = {"successful": 0, "failed": 0, "skipped": 0, "timed_out": 0}
        campaign_times: list[float] = []

        for i, combo in enumerate(combos, 1):
            campaign_start = time.time()

            # --- Browser restart every N campaigns ---
            if i > 1 and (i - 1) % MAX_CAMPAIGNS_PER_SESSION == 0:
                logger.info("--- Browser restart after %d campaigns (session limit: %d) ---", i - 1, MAX_CAMPAIGNS_PER_SESSION)
                await _kill_browser(browser)
                browser = _get_browser(download_dir, keep_alive=True)
                relogin_ok = False
                for relogin_attempt in range(1, 3):
                    try:
                        login_agent = Agent(task=relogin_task, llm=llm, browser=browser)
                        await asyncio.wait_for(login_agent.run(), timeout=AGENT_LOGIN_TIMEOUT)
                        logger.info("--- Re-login successful (attempt %d) ---", relogin_attempt)
                        relogin_ok = True
                        break
                    except asyncio.TimeoutError:
                        logger.warning("--- Re-login attempt %d timed out ---", relogin_attempt)
                        await _kill_browser(browser)
                        browser = _get_browser(download_dir, keep_alive=True)
                    except Exception as e:
                        logger.warning("--- Re-login attempt %d failed: %s ---", relogin_attempt, e)
                        await _kill_browser(browser)
                        browser = _get_browser(download_dir, keep_alive=True)
                if not relogin_ok:
                    elapsed = time.time() - phase2_start
                    push_to_slack(
                        f"*ABORTED* — Re-login failed at campaign {i}/{total}\n"
                        f"Completed: {stats['successful']} ok, {stats['failed']} failed, {stats['skipped']} skipped\n"
                        f"Time elapsed: {elapsed/60:.0f} min"
                    )
                    logger.error("Re-login failed after 2 attempts; stopping campaign loop")
                    await _kill_browser(browser)
                    return

            # --- Navigation reset ---
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
                logger.warning("[%d/%d] Nav reset timed out; trying sidebar fallback", i, total)
                try:
                    fallback_agent = Agent(task=reset_task, llm=llm, browser=browser)
                    await asyncio.wait_for(fallback_agent.run(), timeout=AGENT_RESET_TIMEOUT)
                except Exception:
                    logger.warning("[%d/%d] Sidebar fallback also failed", i, total)
            except Exception as e:
                logger.warning("[%d/%d] Nav reset failed: %s; continuing", i, total, e)

            # --- Health check every 5 campaigns (not on restart boundaries) ---
            if i > 1 and (i - 1) % MAX_CAMPAIGNS_PER_SESSION != 0 and (i - 1) % 5 == 0:
                try:
                    health_agent = Agent(
                        task="Check if the DoorDash Merchant Portal page is loaded and interactive. "
                             "Look for sidebar navigation, page content, or any visible UI elements. "
                             "If the page is blank, showing only a spinner, or unresponsive, say 'PAGE_BLANK'. "
                             "Otherwise say 'PAGE_OK'. Use the done action.",
                        llm=llm, browser=browser,
                    )
                    health_history = await asyncio.wait_for(health_agent.run(), timeout=30)
                    health_result = ""
                    if health_history and hasattr(health_history, "final_result"):
                        val = health_history.final_result
                        health_result = str(val() if callable(val) else val) if val is not None else ""
                    if "PAGE_BLANK" in health_result.upper():
                        logger.warning("[%d/%d] Health check: page blank — restarting browser", i, total)
                        await _kill_browser(browser)
                        browser = _get_browser(download_dir, keep_alive=True)
                        login_agent = Agent(task=relogin_task, llm=llm, browser=browser)
                        await asyncio.wait_for(login_agent.run(), timeout=AGENT_LOGIN_TIMEOUT)
                        push_to_slack(f"Browser auto-restarted (blank page detected) before campaign {i}/{total}")
                        reset_agent = Agent(task=nav_to_marketing_task, llm=llm, browser=browser)
                        await asyncio.wait_for(reset_agent.run(), timeout=AGENT_RESET_TIMEOUT)
                except Exception as health_err:
                    logger.debug("Health check error (non-fatal): %s", health_err)

            # --- Campaign details ---
            campaign_name = str(combo.get("campaign_name", ""))
            store_id = str(combo.get("store_id", ""))
            min_subtotal = str(combo.get("min_subtotal", "10"))
            slot_count = len(combo.get("slot_tags", []))

            # Progress calculation
            pct = (i / total) * 100
            avg_time = sum(campaign_times) / len(campaign_times) if campaign_times else 0
            eta_sec = avg_time * (total - i) if campaign_times else 0
            eta_str = f"{eta_sec/60:.0f}m" if eta_sec > 0 else "calculating..."

            logger.info(
                "[%d/%d] (%.0f%%) %s — store %s, $%s, %d slots | ETA: %s",
                i, total, pct, campaign_name, store_id, min_subtotal, slot_count, eta_str,
            )

            # --- Run campaign agent ---
            campaign_task = get_task_description_campaign_for_subtotal_combo(combo)
            status = "Failed"
            try:
                campaign_tools = _build_campaign_tools()
                campaign_agent = Agent(task=campaign_task, llm=llm, browser=browser, tools=campaign_tools)
                history = await asyncio.wait_for(campaign_agent.run(), timeout=AGENT_CAMPAIGN_TIMEOUT)
                completed_ok = True
                if history is not None:
                    if hasattr(history, "is_successful") and callable(history.is_successful):
                        completed_ok = history.is_successful()
                    elif hasattr(history, "final_result"):
                        val = history.final_result
                        completed_ok = bool(val() if callable(val) else val) if val is not None else False

                # Check for duplicate campaign
                final_text = ""
                if history is not None and hasattr(history, "final_result"):
                    val = history.final_result
                    final_text = str(val() if callable(val) else val) if val is not None else ""
                duplicate_phrases = [
                    "same as one of your live campaigns",
                    "duplicate",
                    "already exists",
                    "campaign are the same",
                ]
                is_duplicate = any(p in final_text.lower() for p in duplicate_phrases)

                if is_duplicate:
                    status = "Skipped (duplicate)"
                    stats["skipped"] += 1
                elif completed_ok:
                    status = "Successful"
                    stats["successful"] += 1
                else:
                    status = "Failed"
                    stats["failed"] += 1
            except asyncio.TimeoutError:
                status = "Failed"
                stats["timed_out"] += 1
                stats["failed"] += 1
            except Exception as e:
                status = "Failed"
                stats["failed"] += 1
                logger.warning("[%d/%d] %s error: %s", i, total, campaign_name, e)

            # --- Timing ---
            campaign_elapsed = time.time() - campaign_start
            campaign_times.append(campaign_elapsed)

            # --- Terminal log with result ---
            status_icon = {"Successful": "OK", "Skipped (duplicate)": "SKIP", "Failed": "FAIL"}.get(status, "FAIL")
            logger.info(
                "[%d/%d] %s %s (%.0fs) | Running: %d ok, %d fail, %d skip, %d timeout",
                i, total, status_icon, campaign_name, campaign_elapsed,
                stats["successful"], stats["failed"], stats["skipped"], stats["timed_out"],
            )

            # --- Slack: per-campaign status + periodic progress ---
            if status == "Successful":
                push_to_slack(f"[{i}/{total}] {campaign_name} — done ({campaign_elapsed:.0f}s)")
            elif status == "Skipped (duplicate)":
                push_to_slack(f"[{i}/{total}] {campaign_name} — skipped (duplicate already live)")
            elif "timed_out" in str(stats.get("_last_reason", "")) or campaign_elapsed >= AGENT_CAMPAIGN_TIMEOUT - 5:
                push_to_slack(f"[{i}/{total}] {campaign_name} — timed out ({AGENT_CAMPAIGN_TIMEOUT}s limit)")
            else:
                push_to_slack(f"[{i}/{total}] {campaign_name} — failed ({campaign_elapsed:.0f}s)")

            # Slack progress summary every 10 campaigns
            if i % 10 == 0 or i == total:
                elapsed_total = time.time() - phase2_start
                remaining = total - i
                eta_total = (elapsed_total / i) * remaining if i > 0 else 0
                push_to_slack(
                    f"*Progress: {i}/{total} ({pct:.0f}%)*\n"
                    f"Results: {stats['successful']} ok | {stats['failed']} failed | {stats['skipped']} skipped | {stats['timed_out']} timed out\n"
                    f"Avg: {sum(campaign_times)/len(campaign_times):.0f}s/campaign | Elapsed: {elapsed_total/60:.0f}m | ETA: {eta_total/60:.0f}m remaining"
                )

            # --- Write status to tracking ---
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

            await asyncio.sleep(1)

        # --- Final summary ---
        total_elapsed = time.time() - phase2_start
        success_rate = (stats["successful"] / total * 100) if total > 0 else 0
        avg_campaign = sum(campaign_times) / len(campaign_times) if campaign_times else 0

        logger.info("=" * 70)
        logger.info("PHASE 2 COMPLETE")
        logger.info("  Total:      %d campaigns in %.0f min (%.1f hrs)", total, total_elapsed / 60, total_elapsed / 3600)
        logger.info("  Successful: %d (%.0f%%)", stats["successful"], success_rate)
        logger.info("  Failed:     %d (timed out: %d)", stats["failed"], stats["timed_out"])
        logger.info("  Skipped:    %d (duplicate)", stats["skipped"])
        logger.info("  Avg time:   %.0fs per campaign", avg_campaign)
        logger.info("  Est. cost:  $%.2f (@ $0.002/step)", total * avg_campaign / 6.8 * 0.002)  # rough estimate
        logger.info("=" * 70)

        push_to_slack(
            f"*Phase 2 complete*\n"
            f"Campaigns: {total} total | {stats['successful']} ok | {stats['failed']} failed | {stats['skipped']} skipped\n"
            f"Success rate: {success_rate:.0f}% | Timeouts: {stats['timed_out']}\n"
            f"Time: {total_elapsed/60:.0f} min ({total_elapsed/3600:.1f} hrs) | Avg: {avg_campaign:.0f}s/campaign"
        )

    else:
        logger.warning(
            "DoorDash (browser-use): No campaign combos from combined_analysis. "
            "Store IDs come from Day-Slot - {StoreID} sheets. Skip campaigns until combined_analysis is created."
        )

    await _kill_browser(browser)

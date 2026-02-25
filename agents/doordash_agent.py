"""
DoorDashAgent: login, 2FA via GmailAgent OTP, navigate to Reports,
create marketing report then financial report (same date range), download both to ./downloads.
"""

import asyncio
import logging
from pathlib import Path
from typing import Callable, Awaitable, Optional, Tuple

from playwright.async_api import Page

logger = logging.getLogger(__name__)

DOORDASH_MERCHANT_URL = "https://merchant-portal.doordash.com/merchant/"
REPORT_WAIT_SEC = 30  # Wait for report generation (marketing)
FINANCIAL_REPORT_WAIT_SEC = 30  # Wait after financial report creation
STEP_DELAY_SEC = 3  # Delay between steps for page stability
DOWNLOAD_TIMEOUT_MS = 60_000  # Wait for download to start (marketing)
FINANCIAL_DOWNLOAD_TIMEOUT_MS = 120_000  # Financial file can be large, allow 2 min
DEFAULT_DOWNLOAD_DIR = Path(__file__).resolve().parent.parent / "downloads"


class DoorDashAgent:
    """Handles DoorDash merchant portal login, 2FA, marketing report download, then financial report download."""

    def __init__(
        self,
        page: Page,
        email: str,
        password: str,
        get_otp_callback: Callable[[], Awaitable[Optional[str]]],
        download_dir: Optional[Path] = None,
    ) -> None:
        self.page = page
        self.email = email
        self.password = password
        self.get_otp = get_otp_callback
        self.download_dir = download_dir or DEFAULT_DOWNLOAD_DIR
        self._marketing_path: Optional[Path] = None
        self._financial_path: Optional[Path] = None

    async def run(
        self,
        start_date: str = "01/01/2026",
        end_date: str = "01/31/2026",
    ) -> Tuple[Optional[Path], Optional[Path]]:
        """
        Full workflow: login, 2FA; create marketing report (no download); create financial report (no download);
        download financial report (latest) first, then download marketing report (previous); then logout.
        Returns (marketing_download_path, financial_download_path).
        """
        self.download_dir.mkdir(parents=True, exist_ok=True)

        await self._login()
        await self._handle_2fa_if_present()

        # Create both reports without downloading
        try:
            await self._do_one_report("marketing", start_date, end_date, skip_download=True)
            logger.info("DoorDashAgent: Marketing report created, creating financial report")
        except Exception as e:
            logger.warning("DoorDashAgent: Marketing report create failed: %s", e)
        try:
            await self._do_one_report("financial", start_date, end_date, skip_download=True)
        except Exception as e:
            logger.warning("DoorDashAgent: Financial report create failed: %s", e)

        # Go to Reports list and download by row: first row = financial, second row = marketing
        try:
            await self._go_to_reports_list()
            self._financial_path = await self._download_from_table_row(0, "financial")
            self._marketing_path = await self._download_from_table_row(1, "marketing")
        except Exception as e:
            logger.warning("DoorDashAgent: Report download failed: %s", e)

        await self._logout()
        return (self._marketing_path, self._financial_path)

    async def _login(self) -> None:
        """Navigate to merchant portal and log in. DoorDash uses two steps: email → Continue → password → Log in."""
        logger.info("DoorDashAgent: Opening merchant portal")
        await self.page.goto(DOORDASH_MERCHANT_URL, wait_until="domcontentloaded")
        await asyncio.sleep(STEP_DELAY_SEC)

        # Step 1: Email field (label "Email" or placeholder "Required") and "Continue to Log In"
        email_input = self.page.get_by_label("Email").or_(
            self.page.locator('input[type="email"], input[name="email"], input[placeholder="Required"]')
        ).first
        await email_input.wait_for(state="visible", timeout=15_000)
        await email_input.fill(self.email)
        logger.info("DoorDashAgent: Filled email, clicking Continue to Log In")
        continue_btn = self.page.get_by_role("button", name="Continue to Log In").or_(
            self.page.get_by_role("button", name="Continue")
        ).first
        await continue_btn.click()
        await self.page.wait_for_load_state("domcontentloaded")
        await asyncio.sleep(STEP_DELAY_SEC)

        # Step 2: Password field and submit
        password_input = self.page.get_by_label("Password").or_(
            self.page.locator('input[type="password"]')
        ).first
        await password_input.wait_for(state="visible", timeout=15_000)
        await password_input.fill(self.password)
        submit = self.page.get_by_role("button", name="Log in").or_(
            self.page.get_by_role("button", name="Sign in")
        ).or_(self.page.get_by_role("button", name="Continue")).first
        await submit.click()
        await self.page.wait_for_load_state("domcontentloaded")
        await asyncio.sleep(STEP_DELAY_SEC)
        logger.info("DoorDashAgent: Login form submitted")

    async def _logout(self) -> None:
        """Click bottom-left profile button, then Log out in the menu. Best-effort; does not raise."""
        await asyncio.sleep(STEP_DELAY_SEC)  # Let page settle after download
        try:
            # 1) Click the bottom-left profile button (sidebar profile/account)
            profile_clicked = False
            # Try role-based Profile button/link first
            for el in [
                self.page.get_by_role("button", name="Profile"),
                self.page.get_by_role("link", name="Profile"),
            ]:
                if await el.count() > 0:
                    await el.first.click(timeout=5_000)
                    await asyncio.sleep(1.5)  # Let menu open
                    profile_clicked = True
                    logger.info("DoorDashAgent: Clicked profile button")
                    break
            if not profile_clicked:
                # Try sidebar / nav elements with "Profile" text (bottom-left)
                for sel in [
                    'nav a:has-text("Profile"), nav button:has-text("Profile")',
                    '[class*="sidebar"] a:has-text("Profile"), [class*="sidebar"] button:has-text("Profile")',
                    'a:has-text("Profile"), button:has-text("Profile")',
                ]:
                    el = self.page.locator(sel).first
                    if await el.count() > 0:
                        await el.click(timeout=5_000)
                        await asyncio.sleep(1.5)
                        profile_clicked = True
                        logger.info("DoorDashAgent: Clicked profile button")
                        break
            if not profile_clicked:
                # Fallback: any visible "Profile" text
                el = self.page.get_by_text("Profile", exact=False).first
                if await el.count() > 0:
                    await el.click(timeout=5_000)
                    await asyncio.sleep(1.5)
                    profile_clicked = True
            # 2) In the opened menu, click Log out / Sign out
            for text in ["Log out", "Sign out", "Logout", "Signout"]:
                out_el = self.page.get_by_text(text, exact=False).first
                if await out_el.count() > 0:
                    await out_el.click(timeout=5_000)
                    await asyncio.sleep(STEP_DELAY_SEC)
                    logger.info("DoorDashAgent: Logged out")
                    return
            out_el = self.page.get_by_role("menuitem", name="Log out").or_(
                self.page.get_by_role("button", name="Log out")
            ).first
            if await out_el.count() > 0:
                await out_el.click(timeout=5_000)
                await asyncio.sleep(STEP_DELAY_SEC)
                logger.info("DoorDashAgent: Logged out")
        except Exception as e:
            logger.debug("DoorDashAgent: Logout skipped or failed: %s", e)

    async def _handle_2fa_if_present(self) -> None:
        """Detect 2FA screen, get OTP from callback, fill and submit."""
        # Common 2FA patterns: code input, "Enter code", "Verification"
        code_input = self.page.locator(
            'input[type="text"][inputmode="numeric"], input[name*="code"], input[placeholder*="code"], input[placeholder*="Code"]'
        ).first
        try:
            await code_input.wait_for(state="visible", timeout=10_000)
        except Exception:
            logger.debug("DoorDashAgent: No 2FA code input found, continuing")
            return

        otp = await self.get_otp()
        if not otp:
            logger.warning("DoorDashAgent: 2FA required. Enter the code manually in the browser and click Submit; waiting up to 90s...")
            try:
                await code_input.wait_for(state="detached", timeout=90_000)
            except Exception:
                pass
            return
        await code_input.fill(otp)
        submit_2fa = self.page.get_by_role("button", name=("Submit")).or_(
            self.page.get_by_role("button", name=("Verify"))
        ).or_(self.page.get_by_role("button", name=("Continue")))
        if await submit_2fa.first.count() > 0:
            await submit_2fa.first.click()
        await self.page.wait_for_load_state("domcontentloaded")
        await asyncio.sleep(STEP_DELAY_SEC)
        logger.info("DoorDashAgent: 2FA submitted")

    async def _do_one_report(
        self,
        report_type: str,
        start_date: str,
        end_date: str,
        skip_download: bool = False,
    ) -> Optional[Path]:
        """
        Reports → Create report → [Marketing|Financial] report → dates → Create → wait.
        If not skip_download: wait for Download button, click, save. Returns path or None.
        If skip_download: returns None (report is ready, caller will download later).
        """
        # Sidebar: Reports (click to open reports / go back to reports list)
        reports_link = self.page.get_by_role("link", name="Reports").or_(
            self.page.locator('a[href*="report"], a:has-text("Reports")')
        ).first
        await reports_link.wait_for(state="visible", timeout=15_000)
        await reports_link.click()
        await self.page.wait_for_load_state("domcontentloaded")
        await asyncio.sleep(STEP_DELAY_SEC)

        # "Create report" button
        create_report_btn = self.page.get_by_role("button", name="Create report").or_(
            self.page.get_by_text("Create report")
        ).first
        await create_report_btn.wait_for(state="visible", timeout=10_000)
        await create_report_btn.click()
        await self.page.wait_for_load_state("domcontentloaded")
        await asyncio.sleep(STEP_DELAY_SEC)

        # Report type: "Marketing report" or "Financial report"
        if report_type.lower() == "marketing":
            report_btn = self.page.get_by_role("button", name="Marketing report").or_(
                self.page.get_by_text("Marketing report")
            ).first
        else:
            report_btn = self.page.get_by_role("button", name="Financial report").or_(
                self.page.get_by_text("Financial report")
            ).first
        await report_btn.wait_for(state="visible", timeout=10_000)
        await report_btn.click()
        await self.page.wait_for_load_state("domcontentloaded")
        await asyncio.sleep(STEP_DELAY_SEC)

        # "Next" → lands on "Choose a time range" page (Select start date / Select end date)
        next_btn = self.page.get_by_role("button", name="Next").first
        await next_btn.wait_for(state="visible", timeout=10_000)
        await next_btn.click()
        await self.page.wait_for_load_state("domcontentloaded")
        await asyncio.sleep(STEP_DELAY_SEC)

        # Wait for the date section to be visible ("Choose a time range") if present
        try:
            await self.page.get_by_text("Choose a time range", exact=False).first.wait_for(state="visible", timeout=8_000)
        except Exception:
            pass
        await asyncio.sleep(0.5)

        # Date range: set "Select start date" and "Select end date" on the Marketing report page
        logger.info("DoorDashAgent: Setting report dates Start=%s End=%s", start_date, end_date)
        start_filled = False
        end_filled = False

        # Strategy 1: exact labels from "Choose a time range" page – "Select start date" / "Select end date"
        try:
            start_field = self.page.get_by_label("Select start date", exact=False).first
            end_field = self.page.get_by_label("Select end date", exact=False).first
            if await start_field.count() > 0:
                await start_field.click()
                await asyncio.sleep(0.3)
                await start_field.press("Control+a")
                await start_field.fill(start_date)
                start_filled = True
            if await end_field.count() > 0:
                await end_field.click()
                await asyncio.sleep(0.3)
                await end_field.press("Control+a")
                await end_field.fill(end_date)
                end_filled = True
        except Exception as e:
            logger.debug("DoorDashAgent: Select start/end date by label failed: %s", e)

        # Strategy 2: by label text "Start date" / "End date" (shorter)
        if not start_filled or not end_filled:
            try:
                start_label = self.page.get_by_label("Start date", exact=False).first
                end_label = self.page.get_by_label("End date", exact=False).first
                if await start_label.count() > 0 and not start_filled:
                    await start_label.click()
                    await asyncio.sleep(0.3)
                    await start_label.press("Control+a")
                    await start_label.fill(start_date)
                    start_filled = True
                if await end_label.count() > 0 and not end_filled:
                    await end_label.click()
                    await asyncio.sleep(0.3)
                    await end_label.press("Control+a")
                    await end_label.fill(end_date)
                    end_filled = True
            except Exception:
                pass

        # Strategy 3: by placeholder/name/aria-label
        if not start_filled or not end_filled:
            start_input = self.page.locator(
                'input[placeholder*="Start"], input[placeholder*="start"], input[name*="start"], input[id*="start"], input[aria-label*="start"]'
            ).first
            end_input = self.page.locator(
                'input[placeholder*="End"], input[placeholder*="end"], input[name*="end"], input[id*="end"], input[aria-label*="end"]'
            ).first
            if await start_input.count() > 0 and not start_filled:
                await start_input.click()
                await asyncio.sleep(0.3)
                await start_input.press("Control+a")
                await start_input.fill(start_date)
                start_filled = True
            if await end_input.count() > 0 and not end_filled:
                await end_input.click()
                await asyncio.sleep(0.3)
                await end_input.press("Control+a")
                await end_input.fill(end_date)
                end_filled = True

        if not start_filled or not end_filled:
            logger.warning("DoorDashAgent: Could not set both dates (start=%s end=%s). Proceeding anyway.", start_filled, end_filled)
        else:
            logger.info("DoorDashAgent: Report dates set successfully")
        await asyncio.sleep(STEP_DELAY_SEC)

        # "Create report" (final)
        create_final = self.page.get_by_role("button", name="Create report").or_(
            self.page.get_by_text("Create report")
        ).first
        await create_final.wait_for(state="visible", timeout=10_000)
        await create_final.click()
        await self.page.wait_for_load_state("domcontentloaded")
        await asyncio.sleep(STEP_DELAY_SEC)

        # Wait for report generation (financial reports take longer)
        wait_sec = FINANCIAL_REPORT_WAIT_SEC if report_type.lower() == "financial" else REPORT_WAIT_SEC
        logger.info("DoorDashAgent: Waiting %s seconds for %s report generation", wait_sec, report_type)
        await asyncio.sleep(wait_sec)

        # Wait for Download button to appear (report ready); allow extra time for financial
        download_btn = self.page.get_by_role("button", name="Download").or_(
            self.page.get_by_text("Download")
        ).first
        wait_download_btn_ms = 60_000 if report_type.lower() == "financial" else 20_000
        await download_btn.wait_for(state="visible", timeout=wait_download_btn_ms)
        if skip_download:
            return None
        await asyncio.sleep(1)  # Brief pause so click registers
        download_timeout_ms = FINANCIAL_DOWNLOAD_TIMEOUT_MS if report_type.lower() == "financial" else DOWNLOAD_TIMEOUT_MS
        async with self.page.expect_download(timeout=download_timeout_ms) as download_info:
            await download_btn.click()
        download = await download_info.value
        target = self.download_dir / download.suggested_filename
        await download.save_as(target)
        logger.info("DoorDashAgent: %s report saved to %s", report_type.capitalize(), target)
        await asyncio.sleep(STEP_DELAY_SEC)
        return target

    async def _go_to_reports_list(self) -> None:
        """Navigate to the Reports list (table of created reports)."""
        reports_link = self.page.get_by_role("link", name="Reports").or_(
            self.page.locator('a[href*="report"], a:has-text("Reports")')
        ).first
        await reports_link.wait_for(state="visible", timeout=10_000)
        await reports_link.click()
        await self.page.wait_for_load_state("domcontentloaded")
        await asyncio.sleep(STEP_DELAY_SEC)

    async def _download_from_table_row(self, row_index: int, report_type: str) -> Optional[Path]:
        """Click the download button in the given table row (0 = first row = financial, 1 = second = marketing). Returns path or None."""
        # Table body rows: first row = Financials, second = Marketing
        rows = self.page.locator("table tbody tr")
        await rows.first.wait_for(state="visible", timeout=15_000)
        row = rows.nth(row_index)
        download_btn = row.get_by_role("button", name="Download").or_(
            row.get_by_title("Download")
        ).or_(row.locator("button").last).first
        await download_btn.wait_for(state="visible", timeout=15_000)
        await asyncio.sleep(1)
        timeout_ms = FINANCIAL_DOWNLOAD_TIMEOUT_MS if report_type.lower() == "financial" else DOWNLOAD_TIMEOUT_MS
        async with self.page.expect_download(timeout=timeout_ms) as download_info:
            await download_btn.click()
        download = await download_info.value
        target = self.download_dir / download.suggested_filename
        await download.save_as(target)
        logger.info("DoorDashAgent: %s report (row %s) saved to %s", report_type.capitalize(), row_index + 1, target)
        await asyncio.sleep(STEP_DELAY_SEC)
        return target

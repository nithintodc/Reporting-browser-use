"""
DoorDashAgent: login, 2FA via GmailAgent OTP, navigate to Reports,
create financial report for date range, wait 30s, download to ./downloads.
"""

import asyncio
import logging
from pathlib import Path
from typing import Callable, Awaitable, Optional

from playwright.async_api import Page

logger = logging.getLogger(__name__)

DOORDASH_MERCHANT_URL = "https://merchant-portal.doordash.com/merchant/"
REPORT_WAIT_SEC = 30  # Explicit 30-second wait as required
STEP_DELAY_SEC = 3  # Delay between steps for page stability
DEFAULT_DOWNLOAD_DIR = Path(__file__).resolve().parent.parent / "downloads"


class DoorDashAgent:
    """Handles DoorDash merchant portal login, 2FA, and financial report download."""

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
        self._downloaded_path: Optional[Path] = None

    async def run(
        self,
        start_date: str = "01/01/2026",
        end_date: str = "01/31/2026",
    ) -> Optional[Path]:
        """
        Full workflow: login, 2FA, Reports → Create report → Financial report
        → date range → Create report → wait 30s → Download. Returns path to downloaded file.
        """
        self.download_dir.mkdir(parents=True, exist_ok=True)

        await self._login()
        await self._handle_2fa_if_present()
        await self._navigate_to_reports_and_create(start_date, end_date)
        return self._downloaded_path

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

    async def _navigate_to_reports_and_create(
        self,
        start_date: str,
        end_date: str,
    ) -> None:
        """Navigate to Reports → Create report → Financial report → date range → Create → wait 30s → Download."""
        # Sidebar: Reports
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

        # "Financial report" option
        financial = self.page.get_by_role("button", name="Financial report").or_(
            self.page.get_by_text("Financial report")
        ).first
        await financial.wait_for(state="visible", timeout=10_000)
        await financial.click()
        await self.page.wait_for_load_state("domcontentloaded")
        await asyncio.sleep(STEP_DELAY_SEC)

        # "Next"
        next_btn = self.page.get_by_role("button", name="Next").first
        await next_btn.wait_for(state="visible", timeout=10_000)
        await next_btn.click()
        await self.page.wait_for_load_state("domcontentloaded")
        await asyncio.sleep(STEP_DELAY_SEC)

        # Date range: start and end (adapt selectors to actual portal)
        start_input = self.page.locator('input[placeholder*="Start"], input[name*="start"], input[id*="start"]').first
        end_input = self.page.locator('input[placeholder*="End"], input[name*="end"], input[id*="end"]').first
        if await start_input.count() > 0:
            await start_input.fill(start_date)
        if await end_input.count() > 0:
            await end_input.fill(end_date)

        # "Create report" (final)
        create_final = self.page.get_by_role("button", name="Create report").or_(
            self.page.get_by_text("Create report")
        ).first
        await create_final.wait_for(state="visible", timeout=10_000)
        await create_final.click()
        await self.page.wait_for_load_state("domcontentloaded")
        await asyncio.sleep(STEP_DELAY_SEC)

        # Explicit 30-second wait as required
        logger.info("DoorDashAgent: Waiting %s seconds for report generation", REPORT_WAIT_SEC)
        await asyncio.sleep(REPORT_WAIT_SEC)

        # Download button: save to download_dir
        download_btn = self.page.get_by_role("button", name="Download").or_(
            self.page.get_by_text("Download")
        ).first
        await download_btn.wait_for(state="visible", timeout=15_000)
        async with self.page.expect_download(timeout=60_000) as download_info:
            await download_btn.click()
        download = await download_info.value
        target = self.download_dir / download.suggested_filename
        await download.save_as(target)
        self._downloaded_path = target
        logger.info("DoorDashAgent: Download saved to %s", target)
        await asyncio.sleep(STEP_DELAY_SEC)

        await self._logout()

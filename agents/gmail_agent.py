"""
GmailAgent: logs into Gmail, finds latest DoorDash email, extracts 6-digit OTP.
Implements retry polling (every 5 seconds, up to 60 seconds).
"""

import asyncio
import logging
import re
import time
from typing import Optional

from playwright.async_api import Page

logger = logging.getLogger(__name__)

GMAIL_URL = "https://mail.google.com/"
OTP_POLL_INTERVAL_SEC = 5
OTP_POLL_TIMEOUT_SEC = 60
# Match 6-digit numeric code (common OTP pattern in email body)
OTP_PATTERN = re.compile(r"\b(\d{6})\b")


class GmailAgent:
    """Handles Gmail login and 2FA code extraction from DoorDash emails."""

    def __init__(
        self,
        page: Page,
        email: str = "",
        password: str = "",
        skip_login: bool = False,
    ) -> None:
        self.page = page
        self.email = email
        self.password = password
        self.skip_login = skip_login

    async def login(self) -> None:
        """Open Gmail and wait for inbox. If not skip_login, sign in with credentials first."""
        logger.info("GmailAgent: Opening Gmail")
        await self.page.goto(GMAIL_URL, wait_until="domcontentloaded")
        await self.page.wait_for_load_state("networkidle", timeout=15_000)

        if self.skip_login:
            # Local browser: already logged in, just wait for inbox
            await self._wait_for_inbox()
            logger.info("GmailAgent: Inbox loaded (using existing session)")
            return

        # Handle "Sign in" or "Email" input
        email_input = self.page.get_by_label("Email or phone")
        if await email_input.count() > 0:
            await email_input.fill(self.email)
            await self.page.get_by_role("button", name="Next").click()
            await self.page.wait_for_load_state("domcontentloaded")
        else:
            # Already on combined email/password page
            email_selector = self.page.locator('input[type="email"]')
            await email_selector.wait_for(state="visible", timeout=10_000)
            await email_selector.fill(self.email)
            await self.page.get_by_role("button", name="Next").click()
            await self.page.wait_for_load_state("domcontentloaded")

        # Password
        password_input = self.page.get_by_label("Enter your password").or_(
            self.page.locator('input[type="password"]')
        )
        await password_input.first.wait_for(state="visible", timeout=15_000)
        await password_input.first.fill(self.password)
        await self.page.get_by_role("button", name="Next").click()

        # Wait for inbox: try multiple indicators (Gmail DOM varies by region/UI)
        await self._wait_for_inbox()
        logger.info("GmailAgent: Inbox loaded")

    async def _wait_for_inbox(self, timeout_ms: int = 45_000) -> None:
        """Wait for any of several inbox indicators; fallback to URL."""
        step_ms = 6_000
        selectors = [
            '[data-tooltip="Compose"]',
            '[aria-label*="Compose"]',
            'div[role="main"]',
            '[role="navigation"] a[href*="#inbox"]',
            'table[role="grid"]',
            'div[class*="aeF"]',
        ]
        deadline = (timeout_ms / 1000) + time.monotonic()
        last_error: Optional[Exception] = None
        while time.monotonic() < deadline:
            # URL already at inbox (redirect after login)
            url = self.page.url
            if "mail.google.com" in url and "accounts.google.com" not in url:
                # Give DOM a moment and try one fast check
                try:
                    await self.page.wait_for_selector(selectors[0], timeout=3_000, state="visible")
                    return
                except Exception:
                    pass
                # Accept URL as success if we're clearly on mail
                if "/mail/" in url or "#inbox" in url or "/u/0/" in url:
                    await self.page.wait_for_load_state("domcontentloaded")
                    return
            for selector in selectors:
                try:
                    await self.page.wait_for_selector(selector, timeout=step_ms, state="visible")
                    return
                except Exception as e:
                    last_error = e
            await asyncio.sleep(1)
        if last_error:
            raise last_error
        raise TimeoutError("Gmail inbox did not load within timeout")

    async def get_otp_from_latest_doordash_email(
        self,
        poll_interval_sec: int = OTP_POLL_INTERVAL_SEC,
        timeout_sec: int = OTP_POLL_TIMEOUT_SEC,
    ) -> Optional[str]:
        """
        Search or detect latest email from DoorDash and extract 6-digit OTP.
        Polls every poll_interval_sec up to timeout_sec.
        """
        logger.info("GmailAgent: Waiting for DoorDash OTP (poll every %ss, timeout %ss)", poll_interval_sec, timeout_sec)
        deadline = time.monotonic() + timeout_sec

        while time.monotonic() < deadline:
            otp = await self._try_extract_otp()
            if otp:
                logger.info("GmailAgent: Extracted OTP: %s", otp)
                return otp
            await asyncio.sleep(poll_interval_sec)

        logger.warning("GmailAgent: No OTP found within %s seconds", timeout_sec)
        return None

    async def _try_extract_otp(self) -> Optional[str]:
        """One attempt: search for DoorDash, open latest, extract 6-digit code."""
        try:
            # Use search for "from:doordash" or "doordash" to get verification emails
            search_box = self.page.get_by_role("searchbox").or_(
                self.page.locator('input[placeholder*="Search"], input[aria-label*="Search"]')
            )
            if await search_box.first.count() > 0:
                await search_box.first.click()
                await search_box.first.fill("from:doordash")
                await self.page.keyboard.press("Enter")
                await self.page.wait_for_load_state("domcontentloaded")

            # Wait for at least one search result / email row
            first_email = self.page.locator('div[role="main"] tr.zA').first
            try:
                await first_email.wait_for(state="visible", timeout=8_000)
            except Exception:
                return None

            await first_email.click()
            await self.page.wait_for_load_state("domcontentloaded")

            # Get body text (iframe or div)
            body_text = ""
            frame = self.page.frame_locator('iframe[title*="Message"]').first
            if await frame.locator("body").count() > 0:
                body_text = await frame.locator("body").inner_text()
            else:
                body_locator = self.page.locator('div[role="main"] div.a3s, div[data-message-id]')
                if await body_locator.count() > 0:
                    body_text = await body_locator.first.inner_text()

            match = OTP_PATTERN.search(body_text)
            if match:
                return match.group(1)
        except Exception as e:
            logger.debug("GmailAgent: OTP extraction attempt failed: %s", e)
        return None

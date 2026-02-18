"""
BrowserManager agent: initializes Playwright in headed mode,
creates browser context with download support, and manages tabs.
Supports connecting to a local browser via CDP (existing session).
"""

import asyncio
import logging
import os
import platform
import socket
import subprocess
import time
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

from playwright.async_api import async_playwright, Browser, BrowserContext, Page, Playwright

logger = logging.getLogger(__name__)

# Default download directory relative to project root
DEFAULT_DOWNLOAD_DIR = Path(__file__).resolve().parent.parent / "downloads"
DEFAULT_CDP_URL = "http://localhost:9222"
CDP_PORT = 9222
AUTO_START_WAIT_SEC = 20

# #region agent log
DEBUG_LOG_PATH = Path(__file__).resolve().parent.parent / ".cursor" / "debug-a30b44.log"
def _debug_log(message: str, data: dict, hypothesis_id: str) -> None:
    import json
    payload = {"id": f"log_{int(time.time()*1000)}", "timestamp": int(time.time()*1000), "location": "browser_manager", "message": message, "data": data, "hypothesisId": hypothesis_id}
    try:
        DEBUG_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(DEBUG_LOG_PATH, "a") as f:
            f.write(json.dumps(payload) + "\n")
    except Exception:
        pass
# #endregion


def _chrome_executable() -> Optional[str]:
    """Return path to Chrome/Chromium executable, or None if not found."""
    path = os.environ.get("CHROME_PATH", "").strip()
    if path and os.path.isfile(path):
        return path
    sys_name = platform.system()
    if sys_name == "Darwin":
        p = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
        return p if os.path.isfile(p) else None
    if sys_name == "Windows":
        for p in (
            os.path.expandvars(r"%LOCALAPPDATA%\Google\Chrome\Application\chrome.exe"),
            r"C:\Program Files\Google\Chrome\Application\chrome.exe",
        ):
            if p and os.path.isfile(p):
                return p
    return None


async def _wait_for_port(host: str, port: int, timeout_sec: float) -> bool:
    """Return True when the port is accepting connections."""

    def try_connect() -> bool:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(2)
        try:
            s.connect((host, port))
            return True
        except (socket.error, OSError):
            return False
        finally:
            s.close()

    deadline = time.monotonic() + timeout_sec
    start = time.monotonic()
    while time.monotonic() < deadline:
        if await asyncio.to_thread(try_connect):
            # #region agent log
            _debug_log("Port became connectable", {"port": port, "elapsed_sec": round(time.monotonic() - start, 1)}, "C")
            # #endregion
            return True
        await asyncio.sleep(0.5)
    # #region agent log
    _debug_log("Port wait timed out", {"port": port, "timeout_sec": timeout_sec, "elapsed_sec": round(time.monotonic() - start, 1)}, "C")
    # #endregion
    return False


def _launch_chrome_with_debug_port(port: int) -> bool:
    """Launch Chrome with --remote-debugging-port. Return True if launched.
    Uses a dedicated user-data-dir so a new process always binds to the port
    (avoids macOS handing off to an already-running Chrome that ignores the flag).
    """
    chrome = _chrome_executable()
    # #region agent log
    _debug_log("Chrome path resolution", {"chrome_path": chrome, "path_exists": os.path.isfile(chrome) if chrome else False}, "A")
    # #endregion
    if not chrome:
        logger.warning("Chrome not found; set CHROME_PATH if needed")
        return False
    # Persistent profile: use CHROME_USER_DATA_DIR if set, else default. Ensures one process binds to port.
    raw_dir = os.environ.get("CHROME_USER_DATA_DIR", "").strip()
    if raw_dir:
        user_data_dir = Path(raw_dir).expanduser().resolve()
    else:
        user_data_dir = Path(__file__).resolve().parent.parent / ".cursor" / "chrome-debug-profile"
    user_data_dir.mkdir(parents=True, exist_ok=True)
    try:
        proc = subprocess.Popen(
            [
                chrome,
                f"--remote-debugging-port={port}",
                f"--user-data-dir={user_data_dir}",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        # #region agent log
        _debug_log("Chrome Popen result", {"launched": True, "pid": proc.pid}, "B")
        # #endregion
        return True
    except Exception as e:
        # #region agent log
        _debug_log("Chrome Popen failed", {"launched": False, "error": str(e)}, "B")
        # #endregion
        logger.warning("Failed to start Chrome: %s", e)
        return False


class BrowserManager:
    """Manages Playwright browser lifecycle and context with download support."""

    def __init__(
        self,
        headless: bool = False,
        download_dir: Optional[Path] = None,
        use_local_browser: bool = False,
        cdp_url: Optional[str] = None,
    ) -> None:
        self.headless = headless
        self.download_dir = download_dir or DEFAULT_DOWNLOAD_DIR
        self.use_local_browser = use_local_browser
        self.cdp_url = (cdp_url or DEFAULT_CDP_URL).strip() or DEFAULT_CDP_URL
        self._playwright: Optional[Playwright] = None
        self._browser: Optional[Browser] = None
        self._context: Optional[BrowserContext] = None

    async def start(self) -> BrowserContext:
        """Start Playwright: either launch browser or connect to local browser via CDP."""
        self.download_dir.mkdir(parents=True, exist_ok=True)

        if self.use_local_browser:
            return await self._connect_local_browser()
        return await self._launch_browser()

    def _port_from_cdp_url(self) -> int:
        """Extract port from self.cdp_url (e.g. http://localhost:9222 -> 9222)."""
        try:
            parsed = urlparse(self.cdp_url)
            if parsed.port is not None:
                return parsed.port
            return CDP_PORT
        except Exception:
            return CDP_PORT

    async def _connect_local_browser(self) -> BrowserContext:
        """Connect to existing Chrome/Edge with remote debugging (reuse logged-in session)."""
        logger.info("Connecting to local browser at %s", self.cdp_url)
        self._playwright = await async_playwright().start()
        port = self._port_from_cdp_url()

        for attempt in range(2):
            try:
                self._browser = await self._playwright.chromium.connect_over_cdp(self.cdp_url)
                break
            except Exception as e:
                if attempt == 0 and ("ECONNREFUSED" in str(e) or str(port) in str(e)):
                    logger.info("Chrome not running on port %s; attempting to start it...", port)
                    launch_ok = _launch_chrome_with_debug_port(port)
                    # #region agent log
                    _debug_log("After launch decision", {"launch_ok": launch_ok}, "E")
                    # #endregion
                    if launch_ok:
                        port_ok = await _wait_for_port("127.0.0.1", port, AUTO_START_WAIT_SEC)
                        # #region agent log
                        _debug_log("After port wait", {"port_ok": port_ok}, "E")
                        # #endregion
                        if port_ok:
                            logger.info("Chrome is up; waiting 3s for CDP to be ready...")
                            await asyncio.sleep(3)
                            # Retry connect a few times (CDP may not be ready immediately)
                            for _ in range(3):
                                try:
                                    self._browser = await self._playwright.chromium.connect_over_cdp(self.cdp_url)
                                    break
                                except Exception as retry_e:
                                    logger.debug("CDP connect retry: %s", retry_e)
                                    await asyncio.sleep(2)
                            else:
                                raise RuntimeError(
                                    "Chrome started but CDP connection failed. Try starting Chrome manually with "
                                    "--remote-debugging-port=9222 and run again."
                                ) from e
                            break
                    raise RuntimeError(
                        "Could not connect to Chrome on port 9222. Start Chrome with remote debugging first:\n"
                        "  macOS: /Applications/Google\\ Chrome.app/Contents/MacOS/Google\\ Chrome --remote-debugging-port=9222\n"
                        "  Windows: \"C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe\" --remote-debugging-port=9222\n"
                        "Or run: ./scripts/start_chrome_debug.sh\n"
                        "Then run this script again."
                    ) from e
                raise
        # Use existing default context (has user's cookies and login state)
        if not self._browser.contexts:
            raise RuntimeError(
                "No browser context found. Ensure Chrome is started with "
                "--remote-debugging-port=9222 and has at least one tab."
            )
        self._context = self._browser.contexts[0]
        self._context.set_default_timeout(30_000)
        self._context.set_default_navigation_timeout(60_000)
        logger.info("Connected to local browser (context has %s page(s))", len(self._context.pages))
        return self._context

    async def _launch_browser(self) -> BrowserContext:
        """Launch a new Chromium instance (fresh profile)."""
        logger.info("Starting browser (headed=%s), download_dir=%s", not self.headless, self.download_dir)
        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.launch(headless=self.headless)
        self._context = await self._browser.new_context(
            accept_downloads=True,
            locale="en-US",
        )
        self._context.set_default_timeout(30_000)
        self._context.set_default_navigation_timeout(60_000)
        logger.info("Browser and context ready")
        return self._context

    @property
    def context(self) -> BrowserContext:
        """Return the current browser context. Raises if not started."""
        if self._context is None:
            raise RuntimeError("BrowserManager not started. Call start() first.")
        return self._context

    async def new_page(self) -> Page:
        """Create and return a new page in the current context."""
        page = await self.context.new_page()
        logger.debug("New page created")
        return page

    async def new_tab(self) -> Page:
        """Alias for new_page(); creates a new tab."""
        return await self.new_page()

    def get_download_dir(self) -> Path:
        """Return the configured download directory path."""
        return self.download_dir

    async def close(self) -> None:
        """Close or disconnect gracefully. With local browser, only disconnects (browser stays open)."""
        if self.use_local_browser:
            # Disconnect only; do not close the user's browser
            self._context = None
            self._browser = None
            if self._playwright:
                await self._playwright.stop()
                self._playwright = None
            logger.info("Disconnected from local browser")
            return
        if self._context:
            await self._context.close()
            self._context = None
        if self._browser:
            await self._browser.close()
            self._browser = None
        if self._playwright:
            await self._playwright.stop()
            self._playwright = None
        logger.info("Browser closed")

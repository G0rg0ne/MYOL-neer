from typing import Any, Optional
import asyncio
from playwright.async_api import async_playwright, Browser, Playwright


class PlaywrightSession:
    """Manages a persistent browser instance for reuse across multiple requests."""
    
    def __init__(self):
        self._playwright: Optional[Playwright] = None
        self._browser: Optional[Browser] = None
        self._lock = asyncio.Lock()
    
    async def _ensure_browser(self):
        """Ensure browser is initialized (thread-safe)."""
        if self._browser is None:
            async with self._lock:
                if self._browser is None:  # Double-check after acquiring lock
                    self._playwright = await async_playwright().start()
                    self._browser = await self._playwright.chromium.launch()
    
    async def initialize(self):
        """Public method to initialize the browser."""
        await self._ensure_browser()
    
    async def fetch(self, url: str) -> str:
        """Fetch content from URL using a new page, reusing the browser instance."""
        await self._ensure_browser()
        
        page = await self._browser.new_page()
        try:
            await page.goto(url, wait_until="networkidle")
            if page.url.startswith("https://consent.google.com"):
                await page.click('text="Accept all"')
            
            await page.wait_for_selector('[role="main"]', timeout=30000)
            body = await page.evaluate(
                "() => document.querySelector('[role=\"main\"]').innerHTML"
            )
            return body
        finally:
            await page.close()
    
    async def close(self):
        """Close the browser and playwright instances."""
        if self._browser:
            await self._browser.close()
            self._browser = None
        if self._playwright:
            await self._playwright.stop()
            self._playwright = None


# Global session instance for reuse
_global_session: Optional[PlaywrightSession] = None


async def fetch_with_playwright(url: str, session: Optional[PlaywrightSession] = None) -> str:
    """Fetch content using Playwright, optionally with a shared session."""
    if session:
        return await session.fetch(url)
    
    # Fallback: create temporary session for backward compatibility
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await browser.new_page()
        try:
            await page.goto(url, wait_until="networkidle")
            if page.url.startswith("https://consent.google.com"):
                await page.click('text="Accept all"')
            
            await page.wait_for_selector('[role="main"]', timeout=30000)
            body = await page.evaluate(
                "() => document.querySelector('[role=\"main\"]').innerHTML"
            )
            return body
        finally:
            await page.close()
            await browser.close()


def get_global_session() -> PlaywrightSession:
    """Get or create the global Playwright session."""
    global _global_session
    if _global_session is None:
        _global_session = PlaywrightSession()
    return _global_session


async def close_global_session():
    """Close the global Playwright session."""
    global _global_session
    if _global_session:
        await _global_session.close()
        _global_session = None


def local_playwright_fetch(params: dict, session: Optional[PlaywrightSession] = None) -> Any:
    """Fetch flight data using Playwright, optionally with a shared session."""
    url = "https://www.google.com/travel/flights?" + "&".join(f"{k}={v}" for k, v in params.items())
    
    if session:
        # Use provided session (for async contexts)
        body = asyncio.run(session.fetch(url))
    else:
        # Use global session for backward compatibility
        global_session = get_global_session()
        body = asyncio.run(global_session.fetch(url))

    class DummyResponse:
        status_code = 200
        text = body
        text_markdown = body

    return DummyResponse

"""
scrapers/instagram.py
Scraper profil Instagram menggunakan Built-in Stealth Browser + yt-dlp.

Strategi:
- Playwright Stealth Browser dengan spoofing fingerprint (Canvas, WebGL, navigator.webdriver)
- Dual-Tab Crawling: Feed utama (/username/) lalu tab Reels (/username/reels/)
- Smart stop-condition: berhenti saat menemukan postingan yang sudah ada di SQLite
- Semantic & Adaptive Selectors (a[href*="/p/"], a[href*="/reel/"])
- Human-like randomized request pacing (random.uniform(3.0, 6.0)) dan safety deadline guards
"""

import asyncio
import json
import logging
import os
import random
import re
from pathlib import Path
from typing import AsyncGenerator, Optional
from playwright.async_api import Browser, BrowserContext, Page, async_playwright

from .base import (
    BaseScraper,
    MediaType,
    PostMedia,
    USER_AGENT,
    DOCKER_CHROMIUM_FLAGS,
)
from core.utils import (
    TAG_CRAWL,
    TAG_SYSTEM,
    TAG_WARN,
    TAG_ERROR,
    TAG_SUCCESS,
)

logger = logging.getLogger(__name__)


class InstagramScraper(BaseScraper):
    """
    Scraper profil Instagram berbasis Built-in Stealth Browser.
    """

    PLATFORM = "instagram"

    def __init__(self, db_manager, session_dir: str, headed: bool = False):
        super().__init__(db_manager, session_dir)
        self.headed = headed
        self.session_dir = Path(session_dir)
        self.session_dir.mkdir(parents=True, exist_ok=True)
        self.netscape_cookie_path = self.session_dir / "instagram_cookies.txt"
        self._playwright = None
        self._browser: Optional[Browser] = None
        self._context: Optional[BrowserContext] = None

    def _extract_username(self, url: str) -> str:
        """Ekstrak username Instagram dari URL profil."""
        match = re.search(r"instagram\.com/([^/?&#/]+)/?", url)
        if match:
            username = match.group(1)
            if username not in {"p", "reel", "explore", "stories", "accounts"}:
                return username
        return ""

    def _extract_post_id(self, url: str) -> str:
        """Ekstrak shortcode postingan dari URL Instagram."""
        match = re.search(r"/p/([A-Za-z0-9_-]+)", url)
        if match:
            return match.group(1)
        match = re.search(r"/reel/([A-Za-z0-9_-]+)", url)
        return match.group(1) if match else ""

    async def _init_browser(self) -> Page:
        """Inisialisasi browser stealth Playwright."""
        self._playwright = await async_playwright().start()
        self._browser, self._context = await BaseScraper.create_stealth_browser(
            self._playwright,
            headed=self.headed,
            viewport={"width": 1280, "height": 800},
            locale="id-ID,id;q=0.9,en-US;q=0.8,en;q=0.7",
        )
        await self.load_and_inject_cookies(self._context, "instagram")
        return await self._context.new_page()

    async def scrape_profile(
        self,
        profile_url: str,
        forced: bool = False,
    ) -> AsyncGenerator[PostMedia, None]:
        """
        Crawl konten profil Instagram secara dual-tab (Feed & Reels).
        """
        username = self._extract_username(profile_url)
        if not username:
            logger.error(f"{TAG_ERROR} Username tidak valid: {profile_url}")
            return
        self._profile_url_username = username

        canonical_url = f"https://www.instagram.com/{username}/"
        reels_url = f"https://www.instagram.com/{username}/reels/"

        # Siapkan cookies
        netscape_path = await self.load_and_inject_cookies(None, "instagram")
        if netscape_path:
            self.netscape_cookie_path = netscape_path

        feed_urls: list[str] = []
        reels_urls: list[str] = []

        try:
            page = await self._init_browser()

            # ──────────────────────────────────────────
            # TAHAP 1: Kumpulkan URL Feed / Grid Utama
            # ──────────────────────────────────────────
            logger.info(f"[TAB 1/2] Mengumpulkan URL feed Instagram: {canonical_url}")
            try:
                await page.goto(canonical_url, wait_until="domcontentloaded", timeout=45000)
                await asyncio.sleep(random.uniform(2.5, 4.0))

                if "accounts/login" in page.url:
                    logger.warning(f"{TAG_WARN} Instagram mengarahkan ke halaman login untuk {canonical_url}")

                seen_in_page = set()
                should_stop = False

                for _ in range(6):
                    if should_stop:
                        break

                    links = await page.locator('a[href*="/p/"], a[href*="/reel/"]').all()
                    for link in links:
                        try:
                            href = await link.get_attribute("href")
                            if not href or href in seen_in_page:
                                continue
                            seen_in_page.add(href)
                            full_post_url = f"https://www.instagram.com{href}" if href.startswith("/") else href
                            p_id = self._extract_post_id(full_post_url)
                            if not p_id:
                                continue

                            if not forced and await self.db.check_post_exists(p_id, self.PLATFORM):
                                logger.info(f"{TAG_CRAWL} Stop-condition feed: post {p_id} sudah ada di DB.")
                                should_stop = True
                                break

                            feed_urls.append(full_post_url)
                        except Exception:
                            pass

                    await page.evaluate("window.scrollBy(0, window.innerHeight * 2)")
                    await asyncio.sleep(random.uniform(2.0, 3.5))

            except Exception as e:
                logger.error(f"{TAG_ERROR} Gagal fetch feed Instagram @{username}: {e}")

            logger.info(f"[TAB 1/2] {len(feed_urls)} post ditemukan di feed @{username}")
            await asyncio.sleep(random.uniform(3.0, 5.0))

            # ──────────────────────────────────────────
            # TAHAP 2: Kumpulkan URL Tab Reels Eksklusif
            # ──────────────────────────────────────────
            logger.info(f"[TAB 2/2] Mengumpulkan URL Reels Instagram: {reels_url}")
            try:
                await page.goto(reels_url, wait_until="domcontentloaded", timeout=45000)
                await asyncio.sleep(random.uniform(2.5, 4.0))

                seen_reels_in_page = set()
                should_stop = False

                for _ in range(6):
                    if should_stop:
                        break

                    links = await page.locator('a[href*="/reel/"]').all()
                    for link in links:
                        try:
                            href = await link.get_attribute("href")
                            if not href or href in seen_reels_in_page:
                                continue
                            seen_reels_in_page.add(href)
                            full_reel_url = f"https://www.instagram.com{href}" if href.startswith("/") else href
                            r_id = self._extract_post_id(full_reel_url)
                            if not r_id:
                                continue

                            if not forced and await self.db.check_post_exists(r_id, self.PLATFORM):
                                logger.info(f"{TAG_CRAWL} Stop-condition Reels: reel {r_id} sudah ada di DB.")
                                should_stop = True
                                break

                            reels_urls.append(full_reel_url)
                        except Exception:
                            pass

                    await page.evaluate("window.scrollBy(0, window.innerHeight * 2.5)")
                    await asyncio.sleep(random.uniform(2.0, 3.5))

            except Exception as e:
                logger.debug(f"Info Reels fetch @{username}: {e}")

            logger.info(f"[TAB 2/2] {len(reels_urls)} Reels ditemukan")

            # ──────────────────────────────────────────
            # TAHAP 3: Deduplikasi & Ekstraksi Metadata Hybrid
            # ──────────────────────────────────────────
            combined_urls = list(dict.fromkeys(feed_urls + reels_urls))
            all_post_objects: list[PostMedia] = []

            logger.info(f"{TAG_CRAWL} Memulai ekstraksi metadata untuk {len(combined_urls)} postingan Instagram...")
            import time as _t
            crawl_start_time = _t.monotonic()
            _max_crawl_minutes = int(os.getenv("MAX_INSTAGRAM_CRAWL_MINUTES", "10"))
            MAX_CRAWL_DURATION = _max_crawl_minutes * 60.0

            for target_url in combined_urls:
                if _t.monotonic() - crawl_start_time > MAX_CRAWL_DURATION:
                    logger.warning(
                        f"{TAG_WARN} Batas waktu crawl Instagram ({_max_crawl_minutes}m) tercapai untuk @{username}."
                    )
                    break

                post_id = self._extract_post_id(target_url)
                if not post_id:
                    continue

                if not forced and await self.db.check_post_exists(post_id, self.PLATFORM):
                    continue

                # 1. Coba ekstraktor cepat yt-dlp
                post_data = await self._extract_metadata_via_ytdlp(target_url)

                # 2. Fallback browser jika yt-dlp gagal
                if not post_data:
                    post_data = await self._scrape_single_post(page, target_url)
                    await asyncio.sleep(random.uniform(3.0, 6.0))

                if post_data:
                    all_post_objects.append(post_data)

            # ──────────────────────────────────────────
            # TAHAP 4: Urutkan Kronologis (Terbaru ke Terlama)
            # ──────────────────────────────────────────
            all_post_objects.sort(
                key=lambda x: str(x.timestamp) if x.timestamp else "1970-01-01T00:00:00.000Z",
                reverse=True,
            )

            logger.info(f"[⚙️ SYSTEM] Mengirim {len(all_post_objects)} post Instagram ke downstream pipeline...")
            for post_media in all_post_objects:
                yield post_media
                await asyncio.sleep(random.uniform(1.0, 2.0))

        except Exception as e:
            logger.error(f"{TAG_ERROR} Instagram scraper error untuk @{username}: {e}")
        finally:
            await self.close()

    async def _extract_metadata_via_ytdlp(self, url: str) -> Optional[PostMedia]:
        """Ekstrak metadata via yt-dlp tanpa membuka browser."""
        import yt_dlp
        from datetime import datetime, timezone

        post_id = self._extract_post_id(url)
        if not post_id:
            return None

        cookies_file = (
            str(self.netscape_cookie_path)
            if self.netscape_cookie_path and self.netscape_cookie_path.exists()
            else None
        )

        ydl_opts = {
            "skip_download": True,
            "quiet": True,
            "no_warnings": True,
            "playlist_items": "1",
        }
        if cookies_file:
            ydl_opts["cookiefile"] = cookies_file

        try:
            loop = asyncio.get_running_loop()

            def _extract():
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:  # pyright: ignore[reportArgumentType]
                    return ydl.extract_info(url, download=False)

            info = await loop.run_in_executor(None, _extract)
            if not info:
                return None

            caption = info.get("description") or info.get("title") or ""
            timestamp = None
            upload_timestamp = info.get("timestamp")
            if upload_timestamp:
                timestamp = datetime.fromtimestamp(upload_timestamp, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            else:
                upload_date = info.get("upload_date")
                if upload_date:
                    try:
                        dt = datetime.strptime(upload_date, "%Y%m%d").replace(tzinfo=timezone.utc)
                        timestamp = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
                    except Exception:
                        pass

            is_video = any(
                f.get("vcodec") is not None and f.get("vcodec") != "none"
                for f in (info.get("formats") or [])
            )
            media_type = MediaType.VIDEO if is_video else MediaType.PHOTO

            return PostMedia(
                post_id=post_id,
                post_url=url,
                profile_url=f"https://www.instagram.com/{self._profile_url_username}/" if hasattr(self, "_profile_url_username") and self._profile_url_username else url,
                platform=self.PLATFORM,
                media_type=media_type,
                media_urls=[],
                caption=caption,
                timestamp=timestamp,
                cookies_file=cookies_file,
            )
        except Exception:
            return None

    async def _scrape_single_post(
        self, page: Page, post_url: str
    ) -> Optional[PostMedia]:
        """Ekstrak detail satu postingan menggunakan Playwright Stealth."""
        post_id = self._extract_post_id(post_url)
        if not post_id:
            return None

        caption = ""
        timestamp = None
        media_type = MediaType.UNKNOWN
        media_urls: list[str] = []

        try:
            await page.goto(post_url, wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(2.0)

            # Caption
            for sel in ["article h1", "[data-testid='post-caption'] h1", "h1"]:
                try:
                    c_el = page.locator(sel).first
                    if await c_el.count() > 0:
                        caption = (await c_el.inner_text()).strip()
                        if caption:
                            break
                except Exception:
                    pass

            # Timestamp
            try:
                t_el = page.locator("time").first
                if await t_el.count() > 0:
                    timestamp = await t_el.get_attribute("datetime")
            except Exception:
                pass

            # Media Detection
            carousel_items = await page.locator("div[role='presentation'] ul li").all()
            if len(carousel_items) > 1:
                media_type = MediaType.CAROUSEL
                for item in carousel_items:
                    v_el = item.locator("video source").first
                    if await v_el.count() > 0:
                        src = await v_el.get_attribute("src")
                        if src and not src.startswith("blob:"):
                            media_urls.append(src)
                            continue
                    img_el = item.locator("img").first
                    if await img_el.count() > 0:
                        src = await img_el.get_attribute("src")
                        if src and not src.startswith("blob:") and "instagram" in src:
                            media_urls.append(src)
            else:
                v_el = page.locator("video").first
                if await v_el.count() > 0:
                    media_type = MediaType.VIDEO
                    src = await v_el.get_attribute("src")
                    if src and not src.startswith("blob:"):
                        media_urls.append(src)
                else:
                    media_type = MediaType.PHOTO

        except Exception as e:
            logger.debug(f"Single post fetch error for {post_url}: {e}")

        cookies_file_str = str(self.netscape_cookie_path) if self.netscape_cookie_path.exists() else None

        return PostMedia(
            post_id=post_id,
            post_url=post_url,
            profile_url=f"https://www.instagram.com/{self._profile_url_username}/" if hasattr(self, "_profile_url_username") and self._profile_url_username else post_url,
            platform=self.PLATFORM,
            media_type=media_type,
            media_urls=media_urls,
            caption=caption,
            timestamp=timestamp,
            cookies_file=cookies_file_str,
        )

    async def close(self) -> None:
        """Cleanup browser resources."""
        try:
            if self._context:
                await self._context.close()
            if self._browser:
                await self._browser.close()
            if self._playwright:
                await self._playwright.stop()
        except Exception as e:
            logger.debug(f"InstagramScraper close error: {e}")
        finally:
            self._context = None
            self._browser = None
            self._playwright = None

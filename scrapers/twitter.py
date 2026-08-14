"""
scrapers/twitter.py
Scraper profil Twitter/X menggunakan Playwright + yt-dlp.

Strategi:
- Twitter memerlukan login aktif untuk melihat media profil
- Playwright headed untuk login manual pada run pertama
- Sesi disimpan ke cookies_tw.json
- Smart scroll dengan stop-condition (sama seperti Instagram)
- yt-dlp digunakan sebagai downloader media (video tweet)

PENTING: Twitter/X memiliki rate limiting yang ketat.
         Gunakan delay yang cukup dan hindari scraping terlalu banyak sekaligus.
"""

import asyncio
import json
import logging
import os
import random
import re
from pathlib import Path
from typing import AsyncGenerator, Optional

import aiofiles  # Async file I/O agar save cookies tidak blokir event loop
from playwright.async_api import Browser, BrowserContext, Page, async_playwright

from .base import BaseScraper, MediaType, PostMedia, USER_AGENT

logger = logging.getLogger(__name__)

COOKIE_FILE = "cookies_twitter.json"


class TwitterScraper(BaseScraper):
    """
    Scraper profil Twitter/X dengan manajemen sesi browser Playwright.
    """

    PLATFORM = "twitter"

    def __init__(self, db_manager, session_dir: str, headed: bool = True):
        super().__init__(db_manager, session_dir)
        self.headed = headed
        self.session_dir = Path(session_dir)
        self.session_dir.mkdir(parents=True, exist_ok=True)
        self.cookie_path = self.session_dir / COOKIE_FILE
        self.netscape_cookie_path = self.session_dir / "twitter_cookies.txt"
        self._playwright = None
        self._browser: Optional[Browser] = None
        self._context: Optional[BrowserContext] = None

    async def _init_browser(self) -> BrowserContext:
        """
        Inisialisasi browser Playwright standard dengan injeksi static JSON cookies Twitter/X.
        """
        self._playwright = await async_playwright().start()

        # Deteksi otomatis path Brave via base class helper
        brave_path = BaseScraper.get_brave_path()
        if brave_path:
            logger.info(f"Menggunakan browser Brave dari: {brave_path}")
        else:
            logger.warning("Browser Brave tidak ditemukan di folder default. Menggunakan Chromium bawaan Playwright.")

        # FIX: replaced minimal 2-arg manual dict with base class helper
        # Twitter fingerprinting is the most aggressive — this is highest-impact stealth fix
        launch_kwargs = BaseScraper.get_browser_launch_kwargs()
        launch_kwargs["headless"] = not self.headed

        self._browser = await self._playwright.chromium.launch(**launch_kwargs)
        if self._browser is None:
            raise RuntimeError("Browser instance is not initialized")

        self._context = await self._browser.new_context(
            user_agent=USER_AGENT,
            viewport={"width": 1280, "height": 900},
            locale="id-ID",
        )

        # Memuat cookie dari static JSON config/cookies/twitter.json
        netscape_path = await self.load_and_inject_cookies(self._context, "twitter")
        if not netscape_path:
            raise ValueError("File cookie Twitter tidak ditemukan atau kosong.")
        self.netscape_cookie_path = netscape_path

        return self._context

    async def scrape_profile(
        self,
        profile_url: str,
        forced: bool = False,
    ) -> AsyncGenerator[PostMedia, None]:
        """
        Crawl semua tweet media dari profil Twitter/X.
        Fokus pada tab "Media" untuk mendapatkan hanya tweet yang berisi media.
        """
        try:
            await self._init_browser()
        except Exception as e:
            logger.error(f"[❌ ERROR  ] Menghentikan scraping profil Twitter/X karena kesalahan browser/cookie: {e}")
            self.failed = True
            return

        username = self._extract_username(profile_url)
        if not username:
            logger.error(f"Username Twitter tidak valid: {profile_url}")
            return

        # Gunakan tab "Media" langsung untuk efisiensi
        # (hanya tweet yang memiliki foto/video)
        media_tab_url = f"https://x.com/{username}/media"
        logger.info(f"Mulai crawl Twitter Media tab: {media_tab_url}")

        if self._context is None:
            raise RuntimeError("Browser context is not initialized")
        page = await self._context.new_page()

        try:
            try:
                logger.info(f"[🔍 CRAWL] Membuka Tab Media Twitter target: {media_tab_url}")
                # FIX: added wait_until="domcontentloaded" — "load" hangs forever on
                # Twitter SPA which has infinite background XHR polling
                await page.goto(media_tab_url, wait_until="domcontentloaded", timeout=20000)
                
                # Target the structural primaryColumn backbone container and wait for tweet selector explicitly
                await page.wait_for_selector('[data-testid="primaryColumn"]', state="attached", timeout=15000)
                try:
                    await page.wait_for_selector('article[data-testid="tweet"]', timeout=15000)
                except Exception:
                    logger.warning("Tweet selector tidak muncul dalam 15s (mungkin akun tidak memiliki media).")
            except Exception as e:
                logger.error(f"[❌ ERROR  ] Gagal memuat Tab Media Twitter atau struktur halaman korup: {e}")
                return # Terminate early safely without throwing unhandled exceptions

            if "i/flow/login" in page.url or "login" in page.url:
                logger.error("[❌ ERROR  ] Sesi Cookie .env Anda EXPIRED!")
                return

            # 3. Mulai proses scrolling dan ekstrasi data asli...
            all_posts = await self._scroll_and_collect_tweets(page, username, profile_url, forced=forced)

            logger.info(
                f"Total {len(all_posts)} tweet media dikumpulkan dari @{username}"
            )

            # Urutan alami: terbaru ke terlama

            for post in all_posts:
                yield post

        finally:
            await page.close()

    async def _scroll_and_collect_tweets(self, page: Page, username: str, profile_url: str, forced: bool = False) -> list[PostMedia]:
        """
        Scroll tab Media Twitter sambil mengumpulkan PostMedia.

        Smart stop-condition: berhenti scroll jika menemukan tweet yang
        sudah ada di database. Ini menghemat kuota data dan waktu.
        """
        collected_posts = []
        seen_ids = set()
        no_new_count = 0  # Counter untuk deteksi akhir feed
        MAX_NO_NEW = 3  # Berhenti setelah 3x scroll tanpa post baru
        
        # Tracking tinggi halaman untuk Hard Limit Protection
        consecutive_same_height_count = 0
        MAX_SAME_HEIGHT = 2

        while no_new_count < MAX_NO_NEW:
            # Tunggu setidaknya satu container tweet/article muncul (auto-wait) untuk mencegah list kosong []
            try:
                await page.locator('article, [data-testid="tweet"], [data-testid="cellInnerDiv"]').first.wait_for(state="attached", timeout=10000)
            except Exception:
                pass

            # Ambil semua tweet container menggunakan stable data-testid atau tag article setelah hard wait 3 detik
            await page.wait_for_timeout(3000)
            tweet_containers = await page.locator('article, [data-testid="tweet"], [data-testid="cellInnerDiv"]').all()
            new_found = 0
            should_stop = False

            for container in tweet_containers:
                try:
                    # Find any link pointing to a status update within this container block
                    status_links = await container.locator('a[href*="/status/"]').all()
                    
                    for link in status_links:
                        href = await link.get_attribute("href")
                        if not href:
                            continue

                        target_user_lower = username.lower()
                        href_lower = href.lower()

                        # Enforce ownership check safely
                        if f"/{target_user_lower}/status/" not in href_lower:
                            continue

                        tweet_id = self._extract_tweet_id(href)
                        if not tweet_id or tweet_id in seen_ids:
                            continue

                        seen_ids.add(tweet_id)
                        tweet_url = f"https://x.com/{username}/status/{tweet_id}"

                        # STOP-CONDITION: Cek SQLite — jika tweet ini sudah didownload,
                        # berarti kita sudah sampai di batas resume terakhir.
                        # Hentikan scroll untuk menghemat waktu dan kuota data.
                        # KECUALI jika forced=True.
                        if not forced and await self.db.check_post_exists(tweet_id, self.PLATFORM):
                            logger.info(
                                f"Stop-condition: tweet {tweet_id} sudah di DB. "
                                f"Berhenti scroll — resume point ditemukan."
                            )
                            should_stop = True
                            break

                        # Extract timestamp safely as an optional field
                        post_timestamp = None
                        time_el = container.locator('time').first
                        if await time_el.count() > 0:
                            post_timestamp = await time_el.get_attribute("datetime")

                        tweet_caption = ""
                        try:
                            text_el = container.locator('[data-testid="tweetText"]').first
                            if await text_el.count() > 0:
                                tweet_caption = await text_el.inner_text()
                        except Exception:
                            pass

                        collected_posts.append(
                            PostMedia(
                                post_id=tweet_id,
                                post_url=tweet_url,
                                profile_url=profile_url,
                                platform=self.PLATFORM,
                                media_type=MediaType.UNKNOWN,
                                caption=tweet_caption,
                                timestamp=post_timestamp,
                                cookies_file=str(self.netscape_cookie_path) if self.netscape_cookie_path.exists() else None,
                            )
                        )
                        new_found += 1
                    if should_stop:
                        break
                except Exception:
                    continue

            if should_stop:
                break

            if new_found == 0:
                no_new_count += 1
                logger.debug(f"Tidak ada tweet baru ({no_new_count}/{MAX_NO_NEW})")
            else:
                no_new_count = 0  # Reset counter jika ada yang baru

            # Simpan tinggi halaman sebelum scroll
            last_height = await page.evaluate("document.body.scrollHeight")

            # Scroll ke bawah untuk lazy loading
            await page.evaluate("window.scrollBy(0, window.innerHeight * 3)")
            await asyncio.sleep(random.uniform(2, 4))

            # Cek tinggi halaman setelah scroll
            new_height = await page.evaluate("document.body.scrollHeight")
            if new_height == last_height:
                consecutive_same_height_count += 1
                logger.debug(
                    f"Scroll height tidak bertambah ({consecutive_same_height_count}/{MAX_SAME_HEIGHT})"
                )
                if consecutive_same_height_count >= MAX_SAME_HEIGHT:
                    logger.info("Hard Limit Protection terpicu: Tinggi halaman mentok. Keluar loop.")
                    break
            else:
                consecutive_same_height_count = 0

        return collected_posts

    def _extract_username(self, url: str) -> str:
        """Ekstrak username Twitter/X dari URL profil."""
        # Format: https://x.com/username atau https://twitter.com/username
        match = re.search(r"(?:x|twitter)\.com/([^/?&#/]+)/?", url)
        if match:
            username = match.group(1)
            # Skip halaman non-profil
            if username not in {"home", "explore", "notifications", "messages", "i"}:
                return username
        return ""

    def _extract_tweet_id(self, url: str) -> str:
        """Ekstrak tweet ID dari URL tweet."""
        match = re.search(r"/status/(\d+)", url)
        return match.group(1) if match else ""

    async def close(self) -> None:
        """Tutup browser dan playwright."""
        if self._context:
            try:
                await self._context.close()
            except Exception:
                pass

        if self._browser:
            try:
                await self._browser.close()
            except Exception:
                pass

        if self._playwright:
            try:
                await self._playwright.stop()
            except Exception:
                pass

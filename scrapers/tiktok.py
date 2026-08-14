"""
scrapers/tiktok.py
Scraper untuk profil TikTok menggunakan Playwright + yt-dlp.

Strategi:
- Gunakan Playwright untuk membuka halaman profil TikTok
- Scroll ke bawah untuk melakukan lazy loading video list
- Ambil semua link video dengan selector a[href*="/video/"]
- Anti-duplikasi: cek SQLite sebelum memproses postingan
- Urutan: dari paling lama ke paling baru (dibalik setelah crawl selesai)
"""

import asyncio
import json
import logging
import os
import re
from pathlib import Path
from typing import AsyncGenerator, Optional

from playwright.async_api import (
    Browser,
    BrowserContext,
    Page,
    async_playwright,
)

import aiofiles
from .base import BaseScraper, MediaType, PostMedia, USER_AGENT

logger = logging.getLogger(__name__)


class TikTokScraper(BaseScraper):
    """
    Scraper profil TikTok menggunakan Playwright (browser automation)
    untuk mengumpulkan video URL, kemudian yt-dlp untuk mengunduhnya.
    """

    PLATFORM = "tiktok"
    REHYDRATION_SELECTOR = "#__UNIVERSAL_DATA_FOR_REHYDRATION__"

    def __init__(self, db_manager, session_dir: str, headed: bool = True):
        super().__init__(db_manager, session_dir)
        self.headed = headed
        self.session_dir = Path(session_dir)
        self.session_dir.mkdir(parents=True, exist_ok=True)
        self.netscape_cookie_path = self.session_dir / "tiktok_cookies.txt"
        self._playwright = None
        self._browser: Optional[Browser] = None
        self._context: Optional[BrowserContext] = None

    async def _init_browser(self) -> Page:
        """Inisialisasi browser Playwright standard dengan konfigurasi Headless Stealth tingkat tinggi."""
        self._playwright = await async_playwright().start()
        brave_path = BaseScraper.get_brave_path()
        if brave_path:
            logger.info(f"Menggunakan browser Brave dari: {brave_path}")

        # FIX: replaced manual launch_kwargs dict with base class helper
        # to ensure all stealth args (--disable-web-security, --disable-features etc.)
        # are consistent across all scrapers
        launch_kwargs = BaseScraper.get_browser_launch_kwargs()
        launch_kwargs["headless"] = not self.headed

        self._browser = await self._playwright.chromium.launch(**launch_kwargs)
        # FIX: replace `assert self._browser is not None` with explicit RuntimeError.
        # assert is stripped when Python runs with -O (optimized flag), making the
        # guard invisible in production. RuntimeError always fires regardless of flags.
        if self._browser is None:
            raise RuntimeError("Playwright failed to launch browser — chromium.launch() returned None")
        
        self._context = await self._browser.new_context(
            user_agent=USER_AGENT,
            viewport={"width": 1280, "height": 720},
            locale="en-US,en;q=0.9",
            timezone_id="Asia/Jakarta"
        )

        # DEEP STEALTH INJECTION: Menyamarkan seluruh properti headless object agar dikira browser manusia asli
        assert self._context is not None
        await self._context.add_init_script("""
            () => {
                // 1. Clear Webdriver flag
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                
                // 2. Mock Chrome runtime structure
                window.chrome = { runtime: {}, loadTimes: function() {}, csi: function() {} };
                
                // 3. Fake Languages & Plugins
                Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
                Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
            }
        """)

        netscape_path = await self.load_and_inject_cookies(self._context, "tiktok")
        if not netscape_path:
            raise ValueError("File cookie TikTok tidak ditemukan atau kosong.")
        self.netscape_cookie_path = netscape_path

        page = await self._context.new_page()
        return page

    async def scrape_profile(
        self,
        profile_url: str,
        forced: bool = False,
    ) -> AsyncGenerator[PostMedia, None]:
        """
        Membuka profil TikTok via Playwright, mengumpulkan seluruh URL video,
        dan memprosesnya dengan pengecekan database SQLite (resume feature).
        """
        logger.info(f"Mulai crawl TikTok profil: {profile_url}")

        username = self._extract_username(profile_url)
        if not username:
            logger.error(f"Tidak bisa ekstrak username dari URL: {profile_url}")
            return

        canonical_url = f"https://www.tiktok.com/@{username}"
        try:
            page = await self._init_browser()
        except Exception as e:
            logger.error(f"[❌ ERROR  ] Menghentikan scraping profil TikTok karena kesalahan browser/cookie: {e}")
            self.failed = True
            return

        try:
            logger.info(f"Membuka halaman profil TikTok: {canonical_url}")
            # domcontentloaded: lebih cepat dari networkidle — TikTok punya infinite
            # background XHR polling yang membuat networkidle tidak pernah tercapai
            # sehingga hang selama 60 detik penuh sebelum timeout.
            await page.goto(canonical_url, wait_until="domcontentloaded", timeout=60000)

            # Tunggu elemen feed TikTok yang stabil muncul sebelum mulai scroll.
            logger.info("Menunggu feed TikTok siap...")
            try:
                await page.wait_for_selector('a[href*="/video/"], a[href*="/photo/"]', timeout=15000)
            except Exception:
                logger.warning("Selector feed TikTok tidak muncul dalam batas timeout; lanjutkan dengan fallback.")

            # 3. PERBAIKAN SCROLL: Menggunakan deteksi tinggi halaman + ekstraksi real-time (DOM virtualization bypass)
            logger.info("Memulai proses scrolling dan pencatatan feed TikTok...")
            collected_urls = []
            seen_urls = set()
            last_height = await page.evaluate("document.body.scrollHeight")
            no_new_scroll_matches = 0

            while no_new_scroll_matches < 5:  # Allow 5 stable scroll breathers before exiting
                # 1. Ambil snapshot URL terarah dari browser context
                urls_snapshot = await page.evaluate("""
                    (targetUsername) => {
                        // Scope exclusively to the main profile timeline feed grid container
                        const gridContainer = document.querySelector('[data-testid="user-post-item-list"]') || document;
                        const links = gridContainer.querySelectorAll('a[href*="/video/"], a[href*="/photo/"]');
                        
                        return Array.from(links)
                            .map(a => a.href)
                            .filter(href => {
                                const lowerHref = href.toLowerCase();
                                const u = targetUsername.toLowerCase();
                                return lowerHref.includes('/@' + u + '/video/') || 
                                       lowerHref.includes('/' + u + '/video/') ||
                                       lowerHref.includes('/@' + u + '/photo/') ||
                                       lowerHref.includes('/' + u + '/photo/');
                            });
                    }
                """, username)

                new_added = 0
                for href in urls_snapshot:
                    clean_url = href.split("?")[0]
                    # Fix TikTok relative URLs
                    if clean_url.startswith('/'):
                        clean_url = f"https://www.tiktok.com{clean_url}"
                        
                    # NOTE: do NOT rewrite /photo/ → /video/ here.
                    # download_post uses "/photo/" in the URL to trigger carousel-first
                    # fallback. Rewriting it would route photo posts to yt-dlp video
                    # extraction first, causing the 15s rehydration timeout on every slide.

                    # Cek duplikasi O(1) menggunakan set
                    if clean_url not in seen_urls:
                        collected_urls.append(clean_url)
                        seen_urls.add(clean_url)
                        new_added += 1

                # 2. Scroll ke bawah untuk memancing data baru
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await asyncio.sleep(3.5)

                new_height = await page.evaluate("document.body.scrollHeight")
                if new_height == last_height and new_added == 0:
                    no_new_scroll_matches += 1
                else:
                    no_new_scroll_matches = 0
                    
                last_height = new_height

            logger.info(
                f"Ditemukan total {len(collected_urls)} postingan URL dari profil TikTok @{username}"
            )

            # Optimasi sorting dengan Decorate-Sort-Undecorate (DSU) pattern
            # Menghindari eksekusi parser regex berulang-ulang di dalam loop sorting
            decorated = [(int(self._extract_post_id(url) or 0), url) for url in collected_urls]
            # FIX: sort comment was misleading — reverse=True = descending = NEWEST FIRST.
            # _patrol_account calls new_posts.reverse() before Discord upload,
            # flipping back to OLDEST FIRST for correct chronological upload order.
            # This double-reverse is intentional: scraper yields newest-first,
            # caller reverses to oldest-first. Do not remove either reverse.
            decorated.sort(key=lambda x: x[0], reverse=True)  # newest-first; caller reverses to oldest-first
            video_list = [url for _, url in decorated]

            # FIX: track how many posts are filtered vs yielded so operators can
            # distinguish "scraper broken" from "all posts already in DB (normal patrol)".
            yield_count = 0
            filtered_count = 0

            for post_url in video_list:
                post_id = self._extract_post_id(post_url)
                if not post_id:
                    continue

                # Anti-duplikasi check
                if await self.db.check_post_exists(post_id, self.PLATFORM):
                    logger.debug(f"TikTok post {post_id} sudah ada di DB — skip")
                    filtered_count += 1
                    continue

                yield_count += 1
                yield PostMedia(
                    post_id=post_id,
                    post_url=post_url,
                    profile_url=canonical_url,
                    platform=self.PLATFORM,
                    # Tipe media dideteksi dinamis oleh downloader via yt-dlp images metadata
                    media_type=MediaType.UNKNOWN,
                    caption=f"TikTok Post {post_id}",  # Caption default, yt-dlp akan update/ambil aslinya nanti
                    timestamp=None,
                    cookies_file=str(self.netscape_cookie_path) if self.netscape_cookie_path.exists() else None,
                )

            # Log summary: makes "67 found → 0 yielded" visible instead of silent
            if filtered_count > 0 and yield_count == 0:
                logger.info(
                    f"TikTok @{username}: {filtered_count}/{len(video_list)} post sudah di DB — "
                    f"tidak ada postingan baru untuk di-yield."
                )
            elif filtered_count > 0:
                logger.info(
                    f"TikTok @{username}: {yield_count} post baru di-yield, "
                    f"{filtered_count} sudah di DB (di-skip)."
                )


        except Exception as e:
            logger.error(f"[❌ ERROR  ] Error saat scraping profil TikTok: {e}")
        finally:
            await self.close()

    def _extract_username(self, url: str) -> str:
        """Ekstrak username TikTok dari berbagai format URL."""
        match = re.search(r"tiktok\.com/@([^/?&#]+)", url)
        return match.group(1) if match else ""

    def _extract_post_id(self, url: str) -> str:
        """Ekstrak ID video atau foto TikTok dari URL."""
        match = re.search(r"/(?:video|photo)/(\d+)", url)
        return match.group(1) if match else ""

    async def close(self) -> None:
        """Tutup browser Playwright."""
        try:
            if self._context:
                await self._context.close()
            if self._browser:
                await self._browser.close()
            if self._playwright:
                await self._playwright.stop()
        except Exception as e:
            logger.warning(f"Error saat menutup browser TikTok: {e}")
        finally:
            self._context = None
            self._browser = None
            self._playwright = None

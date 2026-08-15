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
from core.utils import TAG_CRAWL, TAG_SYSTEM, TAG_WARN, TAG_ERROR, TAG_SUCCESS

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

        # Gunakan timeline profil utama langsung (https://x.com/username)
        # Tab /media sering hanya merender thumbnail grid atau gagal lazy load di headless browser
        timeline_url = f"https://x.com/{username}"
        logger.info(f"Mulai crawl timeline Twitter: {timeline_url}")

        if self._context is None:
            raise RuntimeError("Browser context is not initialized")
        page = await self._context.new_page()

        try:
            try:
                logger.info(f"[🔍 CRAWL] Membuka Timeline Twitter target: {timeline_url}")
                # domcontentloaded: hindari timeout karena infinite XHR polling Twitter
                await page.goto(timeline_url, wait_until="domcontentloaded", timeout=20000)
                
                # Target primaryColumn dan tunggu selector tweet muncul
                await page.wait_for_selector('[data-testid="primaryColumn"]', state="attached", timeout=15000)
                try:
                    await page.wait_for_selector('article[data-testid="tweet"]', timeout=15000)
                except Exception:
                    logger.warning("Tweet selector tidak muncul dalam 15s — mencoba lanjut scroll.")
            except Exception as e:
                logger.error(f"[❌ ERROR  ] Gagal memuat Timeline Twitter atau struktur halaman korup: {e}")
                return

            if "i/flow/login" in page.url or "login" in page.url:
                logger.error(f"{TAG_ERROR} Sesi Cookie expired! Redirect ke login page: {page.url}")
                return

            # Mulai proses scrolling dan ekstraksi data asli
            logger.info(f"{TAG_CRAWL} Mulai scroll timeline Media Twitter @{username}...")
            all_posts = await self._scroll_and_collect_tweets(page, username, profile_url, forced=forced)

            logger.info(
                f"{TAG_CRAWL} Selesai: {len(all_posts)} tweet media terkumpul dari @{username}."
            )

            # Urutan alami: terbaru ke terlama

            for post in all_posts:
                yield post

        finally:
            await page.close()

    async def _scroll_and_collect_tweets(
        self, page: Page, username: str, profile_url: str, forced: bool = False
    ) -> list[PostMedia]:
        """
        Scroll tab Media Twitter menggunakan fast browser JavaScript evaluation
        dan smart stop-condition (cek database SQLite).
        """
        collected_posts = []
        seen_ids = set()
        no_new_count = 0
        MAX_NO_NEW = 4
        consecutive_same_height_count = 0
        MAX_SAME_HEIGHT = 3
        scroll_round = 0

        logger.info(f"{TAG_CRAWL} Memulai scroll loop @{username} (max {35} pass, stop {MAX_NO_NEW}x empty)...")

        while no_new_count < MAX_NO_NEW:
            scroll_round += 1

            # Evaluasi langsung di context Javascript browser (super cepat, 0 round-trip latency)
            tweets_snapshot = await page.evaluate(
                """
                (targetUsername) => {
                    const u = targetUsername.toLowerCase();
                    const articles = document.querySelectorAll('article[data-testid="tweet"], [data-testid="tweet"], article');
                    const results = [];

                    for (const article of articles) {
                        // Cari link status update milik username target
                        const links = article.querySelectorAll('a[href*="/status/"]');
                        let statusHref = null;
                        for (const l of links) {
                            const h = l.getAttribute('href') || l.href || '';
                            if (h.toLowerCase().includes('/' + u + '/status/')) {
                                statusHref = h;
                                break;
                            }
                        }
                        if (!statusHref) continue;

                        const timeEl = article.querySelector('time');
                        const timestamp = timeEl ? timeEl.getAttribute('datetime') : null;

                        const textEl = article.querySelector('[data-testid="tweetText"]');
                        const caption = textEl ? textEl.innerText : '';

                        // Ekstrak URL gambar langsung jika ada (Twitter media image)
                        const mediaImgs = Array.from(article.querySelectorAll('img[src*="pbs.twimg.com/media/"], [data-testid="tweetPhoto"] img'))
                            .map(img => img.src)
                            .filter(src => src && !src.includes('profile_images') && !src.includes('emoji'));

                        // Cek apakah ada video atau gif
                        const hasVideo = !!article.querySelector('video, [data-testid="videoPlayer"], [data-testid="videoComponent"]');

                        // Filter: HANYA ambil tweet yang memiliki media (foto atau video)
                        if (mediaImgs.length === 0 && !hasVideo) {
                            continue; // Lewati tweet teks biasa tanpa media
                        }

                        results.push({
                            href: statusHref,
                            timestamp: timestamp,
                            caption: caption,
                            media_urls: mediaImgs,
                            is_video: hasVideo
                        });
                    }
                    return results;
                }
                """,
                username,
            )

            new_found = 0
            should_stop = False

            for item in tweets_snapshot:
                href = item["href"]
                tweet_id = self._extract_tweet_id(href)
                if not tweet_id or tweet_id in seen_ids:
                    continue

                seen_ids.add(tweet_id)
                tweet_url = f"https://x.com/{username}/status/{tweet_id}"

                # STOP-CONDITION: Cek SQLite jika tweet ini sudah pernah di-scrape
                if not forced and await self.db.check_post_exists(tweet_id, self.PLATFORM):
                    logger.info(
                        f"{TAG_CRAWL} Stop-condition: tweet {tweet_id} sudah di DB — "
                        f"resume point ditemukan."
                    )
                    should_stop = True
                    break

                # Bersihkan URL media Twitter ke kualitas original tertinggi (name=orig)
                cleaned_media_urls = []
                for m_url in item.get("media_urls", []):
                    if "pbs.twimg.com/media/" in m_url:
                        clean_src = m_url.split("?")[0]
                        if "format=" in m_url:
                            high_res = re.sub(r"name=\w+", "name=orig", m_url)
                            if "name=" not in high_res:
                                high_res += "&name=orig"
                        else:
                            high_res = f"{clean_src}?format=jpg&name=orig"
                        cleaned_media_urls.append(high_res)
                    else:
                        cleaned_media_urls.append(m_url)

                is_video = item.get("is_video", False)
                media_type = MediaType.VIDEO if is_video else MediaType.PHOTO

                collected_posts.append(
                    PostMedia(
                        post_id=tweet_id,
                        post_url=tweet_url,
                        profile_url=profile_url,
                        platform=self.PLATFORM,
                        media_type=media_type,
                        caption=item.get("caption", ""),
                        timestamp=item.get("timestamp"),
                        media_urls=cleaned_media_urls if cleaned_media_urls else [],
                        cookies_file=str(self.netscape_cookie_path)
                        if self.netscape_cookie_path.exists()
                        else None,
                    )
                )
                new_found += 1

            if should_stop:
                break

            if new_found > 0:
                logger.info(
                    f"{TAG_CRAWL} Scroll [{scroll_round}]: +{new_found} tweet baru "
                    f"(total: {len(collected_posts)})"
                )
                no_new_count = 0
            else:
                no_new_count += 1
                logger.debug(
                    f"{TAG_CRAWL} Scroll [{scroll_round}]: tidak ada tweet baru "
                    f"({no_new_count}/{MAX_NO_NEW})"
                )

            # Simpan data sebelum scroll
            await page.evaluate("window.scrollBy(0, 1600)")
            await asyncio.sleep(random.uniform(2.5, 3.5))

            # Hard stop protection: jika sudah 35 putaran scroll tanpa batas
            if scroll_round >= 35:
                logger.info(f"{TAG_CRAWL} Batas 35 scroll tercapai untuk @{username} — stop.")
                break

        logger.info(f"{TAG_CRAWL} Scroll selesai @{username}: {len(collected_posts)} tweet media.")
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
        """Tutup browser dan playwright dengan aman + timeout anti-hang untuk headless server."""
        async def _safe_close(coro, label: str) -> None:
            try:
                await asyncio.wait_for(coro, timeout=10.0)
            except asyncio.TimeoutError:
                logger.warning(f"Timeout menutup {label} — lanjutkan shutdown")
            except Exception as e:
                logger.warning(f"Error menutup {label}: {e}")

        if self._context:
            await _safe_close(self._context.close(), "Twitter browser context")
        if self._browser:
            await _safe_close(self._browser.close(), "Twitter browser")
        if self._playwright:
            await _safe_close(self._playwright.stop(), "Twitter playwright")

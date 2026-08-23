"""
scrapers/instagram.py
Engine Scraper Profil Instagram 100% Guest Mode (Zero-Login).

Fitur & Keamanan:
1. Zero-Login Architecture:
   - Tidak memerlukan cookie akun login pengguna sama sekali, bebas dari resiko ban atau checkpoint akun.
2. Multi-Tier Public Extraction:
   - Tier 1: Instagram Public Web API (api/v1/users/web_profile_info/?username={username}) menggunakan curl_cffi TLS impersonation (Chrome 124) dan X-IG-App-ID header resmi (936619743392459).
   - Tier 2: gallery-dl Subprocess Extractor (ekstraksi feed publik langsung tanpa browser).
   - Tier 3: yt-dlp Flat-Playlist (fallback metadata ekstraksi cepat).
   - Tier 4: Playwright Stealth Browser (fallback DOM parser dengan deteksi login wall instan).
3. Direct Video & Image CDN URL Resolution:
   - Menyimpan direct CDN URL video (video_url) dan foto (display_url / sidecar) langsung pada PostMedia sehingga downloader tidak perlu re-scrape jika URL sudah tersedia.
4. Anti-Zombie Subprocess Guards:
   - Seluruh subprocess dilengkapi asyncio.TimeoutError handler dengan proc.kill() dan proc.wait().
5. DSU Chronological Sorting:
   - Stop-condition evaluation pada post terbaru, lalu dispatch ke downstream secara kronologis (oldest-first).
"""

import asyncio
import json
import logging
import os
import random
import re
from pathlib import Path
from typing import AsyncGenerator, Optional, Any
from datetime import datetime, timezone

import httpx

try:
    from curl_cffi import requests as curl_requests
    HAS_CURL_CFFI = True
except ImportError:
    HAS_CURL_CFFI = False

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
    TAG_DOWN,
)

logger = logging.getLogger(__name__)

# Instagram Web App ID resmi untuk public web API client
IG_WEB_APP_ID = "936619743392459"


class InstagramScraper(BaseScraper):
    """
    Scraper profil Instagram 100% Guest Mode (Zero-Login) berbasis Multi-Tier API & Subprocess Fallback.
    """

    PLATFORM = "instagram"

    def __init__(self, db_manager, session_dir: str, headed: bool = False):
        super().__init__(db_manager, session_dir)
        self.headed = headed
        self.session_dir = Path(session_dir)
        self.session_dir.mkdir(parents=True, exist_ok=True)
        self._playwright = None
        self._browser = None
        self._context = None

    def _extract_username(self, url: str) -> str:
        """Ekstrak username Instagram dari berbagai format URL profil."""
        match = re.search(r"instagram\.com/([^/?&#/]+)/?", url)
        if match:
            username = match.group(1)
            if username not in {"p", "reel", "explore", "stories", "accounts"}:
                return username.lower().strip()
        cleaned = url.split("?")[0].rstrip("/").split("/")[-1]
        return cleaned.lower().strip() if cleaned else ""

    def _extract_post_id(self, url: str) -> str:
        """Ekstrak shortcode postingan dari URL Instagram."""
        match = re.search(r"/(?:p|reel)/([A-Za-z0-9_-]+)", url)
        return match.group(1) if match else ""

    async def scrape_profile(
        self,
        profile_url: str,
        forced: bool = False,
    ) -> AsyncGenerator[PostMedia, None]:
        """
        Crawl profil Instagram secara 100% Guest Mode tanpa memerlukan akun login.
        """
        username = self._extract_username(profile_url)
        if not username:
            logger.error(f"{TAG_ERROR} Username tidak valid dari URL: {profile_url}")
            return
        self._profile_url_username = username

        canonical_url = f"https://www.instagram.com/{username}/"
        logger.info(f"{TAG_CRAWL} Memulai scraping profil @{username} (instagram, Zero-Login Mode): {canonical_url}")

        collected_posts: list[PostMedia] = []
        seen_shortcodes: set[str] = set()

        # ─────────────────────────────────────────────────────────────────────
        # TIER 1: Instagram Public Web API (curl_cffi TLS Impersonation + X-IG-App-ID)
        # Super cepat, tanpa browser overhead, mengekstrak timeline publik
        # ─────────────────────────────────────────────────────────────────────
        logger.info(f"{TAG_CRAWL} [TIER 1] Menjalankan Instagram Public Web API untuk @{username}...")
        try:
            api_url = f"https://www.instagram.com/api/v1/users/web_profile_info/?username={username}"
            headers = {
                "User-Agent": USER_AGENT,
                "X-IG-App-ID": IG_WEB_APP_ID,
                "Accept": "*/*",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": canonical_url,
                "Sec-Fetch-Dest": "empty",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Site": "same-origin",
            }

            resp_json = None
            if HAS_CURL_CFFI:
                try:
                    async with curl_requests.AsyncSession(impersonate="chrome124") as session:
                        resp = await session.get(api_url, headers=headers, timeout=15)
                        if resp.status_code == 200:
                            resp_json = resp.json()
                except Exception as curl_err:
                    logger.debug(f"curl_cffi web_profile_info note: {curl_err}")

            if not resp_json:
                async with httpx.AsyncClient(headers=headers, follow_redirects=True, timeout=15.0) as client:
                    resp = await client.get(api_url)
                    if resp.status_code == 200:
                        resp_json = resp.json()

            if resp_json:
                user_data = resp_json.get("data", {}).get("user")
                if user_data:
                    is_private = user_data.get("is_private", False)
                    if is_private:
                        logger.warning(f"{TAG_WARN} Profil @{username} bersifat Private — tidak dapat diakses dalam Guest Mode.")
                        return

                    media_timeline = user_data.get("edge_owner_to_timeline_media", {})
                    edges = media_timeline.get("edges", [])
                    logger.info(f"{TAG_CRAWL} [TIER 1] Public Web API menemukan {len(edges)} post pada timeline @{username}.")

                    for edge in edges:
                        node = edge.get("node", {})
                        shortcode = node.get("shortcode")
                        if not shortcode or shortcode in seen_shortcodes:
                            continue

                        seen_shortcodes.add(shortcode)
                        p_url = f"https://www.instagram.com/p/{shortcode}/"
                        is_video = node.get("is_video", False)
                        media_type = MediaType.VIDEO if is_video else MediaType.PHOTO

                        # Caption & Timestamp
                        caption = ""
                        edges_caption = node.get("edge_media_to_caption", {}).get("edges", [])
                        if edges_caption:
                            caption = edges_caption[0].get("node", {}).get("text", "") or ""

                        ts_val = node.get("taken_at_timestamp")
                        ts_str = None
                        if ts_val:
                            ts_str = datetime.fromtimestamp(ts_val, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

                        # Media direct URLs
                        media_urls = []
                        if is_video:
                            v_url = node.get("video_url")
                            if v_url:
                                media_urls.append(v_url)
                        else:
                            display_url = node.get("display_url")
                            if display_url:
                                media_urls.append(display_url)
                            # Cek carousel items jika ada
                            sidecar = node.get("edge_sidecar_to_children", {}).get("edges", [])
                            if len(sidecar) > 1:
                                media_type = MediaType.CAROUSEL
                                media_urls = []
                                for c_edge in sidecar:
                                    c_node = c_edge.get("node", {})
                                    c_url = c_node.get("video_url") if c_node.get("is_video") else c_node.get("display_url")
                                    if c_url:
                                        media_urls.append(c_url)

                        collected_posts.append(
                            PostMedia(
                                post_id=shortcode,
                                post_url=p_url,
                                profile_url=canonical_url,
                                platform=self.PLATFORM,
                                media_type=media_type,
                                media_urls=media_urls,
                                caption=caption,
                                timestamp=ts_str,
                                cookies_file=None,
                            )
                        )
        except Exception as e:
            logger.debug(f"Tier 1 Public Web API extraction info: {e}")

        # ─────────────────────────────────────────────────────────────────────
        # TIER 2: gallery-dl Subprocess Extractor (Guest Mode Fallback)
        # ─────────────────────────────────────────────────────────────────────
        if not collected_posts:
            logger.info(f"{TAG_CRAWL} [TIER 2] Menjalankan gallery-dl subprocess untuk @{username} (Zero-Login)...")
            gdl_posts = await self._fetch_via_gallery_dl(canonical_url)
            for p in gdl_posts:
                if p.post_id not in seen_shortcodes:
                    seen_shortcodes.add(p.post_id)
                    collected_posts.append(p)

        # ─────────────────────────────────────────────────────────────────────
        # TIER 3: yt-dlp Flat-Playlist Extractor (Guest Mode Fallback)
        # ─────────────────────────────────────────────────────────────────────
        if not collected_posts:
            logger.info(f"{TAG_CRAWL} [TIER 3] Menjalankan yt-dlp flat-playlist untuk @{username} (Zero-Login)...")
            ytdl_posts = await self._fetch_via_ytdlp_flat(canonical_url)
            for p in ytdl_posts:
                if p.post_id not in seen_shortcodes:
                    seen_shortcodes.add(p.post_id)
                    collected_posts.append(p)

        # ─────────────────────────────────────────────────────────────────────
        # TIER 4: Playwright Stealth Browser (Guest Mode DOM Fallback)
        # ─────────────────────────────────────────────────────────────────────
        if not collected_posts:
            logger.info(f"{TAG_CRAWL} [TIER 4] Menjalankan Playwright Guest Browser untuk @{username}...")
            browser_posts = await self._fetch_via_guest_browser(canonical_url, username)
            for p in browser_posts:
                if p.post_id not in seen_shortcodes:
                    seen_shortcodes.add(p.post_id)
                    collected_posts.append(p)

        logger.info(f"{TAG_CRAWL} Total {len(collected_posts)} postingan @{username} berhasil dikumpulkan.")

        # ─────────────────────────────────────────────────────────────────────
        # STEP 5: DSU Sorting & SQLite Checkpoint
        # Urutan: Evaluasi stop-condition dari post terbaru -> Yield oldest-first ke Discord
        # ─────────────────────────────────────────────────────────────────────
        # 1. Sort newest-first untuk evaluasi checkpoint database
        collected_posts.sort(
            key=lambda x: str(x.timestamp) if x.timestamp else "1970-01-01T00:00:00.000Z",
            reverse=True,
        )

        pending_posts: list[PostMedia] = []
        for post in collected_posts:
            if not forced and await self.db.check_post_exists(post.post_id, self.PLATFORM):
                logger.info(f"{TAG_CRAWL} Stop-condition: post {post.post_id} sudah ada di DB — checkpoint tercapai.")
                break
            pending_posts.append(post)

        # 2. Re-sort ke kronologis tertib (oldest-to-newest) untuk pengiriman teratur ke Discord
        pending_posts.sort(
            key=lambda x: str(x.timestamp) if x.timestamp else "1970-01-01T00:00:00.000Z",
            reverse=False,
        )

        logger.info(f"[⚙️ SYSTEM] Mengirim {len(pending_posts)} post Instagram ke downstream pipeline...")
        for post_media in pending_posts:
            yield post_media
            await asyncio.sleep(random.uniform(0.5, 1.5))

    async def _fetch_via_gallery_dl(self, url: str) -> list[PostMedia]:
        """Fallback ekstraksi metadata via gallery-dl subprocess dalam Guest Mode."""
        results: list[PostMedia] = []
        try:
            cmd = ["gallery-dl", "--dump-json", "--range", "1-20", url]
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            try:
                stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=30.0)
                if proc.returncode == 0 and stdout:
                    for line in stdout.decode("utf-8", errors="ignore").splitlines():
                        try:
                            data = json.loads(line.strip())
                            item_data = data[1] if (isinstance(data, list) and len(data) >= 2 and isinstance(data[1], dict)) else (data if isinstance(data, dict) else {})
                            shortcode = item_data.get("shortcode") or item_data.get("code")
                            if not shortcode:
                                continue

                            caption = item_data.get("description") or item_data.get("caption") or ""
                            date_str = item_data.get("date")
                            is_video = bool(item_data.get("video_url") or item_data.get("format") == "video")

                            results.append(
                                PostMedia(
                                    post_id=shortcode,
                                    post_url=f"https://www.instagram.com/p/{shortcode}/",
                                    profile_url=url,
                                    platform=self.PLATFORM,
                                    media_type=MediaType.VIDEO if is_video else MediaType.PHOTO,
                                    caption=caption,
                                    timestamp=date_str,
                                    cookies_file=None,
                                )
                            )
                        except Exception:
                            pass
            except asyncio.TimeoutError:
                proc.kill()
                await proc.wait()
                logger.warning(f"{TAG_WARN} gallery-dl subprocess timeout dibatalkan.")
        except Exception as e:
            logger.debug(f"gallery-dl guest extractor info: {e}")
        return results

    async def _fetch_via_ytdlp_flat(self, url: str) -> list[PostMedia]:
        """Fallback ekstraksi URL postingan via yt-dlp flat playlist dalam Guest Mode."""
        results: list[PostMedia] = []
        try:
            cmd = ["yt-dlp", "--flat-playlist", "--dump-json", "--no-warnings", url]
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            try:
                stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=30.0)
                if proc.returncode == 0 and stdout:
                    for line in stdout.decode("utf-8", errors="ignore").splitlines():
                        try:
                            data = json.loads(line.strip())
                            p_url = data.get("url") or data.get("webpage_url")
                            if p_url and ("/p/" in p_url or "/reel/" in p_url):
                                shortcode = self._extract_post_id(p_url)
                                if shortcode:
                                    is_video = "/reel/" in p_url
                                    results.append(
                                        PostMedia(
                                            post_id=shortcode,
                                            post_url=p_url.split("?")[0],
                                            profile_url=url,
                                            platform=self.PLATFORM,
                                            media_type=MediaType.VIDEO if is_video else MediaType.PHOTO,
                                            caption=data.get("description") or data.get("title") or "",
                                            cookies_file=None,
                                        )
                                    )
                        except Exception:
                            pass
            except asyncio.TimeoutError:
                proc.kill()
                await proc.wait()
                logger.warning(f"{TAG_WARN} yt-dlp flat-playlist timeout dibatalkan.")
        except Exception as e:
            logger.debug(f"yt-dlp flat-playlist guest info: {e}")
        return results

    async def _fetch_via_guest_browser(self, url: str, username: str) -> list[PostMedia]:
        """Fallback ekstraksi DOM via Playwright Guest Mode Browser."""
        results: list[PostMedia] = []
        try:
            from playwright.async_api import async_playwright
            self._playwright = await async_playwright().start()
            self._browser, self._context = await BaseScraper.create_stealth_browser(
                self._playwright,
                headed=self.headed,
                viewport={"width": 1280, "height": 800},
            )
            page = await self._context.new_page()

            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(2.0)

            # Cek login wall instan
            current_url = page.url.lower()
            if "accounts/login" in current_url or "challenge" in current_url:
                logger.warning(f"{TAG_WARN} Instagram mengalihkan @{username} ke login wall — melewati browser pass.")
                await page.close()
                return []

            # Dismiss modal popup jika ada
            try:
                close_btn = await page.query_selector(
                    'button:has-text("Not Now"), button:has-text("Lain Kali"), [aria-label="Close"], [aria-label="Tutup"]'
                )
                if close_btn:
                    await close_btn.click()
                    await asyncio.sleep(1.0)
            except Exception:
                pass

            links = await page.locator('a[href*="/p/"], a[href*="/reel/"]').all()
            seen_ids = set()
            for link in links:
                try:
                    href = await link.get_attribute("href")
                    if href:
                        p_url = f"https://www.instagram.com{href}" if href.startswith("/") else href
                        p_id = self._extract_post_id(p_url)
                        if p_id and p_id not in seen_ids:
                            seen_ids.add(p_id)
                            is_reel = "/reel/" in p_url
                            results.append(
                                PostMedia(
                                    post_id=p_id,
                                    post_url=p_url.split("?")[0],
                                    profile_url=url,
                                    platform=self.PLATFORM,
                                    media_type=MediaType.VIDEO if is_reel else MediaType.PHOTO,
                                    cookies_file=None,
                                )
                            )
                except Exception:
                    pass

            await page.close()
        except Exception as e:
            logger.debug(f"Guest browser fallback error: {e}")
        finally:
            await self.close()

        return results

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

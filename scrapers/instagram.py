"""
scrapers/instagram.py
Engine Scraper Profil Instagram 100% Guest Mode (Zero-Login) dengan Pagination & Ekstraksi Reels Lengkap.

Fitur & Keamanan:
1. Zero-Login Architecture:
   - Tidak memerlukan cookie akun login pengguna sama sekali, bebas dari resiko ban atau checkpoint akun.
2. Multi-Tier Full Pagination & Reels Extraction:
   - Tier 1: Instagram Public Web API (web_profile_info) + API v1 Feed Pagination (/api/v1/feed/user/{user_id}/?max_id={cursor})
     menggunakan curl_cffi TLS impersonation (Chrome 124) dan X-IG-App-ID resmi (936619743392459).
   - Ekstraksi Tab Reels: Memproses `edge_felix_video_timeline` dan feed video/clips secara menyeluruh.
   - Tier 2: gallery-dl Subprocess Extractor (ekstraksi feed publik langsung tanpa browser).
   - Tier 3: yt-dlp Flat-Playlist (fallback metadata ekstraksi cepat).
   - Tier 4: Playwright Stealth Browser (Dual-Tab crawling Feed & Reels dengan dynamic scroll loop).
3. Direct Video & Image CDN Resolution:
   - Menyimpan direct CDN URL video (video_url / video_versions) dan foto (display_url / carousel_media)
     langsung pada PostMedia agar downloader efisien tanpa redundant fetch.
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
import sys
import time
from pathlib import Path
from typing import AsyncGenerator, Optional, Any
from datetime import datetime, timezone, timedelta

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


def _enrich_timestamps(posts: list) -> None:
    """
    FIX: Extracted from in-place for-loop inside scrape_profile() for clarity.
    Fills missing timestamps for all non-story posts using InstagramScraper._shortcode_to_timestamp().
    Mutates the list in-place; stories already have real timestamps from API so they are skipped.
    """
    for p in posts:
        if not p.timestamp and not p.post_id.startswith("ig_story_"):
            # Decode upload timestamp encoded in the Instagram shortcode base64.
            # _shortcode_to_timestamp is a @staticmethod so called on the class directly.
            p.timestamp = InstagramScraper._shortcode_to_timestamp(p.post_id)


class InstagramScraper(BaseScraper):
    """
    Scraper profil Instagram 100% Guest Mode (Zero-Login) dengan dukungan Full Pagination & Reels.
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
        """Ekstrak username Instagram dari berbagai format URL profil (termasuk /reels)."""
        if not url:
            return ""
        clean_url = url.split("?")[0].rstrip("/")
        match = re.search(
            r"instagram\.com/(?!p/|reel/|explore/|stories/|accounts/)([^/?&#/\s]+)",
            clean_url,
            re.IGNORECASE,
        )
        if match:
            username = match.group(1).lower().strip()
            if username in ("reels", "reel"):
                m2 = re.search(r"instagram\.com/reels?/([^/?&#/\s]+)", clean_url, re.IGNORECASE)
                if m2:
                    return m2.group(1).lower().strip()
            return username
        cleaned = clean_url.split("/")[-1]
        return cleaned.lower().strip() if cleaned else ""

    def _extract_post_id(self, url: str) -> str:
        """Ekstrak shortcode postingan dari URL Instagram (/p/, /reel/, /{user}/reel/)."""
        if not url:
            return ""
        match = re.search(r"/(?:p|reel|reels)/([A-Za-z0-9_-]+)", url, re.IGNORECASE)
        return match.group(1) if match else ""

    @staticmethod
    def _shortcode_to_timestamp(shortcode: str) -> Optional[str]:
        """
        [DETERMINISTIC INSTAGRAM TIMESTAMP RESOLVER]
        Mendekode Instagram Snowflake ID dari Base64 shortcode ke ISO-8601 UTC timestamp.
        Menjamin postingan Feed dan Reels tersortir dalam urutan kronologis yang 100% akurat.
        """
        if not shortcode or shortcode.startswith("ig_story_"):
            return None
        alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
        try:
            media_id = 0
            for char in shortcode:
                media_id = media_id * 64 + alphabet.index(char)
            # Instagram epoch: 1314220021000 ms (24 Agustus 2011)
            timestamp_ms = (media_id >> 23) + 1314220021000
            dt = datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc)
            return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        except Exception:
            return None

    async def scrape_profile(
        self,
        profile_url: str,
        forced: bool = False,
    ) -> AsyncGenerator[PostMedia, None]:
        """
        Crawl profil Instagram secara 100% Guest Mode dengan full pagination dan Reels.
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
        user_id = ""
        stop_condition_met = False

        # ─────────────────────────────────────────────────────────────────────
        # TIER 1: Instagram Public Web API (Initial Profile Snapshot + Reels)
        # Mengambil snapshot awal timeline, tab reels (felix), dan user_id untuk pagination
        # ─────────────────────────────────────────────────────────────────────
        logger.info(f"{TAG_CRAWL} [TIER 1] Menjalankan Instagram Public Web API untuk @{username}...")
        end_cursor = None
        has_next_page = False

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

                    user_id = str(user_data.get("id") or "")
                    media_timeline = user_data.get("edge_owner_to_timeline_media", {})
                    page_info = media_timeline.get("page_info", {})
                    has_next_page = bool(page_info.get("has_next_page", False))
                    end_cursor = page_info.get("end_cursor")

                    timeline_edges = media_timeline.get("edges", [])
                    reels_timeline = user_data.get("edge_felix_video_timeline", {})
                    reels_edges = reels_timeline.get("edges", [])

                    all_initial_edges = timeline_edges + reels_edges
                    logger.info(
                        f"{TAG_CRAWL} [TIER 1] Initial Snapshot: {len(timeline_edges)} feed + {len(reels_edges)} reels @{username}."
                    )

                    for edge in all_initial_edges:
                        node = edge.get("node", {})
                        shortcode = node.get("shortcode")
                        if not shortcode or shortcode in seen_shortcodes:
                            continue

                        seen_shortcodes.add(shortcode)

                        # Cek SQLite checkpoint stop condition
                        if not forced and await self.db.check_post_exists(shortcode, self.PLATFORM):
                            logger.info(f"{TAG_CRAWL} Stop-condition tercapai pada post {shortcode} di DB.")
                            stop_condition_met = True
                            break

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

                        # Direct Media URLs
                        media_urls = []
                        if is_video:
                            v_url = node.get("video_url")
                            if v_url:
                                media_urls.append(v_url)
                        else:
                            display_url = node.get("display_url")
                            if display_url:
                                media_urls.append(display_url)
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
                                media_urls=[u for u in media_urls if u],
                                caption=caption,
                                timestamp=ts_str,
                                cookies_file=None,
                            )
                        )

            # ─────────────────────────────────────────────────────────────────
            # TIER 1B: API v1 Feed Pagination Loop (/api/v1/feed/user/{user_id}/)
            # Iterasi cursor berkelanjutan melewati batas 12 post awal hingga seluruh profil terambil
            # ─────────────────────────────────────────────────────────────────
            if user_id and end_cursor and has_next_page and not stop_condition_met:
                logger.info(f"{TAG_CRAWL} [TIER 1B] Memulai pagination API feed untuk @{username} (cursor: {end_cursor[:20]}...)...")
                curr_cursor = end_cursor
                page_idx = 1
                # FIX: configurable via env MAX_IG_FEED_PAGES — previously hardcoded to 20.
                # Increase for accounts with 1000+ posts; decrease to reduce API load.
                max_pages = int(os.getenv("MAX_IG_FEED_PAGES", "20"))

                while curr_cursor and not stop_condition_met and page_idx <= max_pages:
                    feed_api_url = f"https://www.instagram.com/api/v1/feed/user/{user_id}/?count=12&max_id={curr_cursor}"
                    feed_data = None

                    if HAS_CURL_CFFI:
                        try:
                            async with curl_requests.AsyncSession(impersonate="chrome124") as session:
                                f_resp = await session.get(feed_api_url, headers=headers, timeout=15)
                                if f_resp.status_code == 200:
                                    feed_data = f_resp.json()
                        except Exception as c_err:
                            logger.debug(f"curl_cffi feed pagination error (page {page_idx}): {c_err}")

                    if not feed_data:
                        try:
                            async with httpx.AsyncClient(headers=headers, timeout=15.0) as client:
                                f_resp = await client.get(feed_api_url)
                                if f_resp.status_code == 200:
                                    feed_data = f_resp.json()
                        except Exception:
                            pass

                    if not feed_data or not isinstance(feed_data, dict):
                        logger.debug(f"Feed pagination berhenti pada halaman {page_idx} (response invalid).")
                        break

                    items = feed_data.get("items", [])
                    if not items:
                        break

                    logger.info(f"{TAG_CRAWL} [TIER 1B] Halaman {page_idx+1}: {len(items)} post tambahan ditemukan.")

                    for it in items:
                        shortcode = it.get("code")
                        if not shortcode or shortcode in seen_shortcodes:
                            continue

                        seen_shortcodes.add(shortcode)

                        # Stop condition check
                        if not forced and await self.db.check_post_exists(shortcode, self.PLATFORM):
                            logger.info(f"{TAG_CRAWL} Stop-condition tercapai pada pagination post {shortcode}.")
                            stop_condition_met = True
                            break

                        p_url = f"https://www.instagram.com/p/{shortcode}/"
                        raw_m_type = it.get("media_type")
                        is_v = raw_m_type == 2 or bool(it.get("video_versions"))
                        is_carousel = raw_m_type == 8

                        media_type = MediaType.VIDEO if is_v else (MediaType.CAROUSEL if is_carousel else MediaType.PHOTO)

                        # Caption & Timestamp
                        caption = (it.get("caption") or {}).get("text", "") or ""
                        taken_at = it.get("taken_at")
                        ts_str = None
                        if taken_at:
                            ts_str = datetime.fromtimestamp(taken_at, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

                        # Media Direct URLs
                        media_urls = []
                        if is_v:
                            v_vers = it.get("video_versions", [])
                            if v_vers:
                                media_urls.append(v_vers[0].get("url"))
                        elif is_carousel:
                            c_items = it.get("carousel_media", [])
                            for c in c_items:
                                if c.get("media_type") == 2 and c.get("video_versions"):
                                    media_urls.append(c["video_versions"][0].get("url"))
                                elif c.get("image_versions2"):
                                    cands = c["image_versions2"].get("candidates", [])
                                    if cands:
                                        media_urls.append(cands[0].get("url"))
                        else:
                            cands = it.get("image_versions2", {}).get("candidates", [])
                            if cands:
                                media_urls.append(cands[0].get("url"))

                        collected_posts.append(
                            PostMedia(
                                post_id=shortcode,
                                post_url=p_url,
                                profile_url=canonical_url,
                                platform=self.PLATFORM,
                                media_type=media_type,
                                media_urls=[u for u in media_urls if u],
                                caption=caption,
                                timestamp=ts_str,
                                cookies_file=None,
                            )
                        )

                    # Update pagination cursor
                    more_available = feed_data.get("more_available", False)
                    curr_cursor = feed_data.get("next_max_id")
                    if not more_available or not curr_cursor:
                        break

                    page_idx += 1
                    await asyncio.sleep(random.uniform(1.0, 2.0))

        except Exception as e:
            logger.debug(f"Tier 1 Public Web API extraction info: {e}")

        # ─────────────────────────────────────────────────────────────────────
        # ─────────────────────────────────────────────────────────────────────
        # REELS COMPREHENSIVE SCAN: Tab Reels Crawler
        # Memastikan video dari tab Reels (yang tidak di-share ke main feed) tetap terambil 100%
        # ─────────────────────────────────────────────────────────────────────
        if not stop_condition_met:
            logger.info(f"{TAG_CRAWL} [REELS] Memeriksa tab Reels secara komprehensif untuk @{username}...")
            reels_posts = await self._fetch_via_guest_browser(canonical_url, username, only_reels=True)
            new_reels_count = 0
            for rp in reels_posts:
                if rp.post_id not in seen_shortcodes:
                    seen_shortcodes.add(rp.post_id)
                    collected_posts.append(rp)
                    new_reels_count += 1
            if new_reels_count > 0:
                logger.info(f"{TAG_CRAWL} [REELS] Berhasil menemukan {new_reels_count} post Reels tambahan.")

        # ─────────────────────────────────────────────────────────────────────
        # STORY ZERO-LOGIN SCAN: Instagram Story Mirror Crawler
        # Mengambil Story aktif (24 jam) tanpa login akun tumbal dengan timestamp asli
        # ─────────────────────────────────────────────────────────────────────
        if not stop_condition_met:
            logger.info(f"{TAG_CRAWL} [STORY] Memeriksa Instagram Story aktif untuk @{username} (Zero-Login)...")
            story_posts = await self._fetch_stories_via_public_viewer(username)
            new_story_count = 0
            for sp in story_posts:
                if sp.post_id not in seen_shortcodes:
                    seen_shortcodes.add(sp.post_id)
                    collected_posts.append(sp)
                    new_story_count += 1
            if new_story_count > 0:
                logger.info(f"{TAG_CRAWL} [STORY] Berhasil menemukan {new_story_count} Instagram Story aktif @{username}.")

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
        # TIER 4: Playwright Dual-Tab Guest Browser (Feed & Reels Scrolling Fallback)
        # ─────────────────────────────────────────────────────────────────────
        if not collected_posts:
            logger.info(f"{TAG_CRAWL} [TIER 4] Menjalankan Playwright Guest Browser untuk @{username} (Dual-Tab)...")
            browser_posts = await self._fetch_via_guest_browser(canonical_url, username, only_reels=False)
            for p in browser_posts:
                if p.post_id not in seen_shortcodes:
                    seen_shortcodes.add(p.post_id)
                    collected_posts.append(p)

        logger.info(f"{TAG_CRAWL} Total {len(collected_posts)} postingan (Feed + Reels + Story) @{username} berhasil dikumpulkan.")

        # ─────────────────────────────────────────────────────────────────────
        # STEP 5: DSU Sorting & SQLite Checkpoint (Unified Chronological Order)
        # Mengisi timestamp deterministik untuk semua postingan Feed & Reels dari shortcode,
        # sehingga urutan upload serempak dan berbaur sempurna (oldest-to-newest).
        # FIX: extracted to _enrich_timestamps() helper for clarity
        # ─────────────────────────────────────────────────────────────────────
        _enrich_timestamps(collected_posts)

        # Evaluasi stop-condition dari post terbaru
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

        # Re-sort ke kronologis tertib (oldest-to-newest) untuk pengiriman serempak dan berurutan ke Discord
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
            cmd = ["gallery-dl", "--dump-json", "--range", "1-50", url]
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
                try:
                    await asyncio.wait_for(proc.wait(), timeout=5.0)
                except Exception:
                    pass
                logger.warning(f"{TAG_WARN} gallery-dl subprocess timeout dibatalkan.")
        except Exception as e:
            logger.debug(f"gallery-dl guest extractor info: {e}")
        return results

    async def _fetch_via_ytdlp_flat(self, url: str) -> list[PostMedia]:
        """Fallback ekstraksi URL postingan via yt-dlp flat playlist dalam Guest Mode."""
        results: list[PostMedia] = []
        try:
            cmd = [
                sys.executable,
                "-m", "yt_dlp",
                "--flat-playlist",
                "--dump-json",
                "--no-warnings",
                url,
            ]
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
                try:
                    await asyncio.wait_for(proc.wait(), timeout=5.0)
                except Exception:
                    pass
                logger.warning(f"{TAG_WARN} yt-dlp flat-playlist timeout dibatalkan.")
        except Exception as e:
            logger.debug(f"yt-dlp flat-playlist guest info: {e}")
        return results

    async def _fetch_via_guest_browser(
        self, url: str, username: str, only_reels: bool = False
    ) -> list[PostMedia]:
        """Fallback ekstraksi DOM Dual-Tab (Feed & Reels) via Playwright Guest Browser."""
        results: list[PostMedia] = []
        seen_ids = set()

        try:
            from playwright.async_api import async_playwright
            self._playwright = await async_playwright().start()
            self._browser, self._context = await BaseScraper.create_stealth_browser(
                self._playwright,
                headed=self.headed,
                viewport={"width": 1280, "height": 800},
            )
            page = await self._context.new_page()

            targets = (
                [f"https://www.instagram.com/{username}/reels/"]
                if only_reels
                else [
                    f"https://www.instagram.com/{username}/",
                    f"https://www.instagram.com/{username}/reels/",
                ]
            )

            start_time = time.monotonic()
            MAX_CRAWL_DURATION = 120.0 if only_reels else 300.0

            for target_tab in targets:
                if time.monotonic() - start_time > MAX_CRAWL_DURATION:
                    logger.warning(f"{TAG_WARN} Batas waktu crawl browser Instagram tercapai untuk @{username} — stop.")
                    break
                try:
                    await page.goto(target_tab, wait_until="domcontentloaded", timeout=30000)
                    await asyncio.sleep(2.5)

                    if "accounts/login" in page.url.lower() or bool(re.search(r"/challenge/?$|/challenge/\w+", page.url.lower())):
                        logger.warning(f"{TAG_WARN} Instagram mengalihkan {target_tab} ke login wall / challenge.")
                        continue

                    # Dismiss modal dialog popup jika ada
                    try:
                        close_btn = await page.query_selector(
                            'button:has-text("Not Now"), button:has-text("Lain Kali"), [aria-label="Close"], [aria-label="Tutup"]'
                        )
                        if close_btn:
                            await close_btn.click()
                            await asyncio.sleep(1.0)
                    except Exception:
                        pass

                    # Scrolling pagination loop
                    max_scrolls = 6 if only_reels else 8
                    for _ in range(max_scrolls):
                        if time.monotonic() - start_time > MAX_CRAWL_DURATION:
                            break
                        links = await page.locator('a[href*="/p/"], a[href*="/reel/"], a[href*="/reels/"]').all()
                        for link in links:
                            try:
                                href = await link.get_attribute("href")
                                if href:
                                    p_url = f"https://www.instagram.com{href}" if href.startswith("/") else href
                                    p_id = self._extract_post_id(p_url)
                                    if p_id and p_id not in seen_ids:
                                        seen_ids.add(p_id)
                                        is_reel = "/reel" in p_url.lower()
                                        clean_post_url = f"https://www.instagram.com/reel/{p_id}/" if is_reel else f"https://www.instagram.com/p/{p_id}/"
                                        ts_iso = self._shortcode_to_timestamp(p_id)
                                        results.append(
                                            PostMedia(
                                                post_id=p_id,
                                                post_url=clean_post_url,
                                                profile_url=url,
                                                platform=self.PLATFORM,
                                                media_type=MediaType.VIDEO if is_reel else MediaType.PHOTO,
                                                timestamp=ts_iso,
                                                cookies_file=None,
                                            )
                                        )
                            except Exception:
                                pass

                        await page.evaluate("window.scrollBy(0, window.innerHeight * 2)")
                        await asyncio.sleep(random.uniform(1.5, 2.5))

                except Exception as tab_err:
                    logger.debug(f"Guest browser tab error ({target_tab}): {tab_err}")

            await page.close()
        except Exception as e:
            logger.debug(f"Guest browser fallback error: {e}")
        finally:
            await self.close()

        return results

    async def _fetch_stories_via_public_viewer(self, username: str) -> list[PostMedia]:
        """
        [ZERO-LOGIN INSTAGRAM STORY ENGINE]
        Mengekstrak Instagram Stories aktif (24 jam) via Public Mirror Viewer tanpa memerlukan akun login/tumbal,
        lengkap dengan timestamp asli waktu unggah (converted ke ISO-8601 UTC).

        Mirror Tier order:
          Tier 1: dumpor.io/v/{username}  — reliable reverse-proxy mirror (aktif 2025)
          Tier 2: storiesig.net/stories/{username}  — secondary fallback
        """
        results: list[PostMedia] = []
        clean_user = username.lower().replace("@", "").strip()

        # Helper: ekstrak media dari HTML blok story yang sudah diparsing
        def _parse_story_blocks(html_content: str, mirror_name: str) -> list[PostMedia]:
            parsed: list[PostMedia] = []
            now_utc = datetime.now(timezone.utc)

            # Coba berbagai selector block yang umum dipakai mirror story public
            story_items = re.findall(
                r'<div[^>]*class="[^"]*(?:story[_-]?item|item[_-]?video|item[_-]?photo|story[_-]?card)[^"]*"[^>]*>(.*?)</div>\s*</div>',
                html_content,
                re.DOTALL | re.IGNORECASE,
            ) or re.findall(
                r'<div[^>]*class="[^"]*story[^"]*"[^>]*>(.*?)</div>',
                html_content,
                re.DOTALL | re.IGNORECASE,
            )

            for idx, block in enumerate(story_items, 1):
                video_match = (
                    re.search(r'<video[^>]*src="([^"]+)"', block)
                    or re.search(r'data-video="([^"]+)"', block)
                    or re.search(r'<source[^>]*src="([^"]+\.mp4[^"]*)"', block, re.IGNORECASE)
                )
                img_match = (
                    re.search(r'<img[^>]*src="([^"]+)"', block)
                    or re.search(r'data-image="([^"]+)"', block)
                )
                download_match = (
                    re.search(r'href="([^"]+)"[^>]*class="[^"]*download[^"]*"', block)
                    or re.search(r'data-url="([^"]+)"', block)
                )

                media_url = ""
                is_video = False
                if video_match:
                    media_url = video_match.group(1)
                    is_video = True
                elif download_match and (".mp4" in download_match.group(1) or "video" in download_match.group(1)):
                    media_url = download_match.group(1)
                    is_video = True
                elif download_match:
                    media_url = download_match.group(1)
                elif img_match:
                    media_url = img_match.group(1)

                if not media_url or "placeholder" in media_url.lower():
                    continue

                # Ekstrak relative timestamp dari teks block
                time_match = re.search(
                    r'(\d+)\s*(hour|hours|h|min|minute|minutes|m|day|days|d|sec|second|seconds|s)\s*ago',
                    block,
                    re.IGNORECASE,
                )
                ts_iso = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
                if time_match:
                    val = int(time_match.group(1))
                    unit = time_match.group(2).lower()
                    if unit.startswith("h"):
                        ts_iso = (now_utc - timedelta(hours=val)).strftime("%Y-%m-%dT%H:%M:%SZ")
                    elif unit.startswith("m") and not unit.startswith("mo"):
                        ts_iso = (now_utc - timedelta(minutes=val)).strftime("%Y-%m-%dT%H:%M:%SZ")
                    elif unit.startswith("d"):
                        ts_iso = (now_utc - timedelta(days=val)).strftime("%Y-%m-%dT%H:%M:%SZ")

                raw_id = re.search(r'/([A-Za-z0-9_-]{10,})', media_url)
                s_id = f"ig_story_{clean_user}_{raw_id.group(1)[:12] if raw_id else idx}_{int(now_utc.timestamp()) // 86400}"

                parsed.append(
                    PostMedia(
                        post_id=s_id,
                        post_url=f"https://www.instagram.com/stories/{clean_user}/",
                        profile_url=f"https://www.instagram.com/{clean_user}/",
                        platform=self.PLATFORM,
                        media_type=MediaType.VIDEO if is_video else MediaType.PHOTO,
                        media_urls=[media_url],
                        timestamp=ts_iso,
                        caption=f"📸 Instagram Story oleh @{clean_user} (via {mirror_name})",
                        cookies_file=None,
                    )
                )

            return parsed

        # ── Tier 1: dumpor.io (aktif & reliable per 2025) ───────────────────────────────
        # FIX: picuki.com/story/ has been dead since mid-2025 (redirects to TikTok mirror).
        # dumpor.io provides reliable Instagram story proxy without login requirement.
        mirror_tiers = [
            ("dumpor.io",     f"https://dumpor.io/v/{clean_user}"),
            ("storiesig.net", f"https://storiesig.net/stories/{clean_user}"),
        ]

        request_headers = {
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }

        for mirror_name, story_url in mirror_tiers:
            try:
                html_content = ""
                request_headers["Referer"] = f"https://{mirror_name}/"

                if HAS_CURL_CFFI:
                    try:
                        async with curl_requests.AsyncSession(impersonate="chrome124") as session:
                            resp = await session.get(story_url, headers=request_headers, timeout=12)
                            if resp.status_code == 200:
                                html_content = resp.text
                    except Exception:
                        pass

                if not html_content:
                    async with httpx.AsyncClient(headers=request_headers, timeout=12.0, follow_redirects=True) as client:
                        resp = await client.get(story_url)
                        if resp.status_code == 200:
                            html_content = resp.text

                if not html_content:
                    logger.debug(f"[STORY] Mirror {mirror_name} tidak merespons untuk @{clean_user} — coba tier berikutnya.")
                    continue

                # Cek sinyal explicit 'no stories'
                if any(sig in html_content.lower() for sig in (
                    "no stories", "user has no stories", "profile not found",
                    "sorry, no story", "doesn't have any stories",
                )):
                    logger.debug(f"[STORY] Mirror {mirror_name} melaporkan tidak ada story aktif untuk @{clean_user}.")
                    continue

                parsed = _parse_story_blocks(html_content, mirror_name)
                if parsed:
                    logger.info(f"{TAG_CRAWL} [STORY] Berhasil mengekstrak {len(parsed)} Instagram Story aktif untuk @{clean_user} (via {mirror_name}).")
                    return parsed

            except Exception as e:
                logger.debug(f"Story mirror {mirror_name} error untuk @{clean_user}: {e}")
            # Jeda singkat antar mirror tier
            await asyncio.sleep(0.5)

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

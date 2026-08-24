"""
scrapers/tiktok.py
Engine Scraper Profil TikTok 100% Guest Mode (Zero-Login) murni berbasis Multi-Tier API-First Architecture.

Arsitektur & Keamanan:
1. 100% Zero-Browser (No-Playwright for Profile Crawling):
   - Sepenuhnya bebas dari browser Playwright headless untuk mencegah deteksi bot WAF / Captcha di Linux server.
2. Multi-Tier API-First Architecture:
   - Pass 0 (Primary): TikWM User Posts API (Fast, no-auth, JSON response langsung).
   - Pass 1: Scrapling / curl_cffi Chrome124 Fast SSR Rehydration Parser.
   - Pass 2: yt-dlp Flat-Playlist dengan Mobile API extractor args.
3. Strict Author Verification:
   - Validasi ketat author UID dan format URL agar postingan Repost, Likes, maupun Feed Recommendation tidak ikut terambil.
4. Chronological DSU Delivery:
   - Stop-condition evaluation pada post terbaru ke terlama di SQLite, lalu yield ke downstream secara kronologis (oldest-first).
5. Abort Guard:
   - Jika semua layer API gagal, scraper menandai `failed = True` dan `blocked_by_challenge = True` agar tidak menandai scan selesai palsu.
"""

import asyncio
import json
import logging
import os
import re
from pathlib import Path
from typing import AsyncGenerator, Optional, Any
import httpx

from .base import BaseScraper, MediaType, PostMedia, USER_AGENT
from core.utils import (
    TAG_CRAWL,
    TAG_SYSTEM,
    TAG_SUCCESS,
    TAG_WARN,
    TAG_ERROR,
)

logger = logging.getLogger(__name__)


class TikTokScraper(BaseScraper):
    """
    Scraper profil TikTok Zero-Login murni berbasis API-First tanpa browser Playwright.
    """

    PLATFORM = "tiktok"

    def __init__(self, db_manager, session_dir: str, headed: bool = False):
        super().__init__(db_manager, session_dir)
        self.headed = headed
        self.session_dir = Path(session_dir)
        self.session_dir.mkdir(parents=True, exist_ok=True)
        self.netscape_cookie_path = self.session_dir / "tiktok_cookies.txt"
        self.failed = False
        self.blocked_by_challenge = False

    def _extract_username(self, url: str) -> Optional[str]:
        """Ekstrak clean username dari berbagai variasi format URL TikTok."""
        match = re.search(r"@([a-zA-Z0-9_\.\-]+)", url)
        if match:
            return match.group(1).lower().strip()
        cleaned = url.split("?")[0].rstrip("/").split("/")[-1]
        if cleaned.startswith("@"):
            return cleaned[1:].lower().strip()
        return cleaned.lower().strip() if cleaned else None

    def _extract_post_id(self, url: str) -> Optional[str]:
        """Ekstrak numeric ID postingan TikTok dari URL."""
        match = re.search(r"/(?:video|photo|v)/(\d{15,22})", url)
        return match.group(1) if match else None

    def _is_valid_author_post_url(self, post_url: str, target_username: str) -> bool:
        """
        [STRICT AUTHOR URL SANITIZER]
        Hanya terima URL jika path URL diawali persis dengan @{target_username}/(video|photo|v)/{numeric_id}.
        Menolak 100% URL video rekomendasi atau sidebar dari akun lain.
        """
        if not post_url or not isinstance(post_url, str):
            return False
        clean_url = post_url.lower().split("?")[0].rstrip("/")
        u = target_username.lower().replace("@", "").strip()
        expected_patterns = (
            f"tiktok.com/@{u}/video/",
            f"tiktok.com/@{u}/photo/",
            f"tiktok.com/@{u}/v/",
        )
        return any(pat in clean_url for pat in expected_patterns)

    async def scrape_profile(
        self,
        profile_url: str,
        forced: bool = False,
    ) -> AsyncGenerator[PostMedia, None]:
        """
        Crawl profil TikTok secara 100% Guest Mode (Zero-Login & Zero-Browser) berbasis API-First.
        """
        self.failed = False
        self.blocked_by_challenge = False

        username = self._extract_username(profile_url)
        if not username:
            logger.error(f"{TAG_ERROR} Tidak bisa ekstrak username dari URL: {profile_url}")
            return

        canonical_url = f"https://www.tiktok.com/@{username}"
        logger.info(f"{TAG_CRAWL} Memulai scraping profil @{username} (tiktok, Zero-Login API-First Mode): {canonical_url}")

        collected_urls: list[str] = []
        seen_urls: set[str] = set()

        # ─────────────────────────────────────────────────────────────────────
        # PASS 0 (PRIMARY): TikWM User Posts API (No Browser Overhead)
        # ─────────────────────────────────────────────────────────────────────
        logger.info(f"{TAG_CRAWL} [PASS 0] Menjalankan TikWM User Posts API untuk @{username}...")
        try:
            api_url = f"https://www.tikwm.com/api/user/posts?unique_id={username}&count=50"
            headers = {
                "User-Agent": USER_AGENT,
                "Accept": "application/json, text/plain, */*",
                "Referer": "https://www.tikwm.com/",
            }
            resp_data = None
            try:
                from curl_cffi import requests as curl_requests
                async with curl_requests.AsyncSession(impersonate="chrome124") as session:
                    c_resp = await session.get(api_url, headers=headers, timeout=15)
                    if c_resp.status_code == 200:
                        resp_data = c_resp.json()
            except Exception:
                pass

            if not resp_data:
                async with httpx.AsyncClient(headers=headers, timeout=20.0, follow_redirects=True) as client:
                    resp = await client.get(api_url)
                    if resp.status_code == 200:
                        resp_data = resp.json()

            if resp_data and resp_data.get("code") == 0 and "data" in resp_data:
                d_obj = resp_data["data"]
                v_list = d_obj.get("videos", []) or d_obj.get("itemList", []) or []
                for v in v_list:
                    video_id = str(v.get("video_id") or v.get("id") or "")
                    author_data = v.get("author", {})
                    author_uid = str(author_data.get("unique_id") or author_data.get("uniqueId") or "").lower().strip()

                    # Strict Author Verification
                    if author_uid and author_uid != username:
                        continue

                    if video_id and re.match(r"^\d{15,22}$", video_id):
                        is_photo = bool(v.get("images") or v.get("imagePost"))
                        t = "photo" if is_photo else "video"
                        p_url = f"https://www.tiktok.com/@{username}/{t}/{video_id}"
                        if self._is_valid_author_post_url(p_url, username) and p_url not in seen_urls:
                            collected_urls.append(p_url)
                            seen_urls.add(p_url)

                if collected_urls:
                    logger.info(f"{TAG_CRAWL} [PASS 0] Berhasil menemukan {len(collected_urls)} postingan asli via TikWM API.")
        except Exception as e:
            logger.debug(f"[PASS 0] TikWM API info: {e}")

        # ─────────────────────────────────────────────────────────────────────
        # PASS 1: Scrapling / curl_cffi Fast SSR Rehydration
        # ─────────────────────────────────────────────────────────────────────
        if not collected_urls:
            logger.info(f"{TAG_CRAWL} [PASS 1] Menjalankan Scrapling Fast SSR Rehydration untuk @{username}...")
            try:
                html_text = ""
                ssr_headers = {
                    "User-Agent": USER_AGENT,
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.9,id;q=0.8",
                    "Referer": "https://www.tiktok.com/",
                    "Sec-Ch-Ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
                    "Sec-Ch-Ua-Mobile": "?0",
                    "Sec-Ch-Ua-Platform": '"Windows"',
                }
                try:
                    from curl_cffi import requests as curl_requests
                    async with curl_requests.AsyncSession(impersonate="chrome124") as session:
                        resp = await session.get(canonical_url, headers=ssr_headers, timeout=15)
                        if resp.status_code == 200:
                            html_text = resp.text
                except (ImportError, ModuleNotFoundError, Exception):
                    async with httpx.AsyncClient(headers=ssr_headers, follow_redirects=True, timeout=15.0) as client:
                        resp = await client.get(canonical_url)
                        if resp.status_code == 200:
                            html_text = resp.text

                # Hard Abort jika halaman menyajikan Captcha / WAF Challenge
                if html_text and any(block in html_text for block in ("verify-center", "SlardarWAF", "secsdk", "captcha")):
                    logger.warning(f"{TAG_WARN} TikTok SSR terhalang WAF Challenge — membatalkan parsing HTML.")
                    html_text = ""

                if html_text:
                    parsed_urls = self._parse_rehydration_from_html(html_text, username)
                    for r_url in parsed_urls:
                        if self._is_valid_author_post_url(r_url, username) and r_url not in seen_urls:
                            collected_urls.append(r_url)
                            seen_urls.add(r_url)
                    if collected_urls:
                        logger.info(f"{TAG_CRAWL} [PASS 1] Fast SSR berhasil menemukan {len(collected_urls)} post.")
            except Exception as e:
                logger.debug(f"[PASS 1] SSR error: {e}")

        # ─────────────────────────────────────────────────────────────────────
        # PASS 2: yt-dlp Flat-Playlist (Mobile API Mode)
        # ─────────────────────────────────────────────────────────────────────
        if not collected_urls:
            logger.info(f"{TAG_CRAWL} [PASS 2] Mencoba ekstraksi postingan @{username} via yt-dlp flat-playlist...")
            try:
                cmd = [
                    "yt-dlp",
                    "--flat-playlist",
                    "--dump-json",
                    "--no-warnings",
                    "--extractor-args", "tiktok:api_hostname=api16-normal-c-useast1a.tiktokv.com",
                    canonical_url,
                ]
                proc = await asyncio.create_subprocess_exec(
                    *cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                stdout, _ = await proc.communicate()
                if proc.returncode == 0 and stdout:
                    for line in stdout.decode("utf-8", errors="ignore").splitlines():
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            entry = json.loads(line)
                            p_id = entry.get("id")
                            uploader = str(entry.get("uploader") or entry.get("uploader_id") or "").lower().replace("@", "").strip()
                            if uploader and uploader != username:
                                continue
                            if p_id and re.match(r"^\d{15,22}$", str(p_id)):
                                p_url = f"https://www.tiktok.com/@{username}/video/{p_id}"
                                if self._is_valid_author_post_url(p_url, username) and p_url not in seen_urls:
                                    collected_urls.append(p_url)
                                    seen_urls.add(p_url)
                        except Exception:
                            pass
                    if collected_urls:
                        logger.info(f"{TAG_CRAWL} [PASS 2] yt-dlp flat-playlist berhasil menemukan {len(collected_urls)} post.")
            except Exception as e:
                logger.debug(f"[PASS 2] yt-dlp error: {e}")

        # ─────────────────────────────────────────────────────────────────────
        # ABORT GUARD: Jika Semua Layer API Gagal
        # ─────────────────────────────────────────────────────────────────────
        if not collected_urls:
            logger.warning(
                f"{TAG_WARN} @{username} (tiktok) scraping tidak berhasil (semua pass API/HTTP kosong) — "
                f"membatalkan status selesai untuk dicoba ulang di siklus berikutnya."
            )
            self.failed = True
            self.blocked_by_challenge = True
            return

        logger.info(f"{TAG_CRAWL} Total {len(collected_urls)} URL postingan asli @{username} berhasil dikumpulkan.")

        # ─────────────────────────────────────────────────────────────────────
        # DSU Sorting: Urutkan dari TERLAMA ke TERBARU (Oldest to Newest)
        # ─────────────────────────────────────────────────────────────────────
        decorated = [(int(self._extract_post_id(url) or 0), url) for url in collected_urls]
        decorated.sort(key=lambda x: x[0], reverse=False)
        sorted_post_urls = [url for _, url in decorated]

        cookies_file_str = str(self.netscape_cookie_path) if self.netscape_cookie_path.exists() else None

        for post_url in sorted_post_urls:
            post_id = self._extract_post_id(post_url)
            if not post_id:
                continue

            if not forced and await self.db.check_post_exists(post_id, self.PLATFORM):
                logger.info(f"{TAG_CRAWL} Checkpoint: post {post_id} sudah ada di DB — skip.")
                continue

            media_type = MediaType.PHOTO if "/photo/" in post_url else MediaType.VIDEO

            yield PostMedia(
                post_id=post_id,
                post_url=post_url,
                profile_url=profile_url,
                platform=self.PLATFORM,
                media_type=media_type,
                cookies_file=cookies_file_str,
            )

    def _parse_rehydration_from_html(self, html: str, username: str) -> list[str]:
        """
        Parse data rehydration JSON dari HTML SSR TikTok dengan strict author verification.
        """
        found_urls = set()
        u = username.lower().replace("@", "").strip()

        def search_json(obj: Any) -> None:
            if not obj or not isinstance(obj, (dict, list)):
                return
            if isinstance(obj, dict):
                # Filter out Repost flag
                if obj.get("isRepost") or obj.get("repost") or obj.get("is_repost"):
                    return

                item_module = obj.get("ItemModule")
                if isinstance(item_module, dict):
                    for p_id, item in item_module.items():
                        if isinstance(item, dict):
                            author = item.get("author") or item.get("authorName") or ""
                            if isinstance(author, dict):
                                author = author.get("uniqueId") or author.get("unique_id") or ""
                            if str(author).lower().replace("@", "").strip() == u and re.match(r"^\d{15,22}$", str(p_id)):
                                is_photo = bool(item.get("imagePost") or item.get("images") or item.get("imageList"))
                                t = "photo" if is_photo else "video"
                                p_url = f"https://www.tiktok.com/@{u}/{t}/{p_id}"
                                if self._is_valid_author_post_url(p_url, u):
                                    found_urls.add(p_url)

                user_detail = obj.get("webapp.user-detail") or obj.get("user-detail") or obj.get("userPage")
                if isinstance(user_detail, dict):
                    u_items = user_detail.get("itemList") or user_detail.get("videoList")
                    if isinstance(u_items, list):
                        for it in u_items:
                            if isinstance(it, str) and re.match(r"^\d{15,22}$", it):
                                p_url = f"https://www.tiktok.com/@{u}/video/{it}"
                                if self._is_valid_author_post_url(p_url, u):
                                    found_urls.add(p_url)
                            elif isinstance(it, dict):
                                a_info = it.get("author") or {}
                                a_uid = a_info.get("uniqueId") or a_info.get("unique_id") if isinstance(a_info, dict) else str(a_info)
                                if str(a_uid).lower().replace("@", "").strip() == u:
                                    p_id = it.get("id") or it.get("itemId") or it.get("vid")
                                    if p_id and re.match(r"^\d{15,22}$", str(p_id)):
                                        is_photo = bool(it.get("imagePost") or it.get("images"))
                                        t = "photo" if is_photo else "video"
                                        p_url = f"https://www.tiktok.com/@{u}/{t}/{p_id}"
                                        if self._is_valid_author_post_url(p_url, u):
                                            found_urls.add(p_url)

                for val in obj.values():
                    search_json(val)
            elif isinstance(obj, list):
                for elem in obj:
                    search_json(elem)

        scripts = re.findall(
            r'<script[^>]*id="(?:__UNIVERSAL_DATA_FOR_REHYDRATION__|SIGI_STATE|__NEXT_DATA__)"[^>]*>(.*?)</script>',
            html,
            re.DOTALL,
        )
        for script_content in scripts:
            try:
                data = json.loads(script_content.strip())
                search_json(data)
            except Exception:
                pass

        return list(found_urls)

    async def close(self) -> None:
        """Cleanup handler (no-op karena zero-browser architecture)."""
        pass

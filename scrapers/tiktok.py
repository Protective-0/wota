"""
scrapers/tiktok.py
Scraper profil TikTok menggunakan Built-in Stealth Browser + Fast HTTP Rehydration & yt-dlp.

Fitur & Keamanan:
1. Strict Target Scope: Hanya mengekstrak postingan yang dibuat langsung oleh target creator (@username).
   Anti-leakage: Mengabaikan sidebar navigasi, rekomendasi, feed penonton, dan repost.
2. Unauthenticated Guest First: Crawl profil secara guest murni tanpa cookie personal penonton
   agar TikTok tidak mencemari DOM dengan repost / feed pribadi akun yang login.
3. Fast Pass: Ekstraksi Rehydration JSON via HTTPX (Cepat & hemat resource).
4. Dynamic Pass: Playwright Stealth Browser dengan container selector spesifik ([data-e2e="user-post-item"]).
5. DSU (Decorate-Sort-Undecorate) sorting untuk kronologi postingan yang akurat.
"""

import asyncio
import json
import logging
import os
import re
from pathlib import Path
from typing import AsyncGenerator, Optional
import httpx
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
    TAG_SUCCESS,
    TAG_WARN,
    TAG_ERROR,
    TAG_DOWN,
)

logger = logging.getLogger(__name__)


class TikTokScraper(BaseScraper):
    """
    Scraper profil TikTok berbasis Stealth Browser & Fast Rehydration dengan strict author filtering.
    """

    PLATFORM = "tiktok"

    def __init__(self, db_manager, session_dir: str, headed: bool = False):
        super().__init__(db_manager, session_dir)
        self.headed = headed
        self.session_dir = Path(session_dir)
        self.session_dir.mkdir(parents=True, exist_ok=True)
        self.netscape_cookie_path = self.session_dir / "tiktok_cookies.txt"
        self._playwright = None
        self._browser: Optional[Browser] = None
        self._context: Optional[BrowserContext] = None

    def _extract_username(self, url: str) -> Optional[str]:
        """Ekstrak clean username dari berbagai format URL TikTok."""
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

    async def scrape_profile(
        self,
        profile_url: str,
        forced: bool = False,
    ) -> AsyncGenerator[PostMedia, None]:
        """
        Crawl profil TikTok menggunakan Fast Rehydration & Stealth Browser.
        """
        username = self._extract_username(profile_url)
        if not username:
            logger.error(f"{TAG_ERROR} Tidak bisa ekstrak username dari URL: {profile_url}")
            return

        canonical_url = f"https://www.tiktok.com/@{username}"
        logger.info(f"{TAG_CRAWL} Memulai scraping profil @{username} (tiktok): {canonical_url}")

        collected_urls: list[str] = []
        seen_urls: set[str] = set()

        # Siapkan cookie jika ada file Netscape
        netscape_cookie_path = await self.load_and_inject_cookies(None, "tiktok")
        if netscape_cookie_path:
            self.netscape_cookie_path = netscape_cookie_path

        expected_video_count = 0

        # ── Method 1 (Primary): Fast HTTP Rehydration Parser (Guest Mode, Cepat & Akurat) ──
        logger.info(f"{TAG_CRAWL} Menjalankan Fast HTTP Rehydration Parser untuk @{username}...")
        try:
            headers = {
                "User-Agent": USER_AGENT,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": "https://www.tiktok.com/",
            }
            async with httpx.AsyncClient(
                headers=headers,
                follow_redirects=True,
                timeout=15.0,
            ) as client:
                resp = await client.get(canonical_url)
                if resp.status_code == 200:
                    rehydration_data = self._parse_rehydration_from_html(resp.text, username)
                    rehydration_urls = rehydration_data.get("urls", [])
                    expected_video_count = rehydration_data.get("videoCount", 0)

                    if expected_video_count > 0:
                        logger.info(f"{TAG_CRAWL} Profil @{username}: {expected_video_count} video publik terdeteksi.")

                    for r_url in rehydration_urls:
                        if r_url not in seen_urls:
                            collected_urls.append(r_url)
                            seen_urls.add(r_url)

                    if collected_urls:
                        logger.info(f"{TAG_CRAWL} Fast pass berhasil menemukan {len(collected_urls)} post asli @{username}.")
        except Exception as e:
            logger.debug(f"Fast HTTP Rehydration info: {e}")

        # ── Method 2: yt-dlp flat-playlist (Strict Author Matching) ──
        if not collected_urls:
            logger.info(f"{TAG_CRAWL} Mengekstrak postingan @{username} via yt-dlp...")
            try:
                cmd = [
                    "yt-dlp",
                    "--flat-playlist",
                    "--dump-json",
                    "--no-warnings",
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
                            uploader = str(entry.get("uploader") or entry.get("uploader_id") or "").lower().replace("@", "")
                            
                            # Validasi author: wajib milik target creator!
                            if uploader and uploader != username.lower():
                                continue

                            p_url = entry.get("url") or f"https://www.tiktok.com/@{username}/video/{p_id}"
                            if p_id and re.match(r"^\d{15,22}$", str(p_id)):
                                clean_p_url = p_url.split("?")[0]
                                if f"@{username.lower()}" in clean_p_url.lower() and clean_p_url not in seen_urls:
                                    collected_urls.append(clean_p_url)
                                    seen_urls.add(clean_p_url)
                        except Exception:
                            pass
            except Exception as e:
                logger.warning(f"{TAG_WARN} yt-dlp flat-playlist info: {e}")

        # ── Method 3: Stealth Browser Automation dengan Scoped Container Selector ──
        if not collected_urls or (expected_video_count > 0 and len(collected_urls) < expected_video_count):
            logger.info(f"{TAG_CRAWL} Mengaktifkan Stealth Browser Automation untuk @{username}...")
            intercepted_urls: set[str] = set()

            try:
                self._playwright = await async_playwright().start()
                self._browser, self._context = await BaseScraper.create_stealth_browser(
                    self._playwright,
                    headed=self.headed,
                )

                page = await self._context.new_page()

                # Response Interceptor dengan validasi ketat author
                async def _on_response(response):
                    try:
                        req_url = response.url.lower()
                        if "item_list" in req_url or "/api/post" in req_url or "/api/user/post" in req_url:
                            if response.status == 200:
                                data = await response.json()
                                item_list = data.get("itemList", []) or data.get("items", []) or []
                                for item in item_list:
                                    if isinstance(item, dict):
                                        author = item.get("author")
                                        author_name = ""
                                        if isinstance(author, dict):
                                            author_name = author.get("uniqueId") or author.get("unique_id") or author.get("nickname") or ""
                                        elif isinstance(author, str):
                                            author_name = author

                                        author_clean = str(author_name).lower().replace("@", "").strip()
                                        target_clean = username.lower().replace("@", "").strip()

                                        # WAJIB SAMA: hanya terima postingan yang dibuat oleh target creator
                                        if not author_clean or author_clean != target_clean:
                                            continue

                                        item_id = item.get("id") or item.get("itemId") or item.get("vid")
                                        if item_id and re.match(r"^\d{15,22}$", str(item_id)):
                                            is_photo = bool(item.get("imagePost") or item.get("images") or item.get("imageList"))
                                            t = "photo" if is_photo else "video"
                                            intercepted_urls.add(f"https://www.tiktok.com/@{username}/{t}/{item_id}")
                    except Exception:
                        pass

                page.on("response", _on_response)

                logger.info(f"{TAG_CRAWL} Membuka profil TikTok di browser stealth: {canonical_url}")
                await page.goto(canonical_url, wait_until="domcontentloaded", timeout=60000)
                await asyncio.sleep(2.5)

                # Periksa apakah terblokir Captcha
                current_url = page.url.lower()
                if "captcha" in current_url or "verify" in current_url:
                    logger.error(f"[🚨 BLOCKED] TikTok menyajikan Captcha/Verification challenge untuk @{username}!")
                else:
                    # Dismiss modal login jika ada
                    try:
                        close_btn = await page.query_selector('[data-e2e="modal-close-inner-button"], [aria-label="Close"], button[class*="close"]')
                        if close_btn:
                            await close_btn.click()
                            await asyncio.sleep(1.0)
                    except Exception:
                        pass

                    # Multi-step scroll untuk memicu pagination feed creator
                    for _ in range(6):
                        await page.evaluate("window.scrollBy(0, 1500)")
                        await asyncio.sleep(2.0)
                        if expected_video_count > 0 and len(intercepted_urls) >= expected_video_count:
                            break

                    # Ambil link video HANYA dari container grid video creator (bukan sidebar/repost!)
                    dom_links = await page.locator(
                        '[data-e2e="user-post-item"] a, [data-e2e="user-post-item-list"] a, div[data-e2e="user-post-item-desc"] a, #main-content-others_homepage a[href*="/video/"], #main-content-others_homepage a[href*="/photo/"]'
                    ).all()

                    # Fallback locator jika container khusus tidak ditemukan
                    if not dom_links:
                        dom_links = await page.locator('a[href*="/video/"], a[href*="/photo/"]').all()

                    target_user_tag = f"@{username.lower()}"
                    for a_link in dom_links:
                        try:
                            href = await a_link.get_attribute("href")
                            if href and target_user_tag in href.lower():
                                match = re.search(r"/(?:video|photo|v)/(\d{15,22})", href)
                                if match:
                                    is_photo = "/photo/" in href.lower()
                                    t = "photo" if is_photo else "video"
                                    clean_href = f"https://www.tiktok.com/@{username}/{t}/{match.group(1)}"
                                    if clean_href not in seen_urls:
                                        collected_urls.append(clean_href)
                                        seen_urls.add(clean_href)
                        except Exception:
                            pass

                # Gabungkan intercepted URLs
                for i_url in intercepted_urls:
                    if i_url not in seen_urls:
                        collected_urls.append(i_url)
                        seen_urls.add(i_url)

                await page.close()
            except Exception as e:
                logger.error(f"{TAG_ERROR} Browser automation error untuk @{username}: {e}")
            finally:
                await self.close()

        logger.info(
            f"{TAG_CRAWL} Total {len(collected_urls)} URL postingan asli @{username} berhasil dikumpulkan."
        )

        # ── Step 4: DSU Sorting (Decorate-Sort-Undecorate) ──
        decorated = [(int(self._extract_post_id(url) or 0), url) for url in collected_urls]
        decorated.sort(key=lambda x: x[0], reverse=True)  # newest-first
        video_list = [url for _, url in decorated]

        cookies_file_str = str(self.netscape_cookie_path) if self.netscape_cookie_path.exists() else None

        for post_url in video_list:
            post_id = self._extract_post_id(post_url)
            if not post_id:
                continue

            if not forced and await self.db.check_post_exists(post_id, self.PLATFORM):
                logger.info(
                    f"{TAG_CRAWL} Stop-condition: post {post_id} sudah ada di DB — resume point ditemukan."
                )
                break

            media_type = MediaType.PHOTO if "/photo/" in post_url else MediaType.VIDEO

            yield PostMedia(
                post_id=post_id,
                post_url=post_url,
                profile_url=profile_url,
                platform=self.PLATFORM,
                media_type=media_type,
                cookies_file=cookies_file_str,
            )

    def _parse_rehydration_from_html(self, html: str, username: str) -> dict:
        """Parse data rehydration JSON dari raw HTML TikTok dengan author verification ketat."""
        found_urls = set()
        video_count = 0
        sec_uid = ""
        u = username.lower().replace("@", "")

        def search_json(obj):
            nonlocal video_count, sec_uid
            if not obj or not isinstance(obj, (dict, list)):
                return

            if isinstance(obj, dict):
                if obj.get("secUid"):
                    sec_uid = str(obj["secUid"])
                if obj.get("sec_uid"):
                    sec_uid = str(obj["sec_uid"])
                if isinstance(obj.get("user"), dict) and obj["user"].get("secUid"):
                    sec_uid = str(obj["user"]["secUid"])

                # ItemModule: Map dari {postId: ItemDetail}
                item_module = obj.get("ItemModule")
                if isinstance(item_module, dict):
                    for p_id, item in item_module.items():
                        if isinstance(item, dict):
                            author = item.get("author") or item.get("authorName") or ""
                            if isinstance(author, dict):
                                author = author.get("uniqueId") or author.get("unique_id") or ""
                            author_clean = str(author).lower().replace("@", "").strip()

                            # STRICT: hanya masukkan jika author_clean terbukti milik u!
                            if author_clean == u and p_id and re.match(r"^\d{15,22}$", str(p_id)):
                                is_photo = bool(item.get("imagePost") or item.get("images") or item.get("imageList"))
                                t = "photo" if is_photo else "video"
                                found_urls.add(f"https://www.tiktok.com/@{u}/{t}/{p_id}")

                # user-detail / userPage videoList
                user_detail = obj.get("webapp.user-detail") or obj.get("user-detail") or obj.get("userPage")
                if isinstance(user_detail, dict):
                    u_items = user_detail.get("itemList") or user_detail.get("videoList")
                    if isinstance(u_items, list):
                        for it in u_items:
                            if isinstance(it, str) and re.match(r"^\d{15,22}$", it):
                                found_urls.add(f"https://www.tiktok.com/@{u}/video/{it}")
                            elif isinstance(it, dict):
                                p_id = it.get("id") or it.get("itemId") or it.get("vid")
                                if p_id and re.match(r"^\d{15,22}$", str(p_id)):
                                    is_photo = bool(it.get("imagePost") or it.get("images") or it.get("imageList"))
                                    t = "photo" if is_photo else "video"
                                    found_urls.add(f"https://www.tiktok.com/@{u}/{t}/{p_id}")

                stats = obj.get("stats")
                if isinstance(stats, dict):
                    cnt = stats.get("videoCount") or stats.get("video_count")
                    if cnt and isinstance(cnt, int):
                        video_count = max(video_count, cnt)

                if isinstance(obj.get("videoCount"), int):
                    video_count = max(video_count, obj["videoCount"])

                for val in obj.values():
                    search_json(val)

            elif isinstance(obj, list):
                for elem in obj:
                    search_json(elem)

        scripts = re.findall(r'<script[^>]*id="(?:__UNIVERSAL_DATA_FOR_REHYDRATION__|SIGI_STATE|__NEXT_DATA__)"[^>]*>(.*?)</script>', html, re.DOTALL)
        for script_content in scripts:
            try:
                data = json.loads(script_content.strip())
                search_json(data)
            except Exception:
                pass

        return {
            "urls": list(found_urls),
            "videoCount": video_count,
            "secUid": sec_uid,
        }

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
            logger.debug(f"TikTokScraper close error: {e}")
        finally:
            self._context = None
            self._browser = None
            self._playwright = None

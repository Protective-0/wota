"""
scrapers/tiktok.py
Scraper profil TikTok menggunakan Built-in Stealth Browser + Fast HTTP Rehydration & yt-dlp.

Strategi:
1. Ekstraksi cepat via yt-dlp flat-playlist (Primary).
2. Fast Pass: Ekstraksi Rehydration JSON via HTTPX (Ringan, aman tanpa AVX2 requirement).
3. Dynamic Pass: Playwright Stealth Browser dengan response interceptor dan natural scroll.
4. Auto-detection & logging jika terhadang WAF/Captcha ([🚨 BLOCKED]).
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
    Scraper profil TikTok berbasis Stealth Browser & Fast Rehydration.
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
        logger.info(f"Mulai crawl TikTok profil: {profile_url}")

        username = self._extract_username(profile_url)
        if not username:
            logger.error(f"{TAG_ERROR} Tidak bisa ekstrak username dari URL: {profile_url}")
            return

        canonical_url = f"https://www.tiktok.com/@{username}"
        collected_urls: list[str] = []
        seen_urls: set[str] = set()

        # ── Step 0: Siapkan file cookie Netscape untuk yt-dlp / browser ──
        netscape_cookie_path = await self.load_and_inject_cookies(None, "tiktok")
        if netscape_cookie_path:
            self.netscape_cookie_path = netscape_cookie_path

        cookie_dict = await self.load_cookies_as_dict("tiktok")

        # ── Method 1 (Primary): Ekstraksi cepat via yt-dlp flat-playlist ──
        logger.info(f"{TAG_CRAWL} Mengekstrak postingan @{username} via yt-dlp...")
        try:
            cmd = [
                "yt-dlp",
                "--flat-playlist",
                "--dump-json",
                "--no-warnings",
            ]
            if hasattr(self, "netscape_cookie_path") and self.netscape_cookie_path and os.path.exists(str(self.netscape_cookie_path)):
                cmd.extend(["--cookies", str(self.netscape_cookie_path)])
            elif os.path.exists(os.path.join(self.session_dir, "tiktok_cookies.txt")):
                cmd.extend(["--cookies", os.path.join(self.session_dir, "tiktok_cookies.txt")])
            cmd.append(canonical_url)

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
                        p_url = entry.get("url") or f"https://www.tiktok.com/@{username}/video/{p_id}"
                        if p_id and re.match(r"^\d{15,22}$", str(p_id)):
                            clean_p_url = p_url.split("?")[0]
                            if username.lower() in clean_p_url.lower() and clean_p_url not in seen_urls:
                                collected_urls.append(clean_p_url)
                                seen_urls.add(clean_p_url)
                    except Exception:
                        pass
        except Exception as e:
            logger.warning(f"{TAG_WARN} yt-dlp flat-playlist extraction gagal: {e} — lanjut ke Fast Rehydration...")

        expected_video_count = 0

        # ── Method 2: Fast HTTP Rehydration Parser (Ringan & Cepat) ──
        if not collected_urls:
            logger.info(f"{TAG_CRAWL} Menjalankan Fast HTTP Rehydration Parser...")
            try:
                headers = {
                    "User-Agent": USER_AGENT,
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Referer": "https://www.tiktok.com/",
                }
                async with httpx.AsyncClient(
                    headers=headers,
                    cookies=cookie_dict if cookie_dict else None,
                    follow_redirects=True,
                    timeout=15.0,
                ) as client:
                    resp = await client.get(canonical_url)
                    if resp.status_code == 200:
                        rehydration_data = self._parse_rehydration_from_html(resp.text, username)
                        rehydration_urls = rehydration_data.get("urls", [])
                        expected_video_count = rehydration_data.get("videoCount", 0)

                        if expected_video_count > 0:
                            logger.info(f"{TAG_CRAWL} Target @{username}: {expected_video_count} video publik terdeteksi.")

                        for r_url in rehydration_urls:
                            if r_url not in seen_urls:
                                collected_urls.append(r_url)
                                seen_urls.add(r_url)

                        if collected_urls:
                            logger.info(f"{TAG_CRAWL} Fast pass berhasil menemukan {len(collected_urls)} post.")
            except Exception as e:
                logger.debug(f"Fast HTTP Rehydration info: {e}")

        # ── Method 3: Browser Automation dengan Deep Stealth & Response Interceptor ──
        if not collected_urls or (expected_video_count > 0 and len(collected_urls) < expected_video_count):
            logger.info(f"{TAG_CRAWL} Mengaktifkan Stealth Browser Automation untuk @{username}...")
            intercepted_urls: set[str] = set()

            try:
                self._playwright = await async_playwright().start()
                self._browser, self._context = await BaseScraper.create_stealth_browser(
                    self._playwright,
                    headed=self.headed,
                )
                await self.load_and_inject_cookies(self._context, "tiktok")

                page = await self._context.new_page()

                # Response Interceptor untuk stream internal TikTok API
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

                                        if author_clean and author_clean != target_clean:
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

                # Periksa apakah terblokir Captcha / Slider / Verification
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

                    # Multi-step scroll untuk memicu pagination feed
                    for _ in range(5):
                        await page.evaluate("window.scrollBy(0, 1500)")
                        await asyncio.sleep(2.0)
                        if expected_video_count > 0 and len(intercepted_urls) >= expected_video_count:
                            break

                    # Ambil link video dari DOM snapshot
                    dom_links = await page.locator('a[href*="/video/"], a[href*="/photo/"]').all()
                    for a_link in dom_links:
                        try:
                            href = await a_link.get_attribute("href")
                            if href:
                                match = re.search(r"/(?:video|photo|v)/(\d{15,22})", href)
                                if match:
                                    clean_href = f"https://www.tiktok.com/@{username}/video/{match.group(1)}"
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
            f"{TAG_CRAWL} Total {len(collected_urls)} URL postingan @{username} berhasil dikumpulkan."
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
        """Parse data rehydration JSON dari raw HTML TikTok."""
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

                item_list = obj.get("itemList")
                if isinstance(item_list, list):
                    for item in item_list:
                        if isinstance(item, str) and re.match(r"^\d{15,22}$", item):
                            found_urls.add(f"https://www.tiktok.com/@{u}/video/{item}")
                        elif isinstance(item, dict):
                            p_id = item.get("id") or item.get("itemId") or item.get("vid")
                            if p_id and re.match(r"^\d{15,22}$", str(p_id)):
                                is_photo = bool(item.get("imagePost") or item.get("images") or item.get("imageList"))
                                t = "photo" if is_photo else "video"
                                found_urls.add(f"https://www.tiktok.com/@{u}/{t}/{p_id}")

                item_module = obj.get("ItemModule")
                if isinstance(item_module, dict):
                    for p_id, item in item_module.items():
                        if isinstance(item, dict):
                            author = str(item.get("author") or item.get("authorName") or "").lower().replace("@", "")
                            if not author or author == u:
                                is_photo = bool(item.get("imagePost") or item.get("images") or item.get("imageList"))
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

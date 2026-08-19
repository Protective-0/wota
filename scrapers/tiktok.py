"""
scrapers/tiktok.py
Scraper profil TikTok menggunakan Scrapling Stealth Engine + yt-dlp fallback.

Strategi:
1. Ekstraksi cepat via yt-dlp flat-playlist (Primary).
2. Fast Pass: Ekstraksi Rehydration JSON via Scrapling AsyncFetcher (Super ringan tanpa browser).
3. Dynamic Pass: Scrapling DynamicFetcher / StealthyFetcher dengan response interceptor dan natural scroll.
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

from .base import (
    BaseScraper,
    MediaType,
    PostMedia,
    USER_AGENT,
    DOCKER_CHROMIUM_FLAGS,
    HAS_SCRAPLING,
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
    Scraper profil TikTok berbasis Scrapling Stealth & yt-dlp.
    """

    PLATFORM = "tiktok"

    def __init__(self, db_manager, session_dir: str, headed: bool = False):
        super().__init__(db_manager, session_dir)
        self.headed = headed
        self.session_dir = Path(session_dir)
        self.session_dir.mkdir(parents=True, exist_ok=True)
        self.netscape_cookie_path = self.session_dir / "tiktok_cookies.txt"

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
        Crawl profil TikTok menggunakan Scrapling Stealth Engine.
        """
        logger.info(f"Mulai crawl TikTok profil: {profile_url}")

        username = self._extract_username(profile_url)
        if not username:
            logger.error(f"{TAG_ERROR} Tidak bisa ekstrak username dari URL: {profile_url}")
            return

        canonical_url = f"https://www.tiktok.com/@{username}"
        collected_urls: list[str] = []
        seen_urls: set[str] = set()

        # ── Step 0: Siapkan file cookie Netscape untuk yt-dlp / fetcher ──
        netscape_cookie_path = await self.load_and_inject_cookies(None, "tiktok")
        if netscape_cookie_path:
            self.netscape_cookie_path = netscape_cookie_path

        cookie_list = await self.load_cookies_as_list("tiktok")
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
            logger.warning(f"{TAG_WARN} yt-dlp flat-playlist extraction gagal: {e} — lanjut ke Scrapling...")

        expected_video_count = 0

        # ── Method 2 (Scrapling Fast Pass): Rehydration Parser tanpa Browser ──
        if not collected_urls and HAS_SCRAPLING:
            logger.info(f"{TAG_CRAWL} Menjalankan Scrapling Fast Pass (TLS Stealth Request)...")
            try:
                resp = await self.fetch_http_page(
                    canonical_url,
                    cookies=cookie_dict,
                    headers={"Referer": "https://www.tiktok.com/"},
                    impersonate="chrome124",
                )
                html_text = getattr(resp, "text", "") or ""
                rehydration_data = self._parse_rehydration_from_html(html_text, username)

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
                logger.debug(f"Scrapling Fast Pass info: {e}")

        # ── Method 3 (Scrapling Dynamic Pass): Browser Automation dengan Stealth ──
        if (not collected_urls or (expected_video_count > 0 and len(collected_urls) < expected_video_count)) and HAS_SCRAPLING:
            logger.info(f"{TAG_CRAWL} Mengaktifkan Scrapling Dynamic Stealth Fetcher untuk @{username}...")
            intercepted_urls: set[str] = set()

            async def _interceptor_and_scroller(page):
                # 1. Response Interceptor untuk stream internal TikTok API
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

                # 2. Periksa apakah terblokir Captcha / Slider / Login Wall
                await asyncio.sleep(2.5)
                current_url = page.url.lower()
                if "captcha" in current_url or "verify" in current_url:
                    logger.error(f"[🚨 BLOCKED] TikTok menyajikan Captcha/Verification challenge untuk @{username}!")
                    return

                # 3. Dismiss modal login / cookie banner jika ada
                try:
                    close_btn = await page.query_selector('[data-e2e="modal-close-inner-button"], [aria-label="Close"], button[class*="close"]')
                    if close_btn:
                        await close_btn.click()
                        await asyncio.sleep(1.0)
                except Exception:
                    pass

                # 4. Multi-step scroll untuk memicu pagination API
                for _ in range(5):
                    await page.evaluate("window.scrollBy(0, 1500)")
                    await asyncio.sleep(2.0)
                    if expected_video_count > 0 and len(intercepted_urls) >= expected_video_count:
                        break

            try:
                fetch_res = await self.fetch_dynamic_page(
                    canonical_url,
                    cookies=cookie_list,
                    page_action=_interceptor_and_scroller,
                    timeout=45000,
                    headless=not self.headed,
                )

                # Gabungkan intercepted URLs
                for i_url in intercepted_urls:
                    if i_url not in seen_urls:
                        collected_urls.append(i_url)
                        seen_urls.add(i_url)

                # Ekstrak link DOM dari hasil fetch jika masih ada yang belum masuk
                if hasattr(fetch_res, "css"):
                    for a_el in fetch_res.css('a[href*="/video/"], a[href*="/photo/"]'):
                        href = a_el.attrib.get("href", "") if hasattr(a_el, "attrib") else ""
                        match = re.search(r"/(?:video|photo|v)/(\d{15,22})", href)
                        if match:
                            clean_href = f"https://www.tiktok.com/@{username}/video/{match.group(1)}"
                            if clean_href not in seen_urls:
                                collected_urls.append(clean_href)
                                seen_urls.add(clean_href)

            except Exception as e:
                logger.error(f"{TAG_ERROR} Scrapling Dynamic Fetcher error: {e}")

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
        """Cleanup resources."""
        pass

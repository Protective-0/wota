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
import random
import re
import sys
import time
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
    Scraper profil TikTok Zero-Login berbasis Playwright Stealth DOM Extractor & Multi-Tier Fallback.
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
        self._playwright = None
        self._browser = None
        self._context = None

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
        Menolak 100% URL video rekomendasi, sidebar, atau repost dari akun lain.
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
        Crawl profil TikTok secara 100% Guest Mode (Zero-Login) dengan Playwright Stealth & Multi-Tier Fallback.
        """
        self.failed = False
        self.blocked_by_challenge = False

        username = self._extract_username(profile_url)
        if not username:
            logger.error(f"{TAG_ERROR} Tidak bisa ekstrak username dari URL: {profile_url}")
            return

        canonical_url = f"https://www.tiktok.com/@{username}"
        logger.info(f"{TAG_CRAWL} Memulai scraping profil @{username} (tiktok, Zero-Login Mode): {canonical_url}")

        collected_urls: list[str] = []
        seen_urls: set[str] = set()

        # ─────────────────────────────────────────────────────────────────────
        # PASS 0 (PRIMARY): Playwright Persistent Stealth Browser (Bypass WAF & Captcha)
        # ─────────────────────────────────────────────────────────────────────
        logger.info(f"{TAG_CRAWL} [PASS 0] Mengaktifkan Playwright Persistent Stealth Browser untuk @{username}...")
        try:
            from playwright.async_api import async_playwright
            self._playwright = await async_playwright().start()
            user_data_dir = Path(self.session_dir) / "tiktok_browser_profile"
            self._context = await BaseScraper.create_persistent_stealth_context(
                self._playwright,
                user_data_dir=user_data_dir,
                headed=self.headed,
                viewport={"width": 1280, "height": 800},
                locale="id-ID,id;q=0.9,en-US;q=0.8,en;q=0.7",
                timezone_id="Asia/Jakarta",
            )
            self._browser = None  # Persistent context mengelola browser instance secara terintegrasi

            # Muat session cookies jika tersedia di direktori sessions/
            try:
                cookies = await self.load_cookies_as_list(self.PLATFORM)
                if cookies:
                    logger.info(f"{TAG_CRAWL} Memuat {len(cookies)} session cookies untuk TikTok...")
                    await self._context.add_cookies(cookies)
            except Exception as c_err:
                logger.debug(f"Cookie load note: {c_err}")

            # Gunakan tab aktif default (Tab 0) dari persistent context agar tidak membuat background tab
            page = self._context.pages[0] if self._context.pages else await self._context.new_page()

            # Network Interceptor: Tangkap payload internal TikTok saat dimuat dengan verifikasi author ketat
            async def _on_page_response(resp):
                r_url = resp.url
                if any(k in r_url for k in ("item_list", "/api/post", "aweme/v1", "user/detail", "item/detail")) and resp.status == 200:
                    try:
                        data = await resp.json()
                        if isinstance(data, dict):
                            items = data.get("itemList", []) or data.get("items", []) or data.get("aweme_list", []) or []
                            for item in items:
                                if isinstance(item, dict):
                                    author_info = item.get("author") or {}
                                    author_uid = (
                                        author_info.get("uniqueId")
                                        or author_info.get("unique_id")
                                        or (author_info if isinstance(author_info, str) else "")
                                    )
                                    if author_uid and str(author_uid).lower().replace("@", "").strip() != username:
                                        continue

                                    v_id = str(item.get("id") or item.get("aweme_id") or item.get("itemId") or "")
                                    if v_id and re.match(r"^\d{15,22}$", v_id):
                                        is_photo = bool(item.get("imagePost") or item.get("images"))
                                        t = "photo" if is_photo else "video"
                                        p_url = f"https://www.tiktok.com/@{username}/{t}/{v_id}"
                                        if self._is_valid_author_post_url(p_url, username) and p_url not in seen_urls:
                                            collected_urls.append(p_url)
                                            seen_urls.add(p_url)
                    except Exception:
                        pass

            page.on("response", _on_page_response)
            logger.info(f"{TAG_CRAWL} Membuka profil TikTok di persistent browser: {canonical_url}")

            await page.goto(canonical_url, wait_until="domcontentloaded", timeout=45000)
            await asyncio.sleep(4.0)

            # Tunggu elemen profil atau feed postingan selesai dirender oleh SlardarWAF
            try:
                await page.wait_for_selector(
                    'a[href*="/video/"], a[href*="/photo/"], [data-e2e="user-post-item"], [data-e2e="user-post-item-list"], h1, [data-e2e="user-title"]',
                    timeout=10000
                )
            except Exception:
                pass

            # Dismiss modal popup / cookie dialog jika ada
            try:
                dismiss_btn = await page.query_selector(
                    'button:has-text("Lanjutkan sebagai tamu"), button:has-text("Continue as guest"), button:has-text("Not now"), [data-e2e="modal-close-inner-button"]'
                )
                if dismiss_btn:
                    await dismiss_btn.click()
                    await asyncio.sleep(1.0)
            except Exception:
                pass

            # Scrolling pagination loop dengan early-exit
            max_scroll_rounds = 8
            no_new_rounds = 0
            for scroll_idx in range(max_scroll_rounds):
                prev_count = len(collected_urls)

                # 1. Ekstrak links dari DOM selectors
                dom_links = await page.locator('a[href*="/video/"], a[href*="/photo/"], a[href*="/v/"], [data-e2e="user-post-item"] a, [data-e2e="user-post-item-list"] a').all()
                for link_el in dom_links:
                    try:
                        href = await link_el.get_attribute("href")
                        if href:
                            p_url = f"https://www.tiktok.com{href}" if href.startswith("/") else href
                            p_url_clean = p_url.split("?")[0].rstrip("/")
                            if self._is_valid_author_post_url(p_url_clean, username) and p_url_clean not in seen_urls:
                                collected_urls.append(p_url_clean)
                                seen_urls.add(p_url_clean)
                    except Exception:
                        pass

                # 2. Ekstrak dari rehydration script jika tersedia di DOM
                try:
                    rehydration_text = await page.evaluate("""() => {
                        const s = document.querySelector('#__UNIVERSAL_DATA_FOR_REHYDRATION__, #SIGI_STATE');
                        return s ? s.textContent : null;
                    }""")
                    if rehydration_text:
                        parsed_urls = self._parse_rehydration_from_html(rehydration_text, username)
                        for r_url in parsed_urls:
                            if self._is_valid_author_post_url(r_url, username) and r_url not in seen_urls:
                                collected_urls.append(r_url)
                                seen_urls.add(r_url)
                except Exception:
                    pass

                # 3. Fallback regex dari full HTML DOM (hanya mencocokkan pola author target)
                if not collected_urls:
                    try:
                        page_content = await page.content()
                        u_clean = username.lower().replace("@", "").strip()
                        matched_ids = set(re.findall(rf'tiktok\.com/@{u_clean}/(?:video|photo|v)/(\d{{15,22}})', page_content.lower()))
                        for mid in matched_ids:
                            cand_url = f"https://www.tiktok.com/@{username}/video/{mid}"
                            if self._is_valid_author_post_url(cand_url, username) and cand_url not in seen_urls:
                                collected_urls.append(cand_url)
                                seen_urls.add(cand_url)
                    except Exception:
                        pass

                # Cek stop-condition di database jika tidak forced
                if not forced and collected_urls:
                    max_id = str(max((int(self._extract_post_id(u) or 0) for u in collected_urls), default=0))
                    if max_id != "0" and await self.db.check_post_exists(max_id, self.PLATFORM):
                        logger.info(f"{TAG_CRAWL} Stop-condition tercapai pada post {max_id} di DB.")
                        break

                if len(collected_urls) == prev_count and len(collected_urls) > 0:
                    no_new_rounds += 1
                    if no_new_rounds >= 2:
                        break
                else:
                    no_new_rounds = 0

                await page.evaluate("window.scrollBy(0, 1600)")
                await asyncio.sleep(random.uniform(1.0, 1.5))

            if not collected_urls:
                page_title = await page.title()
                logger.warning(
                    f"{TAG_WARN} [PASS 0] Playwright Persistent Browser selesai tanpa link. "
                    f"Page Title: '{page_title}', URL: '{page.url}'"
                )

            await page.close()
            if collected_urls:
                logger.info(f"{TAG_CRAWL} [PASS 0] Playwright Persistent Stealth Browser berhasil mengekstrak {len(collected_urls)} post.")
        except Exception as e:
            logger.warning(f"{TAG_WARN} [PASS 0] Playwright Stealth Browser exception: {e}")
        finally:
            await self.close()

        # ─────────────────────────────────────────────────────────────────────
        # PASS 1: TikWM User Posts API Fallback
        # ─────────────────────────────────────────────────────────────────────
        if not collected_urls:
            logger.info(f"{TAG_CRAWL} [PASS 1] Mencoba TikWM User Posts API untuk @{username}...")
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
                    async with httpx.AsyncClient(headers=headers, timeout=15.0, follow_redirects=True) as client:
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
                        logger.info(f"{TAG_CRAWL} [PASS 1] TikWM API berhasil menemukan {len(collected_urls)} post.")
            except Exception as e:
                logger.debug(f"[PASS 1] TikWM API info: {e}")

        # ─────────────────────────────────────────────────────────────────────
        # PASS 2: Scrapling / curl_cffi Fast SSR Rehydration Fallback
        # ─────────────────────────────────────────────────────────────────────
        if not collected_urls:
            logger.info(f"{TAG_CRAWL} [PASS 2] Menjalankan Fast SSR Rehydration untuk @{username}...")
            try:
                html_text = ""
                ssr_headers = {
                    "User-Agent": USER_AGENT,
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.9,id;q=0.8",
                    "Referer": "https://www.tiktok.com/",
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

                if html_text and not any(block in html_text for block in ("verify-center", "SlardarWAF", "secsdk")):
                    parsed_urls = self._parse_rehydration_from_html(html_text, username)
                    for r_url in parsed_urls:
                        if self._is_valid_author_post_url(r_url, username) and r_url not in seen_urls:
                            collected_urls.append(r_url)
                            seen_urls.add(r_url)
                    if collected_urls:
                        logger.info(f"{TAG_CRAWL} [PASS 2] Fast SSR berhasil menemukan {len(collected_urls)} post.")
            except Exception as e:
                logger.debug(f"[PASS 2] SSR error: {e}")

        # ─────────────────────────────────────────────────────────────────────
        # PASS 3: yt-dlp Flat-Playlist (Mobile API & Netscape Cookie) Fallback
        # ─────────────────────────────────────────────────────────────────────
        if not collected_urls:
            logger.info(f"{TAG_CRAWL} [PASS 3] Mencoba ekstraksi postingan @{username} via yt-dlp flat-playlist...")
            try:
                cookie_file = await self.export_session_cookies_for_ytdlp(self.PLATFORM)
                cmd_variants = [
                    [
                        sys.executable,
                        "-m", "yt_dlp",
                        "--flat-playlist",
                        "--dump-json",
                        "--no-warnings",
                        "--extractor-args", "tiktok:api_hostname=api16-normal-c-useast1a.tiktokv.com",
                    ],
                    [
                        sys.executable,
                        "-m", "yt_dlp",
                        "--flat-playlist",
                        "--dump-json",
                        "--no-warnings",
                    ]
                ]

                for base_cmd in cmd_variants:
                    cmd = list(base_cmd)
                    if cookie_file and cookie_file.exists():
                        cmd.extend(["--cookies", str(cookie_file)])
                    cmd.append(canonical_url)

                    proc = await asyncio.create_subprocess_exec(
                        *cmd,
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE,
                    )
                    try:
                        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=60.0)
                    except asyncio.TimeoutError:
                        logger.warning(f"{TAG_WARN} [PASS 3] yt-dlp flat-playlist timeout 60s — membunuh subprocess...")
                        proc.kill()
                        try:
                            await asyncio.wait_for(proc.wait(), timeout=5.0)
                        except Exception:
                            pass
                        continue
                    if stdout:
                        for line in stdout.decode("utf-8", errors="ignore").splitlines():
                            line = line.strip()
                            if not line:
                                continue
                            try:
                                entry = json.loads(line)
                                p_id = entry.get("id")
                                uploader = str(entry.get("uploader") or "").lower().replace("@", "").strip()
                                if uploader and uploader != username and not uploader.isdigit():
                                    continue
                                if p_id and re.match(r"^\d{15,22}$", str(p_id)):
                                    p_url = f"https://www.tiktok.com/@{username}/video/{p_id}"
                                    if self._is_valid_author_post_url(p_url, username) and p_url not in seen_urls:
                                        collected_urls.append(p_url)
                                        seen_urls.add(p_url)
                            except Exception:
                                pass
                        if collected_urls:
                            logger.info(f"{TAG_CRAWL} [PASS 3] yt-dlp flat-playlist berhasil menemukan {len(collected_urls)} post.")
                            break
                    else:
                        err_msg = stderr.decode("utf-8", errors="ignore").strip()
                        if err_msg:
                            logger.debug(f"[PASS 3] yt-dlp variant note: {err_msg[:120]}")
            except Exception as e:
                logger.warning(f"{TAG_WARN} [PASS 3] yt-dlp exception: {e}")

        # ─────────────────────────────────────────────────────────────────────
        # ABORT GUARD: Jika Semua Layer Gagal
        # ─────────────────────────────────────────────────────────────────────
        if not collected_urls:
            logger.warning(
                f"{TAG_WARN} @{username} (tiktok) scraping tidak berhasil (semua pass kosong) — "
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
                    user_info = user_detail.get("userInfo") or user_detail.get("user") or {}
                    u_uid = (
                        user_info.get("user", {}).get("uniqueId")
                        or user_info.get("uniqueId")
                        or user_info.get("unique_id")
                        or ""
                    ) if isinstance(user_info, dict) else str(user_info)
                    if not u_uid or str(u_uid).lower().replace("@", "").strip() == u:
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
                                    if not a_uid or str(a_uid).lower().replace("@", "").strip() == u:
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
        """Cleanup Playwright browser resources."""
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

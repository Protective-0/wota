"""
scrapers/tiktok.py
Engine Scraper Profil TikTok 100% Guest Mode (Zero-Login) dengan Multi-Layer API-First & Mobile Bypass.

Fitur & Keamanan:
1. Multi-Tier Zero-Login Architecture:
   - Pass 0: External API Profile Extractor (TikWM / Mirror Public Endpoint tanpa browser).
   - Pass 1: Scrapling / curl_cffi Fast SSR Rehydration (Desktop & Mobile) dengan TLS & Sec-Ch-Ua Impersonation.
   - Pass 2: Direct TikTok Web / Mobile API query (item_list) dengan TLS Impersonation (chrome124).
   - Pass 3: yt-dlp Flat-Playlist dengan mobile extractor args.
   - Pass 4: Playwright Desktop Stealth Browser dengan Network Response Interceptor.
   - Pass 5: Playwright Mobile Device Emulation (iPhone / Mobile Safari) jika desktop terhalang Captcha.
2. Anti-False-Completion (Captcha Safety Guard):
   - Jika scraper terhalang Captcha/WAF Challenge dan 0 postingan terkumpul, bot menandai `blocked_by_challenge = True`
     sehingga patrol/historical scrape tidak salah menandai akun sebagai "kosong".
3. Strict Author Verification:
   - Validasi ketat `author.uniqueId == target_username` agar postingan Repost, Likes, maupun Feed Recommendation tidak ikut terambil.
4. Chronological DSU Sorting:
   - Stop-condition evaluation pada post terbaru, lalu dispatch ke downstream secara kronologis (oldest-first).
"""

import asyncio
import json
import logging
import os
import re
from pathlib import Path
from typing import AsyncGenerator, Optional, Any

# Scrapling Engine imports dengan fallback aman
try:
    from scrapling.fetchers import AsyncFetcher
    HAS_SCRAPLING = True
except ImportError:
    HAS_SCRAPLING = False

try:
    from curl_cffi import requests as curl_requests
    HAS_CURL_CFFI = True
except ImportError:
    HAS_CURL_CFFI = False

import httpx

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
    Scraper profil TikTok 100% Guest Mode (Zero-Login) berbasis Multi-Tier API, Scrapling & Mobile Fallback.
    """

    PLATFORM = "tiktok"

    def __init__(self, db_manager, session_dir: str, headed: bool = False):
        super().__init__(db_manager, session_dir)
        self.headed = headed
        self.session_dir = Path(session_dir)
        self.session_dir.mkdir(parents=True, exist_ok=True)
        self.blocked_by_challenge = False
        self._playwright = None
        self._browser = None
        self._context = None

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

    def _is_valid_creator_post(self, item: dict, target_username: str) -> bool:
        """
        [STRICT AUTHOR FILTERING]
        1. Validasi author.uniqueId / unique_id == target_username
        2. Tolak jika postingan berasal dari tab Repost / isRepost == True
        3. Tolak jika author tidak match (mencegah kebocoran likes / recommended sidebar)
        """
        if not item or not isinstance(item, dict):
            return False

        # Filter out Repost flag
        if item.get("isRepost") or item.get("repost") or item.get("is_repost"):
            return False

        # Ekstrak identifier author dari berbagai variasi skema JSON TikTok
        author = item.get("author")
        author_name = ""
        if isinstance(author, dict):
            author_name = author.get("uniqueId") or author.get("unique_id") or author.get("nickname") or ""
        elif isinstance(author, str):
            author_name = author

        author_clean = str(author_name).lower().replace("@", "").strip()
        target_clean = target_username.lower().replace("@", "").strip()

        # WAJIB SAMA: hanya terima postingan yang dibuat langsung oleh creator target
        return bool(author_clean and author_clean == target_clean)

    async def scrape_profile(
        self,
        profile_url: str,
        forced: bool = False,
    ) -> AsyncGenerator[PostMedia, None]:
        """
        Crawl profil TikTok secara 100% Guest Mode (Zero-Login) dengan multi-tier fallback.
        """
        self.blocked_by_challenge = False
        username = self._extract_username(profile_url)
        if not username:
            logger.error(f"{TAG_ERROR} Tidak bisa ekstrak username dari URL: {profile_url}")
            return

        canonical_url = f"https://www.tiktok.com/@{username}"
        mobile_url = f"https://m.tiktok.com/@{username}"
        logger.info(f"{TAG_CRAWL} Memulai scraping profil @{username} (tiktok, Zero-Login Mode): {canonical_url}")

        collected_urls: list[str] = []
        seen_urls: set[str] = set()
        expected_video_count = 0
        sec_uid = ""
        challenge_hit = False

        # ─────────────────────────────────────────────────────────────────────
        # PASS 0 (API-FIRST): External User Posts API (TikWM & Mirror Extractor)
        # Menghindari overhead browser jika feed profil publik tersedia via API
        # ─────────────────────────────────────────────────────────────────────
        logger.info(f"{TAG_CRAWL} [PASS 0] Mencoba API-first profile feed extractor untuk @{username}...")
        try:
            tw_api_urls = [
                f"https://www.tikwm.com/api/user/posts?unique_id={username}&count=35",
                f"https://tikwm.com/api/user/posts?unique_id={username}&count=35",
            ]
            tw_headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                "Referer": "https://www.tikwm.com/",
                "Accept": "application/json, text/plain, */*",
            }
            for tw_url in tw_api_urls:
                if collected_urls:
                    break
                try:
                    async with httpx.AsyncClient(timeout=12.0, headers=tw_headers, follow_redirects=True) as tw_client:
                        tw_resp = await tw_client.get(tw_url)
                        if tw_resp.status_code == 200:
                            tw_json = tw_resp.json()
                            if tw_json.get("code") == 0 and tw_json.get("data"):
                                data_obj = tw_json["data"]
                                video_list = data_obj.get("videos", []) or data_obj.get("itemList", []) or []
                                for vid_item in video_list:
                                    if self._is_valid_creator_post(vid_item, username):
                                        vid_id = vid_item.get("video_id") or vid_item.get("id") or vid_item.get("itemId")
                                        if vid_id and re.match(r"^\d{15,22}$", str(vid_id)):
                                            is_photo = bool(vid_item.get("images") or vid_item.get("imagePost"))
                                            t = "photo" if is_photo else "video"
                                            c_url = f"https://www.tiktok.com/@{username}/{t}/{vid_id}"
                                            if c_url not in seen_urls:
                                                collected_urls.append(c_url)
                                                seen_urls.add(c_url)
                                if collected_urls:
                                    logger.info(f"{TAG_CRAWL} [PASS 0] API Extractor berhasil menemukan {len(collected_urls)} post asli @{username}.")
                except Exception as tw_err:
                    logger.debug(f"Pass 0 TikWM query note: {tw_err}")
        except Exception as p0_err:
            logger.debug(f"Pass 0 outer note: {p0_err}")

        # ─────────────────────────────────────────────────────────────────────
        # PASS 1 (PRIMARY): Scrapling / curl_cffi Fast SSR Rehydration Parser
        # Dual-Endpoint: Desktop & Mobile dengan TLS & Sec-Ch-Ua Impersonation
        # ─────────────────────────────────────────────────────────────────────
        if not collected_urls:
            logger.info(f"{TAG_CRAWL} [PASS 1] Menjalankan Scrapling Fast SSR Rehydration untuk @{username} (Zero-Login)...")
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                "Accept-Language": "en-US,en;q=0.9,id;q=0.8",
                "Referer": "https://www.tiktok.com/",
                "Sec-Ch-Ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
                "Sec-Ch-Ua-Mobile": "?0",
                "Sec-Ch-Ua-Platform": '"Windows"',
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "none",
                "Sec-Fetch-User": "?1",
                "Upgrade-Insecure-Requests": "1",
            }

            target_endpoints = [canonical_url, mobile_url]

            for target_url in target_endpoints:
                if collected_urls:
                    break
                try:
                    html_text = ""
                    if HAS_CURL_CFFI:
                        try:
                            async with curl_requests.AsyncSession(impersonate="chrome124") as session:
                                resp = await session.get(target_url, headers=headers, timeout=12)
                                if resp.status_code == 200:
                                    html_text = resp.text
                                    if "Something went wrong" in html_text or "verify-center" in html_text or "SlardarWAF" in html_text or "secsdk" in html_text:
                                        challenge_hit = True
                                        html_text = ""
                        except Exception as c_err:
                            logger.debug(f"curl_cffi SSR note ({target_url}): {c_err}")

                    if not html_text and HAS_SCRAPLING:
                        try:
                            response = await AsyncFetcher.get(
                                target_url,
                                headers=headers,
                                timeout=12,
                                impersonate="chrome124",
                            )
                            if response.status == 200:
                                html_text = response.text
                                if "Something went wrong" in html_text or "verify-center" in html_text or "SlardarWAF" in html_text or "secsdk" in html_text:
                                    challenge_hit = True
                                    html_text = ""
                        except Exception as sc_err:
                            logger.debug(f"Scrapling AsyncFetcher note ({target_url}): {sc_err}")

                    if not html_text:
                        async with httpx.AsyncClient(
                            headers=headers,
                            follow_redirects=True,
                            timeout=12.0,
                        ) as client:
                            resp = await client.get(target_url)
                            if resp.status_code == 200:
                                html_text = resp.text
                                if "Something went wrong" in html_text or "verify-center" in html_text or "SlardarWAF" in html_text:
                                    challenge_hit = True
                                    html_text = ""

                    if html_text:
                        rehydration_data = self._parse_rehydration_from_html(html_text, username)
                        rehydration_urls = rehydration_data.get("urls", [])
                        expected_video_count = rehydration_data.get("videoCount", 0)
                        sec_uid = rehydration_data.get("secUid", "")

                        if expected_video_count > 0:
                            logger.info(f"{TAG_CRAWL} Profil @{username}: {expected_video_count} postingan terdeteksi pada metadata.")

                        for r_url in rehydration_urls:
                            if r_url not in seen_urls:
                                collected_urls.append(r_url)
                                seen_urls.add(r_url)

                        if collected_urls:
                            logger.info(f"{TAG_CRAWL} [PASS 1] Fast Pass ({target_url}) berhasil menemukan {len(collected_urls)} post asli @{username}.")
                except Exception as e:
                    logger.debug(f"Fast HTTP Rehydration info ({target_url}): {e}")

        # ─────────────────────────────────────────────────────────────────────
        # PASS 2: TikTok Direct Web / Mobile API item_list via curl_cffi
        # ─────────────────────────────────────────────────────────────────────
        if not collected_urls and sec_uid:
            logger.info(f"{TAG_CRAWL} [PASS 2] Menjalankan TikTok Mobile API item_list untuk @{username} (secUid: {sec_uid[:15]}...)...")
            try:
                api_urls = [
                    f"https://www.tiktok.com/api/post/item_list/?count=35&secUid={sec_uid}",
                    f"https://m.tiktok.com/api/post/item_list/?count=35&secUid={sec_uid}",
                ]
                for api_url in api_urls:
                    if collected_urls:
                        break
                    if HAS_CURL_CFFI:
                        async with curl_requests.AsyncSession(impersonate="chrome124") as session:
                            a_resp = await session.get(api_url, headers=headers, timeout=12)
                            if a_resp.status_code == 200:
                                a_data = a_resp.json()
                                items = a_data.get("itemList", []) or a_data.get("items", []) or []
                                for item in items:
                                    if self._is_valid_creator_post(item, username):
                                        p_id = item.get("id") or item.get("itemId")
                                        if p_id and re.match(r"^\d{15,22}$", str(p_id)):
                                            is_photo = bool(item.get("imagePost") or item.get("images"))
                                            t = "photo" if is_photo else "video"
                                            c_url = f"https://www.tiktok.com/@{username}/{t}/{p_id}"
                                            if c_url not in seen_urls:
                                                collected_urls.append(c_url)
                                                seen_urls.add(c_url)
                                if collected_urls:
                                    logger.info(f"{TAG_CRAWL} [PASS 2] TikTok API berhasil menemukan {len(collected_urls)} post @{username}.")
            except Exception as api_err:
                logger.debug(f"TikTok Direct API note: {api_err}")

        # ─────────────────────────────────────────────────────────────────────
        # PASS 3: yt-dlp Flat-Playlist Extractor (Mobile API Mode)
        # ─────────────────────────────────────────────────────────────────────
        if not collected_urls:
            logger.info(f"{TAG_CRAWL} [PASS 3] Mencoba ekstraksi postingan @{username} via yt-dlp flat-playlist (Zero-Login)...")
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
                try:
                    stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=30.0)
                    if proc.returncode == 0 and stdout:
                        for line in stdout.decode("utf-8", errors="ignore").splitlines():
                            line = line.strip()
                            if not line:
                                continue
                            try:
                                entry = json.loads(line)
                                p_id = entry.get("id")
                                uploader = str(entry.get("uploader") or entry.get("uploader_id") or "").lower().replace("@", "").strip()
                                
                                # STRICT AUTHOR MATCH: buang uploader yang tidak cocok
                                if uploader and uploader != username.lower().strip():
                                    continue

                                p_url = entry.get("url") or f"https://www.tiktok.com/@{username}/video/{p_id}"
                                if p_id and re.match(r"^\d{15,22}$", str(p_id)):
                                    clean_p_url = p_url.split("?")[0]
                                    if f"@{username.lower()}" in clean_p_url.lower() and clean_p_url not in seen_urls:
                                        collected_urls.append(clean_p_url)
                                        seen_urls.add(clean_p_url)
                            except Exception:
                                pass
                except asyncio.TimeoutError:
                    proc.kill()
                    await proc.wait()
                    logger.warning(f"{TAG_WARN} yt-dlp flat-playlist timeout dibatalkan.")
            except Exception as e:
                logger.warning(f"{TAG_WARN} yt-dlp flat-playlist info: {e}")

        # ─────────────────────────────────────────────────────────────────────
        # PASS 4: Playwright Dynamic Stealth Browser (Desktop Mode)
        # ─────────────────────────────────────────────────────────────────────
        browser_captcha_hit = False
        if not collected_urls or (expected_video_count > 0 and len(collected_urls) < expected_video_count):
            logger.info(f"{TAG_CRAWL} [PASS 4] Mengaktifkan Playwright Dynamic Stealth Browser untuk @{username} (Desktop)...")
            intercepted_urls: set[str] = set()

            try:
                from playwright.async_api import async_playwright
                self._playwright = await async_playwright().start()
                self._browser, self._context = await BaseScraper.create_stealth_browser(
                    self._playwright,
                    headed=self.headed,
                )

                page = await self._context.new_page()

                async def _on_response(response):
                    try:
                        req_url = response.url.lower()
                        if "item_list" in req_url or "/api/post" in req_url or "/api/user/post" in req_url or "preload/item_list" in req_url:
                            if response.status == 200:
                                data = await response.json()
                                item_list = data.get("itemList", []) or data.get("items", []) or []
                                for item in item_list:
                                    if self._is_valid_creator_post(item, username):
                                        item_id = item.get("id") or item.get("itemId") or item.get("vid")
                                        if item_id and re.match(r"^\d{15,22}$", str(item_id)):
                                            is_photo = bool(item.get("imagePost") or item.get("images") or item.get("imageList"))
                                            t = "photo" if is_photo else "video"
                                            intercepted_urls.add(f"https://www.tiktok.com/@{username}/{t}/{item_id}")
                    except Exception:
                        pass

                page.on("response", _on_response)

                logger.info(f"{TAG_CRAWL} Membuka profil TikTok di browser stealth: {canonical_url}")
                await page.goto(canonical_url, wait_until="domcontentloaded", timeout=45000)
                await asyncio.sleep(2.5)

                # Cek login wall & Captcha verification challenge
                current_url = page.url.lower()
                page_content = await page.content()
                if "captcha" in current_url or "verify" in current_url or "secsdk" in page_content or "SlardarWAF" in page_content:
                    browser_captcha_hit = True
                    challenge_hit = True
                    logger.warning(f"{TAG_WARN} TikTok Desktop menyajikan Captcha challenge untuk @{username}.")
                else:
                    try:
                        close_btn = await page.query_selector(
                            '[data-e2e="modal-close-inner-button"], [aria-label="Close"], button[class*="close"]'
                        )
                        if close_btn:
                            await close_btn.click()
                            await asyncio.sleep(1.0)
                    except Exception:
                        pass

                    for _ in range(6):
                        await page.evaluate("window.scrollBy(0, 1500)")
                        await asyncio.sleep(2.0)
                        if expected_video_count > 0 and len(intercepted_urls) >= expected_video_count:
                            break

                    dom_links = await page.locator(
                        '[data-e2e="user-post-item"] a, '
                        '[data-e2e="user-post-item-list"] [data-e2e="user-post-item"] a, '
                        '#main-content-others_homepage [data-e2e="user-post-item"] a'
                    ).all()

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

                for i_url in intercepted_urls:
                    if i_url not in seen_urls:
                        collected_urls.append(i_url)
                        seen_urls.add(i_url)

                await page.close()
            except Exception as e:
                logger.error(f"{TAG_ERROR} Desktop Browser automation error untuk @{username}: {e}")
            finally:
                await self.close()

        # ─────────────────────────────────────────────────────────────────────
        # PASS 5: Playwright Mobile Device Emulation Fallback (iPhone/Safari)
        # ─────────────────────────────────────────────────────────────────────
        if (not collected_urls and browser_captcha_hit) or (not collected_urls and challenge_hit):
            logger.info(f"{TAG_CRAWL} [PASS 5] Menjalankan Playwright Mobile Device Emulation (iPhone/Safari) untuk @{username}...")
            m_intercepted: set[str] = set()
            try:
                from playwright.async_api import async_playwright
                self._playwright = await async_playwright().start()
                iphone_device = self._playwright.devices.get("iPhone 14 Pro Max") or self._playwright.devices.get("iPhone 13")
                
                launch_kwargs = BaseScraper.get_browser_launch_kwargs()
                self._browser = await self._playwright.chromium.launch(
                    headless=not self.headed,
                    **launch_kwargs,
                )
                if iphone_device:
                    self._context = await self._browser.new_context(
                        **iphone_device,
                        locale="id-ID",
                    )
                else:
                    self._context = await self._browser.new_context(
                        user_agent="Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
                        viewport={"width": 430, "height": 932},
                        is_mobile=True,
                        has_touch=True,
                        locale="id-ID",
                    )

                m_page = await self._context.new_page()

                async def _on_mobile_resp(response):
                    try:
                        req_url = response.url.lower()
                        if "item_list" in req_url or "/api/post" in req_url or "/api/user/post" in req_url or "preload/item_list" in req_url:
                            if response.status == 200:
                                data = await response.json()
                                item_list = data.get("itemList", []) or data.get("items", []) or []
                                for item in item_list:
                                    if self._is_valid_creator_post(item, username):
                                        item_id = item.get("id") or item.get("itemId") or item.get("vid")
                                        if item_id and re.match(r"^\d{15,22}$", str(item_id)):
                                            is_photo = bool(item.get("imagePost") or item.get("images") or item.get("imageList"))
                                            t = "photo" if is_photo else "video"
                                            m_intercepted.add(f"https://www.tiktok.com/@{username}/{t}/{item_id}")
                    except Exception:
                        pass

                m_page.on("response", _on_mobile_resp)

                await m_page.goto(canonical_url, wait_until="domcontentloaded", timeout=35000)
                await asyncio.sleep(3.0)

                for _ in range(4):
                    await m_page.evaluate("window.scrollBy(0, 1000)")
                    await asyncio.sleep(1.5)

                m_links = await m_page.locator('a[href*="/video/"], a[href*="/photo/"]').all()
                target_user_tag = f"@{username.lower()}"
                for a_link in m_links:
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

                for i_url in m_intercepted:
                    if i_url not in seen_urls:
                        collected_urls.append(i_url)
                        seen_urls.add(i_url)

                await m_page.close()
            except Exception as m_err:
                logger.debug(f"Mobile emulation note: {m_err}")
            finally:
                await self.close()

        # ─────────────────────────────────────────────────────────────────────
        # SAFETY CHECK: Deteksi Captcha Block jika URL masih 0
        # ─────────────────────────────────────────────────────────────────────
        if not collected_urls and (challenge_hit or browser_captcha_hit):
            self.blocked_by_challenge = True
            logger.warning(
                f"[🚨 BLOCKED] TikTok menyajikan Captcha/WAF Challenge untuk @{username} — membatalkan crawl agar tidak ditandai selesai palsu."
            )
            return

        logger.info(
            f"{TAG_CRAWL} Total {len(collected_urls)} URL postingan asli @{username} berhasil dikumpulkan."
        )

        # ─────────────────────────────────────────────────────────────────────
        # STEP 6: DSU Sorting & SQLite Stop Condition
        # Urutan Evaluasi: Post PALING BARU ke PALING LAMA untuk stop-condition DB
        # Urutan Yield ke Downstream: KRONOLOGIS (PALING LAMA ke PALING BARU)
        # ─────────────────────────────────────────────────────────────────────
        decorated = [(int(self._extract_post_id(url) or 0), url) for url in collected_urls]
        decorated.sort(key=lambda x: x[0], reverse=True)

        pending_posts: list[tuple[int, str]] = []
        for post_id_num, post_url in decorated:
            post_id = self._extract_post_id(post_url)
            if not post_id:
                continue

            if not forced and await self.db.check_post_exists(post_id, self.PLATFORM):
                logger.info(
                    f"{TAG_CRAWL} Stop-condition: post {post_id} sudah ada di DB — checkpoint tercapai."
                )
                break

            pending_posts.append((post_id_num, post_url))

        pending_posts.sort(key=lambda x: x[0], reverse=False)

        for _, post_url in pending_posts:
            post_id = self._extract_post_id(post_url)
            if not post_id:
                continue
            media_type = MediaType.PHOTO if "/photo/" in post_url else MediaType.VIDEO

            yield PostMedia(
                post_id=post_id,
                post_url=post_url,
                profile_url=profile_url,
                platform=self.PLATFORM,
                media_type=media_type,
                cookies_file=None,
            )

    def _parse_rehydration_from_html(self, html: str, username: str) -> dict:
        """
        Parse data rehydration JSON dari HTML SSR TikTok dengan strict author verification.
        """
        found_urls = set()
        video_count = 0
        sec_uid = ""
        u = username.lower().replace("@", "").strip()

        def search_json(obj: Any) -> None:
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

                # Structure 1: ItemModule -> Map of {postId: ItemDetail}
                item_module = obj.get("ItemModule")
                if isinstance(item_module, dict):
                    for p_id, item in item_module.items():
                        if isinstance(item, dict) and self._is_valid_creator_post(item, u):
                            if p_id and re.match(r"^\d{15,22}$", str(p_id)):
                                is_photo = bool(item.get("imagePost") or item.get("images") or item.get("imageList"))
                                t = "photo" if is_photo else "video"
                                found_urls.add(f"https://www.tiktok.com/@{u}/{t}/{p_id}")

                # Structure 2: webapp.user-detail / userPage videoList
                user_detail = obj.get("webapp.user-detail") or obj.get("user-detail") or obj.get("userPage")
                if isinstance(user_detail, dict):
                    u_items = user_detail.get("itemList") or user_detail.get("videoList")
                    if isinstance(u_items, list):
                        for it in u_items:
                            if isinstance(it, str) and re.match(r"^\d{15,22}$", it):
                                found_urls.add(f"https://www.tiktok.com/@{u}/video/{it}")
                            elif isinstance(it, dict) and self._is_valid_creator_post(it, u):
                                p_id = it.get("id") or it.get("itemId") or it.get("vid")
                                if p_id and re.match(r"^\d{15,22}$", str(p_id)):
                                    is_photo = bool(it.get("imagePost") or it.get("images") or it.get("imageList"))
                                    t = "photo" if is_photo else "video"
                                    found_urls.add(f"https://www.tiktok.com/@{u}/{t}/{p_id}")

                # Stats video counter
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

        return {
            "urls": list(found_urls),
            "videoCount": video_count,
            "secUid": sec_uid,
        }

    async def close(self) -> None:
        """Cleanup browser context and instances."""
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

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
from core.utils import (
    TAG_CRAWL, TAG_SYSTEM, TAG_SUCCESS, TAG_WARN, TAG_ERROR, TAG_DOWN
)

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
        if self._context is None:
            raise RuntimeError("Browser context gagal diinisialisasi oleh Playwright — new_context() return None")
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
            logger.error(f"{TAG_ERROR} Tidak bisa ekstrak username dari URL: {profile_url}")
            return

        canonical_url = f"https://www.tiktok.com/@{username}"
        collected_urls: list[str] = []
        seen_urls: set[str] = set()

        # ── Step 0: Siapkan file cookie Netscape untuk yt-dlp / browser ──
        netscape_cookie_path = await self.load_and_inject_cookies(None, "tiktok")
        if netscape_cookie_path:
            self.netscape_cookie_path = netscape_cookie_path

        # ── Method 1 (Primary / Super Cepat): Ekstraksi profil via yt-dlp flat-playlist ──
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
                            # Pastikan URL valid dan milik target akun
                            if username.lower() in clean_p_url.lower() and clean_p_url not in seen_urls:
                                collected_urls.append(clean_p_url)
                                seen_urls.add(clean_p_url)
                    except Exception:
                        pass
        except Exception as e:
            logger.warning(f"{TAG_WARN} yt-dlp flat-playlist extraction gagal: {e} — lanjut fallback...")

        if not collected_urls:
            # ── Method 2 (Fallback): Browser Automation via Playwright ──
            logger.info(f"{TAG_CRAWL} yt-dlp kosong — fallback ke Playwright browser automation...")
            try:
                page = await self._init_browser()
                intercepted_urls: set[str] = set()

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

                logger.info(f"{TAG_CRAWL} Membuka halaman profil TikTok: {canonical_url}")
                await page.goto(canonical_url, wait_until="domcontentloaded", timeout=60000)
                await asyncio.sleep(2.5)

                # Scroll awal untuk memicu event network loading feed internal TikTok
                await page.evaluate("window.scrollBy(0, 1200)")
                await asyncio.sleep(2.5)

                # 1. First Pass: Coba ekstrak postingan dari data rehydration JSON
                logger.info(f"{TAG_CRAWL} Membaca rehydration JSON dari halaman profil @{username}...")
                rehydration_data = {}
                try:
                    rehydration_data = await page.evaluate(r"""
                        (targetUsername) => {
                            const u = targetUsername.toLowerCase().replace('@', '');
                            const foundUrls = new Set();
                            let videoCount = 0;
                            let secUid = '';

                            function searchJson(obj) {
                                if (!obj || typeof obj !== 'object') return;

                                if (obj.secUid && typeof obj.secUid === 'string') secUid = obj.secUid;
                                if (obj.sec_uid && typeof obj.sec_uid === 'string') secUid = obj.sec_uid;
                                if (obj.user && obj.user.secUid) secUid = obj.user.secUid;

                                if (Array.isArray(obj.itemList)) {
                                    for (const item of obj.itemList) {
                                        if (typeof item === 'string' && /^\d{15,22}$/.test(item)) {
                                            foundUrls.add(`https://www.tiktok.com/@${u}/video/${item}`);
                                        } else if (item && typeof item === 'object') {
                                            const id = item.id || item.itemId || item.vid;
                                            if (id && /^\d{15,22}$/.test(String(id))) {
                                                const isPhoto = item.imagePost || (item.imageList && item.imageList.length > 0) || item.images;
                                                const type = isPhoto ? 'photo' : 'video';
                                                foundUrls.add(`https://www.tiktok.com/@${u}/${type}/${id}`);
                                            }
                                        }
                                    }
                                }

                                if (obj.ItemModule && typeof obj.ItemModule === 'object') {
                                    for (const [id, item] of Object.entries(obj.ItemModule)) {
                                        if (typeof item === 'object' && item) {
                                            const author = (item.author || item.authorName || item.nickname || '').toLowerCase().replace('@', '');
                                            if (!author || author === u) {
                                                const isPhoto = item.imagePost || (item.imageList && item.imageList.length > 0) || item.images;
                                                const type = isPhoto ? 'photo' : 'video';
                                                foundUrls.add(`https://www.tiktok.com/@${u}/${type}/${id}`);
                                            }
                                        }
                                    }
                                }

                                if (obj.stats && (obj.stats.videoCount || obj.stats.video_count)) {
                                    videoCount = obj.stats.videoCount || obj.stats.video_count || videoCount;
                                }
                                if (obj.videoCount && typeof obj.videoCount === 'number') {
                                    videoCount = obj.videoCount;
                                }

                                for (const val of Object.values(obj)) {
                                    if (val && typeof val === 'object') {
                                        searchJson(val);
                                    }
                                }
                            }

                            const scriptEl = document.getElementById('__UNIVERSAL_DATA_FOR_REHYDRATION__') || document.getElementById('SIGI_STATE') || document.getElementById('__NEXT_DATA__');
                            if (scriptEl && scriptEl.textContent) {
                                try {
                                    const data = JSON.parse(scriptEl.textContent);
                                    searchJson(data);
                                } catch (e) {}
                            }
                            if (window.__UNIVERSAL_DATA_FOR_REHYDRATION__) {
                                searchJson(window.__UNIVERSAL_DATA_FOR_REHYDRATION__);
                            }
                            if (window.SIGI_STATE) {
                                searchJson(window.SIGI_STATE);
                            }

                            return {
                                urls: Array.from(foundUrls),
                                videoCount: videoCount,
                                secUid: secUid
                            };
                        }
                    """, username)
                except Exception as e:
                    logger.warning(f"Gagal membaca rehydration data: {e}")
                    rehydration_data = {}

                if not isinstance(rehydration_data, dict):
                    rehydration_data = {}

                rehydration_urls = rehydration_data.get("urls", [])
                expected_video_count = int(rehydration_data.get("videoCount", 0))
                sec_uid = str(rehydration_data.get("secUid", "")).strip()

                if expected_video_count > 0:
                    logger.info(f"{TAG_CRAWL} Target @{username}: {expected_video_count} video publik terdeteksi.")
                if sec_uid:
                    logger.info(f"{TAG_CRAWL} secUid terdeteksi untuk @{username}: {sec_uid[:16]}...")

                # First Pass: Kumpulkan URL dari Network API Interceptor dengan scroll pemicu
                for _ in range(4):
                    for i_url in intercepted_urls:
                        if i_url not in seen_urls:
                            collected_urls.append(i_url)
                            seen_urls.add(i_url)

                    if expected_video_count > 0 and len(collected_urls) >= expected_video_count:
                        break

                    await page.evaluate("window.scrollBy(0, 1500)")
                    await asyncio.sleep(2.0)

                for r_url in rehydration_urls:
                    if r_url not in seen_urls:
                        collected_urls.append(r_url)
                        seen_urls.add(r_url)

                if collected_urls:
                    logger.info(f"{TAG_CRAWL} Berhasil mengumpulkan {len(collected_urls)} postingan dari network stream @{username}.")

                # Second Pass (DOM fallback): jika network stream belum lengkap
                if expected_video_count > 0 and len(collected_urls) < expected_video_count:
                    logger.info(f"{TAG_CRAWL} Menunggu feed DOM TikTok siap...")
                    try:
                        await page.wait_for_selector('a[href*="/video/"], a[href*="/photo/"]', timeout=6000)
                    except Exception:
                        pass

                    for scroll_idx in range(1, 6):
                        urls_snapshot = await page.evaluate(r"""
                            (targetUsername) => {
                                const u = targetUsername.toLowerCase().replace('@', '');
                                const results = [];
                                const seenIds = new Set();
                                const gridContainer = document.querySelector('[data-e2e="user-post-item-list"]') || document.querySelector('main') || document.querySelector('#main-content-others_homepage');
                                const root = gridContainer || document;
                                const allLinks = root.querySelectorAll('a[href*="/video/"], a[href*="/photo/"], a[href*="/v/"]');
                                for (const a of allLinks) {
                                    if (a.closest('aside, [data-e2e="recommend-list"], [data-e2e="sidebar"], nav, footer, [class*="Suggest"]')) continue;
                                    const fullUrl = a.href || a.getAttribute('href') || '';
                                    const match = fullUrl.match(/\/(video|photo|v)\/(\d{15,22})/);
                                    if (match) {
                                        const type = match[1] === 'photo' ? 'photo' : 'video';
                                        const id = match[2];
                                        const lower = fullUrl.toLowerCase();
                                        if (lower.includes('/@') && !lower.includes('/@' + u + '/')) continue;
                                        if (!seenIds.has(id)) {
                                            seenIds.add(id);
                                            results.push(`https://www.tiktok.com/@${u}/${type}/${id}`);
                                        }
                                    }
                                }
                                return results;
                            }
                        """, username)

                        for href in urls_snapshot:
                            clean_url = href.split("?")[0]
                            if clean_url not in seen_urls:
                                collected_urls.append(clean_url)
                                seen_urls.add(clean_url)

                        if expected_video_count > 0 and len(collected_urls) >= expected_video_count:
                            break

                        await page.evaluate("window.scrollBy(0, 1500)")
                        await asyncio.sleep(2.0)

            except Exception as e:
                logger.error(f"{TAG_ERROR} Playwright fallback error: {e}")

        logger.info(
            f"{TAG_CRAWL} Total {len(collected_urls)} URL postingan @{username} berhasil dikumpulkan."
        )

        # Optimasi sorting dengan Decorate-Sort-Undecorate (DSU) pattern
        # Menghindari eksekusi parser regex berulang-ulang di dalam loop sorting
        decorated = [(int(self._extract_post_id(url) or 0), url) for url in collected_urls]
        decorated.sort(key=lambda x: x[0], reverse=True)  # newest-first; caller reverses to oldest-first
        video_list = [url for _, url in decorated]

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

        # Log summary
        if filtered_count > 0 and yield_count == 0:
            logger.info(
                f"{TAG_CRAWL} @{username}: {filtered_count}/{len(video_list)} post sudah di DB "
                f"— tidak ada postingan baru."
            )
        elif filtered_count > 0:
            logger.info(
                f"{TAG_CRAWL} @{username}: {yield_count} post baru di-yield, "
                f"{filtered_count} sudah di DB (di-skip)."
            )

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
        """Tutup browser Playwright dengan aman + timeout anti-hang untuk headless server."""
        async def _safe_close(coro, label: str) -> None:
            try:
                await asyncio.wait_for(coro, timeout=10.0)
            except asyncio.TimeoutError:
                logger.warning(f"Timeout menutup {label} — lanjutkan shutdown")
            except Exception as e:
                logger.warning(f"Error menutup {label}: {e}")

        if self._context:
            await _safe_close(self._context.close(), "TikTok browser context")
        if self._browser:
            await _safe_close(self._browser.close(), "TikTok browser")
        if self._playwright:
            await _safe_close(self._playwright.stop(), "TikTok playwright")

        self._context = None
        self._browser = None
        self._playwright = None

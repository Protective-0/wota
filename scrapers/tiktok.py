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
        try:
            page = await self._init_browser()
        except Exception as e:
            logger.error(f"{TAG_ERROR} Gagal init browser/cookie TikTok: {e}")
            self.failed = True
            return

        intercepted_urls: set[str] = set()

        async def _on_response(response):
            try:
                # Intercept response JSON dari internal endpoint TikTok (post/item_list)
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
                                    author_name = author.get("uniqueId") or author.get("id") or ""
                                elif isinstance(author, str):
                                    author_name = author
                                
                                author_clean = str(author_name).lower().replace("@", "").strip()
                                target_clean = username.lower().replace("@", "").strip()

                                # HANYA ambil jika author postingan sama persis dengan target_clean
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

        try:
            logger.info(f"{TAG_CRAWL} Membuka halaman profil TikTok: {canonical_url}")
            await page.goto(canonical_url, wait_until="domcontentloaded", timeout=60000)
            await asyncio.sleep(3.0)

            # Cek jika ada captcha verify overlay sebelum reload
            has_captcha = await page.evaluate("""() => {
                return !!(document.querySelector('.captcha_verify_container') || document.querySelector('#captcha-verify-image'));
            }""")
            if has_captcha:
                logger.info(f"{TAG_CRAWL} Captcha overlay terdeteksi — melakukan auto-refresh...")
                try:
                    await page.reload(wait_until="domcontentloaded", timeout=60000)
                    await asyncio.sleep(2.5)
                except Exception as e:
                    logger.warning(f"{TAG_WARN} Refresh timeout/gagal: {e} — lanjut proses...")

            # Proteksi: Pastikan browser tidak dialihkan ke login wall, passport, atau akun lain
            current_url = page.url.lower()
            if "/login" in current_url or "/passport" in current_url or username.lower() not in current_url:
                logger.warning(
                    f"{TAG_WARN} Browser dialihkan ke '{page.url}' (bukan @{username}), "
                    f"navigasi paksa ke {canonical_url}..."
                )
                await page.goto(canonical_url, wait_until="domcontentloaded", timeout=30000)
                await asyncio.sleep(2.0)
                if "/login" in page.url.lower() or "/passport" in page.url.lower():
                    logger.error(f"{TAG_ERROR} TikTok login wall / passport terdeteksi untuk @{username} — periksa session token!")
                    return

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

            collected_urls = []
            seen_urls = set()
            rehydration_urls = rehydration_data.get("urls", [])
            expected_video_count = int(rehydration_data.get("videoCount", 0))
            sec_uid = str(rehydration_data.get("secUid", "")).strip()

            if expected_video_count > 0:
                logger.info(f"{TAG_CRAWL} Target @{username}: {expected_video_count} video publik terdeteksi.")
            if sec_uid:
                logger.info(f"{TAG_CRAWL} secUid terdeteksi untuk @{username}: {sec_uid[:16]}...")

            # 1. First Pass: Gabungkan URL dari Network API Interceptor & Rehydration Data
            for i_url in intercepted_urls:
                if i_url not in seen_urls:
                    collected_urls.append(i_url)
                    seen_urls.add(i_url)

            if intercepted_urls:
                logger.info(f"{TAG_CRAWL} API Interceptor: {len(intercepted_urls)} postingan terdeteksi dari network traffic.")

            for r_url in rehydration_urls:
                if r_url not in seen_urls:
                    collected_urls.append(r_url)
                    seen_urls.add(r_url)

            if rehydration_urls:
                logger.info(
                    f"{TAG_CRAWL} Rehydration JSON: {len(rehydration_urls)} postingan langsung dari "
                    f"script tag @{username}."
                )

            # 2. Second Pass (Super Fast & Resilient): Direct In-Browser API Fetch menggunakan secUid
            if sec_uid and len(collected_urls) < (expected_video_count or 1):
                logger.info(f"{TAG_CRAWL} Memanggil direct API endpoint TikTok dengan secUid @{username}...")
                try:
                    direct_urls = await page.evaluate(r"""
                        async (params) => {
                            const u = params.username.toLowerCase().replace('@', '');
                            const secUid = params.secUid;
                            const targetCount = params.targetCount || 35;
                            const results = [];
                            let cursor = 0;
                            let hasMore = true;
                            let attempts = 0;

                            while (hasMore && results.length < targetCount && attempts < 6) {
                                attempts++;
                                try {
                                    const url = `/api/post/item_list/?aid=1988&app_language=en&app_name=tiktok_web&count=35&cursor=${cursor}&secUid=${encodeURIComponent(secUid)}&device_platform=web_pc`;
                                    const res = await fetch(url, {
                                        headers: {
                                            'Accept': 'application/json, text/plain, */*'
                                        }
                                    });
                                    if (!res.ok) break;
                                    const data = await res.json();
                                    const items = data.itemList || data.items || [];
                                    if (!items || !items.length) break;

                                    for (const item of items) {
                                        const author = item.author;
                                        let itemAuthor = '';
                                        if (author && typeof author === 'object') {
                                            itemAuthor = author.uniqueId || author.id || '';
                                        } else if (typeof author === 'string') {
                                            itemAuthor = author;
                                        }
                                        const cleanAuthor = String(itemAuthor).toLowerCase().replace('@', '').trim();
                                        if (cleanAuthor && cleanAuthor !== u) continue;

                                        const id = item.id || item.itemId || item.vid;
                                        if (id) {
                                            const isPhoto = item.imagePost || (item.imageList && item.imageList.length > 0) || item.images;
                                            const type = isPhoto ? 'photo' : 'video';
                                            results.push(`https://www.tiktok.com/@${u}/${type}/${id}`);
                                        }
                                    }

                                    cursor = data.cursor || 0;
                                    hasMore = !!data.hasMore && results.length < targetCount;
                                    if (!hasMore) break;
                                } catch (e) {
                                    break;
                                }
                            }
                            return results;
                        }
                    """, {"username": username, "secUid": sec_uid, "targetCount": expected_video_count or 35})

                    if direct_urls:
                        logger.info(f"{TAG_CRAWL} Direct API: +{len(direct_urls)} postingan berhasil diambil via secUid.")
                        for d_url in direct_urls:
                            if d_url not in seen_urls:
                                collected_urls.append(d_url)
                                seen_urls.add(d_url)
                except Exception as e:
                    logger.warning(f"{TAG_WARN} Direct API fetch error: {e}")

            # Jika pass direct API dan rehydration sudah mencukupi target, langsung selesai tanpa scroll DOM
            if expected_video_count > 0 and len(collected_urls) >= expected_video_count:
                collected_urls = collected_urls[:expected_video_count]
                logger.info(
                    f"{TAG_CRAWL} Target tercapai via API/Rehydration: {len(collected_urls)}/{expected_video_count} "
                    f"URL @{username} terkumpul."
                )
            else:
                # 3. Third Pass: Scrolling DOM & ekstraksi link postingan (bounded loop max 6 scroll)
                logger.info(f"{TAG_CRAWL} Menunggu feed DOM TikTok siap...")
                try:
                    await page.wait_for_selector('a[href*="/video/"], a[href*="/photo/"]', timeout=6000)
                except Exception:
                    logger.debug(f"{TAG_CRAWL} Selector feed DOM timeout — lanjut scroll.")

                await asyncio.sleep(1.5)
                logger.info(f"{TAG_CRAWL} Mulai scroll+ekstraksi feed @{username} (max {6} pass)...")
            consecutive_empty_scrolls = 0
            max_scroll_attempts = 6

            for scroll_idx in range(1, max_scroll_attempts + 1):
                # Tutup dialog modal / cookie banner jika muncul
                await page.evaluate("""() => {
                    const closeBtns = document.querySelectorAll('[data-e2e="modal-close-inner-button"], [aria-label="Close"], button[aria-label="Close"], .tiktok-modal__close');
                    closeBtns.forEach(b => { try { b.click(); } catch(e){} });
                }""")

                # Ambil snapshot URL dari card grid postingan profil (hanya post milik user target)
                urls_snapshot = await page.evaluate(r"""
                    (targetUsername) => {
                        const u = targetUsername.toLowerCase().replace('@', '');
                        const results = [];
                        const seenIds = new Set();
                        
                        // Ekstrak HANYA dari container grid postingan profil
                        const gridContainer = document.querySelector('[data-e2e="user-post-item-list"]') || document.querySelector('main') || document.querySelector('#main-content-others_homepage');
                        const root = gridContainer || document;
                        const allLinks = root.querySelectorAll('a[href*="/video/"], a[href*="/photo/"], a[href*="/v/"]');
                        
                        for (const a of allLinks) {
                            // Abaikan link yang berada di sidebar rekomendasi / footer / suggestions
                            if (a.closest('aside, [data-e2e="recommend-list"], [data-e2e="sidebar"], nav, footer, [class*="Suggest"]')) {
                                continue;
                            }

                            const fullUrl = a.href || a.getAttribute('href') || '';
                            const match = fullUrl.match(/\/(video|photo|v)\/(\d{15,22})/);
                            if (match) {
                                const type = match[1] === 'photo' ? 'photo' : 'video';
                                const id = match[2];
                                
                                const lower = fullUrl.toLowerCase();
                                if (lower.includes('/@')) {
                                    if (!lower.includes('/@' + u + '/')) {
                                        continue; // Milik akun lain
                                    }
                                } else {
                                    // Jika URL relative tanpa username, WAJIB berada di user-post card container
                                    if (!a.closest('[data-e2e="user-post-item"], [data-e2e="user-post-item-list"], div[class*="DivItemContainer"], div[class*="PostItem"]')) {
                                        continue;
                                    }
                                }
                                
                                if (!seenIds.has(id)) {
                                    seenIds.add(id);
                                    results.push(`https://www.tiktok.com/@${u}/${type}/${id}`);
                                }
                            }
                        }
                        
                        return results;
                    }
                """, username)

                new_added = 0

                # Tambahkan URL baru yang tertangkap dari interceptor selama scrolling
                for i_url in intercepted_urls:
                    if i_url not in seen_urls:
                        collected_urls.append(i_url)
                        seen_urls.add(i_url)
                        new_added += 1

                for href in urls_snapshot:
                    clean_url = href.split("?")[0]
                    if clean_url.startswith('/'):
                        clean_url = f"https://www.tiktok.com{clean_url}"

                    if clean_url not in seen_urls:
                        collected_urls.append(clean_url)
                        seen_urls.add(clean_url)
                        new_added += 1

                if new_added > 0:
                    consecutive_empty_scrolls = 0
                    logger.info(
                        f"{TAG_CRAWL} Scroll [{scroll_idx}/{max_scroll_attempts}]: "
                        f"+{new_added} URL baru (total: {len(collected_urls)})"
                    )
                else:
                    consecutive_empty_scrolls += 1
                    logger.debug(
                        f"{TAG_CRAWL} Scroll [{scroll_idx}/{max_scroll_attempts}]: "
                        f"tidak ada URL baru ({consecutive_empty_scrolls}/3 empty)"
                    )

                # Jika sudah mencapai target videoCount yang tertera di profil, potong dan berhenti seketika
                if expected_video_count > 0 and len(collected_urls) >= expected_video_count:
                    collected_urls = collected_urls[:expected_video_count]
                    logger.info(
                        f"{TAG_CRAWL} Target tercapai: {len(collected_urls)}/{expected_video_count} "
                        f"URL @{username} terkumpul."
                    )
                    break

                # Jika 3x scroll berturut-turut tidak ada post baru, akhiri loop
                if consecutive_empty_scrolls >= 3:
                    logger.info(f"{TAG_CRAWL} 3x scroll kosong berturut — feed @{username} sudah habis.")
                    break

                # Scroll ke bawah untuk memicu IntersectionObserver TikTok memuat batch berikutnya
                await page.evaluate("window.scrollBy(0, 1500)")
                await asyncio.sleep(2.5)

            # Safety cap jika ada videoCount terdeteksi
            if expected_video_count > 0 and len(collected_urls) > expected_video_count:
                collected_urls = collected_urls[:expected_video_count]

            logger.info(
                f"{TAG_CRAWL} Total {len(collected_urls)} URL postingan @{username} berhasil dikumpulkan."
            )

            # Optimasi sorting dengan Decorate-Sort-Undecorate (DSU) pattern
            # Menghindari eksekusi parser regex berulang-ulang di dalam loop sorting
            decorated = [(int(self._extract_post_id(url) or 0), url) for url in collected_urls]
            # FIX: sort comment was misleading — reverse=True = descending = NEWEST FIRST.
            # _patrol_account calls new_posts.reverse() before Discord upload,
            # flipping back to OLDEST FIRST for correct chronological upload order.
            # This double-reverse is intentional: scraper yields newest-first,
            # caller reverses to oldest-first. Do not remove either reverse.
            decorated.sort(key=lambda x: x[0], reverse=True)  # newest-first; caller reverses to oldest-first
            video_list = [url for _, url in decorated]

            # FIX: track how many posts are filtered vs yielded so operators can
            # distinguish "scraper broken" from "all posts already in DB (normal patrol)".
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

            # Log summary: makes "67 found → 0 yielded" visible instead of silent
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


        except Exception as e:
            logger.error(f"{TAG_ERROR} Error saat scraping profil TikTok @{username}: {e}")
        finally:
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

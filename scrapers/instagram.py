"""
scrapers/instagram.py
Scraper profil Instagram menggunakan Playwright (browser automation) + yt-dlp.

Strategi:
- Playwright berjalan dalam mode headed agar user bisa login manual pada run pertama
- Session cookies disimpan ke cookies_ig.json dan dimuat ulang otomatis
- Smart scroll dengan stop-condition: berhenti saat menemukan post yang sudah di-DB
- Mendukung foto, video, dan carousel (multi-slide)
- Dual-tab crawling: feed utama (/username/) lalu tab Reels (/username/reels/)
  sehingga video eksklusif Reels yang tidak muncul di grid juga ikut tersapu.

Arsitektur Dual-Tab:
    scrape_profile()
        ├── [Tab 1] _scroll_and_collect_posts()   → URL /p/SHORTCODE  (feed + carousel)
        └── [Tab 2] _scroll_and_collect_reels()   → URL /reel/SHORTCODE (Reels eksklusif)

PENTING: Instagram mendeteksi bot. Gunakan random delay dan jangan scrape terlalu agresif.
"""

import asyncio
import json
import logging
import os
import random
import re
import sys
from pathlib import Path
from typing import AsyncGenerator, Optional

import aiofiles  # Async file I/O agar baca cookies tidak blokir event loop
from playwright.async_api import Browser, BrowserContext, Page, async_playwright

from .base import BaseScraper, MediaType, PostMedia, USER_AGENT

logger = logging.getLogger(__name__)

COOKIE_FILE = "cookies_ig.json"
MANUAL_LOGIN_TIMEOUT = 300


class InstagramScraper(BaseScraper):
    """
    Scraper profil Instagram dengan manajemen sesi browser Playwright.

    Mendukung dua sumber konten secara berurutan:
    1. Feed/grid utama profil  → foto, video, carousel
    2. Tab Reels khusus        → video Reels yang mungkin tidak muncul di feed

    Setiap sumber menggunakan stop-condition SQLite yang sama: scroll berhenti
    otomatis begitu menemukan konten yang sudah pernah diunduh sebelumnya.
    """

    PLATFORM = "instagram"

    def __init__(self, db_manager, session_dir: str, headed: bool = True):
        super().__init__(db_manager, session_dir)
        self.headed = headed
        self.session_dir = Path(session_dir)
        self.session_dir.mkdir(parents=True, exist_ok=True)
        self.cookie_path = self.session_dir / COOKIE_FILE
        # Path cookies format Netscape — digunakan yt-dlp untuk autentikasi
        self.netscape_cookie_path = self.session_dir / "instagram_cookies.txt"
        logger.info(f"Manual login timeout di-set: {MANUAL_LOGIN_TIMEOUT}s")
        self._playwright = None
        self._browser: Optional[Browser] = None
        self._context: Optional[BrowserContext] = None

    async def _init_browser(self) -> BrowserContext:
        """
        Inisialisasi browser Playwright dengan injeksi static JSON cookies.
        """
        self._playwright = await async_playwright().start()

        # Deteksi otomatis path Brave via base class helper
        brave_path = BaseScraper.get_brave_path()
        if brave_path:
            logger.info(f"Menggunakan browser Brave dari: {brave_path}")
        else:
            logger.warning("Browser Brave tidak ditemukan di folder default. Menggunakan Chromium bawaan Playwright.")

        # FIX: replaced manual launch_kwargs dict with base class helper
        # to ensure all stealth args (--disable-web-security, --disable-features etc.)
        # are consistent across all scrapers
        launch_kwargs = BaseScraper.get_browser_launch_kwargs()
        launch_kwargs["headless"] = not self.headed

        self._browser = await self._playwright.chromium.launch(**launch_kwargs)
        # Prevent assertion stripping under python -O optimization flag
        if self._browser is None:
            raise RuntimeError("Browser instance is not initialized")
        self._context = await self._browser.new_context(
            user_agent=USER_AGENT,
            viewport={"width": 1280, "height": 800},
            locale="id-ID",
        )
        await self._context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

        # Memuat cookie dari static JSON config/cookies/instagram.json
        netscape_path = await self.load_and_inject_cookies(self._context, "instagram")
        if not netscape_path:
            raise ValueError("File cookie Instagram tidak ditemukan atau kosong.")
        self.netscape_cookie_path = netscape_path

        return self._context

    async def scrape_profile(
        self,
        profile_url: str,
        forced: bool = False,
    ) -> AsyncGenerator[PostMedia, None]:
        """
        Crawl semua konten dari profil Instagram melalui dua tahap berurutan:

        TAHAP 1 — Feed/Grid Utama:
            Crawl halaman profil utama (/{username}/) untuk mengambil semua
            foto, video biasa, dan carousel dari feed grid.

        TAHAP 2 — Tab Reels Eksklusif:
            Setelah feed selesai, arahkan browser ke (/{username}/reels/)
            untuk mengambil video Reels yang TIDAK muncul di feed utama.

        Kedua tahap menggunakan stop-condition yang sama: scroll berhenti
        saat menemukan konten yang sudah ada di SQLite (resume point).
        """
        try:
            try:
                await self._init_browser()
            except Exception as e:
                logger.error(f"[❌ ERROR  ] Menghentikan scraping profil Instagram karena kesalahan browser/cookie: {e}")
                self.failed = True
                return

            username = self._extract_username(profile_url)
            if not username:
                logger.error(f"Username tidak valid: {profile_url}")
                return
            # Store for use in _extract_metadata_via_ytdlp (post URLs can't yield username)
            self._profile_url_username = username

            canonical_url = f"https://www.instagram.com/{username}/"
            reels_url = f"https://www.instagram.com/{username}/reels/"

            assert self._context is not None
            page = await self._context.new_page()

            try:
                # ──────────────────────────────────────────
                # TAHAP 1: Kumpulkan URL Feed / Grid Utama
                # ──────────────────────────────────────────
                logger.info(f"[TAB 1/2] Mengumpulkan URL feed: {canonical_url}")
                await page.goto(canonical_url, wait_until="domcontentloaded")
                await asyncio.sleep(3)
                feed_urls = await self._scroll_and_collect_posts(page, canonical_url, forced=forced)
                logger.info(f"[TAB 1/2] {len(feed_urls)} post ditemukan di feed {username}")

                # ──────────────────────────────────────────
                # TAHAP 2: Kumpulkan URL Tab Reels Eksklusif
                # ──────────────────────────────────────────
                logger.info(f"[TAB 2/2] Mengumpulkan URL Reels: {reels_url}")
                await page.goto(reels_url, wait_until="domcontentloaded")
                await asyncio.sleep(3)
                reels_urls = await self._scroll_and_collect_reels(page, username, forced=forced)
                logger.info(f"[TAB 2/2] {len(reels_urls)} Reels ditemukan")

                # ──────────────────────────────────────────
                # TAHAP 3: Gabungkan ke Staging Pool Unik
                # ──────────────────────────────────────────
                # Deduplicate while preserving insertion order (set() destroys order)
                combined_urls = list(dict.fromkeys(feed_urls + reels_urls))
                all_post_objects = []

                # ──────────────────────────────────────────
                # TAHAP 4: Ekstraksi Metadata secara Hybrid
                # ──────────────────────────────────────────
                logger.info(f"[⚙️ SYSTEM] Memulai ekstraksi metadata Hybrid untuk {len(combined_urls)} post/reels...")
                for target_url in combined_urls:
                    post_id = self._extract_post_id(target_url)
                    if not post_id:
                        continue

                    if not forced and await self.db.check_post_exists(post_id, self.PLATFORM):
                        continue

                    post_data = None
                    if "/reel/" in target_url:
                        # Coba ekstraktor cepat non-browser
                        post_data = await self._extract_metadata_via_ytdlp(target_url)
                        if not post_data:
                            # Fallback ke browser Playwright jika gagal
                            logger.info(f"Fallback browser untuk reel: {target_url}")
                            post_data = await self._scrape_single_post(page, target_url)
                            await asyncio.sleep(random.uniform(4.0, 7.0))
                    else:
                        # Posting /p/ (foto/carousel statis atau video) -> gunakan browser dengan delay adaptif
                        post_data = await self._scrape_single_post(page, target_url)
                        await asyncio.sleep(random.uniform(4.0, 7.0))

                    if post_data:
                        # Fix: preserve post_data.timestamp as None if extraction failed to maintain type consistency on PostMedia
                        all_post_objects.append(post_data)

                # ──────────────────────────────────────────
                # TAHAP 5: Urutkan Kronologis (Lama ke Baru)
                # ──────────────────────────────────────────
                # Type-consistent sort: handle None gracefully in key function without mutating domain object timestamp
                all_post_objects.sort(key=lambda x: str(x.timestamp) if x.timestamp else "1970-01-01T00:00:00.000Z", reverse=True)

                # ──────────────────────────────────────────
                # TAHAP 6: Yield Linear ke Downstream Pipeline
                # ──────────────────────────────────────────
                logger.info(f"[⚙️ SYSTEM] Yielding {len(all_post_objects)} sorted chronological timeline items to Discord...")
                for post_media_object in all_post_objects:
                    yield post_media_object
                    await asyncio.sleep(random.uniform(1.0, 2.5))

            finally:
                await page.close()
        finally:
            await self.close()

    async def _scroll_and_collect_posts(
        self, page: Page, profile_url: str, forced: bool = False
    ) -> list[str]:
        """
        Scroll halaman profil sambil mengumpulkan URL postingan.

        Smart stop-condition: berhenti scroll jika menemukan post yang
        sudah ada di database (artinya kita sudah sampai di batas download terakhir).
        Ini menghemat waktu dan kuota karena tidak perlu scroll sampai post terlama.
        """
        collected_urls = []
        seen_urls = set()
        should_stop = False

        while not should_stop:
            # Ambil semua link postingan yang saat ini terlihat di halaman
            post_links = await page.locator('a[href*="/p/"]').all()
            new_found_in_batch = 0

            for link in post_links:
                href = await link.get_attribute("href")
                if not href or href in seen_urls:
                    continue

                seen_urls.add(href)
                post_url = (
                    f"https://www.instagram.com{href}" if href.startswith("/") else href
                )

                # Ekstrak post ID
                post_id = self._extract_post_id(post_url)
                if not post_id:
                    continue

                # STOP-CONDITION: Cek apakah post ini sudah ada di database.
                # KECUALI jika forced=True.
                if not forced and await self.db.check_post_exists(post_id, self.PLATFORM):
                    logger.info(
                        f"Stop-condition aktif: post {post_id} sudah ada di DB. "
                        f"Berhenti scroll — resume point ditemukan."
                    )
                    should_stop = True
                    break

                collected_urls.append(post_url)
                new_found_in_batch += 1

            if should_stop:
                break

            # Scroll ke bawah untuk memuat lebih banyak post (lazy loading)
            prev_count = len(collected_urls)
            await page.evaluate("window.scrollBy(0, window.innerHeight * 2)")
            await asyncio.sleep(random.uniform(1.5, 3.0))  # Delay setelah scroll

            # Cek apakah ada post baru setelah scroll
            # Jika tidak ada post baru setelah 3x scroll, anggap sudah sampai bawah
            new_count = len(collected_urls)
            if new_count == prev_count:
                # Coba sekali lagi dengan scroll lebih jauh
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await asyncio.sleep(2)

                # Cek ulang
                post_links_retry = await page.locator('a[href*="/p/"]').all()
                found_new = False
                for link in post_links_retry:
                    href = await link.get_attribute("href")
                    if href and href not in seen_urls:
                        found_new = True
                        break

                if not found_new:
                    logger.info(
                        "Tidak ada post baru setelah scroll — profil sudah habis"
                    )
                    break

        return collected_urls

    async def _scroll_and_collect_reels(self, page: Page, username: str, forced: bool = False) -> list[str]:
        """
        Scroll tab Reels Instagram sambil mengumpulkan URL Reels yang
        BELUM ada di feed utama (belum dicatat di SQLite dari tahap sebelumnya).

        Perbedaan dari _scroll_and_collect_posts():
        - Selector: mencari href yang mengandung '/reel/' (bukan '/p/')
        - Tujuan: hanya mengambil Reels eksklusif yang tidak muncul di grid feed
        - Stop-condition identik: berhenti scroll jika reel_id sudah ada di DB

        Stop-condition sangat penting di sini karena tab Reels bisa berisi
        ratusan video dan rate limit Instagram jauh lebih ketat untuk konten video.
        """
        collected_urls: list[str] = []
        seen_urls: set[str] = set()
        no_new_count = 0
        MAX_NO_NEW = 3  # Berhenti setelah 3x scroll tanpa Reels baru

        while no_new_count < MAX_NO_NEW:
            # Selector untuk link Reels: /reel/SHORTCODE
            # Gunakan XPath-style selector yang lebih presisi untuk menghindari false positive
            reel_links = await page.locator(
                f'a[href*="/{username}/reel/"], a[href*="/reel/"]'
            ).all()
            new_found = 0
            should_stop = False

            for link in reel_links:
                href = await link.get_attribute("href")
                if not href or href in seen_urls:
                    continue

                # Filter: pastikan benar-benar link Reels, bukan link lain
                if "/reel/" not in href:
                    continue

                seen_urls.add(href)
                reel_url = (
                    f"https://www.instagram.com{href}" if href.startswith("/") else href
                )

                reel_id = self._extract_post_id(reel_url)
                if not reel_id:
                    continue

                # STOP-CONDITION (Reels): Cek SQLite — jika Reels ini sudah
                # pernah didownload, berarti kita sudah sampai di batas resume.
                # Break loop sekarang — hemat waktu & minimalisir risiko rate limit!
                # KECUALI jika forced=True.
                if not forced and await self.db.check_post_exists(reel_id, self.PLATFORM):
                    logger.info(
                        f"[Reels] Stop-condition: reel {reel_id} sudah di DB. "
                        f"Berhenti scroll tab Reels — resume point ditemukan."
                    )
                    should_stop = True
                    break

                collected_urls.append(reel_url)
                new_found += 1

            if should_stop:
                break

            if new_found == 0:
                no_new_count += 1
                logger.debug(
                    f"[Reels] Tidak ada Reels baru ({no_new_count}/{MAX_NO_NEW})"
                )
            else:
                no_new_count = 0

            # Scroll ke bawah untuk memuat Reels berikutnya (lazy loading)
            await page.evaluate("window.scrollBy(0, window.innerHeight * 3)")
            await asyncio.sleep(random.uniform(1.5, 3.0))

        logger.info(
            f"[Reels] Selesai scroll — {len(collected_urls)} Reels eksklusif dikumpulkan"
        )
        return collected_urls

    async def _extract_metadata_via_ytdlp(self, url: str) -> Optional[PostMedia]:
        """
        Ekstrak metadata (timestamp, caption) menggunakan yt-dlp secara asinkron/non-blocking
        untuk menghindari navigasi browser Playwright yang lambat dan memicu rate limit.
        """
        import yt_dlp
        from datetime import datetime, timezone
        
        post_id = self._extract_post_id(url)
        if not post_id:
            return None

        cookies_file = (
            str(self.netscape_cookie_path)
            if self.netscape_cookie_path and self.netscape_cookie_path.exists()
            else None
        )

        ydl_opts = {
            "skip_download": True,
            "quiet": True,
            "no_warnings": True,
            "playlist_items": "1",
        }
        if cookies_file:
            ydl_opts["cookiefile"] = cookies_file

        try:
            loop = asyncio.get_running_loop()
            def _extract():
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:  # pyright: ignore[reportArgumentType]
                    return ydl.extract_info(url, download=False)

            info = await loop.run_in_executor(None, _extract)
            if not info:
                return None

            # Ambil caption
            caption = info.get("description") or info.get("title") or ""

            # Ambil timestamp
            timestamp = None
            upload_timestamp = info.get("timestamp")
            if upload_timestamp:
                timestamp = datetime.fromtimestamp(upload_timestamp, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            else:
                upload_date = info.get("upload_date")
                if upload_date:
                    try:
                        dt = datetime.strptime(upload_date, "%Y%m%d").replace(tzinfo=timezone.utc)
                        timestamp = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
                    except Exception:
                        pass

            # Reels selalu video, tapi jika url mengandung /p/ bisa jadi media type lain.
            # vcodec can be None (not string "none") — guard both cases
            is_video = any(
                f.get("vcodec") is not None and f.get("vcodec") != "none"
                for f in (info.get("formats") or [])
            )
            media_type = MediaType.VIDEO if is_video else MediaType.PHOTO

            # Use the profile_url from the outer scrape_profile call, not extracted from post URL
            # (post URLs like /p/SHORTCODE return empty username from _extract_username)
            return PostMedia(
                post_id=post_id,
                post_url=url,
                profile_url=f"https://www.instagram.com/{self._profile_url_username}/" if hasattr(self, '_profile_url_username') and self._profile_url_username else url,
                platform=self.PLATFORM,
                media_type=media_type,
                media_urls=[],
                caption=caption,
                timestamp=timestamp,
                cookies_file=cookies_file,
            )
        except Exception as e:
            logger.warning(f"[yt-dlp] Gagal ekstraksi metadata non-browser untuk {url}: {e}")
            return None

    async def _scrape_single_post(
        self, page: Page, post_url: str
    ) -> Optional[PostMedia]:
        """
        Buka satu halaman postingan dan ekstrak detail media.
        Mendukung foto single, video, dan carousel.
        """
        # Guard: cek context aktif. Cek _browser juga untuk deteksi crash _init_browser.
        # _browser = None terjadi jika: (a) belum init, (b) _init_browser() throw sebelum set _browser
        if not self._context or self._browser is None or not self._browser.is_connected():
            logger.error("Browser context sudah tertutup atau belum siap — tidak bisa scrape post")
            return None

        try:
            # Tambahkan timeout navigasi eksplisit untuk mencegah hang
            await page.goto(
                post_url,
                wait_until="domcontentloaded",
                timeout=30_000,  # 30 detik
            )
            await asyncio.sleep(2)

            post_id = self._extract_post_id(post_url)
            caption = await self._extract_caption(page)
            media_type, media_urls = await self._detect_media_type(page)

            # Tentukan path cookies Netscape jika tersedia (untuk yt-dlp)
            cookies_file = (
                str(self.netscape_cookie_path)
                if self.netscape_cookie_path.exists()
                else None
            )

            # Ambil timestamp post
            timestamp = None
            try:
                time_loc = page.locator("time").first
                if await time_loc.count() > 0:
                    timestamp = await time_loc.get_attribute("datetime")
            except Exception:
                pass

            if not media_urls:
                # Fallback: yt-dlp download langsung dari URL post (gunakan cookies)
                return PostMedia(
                    post_id=post_id,
                    post_url=post_url,
                    profile_url=page.url,
                    platform=self.PLATFORM,
                    media_type=MediaType.UNKNOWN,
                    caption=caption,
                    timestamp=timestamp,
                    cookies_file=cookies_file,
                )

            return PostMedia(
                post_id=post_id,
                post_url=post_url,
                profile_url=page.url,
                platform=self.PLATFORM,
                media_type=media_type,
                media_urls=media_urls,
                caption=caption,
                timestamp=timestamp,
                cookies_file=cookies_file,
            )

        except Exception as e:
            err_str = str(e)
            # ERR_ABORTED: Instagram block/redirect navigasi (rate limit atau challenge)
            if "ERR_ABORTED" in err_str:
                logger.warning(
                    f"Navigasi dibatalkan Instagram untuk {post_url} "
                    f"— kemungkinan rate limit. Tunggu 10 detik..."
                )
                await asyncio.sleep(10)
            # TargetClosedError: browser/context sudah tertutup
            elif "Target page" in err_str or "context or browser" in err_str:
                logger.error("Browser context tertutup tiba-tiba — hentikan scraping post")
                raise  # Re-raise agar generator berhenti
            else:
                logger.error(f"Error scraping post {post_url}: {e}")
            return None

    async def _detect_media_type(self, page: Page) -> tuple[MediaType, list[str]]:
        """Deteksi tipe media dan ekstrak URL dari halaman postingan."""
        media_urls = []

        # Deteksi carousel menggunakan selector yang lebih stabil:
        # div[role='presentation'] ul li menargetkan slide carousel di media area,
        # mencegah pencocokan list komentar pada halaman post.
        carousel_items = await page.locator(
            "div[role='presentation'] ul li"
        ).all()
        if len(carousel_items) > 1:
            # Carousel: kumpulkan semua media URL
            for item in carousel_items:
                # Coba video dulu
                # Gunakan .count() untuk cek keberadaan tanpa trigger timeout 30s
                video_locator = item.locator("video source").first
                if await video_locator.count() > 0:
                    src = await video_locator.get_attribute("src")
                    # SKIP blob:// URL — URL ini hanya ada di dalam browser,
                    # tidak bisa didownload dari luar. Biarkan yt-dlp yang handle.
                    if src and not src.startswith("blob:"):
                        media_urls.append(src)
                        continue
                # Lalu foto — ambil CDN URL langsung (bukan blob)
                img_locator = item.locator("img").first
                if await img_locator.count() > 0:
                    src = await img_locator.get_attribute("src")
                    if src and not src.startswith("blob:") and "instagram" in src:
                        media_urls.append(src)

            # Jika semua item carousel adalah blob URL → media_urls kosong
            # → pipeline akan pakai yt-dlp dari post URL (sudah handle carousel)
            return MediaType.CAROUSEL, media_urls

        # Cek video single
        # Gunakan .count() untuk cek keberadaan tanpa trigger timeout 30s
        video_locator = page.locator("video").first
        if await video_locator.count() > 0:
            src = await video_locator.get_attribute("src")
            # PENTING: Instagram meload video sebagai blob:// URL di browser.
            # Blob URL bersifat lokal (in-memory), tidak bisa diakses httpx dari luar.
            # Solusi: return MediaType.VIDEO dengan media_urls kosong → pipeline
            # akan otomatis pakai yt-dlp yang bisa ekstrak video URL asli dari CDN.
            if src and not src.startswith("blob:"):
                media_urls.append(src)
            # Tetap kembalikan VIDEO agar pipeline tahu tipe kontennya
            return MediaType.VIDEO, media_urls

        # Foto single — gunakan yt-dlp untuk handle Instagram auth
        return MediaType.PHOTO, []

    async def _extract_caption(self, page: Page) -> str:
        """Ekstrak caption postingan dari halaman."""
        # FIX: `h1._aacl` uses Instagram's obfuscated class name \u2014 these change silently
        # with every Instagram frontend deploy and break caption extraction without any error.
        # Use a multi-fallback semantic selector chain instead: article h1 \u2192 testid h1 \u2192 bare h1.
        # None of these rely on Instagram's minified CSS class names.
        selectors = [
            "article h1",
            "[data-testid='post-caption'] h1",
            "h1",
        ]
        try:
            for selector in selectors:
                caption_locator = page.locator(selector).first
                if await caption_locator.count() > 0:
                    text = (await caption_locator.inner_text(timeout=1000)).strip()
                    if text:
                        return text
        except Exception:
            pass
        return ""

    def _extract_username(self, url: str) -> str:
        """Ekstrak username Instagram dari URL profil."""
        match = re.search(r"instagram\.com/([^/?&#/]+)/?", url)
        if match:
            username = match.group(1)
            # Skip URL yang bukan profil user
            if username not in {"p", "reel", "explore", "stories", "accounts"}:
                return username
        return ""

    def _extract_post_id(self, url: str) -> str:
        """Ekstrak shortcode postingan dari URL Instagram."""
        match = re.search(r"/p/([A-Za-z0-9_-]+)", url)
        if match:
            return match.group(1)
        # Coba format reel
        match = re.search(r"/reel/([A-Za-z0-9_-]+)", url)
        return match.group(1) if match else ""

    async def close(self) -> None:
        """Tutup browser dan playwright dengan aman."""
        if self._context:
            try:
                await self._context.close()
            except Exception:
                pass

        if self._browser:
            try:
                await self._browser.close()
            except Exception:
                pass

        if self._playwright:
            try:
                await self._playwright.stop()
            except Exception:
                pass

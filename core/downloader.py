"""
core/downloader.py
Manajer download media dan kompresi video menggunakan ffmpeg.

Alur utama:
1. Terima URL media dari scraper
2. Download menggunakan yt-dlp ke folder temporer
3. Cek ukuran file: jika > MAX_SIZE → kompres dengan ffmpeg
4. Kalkulasi bitrate dinamis dengan safety floor untuk mencegah kualitas burik
5. Return path file yang siap dikirim ke Discord
"""

import asyncio
import json
import logging
import re
import os
import random
import subprocess
from pathlib import Path
from typing import Optional, cast, Any

import yt_dlp
from yt_dlp.utils import DownloadError
import gallery_dl.job
import gallery_dl.config
import aiofiles  # Async file I/O untuk mencegah blocking event loop

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# Konstanta Kompresi
# ──────────────────────────────────────────────
# Batas upload Discord dalam bytes (50MB untuk Nitro, 25MB free tier)
MAX_DISCORD_SIZE_BYTES = 50 * 1024 * 1024

# Target ukuran setelah kompresi (48MB) — beri buffer 2MB untuk metadata
TARGET_SIZE_BYTES = 48 * 1024 * 1024

# Safety floor bitrate: jika kalkulasi menghasilkan nilai di bawah ini,
# kunci di nilai minimum agar video tidak pixelated parah.
# Trade-off: ukuran mungkin sedikit melebihi 50MB → fallback ke send_document
MIN_VIDEO_BITRATE_BPS = 300_000  # 300 kbps

# Estimasi bitrate audio dalam stream output ffmpeg
AUDIO_BITRATE_BPS = 128_000  # 128 kbps

SHARED_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"


def _extract_tiktok_image_urls_from_rehydration(data: dict) -> list[str]:
    """Extract image URLs from TikTok rehydration payload without duplication."""
    image_urls: list[str] = []

    def search_images(d) -> None:
        if isinstance(d, dict):
            if "imagePost" in d:
                img_data = d.get("imagePost")
                if isinstance(img_data, dict) and "images" in img_data:
                    for img in img_data["images"]:
                        urls = img.get("imageURL", {}).get("urlList", [])
                        if urls:
                            image_urls.append(urls[0])
                elif isinstance(img_data, list):
                    for item in img_data:
                        if isinstance(item, dict):
                            urls = item.get("imageURL", {}).get("urlList", [])
                            if urls:
                                image_urls.append(urls[0])
                            elif "url" in item:
                                image_urls.append(item["url"])
                        elif isinstance(item, str):
                            image_urls.append(item)
            for value in d.values():
                search_images(value)
        elif isinstance(d, list):
            for item in d:
                search_images(item)

    search_images(data)
    return image_urls


class MediaDownloader:
    """
    Downloader media asinkron dengan kemampuan kompresi ffmpeg.
    """

    def __init__(self, temp_dir: str, delay_min: float = 2.0, delay_max: float = 5.0):
        self.temp_dir = Path(temp_dir)
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        self.delay_min = delay_min
        self.delay_max = delay_max
        # Browser Playwright reusable
        self._playwright = None
        self._browser = None

        try:
            max_mb = int(os.getenv("MAX_FILE_SIZE_MB", "10"))
        except ValueError:
            max_mb = 10

        try:
            target_mb = int(os.getenv("TARGET_FILE_SIZE_MB", "9"))
        except ValueError:
            target_mb = 9

        self.max_file_size_bytes = max_mb * 1024 * 1024
        self.target_file_size_bytes = target_mb * 1024 * 1024

    async def get_browser(self):
        """Inisialisasi lazy browser Playwright Chromium (stealth). Auto-reinitialize jika browser crash/disconnect."""
        if not self._browser or not self._browser.is_connected():
            # FIX: imports moved here once — removed duplicate block that was at original L126-127
            from playwright.async_api import async_playwright
            from scrapers.base import BaseScraper
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
            self._playwright = await async_playwright().start()
            brave_path = BaseScraper.get_brave_path()
            # FIX: use BaseScraper.get_browser_launch_kwargs() instead of manual dict
            # so stealth args stay in sync with all scrapers
            launch_kwargs = BaseScraper.get_browser_launch_kwargs()
            launch_kwargs["headless"] = True
            if brave_path:
                logger.info(f"Downloader menggunakan browser Brave dari: {brave_path}")
                launch_kwargs["executable_path"] = brave_path
            self._browser = await self._playwright.chromium.launch(**launch_kwargs)
        return self._browser

    async def close_browser(self):
        """Menutup browser Playwright reusable saat shutdown."""
        try:
            if self._browser:
                await self._browser.close()
            if self._playwright:
                await self._playwright.stop()
        except Exception as e:
            logger.warning(f"Error saat menutup browser downloader: {e}")
        finally:
            self._browser = None
            self._playwright = None

    async def random_delay(self) -> None:
        """
        Tambahkan delay acak antara request untuk menyamarkan aktivitas bot.
        Jeda ini penting agar pola request tidak terlihat seperti bot oleh server target.
        """
        delay = random.uniform(self.delay_min, self.delay_max)
        logger.debug(f"Random delay: {delay:.1f}s")
        await asyncio.sleep(delay)

    def _parse_netscape_cookies(self, cookies_file: str) -> dict:
        """Parse file cookies format Netscape menjadi dictionary key-value."""
        cookies_dict = {}
        if cookies_file and Path(cookies_file).exists():
            try:
                with open(cookies_file, "r", encoding="utf-8") as f:
                    for line in f:
                        if line.startswith("#") or not line.strip():
                            continue
                        parts = line.strip().split("\t")
                        if len(parts) >= 7:
                            name = parts[5]
                            value = parts[6]
                            cookies_dict[name] = value.strip()
            except Exception as e:
                logger.warning(f"Gagal parse Netscape cookie file: {e}")
        return cookies_dict

    # ──────────────────────────────────────────────
    # Download via yt-dlp
    # ──────────────────────────────────────────────

    async def download_post(
        self,
        post_url: str,
        post_id: str,
        cookies_file: Optional[str] = None,
        is_video: Optional[bool] = None,
        media_urls: Optional[list[str]] = None,
    ) -> tuple[list[Path], str, Optional[str]]:
        """
        Download semua media dari sebuah URL postingan menggunakan yt-dlp.

        Args:
            post_url: URL postingan (bukan URL profil).
            post_id: ID unik postingan untuk penamaan file.
            cookies_file: Path ke file cookies format Netscape (opsional).
            is_video: Flag opsional apakah postingan adalah video.
            media_urls: List URL media langsung (opsional, jika sudah diekstrak scraper).

        Returns:
            Tuple: (List path file yang berhasil didownload, Caption/deskripsi postingan).
        """
        # Intersepsi khusus TikTok Carousel (hanya untuk URL yang secara eksplisit memuat /photo/):
        if "tiktok.com" in post_url and "/photo/" in post_url:
            browser_cookies = []
            image_urls = []
            caption = ""

            try:
                result = await self._extract_tiktok_carousel_urls(post_url, cookies_file)
                if isinstance(result, tuple) and len(result) == 2:
                    image_urls, caption = result
                else:
                    image_urls, caption = [], ""
            except Exception as e:
                logger.error(f"Gagal memproses manual carousel untuk {post_url}: {e}")
                image_urls, caption = [], ""

            if image_urls:
                logger.info(f"TikTok Carousel terdeteksi ({len(image_urls)} foto) — mulai download async paralel...")
                
                cookies_dict = {c["name"]: c["value"] for c in browser_cookies}
                if cookies_file:
                    cookies_dict.update(self._parse_netscape_cookies(cookies_file))

                headers = {
                    "User-Agent": SHARED_USER_AGENT,
                    "Referer": "https://www.tiktok.com/",
                }

                async def download_single(idx, img_url):
                    filename = f"{post_id}_{idx+1:03d}.jpg"
                    return await self.download_direct_url(img_url, filename, headers=headers, cookies=cookies_dict)

                tasks = [download_single(idx, img_url) for idx, img_url in enumerate(image_urls)]
                results = await asyncio.gather(*tasks)
                
                downloaded_files = [path for path in results if path is not None]
                if downloaded_files:
                    return downloaded_files, caption, None

        # Intersepsi langsung Twitter/X photo post atau pre-extracted media_urls
        if ("twitter.com" in post_url or "x.com" in post_url) and media_urls:
            logger.info(f"Twitter direct media URLs terdeteksi ({len(media_urls)} item) — bypass yt-dlp...")
            headers = {"User-Agent": SHARED_USER_AGENT}
            async def _dl_tw(idx, u):
                ext = "jpg"
                if ".png" in u.lower():
                    ext = "png"
                elif ".mp4" in u.lower():
                    ext = "mp4"
                fname = f"{post_id}_{idx+1:03d}.{ext}"
                return await self.download_direct_url(u, fname, headers=headers)
            tw_results = await asyncio.gather(*[_dl_tw(i, u) for i, u in enumerate(media_urls)])
            tw_files = [p for p in tw_results if p is not None]
            if tw_files:
                return tw_files, "", None

        output_template = str(self.temp_dir / f"{post_id}_%(autonumber)s.%(ext)s")

        ydl_opts = {
            "outtmpl": output_template,
            "format": "best[ext=mp4]/bestvideo+bestaudio/best",
            "quiet": True,
            "no_warnings": True,
            "nooverwrites": True,
            "retries": 3,
            "fragment_retries": 3,
        }

        if cookies_file and Path(cookies_file).exists():
            ydl_opts["cookiefile"] = cookies_file
            logger.debug(f"yt-dlp menggunakan cookies: {cookies_file}")

        if "tiktok.com" in post_url:
            ydl_opts["http_headers"] = {
                "User-Agent": SHARED_USER_AGENT,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": "https://www.tiktok.com/",
            }
            logger.debug("Suntikkan headers & User-Agent anti-403 untuk TikTok")

        loop = asyncio.get_running_loop()

        # Deteksi awal apakah ini post gambar Twitter /photo/ atau Instagram photo post (/p/)
        is_twitter_photo = ("twitter.com" in post_url or "x.com" in post_url) and ("/photo/" in post_url)
        is_instagram_photo = ("instagram.com" in post_url) and ("/p/" in post_url) and (is_video is not True)
        # FIX: yt-dlp throws `Unsupported URL` for every TikTok /photo/ URL \u2014 no recovery path.
        # Bypass yt-dlp entirely for photo mode and go straight to carousel extractors.
        is_tiktok_photo = "tiktok.com" in post_url and "/photo/" in post_url
        download_failed = False
        yt_error = ""

        downloaded_files = []
        real_caption = ""

        ytdl_timestamp = None
        if is_twitter_photo:
            download_failed = True
        elif is_tiktok_photo:
            # TikTok /photo/ \u2014 yt-dlp always fails with `Unsupported URL` on this path.
            # Skip yt-dlp entirely: try lightweight rehydration first, then Playwright.
            logger.info(f"TikTok photo post terdeteksi (/photo/) \u2014 bypass yt-dlp langsung ke carousel extractor...")
            try:
                result = await self._extract_tiktok_carousel_urls(post_url, cookies_file)
                image_urls, fallback_cap = result if isinstance(result, tuple) and len(result) == 2 else ([], "")
            except Exception:
                image_urls, fallback_cap = [], ""

            if image_urls:
                logger.info(f"TikTok /photo/ lightweight extractor: {len(image_urls)} foto ditemukan")
                cookies_dict = self._parse_netscape_cookies(cookies_file) if cookies_file else {}
                tk_headers = {"User-Agent": SHARED_USER_AGENT, "Referer": "https://www.tiktok.com/"}
                async def _dl_photo_bypass(idx, img_url):
                    fname = f"{post_id}_{idx+1:03d}.jpg"
                    return await self.download_direct_url(img_url, fname, headers=tk_headers, cookies=cookies_dict)
                ph_results = await asyncio.gather(*[_dl_photo_bypass(i, u) for i, u in enumerate(image_urls)])
                downloaded_files = [p for p in ph_results if p is not None]
                if fallback_cap:
                    real_caption = fallback_cap

            if not downloaded_files:
                # Lightweight returned nothing \u2014 escalate to Playwright carousel (full browser)
                logger.warning("TikTok /photo/ lightweight kosong, mencoba Playwright carousel fallback...")
                image_urls_pw, browser_caption_pw, browser_cookies_pw = await self._extract_tiktok_carousel_via_browser(post_url, cookies_file)
                if image_urls_pw:
                    logger.info(f"TikTok /photo/ Playwright carousel: {len(image_urls_pw)} foto ditemukan")
                    cookies_dict = {c["name"]: c["value"] for c in browser_cookies_pw}
                    if cookies_file:
                        cookies_dict.update(self._parse_netscape_cookies(cookies_file))
                    pw_headers = {"User-Agent": SHARED_USER_AGENT, "Referer": "https://www.tiktok.com/"}
                    async def _dl_photo_pw(idx, img_url):
                        fname = f"{post_id}_{idx+1:03d}.jpg"
                        return await self.download_direct_url(img_url, fname, headers=pw_headers, cookies=cookies_dict)
                    pw_results = await asyncio.gather(*[_dl_photo_pw(i, u) for i, u in enumerate(image_urls_pw)])
                    downloaded_files = [p for p in pw_results if p is not None]
                    if browser_caption_pw:
                        real_caption = browser_caption_pw
                else:
                    logger.error(f"Semua extractor TikTok /photo/ habis untuk {post_url}")
        elif is_instagram_photo:
            # Bypass yt-dlp langsung ke gallery-dl secara async untuk Instagram photo (/p/) jika bukan video

            logger.info(f"Instagram photo post terdeteksi (/p/) — bypass yt-dlp, langsung ke gallery-dl...")
            gdl_files = await self._run_gallery_dl_async(post_url, cookies_file or "")
            if gdl_files:
                return gdl_files, "", None
            logger.warning("gallery-dl tidak menghasilkan file untuk /p/, fallback ke yt-dlp...")
            try:
                downloaded_files, real_caption, ytdl_timestamp = await loop.run_in_executor(
                    None, self._run_ytdlp, post_url, ydl_opts
                )
            except Exception as e:
                yt_error = str(e)
                download_failed = True
        else:
            try:
                downloaded_files, real_caption, ytdl_timestamp = await loop.run_in_executor(
                    None, self._run_ytdlp, post_url, ydl_opts
                )

                # NOTE: /photo/ URLs no longer reach this branch \u2014 is_tiktok_photo bypasses yt-dlp above.
                # Audio-discard guard removed: dead code eliminated.

                if not downloaded_files and ("twitter.com" in post_url or "x.com" in post_url):
                    download_failed = True
                elif not downloaded_files and "tiktok.com" in post_url:
                    logger.warning("yt-dlp tidak menghasilkan file TikTok, mencoba lightweight carousel extractor...")
                    try:
                        result = await self._extract_tiktok_carousel_urls(post_url, cookies_file)
                        image_urls, fallback_cap = result if isinstance(result, tuple) and len(result) == 2 else ([], "")
                    except Exception:
                        image_urls, fallback_cap = [], ""

                    if image_urls:
                        logger.info(f"Carousel lightweight fallback: {len(image_urls)} foto ditemukan")
                        cookies_dict = self._parse_netscape_cookies(cookies_file) if cookies_file else {}
                        headers = {"User-Agent": SHARED_USER_AGENT, "Referer": "https://www.tiktok.com/"}
                        async def _dl_img_fb(idx, img_url):
                            fname = f"{post_id}_{idx+1:03d}.jpg"
                            return await self.download_direct_url(img_url, fname, headers=headers, cookies=cookies_dict)
                        fb_results = await asyncio.gather(*[_dl_img_fb(i, u) for i, u in enumerate(image_urls)])
                        downloaded_files = [p for p in fb_results if p is not None]
                        if fallback_cap:
                            real_caption = fallback_cap

                    if not downloaded_files:
                        logger.warning("Lightweight carousel fallback kosong, mencoba Playwright carousel fallback...")
                        image_urls, browser_caption, browser_cookies = await self._extract_tiktok_carousel_via_browser(post_url, cookies_file)
                        if image_urls:
                            logger.info(f"Carousel browser fallback: {len(image_urls)} foto ditemukan")
                            cookies_dict = {c["name"]: c["value"] for c in browser_cookies}
                            if cookies_file:
                                cookies_dict.update(self._parse_netscape_cookies(cookies_file))
                            img_headers = {"User-Agent": SHARED_USER_AGENT, "Referer": "https://www.tiktok.com/"}
                            async def _dl_img_fb_pw(idx, img_url):
                                fname = f"{post_id}_{idx+1:03d}.jpg"
                                return await self.download_direct_url(img_url, fname, headers=img_headers, cookies=cookies_dict)
                            fb_results = await asyncio.gather(*[_dl_img_fb_pw(i, u) for i, u in enumerate(image_urls)])
                            downloaded_files = [p for p in fb_results if p is not None]
                            if browser_caption:
                                real_caption = browser_caption
                        else:
                            logger.warning("Carousel fallback kosong, mencoba Playwright video fallback...")
                            video_url, browser_caption, browser_cookies = await self._extract_tiktok_video_via_browser(post_url, cookies_file)
                            if video_url:
                                # FIX: route ke authenticated CDN downloader, bukan generic download_direct_url
                                out_path = await self._download_tiktok_cdn_video(video_url, post_id, browser_cookies, cookies_file)
                                if out_path:
                                    downloaded_files = [out_path]
                                    real_caption = browser_caption
            except Exception as e:
                # FIX: bind original_err immediately so nested awaits cannot rebind `e`
                # inside inner except blocks — `raise e` at the bottom then re-raises the
                # correct original yt-dlp error regardless of how many inner calls run.
                original_err = e
                yt_error = str(e)
                if "twitter.com" in post_url or "x.com" in post_url:
                    logger.warning(f"yt-dlp gagal download, mencoba Twitter fallback: {yt_error}")
                    download_failed = True
                elif "tiktok.com" in post_url:
                    logger.warning(f"yt-dlp gagal download TikTok: {yt_error}. Mencoba Carousel Fallback terlebih dahulu...")

                    # FIX: Carousel first, then video — photo posts have no <video> element
                    # Step 1: Lightweight rehydration scraper (no browser, fast)
                    try:
                        result = await self._extract_tiktok_carousel_urls(post_url, cookies_file)
                        carousel_urls, carousel_cap = result if isinstance(result, tuple) and len(result) == 2 else ([], "")
                    except Exception:
                        carousel_urls, carousel_cap = [], ""

                    if carousel_urls:
                        logger.info(f"Carousel lightweight fallback (exc path): {len(carousel_urls)} foto ditemukan")
                        cookies_dict = self._parse_netscape_cookies(cookies_file) if cookies_file else {}
                        c_headers = {"User-Agent": SHARED_USER_AGENT, "Referer": "https://www.tiktok.com/"}
                        async def _dl_c_exc(idx, img_url):
                            fname = f"{post_id}_{idx+1:03d}.jpg"
                            return await self.download_direct_url(img_url, fname, headers=c_headers, cookies=cookies_dict)
                        c_results = await asyncio.gather(*[_dl_c_exc(i, u) for i, u in enumerate(carousel_urls)])
                        downloaded_files = [p for p in c_results if p is not None]
                        if carousel_cap:
                            real_caption = carousel_cap

                    if not downloaded_files:
                        # Step 2: Playwright carousel (full browser, handles JS-rendered state)
                        logger.warning("Lightweight carousel kosong, mencoba Playwright carousel fallback (exc path)...")
                        image_urls_pw, browser_caption_pw, browser_cookies_pw = await self._extract_tiktok_carousel_via_browser(post_url, cookies_file)
                        if image_urls_pw:
                            logger.info(f"Playwright carousel fallback (exc path): {len(image_urls_pw)} foto ditemukan")
                            cookies_dict = {c["name"]: c["value"] for c in browser_cookies_pw}
                            if cookies_file:
                                cookies_dict.update(self._parse_netscape_cookies(cookies_file))
                            pw_headers = {"User-Agent": SHARED_USER_AGENT, "Referer": "https://www.tiktok.com/"}
                            async def _dl_pw_exc(idx, img_url):
                                fname = f"{post_id}_{idx+1:03d}.jpg"
                                return await self.download_direct_url(img_url, fname, headers=pw_headers, cookies=cookies_dict)
                            pw_results = await asyncio.gather(*[_dl_pw_exc(i, u) for i, u in enumerate(image_urls_pw)])
                            downloaded_files = [p for p in pw_results if p is not None]
                            if browser_caption_pw:
                                real_caption = browser_caption_pw

                    if not downloaded_files:
                        # Step 3: Playwright video — only if carousel returned nothing
                        logger.warning("Carousel fallback kosong, mencoba Playwright video fallback (exc path)...")
                        video_url, browser_caption, browser_cookies = await self._extract_tiktok_video_via_browser(post_url, cookies_file)
                        if video_url:
                            # FIX: route ke authenticated CDN downloader, bukan generic download_direct_url
                            out_path = await self._download_tiktok_cdn_video(video_url, post_id, browser_cookies, cookies_file)
                            if out_path:
                                downloaded_files = [out_path]
                                real_caption = browser_caption
                            else:
                                logger.error("Playwright video fallback download gagal.")
                                # FIX: raise with chained context — original_err preserves yt-dlp root cause
                                raise RuntimeError(f"TikTok CDN download gagal untuk {post_url}") from original_err
                        else:
                            logger.error("Semua fallback TikTok habis — tidak ada media yang bisa diunduh.")
                            # FIX: same — chain from original yt-dlp error, not raw `raise e`
                            raise RuntimeError(f"Semua fallback TikTok habis untuk {post_url}") from original_err
                elif "No video could be found" in yt_error or "No video formats found" in yt_error:
                    logger.warning(f"yt-dlp gagal download (no formats found): {yt_error}")
                    download_failed = True
                else:
                    logger.error(f"yt-dlp gagal download dengan error fatal: {yt_error}")


        # Twitter Image Fallback Handler
        if download_failed and ("twitter.com" in post_url or "x.com" in post_url):
            logger.info(f"[📥 DOWN  ] Menjalankan Twitter Image Fallback Handler untuk: {post_url}")
            import httpx
            import re
            import aiofiles

            auth_token = os.getenv("TWITTER_AUTH_TOKEN")
            ct0 = os.getenv("TWITTER_CT0")
            if cookies_file and Path(cookies_file).exists():
                parsed_c = self._parse_netscape_cookies(cookies_file)
                if not auth_token and "auth_token" in parsed_c:
                    auth_token = parsed_c["auth_token"]
                if not ct0 and "ct0" in parsed_c:
                    ct0 = parsed_c["ct0"]

            # Optimization: reuse existing browser instance from self.get_browser() instead of spawning new async_playwright() instances
            browser = await self.get_browser()
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                viewport={"width": 1280, "height": 900},
                locale="id-ID"
            )

            if auth_token or ct0:
                await context.add_cookies([
                    {"name": "auth_token", "value": auth_token.strip(), "domain": ".x.com", "path": "/"},
                    {"name": "ct0", "value": ct0.strip(), "domain": ".x.com", "path": "/"},
                    {"name": "auth_token", "value": auth_token.strip(), "domain": ".twitter.com", "path": "/"},
                    {"name": "ct0", "value": ct0.strip(), "domain": ".twitter.com", "path": "/"}
                ])

            page = await context.new_page()
            try:
                await page.goto(post_url, wait_until="domcontentloaded", timeout=25000)
                try:
                    await page.wait_for_selector('article, [data-testid="tweet"], img[src*="pbs.twimg.com/media/"]', timeout=12000)
                except Exception:
                    pass
                await asyncio.sleep(2.5) # Beri jeda render gambar

                # Ambil caption/teks tweet
                try:
                    tweet_text_el = await page.query_selector('[data-testid="tweetText"]')
                    if tweet_text_el:
                        real_caption = (await tweet_text_el.inner_text()).strip()
                except Exception:
                    pass

                # Ambil timestamp tweet
                try:
                    time_el = await page.query_selector('article time, time')
                    if time_el:
                        ytdl_timestamp = await time_el.get_attribute("datetime")
                except Exception as e:
                    logger.warning(f"Gagal mengambil timestamp di Twitter fallback: {e}")

                # Kumpulkan semua URL gambar pbs.twimg.com dari tweet
                img_srcs = await page.evaluate("""() => {
                    const imgs = document.querySelectorAll('img[src*="pbs.twimg.com/media/"], [data-testid="tweetPhoto"] img, article img[src*="twimg"]');
                    return Array.from(imgs)
                        .map(img => img.src)
                        .filter(src => src && src.includes('pbs.twimg.com/media/') && !src.includes('profile_images') && !src.includes('emoji'));
                }""")
                unique_images = []
                seen_urls = set()

                for src in img_srcs:
                    if src:
                        clean_src = src.split("?")[0]
                        if clean_src not in seen_urls:
                            seen_urls.add(clean_src)
                            # Gunakan format kualitas original
                            if "format=" in src:
                                high_res = re.sub(r"name=\w+", "name=orig", src)
                                if "name=" not in high_res:
                                    high_res += "&name=orig"
                            else:
                                high_res = f"{clean_src}?format=jpg&name=orig"
                            unique_images.append(high_res)

            finally:
                await context.close()

            # Mulai unduh asinkron menggunakan httpx dengan session cookies
            if unique_images:
                headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                }
                cookie_parts = []
                if auth_token:
                    cookie_parts.append(f"auth_token={auth_token.strip()}")
                if ct0:
                    cookie_parts.append(f"ct0={ct0.strip()}")
                if cookie_parts:
                    headers["Cookie"] = "; ".join(cookie_parts)
                if ct0:
                    headers["x-csrf-token"] = ct0.strip()

                async with httpx.AsyncClient(headers=headers, timeout=30.0) as client:
                    for idx, img_url in enumerate(unique_images):
                        ext = "jpg"
                        if "format=" in img_url:
                            fmt_match = re.search(r"format=(\w+)", img_url)
                            if fmt_match:
                                ext = fmt_match.group(1)

                        filename = f"{post_id}_{idx+1:03d}.{ext}"
                        out_path = self.temp_dir / filename

                        try:
                            res = await client.get(img_url)
                            if res.status_code == 200:
                                async with aiofiles.open(out_path, "wb") as f:
                                    await f.write(res.content)
                                downloaded_files.append(out_path)
                                logger.info(f"[📥 DOWN  ] Berhasil mengunduh gambar Twitter: {filename}")
                            else:
                                logger.warning(f"Gagal unduh gambar Twitter {img_url}: status {res.status_code}")
                        except Exception as dl_err:
                            logger.error(f"Error download gambar Twitter {img_url}: {dl_err}")

        return downloaded_files, real_caption, ytdl_timestamp

    async def _extract_tiktok_carousel_urls(self, url: str, cookies_file: Optional[str] = None) -> tuple[list[str], str]:
        """Ekstrak list URL gambar carousel dan caption dari webpage TikTok secara async."""
        import httpx

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }

        # Load cookies dari Netscape format (jika dikirim oleh scraper) untuk HTTPX
        cookies_dict = {}
        if cookies_file and Path(cookies_file).exists():
            try:
                # Netscape cookie format parser sederhana asinkron
                async with aiofiles.open(cookies_file, "r", encoding="utf-8") as f:
                    async for line in f:
                        if line.startswith("#") or not line.strip():
                            continue
                        parts = line.strip().split("\t")
                        if len(parts) >= 7:
                            domain, _, path, _, _, name, value = parts[:7]
                            cookies_dict[name] = value
                logger.debug(f"Parser memuat {len(cookies_dict)} cookies untuk HTTPX dari {cookies_file}")
            except Exception as e:
                logger.warning(f"Gagal parse Netscape cookie file untuk HTTPX: {e}")

        try:
            async with httpx.AsyncClient(
                headers=headers,
                cookies=cookies_dict if cookies_dict else None,
                follow_redirects=True,
                timeout=15
            ) as client:
                resp = await client.get(url)
                if resp.status_code != 200:
                    return [], ""
                html_content = resp.text

            # Cari script __UNIVERSAL_DATA_FOR_REHYDRATION__ (state rehydration data modern TikTok)
            match = re.search(
                r'<script id="__UNIVERSAL_DATA_FOR_REHYDRATION__"[^>]*>(.*?)</script>',
                html_content,
            )
            if not match:
                # Fallback: Cari SIGI_STATE (state rehydration data lama TikTok)
                match = re.search(
                    r'<script id="SIGI_STATE"[^>]*>(.*?)</script>', html_content
                )
                if not match:
                    return [], ""

            data = json.loads(match.group(1))
            image_urls = _extract_tiktok_image_urls_from_rehydration(data)

            # Ekstrak deskripsi/caption postingan secara aman dari rehydration data
            caption = ""
            try:
                default_scope = data.get("__DEFAULT_SCOPE__", {})
                # Cek flat key first
                video_detail = default_scope.get("webapp.video-detail", {}) or default_scope.get("webapp.videoDetail", {})
                if not video_detail:
                    # Fallback ke nested key
                    webapp = default_scope.get("webapp", {})
                    video_detail = webapp.get("video-detail", {}) or webapp.get("videoDetail", {})
                item_struct = video_detail.get("itemInfo", {}).get("itemStruct", {})
                caption = item_struct.get("desc") or ""
            except Exception as e:
                logger.debug(f"Gagal ekstrak deskripsi dari rehydration data: {e}")

            return image_urls, caption
        except Exception as e:
            logger.error(f"Gagal mengekstrak carousel TikTok secara manual: {e}")
            return [], ""

    async def _extract_tiktok_carousel_via_browser(
        self, post_url: str, cookies_file: Optional[str] = None
    ) -> tuple[list[str], str, list[dict]]:
        """
        Playwright Fallback Extractor khusus TikTok Carousel.
        Membuka halaman postingan TikTok, menanti pemuatan, dan mengekstrak data rehydration JSON secara dinamis.
        """
        import os
        import json

        image_urls = []
        caption = ""
        browser_cookies = []

        tiktok_session_id = os.getenv("TIKTOK_SESSION_ID")

        try:
            browser = await self.get_browser()
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                viewport={"width": 1280, "height": 900},
                locale="en-US"
            )

            # Inject cookies jika ada
            if tiktok_session_id:
                await context.add_cookies([
                    {"name": "sessionid", "value": tiktok_session_id.strip(), "domain": ".tiktok.com", "path": "/"}
                ])

            page = await context.new_page()
            await page.goto(post_url, wait_until="domcontentloaded", timeout=30000)

            # FIX: reduced from 15000ms → 2000ms; rehydration elements are often absent
            # on newer TikTok pages — don't burn 15s waiting for a deprecated element
            rehydration_found = False
            try:
                await page.wait_for_selector("#__UNIVERSAL_DATA_FOR_REHYDRATION__, #SIGI_STATE", timeout=2000)
                rehydration_found = True
            except Exception:
                logger.debug("Rehydration element tidak ditemukan dalam 2s — lanjut ke DOM img fallback")

            # Path A: Try rehydration JSON first (fast, structured)
            if rehydration_found:
                json_content = await page.evaluate("""() => {
                    const el = document.getElementById('__UNIVERSAL_DATA_FOR_REHYDRATION__');
                    if (el) return el.textContent;
                    const sigi = document.getElementById('SIGI_STATE');
                    if (sigi) return sigi.textContent;
                    return null;
                }""")

                if json_content:
                    try:
                        raw_data = json.loads(json_content)
                        image_urls = _extract_tiktok_image_urls_from_rehydration(raw_data)

                        try:
                            default_scope = raw_data.get("__DEFAULT_SCOPE__", {})
                            video_detail = default_scope.get("webapp.video-detail", {}) or default_scope.get("webapp.videoDetail", {})
                            if not video_detail:
                                webapp = default_scope.get("webapp", {})
                                video_detail = webapp.get("video-detail", {}) or webapp.get("videoDetail", {})
                            item_struct = video_detail.get("itemInfo", {}).get("itemStruct", {})
                            caption = item_struct.get("desc") or ""
                        except Exception:
                            pass
                    except Exception as json_err:
                        logger.warning(f"Gagal memparsing rehydration JSON TikTok: {json_err}")

            # Path B: Direct DOM img-tag scraper — fires immediately when rehydration absent
            # Targets photomode CDN images rendered directly in the page
            if not image_urls:
                logger.debug("Rehydration kosong — scraping img tag photomode dari DOM...")
                image_urls = await page.evaluate("""() => {
                    const results = new Set();
                    // Selector 1: photomode swiper images
                    document.querySelectorAll('img[src*="photomode"]').forEach(el => {
                        if (el.src && !el.src.includes('avatar') && !el.src.includes('cover')) {
                            results.add(el.src.split('?')[0] + '?' + el.src.split('?')[1]);
                        }
                    });
                    // Selector 2: tplv-photomode srcset images (highest res)
                    document.querySelectorAll('img[srcset*="photomode"]').forEach(el => {
                        const srcset = el.srcset || '';
                        const parts = srcset.split(',');
                        if (parts.length) {
                            const last = parts[parts.length - 1].trim().split(' ')[0];
                            if (last) results.add(last);
                        }
                    });
                    // Selector 3: tiktokcdn img tags inside swiper slides
                    document.querySelectorAll('.swiper-slide img, [class*="PhotoSwiper"] img').forEach(el => {
                        if (el.src && el.src.startsWith('http')) results.add(el.src);
                    });
                    return Array.from(results);
                }""")
                if image_urls:
                    logger.info(f"DOM img fallback: {len(image_urls)} foto ditemukan dari img tag")

            browser_cookies = await context.cookies()
            await context.close()
        except Exception as e:
            logger.warning(f"Browser fallback extractor failed: {e}")
            browser_cookies = []

        return image_urls, caption, cast(list[dict], browser_cookies)

    async def _extract_tiktok_video_via_browser(
        self, post_url: str, cookies_file: Optional[str] = None
    ) -> tuple[Optional[str], str, list[dict]]:
        """
        Playwright Fallback Extractor khusus TikTok Video.
        Membuka halaman postingan TikTok, menanti pemuatan tag video, dan mengekstrak direct CDN URL serta caption.
        """
        video_url = None
        caption = ""
        browser_cookies = []

        tiktok_session_id = os.getenv("TIKTOK_SESSION_ID")

        try:
            browser = await self.get_browser()
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                viewport={"width": 1280, "height": 900},
                locale="en-US"
            )

            # Inject cookies jika ada
            if tiktok_session_id:
                await context.add_cookies([
                    {"name": "sessionid", "value": tiktok_session_id.strip(), "domain": ".tiktok.com", "path": "/"}
                ])

            page = await context.new_page()
            
            # Masuk ke halaman video
            await page.goto(post_url, wait_until="domcontentloaded", timeout=30000)
            
            # Tunggu elemen video atau rehydration data muncul
            try:
                await page.wait_for_selector("video, #__UNIVERSAL_DATA_FOR_REHYDRATION__, #SIGI_STATE", timeout=15000)
            except Exception:
                logger.warning("Tag video atau rehydration tidak terdeteksi oleh selector Playwright.")

            # Ambil data rehydration JSON secara dinamis
            json_content = await page.evaluate("""() => {
                const el = document.getElementById('__UNIVERSAL_DATA_FOR_REHYDRATION__');
                if (el) return el.textContent;
                const sigi = document.getElementById('SIGI_STATE');
                if (sigi) return sigi.textContent;
                return null;
            }""")

            if json_content:
                try:
                    raw_data = json.loads(json_content)
                    # Ekstrak video_url & caption secara aman
                    try:
                        default_scope = raw_data.get("__DEFAULT_SCOPE__", {})
                        video_detail = default_scope.get("webapp.video-detail", {}) or default_scope.get("webapp.videoDetail", {})
                        if not video_detail:
                            webapp = default_scope.get("webapp", {})
                            video_detail = webapp.get("video-detail", {}) or webapp.get("videoDetail", {})
                        item_struct = video_detail.get("itemInfo", {}).get("itemStruct", {})
                        
                        # Cari playAddr
                        video_info = item_struct.get("video", {})
                        video_url = video_info.get("playAddr") or video_info.get("downloadAddr")
                        caption = item_struct.get("desc") or ""
                        if video_url:
                            logger.info("Berhasil mengekstrak direct video CDN URL dari rehydration JSON")
                    except Exception as parse_err:
                        logger.warning(f"Gagal memparsing detail video dari JSON: {parse_err}")
                except Exception as json_err:
                    logger.warning(f"Gagal memparsing rehydration JSON untuk video TikTok: {json_err}")

            # Fallback: Ambil src dari video tag (pastikan bukan blob URL)
            if not video_url:
                video_url = await page.evaluate("""() => {
                    const video = document.querySelector('video');
                    if (video && video.src && !video.src.startsWith('blob:')) {
                        return video.src;
                    }
                    return null;
                }""")
                if video_url:
                    logger.info("Berhasil mengekstrak video URL dari tag video (non-blob)")

            browser_cookies = await context.cookies()
            await context.close()
        except Exception as e:
            logger.warning(f"Browser video fallback extractor failed: {e}")
            browser_cookies = []

        return video_url, caption, cast(list[dict], browser_cookies)

    async def _download_tiktok_cdn_video(
        self,
        video_url: str,
        post_id: str,
        browser_cookies: list[dict],
        cookies_file: Optional[str] = None,
    ) -> Optional[Path]:
        """
        Download TikTok CDN video URL dengan full session cookie auth untuk bypass HTTP 403.

        TikTok CDN URLs mengandung signed token yang divalidasi bersama session cookies.
        Mengirim GET tanpa cookies → 403 Forbidden. Fix: bangun cookies_dict dari tiga sumber:
          1. browser_cookies (dari Playwright context.cookies() setelah page load)
          2. Netscape cookies file (sessions/tiktok_cookies.txt)
          3. TIKTOK_SESSION_ID env var sebagai fallback minimal

        Kenapa tidak pakai download_direct_url?
        download_direct_url adalah generic downloader. Untuk TikTok CDN, kita butuh
        full-cookie-string di header Cookie (bukan hanya dict) agar token signature match.
        """
        import httpx

        output_path = self.temp_dir / f"{post_id}_video.mp4"
        # FIX: ensure temp_dir exists — can be wiped by /reset_bot mid-session
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # ── Bangun cookies_dict dari semua sumber tersedia ──────────────────
        cookies_dict: dict = {}

        # Sumber 1: Netscape cookie file (paling lengkap — hasil inject + rotate browser)
        netscape_path = Path("sessions/tiktok_cookies.txt")
        if cookies_file and Path(cookies_file).exists():
            netscape_path = Path(cookies_file)
        if netscape_path.exists():
            cookies_dict.update(self._parse_netscape_cookies(str(netscape_path)))
            logger.debug(f"TikTok CDN: {len(cookies_dict)} cookies dari Netscape file")

        # Sumber 2: Playwright context cookies (termasuk cookies yang di-set saat JS render)
        # Overwrite Netscape values jika ada yang lebih fresh dari browser session
        for c in browser_cookies:
            name = c.get("name", "")
            value = c.get("value", "")
            if name and value:
                cookies_dict[name] = value
        logger.debug(f"TikTok CDN: {len(cookies_dict)} cookies setelah merge browser session")

        # Sumber 3: Env var fallback — pastikan sessionid selalu ada
        tiktok_session_id = os.getenv("TIKTOK_SESSION_ID", "").strip()
        if tiktok_session_id and "sessionid" not in cookies_dict:
            cookies_dict["sessionid"] = tiktok_session_id
            logger.debug("TikTok CDN: sessionid diisi dari TIKTOK_SESSION_ID env var")

        # ── Headers wajib untuk CDN signed URL ─────────────────────────────
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Referer": "https://www.tiktok.com/",
            "Origin": "https://www.tiktok.com",
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "Range": "bytes=0-",  # CDN signed URLs sometimes require Range header
        }

        # ── Coba curl_cffi dulu (browser impersonation bypass WAF) ──────────
        try:
            from curl_cffi import requests as curl_requests  # pyright: ignore[reportMissingImports]
            logger.info(f"TikTok CDN: Mengunduh via curl_cffi chrome impersonation...")
            async with curl_requests.AsyncSession(impersonate="chrome124") as session:
                resp = await session.get(
                    video_url,
                    headers=headers,
                    cookies=cookies_dict,
                    timeout=120,
                    allow_redirects=True,
                )
                if resp.status_code in (200, 206):
                    # FIX: stream chunk-write instead of resp.content (loads full video to RAM).
                    # Large TikTok videos (100+ MB) would cause OOM with in-memory load.
                    async with aiofiles.open(output_path, "wb") as f:
                        async for chunk in resp.aiter_bytes(8192):
                            await f.write(chunk)
                    logger.info(f"TikTok CDN: Download sukses via curl_cffi ({output_path.stat().st_size // 1024} KB)")
                    return output_path
                else:
                    logger.warning(f"TikTok CDN curl_cffi: HTTP {resp.status_code} — fallback ke httpx")
        except (ImportError, ModuleNotFoundError):
            logger.debug("curl_cffi tidak tersedia, menggunakan httpx")
        except Exception as ce:
            logger.warning(f"TikTok CDN curl_cffi error: {ce} — fallback ke httpx")

        # ── Fallback: httpx dengan full cookie string di header ─────────────
        try:
            # Build Cookie header string — lebih reliable daripada cookies= dict untuk CDN
            cookie_str = "; ".join(f"{k}={v}" for k, v in cookies_dict.items())
            if cookie_str:
                headers["Cookie"] = cookie_str

            logger.info(f"TikTok CDN: Mengunduh via httpx dengan {len(cookies_dict)} cookies...")
            async with httpx.AsyncClient(
                headers=headers,
                timeout=120.0,
                follow_redirects=True,
            ) as client:
                # FIX: use streaming response to avoid loading entire video into RAM.
                # resp.content on a 200 MB video would spike memory by 200 MB instantly.
                async with client.stream("GET", video_url) as resp:
                    if resp.status_code in (200, 206):
                        async with aiofiles.open(output_path, "wb") as f:
                            async for chunk in resp.aiter_bytes(8192):
                                await f.write(chunk)
                        logger.info(f"TikTok CDN: Download sukses via httpx ({output_path.stat().st_size // 1024} KB)")
                        return output_path
                    else:
                        logger.error(f"TikTok CDN httpx: HTTP {resp.status_code} — cookies tidak valid atau URL expired")
                        return None
        except Exception as he:
            logger.error(f"TikTok CDN httpx error: {he}")
            return None

    def _run_ytdlp(self, url: str, opts: dict) -> tuple[list[Path], str, Optional[str]]:
        """
        Fungsi sinkron yang menjalankan yt-dlp (dipanggil via executor).

        Strategi download multi-tipe:
        - TikTok Carousel (foto): deteksi key 'images' di metadata → download foto via httpx
        - Instagram video/Reels: yt-dlp langsung
        - Instagram foto: yt-dlp gagal 'No video formats found' → fallback ke gallery-dl

        Kenapa cek 'images' SEBELUM download?
        TikTok carousel tidak punya stream video. yt-dlp akan download audio latarnya (.m4a)
        sebagai satu-satunya media yang tersedia, bukan foto-fotonya.
        Solusinya: extract_info dulu tanpa download, cek 'images', lalu download foto manual.
        """
        downloaded = []
        caption = ""
        timestamp = None
        try:
            # ── Tahap 1: Peek metadata TANPA download ──────────────────────
            # Ini diperlukan untuk deteksi TikTok carousel sebelum yt-dlp
            # salah download audio latar sebagai pengganti foto.
            peek_opts = {**opts, "skip_download": True}
            with yt_dlp.YoutubeDL(peek_opts) as ydl:  # pyright: ignore[reportArgumentType]
                info = ydl.extract_info(url, download=False)
                if info:
                    caption = info.get("description") or info.get("title") or ""
                    upload_timestamp = info.get("timestamp")
                    if upload_timestamp:
                        from datetime import datetime, timezone
                        timestamp = datetime.fromtimestamp(upload_timestamp, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                    else:
                        upload_date = info.get("upload_date")
                        if upload_date:
                            try:
                                from datetime import datetime, timezone
                                dt = datetime.strptime(upload_date, "%Y%m%d").replace(tzinfo=timezone.utc)
                                timestamp = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
                            except Exception:
                                pass

            # ── Tahap 2: Cek apakah ini TikTok Carousel (post foto) ────────
            # yt-dlp menyimpan daftar URL foto di key 'images' untuk TikTok photo post.
            # Jika key ini ada atau URL mengandung "/photo/" → ini carousel foto.
            images = info.get("images") if info else None
            
            # ── Tahap 2b: Carousel fallback — deteksi dari format list untuk /photo/ URL ───────
            if info and "tiktok.com" in url and "/photo/" in url and not images:
                formats = info.get("formats") or []
                has_video_format = any(
                    f.get("vcodec") and f.get("vcodec") != "none"
                    for f in formats
                )
                if formats and not has_video_format:
                    logger.warning(
                        "TikTok /photo/: semua format audio-only -> kemungkinan carousel tanpa mp4. "
                        "Return empty agar Playwright carousel fallback dipakai."
                    )
                    return [], caption, timestamp

            if images or (info and "/photo/" in url):
                if images:
                    logger.info(
                        f"TikTok Carousel terdeteksi ({len(images)} foto) — download foto..."
                    )
                    downloaded = self._download_tiktok_images(
                        info, str(opts.get("outtmpl", ""))  # pyright: ignore[reportArgumentType]
                    )
                    return downloaded, caption, timestamp
                else:
                    logger.warning(
                        f"TikTok /photo/ terdeteksi tapi metadata 'images' kosong untuk {url}!"
                    )

            # ── Tahap 3: Download normal untuk video/audio ─────────────────
            with yt_dlp.YoutubeDL(opts) as ydl:  # pyright: ignore[reportArgumentType]
                info = ydl.extract_info(url, download=True)
                if info:
                    # Ambil caption terbaru pasca download (siapa tahu lebih lengkap)
                    caption = info.get("description") or info.get("title") or caption
                    if not timestamp:
                        upload_timestamp = info.get("timestamp")
                        if upload_timestamp:
                            from datetime import datetime, timezone
                            timestamp = datetime.fromtimestamp(upload_timestamp, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                        else:
                            upload_date = info.get("upload_date")
                            if upload_date:
                                try:
                                    from datetime import datetime, timezone
                                    dt = datetime.strptime(upload_date, "%Y%m%d").replace(tzinfo=timezone.utc)
                                    timestamp = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
                                except Exception:
                                    pass

                # Ekstrak path file yang didownload dari info dict
                if "entries" in info:
                    # Playlist / multi-media (carousel Instagram)
                    # FIX: wrap in list() — yt-dlp may return a lazy generator for playlists.
                    # Iterating a generator twice (e.g. here + a debug/log path) exhausts it silently.
                    for entry in list(info["entries"] or []):
                        if entry and "requested_downloads" in entry:
                            for dl in entry["requested_downloads"]:
                                path = Path(dl["filepath"])
                                if path.exists():
                                    downloaded.append(path)
                elif isinstance(info, dict) and "requested_downloads" in info:
                    # Single media
                    dl_items: Any = info.get("requested_downloads") or []
                    for dl in dl_items:
                        path = Path(dl["filepath"])
                        if path.exists():
                            downloaded.append(path)

            return downloaded, caption, timestamp

        except DownloadError as e:
            err_str = str(e)
            # 'No video formats found' = post adalah FOTO, bukan video.
            # yt-dlp tidak bisa download foto Instagram — gunakan gallery-dl sebagai fallback.
            if (
                "No video formats found" in err_str
                or "Requested format is not available" in err_str
            ) and ("instagram.com" in url):
                logger.info(
                    "Post adalah foto — menggunakan gallery-dl sebagai fallback..."
                )
                cookies_file = opts.get("cookiefile")
                gdl_files = self._run_gallery_dl(url, cookies_file or "")
                if gdl_files:
                    logger.info(
                        f"gallery-dl berhasil: {len(gdl_files)} foto didownload"
                    )
                    downloaded.extend(gdl_files)
                else:
                    logger.error("gallery-dl juga gagal download foto")
                    raise e
            else:
                logger.error(f"yt-dlp download error: {e}")
                raise e
        except Exception as e:
            logger.error(f"Unexpected error saat download: {e}")
            raise e

        return downloaded, caption, timestamp

    def _download_tiktok_images(self, info: dict, outtmpl: str) -> list[Path]:
        """
        Download semua foto dari TikTok Carousel menggunakan URL dari metadata yt-dlp.

        Kenapa tidak pakai yt-dlp untuk ini?
        TikTok carousel menyimpan foto-fotonya di key 'images' (bukan 'formats').
        yt-dlp tidak bisa download dari key 'images' secara langsung — dia hanya
        bisa download dari 'formats'. Jika dipaksa, dia akan download audio latar.

        Solusi: ambil URL dari info['images'], download via requests (sync version
        agar tetap bisa dipanggil dari thread pool executor).
        """
        import requests  # Gunakan requests (sync) karena fungsi ini sync (di executor)

        downloaded = []
        images = info.get("images", [])
        post_id = info.get("id", "unknown")

        # Derive output directory dari outtmpl (ambil bagian folder-nya)
        if outtmpl:
            out_dir = Path(outtmpl).parent
        else:
            out_dir = self.temp_dir

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer": "https://www.tiktok.com/",
        }

        for idx, img_info in enumerate(images):
            # yt-dlp menyimpan URL foto di dalam nested dict: images[i]['url']
            # atau di dalam list 'thumbnails' per image.
            img_url = None
            if isinstance(img_info, dict):
                img_url = img_info.get("url") or img_info.get("urls", [None])[0]
            elif isinstance(img_info, str):
                img_url = img_info

            if not img_url:
                logger.warning(f"Foto ke-{idx+1} tidak punya URL — skip")
                continue

            output_path = out_dir / f"{post_id}_{idx+1:03d}.jpg"
            try:
                resp = requests.get(img_url, headers=headers, timeout=30)
                resp.raise_for_status()
                with open(output_path, "wb") as f:
                    f.write(resp.content)
                downloaded.append(output_path)
                logger.debug(
                    f"Foto {idx+1}/{len(images)} didownload: {output_path.name}"
                )
            except Exception as e:
                logger.error(f"Gagal download foto TikTok ke-{idx+1}: {e}")

        logger.info(
            f"TikTok carousel: {len(downloaded)}/{len(images)} foto berhasil didownload"
        )
        # FIX: was missing return — callers received None and crashed on iteration
        return downloaded

    async def _run_gallery_dl_async(self, url: str, cookies_file: Optional[str] = None) -> list[Path]:
        """
        Download foto menggunakan gallery-dl secara async via asyncio.create_subprocess_exec.
        """
        cmd = [
            "--dest",
            str(self.temp_dir),
            "--no-part",
        ]

        if cookies_file and Path(cookies_file).exists():
            cmd += ["--cookies", cookies_file]

        cmd.append(url)

        downloaded = []
        try:
            before = set(self.temp_dir.rglob("*.*"))

            proc = await asyncio.create_subprocess_exec(
                "gallery-dl",
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            try:
                stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=120.0)
            except asyncio.TimeoutError:
                proc.kill()
                # FIX: await proc.wait() setelah kill() untuk mencegah zombie process di Linux.
                # Tanpa wait(), gallery-dl tetap di process table sebagai <defunct> zombie
                # sampai parent process (Python) keluar. Pada server long-running, akumulasi.
                try:
                    await asyncio.wait_for(proc.wait(), timeout=5.0)
                except asyncio.TimeoutError:
                    pass  # Proses bandel — biarkan OS reap saat bot restart
                logger.error("gallery-dl timeout setelah 2 menit")
                return []

            if proc.returncode not in (0, 1):
                logger.error(
                    f"gallery-dl error (code {proc.returncode}): {stderr.decode()[-300:]}"
                )
                return []

            after = set(self.temp_dir.rglob("*.*"))
            new_files = after - before
            downloaded = [f for f in new_files if f.is_file()]

        except FileNotFoundError:
            logger.error("gallery-dl tidak ditemukan — pastikan sudah terinstall di PATH")
        except Exception as e:
            logger.error(f"gallery-dl error tak terduga: {e}")

        return downloaded

    def _run_gallery_dl(self, url: str, cookies_file: Optional[str] = None) -> list[Path]:
        """
        Download foto menggunakan gallery-dl sebagai fallback dari yt-dlp.

        gallery-dl mendukung Instagram foto, carousel, dan video.
        Digunakan sebagai fallback ketika yt-dlp gagal karena post adalah foto
        (yt-dlp Instagram extractor hanya mendukung video, bukan gambar statis).

        gallery-dl bisa autentikasi via:
        - cookies file (Netscape format) — sama dengan yt-dlp
        - Username/password (tidak digunakan di sini)
        """
        cmd = [
            "gallery-dl",
            "--dest",
            str(self.temp_dir),
            # Simpan file tanpa nested subdirectory — taruh langsung di temp_dir/instagram/username/
            "--no-part",  # Jangan simpan file .part
        ]

        if cookies_file and Path(cookies_file).exists():
            cmd += ["--cookies", cookies_file]

        cmd.append(url)

        downloaded = []
        try:
            # Catat semua file yang ada sebelum download (rekursif)
            before = set(self.temp_dir.rglob("*.*"))

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=120,
            )

            # gallery-dl return 0 = sukses, 1 = sebagian sukses, lainnya = error
            if result.returncode not in (0, 1):
                logger.error(
                    f"gallery-dl error (code {result.returncode}): {result.stderr[-300:]}"
                )
                return []

            # Scan rekursif untuk menemukan file baru (gallery-dl buat subdir)
            after = set(self.temp_dir.rglob("*.*"))
            new_files = after - before
            downloaded = [f for f in new_files if f.is_file()]

            if downloaded:
                logger.debug(f"gallery-dl output: {result.stdout[-200:]}")
            elif result.stderr:
                logger.warning(f"gallery-dl stderr: {result.stderr[-200:]}")

        except subprocess.TimeoutExpired:
            logger.error("gallery-dl timeout setelah 2 menit")
        except FileNotFoundError:
            logger.error("gallery-dl tidak ditemukan — coba: pip install gallery-dl")
        except Exception as e:
            logger.error(f"gallery-dl error tak terduga: {e}")

        return downloaded

    async def download_direct_url(
        self, media_url: str, filename: str, headers: Optional[dict] = None, cookies: Optional[dict] = None
    ) -> Optional[Path]:
        """
        Download media langsung dari URL (tanpa yt-dlp) menggunakan curl_cffi untuk WAF bypass.
        """
        output_path = self.temp_dir / filename
        # FIX: temp_dir may be wiped by /reset_bot mid-session — always ensure it exists
        output_path.parent.mkdir(parents=True, exist_ok=True)
        if "pbs.twimg.com" in media_url or "twimg.com" in media_url or "twitter.com" in media_url or "x.com" in media_url:
            referer = "https://x.com/"
        elif "instagram.com" in media_url or "cdninstagram.com" in media_url:
            referer = "https://www.instagram.com/"
        elif "tiktok.com" in media_url or "tiktokcdn.com" in media_url:
            referer = "https://www.tiktok.com/"
        else:
            referer = ""

        default_headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
        }
        if referer:
            default_headers["Referer"] = referer

        req_headers = {**default_headers, **(headers or {})}

        try:
            try:
                from curl_cffi import requests as curl_requests  # pyright: ignore[reportMissingImports]
                async with curl_requests.AsyncSession(impersonate="chrome") as session:
                    response = await session.get(
                        media_url,
                        headers=req_headers,
                        cookies=cookies,
                        timeout=60,
                        allow_redirects=True,
                        stream=True,  # FIX: stream agar tidak load seluruh response ke RAM
                    )
                    if response.status_code != 200:
                        logger.error(f"Gagal download URL langsung (HTTP status {response.status_code})")
                        return None

                    # FIX: streaming chunk write — cegah RAM spike untuk file besar
                    async with aiofiles.open(output_path, "wb") as f:
                        async for chunk in response.aiter_bytes(8192):
                            await f.write(chunk)
                return output_path
            except (ImportError, ModuleNotFoundError):
                import httpx  # pyright: ignore[reportMissingImports]
                async with httpx.AsyncClient(headers=req_headers, cookies=cookies, timeout=60.0, follow_redirects=True) as client:
                    async with client.stream("GET", media_url) as response:
                        if response.status_code != 200:
                            logger.error(f"Gagal download URL langsung via httpx (HTTP status {response.status_code})")
                            return None
                        async with aiofiles.open(output_path, "wb") as f:
                            async for chunk in response.aiter_bytes(8192):
                                await f.write(chunk)
                return output_path

        except Exception as e:
            logger.error(f"Gagal download URL langsung {media_url}: {e}")
            return None

    # ──────────────────────────────────────────────
    # Kompresi Video dengan ffmpeg
    # ──────────────────────────────────────────────

    async def compress_if_needed(self, file_path: Path) -> tuple[Path, bool]:
        """
        Cek ukuran file dan kompres jika melebihi batas Discord.

        Returns:
            Tuple (path_file_output, is_over_limit_after_compression)
            - is_over_limit_after_compression: True jika file masih > limit env
              setelah kompresi (bitrate floor aktif) → harus dikirim sebagai dokumen
        """
        file_size = file_path.stat().st_size

        if file_size <= self.max_file_size_bytes:
            logger.debug(
                f"File {file_path.name} ({file_size/1024/1024:.1f}MB) — tidak perlu kompresi"
            )
            return file_path, False

        logger.info(
            f"File {file_path.name} ({file_size/1024/1024:.1f}MB) melebihi limit — mulai kompresi"
        )

        # Ambil durasi video menggunakan ffprobe / ffmpeg
        duration = await self._get_video_duration(file_path)
        if duration and duration > 0:
            target_bitrate_bps = self._calculate_target_bitrate(duration)
        else:
            # Fallback jika durasi tidak bisa diketahui: gunakan fallback bitrate 1 Mbps
            logger.warning("Durasi video tidak terdeteksi — menggunakan fallback bitrate & file size limit")
            target_bitrate_bps = 1_000_000

        # Jalankan kompresi di thread pool (ffmpeg bersifat blocking)
        compressed_path = file_path.with_stem(file_path.stem + "_compressed")
        loop = asyncio.get_running_loop()  # get_running_loop() — tidak deprecated
        success = await loop.run_in_executor(
            None,
            self._run_ffmpeg_compress,
            file_path,
            compressed_path,
            target_bitrate_bps,
        )

        if not success or not compressed_path.exists():
            logger.error("Kompresi ffmpeg gagal — pakai file original")
            return file_path, True

        compressed_size = compressed_path.stat().st_size
        logger.info(
            f"Kompresi selesai: {file_size/1024/1024:.1f}MB → {compressed_size/1024/1024:.1f}MB"
        )

        # Hapus file original setelah kompresi berhasil
        try:
            file_path.unlink()
        except Exception:
            pass

        # Cek apakah hasil kompresi masih melebihi limit (safety floor aktif)
        still_over_limit = compressed_size > self.max_file_size_bytes
        if still_over_limit:
            logger.warning(
                f"File masih {compressed_size/1024/1024:.1f}MB setelah kompresi "
                f"(safety floor aktif) — akan dikirim sebagai dokumen"
            )

        return compressed_path, still_over_limit

    def _calculate_target_bitrate(self, duration_seconds: float) -> int:
        """
        Hitung target bitrate video untuk kompresi ffmpeg secara dinamis.
        """
        raw_bitrate = (self.target_file_size_bytes * 8) / duration_seconds - AUDIO_BITRATE_BPS

        if raw_bitrate < MIN_VIDEO_BITRATE_BPS:
            logger.warning(
                f"Kalkulasi bitrate ({raw_bitrate:.0f} bps) di bawah safety floor "
                f"({MIN_VIDEO_BITRATE_BPS} bps). Menggunakan floor value."
            )
            return MIN_VIDEO_BITRATE_BPS

        return int(raw_bitrate // 1000) * 1000

    async def _get_video_duration(self, file_path: Path) -> Optional[float]:
        """
        Ambil durasi video menggunakan ffprobe, dengan fallback ke ffmpeg -i jika ffprobe tidak ditemukan/error.
        """
        # Method 1: ffprobe
        try:
            proc = await asyncio.create_subprocess_exec(
                "ffprobe",
                "-v",
                "quiet",
                "-print_format",
                "json",
                "-show_format",
                str(file_path),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, _ = await proc.communicate()
            data = json.loads(stdout.decode())
            # Safe dict access to prevent KeyError on corrupted/malformed media probe
            format_dict = data.get("format", {})
            raw_duration = format_dict.get("duration")
            if raw_duration is not None:
                duration = float(raw_duration)
                logger.debug(f"Durasi video (ffprobe): {duration:.1f}s")
                return duration
        except FileNotFoundError:
            logger.warning("ffprobe tidak ditemukan di PATH — mencoba fallback via ffmpeg...")
        except Exception as e:
            logger.warning(f"ffprobe error ({e}) — mencoba fallback duration probe via ffmpeg...")

        # Method 2: ffmpeg -i parse stderr
        try:
            proc = await asyncio.create_subprocess_exec(
                "ffmpeg",
                "-i",
                str(file_path),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            _, stderr = await proc.communicate()
            match = re.search(r"Duration:\s*(\d+):(\d+):(\d+\.\d+)", stderr.decode("utf-8", errors="ignore"))
            if match:
                hours, minutes, seconds = map(float, match.groups())
                duration = hours * 3600 + minutes * 60 + seconds
                logger.debug(f"Durasi video (ffmpeg fallback): {duration:.1f}s")
                return duration
        except FileNotFoundError:
            logger.error("ffmpeg tidak ditemukan di PATH.")
        except Exception as e:
            logger.warning(f"ffmpeg duration probe error: {e}")

        return None

    def _run_ffmpeg_compress(
        self,
        input_path: Path,
        output_path: Path,
        target_bitrate_bps: int,
    ) -> bool:
        """
        Jalankan ffmpeg untuk mengompres video dengan target bitrate tertentu.
        Menggunakan single-pass encoding dengan -maxrate dan -bufsize untuk
        kontrol yang lebih baik terhadap ukuran output.
        """
        target_kbps = target_bitrate_bps // 1000
        bufsize_kbps = target_kbps * 2

        cmd = [
            "ffmpeg",
            "-i",
            str(input_path),
            "-c:v",
            "libx264",
            "-b:v",
            f"{target_kbps}k",
            "-maxrate",
            f"{target_kbps}k",
            "-bufsize",
            f"{bufsize_kbps}k",
            "-fs",
            f"{self.target_file_size_bytes}",
            "-c:a",
            "aac",
            "-b:a",
            "128k",
            "-movflags",
            "+faststart",
            "-y",
            str(output_path),
        ]

        logger.info(f"ffmpeg kompresi: target {target_kbps}kbps → {output_path.name}")

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=600,
            )
            if result.returncode != 0:
                logger.error(
                    f"ffmpeg error: {result.stderr[-500:]}"
                )
                return False
            return True
        except subprocess.TimeoutExpired:
            logger.error("ffmpeg timeout setelah 10 menit")
            return False
        except FileNotFoundError:
            logger.error(
                "ffmpeg tidak ditemukan — pastikan sudah terinstall dan ada di PATH"
            )
            return False
        except Exception as e:
            logger.error(f"ffmpeg unexpected error: {e}")
            return False

    # ──────────────────────────────────────────────
    # Manajemen File Temporer
    # ──────────────────────────────────────────────

    def cleanup_files(self, file_paths: list[Path]) -> None:
        """
        Hapus file temporer dari disk (versi sinkron).
        Digunakan hanya di konteks non-async (misalnya cleanup darurat).
        """
        for path in file_paths:
            try:
                if path.exists():
                    path.unlink()
                    logger.debug(f"File dihapus: {path.name}")
            except Exception as e:
                logger.warning(f"Gagal hapus file {path}: {e}")

    async def cleanup_files_async(self, file_paths: list[Path]) -> None:
        """
        Hapus file temporer dari disk secara asinkron dengan retry.

        Kenapa perlu versi async?
        - Windows menahan lock file bahkan setelah 'with open' ditutup
          jika proses lain masih memegang reference-nya.
        - Solusi: coba hapus, jika WinError 32 tunggu 2 detik lalu retry sekali.
        - asyncio.sleep memberi waktu event loop memroses events lain sehingga
          client upload sempat melepaskan internal file reference-nya.
        """
        for path in file_paths:
            try:
                if path.exists():
                    path.unlink()
                    logger.debug(f"File dihapus: {path.name}")
            except PermissionError as e:
                # WinError 32: File masih dikunci proses lain saat upload
                # Tunggu 2 detik lagi lalu coba sekali lagi
                logger.debug(f"File masih terkunci, retry dalam 2 detik: {path.name}")
                await asyncio.sleep(2)
                try:
                    if path.exists():
                        path.unlink()
                        logger.debug(f"File dihapus (retry): {path.name}")
                except Exception as e2:
                    logger.warning(
                        f"Gagal hapus file (retry juga gagal) {path.name}: {e2}"
                    )
            except Exception as e:
                logger.warning(f"Gagal hapus file {path}: {e}")

    def get_file_type(self, file_path: Path) -> str:
        """
        Tentukan tipe file berdasarkan ekstensi untuk pemilihan metode kirim Discord.

        Returns:
            'video', 'photo', atau 'document'
        """
        suffix = file_path.suffix.lower()
        if suffix in {".mp4", ".mov", ".avi", ".mkv", ".webm"}:
            return "video"
        elif suffix in {".jpg", ".jpeg", ".png", ".webp", ".gif"}:
            return "photo"
        else:
            return "document"

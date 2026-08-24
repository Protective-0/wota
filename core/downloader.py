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

import shutil
import yt_dlp
from yt_dlp.utils import DownloadError
import gallery_dl.job
import gallery_dl.config
import aiofiles  # Async file I/O untuk mencegah blocking event loop
import time as _time
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

from scrapers.base import (
    BaseScraper,
    DOCKER_CHROMIUM_FLAGS,
)
from .utils import (
    TAG_DOWN, TAG_COMPR, TAG_SYSTEM, TAG_WARN, TAG_ERROR, TAG_SUCCESS, TAG_CRAWL,
    fmt_size, fmt_duration,
)

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
    seen = set()

    def add_url(u: str) -> None:
        if u and isinstance(u, str) and u.startswith("http") and u not in seen:
            if not any(bad in u for bad in ("avatar", "profile", "icon", "placeholder", "logo")):
                seen.add(u)
                image_urls.append(u)

    def search_images(d) -> None:
        if isinstance(d, dict):
            # 1. Direct photo structure checks
            for key in ("imagePost", "imagePostInfo", "images", "photo", "photos", "displayImage", "display_image"):
                if key in d:
                    val = d[key]
                    if isinstance(val, dict):
                        sub_imgs = val.get("images", []) or val.get("imageURL", {}) or val.get("displayImage", {})
                        if isinstance(sub_imgs, list):
                            for img in sub_imgs:
                                if isinstance(img, dict):
                                    urls = (
                                        img.get("imageURL", {}).get("urlList", [])
                                        or img.get("displayImage", {}).get("urlList", [])
                                        or img.get("downloadAddr", {}).get("urlList", [])
                                        or img.get("urlList", [])
                                    )
                                    for u in urls:
                                        add_url(u)
                                        break
                                elif isinstance(img, str):
                                    add_url(img)
                        elif isinstance(sub_imgs, dict):
                            urls = sub_imgs.get("urlList", []) or sub_imgs.get("downloadAddr", [])
                            for u in urls:
                                add_url(u)
                                break
                    elif isinstance(val, list):
                        for item in val:
                            if isinstance(item, dict):
                                urls = (
                                    item.get("imageURL", {}).get("urlList", [])
                                    or item.get("displayImage", {}).get("urlList", [])
                                    or item.get("downloadAddr", {}).get("urlList", [])
                                    or item.get("urlList", [])
                                )
                                for u in urls:
                                    add_url(u)
                                    break
                                if "url" in item and isinstance(item["url"], str):
                                    add_url(item["url"])
                            elif isinstance(item, str):
                                add_url(item)

            for k, value in d.items():
                if k not in ("music", "author", "shareMeta", "stats"):
                    search_images(value)
        elif isinstance(d, list):
            for item in d:
                search_images(item)
        elif isinstance(d, str):
            if ("photomode" in d or "tos-alisg-i" in d or "tiktokcdn" in d) and d.startswith("http"):
                add_url(d)

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
            # FIX: removed dead `browser_cookies = []` variable. It was never populated
            # in this branch, causing `cookies_dict` to always be {} which sent TikTok CDN
            # requests without auth cookies — resulting in 403s on authenticated content.
            # _extract_tiktok_carousel_urls() (called below) already handles cookies
            # internally via the cookies_file parameter, so no browser_cookies needed.
            image_urls: list[str] = []
            caption = ""

            try:
                result = await self._extract_tiktok_carousel_urls(post_url, cookies_file)
                if isinstance(result, tuple) and len(result) == 2:
                    image_urls, caption = result
                else:
                    image_urls, caption = [], ""
            except Exception as e:
                logger.error(f"{TAG_ERROR} Gagal memproses manual carousel untuk {post_url}: {e}")
                image_urls, caption = [], ""

            if image_urls:
                logger.info(
                    f"{TAG_DOWN} TikTok Carousel terdeteksi ({len(image_urls)} foto) "
                    f"\u2014 bypass yt-dlp, download CDN langsung..."
                )

                # Build cookies_dict from cookies_file only (browser_cookies removed)
                cookies_dict: dict[str, str] = {}
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
            logger.info(
                f"{TAG_DOWN} Twitter direct media URLs: {len(media_urls)} item "
                f"\u2014 bypass yt-dlp, CDN langsung."
            )
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
        if "tiktok.com" in post_url:
            # ── Jalur Khusus TikTok: API-First (TikWM Clean API) ─────────────────
            logger.info(f"{TAG_DOWN} TikTok terdeteksi — mencoba download via TikWM Clean API...")
            tk_res = await self._download_tiktok_video_clean(post_url, post_id)
            if tk_res and tk_res[0]:
                tk_files, tk_caption, tk_timestamp = tk_res
                valid_files = []
                for p in tk_files:
                    if p.name.lower().endswith((".mp4", ".mov", ".webm")):
                        if await self._is_valid_tiktok_video(p):
                            comp_p, _ = await self.compress_if_needed(p)
                            valid_files.append(comp_p)
                        else:
                            logger.warning(f"{TAG_WARN} File {p.name} gagal validasi video asli (splash/placeholder terdeteksi).")
                    else:
                        valid_files.append(p)
                if valid_files:
                    return valid_files, tk_caption, tk_timestamp

            # Jika TikWM Clean API tidak menghasilkan file, fallback ke yt-dlp / browser
            logger.warning(f"{TAG_WARN} TikWM API kosong untuk {post_url} — fallback ke yt-dlp / browser...")
            if "/photo/" in post_url:
                try:
                    result = await self._extract_tiktok_carousel_urls(post_url, cookies_file)
                    image_urls, fallback_cap = result if isinstance(result, tuple) and len(result) == 2 else ([], "")
                except Exception:
                    image_urls, fallback_cap = [], ""

                if image_urls:
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
                    image_urls_pw, browser_caption_pw, browser_cookies_pw = await self._extract_tiktok_carousel_via_browser(post_url, cookies_file)
                    if image_urls_pw:
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
                # TikTok Video Fallback via yt-dlp then Playwright Stream Capture
                try:
                    downloaded_files, real_caption, ytdl_timestamp = await self._run_ytdlp_async(
                        post_url, post_id, cookies_file
                    )
                except Exception as yte:
                    logger.warning(f"{TAG_WARN} yt-dlp fallback error: {yte}")

                if not downloaded_files:
                    captured_file, video_url, browser_caption, browser_cookies = await self._extract_tiktok_video_via_browser(post_url, post_id, cookies_file)
                    if captured_file and captured_file.exists() and await self._is_valid_tiktok_video(captured_file):
                        downloaded_files = [captured_file]
                        real_caption = browser_caption
                    elif video_url:
                        out_path = await self._download_tiktok_cdn_video(video_url, post_id, browser_cookies, cookies_file)
                        if out_path and await self._is_valid_tiktok_video(out_path):
                            downloaded_files = [out_path]
                            real_caption = browser_caption

            # Filter validasi akhir dan kompresi untuk seluruh media TikTok yang diunduh
            if downloaded_files:
                final_tk_files = []
                for p in downloaded_files:
                    if p.name.lower().endswith((".mp4", ".mov", ".webm")):
                        if await self._is_valid_tiktok_video(p):
                            comp_p, _ = await self.compress_if_needed(p)
                            final_tk_files.append(comp_p)
                        else:
                            logger.warning(f"{TAG_WARN} TikTok file {p.name} ditolak (video splash/placeholder terdeteksi).")
                    else:
                        final_tk_files.append(p)
                if final_tk_files:
                    return final_tk_files, real_caption, ytdl_timestamp

            logger.error(f"{TAG_ERROR} Semua extractor TikTok gagal mendapatkan media asli untuk {post_url} — return kosong.")
            return [], "", None

        elif is_twitter_photo:
            download_failed = True
        elif is_instagram_photo:
            # Bypass yt-dlp langsung ke gallery-dl secara async untuk Instagram photo (/p/) jika bukan video
            logger.info(
                f"{TAG_DOWN} Instagram /p/ terdeteksi "
                f"— bypass yt-dlp, routing ke gallery-dl."
            )
            gdl_files = await self._run_gallery_dl_async(post_url, cookies_file or "")
            if gdl_files:
                return gdl_files, "", None
            logger.warning("gallery-dl tidak menghasilkan file untuk /p/, fallback ke yt-dlp...")
            try:
                downloaded_files, real_caption, ytdl_timestamp = await self._run_ytdlp_async(
                    post_url, post_id, cookies_file
                )
            except Exception as e:
                yt_error = str(e)
                download_failed = True
        else:
            try:
                downloaded_files, real_caption, ytdl_timestamp = await self._run_ytdlp_async(
                    post_url, post_id, cookies_file
                )
                if not downloaded_files and ("twitter.com" in post_url or "x.com" in post_url):
                    download_failed = True
            except Exception as e:
                # FIX: bind original_err immediately so nested awaits cannot rebind `e`
                # inside inner except blocks — `raise e` at the bottom then re-raises the
                # correct original yt-dlp error regardless of how many inner calls run.
                original_err = e
                yt_error = str(e)
                if "twitter.com" in post_url or "x.com" in post_url:
                    logger.warning(f"{TAG_WARN} yt-dlp gagal untuk Twitter — fallback image handler: {yt_error}")
                    download_failed = True
                elif "tiktok.com" in post_url:
                    if "/video/" in post_url or "/v/" in post_url:
                        logger.warning(f"{TAG_WARN} yt-dlp gagal untuk TikTok Video ({yt_error}) — menjalankan TikWM & Video Fallback...")
                        # Step A: TikWM Clean API Video Fallback
                        tw_file, tw_caption = await self._download_via_tikwm(post_url, post_id)
                        if tw_file and tw_file.exists() and tw_file.stat().st_size > 300_000:
                            downloaded_files = [tw_file]
                            if tw_caption:
                                real_caption = tw_caption

                        # Step B: Playwright Video Extractor + In-flight Stream Capture
                        if not downloaded_files:
                            captured_file, video_url, browser_caption, browser_cookies = await self._extract_tiktok_video_via_browser(post_url, post_id, cookies_file)
                            if captured_file and captured_file.exists() and captured_file.stat().st_size > 300_000:
                                downloaded_files = [captured_file]
                                real_caption = browser_caption
                            elif video_url:
                                out_path = await self._download_tiktok_cdn_video(video_url, post_id, browser_cookies, cookies_file)
                                if out_path:
                                    downloaded_files = [out_path]
                                    real_caption = browser_caption
                    else:
                        logger.warning(f"{TAG_WARN} yt-dlp gagal untuk TikTok Photo — coba Carousel Fallback: {yt_error}")
                        try:
                            result = await self._extract_tiktok_carousel_urls(post_url, cookies_file)
                            carousel_urls, carousel_cap = result if isinstance(result, tuple) and len(result) == 2 else ([], "")
                        except Exception:
                            carousel_urls, carousel_cap = [], ""

                        if carousel_urls:
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
                            image_urls_pw, browser_caption_pw, browser_cookies_pw = await self._extract_tiktok_carousel_via_browser(post_url, cookies_file)
                            if image_urls_pw:
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
                        else:
                            logger.error(f"{TAG_ERROR} Semua fallback TikTok habis \u2014 tidak ada media yang bisa diunduh.")
                            # FIX: same — chain from original yt-dlp error, not raw `raise e`
                            raise RuntimeError(f"Semua fallback TikTok habis untuk {post_url}") from original_err
                elif "No video could be found" in yt_error or "No video formats found" in yt_error:
                    logger.warning(f"yt-dlp gagal download (no formats found): {yt_error}")
                    download_failed = True
                else:
                    logger.error(f"yt-dlp gagal download dengan error fatal: {yt_error}")


        # Twitter Image Fallback Handler
        if download_failed and ("twitter.com" in post_url or "x.com" in post_url):
            logger.info(f"{TAG_DOWN} Twitter Image Fallback Handler aktif untuk: {post_url}")
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

            twitter_cookies = []
            for name, value, domains in [
                ("auth_token", auth_token, [".x.com", ".twitter.com"]),
                ("ct0",        ct0,        [".x.com", ".twitter.com"]),
            ]:
                if value:
                    for domain in domains:
                        twitter_cookies.append({
                            "name": name,
                            "value": value.strip(),
                            "domain": domain,
                            "path": "/",
                        })

            unique_images = []

            async def _twitter_page_action(page):
                nonlocal real_caption, ytdl_timestamp, unique_images
                try:
                    await page.wait_for_selector('article, [data-testid="tweet"], img[src*="pbs.twimg.com/media/"]', timeout=12000)
                except Exception:
                    pass
                await asyncio.sleep(2.0)

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
                except Exception:
                    pass

                # Kumpulkan semua URL gambar pbs.twimg.com dari tweet
                img_srcs = await page.evaluate("""() => {
                    const imgs = document.querySelectorAll('img[src*="pbs.twimg.com/media/"], [data-testid="tweetPhoto"] img, article img[src*="twimg"]');
                    return Array.from(imgs)
                        .map(img => img.src)
                        .filter(src => src && src.includes('pbs.twimg.com/media/') && !src.includes('profile_images') && !src.includes('emoji'));
                }""")
                seen_urls = set()

                for src in (img_srcs or []):
                    if src:
                        clean_src = src.split("?")[0]
                        if clean_src not in seen_urls:
                            seen_urls.add(clean_src)
                            if "format=" in src:
                                high_res = re.sub(r"name=\w+", "name=orig", src)
                                if "name=" not in high_res:
                                    high_res += "&name=orig"
                            else:
                                high_res = f"{clean_src}?format=jpg&name=orig"
                            unique_images.append(high_res)

            try:
                browser = await self.get_browser()
                context = await browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                    viewport={"width": 1280, "height": 900},
                    locale="id-ID"
                )
                if twitter_cookies:
                    await context.add_cookies(twitter_cookies)
                page = await context.new_page()
                try:
                    await page.goto(post_url, wait_until="domcontentloaded", timeout=25000)
                    await _twitter_page_action(page)
                finally:
                    await context.close()
            except Exception as tw_err:
                logger.warning(f"Twitter fallback fetch error: {tw_err}")

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
                                logger.info(
                                    f"{TAG_DOWN} Gambar Twitter terunduh: {filename} "
                                    f"({fmt_size(out_path.stat().st_size if out_path.exists() else 0)})"
                                )
                            else:
                                logger.warning(f"{TAG_WARN} Gagal unduh gambar Twitter {img_url}: status {res.status_code}")
                        except Exception as dl_err:
                            logger.error(f"Error download gambar Twitter {img_url}: {dl_err}")

        # Instagram Fallback Handler (API / Browser)
        if (download_failed or not downloaded_files) and ("instagram.com" in post_url):
            logger.info(f"{TAG_DOWN} Instagram Fallback Handler (API / Browser) aktif untuk: {post_url}")
            ig_files, ig_caption, ig_ts = await self._extract_instagram_media_via_api_or_browser(post_url, post_id)
            if ig_files:
                downloaded_files = ig_files
                if ig_caption:
                    real_caption = ig_caption
                if ig_ts:
                    ytdl_timestamp = ig_ts

        # ── Video / Thumbnail File Isolation ─────────────────────────────────────
        # Jika postingan menghasilkan file video (.mp4, .mov, .webm, .mkv), pastikan
        # HANYA file video yang dikembalikan. Hapus thumbnail/cover (.jpg, .png, .webp)
        # yang mungkin ikut terunduh oleh yt-dlp agar Discord hanya menerima video asli.
        video_files = [f for f in downloaded_files if f.suffix.lower() in {".mp4", ".mov", ".webm", ".mkv"}]
        if video_files:
            image_files = [f for f in downloaded_files if f.suffix.lower() in {".jpg", ".jpeg", ".png", ".webp"}]
            for img_f in image_files:
                try:
                    img_f.unlink(missing_ok=True)
                except Exception:
                    pass
            downloaded_files = video_files

        return downloaded_files, real_caption, ytdl_timestamp

    async def _extract_instagram_media_via_api_or_browser(
        self, post_url: str, post_id: str
    ) -> tuple[list[Path], str, Optional[str]]:
        """
        Ekstraksi dan unduh media Instagram (Video / Carousel / Foto) dalam 100% Guest Mode.
        Tier 1: Instagram Public Web API (curl_cffi TLS impersonation chrome124)
        Tier 2: Playwright Stealth Browser Fallback (dengan seleksi ketat <video> tanpa og:image)
        """
        import httpx
        from datetime import datetime, timezone

        downloaded: list[Path] = []
        caption = ""
        timestamp = None

        # Ekstrak username & shortcode dari URL
        username = ""
        u_match = re.search(r"instagram\.com/([^/?&#/]+)", post_url)
        if u_match:
            u_cand = u_match.group(1).lower().strip()
            if u_cand not in {"p", "reel", "explore", "stories", "accounts"}:
                username = u_cand

        shortcode = post_id
        s_match = re.search(r"/(?:p|reel)/([A-Za-z0-9_-]+)", post_url)
        if s_match:
            shortcode = s_match.group(1)

        # ── Tier 1: Instagram Public Web API via curl_cffi / httpx ──
        if username:
            try:
                api_url = f"https://www.instagram.com/api/v1/users/web_profile_info/?username={username}"
                headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                    "X-IG-App-ID": "936619743392459",
                    "Accept": "*/*",
                    "Referer": f"https://www.instagram.com/{username}/",
                }

                resp_data = None
                try:
                    from curl_cffi import requests as curl_requests
                    async with curl_requests.AsyncSession(impersonate="chrome124") as session:
                        resp = await session.get(api_url, headers=headers, timeout=15)
                        if resp.status_code == 200:
                            resp_data = resp.json()
                except Exception:
                    pass

                if not resp_data:
                    async with httpx.AsyncClient(headers=headers, timeout=15.0) as client:
                        resp = await client.get(api_url)
                        if resp.status_code == 200:
                            resp_data = resp.json()

                if resp_data:
                    edges = resp_data.get("data", {}).get("user", {}).get("edge_owner_to_timeline_media", {}).get("edges", [])
                    target_node = None
                    for e in edges:
                        node = e.get("node", {})
                        if node.get("shortcode") == shortcode:
                            target_node = node
                            break

                    if target_node:
                        # Caption & Timestamp
                        edges_caption = target_node.get("edge_media_to_caption", {}).get("edges", [])
                        if edges_caption:
                            caption = edges_caption[0].get("node", {}).get("text", "") or ""

                        ts_val = target_node.get("taken_at_timestamp")
                        if ts_val:
                            timestamp = datetime.fromtimestamp(ts_val, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

                        is_video = target_node.get("is_video", False)
                        video_url = target_node.get("video_url")
                        sidecar = target_node.get("edge_sidecar_to_children", {}).get("edges", [])

                        if is_video and video_url:
                            # Direct download video (.mp4)
                            v_out = self.temp_dir / f"{post_id}_video.mp4"
                            async with httpx.AsyncClient(headers={"User-Agent": SHARED_USER_AGENT}, timeout=60.0) as client:
                                v_resp = await client.get(video_url)
                                if v_resp.status_code == 200:
                                    async with aiofiles.open(v_out, "wb") as f:
                                        await f.write(v_resp.content)
                                    logger.info(f"{TAG_DOWN} Instagram Video API terunduh: {v_out.name} ({fmt_size(v_out.stat().st_size)})")
                                    return [v_out], caption, timestamp

                        elif sidecar:
                            # Carousel items (bisa campuran gambar & video)
                            sidecar_files = []
                            for idx, c_edge in enumerate(sidecar):
                                c_node = c_edge.get("node", {})
                                c_is_v = c_node.get("is_video", False)
                                c_url = c_node.get("video_url") if c_is_v else c_node.get("display_url")
                                if not c_url:
                                    continue
                                ext = "mp4" if c_is_v else "jpg"
                                out_p = self.temp_dir / f"{post_id}_{idx+1:03d}.{ext}"
                                try:
                                    async with httpx.AsyncClient(headers={"User-Agent": SHARED_USER_AGENT}, timeout=60.0) as client:
                                        c_res = await client.get(c_url)
                                        if c_res.status_code == 200:
                                            async with aiofiles.open(out_p, "wb") as f:
                                                await f.write(c_res.content)
                                            sidecar_files.append(out_p)
                                except Exception as c_err:
                                    logger.warning(f"Gagal unduh sidecar item {idx+1}: {c_err}")

                            if sidecar_files:
                                logger.info(f"{TAG_DOWN} Instagram Carousel API terunduh: {len(sidecar_files)} file")
                                return sidecar_files, caption, timestamp

                        else:
                            # Single photo
                            display_url = target_node.get("display_url")
                            if display_url:
                                out_p = self.temp_dir / f"{post_id}_001.jpg"
                                async with httpx.AsyncClient(headers={"User-Agent": SHARED_USER_AGENT}, timeout=30.0) as client:
                                    d_res = await client.get(display_url)
                                    if d_res.status_code == 200:
                                        async with aiofiles.open(out_p, "wb") as f:
                                            await f.write(d_res.content)
                                        logger.info(f"{TAG_DOWN} Instagram Photo API terunduh: {out_p.name} ({fmt_size(out_p.stat().st_size)})")
                                        return [out_p], caption, timestamp
            except Exception as api_err:
                logger.debug(f"Instagram Tier 1 API fallback note: {api_err}")

        # ── Tier 2: Playwright Stealth Browser Fallback (Video-First Selection) ──
        logger.info(f"{TAG_DOWN} Menjalankan Playwright stealth fallback untuk Instagram: {post_url}")
        try:
            browser = await self.get_browser()
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                viewport={"width": 1280, "height": 900},
                locale="id-ID",
            )
            page = await context.new_page()

            try:
                await page.goto(post_url, wait_until="domcontentloaded", timeout=25000)
                await asyncio.sleep(2.5)

                # Cek login wall
                if "accounts/login" in page.url.lower():
                    logger.warning(f"{TAG_WARN} Instagram post mengalihkan ke login page.")
                else:
                    # Caption
                    for sel in ["article h1", "[data-testid='post-caption'] h1", "h1"]:
                        try:
                            c_el = page.locator(sel).first
                            if await c_el.count() > 0:
                                caption = (await c_el.inner_text()).strip()
                                if caption:
                                    break
                        except Exception:
                            pass

                    # Timestamp
                    try:
                        t_el = page.locator("time").first
                        if await t_el.count() > 0:
                            timestamp = await t_el.get_attribute("datetime")
                    except Exception:
                        pass

                    # 1. Prioritas Utama: Cek Video tag
                    video_urls = []
                    videos = await page.locator("video").all()
                    for v in videos:
                        src = await v.get_attribute("src")
                        if src and not src.startswith("blob:"):
                            video_urls.append(src)
                        sources = await v.locator("source").all()
                        for s in sources:
                            s_src = await s.get_attribute("src")
                            if s_src and not s_src.startswith("blob:"):
                                video_urls.append(s_src)

                    if not video_urls:
                        og_v_loc = page.locator('meta[property="og:video"], meta[property="og:video:secure_url"]')
                        if await og_v_loc.count() > 0:
                            og_src = await og_v_loc.first.get_attribute("content")
                            if og_src:
                                video_urls.append(og_src)

                    if video_urls:
                        # Ini adalah VIDEO: Download video mp4, JANGAN ambil thumbnail image!
                        clean_v_url = video_urls[0]
                        v_out = self.temp_dir / f"{post_id}_video.mp4"
                        async with httpx.AsyncClient(headers={"User-Agent": SHARED_USER_AGENT}, timeout=60.0) as client:
                            v_res = await client.get(clean_v_url)
                            if v_res.status_code == 200:
                                async with aiofiles.open(v_out, "wb") as f:
                                    await f.write(v_res.content)
                                logger.info(f"{TAG_DOWN} Instagram Video (Browser) terunduh: {v_out.name} ({fmt_size(v_out.stat().st_size)})")
                                return [v_out], caption, timestamp

                    # 2. Jika BUKAN video, ambil gambar
                    img_urls = []
                    imgs = await page.locator('article img, div[role="presentation"] img').all()
                    for img in imgs:
                        src = await img.get_attribute("src")
                        if src and not src.startswith("blob:") and "instagram" in src:
                            img_urls.append(src)

                    if not img_urls:
                        og_img_loc = page.locator('meta[property="og:image"]')
                        if await og_img_loc.count() > 0:
                            og_img = await og_img_loc.first.get_attribute("content")
                            if og_img:
                                img_urls.append(og_img)

                    unique_imgs = list(dict.fromkeys(img_urls))
                    img_files = []
                    for idx, img_u in enumerate(unique_imgs):
                        out_p = self.temp_dir / f"{post_id}_{idx+1:03d}.jpg"
                        try:
                            async with httpx.AsyncClient(headers={"User-Agent": SHARED_USER_AGENT}, timeout=30.0) as client:
                                i_res = await client.get(img_u)
                                if i_res.status_code == 200:
                                    async with aiofiles.open(out_p, "wb") as f:
                                        await f.write(i_res.content)
                                    img_files.append(out_p)
                        except Exception as i_err:
                            logger.warning(f"Gagal unduh gambar Instagram: {i_err}")

                    if img_files:
                        logger.info(f"{TAG_DOWN} Instagram Photo (Browser) terunduh: {len(img_files)} foto")
                        return img_files, caption, timestamp

            finally:
                await context.close()
        except Exception as b_err:
            logger.error(f"{TAG_ERROR} Playwright Instagram fallback error: {b_err}")

        return [], caption, timestamp

    async def _extract_tiktok_carousel_urls(self, url: str, cookies_file: Optional[str] = None) -> tuple[list[str], str]:
        """
        Ekstrak list URL gambar carousel dan caption dari webpage TikTok secara async.
        Multi-tier fallback:
          Tier 1: TikWM Clean API (cepat, direct CDN URL)
          Tier 2: Scrapling AsyncFetcher / SSR HTML Rehydration Parser (__UNIVERSAL_DATA_FOR_REHYDRATION__)
          Tier 3: TikTok Official Web Detail API dengan TLS Impersonation
        """
        import httpx
        try:
            from scrapling.fetchers import AsyncFetcher
            has_scrapling = True
        except ImportError:
            has_scrapling = False

        # ── Tier 1: TikWM Clean API (Fastest & most reliable for unauthenticated photo posts) ──
        try:
            async with httpx.AsyncClient(timeout=12.0) as client:
                tw_resp = await client.get(f"https://www.tikwm.com/api/?url={url}")
                if tw_resp.status_code == 200:
                    tw_data = tw_resp.json()
                    if tw_data.get("code") == 0 and tw_data.get("data"):
                        d = tw_data["data"]
                        tw_images = d.get("images", [])
                        tw_title = d.get("title", "") or ""
                        if tw_images and isinstance(tw_images, list) and len(tw_images) > 0:
                            logger.info(f"{TAG_DOWN} TikWM API: Berhasil menemukan {len(tw_images)} foto TikTok.")
                            return tw_images, tw_title
        except Exception as e:
            logger.debug(f"TikWM API extraction error: {e}")

        # ── Tier 2: Scrapling AsyncFetcher / SSR HTML Rehydration Parser ──
        headers = {
            "User-Agent": SHARED_USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.tiktok.com/",
        }

        cookies_dict = {}
        if cookies_file and Path(cookies_file).exists():
            try:
                cookies_dict = self._parse_netscape_cookies(cookies_file)
            except Exception as e:
                logger.warning(f"Gagal parse Netscape cookie file: {e}")

        html_content = ""
        if has_scrapling:
            try:
                response = await AsyncFetcher.get(
                    url,
                    headers=headers,
                    cookies=cookies_dict if cookies_dict else None,
                    timeout=15,
                    impersonate="chrome124",
                )
                if response.status == 200:
                    html_content = response.text
                    logger.debug("Tier 2: Scrapling AsyncFetcher berhasil mengambil HTML.")
            except Exception as sc_err:
                logger.debug(f"Tier 2 Scrapling AsyncFetcher note: {sc_err}")

        if not html_content:
            try:
                async with httpx.AsyncClient(
                    headers=headers,
                    cookies=cookies_dict if cookies_dict else None,
                    follow_redirects=True,
                    timeout=15.0,
                ) as client:
                    resp = await client.get(url)
                    if resp.status_code == 200:
                        html_content = resp.text
            except Exception as e:
                logger.debug(f"HTTPX fetch info: {e}")

        if html_content:
            try:
                match = re.search(
                    r'<script id="__UNIVERSAL_DATA_FOR_REHYDRATION__"[^>]*>([\s\S]*?)</script>',
                    html_content,
                )
                if not match:
                    match = re.search(r'<script id="SIGI_STATE"[^>]*>([\s\S]*?)</script>', html_content)
                if not match:
                    match = re.search(r'<script id="__NEXT_DATA__"[^>]*>([\s\S]*?)</script>', html_content)

                if match:
                    data = json.loads(match.group(1))
                    image_urls = _extract_tiktok_image_urls_from_rehydration(data)
                    caption = ""
                    try:
                        default_scope = data.get("__DEFAULT_SCOPE__", {})
                        video_detail = default_scope.get("webapp.video-detail", {}) or default_scope.get("webapp.videoDetail", {})
                        if not video_detail:
                            webapp = default_scope.get("webapp", {})
                            video_detail = webapp.get("video-detail", {}) or webapp.get("videoDetail", {})
                        item_struct = video_detail.get("itemInfo", {}).get("itemStruct", {})
                        caption = item_struct.get("desc") or ""
                    except Exception:
                        pass

                    if image_urls:
                        logger.info(f"{TAG_DOWN} Rehydration Parser: Berhasil menemukan {len(image_urls)} foto.")
                        return image_urls, caption
            except Exception as e:
                logger.debug(f"Gagal ekstrak rehydration JSON: {e}")

        # ── Tier 3: TikTok Official Item Detail Web API dengan Scrapling / TLS Impersonation ──
        post_id_match = re.search(r"/(?:video|photo|v)/(\d+)", url)
        if post_id_match:
            item_id = post_id_match.group(1)
            api_url = f"https://www.tiktok.com/api/item/detail/?itemId={item_id}"
            api_headers = {
                "User-Agent": SHARED_USER_AGENT,
                "Referer": url,
                "Accept": "application/json, text/plain, */*",
            }
            if has_scrapling:
                try:
                    api_resp = await AsyncFetcher.get(
                        api_url,
                        headers=api_headers,
                        cookies=cookies_dict if cookies_dict else None,
                        timeout=15,
                        impersonate="chrome124",
                    )
                    if api_resp.status == 200:
                        api_data = api_resp.json()
                        image_urls = _extract_tiktok_image_urls_from_rehydration(api_data)
                        caption = api_data.get("itemInfo", {}).get("itemStruct", {}).get("desc") or ""
                        if image_urls:
                            logger.info(f"{TAG_DOWN} TikTok Item API (Scrapling): Berhasil menemukan {len(image_urls)} foto.")
                            return image_urls, caption
                except Exception as sc_api_err:
                    logger.debug(f"Scrapling API fetch note: {sc_api_err}")

            try:
                async with httpx.AsyncClient(
                    headers=api_headers,
                    cookies=cookies_dict if cookies_dict else None,
                    timeout=15.0,
                ) as client:
                    api_resp = await client.get(api_url)
                    if api_resp.status_code == 200:
                        api_data = api_resp.json()
                        image_urls = _extract_tiktok_image_urls_from_rehydration(api_data)
                        caption = api_data.get("itemInfo", {}).get("itemStruct", {}).get("desc") or ""
                        if image_urls:
                            logger.info(f"{TAG_DOWN} TikTok Item API (HTTPX): Berhasil menemukan {len(image_urls)} foto.")
                            return image_urls, caption
            except Exception as e:
                logger.debug(f"TikTok Item Detail API extraction error: {e}")

        return [], ""

    async def _extract_tiktok_carousel_via_browser(
        self, post_url: str, cookies_file: Optional[str] = None
    ) -> tuple[list[str], str, list[dict]]:
        """
        Stealth Browser Fallback Extractor khusus TikTok Carousel / Photo.
        Membuka halaman postingan TikTok di browser stealth, menangkap response API internal,
        dan mengekstrak data rehydration JSON serta DOM image elements.
        """
        # Guard: Jangan pernah jalankan carousel extraction pada URL /video/
        if "/video/" in post_url or "/v/" in post_url:
            return [], "", []

        import os
        import json

        image_urls: list[str] = []
        caption = ""
        browser_cookies: list[dict] = []

        tiktok_session_id = os.getenv("TIKTOK_SESSION_ID")
        cookies_to_add: list[dict] = []

        if cookies_file and Path(cookies_file).exists():
            try:
                parsed_c = self._parse_netscape_cookies(cookies_file)
                for k, v in parsed_c.items():
                    cookies_to_add.append({
                        "name": k,
                        "value": v,
                        "domain": ".tiktok.com",
                        "path": "/",
                        "secure": True,
                    })
            except Exception as e:
                logger.warning(f"Gagal parse Netscape cookie: {e}")

        if not cookies_to_add and tiktok_session_id:
            cookies_to_add.append({
                "name": "sessionid",
                "value": tiktok_session_id.strip(),
                "domain": ".tiktok.com",
                "path": "/",
            })

        async def _carousel_page_action(page):
            nonlocal image_urls, caption, browser_cookies

            # Network Interceptor: Tangkap payload API item/detail internal TikTok
            async def _on_resp(response):
                nonlocal image_urls, caption
                try:
                    r_url = response.url.lower()
                    if ("item/detail" in r_url or "/api/post" in r_url or "aweme/v1" in r_url) and response.status == 200:
                        data = await response.json()
                        extracted = _extract_tiktok_image_urls_from_rehydration(data)
                        if extracted and not image_urls:
                            image_urls = extracted
                except Exception:
                    pass

            page.on("response", _on_resp)
            await asyncio.sleep(2.5)

            # Path A: Rehydration JSON dari state window / DOM
            json_content = await page.evaluate("""() => {
                if (window.__UNIVERSAL_DATA_FOR_REHYDRATION__) {
                    return JSON.stringify(window.__UNIVERSAL_DATA_FOR_REHYDRATION__);
                }
                if (window.SIGI_STATE) {
                    return JSON.stringify(window.SIGI_STATE);
                }
                const el = document.getElementById('__UNIVERSAL_DATA_FOR_REHYDRATION__');
                if (el) return el.textContent;
                const sigi = document.getElementById('SIGI_STATE');
                if (sigi) return sigi.textContent;
                return null;
            }""")

            if json_content and not image_urls:
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

            # Path B: DOM img element selectors khusus Photo Mode (tanpa menangkap cover/thumbnail video)
            if not image_urls:
                image_urls = await page.evaluate("""() => {
                    const results = new Set();
                    document.querySelectorAll('img[src*="photomode"], [data-e2e="photo-mode-image"] img, div[class*="PhotoSwiper"] img, [data-e2e="photo-item"] img, .swiper-slide img').forEach(el => {
                        if (el.src && el.src.startsWith('http') && !el.src.includes('avatar') && !el.src.includes('cover') && !el.src.includes('icon') && !el.src.includes('profile')) {
                            results.add(el.src);
                        }
                    });
                    return Array.from(results);
                }""") or []

            if not caption:
                try:
                    caption = await page.evaluate("""() => {
                        const metaDesc = document.querySelector('meta[property="og:description"]');
                        if (metaDesc && metaDesc.content) return metaDesc.content;
                        const h1 = document.querySelector('h1');
                        if (h1 && h1.innerText) return h1.innerText.trim();
                        return '';
                    }""") or ""
                except Exception:
                    pass

            if hasattr(page, "context"):
                try:
                    browser_cookies = await page.context.cookies()
                except Exception:
                    pass

        try:
            browser = await self.get_browser()
            context = await browser.new_context(
                user_agent=SHARED_USER_AGENT,
                viewport={"width": 1280, "height": 900},
                locale="en-US"
            )
            if cookies_to_add:
                await context.add_cookies(cookies_to_add)
            page = await context.new_page()
            try:
                await page.goto(post_url, wait_until="domcontentloaded", timeout=30000)
                await _carousel_page_action(page)
            finally:
                await context.close()
        except Exception as e:
            logger.warning(f"Browser fallback extractor failed: {e}")

        return image_urls, caption, cast(list[dict], browser_cookies)


    async def _extract_tiktok_video_via_browser(
        self, post_url: str, post_id: str, cookies_file: Optional[str] = None
    ) -> tuple[Optional[Path], Optional[str], str, list[dict]]:
        """
        Stealth Browser Fallback Extractor khusus TikTok Video.
        Menangkap binary video postingan asli langsung dari in-flight network stream Playwright (bypass HTTP 403),
        dengan filter ketat untuk membuang aset animasi splash, watermark, atau logo TikTok.
        """
        captured_file: Optional[Path] = None
        video_url: Optional[str] = None
        caption = ""
        browser_cookies: list[dict] = []
        output_path = self.temp_dir / f"{post_id}_video.mp4"
        output_path.parent.mkdir(parents=True, exist_ok=True)

        BAD_VIDEO_PATTERNS = (
            "logo", "splash", "watermark", "static", "placeholder",
            "intro", "effect", "tiktok_anim", "default_video", "avatar", "icon", "thumb"
        )
        VALID_CDN_DOMAINS = (
            "tiktokcdn.com", "tiktokcdn-us.com", "byteoversea.com",
            "ibyteimg.com", "muscdn.com", "tiktokv.com"
        )

        tiktok_session_id = os.getenv("TIKTOK_SESSION_ID")
        cookies_to_add: list[dict] = []

        if cookies_file and Path(cookies_file).exists():
            try:
                parsed_c = self._parse_netscape_cookies(cookies_file)
                for k, v in parsed_c.items():
                    cookies_to_add.append({
                        "name": k,
                        "value": v,
                        "domain": ".tiktok.com",
                        "path": "/",
                        "secure": True,
                    })
            except Exception as e:
                logger.warning(f"Gagal parse Netscape cookie: {e}")

        if not cookies_to_add and tiktok_session_id:
            cookies_to_add.append({
                "name": "sessionid",
                "value": tiktok_session_id.strip(),
                "domain": ".tiktok.com",
                "path": "/",
            })

        async def _video_page_action(page):
            nonlocal video_url, caption, browser_cookies, captured_file

            # Network Interceptor: Tangkap binary video stream asli kreator & payload item/detail
            async def _on_v_resp(response):
                nonlocal video_url, caption, captured_file
                try:
                    r_url = response.url.lower()
                    c_type = response.headers.get("content-type", "").lower()

                    # 1. In-flight Binary Stream Capture (Bypass HTTP 403 & filter logo/splash)
                    if response.status in (200, 206) and not captured_file:
                        # Abaikan jika mengandung URL aset logo/splash/placeholder statis/preview
                        if any(bad in r_url for bad in BAD_VIDEO_PATTERNS):
                            return
                        # Pastikan berasal dari domain CDN media TikTok resmi
                        if not any(domain in r_url for domain in VALID_CDN_DOMAINS):
                            return

                        if "video/mp4" in c_type or ".mp4" in r_url or "mime_type=video_mp4" in r_url or ("video/" in c_type and "image" not in c_type):
                            try:
                                body = await response.body()
                                # Filter ukuran buffer: video postingan asli >= 150 KB (splash/logo rata-rata ~100 KB)
                                if len(body) >= 150_000:
                                    async with aiofiles.open(output_path, "wb") as f:
                                        await f.write(body)
                                    # Validasi durasi video asli (durasi > 1.0s)
                                    if await self._is_valid_tiktok_video(output_path):
                                        captured_file = output_path
                                        logger.info(f"{TAG_DOWN} Playwright Stream Intercept: Video asli kreator berhasil dicapture ({fmt_size(len(body))})")
                                    else:
                                        output_path.unlink(missing_ok=True)
                            except Exception as b_err:
                                logger.debug(f"Direct stream capture body note: {b_err}")

                    # 2. JSON Payload Parser (Metadata & playAddr)
                    if ("item/detail" in r_url or "/api/post" in r_url or "aweme/v1" in r_url) and response.status == 200:
                        data = await response.json()
                        v_detail = data.get("itemInfo", {}).get("itemStruct", {}) or data.get("aweme_detail", {})
                        if v_detail:
                            v_info = v_detail.get("video", {})
                            cand_url = v_info.get("playAddr") or v_info.get("downloadAddr")
                            if cand_url and isinstance(cand_url, str) and not any(bad in cand_url.lower() for bad in BAD_VIDEO_PATTERNS):
                                if not video_url:
                                    video_url = cand_url
                            if not caption:
                                caption = v_detail.get("desc") or ""
                except Exception:
                    pass

            page.on("response", _on_v_resp)
            await asyncio.sleep(2.0)

            # Tunggu elemen video aktif selesai dirender oleh player browser (bukan splash)
            try:
                await page.wait_for_function(
                    "() => { const v = document.querySelector('video'); return v && (v.duration > 1.0 || isNaN(v.duration)) && !v.src.includes('logo') && !v.src.includes('splash'); }",
                    timeout=10000
                )
            except Exception:
                pass

            # Ambil rehydration JSON
            json_content = await page.evaluate("""() => {
                if (window.__UNIVERSAL_DATA_FOR_REHYDRATION__) {
                    return JSON.stringify(window.__UNIVERSAL_DATA_FOR_REHYDRATION__);
                }
                if (window.SIGI_STATE) {
                    return JSON.stringify(window.SIGI_STATE);
                }
                const el = document.getElementById('__UNIVERSAL_DATA_FOR_REHYDRATION__');
                if (el) return el.textContent;
                const sigi = document.getElementById('SIGI_STATE');
                if (sigi) return sigi.textContent;
                return null;
            }""")

            if json_content and not video_url:
                try:
                    raw_data = json.loads(json_content)
                    try:
                        default_scope = raw_data.get("__DEFAULT_SCOPE__", {})
                        video_detail = default_scope.get("webapp.video-detail", {}) or default_scope.get("webapp.videoDetail", {})
                        if not video_detail:
                            webapp = default_scope.get("webapp", {})
                            video_detail = webapp.get("video-detail", {}) or webapp.get("videoDetail", {})
                        item_struct = video_detail.get("itemInfo", {}).get("itemStruct", {})
                        video_info = item_struct.get("video", {})
                        cand_url = video_info.get("playAddr") or video_info.get("downloadAddr")
                        if cand_url and isinstance(cand_url, str) and not any(bad in cand_url.lower() for bad in BAD_VIDEO_PATTERNS):
                            video_url = cand_url
                        if not caption:
                            caption = item_struct.get("desc") or ""
                        if video_url:
                            logger.info("Berhasil mengekstrak direct video CDN URL postingan asli dari rehydration JSON")
                    except Exception as parse_err:
                        logger.warning(f"Gagal memparsing detail video dari JSON: {parse_err}")
                except Exception as json_err:
                    logger.warning(f"Gagal memparsing rehydration JSON untuk video TikTok: {json_err}")

            # Fallback ke elemen DOM <video> (Pastikan durasi > 1s dan tidak mengandung logo/splash)
            if not video_url:
                video_url = await page.evaluate("""() => {
                    const bad = ["logo", "splash", "watermark", "static", "placeholder", "intro", "effect"];
                    const videos = Array.from(document.querySelectorAll('video'));
                    for (const v of videos) {
                        const src = v.src || (v.querySelector('source') ? v.querySelector('source').src : '');
                        if (src && !src.startsWith('blob:') && !bad.some(b => src.toLowerCase().includes(b))) {
                            if (v.duration > 1 || isNaN(v.duration)) {
                                return src;
                            }
                        }
                    }
                    return null;
                }""")
                if video_url:
                    logger.info("Berhasil mengekstrak video URL postingan asli dari tag video (DOM)")

            if not caption:
                try:
                    caption = await page.evaluate("""() => {
                        const h1 = document.querySelector('h1');
                        if (h1 && h1.innerText) return h1.innerText.trim();
                        const meta = document.querySelector('meta[property="og:description"]');
                        if (meta && meta.content) return meta.content;
                        return '';
                    }""") or ""
                except Exception:
                    pass

            if hasattr(page, "context"):
                try:
                    browser_cookies = await page.context.cookies()
                except Exception:
                    pass

        try:
            browser = await self.get_browser()
            context = await browser.new_context(
                user_agent=SHARED_USER_AGENT,
                viewport={"width": 1280, "height": 900},
                locale="en-US"
            )
            if cookies_to_add:
                await context.add_cookies(cookies_to_add)
            page = await context.new_page()
            try:
                await page.goto(post_url, wait_until="domcontentloaded", timeout=30000)
                await _video_page_action(page)
            finally:
                await context.close()
        except Exception as e:
            logger.warning(f"Browser video fallback extractor failed: {e}")

        return captured_file, video_url, caption, cast(list[dict], browser_cookies)

    async def _is_valid_tiktok_video(self, file_path: Path) -> bool:
        """
        [FILTER VALIDASI VIDEO ASLI TIKTOK]
        Memverifikasi bahwa file video yang diunduh adalah konten postingan asli kreator,
        bukan animasi logo splash / placeholder TikTok (<150KB atau durasi <= 1.0 detik).
        """
        if not file_path or not file_path.exists():
            return False

        # 1. Cek ukuran file: tolak jika di bawah 150 KB (animasi logo placeholder biasanya ~100KB)
        file_size = file_path.stat().st_size
        if file_size < 150_000:
            logger.warning(
                f"{TAG_WARN} File {file_path.name} ditolak: Ukuran ({fmt_size(file_size)}) < 150KB "
                f"(indikasi logo splash / placeholder TikTok)"
            )
            try:
                file_path.unlink(missing_ok=True)
            except Exception:
                pass
            return False

        # 2. Cek durasi video via ffprobe/ffmpeg: tolak jika <= 1.0 detik (video splash/placeholder)
        try:
            duration = await self._get_video_duration(file_path)
            if duration is not None and duration <= 1.0:
                logger.warning(
                    f"{TAG_WARN} File {file_path.name} ditolak: Durasi {duration:.2f}s <= 1.0s "
                    f"(terdeteksi sebagai animasi splash/logo TikTok)"
                )
                try:
                    file_path.unlink(missing_ok=True)
                except Exception:
                    pass
                return False
        except Exception as e:
            logger.debug(f"Pemeriksaan durasi video TikTok note: {e}")

        return True

    async def _download_tiktok_video_clean(
        self, post_url: str, post_id: str, temp_dir: Optional[Path] = None
    ) -> Optional[tuple[list[Path], str, Optional[str]]]:
        """
        Download media TikTok (video / photo carousel) langsung dari public TikWM Clean API.
        Mendukung Multi-Endpoint fallback dan jeda rate-limit.
        Mengembalikan tuple: (downloaded_files, caption, timestamp_str)
        """
        import httpx
        target_dir = temp_dir or self.temp_dir
        target_dir.mkdir(parents=True, exist_ok=True)

        BAD_VIDEO_PATTERNS = (
            "logo", "splash", "watermark_cover", "static_asset", "static",
            "placeholder", "intro", "effect", "tiktok_anim", "default_video",
            "avatar", "icon", "thumb"
        )

        endpoints = [
            f"https://www.tikwm.com/api/?url={post_url}",
            f"https://api.tikwm.com/api/?url={post_url}",
        ]

        headers = {
            "User-Agent": SHARED_USER_AGENT,
            "Referer": "https://www.tikwm.com/",
            "Accept": "application/json, text/plain, */*",
        }

        for ep_url in endpoints:
            try:
                async with httpx.AsyncClient(timeout=20.0, headers=headers, follow_redirects=True) as client:
                    res = await client.get(ep_url)
                    if res.status_code == 200:
                        data = res.json()
                        if data.get("code") == 0 and data.get("data"):
                            d = data["data"]
                            title = d.get("title") or ""
                            create_time = d.get("create_time")
                            timestamp_str = None
                            if create_time:
                                try:
                                    timestamp_str = datetime.fromtimestamp(int(create_time), tz=timezone.utc).isoformat()
                                except Exception:
                                    timestamp_str = str(create_time)

                            # 1. Kasus Photo Carousel (data.images)
                            images = d.get("images")
                            if images and isinstance(images, list) and len(images) > 0:
                                logger.info(f"{TAG_DOWN} TikWM API ({ep_url.split('/')[2]}): Mendeteksi Photo Carousel ({len(images)} gambar)...")
                                downloaded_photos: list[Path] = []
                                for idx, img_url in enumerate(images):
                                    img_path = target_dir / f"{post_id}_{idx+1:03d}.jpg"
                                    img_res = await client.get(img_url)
                                    if img_res.status_code == 200 and len(img_res.content) > 1000:
                                        async with aiofiles.open(img_path, "wb") as f:
                                            await f.write(img_res.content)
                                        downloaded_photos.append(img_path)
                                if downloaded_photos:
                                    logger.info(f"{TAG_DOWN} TikWM Photo Carousel terunduh: {len(downloaded_photos)} gambar")
                                    return downloaded_photos, title, timestamp_str

                            # 2. Kasus Video (data.play, data.hdplay, data.wmplay)
                            play_url = d.get("play") or d.get("hdplay") or d.get("wmplay")
                            if play_url and not any(bad in play_url.lower() for bad in BAD_VIDEO_PATTERNS):
                                out_video_path = target_dir / f"{post_id}_video.mp4"
                                logger.info(f"{TAG_DOWN} TikWM API ({ep_url.split('/')[2]}): Mengunduh stream video asli kreator...")
                                try:
                                    async with client.stream("GET", play_url) as stream_resp:
                                        if stream_resp.status_code in (200, 206):
                                            async with aiofiles.open(out_video_path, "wb") as f:
                                                async for chunk in stream_resp.aiter_bytes(8192):
                                                    await f.write(chunk)
                                except asyncio.CancelledError:
                                    logger.warning(f"{TAG_WARN} TikTok stream download dibatalkan untuk {post_id}")
                                    try:
                                        out_video_path.unlink(missing_ok=True)
                                    except Exception:
                                        pass
                                    raise

                                if await self._is_valid_tiktok_video(out_video_path):
                                    logger.info(f"{TAG_DOWN} TikWM Video terunduh: {out_video_path.name} ({fmt_size(out_video_path.stat().st_size)})")
                                    return [out_video_path], title, timestamp_str
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.debug(f"TikWM endpoint {ep_url} error: {e}")

            # Jeda 1.0 detik hanya jika endpoint ini gagal/tidak menghasilkan file valid (retry backoff)
            await asyncio.sleep(1.0)

        return None

    async def _download_via_tikwm(self, url: str, post_id: str) -> tuple[Optional[Path], str]:
        """
        Download video TikTok menggunakan TikWM Clean API helper (kompatibilitas backward).
        """
        res = await self._download_tiktok_video_clean(url, post_id)
        if res and res[0] and res[0][0].exists():
            return res[0][0], res[1]
        return None, ""

    async def _download_tiktok_cdn_video(
        self,
        video_url: str,
        post_id: str,
        browser_cookies: list[dict],
        cookies_file: Optional[str] = None,
    ) -> Optional[Path]:
        """
        Download TikTok CDN video URL dengan full session cookie auth untuk bypass HTTP 403.
        """
        import httpx

        output_path = self.temp_dir / f"{post_id}_video.mp4"
        output_path.parent.mkdir(parents=True, exist_ok=True)

        BAD_VIDEO_PATTERNS = (
            "logo", "splash", "watermark", "static", "placeholder",
            "intro", "effect", "tiktok_anim", "default_video", "avatar", "icon", "thumb"
        )
        if any(bad in video_url.lower() for bad in BAD_VIDEO_PATTERNS):
            logger.warning(f"TikTok CDN: Skip download karena URL terdeteksi sebagai asset statis/logo: {video_url}")
            return None

        cookies_dict: dict = {}
        netscape_path = Path("sessions/tiktok_cookies.txt")
        if cookies_file and Path(cookies_file).exists():
            netscape_path = Path(cookies_file)
        if netscape_path.exists():
            cookies_dict.update(self._parse_netscape_cookies(str(netscape_path)))

        for c in browser_cookies:
            name = c.get("name", "")
            value = c.get("value", "")
            if name and value:
                cookies_dict[name] = value

        tiktok_session_id = os.getenv("TIKTOK_SESSION_ID", "").strip()
        if tiktok_session_id and "sessionid" not in cookies_dict:
            cookies_dict["sessionid"] = tiktok_session_id

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Referer": "https://www.tiktok.com/",
            "Origin": "https://www.tiktok.com",
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "Range": "bytes=0-",
        }

        # Coba curl_cffi dulu
        try:
            from curl_cffi import requests as curl_requests
            logger.info(f"TikTok CDN: Mengunduh via curl_cffi chrome impersonation...")
            async with curl_requests.AsyncSession(impersonate="chrome124") as session:
                resp = await session.get(
                    video_url,
                    headers=headers,
                    cookies=cookies_dict,
                    timeout=120,
                    allow_redirects=True,
                )
                if resp.status_code in (200, 206) and resp.content and len(resp.content) > 300_000:
                    async with aiofiles.open(output_path, "wb") as f:
                        await f.write(resp.content)
                    logger.info(f"TikTok CDN: Download sukses via curl_cffi ({output_path.stat().st_size // 1024} KB)")
                    return output_path
                else:
                    logger.warning(f"TikTok CDN curl_cffi: HTTP {resp.status_code} (size={len(resp.content) if resp.content else 0}) — fallback ke httpx")
        except (ImportError, ModuleNotFoundError):
            logger.debug("curl_cffi tidak tersedia, menggunakan httpx")
        except Exception as ce:
            logger.warning(f"TikTok CDN curl_cffi error: {ce} — fallback ke httpx")

        # Fallback: httpx dengan streaming
        try:
            cookie_str = "; ".join(f"{k}={v}" for k, v in cookies_dict.items())
            if cookie_str:
                headers["Cookie"] = cookie_str

            logger.info(f"TikTok CDN: Mengunduh via httpx dengan {len(cookies_dict)} cookies...")
            async with httpx.AsyncClient(
                headers=headers,
                timeout=120.0,
                follow_redirects=True,
            ) as client:
                async with client.stream("GET", video_url) as resp:
                    if resp.status_code in (200, 206):
                        async with aiofiles.open(output_path, "wb") as f:
                            async for chunk in resp.aiter_bytes(8192):
                                await f.write(chunk)
                        if output_path.exists() and output_path.stat().st_size > 300_000:
                            logger.info(f"TikTok CDN: Download sukses via httpx ({output_path.stat().st_size // 1024} KB)")
                            return output_path
                        else:
                            logger.warning(f"TikTok CDN httpx: Ukuran video terlalu kecil ({output_path.stat().st_size if output_path.exists() else 0} B) — file splash/placeholder")
                            if output_path.exists():
                                output_path.unlink(missing_ok=True)
                            return None
                    else:
                        logger.error(f"TikTok CDN httpx: HTTP {resp.status_code} — cookies tidak valid atau URL expired")
                        return None
        except Exception as he:
            logger.error(f"TikTok CDN httpx error: {he}")
            return None

    async def _run_ytdlp_async(
        self,
        url: str,
        post_id: str,
        cookies_file: Optional[str] = None,
        out_tmpl: Optional[str] = None,
    ) -> tuple[list[Path], str, Optional[str]]:
        """
        Download media menggunakan yt-dlp via isolated async subprocess.
        Kebal terhadap SIGSEGV / crash interpreter Python (code 139) karena berjalan
        di proses terpisah yang diisolasi oleh OS.
        """
        out_template = out_tmpl or str(self.temp_dir / f"{post_id}_%(autonumber)s.%(ext)s")
        cmd = [
            "yt-dlp",
            "--no-warnings",
            "--no-overwrites",
            "--format", "best[ext=mp4]/bestvideo+bestaudio/best",
            "-o", out_template,
            "--print", "after_move:title",
            "--print", "after_move:description",
            "--print", "after_move:upload_date",
        ]

        if cookies_file and Path(cookies_file).exists():
            cmd += ["--cookies", cookies_file]

        if "tiktok.com" in url:
            cmd += [
                "--extractor-args", "tiktok:api_hostname=api16-normal-c-useast1a.tiktokv.com",
                "--add-header", f"User-Agent:{SHARED_USER_AGENT}",
                "--add-header", "Referer:https://www.tiktok.com/",
            ]
        elif "x.com" in url or "twitter.com" in url:
            cmd += [
                "--add-header", f"User-Agent:{SHARED_USER_AGENT}",
                "--add-header", "Referer:https://x.com/",
            ]

        cmd.append(url)

        caption = ""
        timestamp = None
        proc = None

        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=180.0)

            if stdout:
                lines = [line.strip() for line in stdout.decode(errors="ignore").splitlines() if line.strip()]
                if lines:
                    caption = lines[0]
                    if len(lines) > 1 and lines[1] and lines[1] != "NA":
                        caption = lines[1]
                    for l in lines:
                        if len(l) == 8 and l.isdigit():
                            try:
                                from datetime import datetime, timezone
                                dt = datetime.strptime(l, "%Y%m%d").replace(tzinfo=timezone.utc)
                                timestamp = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
                                break
                            except Exception:
                                pass

            # Scoped glob hanya untuk post_id ini — hemat disk IOPS dibanding recursive rglob
            new_files = [f for f in self.temp_dir.glob(f"{post_id}_*") if f.is_file()]
            return new_files, caption, timestamp

        except asyncio.TimeoutError:
            logger.error(f"{TAG_ERROR} yt-dlp timeout setelah 180s untuk {url} — membunuh child process...")
            if proc:
                try:
                    proc.kill()
                    await asyncio.wait_for(proc.wait(), timeout=5.0)
                except Exception:
                    pass
            return [], "", None
        except Exception as e:
            logger.error(f"{TAG_ERROR} yt-dlp subprocess error untuk {url}: {e}")
            if proc:
                try:
                    proc.kill()
                    await proc.wait()
                except Exception:
                    pass
            return [], "", None

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
            # FIX: scope to flat glob instead of recursive rglob to avoid O(N) scan
            # of the entire temp_dir on every gallery-dl call (expensive on busy servers
            # with thousands of leftover files). gallery-dl writes flat into --dest dir.
            before = set(self.temp_dir.glob("*.*"))

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
                    # Supports both curl_cffi (aiter_content) and httpx (aiter_bytes)
                    async with aiofiles.open(output_path, "wb") as f:
                        if hasattr(response, "aiter_content"):
                            async for chunk in response.aiter_content(8192):
                                await f.write(chunk)
                        elif hasattr(response, "aiter_bytes"):
                            async for chunk in response.aiter_bytes(8192):
                                await f.write(chunk)
                        else:
                            await f.write(response.content)
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

        # Cek ketersediaan binary ffmpeg di system PATH sebelum memulai
        if not shutil.which("ffmpeg"):
            logger.warning(
                f"{TAG_COMPR} {file_path.name} ({fmt_size(file_size)}) melebihi limit ({fmt_size(self.max_file_size_bytes)}), "
                f"tetapi FFmpeg binary tidak terpasang di system PATH — melewati kompresi dan kirim file original."
            )
            return file_path, True

        logger.info(
            f"{TAG_COMPR} {file_path.name} ({fmt_size(file_size)}) melebihi limit "
            f"— memulai kompresi ffmpeg..."
        )

        # Ambil durasi video menggunakan ffprobe / ffmpeg
        duration = await self._get_video_duration(file_path)
        if duration and duration > 0:
            target_bitrate_bps = self._calculate_target_bitrate(duration)
        else:
            # Fallback jika durasi tidak bisa diketahui: gunakan fallback bitrate 1 Mbps
            logger.warning(f"{TAG_WARN} Durasi video tidak terdeteksi \u2014 fallback bitrate 1 Mbps.")
            target_bitrate_bps = 1_000_000

        # Jalankan kompresi via asyncio subprocess (non-blocking, no executor needed)
        compressed_path = file_path.with_stem(file_path.stem + "_compressed")
        t_compress_start = _time.monotonic()
        # FIX: _run_ffmpeg_compress is now async (uses asyncio.create_subprocess_exec),
        # so we await it directly instead of wrapping in run_in_executor.
        # This eliminates thread pool saturation when multiple compressions run concurrently.
        success = await self._run_ffmpeg_compress(
            file_path,
            compressed_path,
            target_bitrate_bps,
        )

        if not success or not compressed_path.exists():
            logger.error(f"{TAG_COMPR} ffmpeg gagal \u2014 pakai file original {file_path.name}.")
            return file_path, True

        compressed_size = compressed_path.stat().st_size
        elapsed = _time.monotonic() - t_compress_start
        logger.info(
            f"{TAG_COMPR} Selesai: {fmt_size(file_size)} \u2192 {fmt_size(compressed_size)} "
            f"({fmt_duration(elapsed)})"
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
                f"{TAG_WARN} {file_path.name}: {fmt_size(compressed_size)} setelah kompresi "
                f"(safety floor aktif) \u2014 kirim sebagai dokumen."
            )

        return compressed_path, still_over_limit

    def _calculate_target_bitrate(self, duration_seconds: float) -> int:
        """
        Hitung target bitrate video untuk kompresi ffmpeg secara dinamis.
        """
        raw_bitrate = (self.target_file_size_bytes * 8) / duration_seconds - AUDIO_BITRATE_BPS

        if raw_bitrate < MIN_VIDEO_BITRATE_BPS:
            logger.warning(
                f"{TAG_COMPR} Kalkulasi bitrate ({raw_bitrate:.0f} bps) di bawah safety floor "
                f"({MIN_VIDEO_BITRATE_BPS} bps) \u2014 pakai floor value."
            )
            return MIN_VIDEO_BITRATE_BPS

        result_kbps = int(raw_bitrate // 1000) * 1000
        logger.debug(f"{TAG_COMPR} Target bitrate: {result_kbps // 1000} kbps (durasi: {duration_seconds:.1f}s)")
        return result_kbps

    async def _get_video_duration(self, file_path: Path) -> Optional[float]:
        """
        Ambil durasi video menggunakan ffprobe, dengan fallback ke ffmpeg -i jika ffprobe tidak ditemukan/error.
        """
        has_ffprobe = bool(shutil.which("ffprobe"))
        has_ffmpeg = bool(shutil.which("ffmpeg"))

        if not has_ffprobe and not has_ffmpeg:
            logger.warning(f"{TAG_WARN} ffprobe/ffmpeg binary tidak terpasang di PATH — skip duration probe.")
            return None

        # Method 1: ffprobe
        if has_ffprobe:
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
        if has_ffmpeg:
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

    async def _run_ffmpeg_compress(
        self,
        input_path: Path,
        output_path: Path,
        target_bitrate_bps: int,
    ) -> bool:
        """
        Jalankan ffmpeg untuk mengompres video dengan target bitrate tertentu.
        Menggunakan asyncio.create_subprocess_exec (non-blocking) agar event loop
        tidak tertahan selama kompresi — konsisten dengan pola yt-dlp di codebase ini.
        """
        if not shutil.which("ffmpeg"):
            logger.error("ffmpeg binary tidak terpasang di system PATH.")
            return False
        target_kbps = target_bitrate_bps // 1000
        bufsize_kbps = target_kbps * 2

        # FIX: hapus `-threads 2` hardcode — biarkan ffmpeg auto-detect jumlah thread
        # optimal. Di container 1-vCPU, -threads 2 menyebabkan context-switch overhead
        # yang tidak perlu. Bisa di-override via FFMPEG_THREADS env jika diperlukan.
        ffmpeg_threads = os.getenv("FFMPEG_THREADS", "")
        thread_args = ["-threads", ffmpeg_threads] if ffmpeg_threads.isdigit() else []

        cmd = [
            "ffmpeg",
            "-i", str(input_path),
            "-c:v", "libx264",
            "-preset", "veryfast",
            *thread_args,
            "-b:v", f"{target_kbps}k",
            "-maxrate", f"{target_kbps}k",
            "-bufsize", f"{bufsize_kbps}k",
            "-fs", f"{self.target_file_size_bytes}",
            "-c:a", "aac",
            "-b:a", "128k",
            "-movflags", "+faststart",
            "-y",
            str(output_path),
        ]

        logger.info(f"ffmpeg kompresi: target {target_kbps}kbps → {output_path.name}")

        proc = None
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            try:
                _, stderr_bytes = await asyncio.wait_for(proc.communicate(), timeout=90.0)
            except asyncio.TimeoutError:
                logger.error("ffmpeg timeout setelah 90 detik — membunuh proses...")
                if proc is not None:
                    proc.kill()
                    try:
                        await asyncio.wait_for(proc.wait(), timeout=5.0)
                    except asyncio.TimeoutError:
                        pass
                return False

            if proc is not None and proc.returncode != 0:
                stderr_text = (stderr_bytes or b"").decode("utf-8", errors="replace")[-500:]
                logger.error(f"ffmpeg error: {stderr_text}")
                return False
            return True
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

"""
scrapers/base.py
Abstract base class untuk semua platform scraper.

Mendefinisikan:
- Kontrak interface yang harus diimplementasikan setiap scraper
- Dataclass PostMedia sebagai format data standar antar scraper
- Enum MediaType untuk klasifikasi tipe media
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
import json
import os
import logging
from pathlib import Path
from typing import AsyncGenerator, Optional
import aiofiles

logger = logging.getLogger(__name__)

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/125.0.0.0 Safari/537.36"
)


class MediaType(Enum):
    """Tipe media yang didukung oleh bot."""

    PHOTO = "photo"
    VIDEO = "video"
    CAROUSEL = "carousel"  # Postingan multi-media (foto+video campur)
    UNKNOWN = "unknown"


@dataclass
class PostMedia:
    """
    Representasi standar sebuah postingan media.
    Semua scraper harus mengembalikan data dalam format ini
    agar pipeline download/send dapat bekerja secara seragam.
    """

    post_id: str  # ID unik postingan (dari platform)
    post_url: str  # URL postingan lengkap
    profile_url: str  # URL profil sumber
    platform: str  # 'tiktok', 'instagram', 'twitter'
    media_type: MediaType = MediaType.UNKNOWN
    media_urls: list[str] = field(
        default_factory=list
    )  # URL langsung media (jika tersedia)
    caption: str = ""  # Caption/deskripsi postingan
    timestamp: Optional[str] = None  # Waktu posting (ISO format)
    # Path file cookies format Netscape untuk yt-dlp (hanya untuk platform yang butuh auth)
    # Instagram: wajib diisi agar yt-dlp bisa download konten yang memerlukan login
    # TikTok/Twitter: None jika tidak diperlukan
    cookies_file: Optional[str] = None


class BaseScraper(ABC):
    """
    Abstract base class yang mendefinisikan kontrak scraper.

    Setiap scraper platform harus mengimplementasikan method:
    - `scrape_profile`: Generator yang menghasilkan PostMedia dari profil
    - `close`: Cleanup resources (tutup browser, dll)
    """

    def __init__(self, db_manager, session_dir: str):
        self.db = db_manager
        self.session_dir = session_dir
        self.failed = False

    @abstractmethod
    async def scrape_profile(
        self,
        profile_url: str,
        forced: bool = False,
    ) -> AsyncGenerator[PostMedia, None]:
        """
        Generator asinkron yang menghasilkan PostMedia satu per satu.
        Harus diimplementasikan oleh setiap scraper platform.

        Urutan: dari post PALING BARU ke PALING LAMA.
        (Scraper mengumpulkan semua ID, lalu membalik urutan sebelum yield
         sehingga yang paling lama dikirim ke Discord duluan.)

        Yields:
            PostMedia: Data postingan yang siap diproses downloader.
        """
        if False:
            yield  # type: ignore
        ...

    @abstractmethod
    async def close(self) -> None:
        """Tutup semua resource (browser, koneksi, dll) dengan aman."""
        ...

    # ──────────────────────────────────────────────
    # Peta token .env per platform
    # ──────────────────────────────────────────────
    # Prioritas: .env session token → JSON cookie file → gagal (skip)
    ENV_TOKEN_MAP = {
        "instagram": [
            {
                "env_key": "INSTAGRAM_SESSION_ID",
                "name": "sessionid",
                "domain": ".instagram.com",
            },
        ],
        "tiktok": [
            {
                "env_key": "TIKTOK_SESSION_ID",
                "name": "sessionid",
                "domain": ".tiktok.com",
            },
        ],
        "twitter": [
            {"env_key": "TWITTER_AUTH_TOKEN", "name": "auth_token", "domain": ".x.com"},
            {"env_key": "TWITTER_CT0", "name": "ct0", "domain": ".x.com"},
        ],
    }

    @staticmethod
    def has_auth_configured(platform: str) -> bool:
        """
        Pre-flight check: apakah platform ini punya auth yang valid?
        Cek .env token ATAU JSON cookie file. Dipakai bot.py sebelum spawn scraper.
        """
        # Cek 1: .env session token
        token_specs = BaseScraper.ENV_TOKEN_MAP.get(platform, [])
        if token_specs:
            # Semua token harus ada (Twitter butuh 2: auth_token + ct0)
            all_present = all(os.getenv(spec["env_key"]) for spec in token_specs)
            if all_present:
                return True

        # Cek 2: JSON cookie file
        BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        cookie_file = os.path.join(BASE_DIR, "config", "cookies", f"{platform}.json")
        if os.path.exists(cookie_file):
            return True

        return False

    @staticmethod
    def get_brave_path() -> Optional[str]:
        """
        Deteksi otomatis path Brave browser ATAU Google Chrome di Windows OS.
        Membantu mengurangi duplikasi pencarian path di setiap subclass scraper.
        Mendukung override via variabel lingkungan BROWSER_EXECUTABLE_PATH atau BRAVE_EXECUTABLE_PATH.
        """
        # 1. Cek explicit override dari env
        for env_key in ["BROWSER_EXECUTABLE_PATH", "BRAVE_EXECUTABLE_PATH"]:
            env_val = os.getenv(env_key)
            if env_val and os.path.exists(env_val):
                return env_val

        # 2. Daftar path Brave Browser
        brave_paths = [
            r"C:\Program Files\BraveSoftware\Brave-Browser\Application\brave.exe",
            r"C:\Program Files (x86)\BraveSoftware\Brave-Browser\Application\brave.exe",
            os.path.expandvars(
                r"%LOCALAPPDATA%\BraveSoftware\Brave-Browser\Application\brave.exe"
            ),
        ]
        for path in brave_paths:
            if os.path.exists(path):
                return path

        # 3. Fallback ke Google Chrome jika Brave tidak ditemukan
        chrome_paths = [
            r"C:\Program Files\Google\Chrome\Application\chrome.exe",
            r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
            os.path.expandvars(
                r"%LOCALAPPDATA%\Google\Chrome\Application\chrome.exe"
            ),
        ]
        for path in chrome_paths:
            if os.path.exists(path):
                logger.info(f"Brave tidak ditemukan, menggunakan Google Chrome dari: {path}")
                return path

        return None

    @staticmethod
    def get_browser_launch_kwargs(proxy: Optional[str] = None) -> dict:
        """
        Bangun kwarg launch browser Playwright terpusat dengan dukungan proxy opsional.
        Format proxy: 'http://username:password@ip:port' atau 'http://ip:port'
        """
        brave_path = BaseScraper.get_brave_path()
        launch_kwargs: dict = {
            "args": [
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-infobars",
                "--window-position=0,0",
                "--ignore-certificate-errors",
                "--disable-web-security",
                "--disable-features=IsolateOrigins,site-per-process",
            ],
        }
        if brave_path:
            launch_kwargs["executable_path"] = brave_path

        if proxy:
            launch_kwargs["proxy"] = {"server": proxy}

        return launch_kwargs

    async def load_and_inject_cookies(self, context, platform: str) -> Optional[Path]:
        """
        Injeksi cookie ke Playwright context + export Netscape untuk yt-dlp.

        Prioritas sumber:
          1. .env session token (INSTAGRAM_SESSION_ID, TIKTOK_SESSION_ID, TWITTER_AUTH_TOKEN+CT0)
          2. Static JSON file dari config/cookies/<platform>.json
          3. Tidak ada keduanya → log error, return None (akun di-skip)
        """
        cookies_to_inject: list[dict] = []
        source = ""  # Untuk logging: "env" atau "json"

        # ── Prioritas 1: Bangun cookie dari .env session token ──
        token_specs = self.ENV_TOKEN_MAP.get(platform, [])
        if token_specs:
            env_cookies = []
            all_present = True

            for spec in token_specs:
                token_value = os.getenv(spec["env_key"])
                if token_value:
                    # Bersihkan whitespace tak sengaja agar Playwright tidak error
                    env_cookies.append(
                        {
                            "name": spec["name"],
                            "value": token_value.strip(),
                            "domain": spec["domain"],
                            "path": "/",
                            "secure": True,
                            "httpOnly": True,
                        }
                    )
                else:
                    all_present = False

            # Hanya pakai .env jika SEMUA token untuk platform ini ada
            if all_present and env_cookies:
                cookies_to_inject = env_cookies
                source = "env"
                logger.info(
                    f"[⚙️ SYSTEM] Cookie {platform} dibangun dari .env session token "
                    f"({len(env_cookies)} cookie)."
                )

        # ── Prioritas 2: Fallback ke static JSON file ──
        if not cookies_to_inject:
            BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            COOKIE_DIR = os.path.join(BASE_DIR, "config", "cookies")
            os.makedirs(COOKIE_DIR, exist_ok=True)

            cookie_file = Path(os.path.join(COOKIE_DIR, f"{platform}.json"))

            if cookie_file.exists():
                try:
                    async with aiofiles.open(cookie_file, "r", encoding="utf-8") as f:
                        content = await f.read()
                    json_cookies = json.loads(content)

                    if json_cookies:
                        cookies_to_inject = json_cookies
                        source = "json"
                        logger.info(
                            f"[⚙️ SYSTEM] Cookie {platform} dimuat dari JSON file: {cookie_file.name}"
                        )
                    else:
                        logger.warning(
                            f"[⚠️ WARN  ] File JSON cookie {platform} kosong."
                        )
                except (json.JSONDecodeError, IOError) as e:
                    logger.error(
                        f"[❌ ERROR  ] Gagal membaca JSON cookie {platform}: {e}"
                    )

        # ── Tidak ada sumber auth sama sekali ──
        if not cookies_to_inject:
            logger.error(
                f"[❌ ERROR  ] Tidak ada autentikasi untuk {platform}! "
                f"Isi .env ({', '.join(s['env_key'] for s in token_specs)}) "
                f"atau taruh config/cookies/{platform}.json. Melewati akun..."
            )
            return None

        # ── Injeksi ke Playwright browser context ──
        try:
            await context.add_cookies(cookies_to_inject)
            logger.info(
                f"[⚙️ SYSTEM] {len(cookies_to_inject)} cookie berhasil diinjeksi "
                f"ke {platform} (sumber: {source})."
            )
        except Exception as e:
            logger.error(
                f"[❌ ERROR  ] Gagal menginjeksi cookie {platform}: {e}",
                exc_info=True,
            )
            return None

        # ── Export ke Netscape format untuk yt-dlp ──
        try:
            netscape_path = Path(self.session_dir) / f"{platform}_cookies.txt"
            netscape_path.parent.mkdir(parents=True, exist_ok=True)

            lines = [
                "# Netscape HTTP Cookie File\n",
                "# Generated by bot session export for yt-dlp\n\n",
            ]
            for cookie in cookies_to_inject:
                domain = cookie.get("domain", "")
                include_subdomains = "TRUE" if domain.startswith(".") else "FALSE"
                path = cookie.get("path", "/")
                secure = "TRUE" if cookie.get("secure", False) else "FALSE"
                expires = cookie.get("expires")
                # FIX: expires=None or 0 writes epoch-0 (Jan 1 1970) → cookie rejected as expired
                # Use 2147483647 (Year 2038, max 32-bit epoch) as far-future sentinel
                expires = int(expires) if expires and int(expires) > 0 else 2147483647
                name = cookie.get("name", "")
                value = cookie.get("value", "")
                lines.append(
                    f"{domain}\t{include_subdomains}\t{path}\t"
                    f"{secure}\t{expires}\t{name}\t{value}\n"
                )

            async with aiofiles.open(netscape_path, "w", encoding="utf-8") as f:
                await f.writelines(lines)

            logger.info(
                f"[⚙️ SYSTEM] Netscape cookies {platform} diekspor ke: {netscape_path}"
            )
            return netscape_path

        except Exception as e:
            logger.error(
                f"[❌ ERROR  ] Gagal menulis Netscape cookie {platform}: {e}",
                exc_info=True,
            )
            return None

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

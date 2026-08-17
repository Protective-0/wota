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
        cookie_dir = Path(os.getenv("COOKIE_DIR", Path.cwd() / "config" / "cookies"))
        cookie_file = cookie_dir / f"{platform}.json"
        if cookie_file.exists():
            return True

        return False

    @staticmethod
    def get_brave_path() -> Optional[str]:
        """
        Deteksi otomatis path browser di Windows dan Linux/Debian.
        Mendukung override via BROWSER_EXECUTABLE_PATH atau BRAVE_EXECUTABLE_PATH.
        Di Linux/Docker: deteksi system Chromium terlebih dahulu sebelum fallback ke
        Playwright built-in (agar tidak re-download browser tiap deploy).
        """
        # 1. Cek explicit override dari env
        for env_key in ["BROWSER_EXECUTABLE_PATH", "BRAVE_EXECUTABLE_PATH"]:
            env_val = os.getenv(env_key)
            if env_val and os.path.exists(env_val):
                return env_val

        import platform as _platform_module
        system = _platform_module.system()

        if system == "Linux":
            # Debian/Ubuntu server: deteksi Chromium dan Chrome system-wide
            linux_paths = [
                "/usr/bin/chromium",
                "/usr/bin/chromium-browser",
                "/usr/bin/google-chrome",
                "/usr/bin/google-chrome-stable",
                "/snap/bin/chromium",
            ]
            for path in linux_paths:
                if os.path.exists(path):
                    logger.info(f"Linux system browser ditemukan: {path}")
                    return path
            # Tidak ada system browser → Playwright unduh Chromium sendiri (normal untuk Docker)
            return None

        # 2. Windows: Brave Browser
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

        # 3. Windows: Fallback ke Google Chrome
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

        PENTING — Flag Linux/Docker wajib:
        --disable-dev-shm-usage: Cegah crash SIGBUS di Docker. Default /dev/shm Docker
            hanya 64MB. Chromium pakai shared memory untuk rendering. Tanpa flag ini,
            scraping halaman media-heavy (Instagram grid, TikTok profile) = SIGBUS crash.
            Dengan flag ini Chromium pakai /tmp sebagai fallback shared memory.
        --disable-gpu: Tidak ada GPU di headless server — skip GPU init yang bisa hang.
        """
        brave_path = BaseScraper.get_brave_path()
        launch_kwargs: dict = {
            "args": [
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",               # Wajib di Docker (no root namespace)
                "--disable-setuid-sandbox",   # Wajib di Docker
                "--disable-dev-shm-usage",    # KRITIS: cegah SIGBUS crash di Docker/Debian
                "--disable-gpu",              # Tidak ada GPU di headless server
                "--disable-software-rasterizer",
                "--disable-infobars",
                "--window-position=0,0",
                "--ignore-certificate-errors",
                "--disable-web-security",
                "--disable-features=IsolateOrigins,site-per-process",
                "--no-first-run",
                "--no-default-browser-check",
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
          1. Full JSON cookie jar dari config/cookies/<platform>.json (hasil export browser)
          2. Fallback ke .env session token (INSTAGRAM_SESSION_ID, TIKTOK_SESSION_ID, TWITTER_AUTH_TOKEN+CT0)
          3. Tidak ada keduanya → log error, return None (akun di-skip)
        """
        cookies_to_inject: list[dict] = []
        source = ""  # Untuk logging: "json" atau "env"

        # ── Prioritas 1: File cookie dari config/cookies/<platform>.json atau <platform>.txt ──
        cookie_dir = Path(os.getenv("COOKIE_DIR", Path.cwd() / "config" / "cookies"))
        cookie_candidates = [
            cookie_dir / f"{platform}.json",
            cookie_dir / f"{platform}.txt",
        ]

        for cookie_file in cookie_candidates:
            if not cookie_file.exists():
                continue

            try:
                async with aiofiles.open(cookie_file, "r", encoding="utf-8") as f:
                    content = (await f.read()).strip()

                if not content:
                    continue

                valid_cookies = []

                # Format A: JSON Array (Cookie-Editor format)
                if content.startswith("[") or content.startswith("{"):
                    json_data = json.loads(content)
                    if isinstance(json_data, dict):
                        json_data = [json_data]
                    if isinstance(json_data, list):
                        for c in json_data:
                            if isinstance(c, dict) and "name" in c and "value" in c:
                                norm_c: dict = {
                                    "name": str(c["name"]).strip(),
                                    "value": str(c["value"]).strip(),
                                    "domain": str(c.get("domain", "")).strip(),
                                    "path": str(c.get("path", "/")).strip(),
                                }
                                if "secure" in c:
                                    norm_c["secure"] = bool(c["secure"])
                                if "httpOnly" in c:
                                    norm_c["httpOnly"] = bool(c["httpOnly"])
                                if "sameSite" in c and c["sameSite"]:
                                    ss = str(c["sameSite"]).capitalize()
                                    if ss in ("Strict", "Lax", "None"):
                                        norm_c["sameSite"] = ss
                                exp = c.get("expirationDate") or c.get("expires")
                                if exp:
                                    try:
                                        norm_c["expires"] = int(float(exp))
                                    except (ValueError, TypeError):
                                        pass
                                valid_cookies.append(norm_c)
                else:
                    # Format B: Netscape HTTP Cookie format (Get cookies.txt format)
                    for line in content.splitlines():
                        line = line.strip()
                        if not line or line.startswith("#"):
                            continue
                        parts = line.split("\t")
                        if len(parts) >= 7:
                            domain, include_sub, path, secure, expires, name, value = parts[:7]
                            norm_c = {
                                "name": name.strip(),
                                "value": value.strip(),
                                "domain": domain.strip(),
                                "path": path.strip() or "/",
                                "secure": secure.upper() == "TRUE",
                                "httpOnly": True,
                            }
                            if expires and expires.isdigit():
                                exp_int = int(expires)
                                if exp_int > 0:
                                    norm_c["expires"] = exp_int
                            valid_cookies.append(norm_c)

                if valid_cookies:
                    cookies_to_inject = valid_cookies
                    source = "file"
                    logger.info(
                        f"[⚙️ SYSTEM] Cookie {platform} berhasil dimuat dari file: {cookie_file.name} "
                        f"({len(valid_cookies)} cookies)."
                    )
                    break
            except Exception as e:
                logger.warning(f"Gagal membaca file cookie {platform} ({cookie_file.name}): {e}")

        # ── Prioritas 2: Fallback ke .env session token ──
        if not cookies_to_inject:
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

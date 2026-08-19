"""
scrapers/base.py
Abstract base class untuk semua platform scraper dengan integrasi Scrapling Stealth Engine.

Mendefinisikan:
- Kontrak interface yang harus diimplementasikan setiap scraper
- Dataclass PostMedia sebagai format data standar antar scraper
- Enum MediaType untuk klasifikasi tipe media
- Centralized Scrapling Fetcher Factory (AsyncFetcher, StealthyFetcher, DynamicFetcher)
- Docker & Linux Headless stealth flags injection (--no-sandbox, --disable-dev-shm-usage)
- Manajemen cookie universal (JSON, Netscape, .env)
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
import json
import logging
import os
from pathlib import Path
import platform as _platform_module
from typing import Any, AsyncGenerator, Callable, Optional, Union
import aiofiles

try:
    from scrapling.fetchers import AsyncFetcher, DynamicFetcher, StealthyFetcher
    HAS_SCRAPLING = True
except ImportError:
    AsyncFetcher = None  # type: ignore
    DynamicFetcher = None  # type: ignore
    StealthyFetcher = None  # type: ignore
    HAS_SCRAPLING = False

logger = logging.getLogger(__name__)

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/125.0.0.0 Safari/537.36"
)

# Standard Chromium / Scrapling stealth flags untuk Linux headless & Docker container
DOCKER_CHROMIUM_FLAGS = [
    "--no-sandbox",                  # Wajib di Docker (mencegah error user namespace non-root)
    "--disable-setuid-sandbox",      # Sandbox fallback untuk Debian/Ubuntu container
    "--disable-dev-shm-usage",       # KRITIS: cegah SIGBUS crash di Docker saat scraping media berat
    "--disable-gpu",                 # Nonaktifkan GPU init pada server headless
    "--disable-software-rasterizer", # Hemat memori CPU
    "--disable-blink-features=AutomationControlled", # Anti-detection layer
    "--disable-web-security",        # Mencegah CORS blocking saat scraping resource CDN
    "--ignore-certificate-errors",
    "--disable-infobars",
    "--window-position=0,0",
    "--disable-features=IsolateOrigins,site-per-process",
    "--no-first-run",
    "--no-default-browser-check",
]


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
    Abstract base class yang mendefinisikan kontrak scraper dengan dukungan Scrapling.

    Setiap scraper platform harus mengimplementasikan method:
    - `scrape_profile`: Generator yang menghasilkan PostMedia dari profil
    - `close`: Cleanup resources (tutup browser/fetcher, dll)
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
        """Tutup semua resource (browser, fetcher, koneksi) dengan aman."""
        ...

    # ──────────────────────────────────────────────
    # Peta token .env per platform
    # ──────────────────────────────────────────────
    # Prioritas: .env session token → JSON cookie file → guest mode
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
        Cek .env token ATAU JSON cookie file.
        """
        token_specs = BaseScraper.ENV_TOKEN_MAP.get(platform, [])
        if token_specs:
            all_present = all(os.getenv(spec["env_key"]) for spec in token_specs)
            if all_present:
                return True

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
        """
        for env_key in ["BROWSER_EXECUTABLE_PATH", "BRAVE_EXECUTABLE_PATH"]:
            env_val = os.getenv(env_key)
            if env_val and os.path.exists(env_val):
                return env_val

        system = _platform_module.system()

        if system == "Linux":
            linux_paths = [
                "/usr/bin/chromium",
                "/usr/bin/chromium-browser",
                "/usr/bin/google-chrome",
                "/usr/bin/google-chrome-stable",
                "/snap/bin/chromium",
            ]
            ms_playwright_dir = Path("/ms-playwright")
            if ms_playwright_dir.exists():
                for chrome_bin in sorted(ms_playwright_dir.glob("chromium-*/chrome-linux/chrome"), reverse=True):
                    if chrome_bin.exists():
                        linux_paths.append(str(chrome_bin))

            for path in linux_paths:
                if os.path.exists(path):
                    logger.info(f"[⚙️ SYSTEM] Linux browser ditemukan: {path}")
                    return path
            return None

        # Windows: Brave Browser
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

        # Windows: Google Chrome fallback
        chrome_paths = [
            r"C:\Program Files\Google\Chrome\Application\chrome.exe",
            r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
            os.path.expandvars(
                r"%LOCALAPPDATA%\Google\Chrome\Application\chrome.exe"
            ),
        ]
        for path in chrome_paths:
            if os.path.exists(path):
                return path

        return None

    @staticmethod
    def get_browser_launch_kwargs(proxy: Optional[str] = None) -> dict:
        """
        Bangun kwarg launch browser Playwright terpusat dengan flag Docker/Linux wajib.
        """
        brave_path = BaseScraper.get_brave_path()
        launch_kwargs: dict = {
            "args": list(DOCKER_CHROMIUM_FLAGS),
        }
        if brave_path:
            launch_kwargs["executable_path"] = brave_path
        elif _platform_module.system() == "Linux":
            ms_playwright_cache = Path.home() / ".cache" / "ms-playwright"
            has_playwright_chromium = any(
                p.name == "chrome" and p.exists()
                for p in ms_playwright_cache.glob("chromium-*/chrome-linux/chrome")
            ) if ms_playwright_cache.exists() else False

            if not has_playwright_chromium:
                logger.error(
                    "[❌ ERROR  ] Tidak ada browser yang ditemukan di Linux! "
                    "Jalankan: `scrapling install` atau set env BROWSER_EXECUTABLE_PATH."
                )
            else:
                logger.debug("[⚙️ SYSTEM] Menggunakan Playwright bundled chromium via channel='chromium'.")
            launch_kwargs["channel"] = "chromium"

        if proxy:
            launch_kwargs["proxy"] = {"server": proxy}

        return launch_kwargs

    # ──────────────────────────────────────────────
    # Scrapling Fetcher Helpers
    # ──────────────────────────────────────────────

    @staticmethod
    async def fetch_stealth_page(
        url: str,
        cookies: Optional[Union[list[dict], dict]] = None,
        page_action: Optional[Callable] = None,
        timeout: int = 30000,
        wait_selector: Optional[str] = None,
        headless: bool = True,
        proxy: Optional[str] = None,
        network_idle: bool = False,
    ) -> Any:
        """
        Ambil halaman menggunakan Scrapling StealthyFetcher (anti-bot fingerprinting).
        """
        if not HAS_SCRAPLING or StealthyFetcher is None:
            raise RuntimeError("Scrapling library tidak terpasang. Jalankan `pip install scrapling[fetchers]`")

        fetcher_kwargs: dict[str, Any] = {
            "headless": headless,
            "timeout": timeout,
            "additional_args": list(DOCKER_CHROMIUM_FLAGS),
        }
        if cookies:
            fetcher_kwargs["cookies"] = cookies
        if page_action:
            fetcher_kwargs["page_action"] = page_action
        if wait_selector:
            fetcher_kwargs["wait_selector"] = wait_selector
        if proxy:
            fetcher_kwargs["proxy"] = proxy
        if network_idle:
            fetcher_kwargs["network_idle"] = network_idle

        brave_path = BaseScraper.get_brave_path()
        if brave_path and os.path.exists(brave_path):
            fetcher_kwargs["executable_path"] = brave_path

        return await StealthyFetcher.async_fetch(url, **fetcher_kwargs)

    @staticmethod
    async def fetch_dynamic_page(
        url: str,
        cookies: Optional[Union[list[dict], dict]] = None,
        page_action: Optional[Callable] = None,
        timeout: int = 30000,
        wait_selector: Optional[str] = None,
        headless: bool = True,
        proxy: Optional[str] = None,
        network_idle: bool = False,
    ) -> Any:
        """
        Ambil halaman menggunakan Scrapling DynamicFetcher untuk JS-heavy pages.
        """
        if not HAS_SCRAPLING or DynamicFetcher is None:
            raise RuntimeError("Scrapling library tidak terpasang. Jalankan `pip install scrapling[fetchers]`")

        fetcher_kwargs: dict[str, Any] = {
            "headless": headless,
            "timeout": timeout,
            "additional_args": list(DOCKER_CHROMIUM_FLAGS),
        }
        if cookies:
            fetcher_kwargs["cookies"] = cookies
        if page_action:
            fetcher_kwargs["page_action"] = page_action
        if wait_selector:
            fetcher_kwargs["wait_selector"] = wait_selector
        if proxy:
            fetcher_kwargs["proxy"] = proxy
        if network_idle:
            fetcher_kwargs["network_idle"] = network_idle

        brave_path = BaseScraper.get_brave_path()
        if brave_path and os.path.exists(brave_path):
            fetcher_kwargs["executable_path"] = brave_path

        return await DynamicFetcher.async_fetch(url, **fetcher_kwargs)

    @staticmethod
    async def fetch_http_page(
        url: str,
        cookies: Optional[dict] = None,
        headers: Optional[dict] = None,
        impersonate: str = "chrome124",
        timeout: int = 20,
        proxy: Optional[str] = None,
    ) -> Any:
        """
        Ambil halaman HTTP cepat menggunakan Scrapling AsyncFetcher (curl_cffi TLS impersonation).
        """
        if not HAS_SCRAPLING or AsyncFetcher is None:
            raise RuntimeError("Scrapling library tidak terpasang. Jalankan `pip install scrapling`")

        req_headers = {"User-Agent": USER_AGENT}
        if headers:
            req_headers.update(headers)

        fetch_kwargs: dict[str, Any] = {
            "headers": req_headers,
            "timeout": timeout,
            "impersonate": impersonate,
        }
        if cookies:
            fetch_kwargs["cookies"] = cookies
        if proxy:
            fetch_kwargs["proxy"] = proxy

        return await AsyncFetcher.get(url, **fetch_kwargs)

    # ──────────────────────────────────────────────
    # Cookie Management
    # ──────────────────────────────────────────────

    async def load_cookies_as_list(self, platform: str) -> list[dict]:
        """
        Muat cookie dari file JSON, Netscape, atau .env dalam format list of dicts:
        [{"name": "...", "value": "...", "domain": "...", "path": "/", "secure": True, ...}]
        """
        cookies: list[dict] = []

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
                                exp = c.get("expirationDate") or c.get("expires")
                                if exp:
                                    try:
                                        norm_c["expires"] = int(float(exp))
                                    except (ValueError, TypeError):
                                        pass
                                cookies.append(norm_c)
                else:
                    for line in content.splitlines():
                        line = line.strip()
                        if not line or line.startswith("#"):
                            continue
                        parts = line.split("\t")
                        if len(parts) >= 7:
                            domain, _, path, secure, expires, name, value = parts[:7]
                            norm_c = {
                                "name": name.strip(),
                                "value": value.strip(),
                                "domain": domain.strip(),
                                "path": path.strip() or "/",
                                "secure": secure.upper() == "TRUE",
                                "httpOnly": True,
                            }
                            if expires and expires.isdigit() and int(expires) > 0:
                                norm_c["expires"] = int(expires)
                            cookies.append(norm_c)

                if cookies:
                    logger.info(f"[⚙️ SYSTEM] Cookie {platform} berhasil dimuat dari {cookie_file.name} ({len(cookies)} cookies).")
                    return cookies
            except Exception as e:
                logger.warning(f"Gagal membaca file cookie {platform} ({cookie_file.name}): {e}")

        # Fallback ke .env
        token_specs = self.ENV_TOKEN_MAP.get(platform, [])
        if token_specs:
            env_cookies = []
            all_present = True
            for spec in token_specs:
                token_val = os.getenv(spec["env_key"])
                if token_val:
                    env_cookies.append({
                        "name": spec["name"],
                        "value": token_val.strip(),
                        "domain": spec["domain"],
                        "path": "/",
                        "secure": True,
                        "httpOnly": True,
                    })
                else:
                    all_present = False
            if all_present and env_cookies:
                logger.info(f"[⚙️ SYSTEM] Cookie {platform} dibangun dari .env token ({len(env_cookies)} cookies).")
                return env_cookies

        return cookies

    async def load_cookies_as_dict(self, platform: str) -> dict[str, str]:
        """
        Muat cookie dari file atau .env dalam format dictionary {name: value}.
        """
        cookie_list = await self.load_cookies_as_list(platform)
        return {c["name"]: c["value"] for c in cookie_list if "name" in c and "value" in c}

    async def load_and_inject_cookies(self, context, platform: str) -> Optional[Path]:
        """
        Injeksi cookie ke browser context + export file Netscape untuk yt-dlp.
        """
        cookies_to_inject = await self.load_cookies_as_list(platform)

        # Injeksi ke browser context jika diberikan
        if context is not None and cookies_to_inject:
            try:
                await context.add_cookies(cookies_to_inject)
                logger.info(f"[⚙️ SYSTEM] {len(cookies_to_inject)} cookie berhasil diinjeksi ke {platform}.")
            except Exception as e:
                logger.error(f"[❌ ERROR  ] Gagal menginjeksi cookie {platform}: {e}", exc_info=True)

        # Export ke format Netscape untuk yt-dlp
        if cookies_to_inject:
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
                    expires = int(expires) if expires and int(expires) > 0 else 2147483647
                    name = cookie.get("name", "")
                    value = cookie.get("value", "")
                    lines.append(
                        f"{domain}\t{include_subdomains}\t{path}\t"
                        f"{secure}\t{expires}\t{name}\t{value}\n"
                    )

                async with aiofiles.open(netscape_path, "w", encoding="utf-8") as f:
                    await f.writelines(lines)

                logger.info(f"[⚙️ SYSTEM] Netscape cookies {platform} diekspor ke: {netscape_path}")
                return netscape_path
            except Exception as e:
                logger.error(f"[❌ ERROR  ] Gagal menulis Netscape cookie {platform}: {e}", exc_info=True)

        return None

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

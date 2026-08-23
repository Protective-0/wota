"""
bot.py — Discord Media Scraper Bot
====================================
Bot Discord yang secara otomatis mendeteksi link profil TikTok, Instagram,
dan Twitter/X dari pesan masuk, mengunduh seluruh media dari profil tersebut,
lalu mengirimkannya ke channel Discord yang ditentukan.

Arsitektur:
- discord.py Client dengan on_message event handler
- Concurrent download+upload via asyncio.Semaphore(N) worker pool
- Anti-duplikasi dan resume via SQLite (aiosqlite)
- Kompresi video ffmpeg (target di bawah Discord upload limit)
- Carousel via multi-file discord.File attachment

Cara Menjalankan:
    1. Salin .env.example ke .env dan isi nilai konfigurasi
    2. Install dependencies: pip install -r requirements.txt
    3. Install Playwright browsers: playwright install chromium
    4. Pastikan ffmpeg sudah terinstall dan ada di PATH
    5. Jalankan: python bot.py
"""

import asyncio
import random
import logging
import signal
from typing import Optional, Any, cast
import os
import re
import shutil
import uuid
from pathlib import Path

import discord
from discord import app_commands
from discord.ext import commands, tasks
from dotenv import load_dotenv

from core import (
    DatabaseManager,
    MediaDownloader,
    MediaSender,
    QueueManager,
    detect_platform,
    extract_username_and_platform,
    setup_logging,
    fmt_size,
    fmt_duration,
)
from core.utils import (
    TAG_PATROL, TAG_QUEUE, TAG_CRAWL, TAG_DOWN,
    TAG_COMPR, TAG_DISCORD, TAG_SYSTEM, TAG_SUCCESS,
    TAG_WARN, TAG_ERROR,
)
from scrapers import InstagramScraper, TikTokScraper, TwitterScraper
from scrapers.base import BaseScraper, PostMedia

# ──────────────────────────────────────────────
# Setup Logging
# ──────────────────────────────────────────────
setup_logging(logging.INFO)

logger = logging.getLogger(__name__)


def clear_directory_contents(dir_path: str | Path) -> None:
    """
    Menghapus seluruh file dan subfolder di dalam direktori tanpa mencoba
    menghapus folder induknya sendiri (aman untuk Docker volume mounts).
    """
    target = Path(dir_path)
    if not target.exists():
        target.mkdir(parents=True, exist_ok=True)
        return
    for item in target.iterdir():
        try:
            if item.is_dir():
                shutil.rmtree(item, ignore_errors=True)
            else:
                item.unlink(missing_ok=True)
        except Exception as e:
            logger.warning(f"Gagal menghapus {item}: {e}")


async def safe_defer(interaction: discord.Interaction, ephemeral: bool = False) -> bool:
    """Defer interaksi dengan aman agar tidak crash jika token Discord 3s timeout."""
    try:
        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=ephemeral)
            return True
    except Exception as e:
        logger.warning(f"Interaction defer timeout/error: {e}")
    return False


async def safe_reply(
    interaction: discord.Interaction,
    text: str,
    fallback_channel: Optional[Any] = None,
    ephemeral: bool = False,
) -> None:
    """Kirim respon interaksi dengan fallback pengiriman langsung ke channel jika interaksi expired."""
    try:
        if interaction.response.is_done():
            await interaction.followup.send(text, ephemeral=ephemeral)
        else:
            await interaction.response.send_message(text, ephemeral=ephemeral)
    except Exception as e:
        logger.warning(f"Interaction response error: {e}")
        if fallback_channel:
            try:
                await fallback_channel.send(text)
            except Exception as ch_err:
                logger.warning(f"Fallback channel send error: {ch_err}")


# ──────────────────────────────────────────────
# Load Konfigurasi dari .env
# ──────────────────────────────────────────────
load_dotenv()

# Discord bot token dari Developer Portal
DISCORD_BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN", "")

# Channel ID tujuan pengiriman media (right-click channel → Copy Channel ID)
try:
    DISCORD_CHANNEL_ID = int(os.getenv("DISCORD_CHANNEL_ID", "0"))
except ValueError:
    DISCORD_CHANNEL_ID = 0

if not DISCORD_CHANNEL_ID:
    raise ValueError("DISCORD_CHANNEL_ID is not set in environment or invalid.")

# User ID Discord yang diizinkan mengirim perintah (right-click user → Copy User ID)
try:
    ALLOWED_USER_ID = int(os.getenv("DISCORD_ALLOWED_USER_ID", "0"))
except ValueError:
    ALLOWED_USER_ID = 0

TEMP_DIR = os.getenv("TEMP_DIR", "./temp_media")
SESSION_DIR = os.getenv("SESSION_DIR", "./sessions")
DB_PATH = os.getenv("DB_PATH", "./data/bot_data.db")
MAX_FILE_SIZE_MB = int(os.getenv("MAX_FILE_SIZE_MB", "10"))
SEND_DELAY = float(os.getenv("SEND_DELAY_SECONDS", "3"))
BROWSER_HEADED = os.getenv("BROWSER_HEADED", "true").lower() == "true"
DOWNLOAD_DELAY_MIN = float(os.getenv("DOWNLOAD_DELAY_MIN", "2"))
DOWNLOAD_DELAY_MAX = float(os.getenv("DOWNLOAD_DELAY_MAX", "5"))

# Jumlah maksimal post yang diproses secara concurrent.
# Dynamic clamping between 1 and 10 to prevent API rate limit bans.
raw_concurrent = int(os.getenv("CONCURRENT_DOWNLOADS", "3"))
CONCURRENT_DOWNLOADS = max(1, min(10, raw_concurrent))

# ──────────────────────────────────────────────




# ──────────────────────────────────────────────
# Discord Bot Client
# ──────────────────────────────────────────────


class MediaScraperBot(commands.Bot):
    """
    Discord Bot yang mendeteksi URL profil dari pesan masuk,
    lalu menjalankan pipeline scraping concurrent dengan routing custom.

    Arsitektur hybrid:
        - Patrol loop (10 menit) untuk akun yang sudah selesai historical dump
        - /force untuk scan manual semua akun
        - URL detection untuk single-profile pipeline

    Lifecycle:
        on_ready()   → init DB, sync tree, start patrol_loop, log status
        on_message() → command parser (/add, !reset), URL pipeline spawn
    """

    def __init__(self):
        # Intents: perlu message_content untuk baca isi pesan
        intents = discord.Intents.default()
        intents.message_content = True
        super().__init__(command_prefix="!", intents=intents)

        self.db = DatabaseManager(DB_PATH)
        self.queue = QueueManager()
        self.downloader = MediaDownloader(
            temp_dir=TEMP_DIR,
            delay_min=DOWNLOAD_DELAY_MIN,
            delay_max=DOWNLOAD_DELAY_MAX,
        )
        # Strong reference set: mencegah background task di-GC sebelum selesai
        self._active_tasks: set = set()
        # Single sequential task queue untuk mencegah task scraping tumpang tindih
        self._job_queue: asyncio.Queue = asyncio.Queue()
        self._worker_task: Optional[asyncio.Task] = None
        # Channel object di-resolve saat on_ready
        self._target_channel: Any = None

        # Flag untuk scan race-condition guard
        self.is_scanning = False
        self.scan_lock = asyncio.Lock()
        # Flag untuk pause/resume patrol loop
        self.patrol_paused = False

        # Registrasi Slash Command /add
        @self.tree.command(
            name="add", description="Maps a username to a specific target channel"
        )
        @app_commands.describe(
            username="Username target (e.g. jkt48.zee atau link profil)",
            channel="Channel target untuk media",
            platform="Platform target jika username berupa teks biasa (e.g. instagram)",
        )
        @app_commands.choices(
            platform=[
                app_commands.Choice(name="Instagram", value="instagram"),
                app_commands.Choice(name="TikTok", value="tiktok"),
                app_commands.Choice(name="Twitter/X", value="twitter"),
            ]
        )
        async def add(
            interaction: discord.Interaction,
            username: str,
            channel: discord.TextChannel,
            platform: Optional[str] = None,
        ):
            if interaction.user.id != ALLOWED_USER_ID:
                await safe_reply(
                    interaction, "⛔ Akses Ditolak: Hanya untuk Administrator.", ephemeral=True
                )
                return

            await safe_defer(interaction)

            try:
                clean_username, detected_platform = extract_username_and_platform(
                    username
                )
                if platform:
                    detected_platform = platform

                if detected_platform == "tiktok":
                    profile_url = f"https://www.tiktok.com/@{clean_username}"
                elif detected_platform == "instagram":
                    profile_url = f"https://www.instagram.com/{clean_username}/"
                elif detected_platform == "twitter":
                    profile_url = f"https://x.com/{clean_username}"
                else:
                    await safe_reply(interaction, "❌ Platform tidak dikenali.", fallback_channel=channel, ephemeral=True)
                    return

                await self.db.add_monitored_account(
                    clean_username, detected_platform, channel.id, last_scraped_id=""
                )

                q_size = self._job_queue.qsize() + 1
                status_text = (
                    "Langsung diproses sekarang."
                    if q_size == 1 and not self.queue.is_busy
                    else f"Masuk antrean urutan ke-#{q_size} (diproses berurutan, tidak numpuk)."
                )

                await safe_reply(
                    interaction,
                    f"✅ Berhasil mendaftarkan **@{clean_username}** ({detected_platform}) ke channel {channel.mention}.\n"
                    f"⏳ {status_text}",
                    fallback_channel=channel
                )

                await self._job_queue.put(
                    (clean_username, detected_platform, profile_url, channel.id)
                )
            except Exception as e:
                logger.error(
                    f"[❌ ERROR  ] Gagal memproses registrasi /add: {e}", exc_info=True
                )
                await safe_reply(interaction, f"❌ Gagal memproses registrasi: {e}", fallback_channel=channel)

        # Registrasi Slash Command /insta
        @self.tree.command(
            name="insta",
            description="Maps an Instagram username to a specific target channel",
        )
        @app_commands.describe(
            username="Instagram username atau URL profil",
            channel="Channel target untuk media",
        )
        async def insta(
            interaction: discord.Interaction,
            username: str,
            channel: discord.TextChannel,
        ):
            if interaction.user.id != ALLOWED_USER_ID:
                await safe_reply(
                    interaction, "⛔ Akses Ditolak: Hanya untuk Administrator.", ephemeral=True
                )
                return

            await safe_defer(interaction)

            try:
                clean_username, _ = extract_username_and_platform(username)
                profile_url = f"https://www.instagram.com/{clean_username}/"

                await self.db.add_monitored_account(
                    clean_username, "instagram", channel.id, last_scraped_id=""
                )

                q_size = self._job_queue.qsize() + 1
                status_text = (
                    "Langsung diproses sekarang."
                    if q_size == 1 and not self.queue.is_busy
                    else f"Masuk antrean urutan ke-#{q_size} (diproses berurutan, tidak numpuk)."
                )

                await safe_reply(
                    interaction,
                    f"✅ Berhasil mendaftarkan Instagram **@{clean_username}** ke channel {channel.mention}.\n"
                    f"⏳ {status_text}",
                    fallback_channel=channel
                )

                await self._job_queue.put(
                    (clean_username, "instagram", profile_url, channel.id)
                )
            except Exception as e:
                logger.error(
                    f"[❌ ERROR  ] Gagal memproses registrasi /insta: {e}",
                    exc_info=True,
                )
                await safe_reply(interaction, f"❌ Gagal memproses registrasi: {e}", fallback_channel=channel)

        # Registrasi Slash Command /tiktok
        @self.tree.command(
            name="tiktok",
            description="Maps a TikTok username to a specific target channel",
        )
        @app_commands.describe(
            username="TikTok username atau URL profil",
            channel="Channel target untuk media",
        )
        async def tiktok(
            interaction: discord.Interaction,
            username: str,
            channel: discord.TextChannel,
        ):
            if interaction.user.id != ALLOWED_USER_ID:
                await safe_reply(
                    interaction, "⛔ Akses Ditolak: Hanya untuk Administrator.", ephemeral=True
                )
                return

            await safe_defer(interaction)

            try:
                clean_username, _ = extract_username_and_platform(username)
                profile_url = f"https://www.tiktok.com/@{clean_username}"

                await self.db.add_monitored_account(
                    clean_username, "tiktok", channel.id, last_scraped_id=""
                )

                q_size = self._job_queue.qsize() + 1
                status_text = (
                    "Langsung diproses sekarang."
                    if q_size == 1 and not self.queue.is_busy
                    else f"Masuk antrean urutan ke-#{q_size} (diproses berurutan, tidak numpuk)."
                )

                await safe_reply(
                    interaction,
                    f"✅ Berhasil mendaftarkan TikTok **@{clean_username}** ke channel {channel.mention}.\n"
                    f"⏳ {status_text}",
                    fallback_channel=channel
                )

                await self._job_queue.put(
                    (clean_username, "tiktok", profile_url, channel.id)
                )
            except Exception as e:
                logger.error(
                    f"[❌ ERROR  ] Gagal memproses registrasi /tiktok: {e}",
                    exc_info=True,
                )
                await safe_reply(interaction, f"❌ Gagal memproses registrasi: {e}", fallback_channel=channel)

        # Registrasi Slash Command /x
        @self.tree.command(
            name="x",
            description="Maps a Twitter/X username to a specific target channel",
        )
        @app_commands.describe(
            username="Twitter/X username atau URL profil",
            channel="Channel target untuk media",
        )
        async def x_cmd(
            interaction: discord.Interaction,
            username: str,
            channel: discord.TextChannel,
        ):
            if interaction.user.id != ALLOWED_USER_ID:
                await safe_reply(
                    interaction, "⛔ Akses Ditolak: Hanya untuk Administrator.", ephemeral=True
                )
                return

            await safe_defer(interaction)

            try:
                clean_username, _ = extract_username_and_platform(username)
                profile_url = f"https://x.com/{clean_username}"

                await self.db.add_monitored_account(
                    clean_username, "twitter", channel.id, last_scraped_id=""
                )

                q_size = self._job_queue.qsize() + 1
                status_text = (
                    "Langsung diproses sekarang."
                    if q_size == 1 and not self.queue.is_busy
                    else f"Masuk antrean urutan ke-#{q_size} (diproses berurutan, tidak numpuk)."
                )

                await safe_reply(
                    interaction,
                    f"✅ Berhasil mendaftarkan Twitter/X **@{clean_username}** ke channel {channel.mention}.\n"
                    f"⏳ {status_text}",
                    fallback_channel=channel
                )

                await self._job_queue.put(
                    (clean_username, "twitter", profile_url, channel.id)
                )
            except Exception as e:
                logger.error(
                    f"[❌ ERROR  ] Gagal memproses registrasi /x: {e}", exc_info=True
                )
                await safe_reply(interaction, f"❌ Gagal memproses registrasi: {e}", fallback_channel=channel)

        # Registrasi Slash Command /delete
        @self.tree.command(
            name="delete",
            description="Permanently remove a profile record from monitored list",
        )
        @app_commands.describe(username="Username atau URL profil yang ingin dihapus")
        async def delete_cmd(interaction: discord.Interaction, username: str):
            if interaction.user.id != ALLOWED_USER_ID:
                await safe_reply(
                    interaction, "⛔ Akses Ditolak.", ephemeral=True
                )
                return
            try:
                clean_username, platform = extract_username_and_platform(username)
                await self.db.delete_monitored_account(clean_username, platform)
                await safe_reply(
                    interaction, f"🗑️ Akun **@{clean_username}** telah dihapus dari daftar monitoring."
                )
            except Exception as e:
                await safe_reply(interaction, f"❌ Gagal menghapus akun: {e}")

        # Registrasi Slash Command /reset_account
        @self.tree.command(
            name="reset_account",
            description="Clear last_scraped_id for a monitored account to force full re-scrape",
        )
        @app_commands.describe(username="Username atau URL profil yang ingin di-reset")
        async def reset_account(interaction: discord.Interaction, username: str):
            if interaction.user.id != ALLOWED_USER_ID:
                await safe_reply(
                    interaction, "⛔ Akses Ditolak.", ephemeral=True
                )
                return
            try:
                clean_username, platform = extract_username_and_platform(username)
                await self.db.reset_account_history(clean_username, platform)
                await safe_reply(
                    interaction,
                    f"🔄 Riwayat scrape untuk **@{clean_username}** telah di-reset. Scrape berikutnya akan mengunduh ulang."
                )
            except Exception as e:
                await safe_reply(
                    interaction, f"❌ Gagal mereset riwayat: {e}"
                )

        # Registrasi Slash Command /reset_bot
        @self.tree.command(
            name="reset_bot",
            description="Completely wipe database records, sessions/ and temp_media/ folders",
        )
        async def reset_bot(interaction: discord.Interaction):
            if interaction.user.id != ALLOWED_USER_ID:
                await safe_reply(
                    interaction, "⛔ Akses Ditolak.", ephemeral=True
                )
                return

            await safe_defer(interaction)
            await safe_reply(
                interaction, "⏳ Memulai pembersihan sistem secara menyeluruh..."
            )

            try:
                # 1. Clear database records
                await self.db.clear_all_data()

                # 2. Wipes directories safely (Docker volume safe)
                await asyncio.to_thread(clear_directory_contents, TEMP_DIR)
                await asyncio.to_thread(clear_directory_contents, SESSION_DIR)

                # Kosongkan antrean job yang tertunda
                # FIX: task_done() removed — tidak ada Queue.join() di codebase ini,
                # task_done() hanya relevan jika join() dipakai. get_nowait() sudah cukup.
                while not self._job_queue.empty():
                    try:
                        self._job_queue.get_nowait()
                    except (asyncio.QueueEmpty, ValueError):
                        break

                self.queue.release()

                await safe_reply(
                    interaction, "✅ **Reset Bot Sukses Total!** Seluruh database, sesi cookies, dan file temporer telah dibersihkan secara bersih."
                )
            except Exception as e:
                logger.error(f"[❌ ERROR  ] Reset bot error: {e}", exc_info=True)
                await safe_reply(interaction, f"❌ Reset gagal: `{str(e)[:200]}`")

        # Registrasi Slash Command /force — master switch untuk scan manual
        @self.tree.command(
            name="force",
            description="Memaksa bot melakukan scan menyeluruh ke semua akun saat ini juga",
        )
        async def force_scan(interaction: discord.Interaction):
            if interaction.user.id != ALLOWED_USER_ID:
                await safe_reply(
                    interaction, "❌ Anda tidak memiliki izin.", ephemeral=True
                )
                return

            await safe_defer(interaction)

            if self.is_scanning:
                await safe_reply(
                    interaction, "⚠️ Scan masih berjalan. Harap tunggu."
                )
                return

            await safe_reply(
                interaction, "🚀 Memulai proses *Force Scan* ke semua akun..."
            )

            # FIX: is_scanning check moved INSIDE scan_lock to prevent TOCTOU race
            # where two /force calls both pass the check before either sets is_scanning=True
            async with self.scan_lock:
                if self.is_scanning:
                    await safe_reply(
                        interaction, "⚠️ Scan masih berjalan. Harap tunggu."
                    )
                    return

                self.is_scanning = True
                try:
                    accounts = await self.db.get_all_monitored_accounts()
                    if not accounts:
                        await safe_reply(
                            interaction, "❌ Database kosong. Tambahkan akun terlebih dahulu."
                        )
                        return

                    logger.info(
                        f"[⚡ FORCED] 🚀 Memulai scan paksa untuk total {len(accounts)} akun..."
                    )
                    success, failed = await self._run_batch_scan(accounts, forced=True)

                    summary = f"**✅ Force Scan Selesai!**\n• Berhasil: {success} akun\n• Gagal: {failed} akun\n• Total: {len(accounts)} akun"
                    await interaction.edit_original_response(content=summary)
                except Exception as e:
                    logger.error(f"[❌ ERROR  ] Kegagalan fatal saat Force Scan: {e}")
                    await interaction.edit_original_response(
                        content=f"❌ Terjadi kesalahan saat scan: {e}"
                    )
                finally:
                    self.is_scanning = False

        # Registrasi Slash Command /list — lihat semua akun yang dimonitor
        @self.tree.command(
            name="list", description="Tampilkan semua akun yang sedang dimonitor"
        )
        async def list_accounts(interaction: discord.Interaction):
            if interaction.user.id != ALLOWED_USER_ID:
                await interaction.response.send_message(
                    "⛔ Akses Ditolak.", ephemeral=True
                )
                return

            accounts = await self.db.get_all_monitored_accounts()
            if not accounts:
                await interaction.response.send_message(
                    "📋 Belum ada akun yang dimonitor."
                )
                return

            # Bangun tabel laporan
            lines = ["**📋 Daftar Akun Monitoring:**\n"]
            for i, acc in enumerate(accounts, 1):
                username = acc["username"]
                platform = acc["platform"]
                channel_id = acc["channel_id"]
                last_id = acc["last_scraped_id"]
                # Status: sudah selesai historical dump atau belum
                status = "✅ Ready" if last_id else "⏳ Pending"
                lines.append(
                    f"`{i}.` **@{username}** ({platform}) → <#{channel_id}> [{status}]"
                )

            report = "\n".join(lines)
            chunks = [report[i : i + 1900] for i in range(0, len(report), 1900)]
            await interaction.response.send_message(chunks[0])
            for chunk in chunks[1:]:
                await interaction.followup.send(chunk)

        # Registrasi Slash Command /sync — force sync command tree
        @self.tree.command(
            name="sync", description="Force sync slash commands ke Discord server"
        )
        async def sync_commands(interaction: discord.Interaction):
            if interaction.user.id != ALLOWED_USER_ID:
                await interaction.response.send_message(
                    "⛔ Akses Ditolak.", ephemeral=True
                )
                return

            # Tembak langsung respon awal instan
            await interaction.response.send_message(
                "⏳ Memulai sinkronisasi slash commands secara global..."
            )
            try:
                synced = await self.tree.sync()
                await interaction.followup.send(
                    f"✅ Berhasil sync **{len(synced)}** slash commands ke server."
                )
                logger.info(f"[⚙️ SYSTEM] Manual sync: {len(synced)} commands synced.")
            except Exception as e:
                logger.error(f"[❌ ERROR  ] Manual sync gagal: {e}")
                await interaction.followup.send(f"❌ Sync gagal: `{str(e)[:200]}`")

        # Registrasi Slash Command /pause — pause patrol loop
        @self.tree.command(name="pause", description="Pause background patrol loop")
        async def pause(interaction: discord.Interaction):
            if interaction.user.id != ALLOWED_USER_ID:
                await interaction.response.send_message(
                    "⛔ Akses Ditolak.", ephemeral=True
                )
                return
            self.patrol_paused = True
            await interaction.response.send_message("⏸️ Patrol loop di-pause.")
            logger.info("[🤖 SATPAM] Patrol loop di-pause oleh admin.")

        # Registrasi Slash Command /resume — resume patrol loop
        @self.tree.command(name="resume", description="Resume background patrol loop")
        async def resume(interaction: discord.Interaction):
            if interaction.user.id != ALLOWED_USER_ID:
                await interaction.response.send_message(
                    "⛔ Akses Ditolak.", ephemeral=True
                )
                return
            self.patrol_paused = False
            await interaction.response.send_message("▶️ Patrol loop di-resume.")
            logger.info("[🤖 SATPAM] Patrol loop di-resume oleh admin.")

    async def _job_worker(self) -> None:
        """Worker background tunggal untuk memproses antrean scraping 1 per 1 secara berurutan."""
        logger.info(f"{TAG_SYSTEM} Background Job Worker aktif (Mode Serial FIFO: 1 task pada satu waktu).")
        while True:
            try:
                job = await self._job_queue.get()
                username, platform, profile_url, channel_id = job
                logger.info(
                    f"{TAG_QUEUE} Mengambil task: @{username} ({platform}) — "
                    f"Sisa antrean: {self._job_queue.qsize()}"
                )
                await self._run_initial_historical_scrape(
                    username, platform, profile_url, channel_id
                )
                self._job_queue.task_done()
                logger.info(f"{TAG_QUEUE} Selesai: @{username} ({platform}).")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"{TAG_ERROR} Kesalahan fatal di Job Worker: {e}", exc_info=True)

    async def on_ready(self) -> None:
        """Dipanggil saat bot berhasil connect ke Discord Gateway."""
        await self.db.initialize()
        Path(TEMP_DIR).mkdir(parents=True, exist_ok=True)
        Path(SESSION_DIR).mkdir(parents=True, exist_ok=True)

        # Resolve channel ID ke object channel
        self._target_channel = self.get_channel(DISCORD_CHANNEL_ID)
        if not self._target_channel:
            logger.error(
                f"[❌ ERROR  ] Channel ID {DISCORD_CHANNEL_ID} tidak ditemukan! "
                f"Pastikan bot sudah di-invite ke server dan ID benar."
            )

        # Sinkronisasi slash commands otomatis ke Discord API saat startup (Global)
        try:
            synced = await self.tree.sync()
            logger.info(f"[⚙️ SYSTEM] Berhasil sync {len(synced)} slash commands secara global ke Discord.")
        except Exception as e:
            logger.error(f"[❌ ERROR  ] Gagal sync slash commands di on_ready: {e}")

        # Start single sequential background worker
        if not self._worker_task or self._worker_task.done():
            self._worker_task = asyncio.create_task(self._job_worker())

        # Start patrol background loop
        if not self.patrol_loop.is_running():
            self.patrol_loop.start()
            logger.info("[⚙️ SYSTEM] Patrol loop started (interval: 10 menit).")

        logger.info(
            "[⚙️ SYSTEM] ============================================================"
        )
        logger.info("[⚙️ SYSTEM]   Discord Media Scraper Bot")
        logger.info(f"[⚙️ SYSTEM]   Logged in as: {self.user}")
        logger.info(f"[⚙️ SYSTEM]   Target channel: {self._target_channel}")
        logger.info(f"[⚙️ SYSTEM]   Allowed user ID: {ALLOWED_USER_ID}")
        logger.info(f"[⚙️ SYSTEM]   Concurrent downloads: {CONCURRENT_DOWNLOADS}")
        logger.info(
            f"[⚙️ SYSTEM]   Browser mode: {'Headed' if BROWSER_HEADED else 'Headless'}"
        )
        logger.info(
            "[⚙️ SYSTEM] ============================================================"
        )

    async def on_message(self, message: discord.Message) -> None:
        """
        Handler utama: filter user, command parser (/add, !reset), URL pipeline spawn.
        """
        # Abaikan pesan dari bot sendiri
        if message.author == self.user:
            return

        # Security: hanya izinkan user yang terdaftar di .env
        if message.author.id != ALLOWED_USER_ID:
            return

        text = message.content.strip()

        # Trik darurat sinkronisasi murni via pesan teks biasa (Bukan Slash Command)
        if text.lower() == "!sync":
            try:
                # Paksa sinkronisasi satu jalur secara bersih secara global (matching on_ready pattern)
                synced = await self.tree.sync()
                await message.reply(
                    f"✅ **Hard Sync Sukses!** {len(synced)} perintah bersih terdaftar secara global."
                )
            except Exception as e:
                await message.reply(f"❌ Gagal Sync: {e}")
            return

        # 1. Command Parser: Slash-style text commands (/add)
        if text.lower().startswith("/add "):
            parts = text.split()
            if len(parts) < 3:
                await message.reply(
                    "⚠️ Gunakan format: `/add <username/URL> <#channel> [platform]`"
                )
                return

            username_input = parts[1]
            channel_mention = parts[2]
            platform_input = parts[3].lower() if len(parts) > 3 else None

            # Extract channel ID dari mention <#CHANNEL_ID>
            channel_match = re.search(r"<#(\d+)>", channel_mention)
            if not channel_match:
                await message.reply(
                    "⚠️ Parameter kedua harus berupa mention channel, e.g. <#channel>"
                )
                return
            target_channel_id = int(channel_match.group(1))

            # Extract username & platform dari input
            username, detected_platform = extract_username_and_platform(username_input)
            if platform_input:
                detected_platform = platform_input

            if detected_platform not in {"instagram", "tiktok", "twitter"}:
                await message.reply(
                    "⚠️ Platform tidak dikenali. Pilih antara: `instagram`, `tiktok`, atau `twitter`."
                )
                return

            # Save ke DB
            await self.db.add_monitored_account(
                username, detected_platform, target_channel_id
            )
            await message.reply(
                f"✅ Berhasil memetakan **@{username}** ({detected_platform}) ke channel <#{target_channel_id}>."
            )
            return

        # Command: !reset — hapus semua data dan sesi
        if text.lower() == "!reset":
            await self._handle_reset(message)
            return

        # 2. Deteksi URL Profil (Manual Trigger)
        platform, profile_url = detect_platform(text)

        if not platform:
            return  # Bukan URL profil — abaikan

        # Pastikan target channel sudah di-resolve
        channel = self._target_channel or message.channel

        # Single-user lock: tolak jika sedang ada proses berjalan
        if self.queue.is_busy:
            busy_msg = self.queue.get_busy_message()
            await message.reply(busy_msg)
            return

        # Konfirmasi ke user bahwa perintah diterima
        await message.add_reaction("🚀")

        # Spawn pipeline di background task — tidak blokir on_message
        task = asyncio.create_task(
            self._run_scraping_pipeline(
                channel=channel,
                platform=platform,
                profile_url=profile_url,
            )
        )
        self._active_tasks.add(task)
        task.add_done_callback(self._on_task_done)

    # ──────────────────────────────────────────────
    # Command: !reset
    # ──────────────────────────────────────────────

    async def _handle_reset(self, message: discord.Message) -> None:
        """Handler reset: hapus seluruh data dan sesi bot (Admin Only)."""
        if message.author.id != ALLOWED_USER_ID:
            await message.reply("⛔ Akses ditolak.")
            return

        status_msg = await message.reply("⏳ Memulai Hard Reset...")

        try:
            await self.db.clear_all_data()

            await asyncio.to_thread(clear_directory_contents, TEMP_DIR)
            await asyncio.to_thread(clear_directory_contents, SESSION_DIR)

            self.queue.release()

            await status_msg.edit(
                content=(
                    "✅ **Reset Bot Sukses Total!**\n"
                    "• Database riwayat & sesi scraping dikosongkan\n"
                    "• Semua file temporer dihapus\n"
                    "• Sesi cookie dibersihkan\n"
                    "• Queue lock dibuka paksa"
                )
            )

        except Exception as e:
            logger.error(f"Reset error: {e}", exc_info=True)
            await status_msg.edit(content=f"❌ Reset gagal: `{str(e)[:200]}`")

    # ──────────────────────────────────────────────
    # Background Patrol Loop (gatekeep: hanya akun yang sudah selesai historical dump)
    # ──────────────────────────────────────────────

    @tasks.loop(minutes=10)
    async def patrol_loop(self) -> None:
        """
        Background patrol: scan berkala (tiap 10 menit).
        Hanya memproses akun yang sudah menyelesaikan historical dump
        (last_scraped_id != '').
        """
        if self.patrol_paused:
            logger.info(f"{TAG_PATROL} Patrol di-pause. Skip siklus ini.")
            return

        if self.is_scanning or self.queue.is_busy or not self._job_queue.empty():
            q_size = self._job_queue.qsize()
            # FIX: downgraded to WARNING so operators see skipped cycles in production logs.
            # Structured log includes current lock holder URL so it's actionable.
            lock_info = (
                f" — lock held by: {self.queue.current_task.profile_url}"
                if self.queue.current_task else ""
            )
            logger.warning(
                f"{TAG_PATROL} Siklus patroli DILEWATI — antrean/task masih aktif "
                f"(queue_busy={self.queue.is_busy}, job_queue={q_size}, "
                f"is_scanning={self.is_scanning}){lock_info}."
            )
            return

        # Hanya ambil akun yang siap dipatroli (historical dump selesai)
        accounts = await self.db.get_patrol_ready_accounts()
        if not accounts:
            logger.info(f"{TAG_PATROL} Tidak ada akun siap dipatroli.")
            return

        logger.info(f"{TAG_PATROL} Memulai patroli {len(accounts)} akun...")
        async with self.scan_lock:
            if self.is_scanning or self.queue.is_busy or not self._job_queue.empty():
                logger.info(f"{TAG_PATROL} Scan baru terdeteksi saat acquire lock. Skip.")
                return
            self.is_scanning = True
        try:
            await self._run_batch_scan(accounts, forced=False)
        finally:
            self.is_scanning = False

    @patrol_loop.before_loop
    async def before_patrol_loop(self) -> None:
        """Tunggu bot siap sebelum mulai patrol."""
        await self.wait_until_ready()

    # ──────────────────────────────────────────────
    # Batch Scan Engine (dipanggil oleh /force dan patrol_loop)
    # ──────────────────────────────────────────────

    async def _run_batch_scan(
        self,
        accounts: Optional[list] = None,
        forced: bool = False,
        interaction: Optional[discord.Interaction] = None,
    ) -> tuple[int, int]:
        """
        Menjalankan scan massal terhadap akun di database.
        forced=True: dipanggil /force (scan semua akun)
        forced=False: dipanggil patrol_loop (akun sudah di-filter)
        Mengembalikan tuple (success_count, fail_count).
        """
        tag = "[⚡ FORCED]" if forced else "[🤖 SATPAM]"

        try:
            if not accounts:
                accounts = await self.db.get_all_monitored_accounts()

            if not accounts:
                logger.info(f"{tag} Tidak ada akun yang dimonitor.")
                if interaction:
                    await interaction.edit_original_response(
                        content="📋 Tidak ada akun yang dimonitor di database."
                    )
                return 0, 0

            total_accounts = len(accounts)

            logger.info(f"{tag} 🚀 Memulai scan untuk total {total_accounts} akun...")

            success_count = 0
            fail_count = 0

            for idx, acc in enumerate(accounts, 1):
                username = acc["username"]
                platform = acc["platform"]
                channel_id = acc["channel_id"]
                last_scraped_id = acc["last_scraped_id"]

                logger.info(
                    f"{tag} 🔄 Memproses akun [{idx}/{total_accounts}]: @{username} ({platform})"
                )

                # Build profile URL
                if platform == "tiktok":
                    profile_url = f"https://www.tiktok.com/@{username}"
                elif platform == "instagram":
                    profile_url = f"https://www.instagram.com/{username}/"
                elif platform == "twitter":
                    profile_url = f"https://x.com/{username}"
                else:
                    logger.warning(
                        f"{tag} Platform {platform} untuk @{username} tidak dikenal. Skip."
                    )
                    fail_count += 1
                    continue

                try:
                    ok = await self._patrol_account(
                        username,
                        platform,
                        profile_url,
                        channel_id,
                        last_scraped_id,
                        forced,
                    )
                    if ok:
                        success_count += 1
                    else:
                        fail_count += 1
                except Exception as e:
                    logger.error(
                        f"[❌ ERROR  ] Gagal scan akun @{username} ({platform}): {e}",
                        exc_info=True,
                    )
                    fail_count += 1

            summary = (
                f"✅ **Scan Selesai!**\n"
                f"• Berhasil: {success_count} akun\n"
                f"• Gagal: {fail_count} akun\n"
                f"• Total Akun: {total_accounts}"
            )
            logger.info(f"[✅ SUCCESS] {summary.replace('**', '')}")

            if interaction:
                await interaction.edit_original_response(content=summary)

            return success_count, fail_count

        except Exception as e:
            logger.error(
                f"[❌ ERROR  ] Error kritis saat batch scan: {e}", exc_info=True
            )
            if interaction:
                await interaction.edit_original_response(
                    content=f"❌ Terjadi kesalahan kritis: `{str(e)[:200]}`"
                )
            return 0, len(accounts) if accounts else 0
        finally:
            pass  # is_scanning dikelola oleh caller (/force atau patrol_loop)

    async def _run_initial_historical_scrape(
        self, username: str, platform: str, profile_url: str, channel_id: int
    ) -> None:
        """
        Melakukan pemrosesan historis awal secara penuh untuk akun baru.
        Satpam Mode tidak akan memantau akun ini hingga initial scrape ini selesai (karena last_scraped_id kosong).
        """
        try:
            logger.info(
                f"{TAG_SYSTEM} Memulai historical scrape awal untuk @{username} ({platform})"
            )
            ok = await self._patrol_account(
                username=username,
                platform=platform,
                profile_url=profile_url,
                channel_id=channel_id,
                last_scraped_id="",
                forced=True,
                wait_for_queue=True,
            )
            if ok:
                await self.db.mark_initial_scan_completed(username, platform)
                logger.info(
                    f"{TAG_SUCCESS} Historical scrape selesai @{username} ({platform}) — Patrol Mode aktif."
                )
            else:
                logger.warning(
                    f"{TAG_ERROR} Historical scrape gagal @{username} ({platform})."
                )
        except Exception as e:
            logger.error(
                f"{TAG_ERROR} Error historical scrape @{username}: {e}",
                exc_info=True,
            )

    async def _patrol_account(
        self,
        username: str,
        platform: str,
        profile_url: str,
        channel_id: int,
        last_scraped_id: Optional[str],
        forced: bool = False,
        wait_for_queue: bool = False,
    ) -> bool:
        """
        Patroli satu akun tertentu: cari post baru, download, kirim, dan update last_scraped_id.
        Mengembalikan True jika sukses/selesai tanpa error, atau False jika skip/gagal.
        """
        tag = TAG_SYSTEM if forced else TAG_PATROL
        channel = self.get_channel(channel_id)
        if not channel:
            logger.error(
                f"{TAG_ERROR} Channel ID {channel_id} untuk @{username} tidak ditemukan. Skip."
            )
            return False

        # Pre-flight check: TikTok & Instagram berjalan dalam Guest Mode murni
        if not BaseScraper.has_auth_configured(platform):
            logger.warning(
                f"[⚠️ WARN  ] Tidak ada session cookies untuk {platform} — melanjutkan dengan Guest Mode (Zero-Login)..."
            )

        # Acquire lock agar tidak bertabrakan dengan manual trigger atau patrol lainnya
        if wait_for_queue:
            await self.queue.acquire(platform, profile_url)
            acquired = True
        else:
            acquired = await self.queue.try_acquire(platform, profile_url)
        if not acquired:
            logger.info(
                f"{tag} @{username} sedang sibuk diproses di pipeline lain. Skip patrol."
            )
            return False

        sender = MediaSender(channel=cast(Any, channel), downloader=self.downloader)
        scraper = self._create_scraper(platform)

        try:
            # 1. Scrape profil
            logger.info(f"[🔍 CRAWL] Memulai scraping profil @{username} ({platform})")
            async with scraper:
                all_scraped: list[PostMedia] = []
                async for post in scraper.scrape_profile(profile_url, forced=forced):
                    all_scraped.append(post)

            # Jika scraper disetop karena browser/cookie initialization failure atau terhalang Captcha
            if scraper.failed or getattr(scraper, "blocked_by_challenge", False):
                logger.warning(
                    f"{tag} @{username} ({platform}) scraping tidak berhasil (failure/challenge) — membatalkan status selesai untuk dicoba ulang di siklus berikutnya."
                )
                return False

            if not all_scraped:
                # FIX: differentiate "account truly empty" from "no NEW posts since last patrol".
                # Before this fix both cases logged "tidak memiliki postingan" (account has no posts),
                # which is misleading when the TikTok scraper pre-filters all known posts via
                # check_post_exists and yields 0 — making a 67-post account look like it has nothing.
                if last_scraped_id:
                    # Patrol mode: scraper yielded 0 because all posts already in downloaded_posts DB.
                    # This is normal — no new content since last patrol cycle.
                    logger.info(
                        f"{tag} @{username} ({platform}) tidak ada postingan baru sejak patrol terakhir "
                        f"(last_scraped_id: {last_scraped_id})."
                    )
                else:
                    # Historical dump: scraper ran but the account genuinely has zero posts.
                    logger.info(f"{tag} @{username} ({platform}) tidak memiliki postingan (akun kosong).")
                return True

            # 2. Filter post baru dibanding last_scraped_id
            new_posts: list[PostMedia] = []
            if last_scraped_id:
                # FIX: detect when last_scraped_id no longer exists in scraped set
                # (post deleted on platform). Without this check, the for-loop exhausts
                # without breaking, leaving new_posts == all_scraped and triggering
                # a full re-upload of the entire profile history.
                scraped_ids = {p.post_id for p in all_scraped}
                if last_scraped_id not in scraped_ids:
                    logger.warning(
                        f"{tag} last_scraped_id '{last_scraped_id}' tidak ditemukan di scrape hasil "
                        f"@{username} ({platform}). Post mungkin dihapus. "
                        f"Menggunakan deduplikasi DB sebagai fallback untuk mencegah re-upload penuh."
                    )
                    # Fall through to DB dedup filter below — it will skip already-seen posts.
                    new_posts = list(all_scraped)
                else:
                    for post in all_scraped:
                        if post.post_id == last_scraped_id:
                            break
                        new_posts.append(post)
            else:
                # Historical dump: ambil SEMUA postingan untuk di-upload pertama kali
                new_posts = list(all_scraped)

            # Filter tambahan: deduplikasi di level post_id terhadap scraped_posts
            # Pass platform to avoid cross-platform post_id collision
            filtered_posts = []
            for post in new_posts:
                is_scraped = await self.db.is_post_scraped(post.post_id, platform)
                if not is_scraped:
                    filtered_posts.append(post)
            new_posts = filtered_posts

            if not new_posts:
                logger.info(
                    f"{tag} @{username} ({platform}) tidak ada postingan baru (atau semua sudah pernah terkirim)."
                )
                return True

            logger.info(
                f"{tag} Ditemukan {len(new_posts)} post baru untuk @{username} ({platform})"
            )

            # Balik urutan agar postingan terlama diposting terlebih dahulu
            new_posts.reverse()

            # Fase 1: Download Semua
            downloaded_data_list = []
            total_new = len(new_posts)
            for dl_idx, post in enumerate(new_posts, 1):
                try:
                    logger.info(f"{TAG_DOWN} [{dl_idx}/{total_new}] Unduh post {post.post_id} ({post.platform})...")
                    files, real_cap, ts = await self._download_post_media_only(post)
                    if files:
                        total_bytes = sum(f.stat().st_size for f in files if f.exists())
                        logger.info(
                            f"{TAG_DOWN} [{dl_idx}/{total_new}] Selesai — "
                            f"{len(files)} file ({fmt_size(total_bytes)})"
                        )
                        downloaded_data_list.append({
                            "post": post,
                            "files": files,
                            "caption": real_cap,
                            "timestamp": ts
                        })
                    else:
                        logger.error(f"{TAG_ERROR} [{dl_idx}/{total_new}] Gagal download post {post.post_id}.")
                except Exception as e:
                    logger.error(f"{TAG_ERROR} [{dl_idx}/{total_new}] Download error post {post.post_id}: {e}")
                
                # Jeda kecil antar download untuk menyamarkan bot
                await asyncio.sleep(random.uniform(1.0, 3.0))

            # Fase 2: Upload Semua ke Discord
            # newest_scraped_id updates on each successful upload; partial progress is preserved
            # if the loop exits mid-way (e.g., exception on a later post).
            newest_scraped_id = last_scraped_id
            for data in downloaded_data_list:
                post = data["post"]
                files = data["files"]
                real_cap = data["caption"]
                ts = data["timestamp"]

                try:
                    success = await self._upload_downloaded_media(
                        post=post,
                        downloaded_files=files,
                        sender=sender,
                        caption=real_cap,
                        timestamp=ts
                    )
                    if success:
                        newest_scraped_id = post.post_id
                        # Catat ke SQLite riwayat unduhan
                        await self.db.mark_post_downloaded(
                            post_id=post.post_id,
                            platform=post.platform,
                            profile_url=post.profile_url,
                            media_count=max(len(files), 1),
                        )
                        # Catat ke SQLite deduplikasi kirim Discord
                        await self.db.mark_post_scraped(
                            post.post_id, username, post.platform
                        )
                        logger.info(
                            f"{TAG_SUCCESS} Post {post.post_id} terkirim ke channel {channel_id}."
                        )
                    
                    # Jeda aman antar postingan agar sesuai urutan dan meminimalisir rate limit Discord
                    await asyncio.sleep(SEND_DELAY)
                    
                except Exception as e:
                    logger.error(f"[❌ SKIP] Error occurred on uploading post {post.post_id}: {e}")
                    await asyncio.sleep(4.0)
                    continue

            # 4. Update last_scraped_id
            if newest_scraped_id and newest_scraped_id != last_scraped_id:
                await self.db.update_last_scraped_id(username, platform, newest_scraped_id)
                logger.info(
                    f"[✅ SUCCESS] last_scraped_id @{username} diperbarui ke {newest_scraped_id}"
                )

            return True
        except Exception as e:
            logger.error(
                f"[❌ ERROR  ] Exception pada patroli @{username}: {e}", exc_info=True
            )
            return False
        finally:
            self.queue.release()

    # ──────────────────────────────────────────────
    # Pipeline Utama: Scraping → Download → Send
    # ──────────────────────────────────────────────

    async def _run_scraping_pipeline(
        self,
        channel: Any,
        platform: str,
        profile_url: str,
    ) -> None:
        """
        Pipeline concurrent: scraping → gather download+send → cleanup.
        """
        acquired = await self.queue.try_acquire(platform, profile_url)
        if not acquired:
            return

        # Ekstrak username dari profile URL untuk mencari custom routing channel
        username = ""
        if platform == "tiktok":
            match = re.search(r"tiktok\.com/@([^/?&#]+)", profile_url)
            if match:
                username = match.group(1)
        elif platform == "instagram":
            match = re.search(r"instagram\.com/([^/?&#/]+)", profile_url)
            if match:
                username = match.group(1)
        elif platform == "twitter":
            match = re.search(r"(?:x|twitter)\.com/([^/?&#/]+)", profile_url)
            if match:
                username = match.group(1)

        # Cek custom routing di database
        target_channel = channel
        if username:
            mapped_channel_id = await self.db.get_monitored_account_channel(username, platform)
            if mapped_channel_id:
                resolved = self.get_channel(mapped_channel_id)
                if resolved:
                    target_channel = resolved
                    ch_name = getattr(resolved, "name", str(resolved))
                    logger.info(
                        f"[📤 DISCORD] Custom routing aktif: @{username} -> #{ch_name}"
                    )
                else:
                    logger.warning(
                        f"[📤 DISCORD] Custom channel ID {mapped_channel_id} tidak ditemukan. Menggunakan default."
                    )

        sender = MediaSender(channel=cast(Any, target_channel), downloader=self.downloader)

        # Pre-flight check: TikTok & Instagram berjalan dalam Guest Mode murni
        if not BaseScraper.has_auth_configured(platform):
            logger.warning(
                f"[⚠️ WARN  ] Tidak ada session cookies untuk {platform} — menggunakan Guest Mode (Zero-Login)..."
            )
        try:
            await sender.send_text(
                f"🚀 Memulai scraping profil **{platform.capitalize()}**...\n"
                f"🔗 {profile_url}\n"
                f"⚡ Mode: {CONCURRENT_DOWNLOADS} post concurrent"
            )

            already_downloaded = await self.db.get_downloaded_count(
                profile_url, platform
            )
            if already_downloaded > 0:
                await sender.send_text(
                    f"📋 Ditemukan {already_downloaded} post yang sudah pernah diunduh.\n"
                    f"Bot akan melanjutkan dari post terakhir (resume mode)."
                )

            session_id = str(uuid.uuid4())
            await self.db.create_session(session_id, platform, profile_url)
            scraper = self._create_scraper(platform)

            posts_processed = 0
            posts_failed = 0

            logger.info(
                f"[🔍 CRAWL] Memulai scraping profil @{username if username else profile_url} ({platform})"
            )
            async with scraper:
                all_posts: list[PostMedia] = []
                async for post in scraper.scrape_profile(profile_url, forced=True):
                    all_posts.append(post)

            # Filter: singkirkan post yang sudah pernah terkirim ke Discord
            filtered_posts = []
            for post in all_posts:
                is_scraped = await self.db.is_post_scraped(post.post_id, post.platform)
                if not is_scraped:
                    filtered_posts.append(post)
            all_posts = filtered_posts

            # Balik urutan agar postingan terlama diposting terlebih dahulu (older to latest)
            all_posts.reverse()

            logger.info(
                f"[⚙️ SYSTEM] Total {len(all_posts)} post baru siap diproses — mulai sekuensial processing"
            )

            if all_posts:
                self.queue.update_progress(posts_done=0, posts_total=len(all_posts))

            # Fase 1: Download Semua
            downloaded_data_list = []
            for post in all_posts:
                try:
                    files, real_cap, ts = await self._download_post_media_only(post)
                    if files:
                        downloaded_data_list.append({
                            "post": post,
                            "files": files,
                            "caption": real_cap,
                            "timestamp": ts
                        })
                    else:
                        posts_failed += 1
                        await self.db.mark_post_failed(
                            post.post_id, post.platform, post.profile_url
                        )
                except Exception as task_err:
                    logger.error(
                        f"[❌ ERROR  ] Download error: {task_err}"
                    )
                    posts_failed += 1
                    await self.db.mark_post_failed(
                        post.post_id, post.platform, post.profile_url
                    )
                
                # Jeda kecil antar download untuk menyamarkan bot
                await asyncio.sleep(random.uniform(1.0, 3.0))

            # Fase 2: Upload Semua ke Discord
            for data in downloaded_data_list:
                post = data["post"]
                files = data["files"]
                real_cap = data["caption"]
                ts = data["timestamp"]

                try:
                    success = await self._upload_downloaded_media(
                        post=post,
                        downloaded_files=files,
                        sender=sender,
                        caption=real_cap,
                        timestamp=ts
                    )
                    if success:
                        posts_processed += 1
                        await self.db.mark_post_downloaded(
                            post_id=post.post_id,
                            platform=post.platform,
                            profile_url=post.profile_url,
                            media_count=max(len(files), 1),
                        )
                        await self.db.mark_post_scraped(
                            post.post_id, username, post.platform
                        )
                        await self.db.update_session_progress(
                            session_id, post.post_id, posts_processed
                        )
                        self.queue.update_progress(
                            posts_done=posts_processed,
                            current_post_id=post.post_id,
                        )
                    else:
                        posts_failed += 1
                        await self.db.mark_post_failed(
                            post.post_id, post.platform, post.profile_url
                        )
                except Exception as task_err:
                    logger.error(
                        f"[❌ ERROR  ] Upload error: {task_err}"
                    )
                    posts_failed += 1
                    await self.db.mark_post_failed(
                        post.post_id, post.platform, post.profile_url
                    )

                # Jeda aman antar postingan agar sesuai urutan dan meminimalisir rate limit
                await asyncio.sleep(SEND_DELAY)

            # Sinkronkan status patrol loop jika akun terdaftar di monitoring list
            if all_posts and posts_processed > 0 and username:
                # all_posts is reversed (oldest first), so [-1] is the newest post
                newest_post_id = all_posts[-1].post_id
                await self.db.update_last_scraped_id(username, platform, newest_post_id)
                logger.info(
                    f"[✅ SUCCESS] Manual pipeline selesai. last_scraped_id @{username} disinkronkan ke {newest_post_id}"
                )

            await self.db.complete_session(session_id)

            await sender.send_text(
                f"✅ **Scraping selesai!**\n\n"
                f"📊 **Statistik:**\n"
                f"  • Berhasil: {posts_processed} post\n"
                f"  • Gagal: {posts_failed} post\n"
                f"  • Profil: {profile_url}"
            )

        except TimeoutError as e:
            await sender.send_text(
                f"⏰ Timeout! {str(e)}\nSilakan jalankan ulang bot dan login di browser."
            )
        except Exception as e:
            logger.error(f"[❌ ERROR  ] Pipeline error: {e}", exc_info=True)
            await sender.send_text(
                f"❌ Error tidak terduga:\n```{str(e)[:300]}```\nCek log untuk detail."
            )
        finally:
            self.queue.release()

    async def _download_post_media_only(
        self, post: PostMedia
    ) -> tuple[list[Path], Optional[str], Optional[str]]:
        """
        Mendownload media dari post (tanpa mengirimnya).
        """
        downloaded_files = []
        real_caption = None
        ytdl_timestamp = None

        if post.media_urls:
            logger.info(
                f"[📥 DOWN  ] Mengunduh direct media ({len(post.media_urls)} file) untuk {post.post_id}..."
            )

            async def download_one(i, url):
                ext = url.split("?")[0].split(".")[-1].lower()
                if ext not in {"jpg", "jpeg", "png", "webp", "mp4", "mov"}:
                    if post.media_type == MediaType.VIDEO or "/v/" in url or ".mp4" in url or "video" in url:
                        ext = "mp4"
                    else:
                        ext = "jpg"
                filename = f"{post.post_id}_{i+1:03d}.{ext}"
                return await self.downloader.download_direct_url(url, filename)

            tasks = [download_one(i, url) for i, url in enumerate(post.media_urls)]
            results = await asyncio.gather(*tasks)
            downloaded_files = [p for p in results if p is not None]
        else:
            logger.info(
                f"[📥 DOWN  ] Mengunduh postingan via scraper/yt-dlp: {post.post_url}"
            )
            downloaded_files, real_caption, ytdl_timestamp = (
                await self.downloader.download_post(
                    post.post_url,
                    post.post_id,
                    cookies_file=post.cookies_file,
                )
            )
        return downloaded_files, real_caption, ytdl_timestamp

    async def _upload_downloaded_media(
        self,
        post: PostMedia,
        downloaded_files: list[Path],
        sender: MediaSender,
        caption: Optional[str] = None,
        timestamp: Optional[str] = None,
    ) -> bool:
        """
        Mengompresi video (jika perlu) dan mengirim media yang sudah terdownload ke Discord.
        """
        processed_files = []
        over_limit_flags = []

        for file_path in downloaded_files:
            if self.downloader.get_file_type(file_path) == "video":
                compressed, is_over = await self.downloader.compress_if_needed(
                    file_path
                )
                processed_files.append(compressed)
                over_limit_flags.append(is_over)
            else:
                processed_files.append(file_path)
                over_limit_flags.append(False)

        # Ekstrak username dari profile_url secara aman
        username = None
        if post.profile_url:
            if "tiktok.com" in post.profile_url:
                match = re.search(r"tiktok\.com/@([^/?&#]+)", post.profile_url)
                if match:
                    username = match.group(1)
            elif "instagram.com" in post.profile_url:
                match = re.search(r"instagram\.com/([^/?&#/]+)", post.profile_url)
                if match:
                    username = match.group(1)
            elif "x.com" in post.profile_url or "twitter.com" in post.profile_url:
                match = re.search(r"(?:x|twitter)\.com/([^/?&#/]+)", post.profile_url)
                if match:
                    username = match.group(1)
        if username and not username.startswith("@"):
            username = f"@{username}"

        post_date = timestamp or post.timestamp
        post_caption = (caption or post.caption or "")[:1900]

        logger.info(
            f"[📤 DISCORD] Mengirim media post {post.post_id} ke channel Discord..."
        )
        success = await sender.send_post(
            file_paths=processed_files,
            caption=post_caption,
            files_over_limit=over_limit_flags,
            username=username,
            post_url=post.post_url,
            post_date=post_date,
        )
        return success

    def _create_scraper(self, platform: str):
        """Factory method untuk membuat instance scraper."""
        if platform == "tiktok":
            return TikTokScraper(db_manager=self.db, session_dir=SESSION_DIR, headed=BROWSER_HEADED)
        elif platform == "instagram":
            return InstagramScraper(
                db_manager=self.db,
                session_dir=SESSION_DIR,
                headed=BROWSER_HEADED,
            )
        elif platform == "twitter":
            return TwitterScraper(
                db_manager=self.db,
                session_dir=SESSION_DIR,
                headed=BROWSER_HEADED,
            )

    async def close(self) -> None:
        """Graceful shutdown hook terpadu untuk membersihkan worker task, loop patrol, browser, dan DB."""
        logger.info(f"{TAG_SYSTEM} Menutup bot dan membersihkan seluruh background worker & subprocess...")
        
        # 1. Batalkan worker background queue dan tunggu selesai
        if self._worker_task and not self._worker_task.done():
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass

        # 2. Hentikan background patrol loop dan tunggu task-nya selesai
        if self.patrol_loop.is_running():
            self.patrol_loop.cancel()
            # FIX: await the underlying task so the loop cannot fire once more
            # after close() returns. tasks.Loop.cancel() is async-fire-and-forget;
            # without this the loop may still execute one more cycle.
            patrol_task = self.patrol_loop.get_task()
            if patrol_task is not None and not patrol_task.done():
                try:
                    await asyncio.wait_for(patrol_task, timeout=5.0)
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    pass

        # 3. Tutup browser Playwright shared pool
        try:
            await self.downloader.close_browser()
        except Exception as e:
            logger.warning(f"{TAG_WARN} Gagal menutup browser downloader: {e}")

        # 4. Flush WAL dan tutup koneksi database SQLite
        try:
            await self.db.close()
        except Exception as e:
            logger.warning(f"{TAG_WARN} Gagal menutup koneksi DB: {e}")

        logger.info(f"{TAG_SYSTEM} Bot shutdown selesai — resource bersih.")
        await super().close()


# ──────────────────────────────────────────────
# Entry Point
# ──────────────────────────────────────────────


def main() -> None:
    """Fungsi utama untuk menjalankan bot dengan graceful SIGTERM handling."""
    if not DISCORD_BOT_TOKEN:
        raise ValueError("DISCORD_BOT_TOKEN tidak ditemukan di file .env!")
    if DISCORD_CHANNEL_ID == 0:
        raise ValueError("DISCORD_CHANNEL_ID tidak ditemukan di file .env!")
    if ALLOWED_USER_ID == 0:
        raise ValueError("DISCORD_ALLOWED_USER_ID tidak ditemukan di file .env!")

    bot = MediaScraperBot()

    async def _run():
        loop = asyncio.get_running_loop()

        # Graceful shutdown: SIGTERM (Docker stop / systemd stop) dan SIGINT (Ctrl+C)
        # FIX: _shutdown_flag prevents double-close race.
        # Scenario: SIGTERM fires _signal_handler → schedules bot.close().
        # Then finally block at L1675 also calls bot.close() if not bot.is_closed().
        # Two concurrent close() calls race on _worker_task cancellation and db.close().
        # Guard: check bot.is_closed() AND a local flag (is_closed() may not be True yet
        # during the brief window between close() being scheduled and it setting the flag).
        _shutdown_scheduled = False

        def _signal_handler():
            nonlocal _shutdown_scheduled
            if _shutdown_scheduled or bot.is_closed():
                logger.debug(f"{TAG_SYSTEM} Shutdown sudah dijadwalkan — sinyal duplikat diabaikan.")
                return
            _shutdown_scheduled = True
            logger.info(f"{TAG_SYSTEM} SIGTERM/SIGINT diterima — menjadwalkan graceful shutdown...")
            loop.call_soon_threadsafe(loop.create_task, bot.close())

        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, _signal_handler)
            except (NotImplementedError, OSError):
                # Windows tidak support add_signal_handler — skip, biarkan default handler
                pass

        try:
            await bot.start(DISCORD_BOT_TOKEN)
        except (asyncio.CancelledError, KeyboardInterrupt):
            pass
        except Exception as e:
            # Jika bot sedang shutdown/close, abaikan exception penutupan connector aiohttp
            if bot.is_closed() or "Connector is closed" in str(e):
                pass
            else:
                logger.error(f"{TAG_ERROR} Bot runtime error: {e}")
        finally:
            if not bot.is_closed():
                await bot.close()

    asyncio.run(_run())


if __name__ == "__main__":
    main()

"""
core/sender.py
Manajer pengiriman media ke Discord channel menggunakan discord.py.

Fitur:
- Kirim single media (foto/video) via discord.File
- Kirim carousel multi-file (max 10 per message, chunked otomatis)
- File > 25MB: kirim sebagai link/warning (Discord Nitro = 50MB, free = 25MB)
- Delay aman antar pengiriman
- Hapus file lokal setelah sukses terkirim

Arsitektur:
    send_post()
        ├── 1 file  → send_single_media() → channel.send(file=discord.File)
        └── N files → send_carousel()     → channel.send(files=[discord.File...])
                         ├── chunk 1 (max 10 files)
                         ├── chunk 2 (max 10 files)
                         └── ...

Discord Upload Limits:
    - Free tier: 25MB per file
    - Nitro Basic: 50MB per file
    - Max 10 files per message (attachments)
    - Max 2000 chars per message content
"""

import asyncio
import logging
from io import BytesIO
from pathlib import Path
from typing import Optional
from datetime import datetime, timedelta

import aiofiles  # Async file I/O — tidak blokir event loop saat baca file besar
import discord

from .downloader import MediaDownloader
from .utils import TAG_DISCORD, TAG_WARN, TAG_ERROR, TAG_SUCCESS, fmt_size

logger = logging.getLogger(__name__)

import os

# Jeda aman antar pengiriman pesan
SEND_DELAY_SECONDS = 3

# Max files per discord message (hard limit dari Discord API)
DISCORD_MAX_FILES_PER_MSG = 10

# Max file size per attachment di-load dari env MAX_FILE_SIZE_MB (default 10MB)
MAX_FILE_SIZE_MB = int(os.getenv("MAX_FILE_SIZE_MB", "10"))
DISCORD_MAX_FILE_BYTES = MAX_FILE_SIZE_MB * 1024 * 1024


class MediaSender:
    """
    Handler pengiriman media ke Discord channel.

    Menerima discord.TextChannel sebagai target — semua media
    dikirim ke channel ini via channel.send().
    """

    def __init__(
        self,
        channel: discord.TextChannel,
        downloader: MediaDownloader,
    ):
        self.channel = channel
        self.downloader = downloader

    @property
    def max_file_size_mb(self) -> int:
        """Dynamic evaluation of MAX_FILE_SIZE_MB env variable."""
        try:
            return int(os.getenv("MAX_FILE_SIZE_MB", "10"))
        except ValueError:
            return 10

    @property
    def max_file_bytes(self) -> int:
        """Dynamic evaluation of MAX_FILE_BYTES."""
        return self.max_file_size_mb * 1024 * 1024

    # ──────────────────────────────────────────────
    # Pengiriman Single Media
    # ──────────────────────────────────────────────

    async def send_video(
        self,
        file_path: Path,
        caption: str = "",
    ) -> bool:
        """
        Kirim file video ke Discord channel menggunakan discord.File stream.
        """
        file_size = file_path.stat().st_size
        size_mb = file_size / (1024 * 1024)

        if file_size > self.max_file_bytes:
            logger.warning(
                f"File {file_path.name} ({size_mb:.1f}MB) melebihi limit Discord ({self.max_file_size_mb}MB)."
            )
            await self.send_text(
                f"⚠️ File `{file_path.name}` ({size_mb:.1f}MB) melebihi limit upload Discord ({self.max_file_size_mb}MB).\n"
                f"File tetap tersedia di server lokal."
            )
            await self.downloader.cleanup_files_async([file_path])
            return False

        discord_file = discord.File(file_path)
        # FIX: removed duplicate file_size = file_path.stat().st_size call here.
        # file_size was already captured at L96 before the early-return guards above.
        # Re-reading stat() here was redundant (and a syscall per send).
        try:
            await self.channel.send(
                file=discord_file,
            )
            logger.info(f"{TAG_DISCORD} Video terkirim: {file_path.name} ({fmt_size(file_size)})")
        except discord.HTTPException as e:
            logger.error(f"{TAG_ERROR} Discord HTTP error kirim video {file_path.name}: {e}")
            return False
        except Exception as e:
            logger.error(f"{TAG_ERROR} Error tak terduga kirim video {file_path.name}: {e}")
            return False
        finally:
            discord_file.close()

        await asyncio.sleep(1.5)
        await self.downloader.cleanup_files_async([file_path])
        return True

    async def send_single_media(
        self,
        file_path: Path,
        caption: str = "",
    ) -> bool:
        """
        Kirim satu file media ke Discord channel.
        Mendeteksi tipe file dan merujuk video ke send_video.
        """
        file_type = self.downloader.get_file_type(file_path)

        if file_type == "video":
            return await self.send_video(file_path, caption)

        file_size = file_path.stat().st_size

        # Cek apakah file melebihi limit upload Discord
        if file_size > self.max_file_bytes:
            size_mb = file_size / (1024 * 1024)
            logger.warning(
                f"{TAG_WARN} {file_path.name} ({size_mb:.1f}MB) melebihi "
                f"limit Discord ({self.max_file_size_mb}MB) — skip."
            )
            # Kirim notifikasi teks bahwa file terlalu besar
            await self.send_text(
                f"⚠️ File `{file_path.name}` ({size_mb:.1f}MB) melebihi limit upload Discord ({self.max_file_size_mb}MB).\n"
                f"File tetap tersedia di server lokal."
            )
            # Cleanup file yang tidak bisa dikirim
            await self.downloader.cleanup_files_async([file_path])
            return False

        discord_file = discord.File(file_path)
        # FIX: removed duplicate file_size = file_path.stat().st_size call here.
        # file_size was already captured at L144 before the early-return guards above.
        try:
            await self.channel.send(
                file=discord_file,
            )
            logger.info(f"{TAG_DISCORD} Media terkirim: {file_path.name} ({fmt_size(file_size)})")
        except discord.HTTPException as e:
            logger.error(f"{TAG_ERROR} Discord HTTP error kirim {file_path.name}: {e}")
            return False
        except Exception as e:
            logger.error(f"{TAG_ERROR} Error tak terduga kirim {file_path.name}: {e}")
            return False
        finally:
            discord_file.close()

        # Jeda sebelum cleanup agar tidak race condition dengan Discord upload buffer
        await asyncio.sleep(1.5)
        await self.downloader.cleanup_files_async([file_path])
        return True

    # ──────────────────────────────────────────────
    # Pengiriman Carousel (Multi-File)
    # ──────────────────────────────────────────────

    @staticmethod
    def chunk_file_list(
        file_list: list[Path],
        chunk_size: int = DISCORD_MAX_FILES_PER_MSG,
    ) -> list[list[Path]]:
        """
        Pecah list file menjadi chunks (max 10 per message).

        Contoh:
            18 files → [[file1..file10], [file11..file18]]
            → 2x channel.send() calls
        """
        return [
            file_list[i : i + chunk_size] for i in range(0, len(file_list), chunk_size)
        ]

    async def send_carousel(
        self,
        file_paths: list[Path],
        caption: str = "",
    ) -> bool:
        """
        Kirim kumpulan file sebagai satu/beberapa message dengan multi-file attachment.
        Tanpa menggunakan embed, melampirkan multiple discord.File dalam satu message.
        """
        if not file_paths:
            return False

        # Filter file yang terlalu besar sebelum proses
        # FIX: use self.max_file_bytes (dynamic property) instead of module-level DISCORD_MAX_FILE_BYTES
        # constant (computed once at import). If MAX_FILE_SIZE_MB env changes at runtime,
        # the old constant stays stale and wrong files get skipped.
        uploadable: list[Path] = []
        skipped = 0
        for path in file_paths:
            if path.stat().st_size <= self.max_file_bytes:
                uploadable.append(path)
            else:
                skipped += 1
                logger.warning(
                    f"{TAG_WARN} Skip {path.name}: {fmt_size(path.stat().st_size)} "
                    f"> {self.max_file_size_mb}MB limit"
                )

        if skipped > 0:
            await self.send_text(
                f"⚠️ {skipped} file terlalu besar untuk Discord (>{self.max_file_size_mb}MB), di-skip."
            )

        if not uploadable:
            # Semua file terlalu besar
            await self.downloader.cleanup_files_async(file_paths)
            return False

        # Pecah menjadi chunks
        chunks = self.chunk_file_list(uploadable, DISCORD_MAX_FILES_PER_MSG)
        total_chunks = len(chunks)
        all_success = True
        sent_messages = []

        for idx, chunk in enumerate(chunks):
            # Bangun list discord.File langsung dari path
            discord_files = [discord.File(p) for p in chunk]
            chunk_total_bytes = sum(p.stat().st_size for p in chunk)

            logger.info(
                f"{TAG_DISCORD} Upload carousel [{idx + 1}/{total_chunks}] "
                f"— {len(discord_files)} file ({fmt_size(chunk_total_bytes)})"
            )

            try:
                # Kirim ke channel tanpa caption karena sudah ada di visual divider embed
                sent_message = await self.channel.send(
                    files=discord_files,
                )
                sent_messages.append(sent_message)
                logger.info(f"{TAG_DISCORD} Carousel [{idx + 1}/{total_chunks}] terkirim.")
            except discord.HTTPException as e:
                logger.error(f"{TAG_ERROR} Discord error carousel chunk {idx + 1}: {e}")
                all_success = False
            finally:
                # Tutup fp dari file discord agar tidak lock di Windows
                for f in discord_files:
                    f.close()

            # Jeda antar chunk agar tidak kena rate limit Discord
            if idx < total_chunks - 1:
                await asyncio.sleep(SEND_DELAY_SECONDS)

        # Keep files after partial delivery. Caller can retry instead of losing failed chunks.
        if all_success:
            await asyncio.sleep(1.5)
            await self.downloader.cleanup_files_async(file_paths)
        else:
            # Restore all-or-nothing behavior: a retry must not duplicate earlier chunks.
            for message in sent_messages:
                try:
                    await message.delete()
                except discord.HTTPException as e:
                    logger.error(f"{TAG_ERROR} Gagal rollback carousel message: {e}")
            logger.warning(f"{TAG_WARN} Carousel partial delivery — file dipertahankan untuk recovery.")
            # FIX: cleanup temp files even on failure to prevent disk leak accumulation.
            # Rollback deletes the Discord messages, so retaining local files serves no purpose
            # (caller has no retry mechanism that would use them). Cleanup immediately.
            await asyncio.sleep(1.5)
            await self.downloader.cleanup_files_async(file_paths)
        return all_success

    # ──────────────────────────────────────────────
    # Orkestrasi Pengiriman Postingan
    # ──────────────────────────────────────────────

    async def send_post(
        self,
        file_paths: list[Path],
        caption: str = "",
        files_over_limit: Optional[list[bool]] = None,
        username: Optional[str] = None,
        post_url: Optional[str] = None,
        post_date: Optional[str] = None,
    ) -> bool:
        """
        Kirim sebuah postingan ke Discord.
        Otomatis memilih single media atau carousel.

        Args:
            file_paths: List path file media
            caption: Caption postingan
            files_over_limit: List boolean per file, True jika file masih
                             terlalu besar setelah kompresi (akan dicoba kirim
                             individual dengan warning)
            username: Username dari akun media sosial
            post_url: URL postingan asli
            post_date: Tanggal postingan
        """
        if not file_paths:
            return False

        if files_over_limit is None:
            files_over_limit = [False] * len(file_paths)

        # GUARANTEED TIMESTAMP SAFE GUARD
        formatted_date = "Baru Saja (Patrol)"
        if post_date and str(post_date).lower() != "none":
            try:
                clean_date_str = str(post_date).split('.')[0].replace('T', ' ').replace('Z', '')
                dt = datetime.strptime(clean_date_str, "%Y-%m-%d %H:%M:%S")
                formatted_date = (dt + timedelta(hours=7)).strftime("%d-%m-%Y %H:%M:%S")
            except Exception:
                formatted_date = str(post_date)

        # FORCE EMBED RENDERING REGARDLESS OF THE METADATA SOURCE
        clean_user = username.lstrip('@') if username else "Unknown User"
        display_caption = caption if caption else "Tidak ada deskripsi/caption."

        # Tentukan link profil berdasarkan platform
        # Default empty string — avoids wrong Instagram fallback when post_url is None
        profile_url = ""
        if post_url:
            if "tiktok.com" in post_url:
                profile_url = f"https://www.tiktok.com/@{clean_user}"
            elif "twitter.com" in post_url or "x.com" in post_url:
                profile_url = f"https://x.com/{clean_user}"
            else:
                profile_url = f"https://www.instagram.com/{clean_user}/"

        icons_dir = Path(os.getenv("ICONS_DIR", "./icons"))
        icons_dir.mkdir(parents=True, exist_ok=True)

        platform_name = "Instagram"
        icon_filename = "instagram.png"
        if post_url:
            if "tiktok.com" in post_url:
                platform_name = "TikTok"
                icon_filename = "tiktok.png"
            elif "twitter.com" in post_url or "x.com" in post_url:
                platform_name = "Twitter/X"
                icon_filename = "twitter.png"

        icon_path = icons_dir / icon_filename
        has_local_icon = icon_path.exists()

        # Build description
        description_text = (
            f"🔗 **[{platform_name} Post @{clean_user}]({post_url})**\n\n"
            f"{display_caption}\n\n▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬"
        )

        divider_embed = discord.Embed(
            description=description_text,
            color=discord.Color.dark_teal()
        )
        divider_embed.add_field(name="📅 Tanggal Post (WIB)", value=f"`{formatted_date}`", inline=True)

        # Send header embed BEFORE media attachments so caption metadata renders above attachments in Discord channel
        try:
            await self.channel.send(embed=divider_embed)
            logger.info(f"{TAG_DISCORD} Header embed terkirim untuk post @{clean_user}.")
        except discord.HTTPException as e:
            logger.error(f"{TAG_ERROR} Gagal kirim header embed: {e}")

        if len(file_paths) == 1:
            # Single media
            success = await self.send_single_media(file_paths[0], caption="")
        else:
            # Multi-media (carousel)
            # Jika ada file over limit, kirim satu-satu agar yang kecil tetap terkirim
            if any(files_over_limit):
                success = True
                for i, (path, over) in enumerate(zip(file_paths, files_over_limit)):
                    r = await self.send_single_media(path, caption="")
                    success = success and r
                    # FIX: skip sleep after the very last file to avoid unnecessary 3s delay
                    if i < len(file_paths) - 1:
                        await asyncio.sleep(SEND_DELAY_SECONDS)
            else:
                success = await self.send_carousel(file_paths, caption="")

        return success

    # ──────────────────────────────────────────────
    # Utility: Pesan Teks
    # ──────────────────────────────────────────────

    async def send_text(self, text: str) -> None:
        """Kirim pesan teks biasa ke channel."""
        try:
            await self.channel.send(content=text[:2000])
        except discord.HTTPException as e:
            logger.error(f"{TAG_ERROR} Gagal kirim pesan teks: {e}")

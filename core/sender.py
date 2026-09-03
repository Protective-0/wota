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
import os
from pathlib import Path
from typing import Optional
from datetime import datetime, timedelta

import discord

from .downloader import MediaDownloader
from .utils import (
    TAG_DISCORD,
    TAG_WARN,
    TAG_ERROR,
    TAG_SUCCESS,
    fmt_size,
    format_wib_date,
)

logger = logging.getLogger(__name__)

# Jeda aman antar pengiriman pesan
# FIX: configurable via env SEND_DELAY_SECONDS — previously hardcoded to 3s
SEND_DELAY_SECONDS = int(os.getenv("SEND_DELAY_SECONDS", "3"))

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
        Kirim file video ke Discord channel menggunakan async file stream.
        """
        try:
            file_size = file_path.stat().st_size
        except Exception as e:
            logger.error(f"{TAG_ERROR} Gagal membaca ukuran file video {file_path.name}: {e}")
            return False

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

        try:
            # FIX: pass file path directly to discord.File instead of reading
            # the entire file into RAM via aiofiles.read() + BytesIO.
            # discord.py natively accepts a file-like object or Path and streams
            # it from disk — this saves up to 2x file-size worth of heap allocation.
            discord_file = discord.File(fp=file_path, filename=file_path.name)
            try:
                await self.channel.send(file=discord_file)
                logger.info(f"{TAG_DISCORD} Video terkirim: {file_path.name} ({fmt_size(file_size)})")
            finally:
                discord_file.close()
        except discord.HTTPException as e:
            logger.error(f"{TAG_ERROR} Discord HTTP error kirim video {file_path.name}: {e}")
            return False
        except Exception as e:
            logger.error(f"{TAG_ERROR} Error tak terduga kirim video {file_path.name}: {e}")
            return False

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

        try:
            file_size = file_path.stat().st_size
        except Exception as e:
            logger.error(f"{TAG_ERROR} Gagal membaca ukuran file {file_path.name}: {e}")
            return False

        # Cek apakah file melebihi limit upload Discord
        if file_size > self.max_file_bytes:
            size_mb = file_size / (1024 * 1024)
            logger.warning(
                f"{TAG_WARN} {file_path.name} ({size_mb:.1f}MB) melebihi "
                f"limit Discord ({self.max_file_size_mb}MB) — skip."
            )
            await self.send_text(
                f"⚠️ File `{file_path.name}` ({size_mb:.1f}MB) melebihi limit upload Discord ({self.max_file_size_mb}MB).\n"
                f"File tetap tersedia di server lokal."
            )
            await self.downloader.cleanup_files_async([file_path])
            return False

        try:
            # FIX: pass file path directly to discord.File — avoids reading whole file
            # into RAM via aiofiles + BytesIO (saves up to 2x file-size heap usage).
            discord_file = discord.File(fp=file_path, filename=file_path.name)
            try:
                await self.channel.send(file=discord_file)
                logger.info(f"{TAG_DISCORD} Media terkirim: {file_path.name} ({fmt_size(file_size)})")
            finally:
                discord_file.close()
        except discord.HTTPException as e:
            logger.error(f"{TAG_ERROR} Discord HTTP error kirim {file_path.name}: {e}")
            return False
        except Exception as e:
            logger.error(f"{TAG_ERROR} Error tak terduga kirim {file_path.name}: {e}")
            return False

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
        Kirim kumpulan file sebagai postingan multi-media (carousel).
        - Gambar (foto) dikelompokkan dalam satu/beberapa pesan agar Discord menampilkannya sebagai collage grid.
        - Video dikirim per pesan individual agar Discord merender player video aktif untuk SETIAP video
          (karena Discord client hanya merender player video pertama jika multiple video digabung).
        - Memastikan ukuran tiap pesan tidak melebihi batas upload Discord (10MB default).
        """
        if not file_paths:
            return False

        uploadable: list[Path] = []
        skipped = 0
        for path in file_paths:
            try:
                file_size = path.stat().st_size
                if file_size <= self.max_file_bytes:
                    uploadable.append(path)
                else:
                    skipped += 1
                    logger.warning(
                        f"{TAG_WARN} Skip {path.name}: {fmt_size(file_size)} "
                        f"> {self.max_file_size_mb}MB limit"
                    )
            except Exception as e:
                logger.warning(f"Gagal stat file carousel {path.name}: {e}")

        if skipped > 0:
            await self.send_text(
                f"⚠️ {skipped} file terlalu besar untuk Discord (>{self.max_file_size_mb}MB), di-skip."
            )

        if not uploadable:
            await self.downloader.cleanup_files_async(file_paths)
            return False

        # Pisahkan file foto dan file video
        image_files = [p for p in uploadable if p.suffix.lower() in {".jpg", ".jpeg", ".png", ".webp"}]
        video_files = [p for p in uploadable if p.suffix.lower() in {".mp4", ".mov", ".webm", ".mkv"}]

        delivered_count = 0
        sent_messages = []

        # 1. Kirim Foto (dikelompokkan agar Discord membuat collage grid foto)
        # Batasi ukuran per chunk agar total bytes foto <= max_file_bytes dan jumlah <= 10
        if image_files:
            image_chunks: list[list[Path]] = []
            curr_chunk: list[Path] = []
            curr_chunk_bytes = 0

            for img in image_files:
                img_sz = img.stat().st_size
                if len(curr_chunk) >= DISCORD_MAX_FILES_PER_MSG or (curr_chunk and curr_chunk_bytes + img_sz > self.max_file_bytes):
                    image_chunks.append(curr_chunk)
                    curr_chunk = [img]
                    curr_chunk_bytes = img_sz
                else:
                    curr_chunk.append(img)
                    curr_chunk_bytes += img_sz
            if curr_chunk:
                image_chunks.append(curr_chunk)

            total_img_chunks = len(image_chunks)
            for idx, chunk in enumerate(image_chunks):
                discord_files = []
                chunk_bytes = 0
                for p in chunk:
                    try:
                        discord_files.append(discord.File(fp=p, filename=p.name))
                        chunk_bytes += p.stat().st_size
                    except Exception as err:
                        logger.error(f"{TAG_ERROR} Gagal membuka foto {p.name}: {err}")

                if not discord_files:
                    continue

                logger.info(
                    f"{TAG_DISCORD} Upload carousel foto [{idx + 1}/{total_img_chunks}] "
                    f"— {len(discord_files)} foto ({fmt_size(chunk_bytes)})"
                )

                try:
                    sent_msg = await self.channel.send(files=discord_files)
                    sent_messages.append(sent_msg)
                    delivered_count += len(discord_files)
                    logger.info(f"{TAG_DISCORD} Carousel foto [{idx + 1}/{total_img_chunks}] terkirim.")
                except Exception as e:
                    logger.error(f"{TAG_ERROR} Gagal kirim chunk foto {idx + 1}: {e}")
                finally:
                    for f in discord_files:
                        f.close()

                await asyncio.sleep(SEND_DELAY_SECONDS)

        # 2. Kirim Video (setiap video dikirim per pesan individual)
        # Sangat krusial: Discord hanya menampilkan player video pertama jika digabung dalam 1 pesan!
        total_videos = len(video_files)
        for v_idx, v_path in enumerate(video_files):
            v_size = v_path.stat().st_size
            logger.info(
                f"{TAG_DISCORD} Upload carousel video [{v_idx + 1}/{total_videos}] "
                f"— {v_path.name} ({fmt_size(v_size)})"
            )
            try:
                d_file = discord.File(fp=v_path, filename=v_path.name)
                try:
                    sent_msg = await self.channel.send(file=d_file)
                    sent_messages.append(sent_msg)
                    delivered_count += 1
                    logger.info(f"{TAG_DISCORD} Carousel video [{v_idx + 1}/{total_videos}] terkirim.")
                finally:
                    d_file.close()
            except Exception as e:
                logger.error(f"{TAG_ERROR} Gagal kirim video carousel {v_path.name}: {e}")

            if v_idx < total_videos - 1:
                await asyncio.sleep(SEND_DELAY_SECONDS)

        # Cleanup file temporer
        await asyncio.sleep(1.5)
        await self.downloader.cleanup_files_async(file_paths)

        # Jika minimal ada 1 media terkirim, anggap sukses agar bot tidak gagal total
        return delivered_count > 0

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

        # Deteksi tipe konten (Story vs Postingan Reguler)
        is_story = bool(
            "/stories/" in (post_url or "").lower()
            or "is_story=1" in (post_url or "").lower()
            or "story oleh @" in (display_caption or "").lower()
            or "instagram story" in (display_caption or "").lower()
            or "tiktok story" in (display_caption or "").lower()
        )
        content_label = f"{platform_name} Story" if is_story else f"{platform_name} Post"
        field_date_name = "📅 Tanggal Story (WIB)" if is_story else "📅 Tanggal Post (WIB)"

        # Build description
        description_text = (
            f"🔗 **[{content_label} @{clean_user}]({post_url})**\n\n"
            f"{display_caption}\n\n▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬"
        )

        divider_embed = discord.Embed(
            description=description_text,
            color=discord.Color.gold() if is_story else discord.Color.dark_teal()
        )
        divider_embed.add_field(
            name=field_date_name,
            value=format_wib_date(post_date),
            inline=False
        )

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

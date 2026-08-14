"""
core/queue_manager.py
Manajer antrean single-user dengan asyncio.Lock.
Memastikan bot hanya memproses satu sesi download pada satu waktu,
dan menolak request baru selama proses sedang berjalan.
"""

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class TaskState:
    """Representasi state task yang sedang berjalan."""
    platform: str
    profile_url: str
    started_at: datetime = field(default_factory=datetime.now)
    posts_done: int = 0
    posts_total: int = 0
    current_post_id: Optional[str] = None

    def to_status_message(self) -> str:
        """Format status task menjadi pesan yang bisa dibaca user."""
        elapsed = int((datetime.now() - self.started_at).total_seconds())  # .total_seconds() agar benar > 1 jam
        mins, secs = divmod(elapsed, 60)
        progress = f"{self.posts_done}/{self.posts_total}" if self.posts_total > 0 else f"{self.posts_done} post"
        return (
            f"⏳ *Sedang memproses profil {self.platform.capitalize()}*\n"
            f"🔗 `{self.profile_url}`\n"
            f"📦 Progress: {progress}\n"
            f"⏱ Berjalan selama: {mins}m {secs}s\n\n"
            f"_Bot akan menerima link baru setelah selesai._"
        )


class QueueManager:
    """
    Single-user state lock menggunakan asyncio.Lock.

    Cara kerja:
    - Saat bot mulai memproses link, lock di-acquire.
    - Jika ada link baru masuk dan lock sedang aktif, bot menolak dan
      memberitahu user bahwa sedang ada proses berjalan.
    - Lock di-release otomatis setelah proses selesai (via context manager).
    """

    def __init__(self):
        self._lock = asyncio.Lock()
        self._current_task: Optional[TaskState] = None

    @property
    def is_busy(self) -> bool:
        """Cek apakah bot sedang memproses task."""
        return self._lock.locked()

    @property
    def current_task(self) -> Optional[TaskState]:
        """Ambil info task yang sedang berjalan."""
        return self._current_task

    async def try_acquire(self, platform: str, profile_url: str) -> bool:
        """
        Coba ambil lock untuk task baru secara atomic.

        Returns:
            True jika berhasil acquire (bot bebas), False jika sudah sibuk.
        """
        # Atomic non-blocking check: locked() dan acquire() tidak dipisah
        # agar tidak ada race condition antara pengecekan dan akuisisi lock.
        if self._lock.locked():
            logger.info(f"Lock busy — menolak request baru: {profile_url}")
            return False

        # Non-blocking acquire with short timeout to handle TOCTOU race.
        # No asyncio.shield: shielding causes lock leak if acquire completes after timeout.
        try:
            await asyncio.wait_for(self._lock.acquire(), timeout=0.1)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            logger.info(f"Lock busy (race) — menolak request baru: {profile_url}")
            return False

        self._current_task = TaskState(
            platform=platform,
            profile_url=profile_url,
        )
        logger.info(f"Lock acquired untuk: {platform} — {profile_url}")
        return True

    async def acquire(self, platform: str, profile_url: str) -> None:
        """Wait for queued work instead of dropping a background job."""
        await self._lock.acquire()
        # FIX: guard against CancelledError between lock.acquire() and _current_task assignment.
        # If the coroutine is cancelled at this exact await point, the lock is held but
        # _current_task stays None \u2014 bot appears permanently busy with no active task.
        # Catch CancelledError, release the lock, then re-raise so the caller's finally
        # block still runs cleanly.
        try:
            self._current_task = TaskState(platform=platform, profile_url=profile_url)
            logger.info(f"Lock acquired after wait for: {platform} - {profile_url}")
        except asyncio.CancelledError:
            self._lock.release()
            logger.warning(f"Lock released on CancelledError during acquire for: {profile_url}")
            raise

    def release(self) -> None:
        """Lepas lock dan bersihkan state task (termasuk dereference object untuk cegah memory leak)."""
        self._current_task = None
        if self._lock.locked():
            self._lock.release()
            logger.info("Lock released — bot siap menerima task baru")

    def update_progress(self, posts_done: int, posts_total: int = 0, current_post_id: Optional[str] = None) -> None:
        """Update progress counter untuk task yang sedang berjalan."""
        if self._current_task:
            self._current_task.posts_done = posts_done
            if posts_total:
                self._current_task.posts_total = posts_total
            if current_post_id:
                self._current_task.current_post_id = current_post_id

    def get_busy_message(self) -> str:
        """Ambil pesan penolakan saat bot sedang sibuk."""
        if self._current_task:
            return self._current_task.to_status_message()
        return (
            "⚠️ *Bot sedang sibuk memproses request sebelumnya.*\n"
            "_Harap tunggu hingga selesai sebelum mengirim link baru._"
        )

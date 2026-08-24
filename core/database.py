"""
core/database.py
Manajer database SQLite asinkron menggunakan aiosqlite.
Bertanggung jawab atas anti-duplikasi, pencatatan status download,
dan state management untuk fitur resume.
"""

import aiosqlite
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

from .utils import extract_username_and_platform

logger = logging.getLogger(__name__)


# Skema DDL untuk semua tabel yang dibutuhkan bot
SCHEMA_SQL = """
-- Tabel utama: mencatat setiap postingan yang berhasil diunduh
CREATE TABLE IF NOT EXISTS downloaded_posts (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    post_id     TEXT NOT NULL,          -- ID unik postingan dari platform
    platform    TEXT NOT NULL,          -- 'tiktok', 'instagram', 'twitter'
    profile_url TEXT NOT NULL,          -- URL profil sumber
    media_count INTEGER DEFAULT 0,     -- Jumlah file media dalam postingan
    status      TEXT DEFAULT 'done',   -- 'done', 'failed', 'partial'
    downloaded_at DATETIME DEFAULT (datetime('now', 'localtime')),
    UNIQUE(post_id, platform)           -- Mencegah duplikasi di level DB
);

-- Tabel session: menyimpan state scraping terakhir per profil
-- Digunakan untuk fitur resume jika proses terhenti di tengah jalan
CREATE TABLE IF NOT EXISTS scrape_sessions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id      TEXT UNIQUE NOT NULL,   -- UUID sesi scraping
    platform        TEXT NOT NULL,
    profile_url     TEXT NOT NULL,
    last_post_id    TEXT,                   -- ID postingan terakhir yang diproses
    total_posts     INTEGER DEFAULT 0,      -- Estimasi total post di profil
    scraped_posts   INTEGER DEFAULT 0,      -- Counter post yang sudah diproses
    status          TEXT DEFAULT 'running', -- 'running', 'completed', 'stopped'
    created_at      DATETIME DEFAULT (datetime('now', 'localtime')),
    updated_at      DATETIME DEFAULT (datetime('now', 'localtime'))
);

-- Index untuk mempercepat query cek duplikasi
CREATE INDEX IF NOT EXISTS idx_post_lookup ON downloaded_posts(post_id, platform);
CREATE INDEX IF NOT EXISTS idx_session_profile ON scrape_sessions(profile_url, platform);

-- Tabel monitored_accounts: memetakan username ke Discord channel ID, platform, dan last_scraped_id
CREATE TABLE IF NOT EXISTS monitored_accounts (
    username           TEXT NOT NULL,          -- Username target scraper (lowercase)
    platform           TEXT NOT NULL,          -- 'tiktok', 'instagram', 'twitter'
    channel_id         INTEGER NOT NULL,       -- Discord Channel ID untuk routing media
    last_scraped_id    TEXT,                   -- ID postingan terakhir yang diproses
    initial_scan_completed INTEGER NOT NULL DEFAULT 0,
    created_at         DATETIME DEFAULT (datetime('now', 'localtime')),
    PRIMARY KEY (username, platform)
);

-- Tabel scraped_posts: mencatat deduplikasi post yang berhasil dikirim ke Discord
CREATE TABLE IF NOT EXISTS scraped_posts (
    post_id     TEXT NOT NULL,
    username    TEXT,
    platform    TEXT NOT NULL,
    uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (post_id, platform)
);
"""


class DatabaseManager:
    """
    Manajer database SQLite untuk bot scraper.
    Semua operasi bersifat asinkron menggunakan aiosqlite.
    """

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._db: Optional[aiosqlite.Connection] = None

    @property
    def db(self) -> aiosqlite.Connection:
        # Explicit check to prevent stripping under python -O optimization flag
        if self._db is None:
            raise RuntimeError("Database connection is not initialized")
        return self._db


    async def initialize(self) -> None:
        """Buka koneksi DB dan buat tabel jika belum ada."""
        db_path_obj = Path(self.db_path)

        # Jika path yang dituju ternyata sebuah direktori (misal karena folder mount Docker),
        # simpan file database di dalam direktori tersebut
        if db_path_obj.exists() and db_path_obj.is_dir():
            db_path_obj = db_path_obj / "bot_data.db"
        else:
            db_path_obj.parent.mkdir(parents=True, exist_ok=True)

        self.db_path = str(db_path_obj)
        self._db = await aiosqlite.connect(self.db_path, timeout=30.0)
        self._db.row_factory = aiosqlite.Row  # Hasil query bisa diakses seperti dict

        # Set timeout DULU sebelum mengaktifkan WAL untuk mencegah instant lock pada Linux disk
        # FIX: busy_timeout harus diset SEBELUM journal_mode=WAL karena aktivasi WAL
        # sendiri membutuhkan brief exclusive lock. Jika reader lain menahannya,
        # tanpa busy_timeout yang aktif → langsung OperationalError: database is locked.
        await self._db.execute("PRAGMA busy_timeout = 5000")
        await self._db.execute("PRAGMA journal_mode=WAL")
        await self._db.execute("PRAGMA foreign_keys=ON")
        await self._db.execute("PRAGMA synchronous=NORMAL")

        # Jalankan skema DDL untuk membuat semua tabel
        await self._db.executescript(SCHEMA_SQL)
        await self._db.commit()

        # Migrate legacy keys without discarding monitored accounts or dedup history.
        await self._migrate_monitored_accounts()
        await self._migrate_scraped_posts()

        logger.info(f"[⚙️ SYSTEM] Database initialized: {self.db_path}")

    async def _migrate_monitored_accounts(self) -> None:
        """
        Melakukan pengecekan kolom pada monitored_accounts dan memigrasikan tabel
        jika ada kolom baru yang belum ada.
        Jika migrasi gagal karena ketidaksesuaian/kerusakan data, rebuild tabel monitored_accounts.
        """
        try:
            # Dapatkan list kolom saat ini dari tabel monitored_accounts (1x query)
            current_columns = []
            primary_key = []
            async with self.db.execute("PRAGMA table_info(monitored_accounts)") as cursor:
                async for row in cursor:
                    current_columns.append(row["name"])
                    if row["pk"]:
                        primary_key.append(row["name"])

            if not current_columns:
                logger.info("[⚙️ SYSTEM] Tabel monitored_accounts baru dibuat, tidak memerlukan migrasi.")
                return

            # Cek kolom yang dibutuhkan (presence check saja, bukan DDL generation)
            # FIX: hapus col_def yang stale — schema asli pakai composite PK (username, platform)
            # bukan TEXT PRIMARY KEY per kolom. Dict ini hanya dipakai untuk cek keberadaan kolom.
            required_cols = {
                "username",
                "platform",
                "channel_id",
                "last_scraped_id",
                "initial_scan_completed",
            }

            needs_rebuild = (
                primary_key != ["username", "platform"]
                or "channel_id" not in current_columns
                or "platform" not in current_columns
                or "initial_scan_completed" not in current_columns
            )

            if needs_rebuild:
                logger.info("[⚙️ SYSTEM] Migrasi memerlukan rekonstruksi tabel monitored_accounts (mengubah primary key / kolom)...")
                await self._rebuild_monitored_accounts_safely()
                return

            # Jika tidak butuh rebuild total, tambahkan kolom yang kurang via ALTER TABLE
            for col_name in required_cols:
                if col_name not in current_columns:
                    # Tentukan tipe default berdasarkan nama kolom
                    col_type = (
                        "INTEGER NOT NULL DEFAULT 0" if col_name == "initial_scan_completed"
                        else "INTEGER NOT NULL" if col_name == "channel_id"
                        else "TEXT"
                    )
                    logger.info(f"[⚙️ SYSTEM] Migrasi monitored_accounts: Menambahkan kolom {col_name} ({col_type})")
                    await self.db.execute(f"ALTER TABLE monitored_accounts ADD COLUMN {col_name} {col_type}")
            
            await self.db.commit()

        except Exception as e:
            logger.error(f"[❌ ERROR  ] Gagal melakukan migrasi monitored_accounts: {e}. Fallback ke rebuild...", exc_info=True)
            await self._rebuild_monitored_accounts_safely()

    async def _rebuild_monitored_accounts_safely(self) -> None:
        """Replace legacy schema only after copied rows are safely staged."""
        try:
            await self.db.execute("""
                CREATE TABLE monitored_accounts_new (
                    username TEXT NOT NULL,
                    platform TEXT NOT NULL,
                    channel_id INTEGER NOT NULL,
                    last_scraped_id TEXT,
                    initial_scan_completed INTEGER NOT NULL DEFAULT 0,
                    created_at DATETIME DEFAULT (datetime('now', 'localtime')),
                    PRIMARY KEY (username, platform)
                )
            """)
            await self.db.execute("""
                INSERT OR REPLACE INTO monitored_accounts_new
                    (username, platform, channel_id, last_scraped_id, initial_scan_completed, created_at)
                SELECT username, COALESCE(platform, 'instagram'), channel_id,
                       COALESCE(last_scraped_id, ''), 0,
                       COALESCE(created_at, datetime('now', 'localtime'))
                FROM monitored_accounts
            """)
            await self.db.execute("DROP TABLE monitored_accounts")
            await self.db.execute("ALTER TABLE monitored_accounts_new RENAME TO monitored_accounts")
            await self.db.commit()
        except Exception as rebuild_err:
            try:
                await self.db.rollback()
            except Exception:
                pass
            logger.error(f"Monitored-account migration failed: {rebuild_err}", exc_info=True)
            raise

    async def _migrate_scraped_posts(self) -> None:
        """Upgrade post-only deduplication key without dropping history."""
        async with self.db.execute("PRAGMA table_info(scraped_posts)") as cursor:
            columns = [row async for row in cursor]
        if [row["name"] for row in columns if row["pk"]] == ["post_id", "platform"]:
            return
        try:
            await self.db.execute("""
                CREATE TABLE scraped_posts_new (
                    post_id TEXT NOT NULL,
                    username TEXT,
                    platform TEXT NOT NULL,
                    uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (post_id, platform)
                )
            """)
            await self.db.execute("""
                INSERT OR IGNORE INTO scraped_posts_new (post_id, username, platform, uploaded_at)
                SELECT post_id, username, COALESCE(NULLIF(platform, ''), 'unknown'), uploaded_at
                FROM scraped_posts
            """)
            await self.db.execute("DROP TABLE scraped_posts")
            await self.db.execute("ALTER TABLE scraped_posts_new RENAME TO scraped_posts")
            await self.db.commit()
        except Exception:
            try:
                await self.db.rollback()
            except Exception:
                pass
            raise

    async def close(self) -> None:
        """Tutup koneksi database SQLite."""
        if self._db:
            await self._db.close()
            self._db = None
            logger.info("[⚙️ SYSTEM] Database connection closed.")

    # ──────────────────────────────────────────────
    # Operasi Riwayat Postingan
    # ──────────────────────────────────────────────

    async def check_post_exists(self, post_id: str, platform: str) -> bool:
        """
        Cek apakah post_id dari platform tertentu sudah pernah di-scrape/dikirim ke Discord.
        """
        plat = platform.lower()
        async with self.db.execute(
            """
            SELECT 1 FROM scraped_posts WHERE post_id = ? AND platform = ?
            UNION
            SELECT 1 FROM downloaded_posts WHERE post_id = ? AND platform = ? AND status = 'done'
            LIMIT 1
            """,
            (post_id, plat, post_id, plat),
        ) as cursor:
            row = await cursor.fetchone()
            return row is not None

    async def is_post_scraped(self, post_id: str, platform: str = "") -> bool:
        """
        Cek apakah sebuah post_id sudah tercatat di database (sudah dikirim ke Discord).
        """
        if platform:
            return await self.check_post_exists(post_id, platform)
        async with self.db.execute(
            """
            SELECT 1 FROM scraped_posts WHERE post_id = ?
            UNION
            SELECT 1 FROM downloaded_posts WHERE post_id = ? AND status = 'done'
            LIMIT 1
            """,
            (post_id, post_id),
        ) as cursor:
            row = await cursor.fetchone()
            return row is not None

    async def check_posts_exist_bulk(
        self, post_ids: list[str], platform: str
    ) -> set[str]:
        """
        Cek keberadaan banyak post_id sekaligus dalam 1 query.
        """
        if not post_ids:
            return set()

        placeholders = ",".join("?" for _ in post_ids)
        query = f"""
            SELECT post_id FROM scraped_posts
            WHERE platform = ? AND post_id IN ({placeholders})
        """
        params = [platform.lower()] + post_ids

        async with self.db.execute(query, params) as cursor:
            rows = await cursor.fetchall()
            return {row["post_id"] for row in rows}

    async def mark_post_scraped(self, post_id: str, username: str, platform: str) -> None:
        """
        Catat post_id ke scraped_posts dan downloaded_posts setelah sukses dikirim ke Discord.
        """
        plat = platform.lower()
        uname = username.lower()
        if plat == "tiktok":
            profile_url_built = f"https://www.tiktok.com/@{uname}"
        elif plat == "twitter":
            profile_url_built = f"https://x.com/{uname}"
        else:
            profile_url_built = f"https://www.instagram.com/{uname}/"

        await self.db.execute(
            """
            INSERT OR IGNORE INTO scraped_posts (post_id, username, platform)
            VALUES (?, ?, ?)
            """,
            (post_id, uname, plat),
        )
        await self.db.execute(
            """
            INSERT OR IGNORE INTO downloaded_posts (post_id, platform, profile_url, media_count, status)
            VALUES (?, ?, ?, 1, 'done')
            """,
            (post_id, plat, profile_url_built),
        )
        await self.db.commit()

    async def mark_post_downloaded(
        self,
        post_id: str,
        platform: str,
        profile_url: str,
        media_count: int = 1,
        status: str = "done",
    ) -> None:
        """
        Catat postingan yang sudah berhasil diunduh dan dikirim ke Discord.
        Gunakan INSERT OR IGNORE untuk mencegah error duplikat.
        """
        username, _ = extract_username_and_platform(profile_url)
        if not username:
            username = profile_url.split("?")[0].rstrip("/").split("/")[-1].replace("@", "")

        await self.db.execute(
            """
            INSERT OR IGNORE INTO downloaded_posts
                (post_id, platform, profile_url, media_count, status)
            VALUES (?, ?, ?, ?, ?)
            """,
            (post_id, platform, profile_url, media_count, status),
        )
        await self.db.execute(
            """
            INSERT OR IGNORE INTO scraped_posts (post_id, username, platform)
            VALUES (?, ?, ?)
            """,
            (post_id, username, platform),
        )
        await self.db.commit()

    async def mark_post_failed(self, post_id: str, platform: str, profile_url: str) -> None:
        """Tandai postingan yang gagal diproses (untuk tracking error)."""
        await self.mark_post_downloaded(
            post_id, platform, profile_url, media_count=0, status="failed"
        )

    # ──────────────────────────────────────────────
    # Operasi Session / Resume State
    # ──────────────────────────────────────────────

    async def create_session(self, session_id: str, platform: str, profile_url: str) -> None:
        """Buat record sesi scraping baru."""
        await self.db.execute(
            """
            INSERT OR REPLACE INTO scrape_sessions
                (session_id, platform, profile_url, status)
            VALUES (?, ?, ?, 'running')
            """,
            (session_id, platform, profile_url),
        )
        await self.db.commit()

    async def update_session_progress(
        self,
        session_id: str,
        last_post_id: str,
        scraped_posts: int,
        total_posts: int = 0,
    ) -> None:
        """Update progress sesi scraping yang sedang berjalan."""
        await self.db.execute(
            """
            UPDATE scrape_sessions
            SET last_post_id = ?,
                scraped_posts = ?,
                total_posts = ?,
                updated_at = datetime('now', 'localtime')
            WHERE session_id = ?
            """,
            (last_post_id, scraped_posts, total_posts, session_id),
        )
        await self.db.commit()

    async def complete_session(self, session_id: str) -> None:
        """Tandai sesi scraping sebagai selesai."""
        if not self._db:
            logger.warning("Database connection is already closed — skip complete_session")
            return
        await self.db.execute(
            """
            UPDATE scrape_sessions
            SET status = 'completed', updated_at = datetime('now', 'localtime')
            WHERE session_id = ?
            """,
            (session_id,),
        )
        await self.db.commit()

    async def get_downloaded_count(self, profile_url: str, platform: str) -> int:
        """
        Ambil jumlah postingan yang sudah berhasil diunduh untuk profil tertentu.
        Berguna untuk menampilkan progress ke user.
        """
        async with self.db.execute(
            """
            SELECT COUNT(*) FROM downloaded_posts
            WHERE profile_url = ? AND platform = ? AND status = 'done'
            """,
            (profile_url, platform),
        ) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else 0

    # ──────────────────────────────────────────────
    # Routing Multi-Channel & Patrol (Monitored Accounts)
    # ──────────────────────────────────────────────

    async def add_monitored_account(
        self,
        username: str,
        platform: str,
        channel_id: int,
        last_scraped_id: Optional[str] = None,
    ) -> None:
        """Tambah atau update pemetaan username ke Discord channel ID dan platform."""
        last_id = last_scraped_id if last_scraped_id is not None else ""
        await self.db.execute(
            """
            INSERT INTO monitored_accounts (username, platform, channel_id, last_scraped_id)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(username, platform) DO UPDATE SET channel_id = excluded.channel_id
            """,
            (username.lower(), platform.lower(), channel_id, last_id),
        )
        await self.db.commit()

    async def get_monitored_account_channel(self, username: str, platform: str) -> Optional[int]:
        """Ambil Discord channel ID untuk username tertentu jika terdaftar."""
        async with self.db.execute(
            "SELECT channel_id FROM monitored_accounts WHERE username = ? AND platform = ?",
            (username.lower(), platform.lower()),
        ) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else None

    async def get_monitored_account(self, username: str, platform: str) -> Optional[dict]:
        """Ambil data lengkap akun yang dimonitor."""
        async with self.db.execute(
            "SELECT username, platform, channel_id, last_scraped_id, initial_scan_completed FROM monitored_accounts WHERE username = ? AND platform = ?",
            (username.lower(), platform.lower()),
        ) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None

    async def get_all_monitored_accounts(self) -> list[dict]:
        """Ambil seluruh daftar akun yang dipantau (terurut kronologis penambahan)."""
        async with self.db.execute(
            "SELECT username, platform, channel_id, last_scraped_id, initial_scan_completed FROM monitored_accounts ORDER BY created_at ASC"
        ) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

    async def get_patrol_ready_accounts(self) -> list[dict]:
        """
        Ambil akun yang siap dipatroli — hanya yang last_scraped_id TIDAK kosong.
        Akun dengan last_scraped_id kosong masih dalam proses historical dump awal,
        sehingga harus di-skip oleh patrol loop untuk menghindari race condition.
        Fix: pastikan query mensyaratkan (last_scraped_id IS NOT NULL AND last_scraped_id != '')
        agar initial_scan_completed = 1 tanpa last_scraped_id valid tidak memicu historical dump loop.
        """
        async with self.db.execute(
            "SELECT username, platform, channel_id, last_scraped_id "
            "FROM monitored_accounts WHERE (last_scraped_id IS NOT NULL AND last_scraped_id != '')"
        ) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

    async def update_last_scraped_id(self, username: str, platform: str, last_scraped_id: str) -> None:
        """Update postingan ID terakhir yang berhasil di-scrape/kirim."""
        # FIX: assert stripped by python -O; use explicit guard
        if self._db is None:
            raise RuntimeError("Database is not initialized. Call initialize() first.")
        await self.db.execute(
            "UPDATE monitored_accounts SET last_scraped_id = ? WHERE username = ? AND platform = ?",
            (last_scraped_id, username.lower(), platform.lower()),
        )
        await self.db.commit()

    async def delete_monitored_account(self, username: str, platform: str) -> None:
        """Hapus akun dari daftar pantau."""
        # FIX: assert stripped by python -O; use explicit guard
        if self._db is None:
            raise RuntimeError("Database is not initialized. Call initialize() first.")
        await self.db.execute(
            "DELETE FROM monitored_accounts WHERE username = ? AND platform = ?",
            (username.lower(), platform.lower()),
        )
        await self.db.commit()

    async def reset_account_history(self, username: str, platform: str) -> None:
        """Reset history scraping akun tertentu agar mendownload ulang dari awal."""
        # FIX: assert stripped by python -O; use explicit guard
        if self._db is None:
            raise RuntimeError("Database is not initialized. Call initialize() first.")
        await self.db.execute(
            "UPDATE monitored_accounts SET last_scraped_id = '', initial_scan_completed = 0 WHERE username = ? AND platform = ?",
            (username.lower(), platform.lower()),
        )
        await self.db.commit()

    async def mark_initial_scan_completed(self, username: str, platform: str) -> None:
        # FIX: assert stripped by python -O; use explicit guard
        if self._db is None:
            raise RuntimeError("Database is not initialized. Call initialize() first.")
        await self.db.execute(
            "UPDATE monitored_accounts SET initial_scan_completed = 1 WHERE username = ? AND platform = ?",
            (username.lower(), platform.lower()),
        )
        await self.db.commit()

    async def clear_all_data(self) -> None:
        """Kosongkan seluruh data history, sesi, dan akun terdaftar dari database."""
        # FIX: assert stripped by python -O; use explicit guard
        if self._db is None:
            raise RuntimeError("Database is not initialized. Call initialize() first.")
        await self.db.execute("DELETE FROM downloaded_posts")
        await self.db.execute("DELETE FROM scrape_sessions")
        await self.db.execute("DELETE FROM monitored_accounts")
        await self.db.execute("DELETE FROM scraped_posts")
        await self.db.commit()

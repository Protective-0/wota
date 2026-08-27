"""
core/utils.py
Fungsi helper terpusat untuk ekstraksi username, deteksi platform, manipulasi URL,
dan unified logging system dengan ANSI color support untuk terminal Linux/Docker.
"""

import logging
import re
import os
import sys
from datetime import datetime, timezone, timedelta
from typing import Optional, Any

# ──────────────────────────────────────────────
# ANSI Color Codes (Linux/Docker terminal support)
# ──────────────────────────────────────────────

# Detect if terminal supports color (Linux/Docker: yes; Windows without ANSI: no)
# FORCE_COLOR=0  — explicit opt-out for log collectors that set TERM=xterm even when
#                   output is piped to journald/file, which would print raw ANSI escape codes.
# FORCE_COLOR=1  — explicit opt-in (useful in CI/CD pipelines).
_force_color_env = os.getenv("FORCE_COLOR", "").lower()
_USE_COLOR = (
    _force_color_env not in ("0", "false", "no")  # explicit opt-out wins
    and (
        _force_color_env in ("1", "true", "yes")  # explicit opt-in
        or sys.stdout.isatty()                    # real terminal
        or os.getenv("TERM", "") not in ("", "dumb")  # terminal type hint
    )
)

# Reset
RESET  = "\033[0m"  if _USE_COLOR else ""
BOLD   = "\033[1m"  if _USE_COLOR else ""
DIM    = "\033[2m"  if _USE_COLOR else ""

# Foreground colors
BLACK   = "\033[30m" if _USE_COLOR else ""
RED     = "\033[31m" if _USE_COLOR else ""
GREEN   = "\033[32m" if _USE_COLOR else ""
YELLOW  = "\033[33m" if _USE_COLOR else ""
BLUE    = "\033[34m" if _USE_COLOR else ""
MAGENTA = "\033[35m" if _USE_COLOR else ""
CYAN    = "\033[36m" if _USE_COLOR else ""
WHITE   = "\033[37m" if _USE_COLOR else ""

# Bright variants
BRIGHT_RED     = "\033[91m" if _USE_COLOR else ""
BRIGHT_GREEN   = "\033[92m" if _USE_COLOR else ""
BRIGHT_YELLOW  = "\033[93m" if _USE_COLOR else ""
BRIGHT_BLUE    = "\033[94m" if _USE_COLOR else ""
BRIGHT_MAGENTA = "\033[95m" if _USE_COLOR else ""
BRIGHT_CYAN    = "\033[96m" if _USE_COLOR else ""
BRIGHT_WHITE   = "\033[97m" if _USE_COLOR else ""


# ──────────────────────────────────────────────
# Tag Constants (use in logger calls)
# ──────────────────────────────────────────────
TAG_PATROL  = "[🤖 PATROL]"
TAG_QUEUE   = "[📦 QUEUE ]"
TAG_CRAWL   = "[🔍 CRAWL ]"
TAG_DOWN    = "[📥 DOWN  ]"
TAG_COMPR   = "[🗜️  COMPR ]"
TAG_DISCORD = "[📤 DISCORD]"
TAG_SYSTEM  = "[⚙️  SYSTEM]"
TAG_SUCCESS = "[✅ SUCCESS]"
TAG_WARN    = "[⚠️  WARN  ]"
TAG_ERROR   = "[❌ ERROR  ]"


# ──────────────────────────────────────────────
# Colored Formatter
# ──────────────────────────────────────────────

# Map log level → (level_color, message_color)
_LEVEL_COLORS: dict[int, tuple[str, str]] = {
    logging.DEBUG:    (DIM + WHITE,          DIM + WHITE),
    logging.INFO:     (BRIGHT_CYAN,          WHITE),
    logging.WARNING:  (BRIGHT_YELLOW,        BRIGHT_YELLOW),
    logging.ERROR:    (BRIGHT_RED,           BRIGHT_RED),
    logging.CRITICAL: (BOLD + BRIGHT_RED,    BOLD + BRIGHT_RED),
}

# Tag substring → highlight color for the tag badge
_TAG_COLORS: dict[str, str] = {
    "PATROL":  BRIGHT_BLUE,
    "QUEUE":   MAGENTA,
    "CRAWL":   BRIGHT_CYAN,
    "DOWN":    GREEN,
    "COMPR":   YELLOW,
    "DISCORD": BRIGHT_MAGENTA,
    "SYSTEM":  CYAN,
    "SUCCESS": BRIGHT_GREEN,
    "WARN":    BRIGHT_YELLOW,
    "ERROR":   BRIGHT_RED,
}


class ColoredFormatter(logging.Formatter):
    """
    Custom formatter: colorizes timestamp, level, and tag badge for
    Linux/Docker terminal output.

    Format: YYYY-MM-DD HH:MM:SS | LEVEL   | [TAG] message
    """

    BASE_FORMAT = "%(asctime)s | %(levelname)-7s | %(message)s"
    DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

    def format(self, record: logging.LogRecord) -> str:  # noqa: A003
        level_color, msg_color = _LEVEL_COLORS.get(record.levelno, ("", ""))

        # Format timestamp in dim white
        self.datefmt = self.DATE_FORMAT
        base = super().format(record)

        if not _USE_COLOR:
            return base

        # Rebuild with colors applied to each segment
        # base → "2026-08-15 01:23:45 | INFO    | [TAG] message"
        parts = base.split(" | ", maxsplit=2)
        if len(parts) != 3:
            return base

        ts_part, lvl_part, msg_part = parts

        # Colorize timestamp
        colored_ts = f"{DIM}{WHITE}{ts_part}{RESET}"

        # Colorize level
        colored_lvl = f"{level_color}{lvl_part}{RESET}"

        # Colorize tag badge inside message (e.g., [🤖 PATROL])
        # FIX: `import re as _re` was inside this loop, triggering a module lookup
        # on every log line (Python caches it in sys.modules, but the dict lookup
        # + local variable assignment per call is wasteful). Hoisted to module-level
        # `import re` at the top of the file — use module-level `re` directly.
        colored_msg = msg_part
        for tag_key, tag_color in _TAG_COLORS.items():
            if tag_key in msg_part:
                # Highlight just the bracket badge, leave rest in msg_color
                colored_msg = re.sub(
                    r"(\[[\W\w]{1,3}" + tag_key + r"[\W\s]*\])",
                    lambda m: f"{tag_color}{BOLD}{m.group(0)}{RESET}{msg_color}",
                    msg_part,
                    count=1,
                )
                break

        colored_msg = f"{msg_color}{colored_msg}{RESET}"

        return f"{colored_ts} | {colored_lvl} | {colored_msg}"


def setup_logging(level: int = logging.INFO) -> None:
    """
    Configure root logging with ColoredFormatter.
    Call once from bot.py entrypoint before any module imports loggers.

    Suppresses noisy third-party loggers.
    """
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(ColoredFormatter(ColoredFormatter.BASE_FORMAT))

    root = logging.getLogger()
    root.setLevel(level)

    # Remove default handlers to prevent duplicate output
    root.handlers.clear()
    root.addHandler(handler)

    # Suppress noisy third-party loggers
    for noisy in ("httpx", "httpcore", "discord", "playwright", "aiosqlite", "asyncio"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


# ──────────────────────────────────────────────
# Regex pattern deteksi URL profil
# ──────────────────────────────────────────────

TIKTOK_PROFILE_REGEX = re.compile(
    r"https?://(?:www\.)?tiktok\.com/@[\w.-]+/?(?:\?[^/\s]*)?",
    re.IGNORECASE,
)

INSTAGRAM_PROFILE_REGEX = re.compile(
    r"https?://(?:www\.)?instagram\.com/(?!p/|reel/|explore/|stories/|accounts/)[\w.]+/?(?:\?[^/\s]*)?",
    re.IGNORECASE,
)

TWITTER_PROFILE_REGEX = re.compile(
    r"https?://(?:www\.)?(?:twitter|x)\.com/(?!home|explore|notifications|messages|i/)[\w]+/?(?:\?[^/\s]*)?",
    re.IGNORECASE,
)


def detect_platform(text: str) -> tuple[str, str]:
    """Deteksi platform dan ekstrak URL profil dari teks pesan."""
    urls = re.findall(r"https?://\S+", text)

    for url in urls:
        url = url.rstrip(".,)!?")
        if TIKTOK_PROFILE_REGEX.search(url):
            return "tiktok", url
        if INSTAGRAM_PROFILE_REGEX.search(url):
            return "instagram", url
        if TWITTER_PROFILE_REGEX.search(url):
            return "twitter", url

    return "", ""


def extract_username_and_platform(input_str: str) -> tuple[str, str]:
    """Ekstrak username clean dan platform dari input URL atau username biasa."""
    if not input_str or not input_str.strip():
        return "", ""

    input_str = input_str.strip()
    platform = ""  # Jangan default ke instagram jika input bukan URL
    username = input_str

    if "tiktok.com" in input_str:
        platform = "tiktok"
        match = re.search(r"tiktok\.com/@([^/?&#\s]+)", input_str)
        if match:
            username = match.group(1)
    elif "instagram.com" in input_str:
        platform = "instagram"
        match = re.search(r"instagram\.com/([^/?&#/\s]+)", input_str)
        if match:
            username = match.group(1)
    elif "twitter.com" in input_str or "x.com" in input_str:
        platform = "twitter"
        match = re.search(r"(?:x|twitter)\.com/([^/?&#/\s]+)", input_str)
        if match:
            username = match.group(1)

    if username.startswith("@"):
        username = username[1:]

    return username.lower().strip(), platform


def fmt_size(size_bytes: int) -> str:
    """Format byte size ke string human-readable (KB/MB)."""
    if size_bytes >= 1_048_576:
        return f"{size_bytes / 1_048_576:.1f}MB"
    return f"{size_bytes / 1024:.1f}KB"


def fmt_duration(seconds: float) -> str:
    """Format detik ke string human-readable (e.g. '2.3s' atau '1m 12s')."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    mins = int(seconds // 60)
    secs = seconds % 60
    return f"{mins}m {secs:.0f}s"


# ──────────────────────────────────────────────
# Datetime & Discord Timestamp Formatting (WIB)
# ──────────────────────────────────────────────

INDONESIAN_MONTHS = [
    "", "Januari", "Februari", "Maret", "April", "Mei", "Juni",
    "Juli", "Agustus", "September", "Oktober", "November", "Desember"
]
WIB_TZ = timezone(timedelta(hours=7))


def format_wib_date(raw_timestamp: Any) -> str:
    """
    Format raw timestamp (ISO string, unix int/float, atau datetime)
    menjadi format tanggal Bahasa Indonesia (WIB) dan Discord Dynamic Timestamp:
    
    **DD Bulan YYYY, HH:MM WIB**
    <t:{unix_timestamp}:f> (<t:{unix_timestamp}:R>)

    Fallback: Jika timestamp None/kosong/tidak valid, mengembalikan 'Baru Saja (Patrol)'.
    """
    if raw_timestamp is None:
        return "Baru Saja (Patrol)"

    if isinstance(raw_timestamp, str):
        val_str = raw_timestamp.strip()
        if not val_str or val_str.lower() in ("none", "null", ""):
            return "Baru Saja (Patrol)"

    dt_utc: Optional[datetime] = None

    # 1. datetime instance
    if isinstance(raw_timestamp, datetime):
        if raw_timestamp.tzinfo is None:
            dt_utc = raw_timestamp.replace(tzinfo=timezone.utc)
        else:
            dt_utc = raw_timestamp.astimezone(timezone.utc)

    # 2. int / float (Unix timestamp)
    elif isinstance(raw_timestamp, (int, float)):
        try:
            dt_utc = datetime.fromtimestamp(raw_timestamp, tz=timezone.utc)
        except Exception:
            return "Baru Saja (Patrol)"

    # 3. string parsing
    elif isinstance(raw_timestamp, str):
        val_str = raw_timestamp.strip()

        # Cek apakah string berupa unix timestamp numeric
        try:
            if val_str.replace(".", "", 1).isdigit():
                num = float(val_str)
                dt_utc = datetime.fromtimestamp(num, tz=timezone.utc)
        except Exception:
            pass

        # Cek ISO-8601 string
        if dt_utc is None:
            clean_str = val_str.replace("Z", "+00:00")
            try:
                dt_parsed = datetime.fromisoformat(clean_str)
                if dt_parsed.tzinfo is None:
                    dt_utc = dt_parsed.replace(tzinfo=timezone.utc)
                else:
                    dt_utc = dt_parsed.astimezone(timezone.utc)
            except Exception:
                pass

        # Cek standard datetime string formats
        if dt_utc is None:
            formats = [
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%d %H:%M",
                "%Y-%m-%d",
                "%Y%m%d",
                "%d-%m-%Y %H:%M:%S",
                "%d/%m/%Y %H:%M:%S",
            ]
            for fmt in formats:
                try:
                    dt_parsed = datetime.strptime(val_str.split(".")[0], fmt)
                    dt_utc = dt_parsed.replace(tzinfo=timezone.utc)
                    break
                except Exception:
                    pass

    if dt_utc is None:
        return "Baru Saja (Patrol)"

    # Konversi ke WIB (UTC+7)
    dt_wib = dt_utc.astimezone(WIB_TZ)
    unix_ts = int(dt_utc.timestamp())
    month_name = INDONESIAN_MONTHS[dt_wib.month]
    time_str = dt_wib.strftime("%H:%M")
    wib_str = f"{dt_wib.day:02d} {month_name} {dt_wib.year}, {time_str} WIB"

    return f"**{wib_str}**\n<t:{unix_ts}:f> (<t:{unix_ts}:R>)"

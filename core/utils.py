"""
core/utils.py
Fungsi helper terpusat untuk ekstraksi username, deteksi platform, dan manipulasi URL.
"""

import re

# Regex pattern deteksi URL profil
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
    platform = "instagram"  # default fallback
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

    return username.lower(), platform

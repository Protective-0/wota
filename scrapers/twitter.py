"""
scrapers/twitter.py
Scraper profil Twitter/X menggunakan Built-in Stealth Browser + yt-dlp.

Strategi:
- Playwright Stealth Browser untuk bypass Cloudflare/Twitter bot barriers
- Ekstraksi timeline profil (https://x.com/username) dengan browser fingerprint spoofing
- Smart stop-condition: berhenti saat menemukan tweet yang sudah ada di database SQLite
- Ekstraksi gambar resolusi tinggi (pbs.twimg.com/media/...?format=jpg&name=orig)
- Guard timeline iteration dengan safety deadline timer (MAX_SCROLL_SECONDS = 180.0)
"""

import asyncio
import json
import logging
import os
import random
import re
import time
from pathlib import Path
from typing import AsyncGenerator, Optional
from playwright.async_api import Browser, BrowserContext, Page, async_playwright

from .base import (
    BaseScraper,
    MediaType,
    PostMedia,
    USER_AGENT,
    DOCKER_CHROMIUM_FLAGS,
)
from core.utils import (
    TAG_CRAWL,
    TAG_SYSTEM,
    TAG_WARN,
    TAG_ERROR,
    TAG_SUCCESS,
)

logger = logging.getLogger(__name__)


class TwitterScraper(BaseScraper):
    """
    Scraper profil Twitter/X berbasis Built-in Stealth Browser.
    """

    PLATFORM = "twitter"

    def __init__(self, db_manager, session_dir: str, headed: bool = False):
        super().__init__(db_manager, session_dir)
        self.headed = headed
        self.session_dir = Path(session_dir)
        self.session_dir.mkdir(parents=True, exist_ok=True)
        self.netscape_cookie_path = self.session_dir / "twitter_cookies.txt"
        self._playwright = None
        self._browser: Optional[Browser] = None
        self._context: Optional[BrowserContext] = None

    def _extract_username(self, url: str) -> str:
        """Ekstrak username Twitter/X dari URL profil."""
        match = re.search(r"(?:x|twitter)\.com/([^/?&#/]+)/?", url)
        if match:
            username = match.group(1)
            if username not in {"home", "explore", "notifications", "messages", "i"}:
                return username
        return ""

    def _extract_tweet_id(self, url: str) -> str:
        """Ekstrak tweet ID numerik dari URL tweet."""
        match = re.search(r"/status/(\d+)", url)
        return match.group(1) if match else ""

    async def _init_browser(self) -> Page:
        """Inisialisasi browser stealth Playwright."""
        self._playwright = await async_playwright().start()
        self._browser, self._context = await BaseScraper.create_stealth_browser(
            self._playwright,
            headed=self.headed,
            viewport={"width": 1280, "height": 900},
            locale="id-ID,id;q=0.9,en-US;q=0.8,en;q=0.7",
        )
        await self.load_and_inject_cookies(self._context, "twitter")
        page = self._context.pages[0] if self._context.pages else await self._context.new_page()
        return page

    async def scrape_profile(
        self,
        profile_url: str,
        forced: bool = False,
    ) -> AsyncGenerator[PostMedia, None]:
        """
        Crawl semua tweet media dari profil Twitter/X via Stealth Browser.
        """
        username = self._extract_username(profile_url)
        if not username:
            logger.error(f"{TAG_ERROR} Username Twitter tidak valid: {profile_url}")
            return

        timeline_url = f"https://x.com/{username}"
        logger.info(f"{TAG_CRAWL} Mulai crawl timeline Twitter/X: {timeline_url}")

        # Siapkan cookies
        netscape_path = await self.export_session_cookies_for_ytdlp("twitter")
        if netscape_path:
            self.netscape_cookie_path = netscape_path

        collected_posts: list[PostMedia] = []

        try:
            page = await self._init_browser()
            logger.info(f"{TAG_CRAWL} Membuka Timeline Twitter target: {timeline_url}")

            await page.goto(timeline_url, wait_until="domcontentloaded", timeout=45000)
            await asyncio.sleep(2.5)

            try:
                await page.wait_for_selector('[data-testid="primaryColumn"]', state="attached", timeout=15000)
            except Exception:
                logger.warning("Primary column selector timeout — lanjut mencoba scroll...")

            if "i/flow/login" in page.url or "login" in page.url:
                logger.error(f"{TAG_ERROR} Sesi Cookie expired / Guest limit! Redirect ke login page: {page.url}")
                return

            seen_ids = set()
            no_new_count = 0
            # FIX: raised from 4 to 6 — value of 4 is too aggressive for reply-heavy
            # profiles where many consecutive scrolls may contain only text-only tweets.
            MAX_NO_NEW = 6
            scroll_round = 0
            # FIX: configurable via env MAX_TWITTER_SCROLLS — previously hardcoded to 35
            MAX_TWITTER_SCROLLS = int(os.getenv("MAX_TWITTER_SCROLLS", "100"))

            scroll_start = time.monotonic()
            MAX_SCROLL_SECONDS = 300.0

            while no_new_count < MAX_NO_NEW:
                if time.monotonic() - scroll_start > MAX_SCROLL_SECONDS:
                    logger.warning(f"{TAG_WARN} Batas waktu scroll Twitter {MAX_SCROLL_SECONDS}s tercapai untuk @{username} — stop.")
                    break

                scroll_round += 1

                tweets_snapshot = await page.evaluate(
                    """
                    (targetUsername) => {
                        const u = targetUsername.toLowerCase();
                        const articles = document.querySelectorAll('article[data-testid="tweet"], [data-testid="tweet"], article');
                        const results = [];

                        for (const article of articles) {
                            const links = article.querySelectorAll('a[href*="/status/"]');
                            let statusHref = null;
                            for (const l of links) {
                                const h = l.getAttribute('href') || l.href || '';
                                if (h.toLowerCase().includes('/' + u + '/status/')) {
                                    statusHref = h;
                                    break;
                                }
                            }
                            if (!statusHref) continue;

                            const timeEl = article.querySelector('time');
                            const timestamp = timeEl ? timeEl.getAttribute('datetime') : null;

                            const textEl = article.querySelector('[data-testid="tweetText"]');
                            const caption = textEl ? textEl.innerText : '';

                            const mediaImgs = Array.from(article.querySelectorAll('img[src*="pbs.twimg.com/media/"], [data-testid="tweetPhoto"] img'))
                                .map(img => img.src)
                                .filter(src => src && !src.includes('profile_images') && !src.includes('emoji'));

                            const hasVideo = !!article.querySelector('video, [data-testid="videoPlayer"], [data-testid="videoComponent"]');

                            if (mediaImgs.length === 0 && !hasVideo) {
                                continue;
                            }

                            results.push({
                                href: statusHref,
                                timestamp: timestamp,
                                caption: caption,
                                media_urls: mediaImgs,
                                is_video: hasVideo
                            });
                        }
                        return results;
                    }
                    """,
                    username,
                )

                new_found = 0
                stop_after_batch = False

                for item in (tweets_snapshot or []):
                    href = item.get("href", "")
                    tweet_id = self._extract_tweet_id(href)
                    if not tweet_id or tweet_id in seen_ids:
                        continue

                    seen_ids.add(tweet_id)
                    tweet_url = f"https://x.com/{username}/status/{tweet_id}"

                    if not forced and await self.db.check_post_exists(tweet_id, self.PLATFORM):
                        logger.info(
                            f"{TAG_CRAWL} Stop-condition: tweet {tweet_id} sudah di DB — selesaikan batch ini lalu berhenti."
                        )
                        stop_after_batch = True
                        continue

                    # Bersihkan media URLs ke resolusi original (format=jpg&name=orig)
                    cleaned_media_urls = []
                    for m_url in item.get("media_urls", []):
                        if "pbs.twimg.com/media/" in m_url:
                            clean_src = m_url.split("?")[0]
                            format_val = "jpg"
                            fmt_match = re.search(r"format=(\w+)", m_url)
                            if fmt_match and fmt_match.group(1).lower() in ("png", "gif"):
                                format_val = fmt_match.group(1).lower()
                            high_res = f"{clean_src}?format={format_val}&name=orig"
                            cleaned_media_urls.append(high_res)
                        else:
                            cleaned_media_urls.append(m_url)

                    is_video = item.get("is_video", False)
                    media_type = MediaType.VIDEO if is_video else MediaType.PHOTO

                    cookies_file_str = str(self.netscape_cookie_path) if self.netscape_cookie_path.exists() else None

                    collected_posts.append(
                        PostMedia(
                            post_id=tweet_id,
                            post_url=tweet_url,
                            profile_url=profile_url,
                            platform=self.PLATFORM,
                            media_type=media_type,
                            caption=item.get("caption", ""),
                            timestamp=item.get("timestamp"),
                            media_urls=cleaned_media_urls if cleaned_media_urls else [],
                            cookies_file=cookies_file_str,
                        )
                    )
                    new_found += 1

                if stop_after_batch:
                    break

                if new_found > 0:
                    logger.info(f"{TAG_CRAWL} Scroll [{scroll_round}]: +{new_found} tweet baru (total: {len(collected_posts)})")
                    no_new_count = 0
                else:
                    no_new_count += 1

                await page.evaluate("window.scrollBy(0, 1600)")
                await asyncio.sleep(random.uniform(2.5, 3.5))

                if scroll_round >= MAX_TWITTER_SCROLLS:
                    logger.info(f"{TAG_CRAWL} Batas {MAX_TWITTER_SCROLLS} scroll tercapai untuk @{username} — stop.")
                    break

        except Exception as e:
            logger.error(f"{TAG_ERROR} Gagal scrape timeline Twitter @{username}: {e}")
        finally:
            # FIX: page.close() moved into finally so it always runs before self.close().
            # Previously it was inside the try-block, which could raise 'Target closed'
            # if an exception caused self.close() (context.close()) to fire via finally
            # while page was still open. Closing page first avoids that race.
            try:
                await page.close()
            except Exception:
                pass
            await self.close()

        logger.info(f"{TAG_CRAWL} Selesai: {len(collected_posts)} tweet media terkumpul dari @{username}.")

        # Re-sort ke kronologis tertib (oldest-to-newest) untuk pengiriman teratur ke Discord
        collected_posts.sort(
            key=lambda p: (
                str(p.timestamp) if p.timestamp else "1970-01-01T00:00:00Z",
                int(p.post_id) if str(p.post_id).isdigit() else str(p.post_id),
            ),
            reverse=False,
        )

        for post in collected_posts:
            yield post

    async def close(self) -> None:
        """Cleanup browser resources."""
        try:
            if self._context:
                await self._context.close()
            if self._browser:
                await self._browser.close()
            if self._playwright:
                await self._playwright.stop()
        except Exception as e:
            logger.debug(f"TwitterScraper close error: {e}")
        finally:
            self._context = None
            self._browser = None
            self._playwright = None

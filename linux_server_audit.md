# Linux Server Audit — Discord Media Scraper Bot

**Scope:** Full repo review, Debian headless focus. Severity: 🔴 bug | 🟡 risk | 🔵 nit

---

## `bot.py`

`L691-702` vs `L1601-1622` — 🔴 **bug: duplicate `close()` definition.** Python silently uses the second definition (L1601). First definition (L691) — which calls `self.patrol_loop.cancel()` and `await self.db.close()` — is dead code and never runs. On shutdown: patrol_loop is NOT cancelled, DB connection is NOT closed, SQLite WAL may not flush final commit. **Fix:** merge both into a single `close()` that calls `self.patrol_loop.cancel()`, `await self.db.close()`, cancel+await `_worker_task`, close `downloader._browser`/_playwright, then `await super().close()`.

`L1647-1649` — 🟡 **risk: `_signal_handler` spawns `asyncio.create_task(bot.close())` inside a sync signal callback.** `create_task` requires the event loop to be running; on some asyncio shutdown sequences the loop is already draining and `create_task` raises `RuntimeError: no running event loop`. **Fix:** use `loop.call_soon_threadsafe(loop.create_task, bot.close())` or `asyncio.ensure_future`.

`L901` — 🟡 **risk: `patrol_loop` skips if `queue.is_busy` but never retries.** If a 30-minute historical scrape holds the lock, all patrol cycles during that window are silently dropped, not deferred. **Fix:** log a structured warning with estimated lock duration so operators can tune interval.

`L511-516` — 🔵 **nit: `task_done()` called on empty-queue items in `/reset_bot`.** `get_nowait()` removes the item; `task_done()` is only meaningful if `join()` is used. No `join()` exists. Remove `task_done()` calls to avoid misleading code.

`L1649` — 🟡 **risk: signal handler calls `bot.close()` but never awaits `bot.start()` cancellation.** If `bot.start()` is still running, closing the bot raises `ClientConnectionClosed` before the loop exits cleanly. **Fix:** signal handler should call `bot.close()` AND cancel `_run` task if it exists.

---

## `core/database.py`

`L104` — 🟡 **risk: single persistent `aiosqlite.Connection` shared across all async callers.** aiosqlite is thread-safe but not concurrent-safe for simultaneous `execute()` calls from multiple coroutines without serialisation. In WAL mode this usually works but can silently produce `database is locked` under high concurrency. **Fix:** use `aiosqlite.connect()` as async context manager per-operation, or add an `asyncio.Lock` around write operations.

`L108-110` — 🔵 **nit: `PRAGMA busy_timeout = 5000` issued after `PRAGMA journal_mode=WAL`.** Order matters — busy_timeout should be set first so it applies if WAL activation itself needs to acquire a lock. Reorder.

`L149-165` — 🟡 **risk: `_migrate_monitored_accounts` does a double `PRAGMA table_info` query** (L131 and L149). First result is discarded; second fetches same data. Wasteful and racey if another process modifies schema between calls (unlikely but bad hygiene). **Fix:** cache result of first query.

`L193` — 🟡 **risk: `async with self.db:` used as transaction manager inside `_rebuild_monitored_accounts_safely`.** aiosqlite's `__aenter__` issues `BEGIN`. In WAL mode with `autocommit=False` (default), aiosqlite may already be in an implicit transaction from prior executescript(). Double-BEGIN raises `OperationalError`. **Fix:** `await self.db.execute("BEGIN")` → `await self.db.commit()`/`rollback()` pattern explicitly, or call `await self.db.commit()` before entering the rebuild block.

---

## `core/queue_manager.py`

`L75-85` — 🟡 **risk: TOCTOU gap between `locked()` check (L75) and `wait_for(acquire)` (L82).** Two concurrent `try_acquire` calls both see `locked()=False`, both enter `wait_for`; one acquires, one gets `TimeoutError` at 100ms. This is the intended behaviour but the 100ms window creates measurable latency under load. **Fix:** use `Lock.acquire()` non-blocking directly: `acquired = self._lock.acquire_nowait()` (py 3.10+: `asyncio.Lock` doesn't have this). Alternatively use `asyncio.Semaphore(1)` which supports `acquire(blocking=False)`.

`L110-115` — 🔵 **nit: `release()` checks `locked()` before `release()`.** If `release()` is called when lock is not held it silently no-ops. This hides double-release bugs. **Fix:** Remove the guard and let `asyncio.Lock.release()` raise `RuntimeError` on improper call so the bug surfaces in logs.

---

## `scrapers/base.py`

`L143` — 🟡 **risk: `has_auth_configured` uses `__file__` path resolution to find cookie files.** On Docker with bind mounts or non-standard working directories, `os.path.abspath(__file__)` may resolve to container build path, not runtime mount. **Fix:** Use `Path(os.getcwd()) / "config" / "cookies"` or read from env `COOKIE_DIR` so runtime path is explicit.

`L164` — 🔵 **nit: `import platform` shadows the `platform: str` parameter used elsewhere in base class methods.** If any subclass ever calls `platform.system()` without the local import, it'll get the string, not the module. **Fix:** rename import to `import platform as _platform_module`.

---

## `scrapers/tiktok.py`

`L55` — 🔴 **bug: `async_playwright().start()` is called directly in `_init_browser`, creating a new Playwright instance every time `scrape_profile` is called.** If `scrape_profile` raises before `await self.close()`, the Playwright process leaks. On Debian, each leak = 1 orphan `node` process consuming ~50MB RAM. **Fix:** wrap in `try/except` and always call `self._playwright.stop()` in `finally`. Or instantiate Playwright as a context manager at the call site.

`L131` — 🟡 **risk: `page.goto(wait_until="domcontentloaded", timeout=60000)`.** 60s is very long for a headless server with potentially poor network. If TikTok serves an empty shell (captcha interstitial), DOMContentLoaded fires immediately but page is unusable — bot proceeds as if load succeeded. **Fix:** after `goto`, assert presence of profile-specific selector (e.g. `[data-e2e="user-post-item"]`) before scraping.

`L143-148` — 🟡 **risk: redirect guard checks `username.lower() not in page.url.lower()`.** If TikTok redirects to `https://www.tiktok.com/login?redirect_url=...`, URL contains the original username as a query param — guard passes, but page is the login wall. **Fix:** check for `/login` or `/captcha` in `page.url` explicitly.

`L235-321` — 🟡 **risk: scroll loop `max_scroll_attempts = 6` is hardcoded.**  For accounts with 200+ posts (paginated), 6 scrolls × ~15 posts/scroll = ~90 posts max; remaining posts silently missed. **Fix:** derive `max_scroll_attempts` from `expected_video_count` or make it configurable via env.

`L324` — 🔵 **nit: `window.scrollBy(0, 1200)` fixed pixel amount.** On TikTok, each post card is ~330px; 1200px ≈ 3.6 cards. For dense grids this under-scrolls, causing repeated empty passes. **Fix:** `window.scrollBy(0, window.innerHeight)` for a full viewport scroll.

---

## `scrapers/twitter.py`

`L171` — 🟡 **risk: `while no_new_count < MAX_NO_NEW` loop has no hard-time-limit guard for the outer while.** The `scroll_round >= 35` guard (L301) protects against infinite scroll but only counts rounds, not elapsed time. A slow Twitter page can make each round take 10s → 350s total per account. **Fix:** add a `time.monotonic()` start + max_seconds check.

`L179` — 🟡 **risk: `article` selector matches ALL articles in the DOM including ads and promotions.** Twitter injects promoted tweets that may have `/status/` links from accounts other than `targetUsername`. The `statusHref` filter (L188) filters by username — but only if the promoted tweet happens to have a different username in the URL, which is always true. Verify this filter actually catches promoted content on real scrapes.

`L241-246` — 🟡 **risk: stop-condition fires on first known tweet seen, breaking incremental patrol for accounts that retweet their own old content.** If tweet ID `X` is in DB and Twitter shows it in the timeline because it was RT'd, patrol stops — missing all newer tweets posted after the RT. **Fix:** don't break on first seen ID; instead collect all new IDs above the first known ID, allowing RT interleaving.

`L297` — 🔵 **nit: `window.scrollBy(0, 1600)` fixed pixels.** Same issue as TikTok scraper. Use `window.innerHeight`.

---

## `core/downloader.py`

`L222-223` — 🔴 **bug: `browser_cookies = []` (L206) is never populated when entering the TikTok carousel early-exit branch (L205-241), so `cookies_dict` at L223 is always an empty dict.** The TikTok carousel download at L236 then runs without cookies. **Fix:** remove the dead `browser_cookies` variable here entirely — the branch already calls `_extract_tiktok_carousel_urls` which uses `cookies_file` directly.

`L285` — 🟡 **risk: `loop = asyncio.get_running_loop()` assigned but never used** (L285, download_post). Dead variable. **Fix:** remove.

`L506-518` — 🟡 **risk: Twitter fallback creates new `browser.new_context()` and injects only `auth_token`+`ct0` cookies.** If `auth_token` or `ct0` is `None` (missing from both env and cookies_file), `auth_token.strip()` raises `AttributeError`. **Fix:** guard each cookie injection with `if auth_token:` and `if ct0:` separately.

`L512-518` — 🟡 **risk: Twitter cookies injected with hardcoded domain `.x.com` and `.twitter.com`.** If user's cookies_file has cookies for `twitter.com` (without dot prefix) Playwright will reject them silently (domain mismatch). **Fix:** normalize domain to always have leading dot.

`L1095` — 🟡 **risk: `before = set(self.temp_dir.rglob("*.*"))` scans entire temp_dir recursively before every yt-dlp call.** On a server with thousands of leftover temp files (e.g. after /reset_bot fails), this is an O(N) filesystem scan per download. **Fix:** use `self.temp_dir.glob(f"{post_id}_*")` to scope the scan.

`L1104-1105` — 🟡 **risk: `await asyncio.wait_for(proc.communicate(), timeout=180.0)` but proc is not killed on timeout.** When `TimeoutError` fires, the `yt-dlp` subprocess continues running as a zombie. **Fix:**
```python
except asyncio.TimeoutError:
    proc.kill()
    await proc.wait()
    ...
```

`L1704` — 🟡 **risk: `subprocess.run(ffmpeg, timeout=600)` is synchronous and blocks the event loop for up to 10 minutes.** Already wrapped in `run_in_executor` (L1561) — but executor threads share the process-level GIL; if multiple ffmpeg compressions run concurrently they serialize on GIL release points. Not a correctness bug but a throughput issue on multi-account setups. **Fix:** migrate to `asyncio.create_subprocess_exec` like yt-dlp (already done for consistency).

`L1706` — 🔵 **nit: `capture_output=True` with `text=True` — stderr decoded twice** (once implicitly by subprocess, once explicitly at L1712 via `result.stderr[-500:]`). This is fine but redundant. Use `errors="replace"` to handle non-UTF8 ffmpeg output on non-English locales.

---

## Security

**`bot.py:L759`** — `ALLOWED_USER_ID` guards all message handlers. `on_message` drops all non-owner messages before processing. ✅

**`base.py:L134-146`** — `has_auth_configured` reads from env and filesystem but never logs token values. ✅

**`downloader.py:L496-501`** — `auth_token`/`ct0` read from env with fallback to Netscape file. Tokens never logged. ✅

**`bot.py:L122`** — `DISCORD_BOT_TOKEN` read from env, never logged. ✅

---

## Top Priority Fixes (Ordered)

| # | File | Lines | Issue |
|---|------|-------|-------|
| 1 | `bot.py` | 691–702 vs 1601–1622 | Duplicate `close()` — DB never closed, patrol_loop never cancelled |
| 2 | `downloader.py` | 222–223 | Dead `browser_cookies=[]` — TikTok carousel runs cookieless |
| 3 | `downloader.py` | 1104–1105 | yt-dlp subprocess not killed on timeout → zombie process |
| 4 | `bot.py` | 1647–1649 | Signal handler `create_task` may fail after loop starts draining |
| 5 | `scrapers/tiktok.py` | 55 | Playwright leak on exception before `close()` |
| 6 | `downloader.py` | 506–518 | `auth_token.strip()` NoneType crash if token missing |

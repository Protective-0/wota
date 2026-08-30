# 🤖 WotaPedia — Discord Media Scraper Bot

Bot Discord otomatis untuk memantau (patrol), mengunduh, dan mengirim media dari profil **Instagram**, **TikTok**, dan **Twitter/X** langsung ke channel Discord target. Zero-login mode, multi-server, tanpa batasan user.

---

## ✨ Fitur Utama

### 📸 Multi-Platform Media Scraping
- **Instagram:** Feed foto/video, Carousel, Reels (tab eksklusif + yang dibagikan ke feed), dan Stories via mirror publik.
- **TikTok:** Video feed, Carousel foto slideshow, multi-fallback (API + Playwright Stealth DOM).
- **Twitter/X:** Tweet gambar, video, dan thread multi-media — resolusi full CDN original.

### 🔄 Otomatisasi Patrol & Anti-Duplikasi
- Background patrol loop memantau postingan baru secara berkala.
- Anti-duplikasi via SQLite (menggunakan shortcode Instagram Snowflake decoder untuk urutan kronologis akurat).
- Feed dan Reels diurutkan bersama dalam satu antrean berdasarkan timestamp asli, bukan dipisah per tipe.

### 🔐 Otorisasi Multi-Server & Multi-User (Zero Config)
- Bot dapat diundang ke banyak server sekaligus — tidak terbatas satu channel/server.
- Siapa saja yang memiliki role **Administrator**, **Manage Server**, atau **Manage Channels** di server bisa menggunakan slash commands.
- **Bot Owner otomatis terdeteksi** dari Discord Developer Portal — tidak perlu set manual `DISCORD_ALLOWED_USER_ID`.
- `DISCORD_CHANNEL_ID` opsional — bot otomatis merespons ke channel tempat perintah dikirim.

### 🗜️ Kompresi Video Pintar
- Otomatis kompres video > batas upload Discord menggunakan `ffmpeg` (target < 10 MB).
- Carousel multi-file dikirim sebagai attachment Discord sekaligus.

### ⚡ Stealth Anti-Bot
- Playwright headless dengan Linux platform spoofing, mobile UA, dan cookie isolation.
- Multi-tier fallback: API → Playwright Stealth DOM → yt-dlp.

---

## 📋 Prasyarat Sistem

| Kebutuhan | Keterangan |
| :--- | :--- |
| **Python 3.10+** | Runtime utama |
| **FFmpeg** | Kompresi video, harus terdaftar di `PATH` |
| **Playwright Chromium** | Rendering browser headless untuk scraping |
| **Git** | Clone repository |

---

## 🚀 Panduan Instalasi & Menjalankan Bot

### 1. Clone Repository
```bash
git clone https://github.com/Protective-0/wota.git
cd wota
```

### 2. Buat Virtual Environment & Install Dependencies
```bash
# Windows (PowerShell)
python -m venv .venv
.\.venv\Scripts\activate

# Linux / macOS
python3 -m venv .venv
source .venv/bin/activate

# Install Python packages
pip install -r requirements.txt

# Install browser Playwright
playwright install chromium
```

### 3. Konfigurasi File `.env`
Salin file template `.env.example` menjadi `.env`:
```bash
# Windows (PowerShell)
copy .env.example .env

# Linux / macOS
cp .env.example .env
```

Buka file `.env` dan isi konfigurasi berikut:

| Variable | Status | Keterangan |
| :--- | :--- | :--- |
| `DISCORD_BOT_TOKEN` | **Wajib** | Token bot dari [Discord Developer Portal](https://discord.com/developers/applications) |
| `DISCORD_CHANNEL_ID` | Opsional | Fallback channel ID jika tidak dispesifikasikan di `/add`. Jika kosong, bot merespons ke channel aktif. |
| `DISCORD_ALLOWED_USER_ID` | Opsional | User ID Superadmin (Bot Owner). Jika kosong, **otomatis terdeteksi** dari Developer Portal. |
| `INSTAGRAM_SESSION_ID` | Opsional | Cookie `sessionid` Instagram (meningkatkan batas scraping) |
| `TIKTOK_SESSION_ID` | Opsional | Cookie `sessionid` TikTok |
| `TWITTER_AUTH_TOKEN` + `TWITTER_CT0` | Opsional | Cookie autentikasi Twitter/X |

> **Catatan:** Bot dapat berjalan hanya dengan `DISCORD_BOT_TOKEN`. Semua konfigurasi lainnya bersifat opsional.

### 4. Jalankan Bot
```bash
python bot.py
```

---

## 🎮 Daftar Slash Commands

| Perintah | Deskripsi | Izin Minimum |
| :--- | :--- | :--- |
| `/add` | Daftarkan akun target (Instagram/TikTok/Twitter) ke channel tertentu | Administrator / Manager |
| `/insta` | Daftarkan akun Instagram ke channel tertentu | Administrator / Manager |
| `/tiktok` | Daftarkan akun TikTok ke channel tertentu | Administrator / Manager |
| `/x` | Daftarkan akun Twitter/X ke channel tertentu | Administrator / Manager |
| `/delete` | Hapus akun dari daftar monitoring | Administrator / Manager |
| `/reset_account` | Reset riwayat scrape agar akun diunduh ulang dari awal | Administrator / Manager |
| `/reset_bot` | Wipe total database, sessions, dan temp files | **Bot Owner Only** |
| `/force` | Paksa scan menyeluruh ke semua akun saat ini juga | Administrator / Manager |
| `/list` | Tampilkan semua akun yang sedang dimonitor | Administrator / Manager |
| `/sync` | Force sync slash commands ke Discord server | Administrator / Manager |
| `/pause` | Pause background patrol loop | Administrator / Manager |
| `/resume` | Resume background patrol loop | Administrator / Manager |

---

## 📂 Struktur Direktori

```text
├── bot.py                # Entrypoint bot Discord
├── requirements.txt      # Daftar pustaka Python
├── .env.example          # Template konfigurasi environment
├── .gitignore            # Filter file sensitif & temporer
├── README.md             # Dokumentasi proyek
├── core/
│   ├── database.py       # SQLite manager (aiosqlite)
│   ├── downloader.py     # Media downloader & ffmpeg compressor
│   ├── sender.py         # Discord embed & attachment sender
│   ├── queue.py          # Concurrency & job queue manager
│   └── utils.py          # Helper: detect_platform, extract_username, fmt_size
├── scrapers/
│   ├── base.py           # BaseScraper: Playwright stealth browser factory
│   ├── instagram.py      # Instagram: Feed API + Reels tab + Story mirror
│   ├── tiktok.py         # TikTok: API + Playwright Stealth DOM + yt-dlp
│   └── twitter.py        # Twitter/X: CDN original resolution downloader
├── config/               # Konfigurasi cookie static
├── sessions/             # Export sesi cookies runtime (gitignored)
├── icons/                # Aset icon platform untuk embed Discord
└── temp_media/           # Penyimpanan berkas unduhan sementara (gitignored)
```

---

## 🧠 Cara Kerja Instagram Scraping

```
[1] Feed API Pagination  → Mengambil semua postingan dari grid utama (foto, video, Reels yang dibagikan ke feed)
[2] Reels Tab Scan       → Playwright headless scroll pada /reels/ untuk Reels eksklusif (tidak masuk grid feed)
[3] Story Mirror         → Query mirror publik pihak ketiga (zero-login, tidak memerlukan sesi Instagram)
[4] Sort & Dedupe        → Shortcode Instagram di-decode jadi Snowflake timestamp → diurutkan kronologis
[5] Upload               → Feed + Reels dikirim bersama dalam satu antrean berurutan (bukan dipisah)
```

---

## 🔒 Keamanan

> [!CAUTION]
> **Jangan pernah membagikan atau meng-commit file `.env` atau isi folder `sessions/` ke publik.**
> File tersebut berisi token Discord dan cookie sesi login akun Anda yang dapat disalahgunakan.

> [!IMPORTANT]
> Bot berjalan dalam mode **Zero-Login** secara default. Cookie sesi Instagram/TikTok/Twitter **bersifat opsional** dan hanya digunakan untuk meningkatkan batas scraping pada akun publik dengan banyak postingan.

---

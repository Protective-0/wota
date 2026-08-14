# 🤖 Discord Media Scraper Bot

Bot Discord cerdas untuk melakukan pemantauan (patrol), pengunduhan media (foto, video, carousel/slideshow, reels), dan pengiriman otomatis dari profil **Instagram**, **TikTok**, dan **Twitter/X** langsung ke channel Discord target.

---

## ✨ Fitur Utama

- 🔄 **Otomatisasi Patrol & Polling:** Memantau postingan baru secara berkala tanpa upload berulang (anti-duplikasi via SQLite).
- 📸 **Multi-Platform Support:**
  - **Instagram:** Postingan tunggal, Carousel foto/video, dan Reels.
  - **TikTok:** Video feed dan Carousel foto slideshow.
  - **Twitter / X:** Tweet tunggal, multi-gambar, dan video media.
- 🗜️ **Kompresi Video Pintar:** Otomatis mengompres video besar menggunakan `ffmpeg` agar tetap di bawah batas upload Discord (misal 10 MB).
- 🍪 **Manajemen Sesi Fleksibel:** Mendukung session token via `.env` atau file cookie JSON (`config/cookies/`).
- ⚡ **Slash Commands Terintegrasi:** Kontrol penuh bot langsung dari Discord UI (`/add-account`, `/force`, `/status`, dll.).

---

## 📋 Prasyarat Sistem

1. **Python 3.10+**
2. **FFmpeg** terpasang di sistem dan terdaftar di `PATH`.
3. **Playwright Chromium** untuk rendering browser headless/headed.

---

## 🚀 Panduan Instalasi & Menjalankan Bot

### 1. Clone Repository
```bash
git clone https://github.com/USERNAME/REPO_NAME.git
cd REPO_NAME
```

### 2. Buat Virtual Environment & Install Dependencies
```bash
# Windows
python -m venv venv
venv\Scripts\activate

# Linux / MacOS
python3 -m venv venv
source venv/bin/activate

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

# Linux / MacOS
cp .env.example .env
```

Buka file `.env` dan lengkapi konfigurasi penting:
- `DISCORD_BOT_TOKEN`: Token bot dari [Discord Developer Portal](https://discord.com/developers/applications).
- `DISCORD_CHANNEL_ID`: Channel ID Discord target pengiriman media.
- `DISCORD_ALLOWED_USER_ID`: User ID Anda untuk hak akses perintah admin.
- `INSTAGRAM_SESSION_ID`: Cookie `sessionid` Instagram (opsional, jika scrape akun target).
- `TIKTOK_SESSION_ID`: Cookie `sessionid` TikTok (opsional).
- `TWITTER_AUTH_TOKEN` & `TWITTER_CT0`: Cookie autentikasi Twitter/X (opsional).

### 4. Jalankan Bot
```bash
python bot.py
```

---

## 🎮 Daftar Slash Commands

| Perintah | Deskripsi |
| :--- | :--- |
| `/force` | Memaksa bot melakukan scan/patroli ke semua akun target saat ini juga |
| `/status` | Menampilkan status patroli, kuota, dan ringkasan akun terdaftar |
| `/add-account` | Mendaftarkan akun target baru untuk dipantau otomatis |
| `/remove-account` | Menghapus akun target dari daftar pantauan |
| `/list-accounts` | Menampilkan seluruh daftar akun yang sedang dipantau |
| `/reset-bot` | Menghapus riwayat DB dan membersihkan cache (Admin Only) |

---

## 📂 Struktur Direktori

```text
├── bot.py                # Entrypoint bot Discord
├── requirements.txt      # Daftar pustaka Python
├── .env.example          # Template konfigurasi environment
├── .gitignore            # Filter file sensitif & temporer
├── README.md             # Dokumentasi proyek
├── core/                 # Modul logika bot (Database, Downloader, Sender, Queue)
├── scrapers/             # Handler scraping platform (Instagram, TikTok, Twitter)
├── config/               # Konfigurasi cookie static
├── sessions/             # Export sesi cookies runtime
├── icons/                # Aset icon platform untuk embed Discord
└── temp_media/           # Penyimpanan berkas unduhan sementara
```

---

## 🔒 Keamanan
> [!IMPORTANT]
> **Jangan pernah membagikan atau meng-commit file `.env` atau isi folder `sessions/` ke publik.** File tersebut berisi token Discord dan cookie sesi login akun Anda.

# ============================================================
# Dockerfile — Discord Media Scraper Bot
# ============================================================

# 1. Base image resmi Playwright Python (Chromium & dependensi Linux sudah terpasang)
FROM mcr.microsoft.com/playwright/python:v1.49.0-noble

# 2. Environment variables untuk runtime
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DEBIAN_FRONTEND=noninteractive \
    BROWSER_HEADED=false \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

# 3. Install FFmpeg untuk yt-dlp & gallery-dl
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# 4. Set direktori kerja aplikasi
WORKDIR /app

# 5. Layer caching dependensi Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 6. Copy seluruh source code project
COPY . .

# 7. Siapkan direktori penyimpanan data/temporer
RUN mkdir -p data temp_media sessions config

# 8. Jalankan bot Discord
CMD ["python", "bot.py"]

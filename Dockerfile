# ============================================================
# Dockerfile — Discord Media Scraper Bot
# ============================================================

# 1. Base image Python slim
FROM python:3.11-slim

# 2. Environment variables untuk runtime
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DEBIAN_FRONTEND=noninteractive \
    BROWSER_HEADED=false

# 3. Install dependensi sistem dasar & FFmpeg
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# 4. Set direktori kerja aplikasi
WORKDIR /app

# 5. Layer caching dependensi Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 6. Install browser Chromium & dependensi sistem Linux Playwright
RUN playwright install --with-deps chromium

# 7. Copy seluruh source code project
COPY . .

# 8. Siapkan direktori penyimpanan data/temporer
RUN mkdir -p temp_media sessions config

# 9. Jalankan bot Discord
CMD ["python", "bot.py"]

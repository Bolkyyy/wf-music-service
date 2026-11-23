FROM denoland/deno:bin AS deno
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=7860

# FFmpeg для быстрой упаковки/конвертации аудио + Deno (официальный JS-рантайм для yt-dlp EJS на YouTube)
COPY --from=deno /deno /usr/local/bin/deno

RUN apt-get update && apt-get install -y --no-install-recommends \
      ffmpeg \
      ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd -m -u 1000 user

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY --chown=user:user app.py .

USER user
ENV HOME=/home/user

EXPOSE 7860

CMD ["sh", "-c", "uvicorn app:app --host 0.0.0.0 --port ${PORT:-7860} --workers 1"]

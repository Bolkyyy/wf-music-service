# WF Music Service (yt-dlp + FFmpeg)

Небольшой бесплатный микросервис на Python (FastAPI + `yt-dlp` + `FFmpeg`), который ищет треки и конвертирует их в полный MP3 с тегами и обложкой для бота на Cloudflare Workers.

> **Почему отдельно от Worker?** Cloudflare Workers не умеет запускать нативные бинарники (`yt-dlp`, `FFmpeg`). Сервис берёт на себя поиск и конвертацию, а бот кэширует выданный Telegram `file_id` в базе D1 — повторные запросы того же трека отдаются мгновенно и вообще не нагружают этот сервис.

---

## Вариант 1 (бесплатно в облаке): Render.com

1. Загрузи репозиторий на GitHub.
2. Зайди на [render.com](https://render.com) → **New +** → **Blueprint** (или **Web Service** → Docker, Root Directory: `music-service`).
3. Render подхватит `render.yaml` (`plan: free`) и сам сгенерирует `MUSIC_API_SECRET`.
4. Скопируй адрес сервиса (например, `https://wf-music-service.onrender.com`) и значение `MUSIC_API_SECRET`.

*(Также этот же `Dockerfile` бесплатно работает на Hugging Face Spaces или Koyeb.)*

---

## Вариант 2 (бесплатно на своём ПК, Windows / Linux)

Нужны **Python 3.11+**, **FFmpeg** и бесплатный **Cloudflare Tunnel** (`cloudflared`):

```powershell
cd music-service
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt

$env:MUSIC_API_SECRET="придумай_длинную_строку"
uvicorn app:app --host 127.0.0.1 --port 8000
```

Во втором окне пробрось локальный порт наружу через бесплатный Cloudflare Tunnel:

```powershell
cloudflared tunnel --url http://127.0.0.1:8000
```

Туннель выдаст HTTPS-адрес вида `https://xxxx.trycloudflare.com`.

---

## Подключение к боту (Cloudflare Workers)

1. Примени новую миграцию D1 (таблица кэша `music_cache`):
   ```bash
   npm run db:migrate
   ```
2. В `wrangler.toml` в блоке `[vars]` раскомментируй и впиши URL сервиса:
   ```toml
   MUSIC_API_URL = "https://wf-music-service.onrender.com"
   ```
3. Задай общий секрет в Cloudflare:
   ```bash
   npx wrangler secret put MUSIC_API_SECRET
   ```
   *(вставь то же значение, что в сервисе)*
4. Задеплой бота:
   ```bash
   npm run deploy
   ```

---

## Если YouTube просит «Подтвердите, что вы не бот»

На некоторых облачных IP YouTube просит cookies. Экспортируй `cookies.txt` из браузера (Netscape format), закодируй в base64 и добавь переменную окружения `YT_COOKIES_B64` в настройки сервиса — `app.py` подхватит её автоматически.

## Переменные окружения

| Переменная | По умолчанию | Описание |
|---|---|---|
| `MUSIC_API_SECRET` | *(обязательна)* | Bearer-токен для авторизации запросов от бота |
| `MAX_DURATION_SEC` | `1200` (20 мин) | Максимальная длительность трека |
| `MAX_FILE_BYTES` | `47185920` (45 МБ) | Максимальный размер готового MP3 |
| `MAX_CONCURRENT` | `2` | Лимит одновременных загрузок (защита от OOM) |
| `YT_COOKIES_B64` | — | Опциональные cookies YouTube в base64 |
| `YT_COOKIES_FILE` | — | Опциональный путь к `cookies.txt` |

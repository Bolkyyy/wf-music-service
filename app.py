"""
Микросервис поиска и скачивания полных аудиотреков через yt-dlp + FFmpeg для TG Chat Controller.
Поддерживает:
- Быстрый поиск (SoundCloud без bot-check на облачных IP или YouTube при наличии cookies)
- Асинхронные задачи (/jobs/download, /jobs/{id}, /jobs/{id}/file) с реальным прогрессом (%) и ETA
- Быстрое копирование потока FFmpeg (-c:a copy для MP3/M4A) и параллельную загрузку HLS-фрагментов
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import ipaddress
import json
import os
import re
import secrets
import shutil
import socket
import subprocess
import tempfile
import threading
import time
import urllib.request
from collections import OrderedDict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlparse

import yt_dlp
from fastapi import Depends, FastAPI, Header, HTTPException
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel, Field

APP_VERSION = "2.0.0"

MUSIC_API_SECRET = os.getenv("MUSIC_API_SECRET", "").strip()
MAX_DURATION_SEC = int(os.getenv("MAX_DURATION_SEC", "1200"))  # 20 минут
MAX_FILE_BYTES = int(os.getenv("MAX_FILE_BYTES", str(45 * 1024 * 1024)))  # 45 МБ (лимит Telegram — 50 МБ)
MAX_CONCURRENT = max(1, int(os.getenv("MAX_CONCURRENT", "2")))
FFMPEG_BIN = os.getenv("FFMPEG_BIN", "ffmpeg")
JOB_TTL_SEC = 600  # Храним готовый файл задачи в памяти 10 минут
MAX_CATALOG_ITEMS = 2000

VIDEO_ID_RE = re.compile(r"^[A-Za-z0-9_-]{11}$")
SC_ID_RE = re.compile(r"^\d{4,20}$")
JOB_ID_RE = re.compile(r"^[a-f0-9]{12}$")

CLUTTER_RE = re.compile(
    r"\s*[\(\[]\s*(?:"
    r"official\s+(?:music\s+)?(?:video|audio|lyric\s+video|visualizer|mv)"
    r"|lyric(?:s)?\s*(?:video)?|audio|hd|4k|hq|mv|премьера(?:\s+клипа|\s+трека)?|клип|официальн\w+\s+\w+"
    r")\s*[\)\]]\s*",
    re.IGNORECASE,
)
SEPARATOR_RE = re.compile(r"\s+(?:-|—|–)\s+")
CHANNEL_SUFFIX_RE = re.compile(r"\s*(?:-\s*Topic|VEVO|Official)$", re.IGNORECASE)
PENALTY_WORDS = ("remix", "slowed", "sped up", "nightcore", "8d", "karaoke", "instrumental", "cover", "live", "mashup")

_semaphore = asyncio.Semaphore(MAX_CONCURRENT)
_cookie_path_cache: str | None = None
_yt_blocked_until: float = 0.0

# Кэш метаданных SoundCloud по числовому ID (чтобы callback_data Telegram укладывался в 64 байта)
_sc_catalog: OrderedDict[str, dict[str, Any]] = OrderedDict()
_catalog_lock = threading.Lock()


@dataclass
class DownloadJob:
    id: str
    status: str = "queued"  # queued | probing | downloading | converting | ready | error
    percent: int = 5
    downloaded: int = 0
    total: int = 0
    eta: int | None = 8
    stage: str = "В очереди…"
    title: str = ""
    performer: str = ""
    duration: int = 0
    ext: str = "mp3"
    content_type: str = "audio/mpeg"
    track_key: str = ""
    source_url: str = ""
    thumb_url: str = ""
    error: str | None = None
    error_status: int = 0
    data: bytes | None = field(default=None, repr=False)
    created_at: float = field(default_factory=time.monotonic)
    updated_at: float = field(default_factory=time.monotonic)

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "status": self.status,
            "percent": self.percent,
            "downloaded": self.downloaded,
            "total": self.total,
            "eta": self.eta,
            "stage": self.stage,
            "title": self.title,
            "performer": self.performer,
            "duration": self.duration,
            "ext": self.ext,
            "trackKey": self.track_key,
            "sourceUrl": self.source_url,
            "thumbUrl": self.thumb_url,
            "error": self.error,
            "errorStatus": self.error_status,
        }


_jobs: dict[str, DownloadJob] = {}
_jobs_lock = threading.Lock()

app = FastAPI(title="WF Music Service", version=APP_VERSION, docs_url=None, redoc_url=None)


# ---------- авторизация ----------


def require_auth(
    x_music_secret: str | None = Header(default=None, alias="X-Music-Secret"),
    authorization: str | None = Header(default=None),
) -> None:
    if not MUSIC_API_SECRET:
        raise HTTPException(status_code=503, detail="MUSIC_API_SECRET не задан на сервере")
    token = (x_music_secret or "").strip()
    if not token and authorization and authorization.startswith("Bearer "):
        token = authorization[7:].strip()
    if not token:
        raise HTTPException(status_code=401, detail="Нет токена авторизации")
    if not hmac.compare_digest(token, MUSIC_API_SECRET):
        raise HTTPException(status_code=403, detail="Неверный токен авторизации")


# ---------- кэш каталога и очистка задач ----------


def _remember_sc_track(track_id: str, item: dict[str, Any]) -> None:
    with _catalog_lock:
        _sc_catalog[track_id] = item
        _sc_catalog.move_to_end(track_id)
        while len(_sc_catalog) > MAX_CATALOG_ITEMS:
            _sc_catalog.popitem(last=False)


def _lookup_sc_track(track_id: str) -> dict[str, Any] | None:
    with _catalog_lock:
        return _sc_catalog.get(track_id)


def _prune_old_jobs() -> None:
    now = time.monotonic()
    with _jobs_lock:
        stale = [jid for jid, j in _jobs.items() if now - j.updated_at > JOB_TTL_SEC]
        for jid in stale:
            _jobs.pop(jid, None)


# ---------- валидация и разбор метаданных ----------


def resolve_cookie_file() -> str | None:
    """Поддержка cookies.txt через YT_COOKIES_FILE или YT_COOKIES_B64."""
    global _cookie_path_cache
    direct = os.getenv("YT_COOKIES_FILE", "").strip()
    if direct and Path(direct).is_file():
        return direct
    b64 = os.getenv("YT_COOKIES_B64", "").strip()
    if not b64:
        return None
    if _cookie_path_cache and Path(_cookie_path_cache).is_file():
        return _cookie_path_cache
    try:
        raw = base64.b64decode(b64)
        target = Path(tempfile.gettempdir()) / "wf_music_cookies.txt"
        target.write_bytes(raw)
        _cookie_path_cache = str(target)
        return _cookie_path_cache
    except Exception:
        return None


def validate_public_url(url: str) -> str:
    """Проверяет, что ссылка публичная (http/https) и не ведёт на локальную сеть (защита от SSRF)."""
    parsed = urlparse(url.strip())
    if parsed.scheme not in ("http", "https") or not parsed.hostname:
        raise HTTPException(status_code=400, detail="Некорректная ссылка")

    host = parsed.hostname.lower().rstrip(".")
    if host in ("localhost",) or host.endswith(".local") or host.endswith(".internal"):
        raise HTTPException(status_code=400, detail="Локальные адреса запрещены")

    try:
        ip = ipaddress.ip_address(host)
        if not ip.is_global:
            raise HTTPException(status_code=400, detail="Локальные IP-адреса запрещены")
    except ValueError:
        try:
            infos = socket.getaddrinfo(host, None)
            for info in infos:
                addr = ipaddress.ip_address(info[4][0])
                if not addr.is_global:
                    raise HTTPException(status_code=400, detail="Ссылка ведёт на внутренний адрес")
        except HTTPException:
            raise
        except OSError:
            pass

    return url.strip()


def clean_title_noise(text: str) -> str:
    cleaned = CLUTTER_RE.sub(" ", text).strip()
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" -—–|")
    return cleaned or text.strip()


def split_artist_title(
    raw_title: str,
    uploader: str | None = None,
    artist_meta: str | None = None,
    track_meta: str | None = None,
) -> tuple[str, str]:
    """Определяет (performer, title) для тегов аудио и плеера Telegram."""
    if artist_meta and track_meta:
        return artist_meta.strip()[:64], clean_title_noise(track_meta)[:64]

    cleaned = clean_title_noise(raw_title)
    parts = SEPARATOR_RE.split(cleaned, maxsplit=1)
    if len(parts) == 2 and parts[0].strip() and parts[1].strip():
        return parts[0].strip()[:64], parts[1].strip()[:64]

    performer = CHANNEL_SUFFIX_RE.sub("", (uploader or "")).strip() or "Неизвестный исполнитель"
    return performer[:64], (cleaned or "Без названия")[:64]


def track_key_for(info: dict[str, Any], fallback_url: str) -> str:
    extractor = str(info.get("extractor_key") or info.get("ie_key") or "").lower()
    vid = str(info.get("id") or "")
    if "youtube" in extractor and VIDEO_ID_RE.match(vid):
        return f"yt:{vid}"
    if "soundcloud" in extractor and SC_ID_RE.match(vid):
        return f"sc:{vid}"
    canonical = str(info.get("webpage_url") or fallback_url)
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:24]
    return f"url:{digest}"


def best_thumbnail_url(info: dict[str, Any]) -> str:
    vid = str(info.get("id") or "")
    extractor = str(info.get("extractor_key") or info.get("ie_key") or "").lower()
    if "youtube" in extractor and VIDEO_ID_RE.match(vid):
        return f"https://i.ytimg.com/vi/{vid}/hqdefault.jpg"
    thumb = info.get("thumbnail")
    if isinstance(thumb, str) and thumb.startswith(("http://", "https://")):
        return thumb
    thumbs = info.get("thumbnails")
    if isinstance(thumbs, list):
        for t in reversed(thumbs):
            u = t.get("url") if isinstance(t, dict) else None
            if isinstance(u, str) and u.startswith(("http://", "https://")):
                return u
    return ""


def base_ydl_opts(use_yt_alt_clients: bool = False) -> dict[str, Any]:
    opts: dict[str, Any] = {
        "quiet": True,
        "no_warnings": True,
        "noplaylist": True,
        "socket_timeout": 15,
        "retries": 2,
    }
    cookiefile = resolve_cookie_file()
    if cookiefile:
        opts["cookiefile"] = cookiefile
    elif use_yt_alt_clients:
        opts["extractor_args"] = {"youtube": {"player_client": ["android_vr", "tv_downgraded", "ios", "web"]}}
    return opts


def _is_preview_only(info: dict[str, Any]) -> bool:
    """Отсекает 30-секундные SoundCloud Go+ превью, чтобы отдавать только полные треки."""
    dur = info.get("duration")
    if isinstance(dur, (int, float)) and 0 < dur <= 35:
        return True
    formats = info.get("formats")
    if isinstance(formats, list) and formats:
        non_preview = [
            f for f in formats
            if isinstance(f, dict)
            and "preview" not in str(f.get("format_id") or "").lower()
            and "/preview/" not in str(f.get("url") or "").lower()
        ]
        if not non_preview:
            return True
    return False


def _penalty_score(text: str, query_lower: str) -> int:
    low = text.lower()
    return sum(1 for w in PENALTY_WORDS if w in low and w not in query_lower)


# ---------- поиск ----------


def _search_soundcloud_sync(query: str, limit: int) -> list[dict[str, Any]]:
    """Быстрый поиск полных треков на SoundCloud (~1–2 сек, без bot-check на облачных IP)."""
    fetch_count = min(16, max(limit * 3, 10))
    opts = {
        **base_ydl_opts(),
        "noplaylist": False,
        "extract_flat": "in_playlist",
        "skip_download": True,
        "ignoreerrors": True,
    }
    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            res = ydl.extract_info(f"scsearch{fetch_count}:{query}", download=False)
    except Exception:
        return []

    entries = res.get("entries") if isinstance(res, dict) else []
    q_lower = query.lower()
    scored: list[tuple[int, dict[str, Any]]] = []
    seen_ids: set[str] = set()

    for idx, entry in enumerate(entries or []):
        if not isinstance(entry, dict):
            continue
        tid = str(entry.get("id") or "").strip()
        url = str(entry.get("url") or entry.get("webpage_url") or "").strip()
        if not SC_ID_RE.match(tid) or tid in seen_ids or not url.startswith(("http://", "https://")):
            continue
        dur = entry.get("duration")
        duration = int(dur) if isinstance(dur, (int, float)) and dur > 0 else 0
        # Отсекаем 30-секундные Go+ превью и слишком длинные сеты (>20 мин)
        if duration <= 45 or duration > MAX_DURATION_SEC:
            continue
        raw_title = str(entry.get("title") or "").strip()
        if not raw_title:
            continue
        uploader = str(entry.get("uploader") or entry.get("channel") or "").strip()
        performer, title = split_artist_title(raw_title, uploader)
        thumb = best_thumbnail_url(entry)
        seen_ids.add(tid)

        item = {
            "id": f"sc:{tid}",
            "source": "sc",
            "videoId": tid,
            "title": title,
            "performer": performer,
            "duration": duration,
            "url": url,
            "thumbnail": thumb,
        }
        _remember_sc_track(tid, item)
        penalty = _penalty_score(f"{raw_title} {url}", q_lower)
        scored.append((penalty * 100 + idx, item))

    scored.sort(key=lambda x: x[0])
    return [item for _, item in scored[:limit]]


def _search_youtube_sync(query: str, limit: int) -> list[dict[str, Any]]:
    fetch_count = min(15, max(limit + 3, 8))
    opts = {
        **base_ydl_opts(),
        "noplaylist": False,
        "extract_flat": "in_playlist",
        "skip_download": True,
        "ignoreerrors": True,
    }
    with yt_dlp.YoutubeDL(opts) as ydl:
        res = ydl.extract_info(f"ytsearch{fetch_count}:{query}", download=False)

    entries = res.get("entries") if isinstance(res, dict) else []
    q_lower = query.lower()
    scored: list[tuple[int, dict[str, Any]]] = []
    seen_ids: set[str] = set()

    for idx, entry in enumerate(entries or []):
        if not isinstance(entry, dict):
            continue
        vid = str(entry.get("id") or "")
        if not VIDEO_ID_RE.match(vid) or vid in seen_ids:
            continue
        if entry.get("live_status") == "is_live" or entry.get("is_live"):
            continue
        dur = entry.get("duration")
        duration = int(dur) if isinstance(dur, (int, float)) and dur > 0 else 0
        if duration > MAX_DURATION_SEC:
            continue
        raw_title = str(entry.get("title") or "").strip()
        if not raw_title:
            continue
        uploader = str(entry.get("channel") or entry.get("uploader") or "").strip()
        performer, title = split_artist_title(raw_title, uploader)
        seen_ids.add(vid)
        item = {
            "id": f"yt:{vid}",
            "source": "yt",
            "videoId": vid,
            "title": title,
            "performer": performer,
            "duration": duration,
            "url": f"https://www.youtube.com/watch?v={vid}",
            "thumbnail": f"https://i.ytimg.com/vi/{vid}/hqdefault.jpg",
        }
        penalty = _penalty_score(raw_title, q_lower)
        scored.append((penalty * 100 + idx, item))

    scored.sort(key=lambda x: x[0])
    return [item for _, item in scored[:limit]]


def _search_sync(query: str, limit: int) -> list[dict[str, Any]]:
    # Если заданы YouTube cookies — ищем сразу на YouTube; иначе на облачном IP сразу берём полные треки с SoundCloud
    if resolve_cookie_file():
        yt = _search_youtube_sync(query, limit)
        if yt:
            return yt
        return _search_soundcloud_sync(query, limit)

    sc = _search_soundcloud_sync(query, limit)
    if sc:
        return sc
    return _search_youtube_sync(query, limit)


def _find_soundcloud_full_url(query: str, exclude_url: str = "") -> dict[str, Any] | None:
    """Ищет полную (не Go+ 30с превью) версию трека на SoundCloud через yt-dlp."""
    flat_opts = {
        **base_ydl_opts(),
        "noplaylist": False,
        "extract_flat": "in_playlist",
        "skip_download": True,
        "ignoreerrors": True,
    }
    try:
        with yt_dlp.YoutubeDL(flat_opts) as ydl:
            res = ydl.extract_info(f"scsearch8:{query}", download=False)
    except Exception:
        return None

    entries = res.get("entries") if isinstance(res, dict) else []
    q_lower = query.lower()
    candidates: list[tuple[int, str]] = []
    for idx, item in enumerate(entries or []):
        if not isinstance(item, dict):
            continue
        url = str(item.get("url") or item.get("webpage_url") or "").strip()
        if not url.startswith(("http://", "https://")) or url == exclude_url:
            continue
        flat_dur = item.get("duration")
        if isinstance(flat_dur, (int, float)) and (flat_dur <= 45 or flat_dur > MAX_DURATION_SEC):
            continue
        penalty = _penalty_score(f"{item.get('title') or ''} {url}", q_lower)
        candidates.append((penalty * 100 + idx, url))

    candidates.sort(key=lambda x: x[0])
    probe_opts = {**base_ydl_opts(), "skip_download": True, "ignoreerrors": True}
    for _, url in candidates[:4]:
        try:
            with yt_dlp.YoutubeDL(probe_opts) as ydl:
                full = ydl.extract_info(url, download=False)
        except Exception:
            continue
        if not isinstance(full, dict):
            continue
        dur = full.get("duration")
        if not isinstance(dur, (int, float)) or dur < 50 or dur > MAX_DURATION_SEC:
            continue
        if _is_preview_only(full):
            continue
        full.setdefault("webpage_url", url)
        return full
    return None


def _youtube_oembed(url: str) -> tuple[str, str] | None:
    """Быстрый fallback для названия/автора YouTube (oEmbed не блокируется на облачных IP)."""
    try:
        oembed_url = f"https://www.youtube.com/oembed?url={quote(url, safe='')}&format=json"
        req = urllib.request.Request(oembed_url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read().decode("utf-8", errors="replace"))
        raw_title = str(data.get("title") or "").strip()
        author = str(data.get("author_name") or "").strip()
        if raw_title:
            return split_artist_title(raw_title, author)
    except Exception:
        return None
    return None


# ---------- FFmpeg обработка ----------


def _process_audio(
    src_audio: Path,
    tmp_dir: Path,
    title: str,
    performer: str,
) -> tuple[Path, str, str]:
    """
    Быстрая упаковка аудио через FFmpeg с тегами для Telegram sendAudio:
    - Если исходник .mp3 -> -c:a copy (~0.2 сек, без потери качества)
    - Если исходник .m4a/.aac -> -c:a copy в .m4a (~0.2 сек, без перекодирования!)
    - Иначе (.opus/.webm/.ogg) -> быстрый нативный AAC 128k в .m4a (~3-5 сек вместо 20 сек libmp3lame!)
    Возвращает (path, ext, content_type).
    """
    suf = src_audio.suffix.lower()
    if suf == ".mp3":
        dst = tmp_dir / "track.mp3"
        cmd = [
            FFMPEG_BIN, "-y", "-hide_banner", "-loglevel", "error",
            "-i", str(src_audio), "-vn", "-c:a", "copy",
            "-id3v2_version", "3",
            "-metadata", f"title={title}",
            "-metadata", f"artist={performer}",
            str(dst),
        ]
        ext, ctype = "mp3", "audio/mpeg"
    elif suf in (".m4a", ".aac", ".mp4"):
        dst = tmp_dir / "track.m4a"
        cmd = [
            FFMPEG_BIN, "-y", "-hide_banner", "-loglevel", "error",
            "-i", str(src_audio), "-vn", "-c:a", "copy",
            "-movflags", "+faststart",
            "-metadata", f"title={title}",
            "-metadata", f"artist={performer}",
            str(dst),
        ]
        ext, ctype = "m4a", "audio/mp4"
    else:
        dst = tmp_dir / "track.m4a"
        cmd = [
            FFMPEG_BIN, "-y", "-hide_banner", "-loglevel", "error",
            "-i", str(src_audio), "-vn",
            "-c:a", "aac", "-b:a", "128k", "-ar", "44100",
            "-movflags", "+faststart",
            "-metadata", f"title={title}",
            "-metadata", f"artist={performer}",
            str(dst),
        ]
        ext, ctype = "m4a", "audio/mp4"

    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    if proc.returncode != 0 or not dst.is_file():
        # Запасной вариант: MP3 128k
        dst_fb = tmp_dir / "track_fb.mp3"
        fb_cmd = [
            FFMPEG_BIN, "-y", "-hide_banner", "-loglevel", "error",
            "-i", str(src_audio), "-vn",
            "-c:a", "libmp3lame", "-b:a", "128k", "-compression_level", "9", "-ar", "44100",
            "-id3v2_version", "3",
            "-metadata", f"title={title}",
            "-metadata", f"artist={performer}",
            str(dst_fb),
        ]
        proc2 = subprocess.run(fb_cmd, capture_output=True, text=True, timeout=90)
        if proc2.returncode != 0 or not dst_fb.is_file():
            raise RuntimeError(f"FFmpeg error: {(proc2.stderr or proc.stderr).strip()[:200]}")
        return dst_fb, "mp3", "audio/mpeg"
    return dst, ext, ctype


# ---------- основная загрузка с хуками прогресса ----------


def _download_sync(
    target_url: str,
    title_hint: str | None = None,
    performer_hint: str | None = None,
    canonical_key_hint: str | None = None,
    job: DownloadJob | None = None,
) -> tuple[bytes, dict[str, str], str]:
    global _yt_blocked_until

    def update_job(**kwargs: Any) -> None:
        if job is None:
            return
        with _jobs_lock:
            for k, v in kwargs.items():
                setattr(job, k, v)
            job.updated_at = time.monotonic()

    if not shutil.which(FFMPEG_BIN):
        raise HTTPException(status_code=500, detail="FFmpeg не найден на сервере")

    update_job(status="probing", percent=15, stage="Получаю аудиопоток…", eta=6)

    with tempfile.TemporaryDirectory(prefix="wf_music_") as tmp:
        tmp_dir = Path(tmp)
        canonical_key = canonical_key_hint
        yt_match = re.search(r"[?&]v=([A-Za-z0-9_-]{11})", target_url)
        if not canonical_key and yt_match:
            canonical_key = f"yt:{yt_match.group(1)}"

        # 1. Получаем метаданные до скачивания
        info: dict[str, Any] | None = None
        actual_url = target_url
        bot_blocked = False
        is_yt_url = bool(yt_match)
        skip_yt_probe = is_yt_url and not resolve_cookie_file() and time.monotonic() < _yt_blocked_until

        if skip_yt_probe:
            bot_blocked = True
        else:
            probe_opts = {**base_ydl_opts(use_yt_alt_clients=is_yt_url), "skip_download": True}
            try:
                with yt_dlp.YoutubeDL(probe_opts) as ydl:
                    extracted = ydl.extract_info(target_url, download=False)
                    if isinstance(extracted, dict):
                        info = extracted
            except yt_dlp.utils.DownloadError as exc:
                msg = str(exc)
                if "Sign in" in msg or "bot" in msg.lower() or "403" in msg:
                    bot_blocked = True
                    if is_yt_url and not resolve_cookie_file():
                        _yt_blocked_until = time.monotonic() + 1800
                else:
                    raise HTTPException(status_code=404, detail="Не удалось получить трек по этой ссылке")

        # Если трек с SoundCloud оказался 30с Go+ превью или YouTube заблокировал IP — ищем полную версию на SoundCloud
        need_sc_fallback = bot_blocked or info is None or (isinstance(info, dict) and _is_preview_only(info))
        if need_sc_fallback:
            p_hint, t_hint = (performer_hint or "").strip(), (title_hint or "").strip()
            if (not t_hint or not p_hint) and isinstance(info, dict):
                p2, t2 = split_artist_title(
                    str(info.get("title") or ""),
                    str(info.get("channel") or info.get("uploader") or ""),
                )
                p_hint = p_hint or p2
                t_hint = t_hint or t2
            if (not t_hint or not p_hint) and is_yt_url:
                oe = _youtube_oembed(target_url)
                if oe:
                    p_hint = p_hint or oe[0]
                    t_hint = t_hint or oe[1]
            update_job(status="probing", percent=22, stage="Подбираю полную версию…", eta=6)
            sc_query = f"{p_hint} {t_hint}".strip()
            sc_entry = _find_soundcloud_full_url(sc_query, exclude_url=target_url) if sc_query else None
            if sc_entry and sc_entry.get("webpage_url"):
                info = sc_entry
                actual_url = str(sc_entry["webpage_url"])
                if not title_hint and t_hint:
                    title_hint = t_hint
                if not performer_hint and p_hint:
                    performer_hint = p_hint
            elif bot_blocked:
                raise HTTPException(status_code=503, detail="YouTube временно ограничил загрузку. Попробуй другое название.")
            elif info is None:
                raise HTTPException(status_code=404, detail="Трек не найден")
            else:
                raise HTTPException(status_code=422, detail="Доступно только 30-секундное превью — попробуй другой вариант")

        if info.get("is_live") or info.get("live_status") == "is_live":
            raise HTTPException(status_code=422, detail="Прямые трансляции нельзя скачать как песню")

        dur = info.get("duration")
        duration = int(dur) if isinstance(dur, (int, float)) and dur > 0 else 0
        if duration > MAX_DURATION_SEC:
            max_min = MAX_DURATION_SEC // 60
            raise HTTPException(status_code=422, detail=f"Трек слишком длинный (максимум {max_min} мин)")

        performer, title = split_artist_title(
            str(info.get("title") or ""),
            str(info.get("channel") or info.get("uploader") or ""),
            artist_meta=info.get("artist") or info.get("creator"),
            track_meta=info.get("track"),
        )
        if actual_url != target_url and title_hint and performer_hint:
            title = title_hint[:64]
            performer = performer_hint[:64]
        else:
            if title_hint and title == "Без названия":
                title = title_hint[:64]
            if performer_hint and performer == "Неизвестный исполнитель":
                performer = performer_hint[:64]

        update_job(
            status="downloading",
            percent=28,
            stage="Скачиваю аудио…",
            title=title,
            performer=performer,
            duration=duration,
            eta=5,
        )

        # 2. Скачиваем аудио с параллельной загрузкой HLS-фрагментов и реальным прогресс-хуком
        dl_started = time.monotonic()

        def on_progress(d: dict[str, Any]) -> None:
            st = d.get("status")
            if st == "downloading":
                downloaded = int(d.get("downloaded_bytes") or 0)
                total = int(d.get("total_bytes") or d.get("total_bytes_estimate") or 0)
                frac = 0.0
                if total > 0:
                    frac = min(1.0, downloaded / total)
                else:
                    frag_idx = d.get("fragment_index")
                    frag_cnt = d.get("fragment_count")
                    if isinstance(frag_idx, (int, float)) and isinstance(frag_cnt, (int, float)) and frag_cnt > 0:
                        frac = min(1.0, frag_idx / frag_cnt)
                pct = min(82, max(28, int(28 + frac * 54)))
                raw_eta = d.get("eta")
                if isinstance(raw_eta, (int, float)) and raw_eta >= 0:
                    eta_sec = max(1, int(raw_eta) + 2)
                elif frac > 0.05:
                    elapsed = max(0.5, time.monotonic() - dl_started)
                    eta_sec = max(1, int(elapsed * (1.0 - frac) / frac) + 2)
                else:
                    eta_sec = 5
                update_job(
                    status="downloading",
                    percent=pct,
                    downloaded=downloaded,
                    total=total,
                    eta=eta_sec,
                    stage="Скачиваю аудио…",
                )
            elif st == "finished":
                update_job(status="converting", percent=85, stage="Упаковываю FFmpeg…", eta=2)

        dl_opts = {
            **base_ydl_opts(use_yt_alt_clients=(is_yt_url and actual_url == target_url)),
            # Предпочитаем форматы, которые FFmpeg копирует без перекодирования за 0.2 сек (MP3 / M4A / AAC)
            "format": (
                "bestaudio[ext=mp3][protocol^=http]/"
                "bestaudio[ext=m4a][protocol^=http]/"
                "bestaudio[ext=mp3]/"
                "bestaudio[ext=m4a]/"
                "bestaudio[acodec^=mp4a]/"
                "bestaudio/best"
            ),
            "outtmpl": str(tmp_dir / "source.%(ext)s"),
            "concurrent_fragment_downloads": 8,
            "max_filesize": 100 * 1024 * 1024,
            "progress_hooks": [on_progress],
        }
        try:
            with yt_dlp.YoutubeDL(dl_opts) as ydl:
                ydl.download([actual_url])
        except yt_dlp.utils.DownloadError:
            raise HTTPException(status_code=502, detail="Не получилось скачать аудиодорожку")

        audio_files = [
            p for p in tmp_dir.iterdir()
            if p.is_file() and p.name.startswith("source.") and p.suffix.lower() not in (".jpg", ".jpeg", ".png", ".webp", ".part")
        ]
        if not audio_files:
            raise HTTPException(status_code=502, detail="Аудиофайл не скачался")
        src_audio = audio_files[0]

        # 3. Быстрая упаковка / конвертация через FFmpeg
        is_copy = src_audio.suffix.lower() in (".mp3", ".m4a", ".aac", ".mp4")
        update_job(
            status="converting",
            percent=88,
            stage="Упаковываю теги FFmpeg…" if is_copy else "Конвертирую FFmpeg…",
            eta=1 if is_copy else 4,
        )
        try:
            out_file, ext, content_type = _process_audio(src_audio, tmp_dir, title, performer)
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"Ошибка FFmpeg: {exc}")

        data = out_file.read_bytes()
        if len(data) == 0:
            raise HTTPException(status_code=500, detail="Пустой аудиофайл после обработки")
        if len(data) > MAX_FILE_BYTES:
            raise HTTPException(status_code=422, detail="Файл получился больше 45 МБ — Telegram его не примет")

        track_key = canonical_key or track_key_for(info, target_url)
        source_url = str(info.get("webpage_url") or actual_url)
        thumb_url = best_thumbnail_url(info)

        headers = {
            "X-Track-Key": quote(track_key, safe=":"),
            "X-Track-Title": quote(title, safe=""),
            "X-Track-Performer": quote(performer, safe=""),
            "X-Track-Duration": str(duration),
            "X-Track-Url": quote(source_url, safe=":/?&=%"),
            "X-Track-Thumb": quote(thumb_url, safe=":/?&=%"),
            "X-Track-Ext": ext,
        }

        update_job(
            status="ready",
            percent=100,
            downloaded=len(data),
            total=len(data),
            eta=0,
            stage="Готово!",
            title=title,
            performer=performer,
            duration=duration,
            ext=ext,
            content_type=content_type,
            track_key=track_key,
            source_url=source_url,
            thumb_url=thumb_url,
            data=data,
        )
        return data, headers, content_type


# ---------- разбор запроса загрузки ----------


def _resolve_download_target(req: DownloadRequest) -> tuple[str, str | None, str | None, str | None]:
    title_hint = req.title
    performer_hint = req.performer
    canonical_key: str | None = None

    if req.url:
        target_url = validate_public_url(req.url)
        if req.id:
            canonical_key = req.id.strip()
        return target_url, title_hint, performer_hint, canonical_key

    if req.id:
        raw_id = req.id.strip()
        source = (req.source or "").strip().lower()
        if raw_id.startswith("sc:") or source == "sc":
            sc_id = raw_id.removeprefix("sc:")
            if not SC_ID_RE.match(sc_id):
                raise HTTPException(status_code=400, detail="Некорректный SoundCloud ID")
            cached = _lookup_sc_track(sc_id)
            canonical_key = f"sc:{sc_id}"
            if cached and cached.get("url"):
                return (
                    str(cached["url"]),
                    title_hint or str(cached.get("title") or ""),
                    performer_hint or str(cached.get("performer") or ""),
                    canonical_key,
                )
            return f"https://api.soundcloud.com/tracks/{sc_id}", title_hint, performer_hint, canonical_key

        vid = raw_id.removeprefix("yt:")
        if not VIDEO_ID_RE.match(vid):
            raise HTTPException(status_code=400, detail="Некорректный ID трека")
        return f"https://www.youtube.com/watch?v={vid}", title_hint, performer_hint, f"yt:{vid}"

    raise HTTPException(status_code=400, detail="Нужно указать id или url")


# ---------- схемы и маршруты ----------


class SearchRequest(BaseModel):
    query: str = Field(..., min_length=1, max_length=200)
    limit: int = Field(default=5, ge=1, le=10)


class DownloadRequest(BaseModel):
    id: str | None = None
    source: str | None = None
    url: str | None = None
    title: str | None = None
    performer: str | None = None


@app.get("/health")
async def health() -> dict[str, Any]:
    return {
        "ok": True,
        "version": APP_VERSION,
        "yt_dlp": yt_dlp.version.__version__,
        "ffmpeg": bool(shutil.which(FFMPEG_BIN)),
    }


@app.post("/search", dependencies=[Depends(require_auth)])
async def search(req: SearchRequest) -> JSONResponse:
    query = req.query.strip()
    if not query:
        raise HTTPException(status_code=400, detail="Пустой запрос")
    async with _semaphore:
        tracks = await asyncio.to_thread(_search_sync, query, req.limit)
    return JSONResponse({"tracks": tracks})


@app.post("/download", dependencies=[Depends(require_auth)])
async def download(req: DownloadRequest) -> Response:
    target_url, title_hint, performer_hint, canonical_key = _resolve_download_target(req)
    async with _semaphore:
        audio_bytes, headers, content_type = await asyncio.to_thread(
            _download_sync, target_url, title_hint, performer_hint, canonical_key, None
        )
    return Response(content=audio_bytes, media_type=content_type, headers=headers)


async def _run_job_bg(
    job: DownloadJob,
    target_url: str,
    title_hint: str | None,
    performer_hint: str | None,
    canonical_key: str | None,
) -> None:
    try:
        async with _semaphore:
            await asyncio.to_thread(_download_sync, target_url, title_hint, performer_hint, canonical_key, job)
    except HTTPException as exc:
        with _jobs_lock:
            job.status = "error"
            job.error = str(exc.detail)
            job.error_status = exc.status_code
            job.eta = 0
            job.updated_at = time.monotonic()
    except Exception as exc:
        with _jobs_lock:
            job.status = "error"
            job.error = str(exc)[:200] or "Неизвестная ошибка загрузки"
            job.error_status = 500
            job.eta = 0
            job.updated_at = time.monotonic()


@app.post("/jobs/download", dependencies=[Depends(require_auth)])
async def start_download_job(req: DownloadRequest) -> JSONResponse:
    _prune_old_jobs()
    target_url, title_hint, performer_hint, canonical_key = _resolve_download_target(req)
    job_id = secrets.token_hex(6)
    job = DownloadJob(
        id=job_id,
        title=(title_hint or "")[:64],
        performer=(performer_hint or "")[:64],
        track_key=canonical_key or "",
    )
    with _jobs_lock:
        _jobs[job_id] = job
    asyncio.create_task(_run_job_bg(job, target_url, title_hint, performer_hint, canonical_key))
    return JSONResponse(job.to_dict(), status_code=202)


@app.get("/jobs/{job_id}", dependencies=[Depends(require_auth)])
async def get_download_job(job_id: str) -> JSONResponse:
    if not JOB_ID_RE.match(job_id):
        raise HTTPException(status_code=400, detail="Некорректный ID задачи")
    with _jobs_lock:
        job = _jobs.get(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Задача не найдена или устарела")
        return JSONResponse(job.to_dict())


@app.get("/jobs/{job_id}/file", dependencies=[Depends(require_auth)])
async def get_download_job_file(job_id: str) -> Response:
    if not JOB_ID_RE.match(job_id):
        raise HTTPException(status_code=400, detail="Некорректный ID задачи")
    with _jobs_lock:
        job = _jobs.get(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Задача не найдена или устарела")
        if job.status == "error":
            raise HTTPException(status_code=job.error_status or 500, detail=job.error or "Ошибка загрузки")
        if job.status != "ready" or not job.data:
            raise HTTPException(status_code=409, detail="Трек ещё загружается")
        data = job.data
        content_type = job.content_type
        headers = {
            "X-Track-Key": quote(job.track_key, safe=":"),
            "X-Track-Title": quote(job.title or "Без названия", safe=""),
            "X-Track-Performer": quote(job.performer or "Неизвестный исполнитель", safe=""),
            "X-Track-Duration": str(job.duration),
            "X-Track-Url": quote(job.source_url, safe=":/?&=%"),
            "X-Track-Thumb": quote(job.thumb_url, safe=":/?&=%"),
            "X-Track-Ext": job.ext,
        }
    return Response(content=data, media_type=content_type, headers=headers)

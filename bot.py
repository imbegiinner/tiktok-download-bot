"""
TikTok downloader bot для Telegram.

Режимы:
  1. Личка: отправь боту ссылку на TikTok — получишь видео.
  2. Inline: в любом чате напиши  @имябота <ссылка>  и выбери результат.

Как работает inline:
  - Если видео уже есть в кэше (file_id) — сразу отдаём InlineQueryResultCachedVideo.
  - Если нет — отдаём заглушку «⏳ Загружаю…». Когда пользователь её выбирает,
    Telegram присылает chosen_inline_result; бот качает видео, загружает его
    в служебный чат (CACHE_CHAT_ID), получает file_id и подменяет заглушку
    на видео через editMessageMedia.
"""

import asyncio
import hashlib
import html
import json
import logging
import os
import re
import sqlite3
import tempfile
from pathlib import Path

import aiohttp
import yt_dlp
from aiohttp_socks import ProxyConnector
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ChatAction, ParseMode
from aiogram.filters import CommandStart
from aiogram.types import (
    ChosenInlineResult,
    FSInputFile,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InlineQuery,
    InlineQueryResultArticle,
    InlineQueryResultCachedVideo,
    InputMediaVideo,
    InputTextMessageContent,
    Message,
)
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.environ["BOT_TOKEN"]
CACHE_CHAT_ID = int(os.environ["CACHE_CHAT_ID"])  # чат/канал, куда бот складывает видео ради file_id
PROXY = os.getenv("PROXY") or None                # напр. socks5://user:pass@host:1080
COOKIES_FILE = os.getenv("COOKIES_FILE") or None  # cookies.txt (Netscape) — если TikTok начнёт блокировать
DB_PATH = os.getenv("DB_PATH", "cache.sqlite3")
MAX_PARALLEL = int(os.getenv("MAX_PARALLEL", "3"))

MAX_BYTES = 50 * 1024 * 1024  # лимит Bot API на загрузку файла

TIKTOK_RE = re.compile(
    r"https?://(?:www\.|vm\.|vt\.|m\.)?tiktok\.com/[^\s]+", re.IGNORECASE
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("tiktok-bot")


# ---------------------------------------------------------------- кэш file_id

class Cache:
    def __init__(self, path: str):
        self.db = sqlite3.connect(path)
        self.db.execute("CREATE TABLE IF NOT EXISTS videos (key TEXT PRIMARY KEY, file_id TEXT NOT NULL)")
        self.db.commit()

    def get(self, key: str) -> str | None:
        row = self.db.execute("SELECT file_id FROM videos WHERE key = ?", (key,)).fetchone()
        return row[0] if row else None

    def set(self, keys: list[str], file_id: str) -> None:
        self.db.executemany(
            "INSERT OR REPLACE INTO videos (key, file_id) VALUES (?, ?)",
            [(k, file_id) for k in keys if k],
        )
        self.db.commit()


cache = Cache(DB_PATH)
semaphore = asyncio.Semaphore(MAX_PARALLEL)
url_locks: dict[str, asyncio.Lock] = {}


def extract_url(text: str | None) -> str | None:
    if not text:
        return None
    m = TIKTOK_RE.search(text)
    return m.group(0).rstrip(").,!?»\"'") if m else None


def url_key(url: str) -> str:
    return "url:" + url.split("?")[0].rstrip("/").lower()


# ---------------------------------------------------------------- скачивание

class DownloadError(Exception):
    pass


def _download_sync(url: str, out_dir: str) -> tuple[dict, Path]:
    opts = {
        "outtmpl": f"{out_dir}/%(id)s.%(ext)s",
        # h264 в mp4 нормально играет во всех клиентах Telegram (h265 — не везде)
        "format": "best[vcodec^=h264][ext=mp4]/best[ext=mp4]/best",
        "noplaylist": True,
        "quiet": True,
        "no_warnings": True,
        "no_color": True,  # без ANSI-кодов вроде [0;31m в тексте ошибки
        "max_filesize": MAX_BYTES,
        "retries": 3,
    }
    if PROXY:
        opts["proxy"] = PROXY
    if COOKIES_FILE:
        opts["cookiefile"] = COOKIES_FILE

    with yt_dlp.YoutubeDL(opts) as ydl:
        info = ydl.extract_info(url, download=True)
        path = Path(ydl.prepare_filename(info))

    if not path.exists():
        raise DownloadError("Видео не скачалось (возможно, больше 50 МБ или это фото-карусель).")
    if path.stat().st_size > MAX_BYTES:
        raise DownloadError("Видео больше 50 МБ — Telegram не даёт ботам загружать такие файлы.")
    return info, path


TIKWM_API = "https://www.tikwm.com/api/"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0 Safari/537.36"


def _http_session() -> aiohttp.ClientSession:
    """Сессия aiohttp; PROXY (http или socks5) применяется и здесь, не только в yt-dlp."""
    connector = ProxyConnector.from_url(PROXY) if PROXY else None
    return aiohttp.ClientSession(
        connector=connector,
        timeout=aiohttp.ClientTimeout(total=60),
        headers={"User-Agent": UA},
    )


async def _tikwm_info(s: aiohttp.ClientSession, url: str) -> dict:
    last = "нет ответа"
    for attempt in range(3):
        if attempt:
            await asyncio.sleep(1.5)  # у tikwm лимит ~1 запрос/сек
        try:
            async with s.get(TIKWM_API, params={"url": url, "hd": "1"}) as r:
                body = await r.text()
                status = r.status
        except aiohttp.ClientError as e:
            last = f"сеть: {e.__class__.__name__}: {e}"
            continue
        try:
            payload = json.loads(body)
        except ValueError:
            last = f"HTTP {status}, не JSON: {body[:120]!r}"
            continue
        if not isinstance(payload, dict):
            last = f"HTTP {status}, ответ: {body[:120]!r}"
            continue
        if payload.get("code") == 0 and isinstance(payload.get("data"), dict):
            return payload["data"]
        last = payload.get("msg") or f"code={payload.get('code')}"
    raise DownloadError(f"tikwm: {last}")


async def _download_tikwm(url: str, out_dir: str) -> tuple[dict, Path]:
    """Запасной способ: публичный API tikwm.com (видео без водяного знака)."""
    async with _http_session() as s:
        data = await _tikwm_info(s, url)
        if data.get("images"):
            raise DownloadError("Это фото-карусель, а не видео — такие пока не поддерживаются.")

        # HD, если влезает в лимит, иначе обычное качество
        hd_size = data.get("hd_size") or 0
        video_url = data.get("hdplay") if 0 < hd_size <= MAX_BYTES else None
        video_url = video_url or data.get("play")
        if not video_url:
            raise DownloadError("tikwm: не вернул ссылку на видео")
        if video_url.startswith("/"):
            video_url = "https://www.tikwm.com" + video_url

        path = Path(out_dir) / f"{data.get('id') or 'video'}.mp4"
        size = 0
        async with s.get(video_url) as r:
            if r.status != 200:
                raise DownloadError(f"tikwm: файл видео вернул HTTP {r.status}")
            with open(path, "wb") as f:
                async for chunk in r.content.iter_chunked(1 << 16):
                    size += len(chunk)
                    if size > MAX_BYTES:
                        raise DownloadError("Видео больше 50 МБ — Telegram не даёт ботам загружать такие файлы.")
                    f.write(chunk)

    info = {"id": data.get("id"), "duration": data.get("duration")}
    return info, path


async def download(url: str, out_dir: str) -> tuple[dict, Path]:
    async with semaphore:
        try:
            return await asyncio.to_thread(_download_sync, url, out_dir)
        except DownloadError:
            raise
        except Exception as e:
            log.warning("yt-dlp failed for %s: %s — пробую tikwm", url, e)
            ytdlp_error = str(e).replace("ERROR: ", "")
        try:
            return await _download_tikwm(url, out_dir)
        except DownloadError as e:
            log.warning("tikwm failed for %s: %s", url, e)
            raise DownloadError(f"Не удалось скачать.\nyt-dlp: {ytdlp_error[:300]}\n{e}") from e
        except Exception as e:
            log.exception("tikwm crashed for %s", url)
            raise DownloadError(f"Не удалось скачать.\nyt-dlp: {ytdlp_error[:300]}\ntikwm: {e}") from e


def video_kwargs(info: dict) -> dict:
    kw = {"supports_streaming": True}
    for k in ("width", "height", "duration"):
        if info.get(k):
            kw[k] = int(info[k])
    return kw


async def get_file_id(bot: Bot, url: str) -> str:
    """Возвращает file_id видео: из кэша либо скачивает и заливает в CACHE_CHAT_ID."""
    key = url_key(url)
    if fid := cache.get(key):
        return fid

    lock = url_locks.setdefault(key, asyncio.Lock())
    try:
        return await _fetch_locked(bot, url, key, lock)
    finally:
        if not lock.locked():
            url_locks.pop(key, None)


async def _fetch_locked(bot: Bot, url: str, key: str, lock: asyncio.Lock) -> str:
    async with lock:
        if fid := cache.get(key):  # пока ждали — мог скачать параллельный запрос
            return fid
        with tempfile.TemporaryDirectory() as tmp:
            info, path = await download(url, tmp)
            if fid := cache.get(f"id:{info.get('id')}"):  # то же видео по другой ссылке
                cache.set([key], fid)
                return fid
            msg = await bot.send_video(
                CACHE_CHAT_ID, FSInputFile(path), caption=url,
                disable_notification=True, **video_kwargs(info),
            )
        fid = msg.video.file_id
        cache.set([key, f"id:{info.get('id')}"], fid)
        return fid


# ---------------------------------------------------------------- хендлеры

router = Router()


@router.message(CommandStart())
async def start(message: Message, bot: Bot):
    me = await bot.get_me()
    await message.answer(
        "Привет! Пришли мне ссылку на TikTok — я отправлю видео.\n\n"
        f"А ещё можно в любом чате написать <code>@{me.username} ссылка</code> "
        "и выбрать результат — видео отправится прямо в переписку."
    )


@router.message(F.text)
async def on_text(message: Message, bot: Bot):
    url = extract_url(message.text)
    if not url:
        await message.answer("Пришли ссылку на видео TikTok 🙂")
        return

    await bot.send_chat_action(message.chat.id, ChatAction.UPLOAD_VIDEO)
    try:
        fid = await get_file_id(bot, url)
    except DownloadError as e:
        await message.reply(f"⚠️ {html.escape(str(e))}")
        return
    except Exception:
        log.exception("Unexpected error for %s", url)
        await message.reply("⚠️ Что-то пошло не так, попробуй ещё раз позже.")
        return
    await message.answer_video(fid, reply_to_message_id=message.message_id)


@router.inline_query()
async def on_inline(query: InlineQuery):
    url = extract_url(query.query)
    if not url:
        await query.answer(
            [
                InlineQueryResultArticle(
                    id="help",
                    title="Вставь ссылку на TikTok",
                    description="Например: https://vm.tiktok.com/XXXXXXX/",
                    input_message_content=InputTextMessageContent(
                        message_text="Чтобы отправить видео, напиши имя бота и ссылку на TikTok."
                    ),
                )
            ],
            cache_time=300,
        )
        return

    rid = hashlib.md5(url.encode()).hexdigest()
    fid = cache.get(url_key(url))

    if fid:
        result = InlineQueryResultCachedVideo(id=rid, video_file_id=fid, title="Отправить видео из TikTok")
    else:
        # Кнопка обязательна: без reply_markup Telegram не пришлёт inline_message_id
        result = InlineQueryResultArticle(
            id=rid,
            title="Отправить видео из TikTok",
            description="Нажми — видео подгрузится через пару секунд",
            input_message_content=InputTextMessageContent(message_text="⏳ Загружаю видео…"),
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="🔗 Оригинал", url=url)]]
            ),
        )
    await query.answer([result], cache_time=5, is_personal=True)


@router.chosen_inline_result()
async def on_chosen(chosen: ChosenInlineResult, bot: Bot):
    if not chosen.inline_message_id:
        return  # отправили уже готовое видео из кэша — делать нечего
    url = extract_url(chosen.query)
    if not url:
        return
    try:
        fid = await get_file_id(bot, url)
        await bot.edit_message_media(
            inline_message_id=chosen.inline_message_id,
            media=InputMediaVideo(media=fid, supports_streaming=True),
        )
    except DownloadError as e:
        await bot.edit_message_text(inline_message_id=chosen.inline_message_id, text=f"⚠️ {html.escape(str(e))}")
    except Exception:
        log.exception("Inline edit failed for %s", url)
        await bot.edit_message_text(
            inline_message_id=chosen.inline_message_id,
            text="⚠️ Не удалось загрузить видео, попробуй ещё раз.",
        )


async def main():
    bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher()
    dp.include_router(router)
    # resolve_used_update_types() включит inline_query и chosen_inline_result
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())


if __name__ == "__main__":
    asyncio.run(main())

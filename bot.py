import os
import asyncio
import logging
import re
import random
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse, urljoin
from typing import List, Dict, Any, Tuple, Set

import asyncpg
import aiohttp
from aiohttp import web
import feedparser
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import pymorphy3
from thefuzz import fuzz

from aiogram import Bot, Dispatcher, types, F
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.client.default import DefaultBotProperties
from aiogram.exceptions import TelegramAPIError
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application

# --- 0. PRE-CONFIGURATION ---
load_dotenv()
try:
    morph = pymorphy3.MorphAnalyzer(lang='uk')
except Exception as e:
    print(f"Error initializing pymorphy3: {e}")
    morph = None

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

KYIV_TZ = timezone(timedelta(hours=3), 'Europe/Kyiv')

# --- 1. CONFIGURATION AND CONSTANTS ---
class Config:
    POSTING_INTERVAL_MIN = 5
    MAX_NEWS_PER_CYCLE = 3
    MAX_AGE_MIN = 60
    MIN_TITLE_LENGTH = 25
    MIN_SUMMARY_LENGTH = 50
    FETCH_LIMIT = 30
    NUM_SOURCES_TO_FETCH = 24
    HTTP_TIMEOUT = 15
    MAX_CONCURRENCY = 25
    MAX_RETRIES = 3
    RETRY_DELAY_SEC = 2
    DUPLICATE_TITLE_THRESHOLD = 88
    DB_POOL_MIN = 2
    DB_POOL_MAX = 8
    DB_CLEANUP_DAYS = 5
    CLEANUP_INTERVAL_HOURS = 2
    BLOCKED_HTTP_CODES = [403, 404, 429, 500, 502, 503]
    SOURCE_BLOCK_THRESHOLD = 5
    SOURCE_BLOCK_DURATION_HOURS = 4
    DEFAULT_HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 UkrainianNewsBot/2.0',
        'Accept': 'application/xml;q=0.9, text/html,application/xhtml+xml;q=0.8,*/*;q=0.7',
        'Accept-Language': 'uk-UA,uk;q=0.9,en-US;q=0.8,en;q=0.7',
    }
    SOURCES: List[str] = [
        "https://tsn.ua/rss/all.xml", "https://www.pravda.com.ua/rss/news/",
        "https://censor.net/rss/all_news", "https://www.rbc.ua/static/rss/all.xml",
        "https://www.ukrinform.ua/rss/all.xml", "https://www.liga.net/rss/news.xml",
        "https://www.obozrevatel.com/rss/main.xml", "https://minfin.com.ua/rss/news/",
        "https://focus.ua/rss/latest.xml", "https://ua.korrespondent.net/rss/all",
        "https://gazeta.ua/rss/all", "https://24tv.ua/rss/all.xml",
        "https://nv.ua/ukr/rss/all.xml", "https://delo.ua/rss/all.xml",
        "https://suspilne.media/feed/", "https://www.bbc.com/ukrainian/rss.xml",
        "https://news.finance.ua/ua/rss", "https://www.unian.ua/rss/news.rss",
        "https://ua.interfax.com.ua/news/ukraine.rss", "https://hromadske.ua/feed/news",
        "https://biz.censor.net/rss", "https://slovoidilo.ua/rss/index.xml",
        "https://apostrophe.ua/rss", "https://babel.ua/rss"
    ]

# --- 2. ENVIRONMENT VARIABLES & GLOBALS ---
BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
CHANNEL_ID = os.getenv("CHANNEL_ID")
WEB_SERVER_HOST = os.getenv("WEB_SERVER_HOST", "0.0.0.0")
WEB_SERVER_PORT = int(os.getenv("PORT", 8080))
WEBHOOK_HOST = os.getenv("WEBHOOK_HOST")
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", f"/{BOT_TOKEN}")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET")
WEBHOOK_URL = urljoin(WEBHOOK_HOST, WEBHOOK_PATH) if WEBHOOK_HOST else None
ADMIN_ID = int(os.getenv("ADMIN_ID", 0))

db_pool: asyncpg.Pool = None
bot: Bot = None
dp: Dispatcher = None
current_post_limit: int = 0
app_tasks: Set[asyncio.Task] = set()

# --- 3. DATABASE (POSTGRESQL/NEON) ---
async def connect_db():
    global db_pool
    try:
        db_pool = await asyncpg.create_pool(
            DATABASE_URL, min_size=Config.DB_POOL_MIN, max_size=Config.DB_POOL_MAX, timeout=10, statement_cache_size=0
        )
        logger.info(f"✅ DB connected. Pool size: {Config.DB_POOL_MIN}-{Config.DB_POOL_MAX}.")
    except Exception as e:
        logger.critical(f"❌ Critical DB connection error: {e}", exc_info=True)
        await asyncio.sleep(60); exit(1)

async def init_db():
    if not db_pool: return
    async with db_pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS news (
                id SERIAL PRIMARY KEY, source VARCHAR(255) NOT NULL, url TEXT UNIQUE NOT NULL, title TEXT NOT NULL,
                summary TEXT, image_url TEXT, published_at TIMESTAMPTZ NOT NULL,
                inserted_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP, is_posted BOOLEAN DEFAULT FALSE, score SMALLINT DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS source_stats (
                source_url TEXT PRIMARY KEY, error_count INTEGER DEFAULT 0, last_error_at TIMESTAMPTZ, is_blocked BOOLEAN DEFAULT FALSE
            );
            CREATE UNIQUE INDEX IF NOT EXISTS news_url_idx ON news (url);
            CREATE INDEX IF NOT EXISTS news_is_posted_idx ON news (is_posted, score DESC, published_at DESC);
        """)
    logger.info("DB tables initialized/verified.")

async def save_news_with_transaction(news_items: List[Dict[str, Any]]) -> int:
    if not news_items or not db_pool: return 0
    unique_news = []
    try:
        async with db_pool.acquire() as conn:
            recent_titles = {r['title'] for r in await conn.fetch("SELECT title FROM news WHERE published_at > $1", datetime.now(KYIV_TZ) - timedelta(hours=3))}
        for item in news_items:
            if not any(fuzz.ratio(item['title'], t) > Config.DUPLICATE_TITLE_THRESHOLD for t in recent_titles):
                unique_news.append(item); recent_titles.add(item['title'])
    except Exception as e:
        logger.error(f"Duplicate check failed: {e}. Skipping check."); unique_news = news_items

    if not unique_news: return 0
    sql = """
        INSERT INTO news (source, url, title, summary, image_url, published_at, score)
        SELECT * FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::timestamptz[], $7::smallint[])
        ON CONFLICT (url) DO NOTHING;
    """
    params = ([item[key] for item in unique_news] for key in ['source', 'url', 'title', 'summary', 'image_url', 'published_at', 'score'])
    try:
        async with db_pool.acquire() as conn:
            result = await conn.execute(sql, *params)
            return int(result.split()[-1])
    except asyncpg.PostgresError as e:
        logger.error(f"❌ DB transaction insert error: {e}"); return 0

async def get_active_sources_from_db() -> Set[str]:
    if not db_pool: return set(Config.SOURCES)
    await update_source_block_status()
    try:
        async with db_pool.acquire() as conn:
            blocked_urls = {r['source_url'] for r in await conn.fetch("SELECT source_url FROM source_stats WHERE is_blocked = TRUE;")}
            active_sources = set(Config.SOURCES) - blocked_urls
            all_db_sources = {r['source_url'] for r in await conn.fetch("SELECT source_url FROM source_stats;")}
            if new_sources := set(Config.SOURCES) - all_db_sources:
                 await conn.executemany("INSERT INTO source_stats (source_url) VALUES ($1) ON CONFLICT DO NOTHING;", [(url,) for url in new_sources])
            logger.info(f"Sources: {len(active_sources)} active, {len(blocked_urls)} blocked.")
            return active_sources
    except asyncpg.PostgresError as e:
        logger.error(f"❌ Failed to get active sources: {e}"); return set(Config.SOURCES)

async def update_source_error_count(source_url: str, is_error: bool, http_code: int = None):
    if not db_pool: return
    async with db_pool.acquire() as conn:
        if is_error and http_code in Config.BLOCKED_HTTP_CODES:
            await conn.execute("""
                INSERT INTO source_stats (source_url, error_count, last_error_at) VALUES ($1, 1, $2)
                ON CONFLICT (source_url) DO UPDATE SET error_count = source_stats.error_count + 1, last_error_at = $2;
            """, source_url, datetime.now(KYIV_TZ))
            if (rec := await conn.fetchrow("SELECT error_count FROM source_stats WHERE source_url = $1;", source_url)) and rec['error_count'] >= Config.SOURCE_BLOCK_THRESHOLD:
                await conn.execute("UPDATE source_stats SET is_blocked = TRUE WHERE source_url = $1;", source_url)
                logger.warning(f"🚨 Source blocked: {source_url}. Errors: {rec['error_count']}.")
        elif not is_error:
            await conn.execute("UPDATE source_stats SET error_count = 0, is_blocked = FALSE WHERE source_url = $1;", source_url)

async def update_source_block_status():
    if not db_pool: return
    unlock_time = datetime.now(KYIV_TZ) - timedelta(hours=Config.SOURCE_BLOCK_DURATION_HOURS)
    async with db_pool.acquire() as conn:
        if records := await conn.fetch("UPDATE source_stats SET is_blocked = FALSE, error_count = 0 WHERE is_blocked = TRUE AND last_error_at < $1 RETURNING source_url;", unlock_time):
            logger.info(f"🔓 Unblocked {len(records)} sources.")

async def get_unique_news_from_db(limit: int) -> List[Dict[str, Any]]:
    if not db_pool or limit == 0: return []
    sql = """
        SELECT source, url, title, summary, image_url, published_at FROM news WHERE is_posted = FALSE
        ORDER BY score DESC, (CASE WHEN image_url IS NOT NULL THEN 0 ELSE 1 END), published_at DESC LIMIT $1;
    """
    try:
        async with db_pool.acquire() as conn: return [dict(r) for r in await conn.fetch(sql, limit)]
    except Exception as e:
        logger.error(f"❌ DB get news error: {e}"); return []

async def mark_news_as_posted(urls: List[str]):
    if urls and db_pool:
        async with db_pool.acquire() as conn: await conn.execute("UPDATE news SET is_posted = TRUE WHERE url = ANY($1::text[]);", urls)

async def cleanup_db():
    if not db_pool: return
    cutoff = datetime.now(KYIV_TZ) - timedelta(days=Config.DB_CLEANUP_DAYS)
    async with db_pool.acquire() as conn:
        if (res := await conn.execute("DELETE FROM news WHERE inserted_at < $1;", cutoff)) and (count := int(res.split()[-1])) > 0:
            logger.info(f"🗑️ DB cleanup deleted {count} old news items.")

async def get_db_stats() -> Dict[str, Any]:
    if not db_pool: return {}
    sql = """
        SELECT COUNT(*) AS total, COUNT(*) FILTER (WHERE is_posted) AS posted,
               COUNT(*) FILTER (WHERE NOT is_posted) AS unposted,
               COUNT(*) FILTER (WHERE NOT is_posted AND image_url IS NOT NULL) AS unposted_img
        FROM news;
    """
    async with db_pool.acquire() as conn: return dict(await conn.fetchrow(sql) or {})

# --- 4. CONTENT PROCESSING & HASHTAGS ---
def calculate_news_score(title: str, summary: str) -> int:
    content = (title + ' ' + summary).lower()
    score = 0
    PRIORITY_KW = {'зсу': 15, 'війна': 12, 'обстріл': 12, 'фронт': 12, 'ракета': 10, 'президент': 10, 'зеленський': 10, 'кабмін': 8, 'сбу': 8, 'гур': 8, 'сша': 7, 'нато': 7, 'допомога': 7, 'санкції': 7}
    NEGATIVE_KW = ['гороскоп', 'астрологічний', 'реклама', 'погода', 'рецепт', 'шоу-бізнес', 'ви не повірите', 'шокуюча правда']
    if any(kw in content for kw in NEGATIVE_KW): return -1
    for kw, value in PRIORITY_KW.items():
        if kw in content: score += value
    return score

def normalize_summary(text: str) -> str:
    if not text: return ""
    soup = BeautifulSoup(text, 'html.parser')
    clean_text = re.sub(r'\s+', ' ', soup.get_text()).strip()
    if len(clean_text) > 450:
        clean_text = clean_text[:420]
        if (last_end := max(clean_text.rfind('.'), clean_text.rfind('!'), clean_text.rfind('?'))) > 100:
            clean_text = clean_text[:last_end + 1]
        else: clean_text += "..."
    return clean_text

def extract_image_url(entry: feedparser.FeedParserDict) -> str | None:
    if 'enclosures' in entry:
        for enc in entry.enclosures:
            if enc.get('type', '').startswith('image/'): return enc.get('href')
    if 'media_content' in entry:
        for media in entry.media_content:
            if media.get('medium') == 'image' and 'url' in media: return media.get('url')
    if html_content := entry.get('content', [{}])[0].get('value') or entry.get('summary'):
        if img := BeautifulSoup(html_content, 'html.parser').find('img'): return img.get('src')
    return None

def parse_published_time(entry: feedparser.FeedParserDict) -> datetime:
    if hasattr(entry, 'published_parsed') and entry.published_parsed:
        try: return datetime(*entry.published_parsed[:6], tzinfo=timezone.utc).astimezone(KYIV_TZ)
        except (ValueError, TypeError): pass
    return datetime.now(KYIV_TZ)

def generate_hashtags(title: str, source: str) -> str:
    if not morph: return "#Новини"
    STOP_WORDS = {'на', 'в', 'у', 'з', 'до', 'про', 'від', 'для', 'це', 'що', 'як', 'та', 'і', 'за', 'під', 'проти'}
    MULTI_WORD_MAP = {
        ('володимир', 'зеленський'): 'ВолодимирЗеленський', ('джо', 'байден'): 'ДжоБайден',
        ('олександр', 'сирський'): 'ОлександрСирський', ('кирило', 'буданов'): 'КирилоБуданов',
        ('сполучені', 'штати'): 'США', ('велика', 'британія'): 'ВеликаБританія', ('європейський', 'союз'): 'ЄС'
    }
    hashtags: Set[str] = {f"#{urlparse(f'https://{source}').netloc.split('.')[-2].capitalize()}"}
    words = re.sub(r'[^\w\s-]', '', title).lower().split()
    i = 0
    while i < len(words):
        found = False
        for (w1, w2), tag in MULTI_WORD_MAP.items():
            if i + 1 < len(words) and words[i] == w1 and words[i+1] == w2:
                hashtags.add(f"#{tag}"); i += 2; found = True; break
        if found: continue
        word = words[i]
        if len(word) > 3 and word not in STOP_WORDS:
            p = morph.parse(word)[0]
            if 'NOUN' in p.tag and p.score > 0.4:
                hashtags.add(f"#{p.normal_form.capitalize()}")
        i += 1
    return " ".join(["#Новини"] + sorted(list(hashtags), key=len, reverse=True)[:6])

# --- 5. CORE PARSING & POSTING LOGIC ---
async def fetch_and_parse_source(session: aiohttp.ClientSession, rss_url: str) -> List[Dict[str, Any]]:
    news_items, source_domain = [], urlparse(rss_url).netloc.replace('www.', '')
    for attempt in range(Config.MAX_RETRIES):
        try:
            async with session.get(rss_url, timeout=Config.HTTP_TIMEOUT) as resp:
                if resp.status == 200:
                    await update_source_error_count(rss_url, is_error=False)
                    feed = feedparser.parse(await resp.text())
                    for entry in feed.entries[:Config.FETCH_LIMIT]:
                        if (pub_time := parse_published_time(entry)) and datetime.now(KYIV_TZ) - pub_time > timedelta(minutes=Config.MAX_AGE_MIN): continue
                        title, summary = entry.title.strip(), normalize_summary(entry.get('summary') or entry.get('description'))
                        if len(title) < Config.MIN_TITLE_LENGTH or len(summary) < Config.MIN_SUMMARY_LENGTH: continue
                        if (score := calculate_news_score(title, summary)) < 0: continue
                        news_items.append({'source': source_domain, 'title': title, 'url': entry.link, 'summary': summary,
                                           'image_url': extract_image_url(entry), 'published_at': pub_time, 'score': score})
                    return news_items
                elif resp.status in Config.BLOCKED_HTTP_CODES:
                    logger.warning(f"⚠️ HTTP {resp.status} for {rss_url}. Blocking."); await update_source_error_count(rss_url, True, resp.status); return []
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.warning(f"❌ Network error on {rss_url} (attempt {attempt+1}): {type(e).__name__}.")
        if attempt < Config.MAX_RETRIES - 1: await asyncio.sleep(Config.RETRY_DELAY_SEC * (2 ** attempt))
    await update_source_error_count(rss_url, True, 599); return []

async def fetch_all_sources() -> Tuple[List[Dict[str, Any]], float]:
    start_time = datetime.now()
    active_sources = await get_active_sources_from_db()
    selected_sources = random.sample(list(active_sources), min(Config.NUM_SOURCES_TO_FETCH, len(active_sources)))
    logger.info(f"⏳ Parsing {len(selected_sources)} random active sources...")
    conn = aiohttp.TCPConnector(limit=Config.MAX_CONCURRENCY, ssl=False)
    async with aiohttp.ClientSession(connector=conn, headers=Config.DEFAULT_HEADERS) as session:
        tasks = [fetch_and_parse_source(session, url) for url in selected_sources]
        results = await asyncio.gather(*tasks, return_exceptions=True)
    all_news = [item for res in results if isinstance(res, list) for item in res]
    return all_news, (datetime.now() - start_time).total_seconds()

def format_news_post(item: Dict[str, Any]) -> str:
    hashtags = generate_hashtags(item['title'], item['source'])
    return (f"<b>⚡️ {item['title']}</b>\n\n{item['summary']}\n\n"
            f"<a href='{item['url']}'>Подробиці на {item['source']}</a>\n\n{hashtags}")

async def send_news_to_channel(news_to_post: List[Dict[str, Any]]) -> int:
    posted_urls = []
    for news in news_to_post:
        try:
            caption = format_news_post(news)
            if img_url := news.get('image_url'):
                await bot.send_photo(CHANNEL_ID, photo=img_url, caption=caption)
            else:
                await bot.send_message(CHANNEL_ID, text=caption, disable_web_page_preview=True)
            posted_urls.append(news['url'])
            await asyncio.sleep(1.5)
        except TelegramAPIError as e:
            logger.error(f"❌ Telegram API Error for '{news['title'][:40]}...': {e.message}")
            if "failed to get HTTP URL content" in e.message or "PHOTO_INVALID" in e.message:
                posted_urls.append(news['url']) # Mark as posted to avoid error loops
        except Exception as e:
            logger.error(f"❌ Unknown sending error for '{news['title'][:40]}...': {e}", exc_info=True)
    if posted_urls: await mark_news_as_posted(posted_urls)
    return len(posted_urls)

# --- 6. BACKGROUND LOOPS & ADMIN COMMANDS ---
async def auto_posting_loop():
    global current_post_limit
    wait_time = Config.POSTING_INTERVAL_MIN * 60
    current_post_limit = 0
    while True:
        try:
            logger.info("--- 🚀 Starting auto-posting cycle ---")
            news, p_dur = await fetch_all_sources()
            new_count = await save_news_with_transaction(news)
            current_post_limit = min(current_post_limit + 1, Config.MAX_NEWS_PER_CYCLE)
            news_to_post = await get_unique_news_from_db(current_post_limit)
            posted_count = await send_news_to_channel(news_to_post)
            logger.info(f"--- ✅ Cycle finished. New: {new_count}. Limit: {current_post_limit}. Posted: {posted_count}. Parse time: {p_dur:.2f}s ---")
        except Exception as e:
            logger.critical(f"❌ CRITICAL error in auto-posting loop: {e}", exc_info=True)
        await asyncio.sleep(wait_time)

async def db_maintenance_loop():
    while True:
        await asyncio.sleep(Config.CLEANUP_INTERVAL_HOURS * 3600)
        logger.info("--- ♻️ Running background DB maintenance ---")
        await cleanup_db()
        await update_source_block_status()

async def cmd_status(message: types.Message):
    stats = await get_db_stats(); active_src = await get_active_sources_from_db()
    msg = (f"<b>🤖 Платформа Новин: Онлайн</b>\n\n"
           f"<b>⚙️ Налаштування:</b>\n"
           f"  - Інтервал: <b>{Config.POSTING_INTERVAL_MIN} хв</b>\n"
           f"  - Поточний ліміт/макс: <b>{current_post_limit}/{Config.MAX_NEWS_PER_CYCLE}</b>\n"
           f"  - Джерела: <b>{len(active_src)}/{len(Config.SOURCES)}</b> активних\n\n"
           f"📊 <b>Статистика БД:</b>\n"
           f"  - Всього новин: {stats.get('total', 0)}\n"
           f"  - Опубліковано: {stats.get('posted', 0)}\n"
           f"  - В черзі: <b>{stats.get('unposted', 0)}</b> (з фото: {stats.get('unposted_img', 0)})")
    await message.answer(msg)

async def cmd_forcepost(message: types.Message):
    await message.answer("⏳ Примусовий запуск циклу...")
    news, p_dur = await fetch_all_sources()
    new_count = await save_news_with_transaction(news)
    posted_count = await send_news_to_channel(await get_unique_news_from_db(Config.MAX_NEWS_PER_CYCLE))
    await message.answer(f"✅ <b>Примусовий цикл завершено!</b>\n"
                         f"  - Знайдено нових: <b>{new_count}</b>\n"
                         f"  - Опубліковано: <b>{posted_count}</b> (ліміт: {Config.MAX_NEWS_PER_CYCLE})\n"
                         f"  - Час парсингу: {p_dur:.2f} сек")

async def cmd_blocked(message: types.Message):
    async with db_pool.acquire() as conn:
        records = await conn.fetch("SELECT source_url, last_error_at FROM source_stats WHERE is_blocked = TRUE;")
    if not records: await message.answer("✅ Всі джерела активні."); return
    lines = ["<b>🚨 Заблоковані джерела:</b>"]
    for r in records:
        ago = (datetime.now(KYIV_TZ) - r['last_error_at']).total_seconds() / 3600
        lines.append(f"  - <code>{r['source_url']}</code> ({ago:.1f} год тому)")
    await message.answer("\n".join(lines))

# --- 7. BOT LAUNCH & WEB SERVER ---
async def health_check(request: web.Request) -> web.Response:
    return web.json_response({"status": "ok", "timestamp": datetime.now(KYIV_TZ).isoformat()})

async def on_startup(bot_instance: Bot, app: web.Application):
    await connect_db(); await init_db()
    await bot_instance.set_webhook(url=WEBHOOK_URL, secret_token=WEBHOOK_SECRET, drop_pending_updates=True)
    logger.info(f"✅ Webhook set on: {WEBHOOK_URL}")
    task1 = asyncio.create_task(auto_posting_loop())
    task2 = asyncio.create_task(db_maintenance_loop())
    app_tasks.update([task1, task2])
    logger.info("🚀 Bot started. Background tasks running.")

async def on_shutdown(bot_instance: Bot, app: web.Application):
    logger.info("🔻 Shutting down...")
    for task in app_tasks: task.cancel()
    await asyncio.gather(*app_tasks, return_exceptions=True)
    if bot_instance: await bot_instance.delete_webhook(); await bot_instance.session.close()
    if db_pool: await db_pool.close()
    logger.info(" Bot gracefully stopped.")

def main():
    if not all([BOT_TOKEN, DATABASE_URL, CHANNEL_ID, WEBHOOK_HOST, ADMIN_ID]):
        logger.critical("CRITICAL: Missing one or more required environment variables."); return

    global bot, dp
    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher()
    
    admin_filter = F.from_user.id == ADMIN_ID
    dp.message.register(cmd_status, Command("status"), admin_filter)
    dp.message.register(cmd_forcepost, Command("forcepost"), admin_filter)
    dp.message.register(cmd_blocked, Command("blocked"), admin_filter)

    app = web.Application()
    app.router.add_get("/health", health_check) # Health check endpoint
    
    webhook_handler = SimpleRequestHandler(dispatcher=dp, bot=bot, secret_token=WEBHOOK_SECRET)
    webhook_handler.register(app, path=WEBHOOK_PATH)
    
    app.on_startup.append(lambda a: on_startup(bot, a))
    app.on_shutdown.append(lambda a: on_shutdown(bot, a))
    
    web.run_app(app, host=WEB_SERVER_HOST, port=WEB_SERVER_PORT)

if __name__ == "__main__":
    try:
        main()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot stopped manually.")
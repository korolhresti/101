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
from aiogram.webhook.aiohttp_server import SimpleRequestHandler

# --- 0. PRE-CONFIGURATION ---

# Завантаження змінних оточення (для локального тестування)
load_dotenv()

# Налаштування морфологічного аналізатора для української мови
morph = pymorphy3.MorphAnalyzer(lang='uk')

# Налаштування логування
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(filename)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Використовуйте Kyiv time zone (UTC+3)
KYIV_TZ = timezone(timedelta(hours=3), 'Europe/Kyiv')


# --- 1. CONFIGURATION AND CONSTANTS ---

class Config:
    """Конфігурація платформи, зібрана в одному місці."""

    # ⚙️ ОСНОВНІ ПАРАМЕТРИ ЦИКЛУ
    POSTING_INTERVAL_MIN = 5   # Кожні 5 хвилин
    MAX_NEWS_PER_CYCLE = 3     # СТРОГИЙ ЛІМІТ: До 3 новин за цикл (ТОП-3)
    MAX_AGE_MIN = 45           # Не публікувати новини старше 45 хвилин (розширено для гнучкості)
    MIN_TITLE_LENGTH = 20      # Мінімальна довжина заголовка для фільтрації клікбейту

    # 🛡️ ПАРАМЕТРИ НАДІЙНОСТІ ТА ПРОДУКТИВНОСТІ
    FETCH_LIMIT = 25           # Макс. кількість записів для обробки з одного RSS-фіда
    NUM_SOURCES_TO_FETCH = 24  # Кількість випадкових джерел, які парсяться за цикл
    HTTP_TIMEOUT = 12          # Таймаут для HTTP-запитів (трохи збільшено)
    MAX_CONCURRENCY = 25       # Макс. одночасних з'єднань для парсингу
    MAX_RETRIES = 3            # Макс. кількість повторних спроб для HTTP-запиту
    RETRY_DELAY_SEC = 2        # Початкова затримка повтору (з експоненційною витримкою)
    DUPLICATE_TITLE_THRESHOLD = 85 # Поріг схожості заголовків для визначення дублікатів (у %)

    # 💾 ПАРАМЕТРИ ОБСЛУГОВУВАННЯ БАЗИ ДАНИХ (Оптимізація Neon)
    DB_POOL_MIN = 2            # Мінімальний розмір пулу
    DB_POOL_MAX = 7            # Максимальний розмір пулу (для кращої конкурентності)
    DB_CLEANUP_DAYS = 7        # Видаляти записи новин старше 7 днів
    CLEANUP_INTERVAL_HOURS = 2 # Частота очистки БД

    # ❌ ПАРАМЕТРИ БЛОКУВАННЯ ДЖЕРЕЛ (Економія Compute Time)
    BLOCKED_HTTP_CODES = [403, 404, 500, 503]
    SOURCE_BLOCK_THRESHOLD = 5 # Кількість помилок, після яких джерело блокується
    SOURCE_BLOCK_DURATION_HOURS = 3 # На скільки годин блокувати джерело

    DEFAULT_HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 UkrainianNewsBot/1.0 (+https://t.me/YourNewsBotChannel)',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'uk-UA,uk;q=0.9,en-US;q=0.8,en;q=0.7',
    }

    # 📰 Джерела новин (Оновлений список)
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
        "https://apostrophe.ua/rss", "https://babel.ua/rss"  # <-- ДОДАНО
    ]


# --- 2. WEBHOOK AND ENVIRONMENT VARIABLES ---
BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
CHANNEL_ID = os.getenv("CHANNEL_ID")
WEB_SERVER_HOST = os.getenv("WEB_SERVER_HOST", "0.0.0.0")
WEB_SERVER_PORT = int(os.getenv("PORT", 8080))
WEBHOOK_HOST = os.getenv("WEBHOOK_HOST")
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "/webhook")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET")
WEBHOOK_URL = urljoin(WEBHOOK_HOST, WEBHOOK_PATH) if WEBHOOK_HOST else None
try:
    ADMIN_ID = int(os.getenv("ADMIN_ID", 0))
except ValueError:
    ADMIN_ID = 0

# Глобальні змінні для DB, Bot, Dispatcher
db_pool: asyncpg.Pool = None
dp: Dispatcher = None
bot: Bot = None
current_post_limit: int = 0


# --- 3. DATABASE (POSTGRESQL/NEON) ---

async def connect_db():
    """Створює пул з'єднань до PostgreSQL, оптимізований для Neon."""
    global db_pool
    if not DATABASE_URL:
        logger.critical("Критична помилка: Не задано DATABASE_URL.")
        return
    try:
        db_pool = await asyncpg.create_pool(
            DATABASE_URL,
            min_size=Config.DB_POOL_MIN,
            max_size=Config.DB_POOL_MAX,
            timeout=10,
            statement_cache_size=0
        )
        logger.info(f"✅ Успішно підключено до Neon PostgreSQL. Пул: {Config.DB_POOL_MIN}-{Config.DB_POOL_MAX}.")
    except Exception as e:
        logger.error(f"❌ Критична помилка підключення до DB: {e}")
        await asyncio.sleep(60)
        exit(1)


async def init_db():
    """Створює таблиці 'news' та 'source_stats' для надійної роботи."""
    if not db_pool: return
    async with db_pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS news (
                id SERIAL PRIMARY KEY,
                source VARCHAR(255) NOT NULL,
                url TEXT UNIQUE NOT NULL,
                title TEXT NOT NULL,
                summary TEXT,
                image_url TEXT,
                published_at TIMESTAMP WITH TIME ZONE NOT NULL,
                inserted_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                is_posted BOOLEAN DEFAULT FALSE,
                score SMALLINT DEFAULT 0
            );
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS source_stats (
                source_url TEXT PRIMARY KEY,
                error_count INTEGER DEFAULT 0,
                last_error_at TIMESTAMP WITH TIME ZONE,
                is_blocked BOOLEAN DEFAULT FALSE
            );
        """)
        try:
            await conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS news_url_idx ON news (url);")
            await conn.execute("CREATE INDEX IF NOT EXISTS news_is_posted_idx ON news (is_posted, score DESC, published_at DESC);")
        except Exception as e:
            logger.error(f"Помилка при створенні індексу: {e}")
    logger.info("Таблиці DB перевірені/оновлені.")


async def save_news_with_transaction(news_items: List[Dict[str, Any]]) -> int:
    """Виконує пакетну вставку новин в одній транзакції з перевіркою на дублікати."""
    if not news_items or not db_pool:
        return 0

    # 1. Фільтрація дублікатів за схожістю заголовків
    try:
        async with db_pool.acquire() as conn:
            recent_titles = await conn.fetch("SELECT title FROM news WHERE published_at > $1", datetime.now(KYIV_TZ) - timedelta(hours=2))
            recent_titles_set = {r['title'] for r in recent_titles}

        unique_news = []
        for item in news_items:
            is_duplicate = False
            for existing_title in recent_titles_set:
                if fuzz.ratio(item['title'], existing_title) > Config.DUPLICATE_TITLE_THRESHOLD:
                    is_duplicate = True
                    break
            if not is_duplicate:
                unique_news.append(item)
                recent_titles_set.add(item['title'])
    except Exception as e:
        logger.error(f"Помилка перевірки на дублікати: {e}. Продовжуємо без неї.")
        unique_news = news_items

    if not unique_news:
        logger.info("Всі знайдені новини відфільтровані як дублікати.")
        return 0

    # 2. Пакетна вставка унікальних новин
    sql = """
        INSERT INTO news (source, url, title, summary, image_url, published_at, score)
        SELECT * FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::timestamptz[], $7::smallint[])
        ON CONFLICT (url) DO NOTHING;
    """
    params = ([item[key] for item in unique_news] for key in ['source', 'url', 'title', 'summary', 'image_url', 'published_at', 'score'])
    
    try:
        async with db_pool.acquire() as conn:
            result = await conn.execute(sql, *params)
            inserted_count = int(result.split()[-1])
            return inserted_count
    except asyncpg.exceptions.PostgresError as e:
        logger.error(f"❌ Помилка транзакційної вставки в БД: {e}")
        return 0


async def get_active_sources_from_db() -> Set[str]:
    """Вибирає лише ті джерела, які не заблоковані або час блокування минув."""
    if not db_pool: return set(Config.SOURCES)
    
    await update_source_block_status()
    
    sql_get_blocked = "SELECT source_url FROM source_stats WHERE is_blocked = TRUE;"
    try:
        async with db_pool.acquire() as conn:
            blocked_records = await conn.fetch(sql_get_blocked)
            blocked_urls = {r['source_url'] for r in blocked_records}
            active_sources = set(Config.SOURCES) - blocked_urls

            all_db_sources = {r['source_url'] for r in await conn.fetch("SELECT source_url FROM source_stats;")}
            new_sources = set(Config.SOURCES) - all_db_sources
            if new_sources:
                 await conn.executemany("INSERT INTO source_stats (source_url) VALUES ($1) ON CONFLICT DO NOTHING;", [(url,) for url in new_sources])
                 
            logger.info(f"Активних джерел: {len(active_sources)}. Заблокованих: {len(blocked_urls)}")
            return active_sources
    except asyncpg.exceptions.PostgresError as e:
        logger.error(f"❌ Помилка отримання активних джерел: {e}")
        return set(Config.SOURCES)


async def update_source_error_count(source_url: str, is_error: bool, http_code: int = None):
    """Оновлює статистику джерела та блокує його при досягненні порога."""
    if not db_pool: return
    async with db_pool.acquire() as conn:
        if is_error and http_code in Config.BLOCKED_HTTP_CODES:
            await conn.execute("""
                INSERT INTO source_stats (source_url, error_count, last_error_at) VALUES ($1, 1, $2)
                ON CONFLICT (source_url) DO UPDATE SET error_count = source_stats.error_count + 1, last_error_at = $2;
            """, source_url, datetime.now(KYIV_TZ))
            record = await conn.fetchrow("SELECT error_count FROM source_stats WHERE source_url = $1;", source_url)
            if record and record['error_count'] >= Config.SOURCE_BLOCK_THRESHOLD:
                await conn.execute("UPDATE source_stats SET is_blocked = TRUE WHERE source_url = $1;", source_url)
                logger.warning(f"🚨 Джерело заблоковано: {source_url}. Помилок: {record['error_count']}.")
        elif not is_error:
            await conn.execute("""
                INSERT INTO source_stats (source_url, error_count, is_blocked) VALUES ($1, 0, FALSE)
                ON CONFLICT (source_url) DO UPDATE SET error_count = 0, is_blocked = FALSE;
            """, source_url)


async def update_source_block_status():
    """Розблоковує джерела, час блокування яких минув."""
    if not db_pool: return
    unlock_time = datetime.now(KYIV_TZ) - timedelta(hours=Config.SOURCE_BLOCK_DURATION_HOURS)
    async with db_pool.acquire() as conn:
        records = await conn.fetch("UPDATE source_stats SET is_blocked = FALSE, error_count = 0 WHERE is_blocked = TRUE AND last_error_at < $1 RETURNING source_url;", unlock_time)
        if records: logger.info(f"🔓 Розблоковано {len(records)} джерел.")


async def get_unique_news_from_db(limit: int) -> List[Dict[str, Any]]:
    """Вибирає свіжі, неопубліковані новини з пріоритетом за рейтингом, наявністю фото та свіжістю."""
    if not db_pool or limit == 0: return []
    sql = """
        SELECT source, url, title, summary, image_url, published_at
        FROM news
        WHERE is_posted = FALSE
        ORDER BY 
            score DESC,                                          -- 1. Важливість новини
            (CASE WHEN image_url IS NOT NULL THEN 0 ELSE 1 END), -- 2. Пріоритет фото
            published_at DESC                                    -- 3. Свіжість
        LIMIT $1;
    """
    try:
        async with db_pool.acquire() as conn:
            return [dict(r) for r in await conn.fetch(sql, limit)]
    except Exception as e:
        logger.error(f"❌ Помилка отримання новин з БД: {e}")
        return []


async def mark_news_as_posted(urls: List[str]):
    """Пакетне оновлення статусу 'is_posted'."""
    if not urls or not db_pool: return
    try:
        async with db_pool.acquire() as conn:
            await conn.execute("UPDATE news SET is_posted = TRUE WHERE url = ANY($1::text[]);", urls)
    except Exception as e:
        logger.error(f"❌ Помилка пакетного оновлення статусу: {e}")


async def cleanup_db():
    """Видаляє старі записи новин."""
    if not db_pool: return
    cutoff_time = datetime.now(KYIV_TZ) - timedelta(days=Config.DB_CLEANUP_DAYS)
    try:
        async with db_pool.acquire() as conn:
            result = await conn.execute("DELETE FROM news WHERE inserted_at < $1;", cutoff_time)
            deleted_count = int(result.split()[-1])
            if deleted_count > 0:
                logger.info(f"🗑️ Видалено {deleted_count} старих записів новин (старше {Config.DB_CLEANUP_DAYS} днів).")
    except Exception as e:
        logger.error(f"❌ Помилка очищення БД: {e}")


async def get_db_stats() -> Dict[str, int]:
    """Отримує загальну статистику для команд адміністратора."""
    if not db_pool: return {}
    sql = """
        SELECT 
            COUNT(*) AS total_news,
            COUNT(*) FILTER (WHERE is_posted = TRUE) AS posted_news,
            COUNT(*) FILTER (WHERE is_posted = FALSE) AS unposted_total,
            COUNT(*) FILTER (WHERE is_posted = FALSE AND image_url IS NOT NULL) AS unposted_with_image
        FROM news;
    """
    try:
        async with db_pool.acquire() as conn:
            record = await conn.fetchrow(sql)
            return dict(record) if record else {}
    except Exception as e:
        logger.error(f"❌ Помилка отримання статистики: {e}")
        return {}


# --- 4. PARSING HELPERS AND CONTENT PROCESSING ---

def calculate_news_score(title: str, summary: str) -> int:
    """Оцінює новину за ключовими словами для пріоритезації."""
    content = (title + ' ' + summary).lower()
    score = 0
    
    # Пріоритетні теми
    priority_keywords = {
        'зсу': 15, 'війна': 12, 'обстріл': 12, 'атака': 12, 'фронт': 12, 'ракета': 10, 'дрон': 10,
        'президент': 10, 'зеленський': 10, 'кабмін': 8, 'рада': 8, 'сбу': 8, 'гур': 8,
        'сша': 7, 'нато': 7, 'єс': 7, 'допомога': 7, 'санкції': 7,
        'київ': 5, 'львів': 5, 'харків': 5, 'одеса': 5, 'дніпро': 5
    }
    # Негативні/спамні теми
    negative_keywords = ['гороскоп', 'астрологічний', 'реклама', 'погода', 'рецепт', 'шоу-бізнес']
    
    if any(kw in content for kw in negative_keywords):
        return -1 # Сигнал для ігнорування новини
    
    for kw, value in priority_keywords.items():
        if kw in content:
            score += value
    return score


def normalize_summary(text: str) -> str:
    """Очищення та скорочення тексту для використання в Telegram."""
    if not text: return "Деталі за посиланням."
    soup = BeautifulSoup(text, 'html.parser')
    clean_text = re.sub(r'\s+', ' ', soup.get_text()).strip()
    if len(clean_text) > 450:
        clean_text = clean_text[:420]
        last_sentence_end = max(clean_text.rfind('.'), clean_text.rfind('!'), clean_text.rfind('?'))
        clean_text = clean_text[:last_sentence_end + 1] if last_sentence_end > 100 else clean_text + "..."
    return clean_text


def extract_image_url(entry: feedparser.FeedParserDict) -> str | None:
    """Вилучення URL зображення з різних полів RSS-запису."""
    targets = [
        # Пріоритет на якісні зображення
        ('media_content', lambda m: m.get('url') if m.get('medium') == 'image' else None),
        ('enclosures', lambda enc: enc.get('href') if enc.get('type', '').startswith('image/') else None),
        ('media_thumbnail', lambda thumb: thumb.get('url'))
    ]
    for key, extractor in targets:
        if key in entry:
            items = entry[key]
            if isinstance(items, list):
                for item in items:
                    url = extractor(item)
                    if url: return url
            elif isinstance(items, dict):
                url = extractor(items)
                if url: return url
    # Запасний варіант: парсинг HTML
    html_content = entry.get('content', [{}])[0].get('value') or entry.get('summary')
    if html_content:
        img = BeautifulSoup(html_content, 'html.parser').find('img')
        if img and img.get('src'): return img['src']
    return None


def parse_published_time(entry: feedparser.FeedParserDict) -> datetime:
    """Парсинг часу публікації та конвертація в Kyiv Time Zone."""
    if hasattr(entry, 'published_parsed') and entry.published_parsed:
        try:
            return datetime(*entry.published_parsed[:6], tzinfo=timezone.utc).astimezone(KYIV_TZ)
        except (ValueError, TypeError):
            pass
    return datetime.now(KYIV_TZ)


# --- 5. HASHTAG GENERATION (IMPROVED) ---

def generate_hashtags(title: str, source: str) -> str:
    """Генерує релевантні хештеги з нормалізацією слів."""
    
    stop_words = {'на', 'в', 'у', 'з', 'до', 'про', 'від', 'для', 'це', 'що', 'як', 'та', 'але', 'і', 'по', 'за', 'під', 'над', 'коли', 'буде', 'було', 'є', 'він', 'вона', 'вони'}
    
    # Карта для об'єднання багатослівних назв
    multi_word_map = {
        ('володимир', 'зеленський'): 'ВолодимирЗеленський', ('дмитро', 'кулеба'): 'ДмитроКулеба',
        ('валерій', 'залужний'): 'ВалерійЗалужний', ('кирило', 'буданов'): 'КирилоБуданов',
        ('олександр', 'сирський'): 'ОлександрСирський', ('джо', 'байден'): 'ДжоБайден',
        ('дональд', 'трамп'): 'ДональдТрамп', ('сполучені', 'штати'): 'США', ('велика', 'британія'): 'ВеликаБританія'
    }
    
    hashtags: Set[str] = set()
    
    # 1. Хештег джерела
    source_parts = urlparse(f"https://{source}").netloc.split('.')
    clean_source = source_parts[-2] if len(source_parts) > 1 else source_parts[0]
    hashtags.add(f"#{clean_source.capitalize()}")

    # 2. Обробка заголовка
    words = re.sub(r'[^\w\s-]', '', title).lower().split()
    
    i = 0
    while i < len(words):
        # Перевірка на багатослівні комбінації
        found_multi = False
        for (w1, w2), tag in multi_word_map.items():
            if i + 1 < len(words) and words[i] == w1 and words[i+1] == w2:
                hashtags.add(f"#{tag}")
                i += 2
                found_multi = True
                break
        if found_multi: continue

        word = words[i]
        if len(word) > 3 and word not in stop_words:
            # Нормалізація (приведення до початкової форми)
            p = morph.parse(word)[0]
            normal_form = p.normal_form
            if len(normal_form) > 2:
                 # Додаємо хештег, якщо це іменник або значуще слово
                if 'NOUN' in p.tag or (p.score > 0.5 and 'ADJF' not in p.tag and 'VERB' not in p.tag):
                    hashtags.add(f"#{normal_form.capitalize()}")
        i += 1

    final_tags = ["#Новини"] + sorted(list(hashtags), key=len, reverse=True)
    return " ".join(final_tags[:7]) # Обмеження на 7 хештегів


# --- 6. CORE PARSING LOGIC ---

async def fetch_and_parse_source(session: aiohttp.ClientSession, rss_url: str) -> List[Dict[str, Any]]:
    """Отримує, парсить та фільтрує новини з одного RSS-джерела з повторними спробами."""
    news_items = []
    source_domain = urlparse(rss_url).netloc.replace('www.', '')
    
    for attempt in range(Config.MAX_RETRIES):
        try:
            async with session.get(rss_url, headers=Config.DEFAULT_HEADERS, timeout=Config.HTTP_TIMEOUT) as response:
                if response.status == 200:
                    await update_source_error_count(rss_url, is_error=False)
                    content = await response.text()
                    feed = feedparser.parse(content)
                    
                    now_kyiv = datetime.now(KYIV_TZ)
                    max_age_dt = timedelta(minutes=Config.MAX_AGE_MIN)

                    for entry in feed.entries[:Config.FETCH_LIMIT]:
                        published_time = parse_published_time(entry)
                        if now_kyiv - published_time > max_age_dt: continue

                        title = entry.title.strip()
                        summary = normalize_summary(entry.get('summary') or entry.get('description') or title)
                        score = calculate_news_score(title, summary)
                        
                        # PRE-POSTING VALIDATION
                        if score < 0 or len(title) < Config.MIN_TITLE_LENGTH: continue
                        
                        news_items.append({
                            'source': source_domain, 'title': title, 'url': entry.link,
                            'summary': summary, 'image_url': extract_image_url(entry),
                            'published_at': published_time, 'score': score
                        })
                    return news_items
                
                elif response.status in Config.BLOCKED_HTTP_CODES:
                    logger.warning(f"⚠️ HTTP Помилка {response.status} для {rss_url}. Блокую...")
                    await update_source_error_count(rss_url, is_error=True, http_code=response.status)
                    return []
                else:
                    logger.warning(f"⚠️ HTTP Помилка {response.status} ({attempt+1}/{Config.MAX_RETRIES}) для {rss_url}.")
                    
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.warning(f"❌ Помилка мережі ({attempt+1}/{Config.MAX_RETRIES}) для {rss_url}: {type(e).__name__}.")
        
        if attempt < Config.MAX_RETRIES - 1:
            await asyncio.sleep(Config.RETRY_DELAY_SEC * (2 ** attempt))
            
    await update_source_error_count(rss_url, is_error=True, http_code=599) # 599 - Network Connect Timeout Error
    return []


async def fetch_all_sources() -> Tuple[List[Dict[str, Any]], float]:
    """Запускає одночасний парсинг активних джерел."""
    start_time = datetime.now()
    active_sources_urls = await get_active_sources_from_db()
    
    num_to_fetch = min(Config.NUM_SOURCES_TO_FETCH, len(active_sources_urls))
    selected_sources = random.sample(list(active_sources_urls), num_to_fetch)
    logger.info(f"⏳ Парсинг {len(selected_sources)} випадкових активних джерел...")

    connector = aiohttp.TCPConnector(limit_per_host=5, limit=Config.MAX_CONCURRENCY, ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [fetch_and_parse_source(session, url) for url in selected_sources]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    all_news = [item for res in results if isinstance(res, list) for item in res]
    duration = (datetime.now() - start_time).total_seconds()
    return all_news, duration


# --- 7. FORMATTING AND POSTING ---

def format_news_post(news_item: Dict[str, Any]) -> str:
    """Форматує новину для Telegram, включаючи якісні хештеги."""
    source_display = news_item['source']
    hashtags = generate_hashtags(news_item['title'], source_display)
    
    return (
        f"<b>⚡️ {news_item['title']}</b>\n\n"
        f"{news_item['summary']}\n\n"
        f"<a href='{news_item['url']}'>Подробиці на {source_display}</a>"
        f"\n\n{hashtags}"
    )


async def send_news_to_channel(news_to_post: List[Dict[str, Any]]) -> int:
    """Надсилає новини в Telegram-канал з безпечним оновленням статусу DB."""
    posted_urls = []
    for news in news_to_post:
        try:
            caption = format_news_post(news)
            if news.get('image_url'):
                await bot.send_photo(CHANNEL_ID, photo=news['image_url'], caption=caption)
            else:
                await bot.send_message(CHANNEL_ID, text=caption, disable_web_page_preview=True)
            
            posted_urls.append(news['url'])
            await asyncio.sleep(1.5) # Пауза для уникнення FloodWait
            
        except TelegramAPIError as e:
            logger.error(f"❌ Telegram API Error для '{news['title'][:50]}...': {e.message}")
            if "failed to get HTTP URL content" in e.message or "PHOTO_INVALID" in e.message:
                logger.warning("-> Проблема з URL зображення. Позначаємо як опубліковану, щоб не повторювати помилку.")
                posted_urls.append(news['url']) # Позначаємо, щоб уникнути циклічної помилки
            continue
        except Exception as e:
            logger.error(f"❌ Невідома помилка відправки для '{news['title'][:50]}...': {e}")
            continue

    if posted_urls:
        await mark_news_as_posted(posted_urls)
    return len(posted_urls)


# --- 8. LOOPS AND ADMIN COMMANDS ---

async def db_maintenance_loop():
    """Асинхронний цикл для періодичного очищення БД та оновлення джерел."""
    while True:
        await asyncio.sleep(Config.CLEANUP_INTERVAL_HOURS * 3600)
        logger.info("--- ♻️ Запуск фонового обслуговування БД ---")
        await cleanup_db()
        await update_source_block_status()


async def auto_posting_loop():
    """Головний цикл, який періодично перевіряє та публікує новини."""
    global current_post_limit
    wait_time = Config.POSTING_INTERVAL_MIN * 60
    current_post_limit = 0 # Починаємо з 0 для поступового "розігріву"
    
    while True:
        try:
            logger.info("--- 🚀 Запуск циклу автопостингу ---")
            
            fetched_news, parse_duration = await fetch_all_sources()
            new_count = await save_news_with_transaction(fetched_news)
            
            # Динамічний ліміт: 1 -> 2 -> 3. Це "розігрів" після запуску.
            current_post_limit = min(current_post_limit + 1, Config.MAX_NEWS_PER_CYCLE)
            
            news_to_post = await get_unique_news_from_db(current_post_limit)
            
            post_start_time = datetime.now()
            posted_count = await send_news_to_channel(news_to_post)
            post_duration = (datetime.now() - post_start_time).total_seconds()
            
            logger.info(
                f"--- ✅ Цикл завершено. Нових: {new_count}. Ліміт: {current_post_limit}. Опубліковано: {posted_count}. "
                f"Таймінги: Парсинг={parse_duration:.2f}с, Постинг={post_duration:.2f}с ---"
            )
            
        except Exception as e:
            logger.critical(f"❌ Критична помилка в циклі автопостингу: {e}", exc_info=True)

        await asyncio.sleep(wait_time)


# --- ADMIN COMMANDS ---

async def cmd_status(message: types.Message):
    """Показує поточний статус бота та конфігурацію."""
    stats = await get_db_stats()
    active_sources = await get_active_sources_from_db()
    status_msg = (
        f"<b>🤖 Статус Платформи Новин</b>\n\n"
        f"<b>⚙️ Конфігурація:</b>\n"
        f"  - Інтервал: <b>{Config.POSTING_INTERVAL_MIN} хв</b>\n"
        f"  - Макс. вік новини: {Config.MAX_AGE_MIN} хв\n"
        f"  - Макс. постів за цикл: <b>{Config.MAX_NEWS_PER_CYCLE}</b> (поточний: <b>{current_post_limit}</b>)\n"
        f"  - Джерел: <b>{len(active_sources)}</b> активних / {len(Config.SOURCES)} всього\n\n"
        f"📊 <b>Статистика DB:</b>\n"
        f"  - Всього новин: {stats.get('total_news', 0)}\n"
        f"  - Опубліковано: {stats.get('posted_news', 0)}\n"
        f"  - У черзі: {stats.get('unposted_total', 0)} (з фото: {stats.get('unposted_with_image', 0)})"
    )
    await message.answer(status_msg)


async def cmd_forcepost(message: types.Message):
    """Примусово запускає один повний цикл парсингу та постингу."""
    await message.answer("⏳ Примусовий запуск циклу... Це може зайняти до хвилини.")
    
    try:
        fetched_news, p_dur = await fetch_all_sources()
        new_count = await save_news_with_transaction(fetched_news)
        limit = Config.MAX_NEWS_PER_CYCLE # Примусовий пост завжди з максимальним лімітом
        news_to_post = await get_unique_news_from_db(limit)
        posted_count = await send_news_to_channel(news_to_post)
        
        result_msg = (
            f"✅ <b>Цикл примусового постингу завершено!</b>\n"
            f"  - Знайдено нових новин: <b>{new_count}</b>\n"
            f"  - Опубліковано новин: <b>{posted_count}</b> (ліміт: {limit})\n"
            f"  - Час парсингу: {p_dur:.2f} сек"
        )
    except Exception as e:
        logger.error(f"Помилка примусового постингу: {e}", exc_info=True)
        result_msg = f"❌ <b>Критична помилка примусового постингу:</b> {e}"
    
    await message.answer(result_msg)


async def cmd_blocked(message: types.Message):
    """Показує список заблокованих джерел."""
    if not db_pool:
        await message.answer("❌ База даних недоступна.")
        return
    
    async with db_pool.acquire() as conn:
        records = await conn.fetch("SELECT source_url, last_error_at FROM source_stats WHERE is_blocked = TRUE ORDER BY last_error_at DESC;")
    
    if not records:
        await message.answer("✅ Всі джерела активні. Заблокованих немає.")
        return
        
    response_lines = ["<b>🚨 Список заблокованих джерел:</b>"]
    for r in records:
        time_ago = datetime.now(KYIV_TZ) - r['last_error_at']
        hours_ago = time_ago.total_seconds() / 3600
        response_lines.append(f"  - <code>{r['source_url']}</code>\n    (остання помилка: {hours_ago:.1f} год тому)")
        
    await message.answer("\n".join(response_lines))


# --- 9. BOT LAUNCH ---

async def on_startup(bot_instance: Bot):
    """Дії при старті: підключення до БД, ініціалізація, встановлення вебхука."""
    await connect_db()
    if not db_pool: return
    await init_db()

    await bot_instance.set_webhook(
        url=WEBHOOK_URL,
        secret_token=WEBHOOK_SECRET,
        drop_pending_updates=True
    )
    logger.info(f"✅ Webhook встановлено на: {WEBHOOK_URL}")
    
    # Запуск фонових циклів
    loop = asyncio.get_event_loop()
    loop.create_task(auto_posting_loop())
    loop.create_task(db_maintenance_loop())
    logger.info("🚀 Бот запущено. Початок роботи (WEBHOOK MODE).")


async def on_shutdown(bot_instance: Bot):
    """Дії при зупинці: закриття з'єднань."""
    logger.info("Бот зупиняється...")
    if bot_instance:
        await bot_instance.delete_webhook(drop_pending_updates=True)
        await bot_instance.session.close()
        logger.info("Webhook успішно вимкнено.")
    if db_pool:
        await db_pool.close()
        logger.info("Пул з'єднань до БД закрито.")


def main():
    """Основна функція для ініціалізації та запуску бота через Webhook."""
    if not all([BOT_TOKEN, DATABASE_URL, CHANNEL_ID, WEBHOOK_HOST, WEBHOOK_SECRET, ADMIN_ID]):
        logger.critical("Критична помилка: Не задані всі необхідні змінні середовища.")
        return

    global bot, dp
    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher()

    # Реєстрація хуків життєвого циклу
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)
    
    # Реєстрація команд адміністратора
    admin_filter = F.from_user.id == ADMIN_ID
    dp.message.register(cmd_status, Command("status"), admin_filter)
    dp.message.register(cmd_forcepost, Command("forcepost"), admin_filter)
    dp.message.register(cmd_blocked, Command("blocked"), admin_filter)

    # Налаштування та запуск веб-сервера aiohttp
    app = web.Application()
    SimpleRequestHandler(dispatcher=dp, bot=bot, secret_token=WEBHOOK_SECRET).register(app, path=WEBHOOK_PATH)
    web.run_app(app, host=WEB_SERVER_HOST, port=WEB_SERVER_PORT)


if __name__ == "__main__":
    try:
        main()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Бот зупинено.")
    except Exception as e:
        logger.critical(f"❌ Головна помилка виконання: {e}", exc_info=True)
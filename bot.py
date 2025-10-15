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

from aiogram import Bot, Dispatcher, types, F
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.client.default import DefaultBotProperties
from aiogram.exceptions import TelegramAPIError
from aiogram.webhook.aiohttp_server import SimpleRequestHandler

# --- 0. ЗАВАНТАЖЕННЯ ЗМІННИХ СЕРЕДОВИЩА ---
load_dotenv()

# --- 1. НАЛАШТУВАННЯ І КОНСТАНТИ ---

# Використовуйте Kyiv time zone (UTC+3)
KYIV_TZ = timezone(timedelta(hours=3), 'Europe/Kyiv')

class Config:
    """Конфігурація платформи, зібрана в одному місці."""

    # ⚙️ ОСНОВНІ ПАРАМЕТРИ ЦИКЛУ
    POSTING_INTERVAL_MIN = 4   # Інтервал запуску циклу (зменшено для оперативності)
    MAX_NEWS_PER_CYCLE = 4     # Максимальна кількість новин за один цикл
    MAX_AGE_MIN = 45           # Не публікувати новини старше 45 хвилин

    # 🛡️ ПАРАМЕТРИ НАДІЙНОСТІ ТА ПРОДУКТИВНОСТІ
    FETCH_LIMIT = 25           # Макс. кількість записів для обробки з одного RSS-фіда
    NUM_SOURCES_TO_FETCH = 20  # Кількість випадкових джерел, які парсяться за цикл
    HTTP_TIMEOUT = 12          # Таймаут для HTTP-запитів (збільшено)
    MAX_CONCURRENCY = 25       # Макс. одночасних з'єднань для парсингу
    MAX_RETRIES = 2            # Макс. кількість повторних спроб для HTTP-запиту
    RETRY_DELAY_SEC = 3        # Початкова затримка повтору

    # 💾 ПАРАМЕТРИ ОБСЛУГОВУВАННЯ БАЗИ ДАНИХ
    DB_POOL_MIN = 2            # Мінімальний розмір пулу
    DB_POOL_MAX = 7            # Максимальний розмір пулу (для обробки пікових навантажень)
    DB_CLEANUP_DAYS = 5        # Видаляти записи новин старше 5 днів
    CLEANUP_INTERVAL_HOURS = 2 # Частота очистки БД

    # ❌ ПАРАМЕТРИ БЛОКУВАННЯ ДЖЕРЕЛ
    BLOCKED_HTTP_CODES = [403, 404, 500, 503]
    SOURCE_BLOCK_THRESHOLD = 5 # Кількість помилок, після яких джерело блокується
    SOURCE_BLOCK_DURATION_HOURS = 3 # На скільки годин блокувати джерело

    # 🔍 ПАРАМЕТРИ УНІКАЛЬНОСТІ ТА ЯКОСТІ
    SIMILARITY_THRESHOLD = 0.85 # Поріг схожості тексту для визначення дублікатів (85%)
    MIN_TITLE_LEN = 20         # Мінімальна довжина заголовка
    MIN_SUMMARY_LEN = 50       # Мінімальна довжина опису

    DEFAULT_HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36 UkrainianNewsBot/1.0',
        'Accept': 'application/rss+xml,application/xml,text/html;q=0.9',
        'Accept-Language': 'uk-UA,uk;q=0.9,en-US;q=0.8,en;q=0.7',
    }

    # 📰 Джерела новин (оновлений список)
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
        "https://ua.interfax.com.ua/news/ukraine.rss", "https://babel.ua/rss", # <--- Оновлено
        "https://hromadske.ua/feed/news", "https://biz.censor.net/rss",
        "https://slovoidilo.ua/rss/index.xml", "https://apostrophe.ua/rss"
    ]

# Налаштування логування
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- WEBHOOK І СЕРВІСНІ ЗМІННІ СЕРЕДОВИЩА ---
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

# Глобальні змінні
db_pool: asyncpg.Pool = None
dp: Dispatcher = None
bot: Bot = None
current_post_limit: int = Config.MAX_NEWS_PER_CYCLE # Починаємо з максимального ліміту


# --- 2. БАЗА ДАНИХ (POSTGRESQL/NEON) ---

async def connect_db():
    """Створює пул з'єднань до PostgreSQL."""
    global db_pool
    if not DATABASE_URL:
        logger.critical("Критична помилка: Не задано DATABASE_URL.")
        return
    try:
        db_pool = await asyncpg.create_pool(
            DATABASE_URL,
            min_size=Config.DB_POOL_MIN,
            max_size=Config.DB_POOL_MAX,
            command_timeout=30
        )
        logger.info(f"✅ Успішно підключено до PostgreSQL. Пул: {Config.DB_POOL_MIN}-{Config.DB_POOL_MAX}.")
    except Exception as e:
        logger.error(f"❌ Критична помилка підключення до DB: {e}", exc_info=True)
        await asyncio.sleep(60)
        exit(1)

async def init_db():
    """Ініціалізує таблиці 'news' та 'source_stats'."""
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
                post_vector tsvector
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
            await conn.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
            await conn.execute("CREATE INDEX IF NOT EXISTS news_url_idx ON news (url);")
            await conn.execute("CREATE INDEX IF NOT EXISTS news_is_posted_idx ON news (is_posted, published_at);")
            await conn.execute("CREATE INDEX IF NOT EXISTS news_post_vector_idx ON news USING GIN(post_vector);")
        except Exception as e:
            logger.warning(f"Помилка при створенні індексу або розширення: {e}")
    logger.info("Таблиці DB перевірені/оновлені.")

async def save_news_with_transaction(news_items: List[Dict[str, Any]]) -> int:
    """Виконує пакетну вставку новин в одній транзакції."""
    if not news_items or not db_pool: return 0
    sql = """
        INSERT INTO news (source, url, title, summary, image_url, published_at, post_vector)
        SELECT * FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::timestamptz[], $7::tsvector[])
        ON CONFLICT (url) DO NOTHING;
    """
    data = ([], [], [], [], [], [], [])
    for item in news_items:
        data[0].append(item['source'])
        data[1].append(item['url'])
        data[2].append(item['title'])
        data[3].append(item['summary'])
        data[4].append(item['image_url'])
        data[5].append(item['published_at'])
        # Створюємо tsvector для майбутнього повнотекстового пошуку
        data[6].append(f"to_tsvector('simple', '{item['title'].replace('''', '')} {item['summary'].replace('''', '')}')")

    try:
        async with db_pool.acquire() as conn:
            async with conn.transaction():
                result = await conn.execute(sql, *data)
                return int(result.split()[-1])
    except Exception as e:
        logger.error(f"❌ Помилка транзакційної вставки в БД: {e}", exc_info=True)
        return 0

async def get_active_sources_from_db() -> Set[str]:
    """Вибирає лише активні (не заблоковані) джерела."""
    if not db_pool: return set(Config.SOURCES)
    await update_source_block_status()
    sql = "SELECT source_url FROM source_stats WHERE is_blocked = FALSE;"
    try:
        async with db_pool.acquire() as conn:
            active_records = await conn.fetch(sql)
            active_urls_from_db = {r['source_url'] for r in active_records}
            
            # Додаємо нові джерела з конфігу, яких ще немає в базі
            all_config_sources = set(Config.SOURCES)
            new_sources = all_config_sources - (await get_all_sources_from_db(conn))
            if new_sources:
                insert_sql = "INSERT INTO source_stats (source_url) VALUES ($1) ON CONFLICT (source_url) DO NOTHING;"
                await conn.executemany(insert_sql, [(url,) for url in new_sources])
                logger.info(f"Додано {len(new_sources)} нових джерел у статистику.")
                active_urls_from_db.update(new_sources)

            active_sources = all_config_sources.intersection(active_urls_from_db)
            logger.info(f"Активних джерел: {len(active_sources)}. Заблокованих: {len(all_config_sources) - len(active_sources)}")
            return active_sources
    except Exception as e:
        logger.error(f"❌ Помилка отримання активних джерел: {e}")
        return set(Config.SOURCES)

async def get_all_sources_from_db(conn: asyncpg.Connection) -> Set[str]:
    """Допоміжна функція для отримання всіх джерел з таблиці статистики."""
    return {r['source_url'] for r in await conn.fetch("SELECT source_url FROM source_stats;")}

async def update_source_error_count(source_url: str, is_error: bool, http_code: int = None):
    """Оновлює статистику помилок для джерела."""
    if not db_pool: return
    async with db_pool.acquire() as conn:
        if is_error:
            if http_code in Config.BLOCKED_HTTP_CODES:
                await conn.execute("""
                    INSERT INTO source_stats (source_url, error_count, last_error_at) VALUES ($1, 1, NOW())
                    ON CONFLICT (source_url) DO UPDATE SET error_count = source_stats.error_count + 1, last_error_at = NOW();
                """, source_url)
                record = await conn.fetchrow("SELECT error_count FROM source_stats WHERE source_url = $1;", source_url)
                if record and record['error_count'] >= Config.SOURCE_BLOCK_THRESHOLD:
                    await conn.execute("UPDATE source_stats SET is_blocked = TRUE WHERE source_url = $1;", source_url)
                    logger.warning(f"🚨 Джерело заблоковано: {source_url}. Помилок: {record['error_count']}.")
        else:
            await conn.execute("""
                UPDATE source_stats SET error_count = 0, is_blocked = FALSE WHERE source_url = $1;
            """, source_url)

async def update_source_block_status():
    """Розблоковує джерела, час блокування яких минув."""
    if not db_pool: return
    unlock_time = datetime.now(KYIV_TZ) - timedelta(hours=Config.SOURCE_BLOCK_DURATION_HOURS)
    sql = "UPDATE source_stats SET is_blocked = FALSE, error_count = 0 WHERE is_blocked = TRUE AND last_error_at < $1 RETURNING source_url;"
    async with db_pool.acquire() as conn:
        records = await conn.fetch(sql, unlock_time)
        if records:
            logger.info(f"🔓 Розблоковано {len(records)} джерел.")

async def get_unique_news_from_db(limit: int) -> List[Dict[str, Any]]:
    """Вибирає свіжі, неопубліковані новини з пріоритетом для тих, що мають зображення."""
    if not db_pool or limit <= 0: return []
    sql = """
        SELECT id, source, url, title, summary, image_url, published_at
        FROM news
        WHERE is_posted = FALSE AND published_at >= $1
        ORDER BY (CASE WHEN image_url IS NOT NULL THEN 0 ELSE 1 END), published_at DESC
        LIMIT $2;
    """
    cutoff_time = datetime.now(KYIV_TZ) - timedelta(minutes=Config.MAX_AGE_MIN * 2)
    try:
        async with db_pool.acquire() as conn:
            records = await conn.fetch(sql, cutoff_time, limit * 5) # Беремо більше для аналізу
            
            # Фільтруємо на унікальність контенту
            unique_news = []
            for record in records:
                if await is_news_unique(dict(record)):
                    unique_news.append(dict(record))
                    if len(unique_news) >= limit:
                        break
            return unique_news
    except Exception as e:
        logger.error(f"❌ Помилка отримання новин з БД: {e}", exc_info=True)
        return []

async def is_news_unique(news_item: Dict[str, Any]) -> bool:
    """Перевіряє унікальність новини за допомогою схожості тексту (pg_trgm)."""
    if not db_pool: return True
    sql = """
        SELECT title, similarity(title, $1) as title_sim
        FROM news
        WHERE is_posted = TRUE AND inserted_at > NOW() - INTERVAL '12 hours'
        AND title % $1
        ORDER BY title_sim DESC
        LIMIT 5;
    """
    try:
        async with db_pool.acquire() as conn:
            similar_records = await conn.fetch(sql, news_item['title'])
            for r in similar_records:
                if r['title_sim'] > Config.SIMILARITY_THRESHOLD:
                    logger.info(f"🔍 Знайдено схожу новину (схожість: {r['title_sim']:.2f}). Пропуск: '{news_item['title'][:50]}...'")
                    return False
            return True
    except Exception as e:
        logger.error(f"❌ Помилка перевірки унікальності: {e}")
        return True # У разі помилки краще опублікувати

async def mark_news_as_posted(urls: List[str]):
    """Пакетне оновлення статусу 'is_posted'."""
    if not urls or not db_pool: return
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE news SET is_posted = TRUE WHERE url = ANY($1::text[]);", urls)

async def cleanup_db():
    """Видаляє старі записи новин."""
    if not db_pool: return
    cutoff = datetime.now(KYIV_TZ) - timedelta(days=Config.DB_CLEANUP_DAYS)
    try:
        async with db_pool.acquire() as conn:
            result = await conn.execute("DELETE FROM news WHERE inserted_at < $1;", cutoff)
            count = int(re.search(r'\d+$', result).group())
            if count > 0:
                logger.info(f"🗑️ Видалено {count} старих записів новин.")
    except Exception as e:
        logger.error(f"❌ Помилка очищення БД: {e}", exc_info=True)

# --- 3. ХЕЛПЕРИ ПАРСИНГУ ---

def is_valid_for_posting(news_item: Dict[str, Any]) -> bool:
    """Перевірка якості новини перед постингом."""
    # Перевірка довжини
    if len(news_item['title']) < Config.MIN_TITLE_LEN or len(news_item['summary']) < Config.MIN_SUMMARY_LEN:
        return False
    # Перевірка на "погані" зображення
    if img_url := news_item.get('image_url'):
        bad_patterns = ['logo', 'placeholder', 'default', 'no-image', '.gif']
        if any(p in img_url.lower() for p in bad_patterns):
            news_item['image_url'] = None # Просто видаляємо погане фото
    return True

def normalize_summary(text: str) -> str:
    """Очищення та скорочення тексту."""
    if not text: return "Деталі за посиланням."
    soup = BeautifulSoup(text, 'html.parser')
    clean_text = re.sub(r'\s+', ' ', soup.get_text()).strip()
    if len(clean_text) > 450:
        clean_text = clean_text[:447]
        last_sentence_end = max(clean_text.rfind('.'), clean_text.rfind('!'), clean_text.rfind('?'))
        if last_sentence_end > 0:
            clean_text = clean_text[:last_sentence_end + 1]
        else:
            clean_text += "..."
    return clean_text

def extract_image_url(entry: feedparser.FeedParserDict) -> str | None:
    """Вилучення URL зображення з RSS."""
    for key in ['enclosures', 'media_content', 'media_thumbnail']:
        if key in entry:
            items = entry[key]
            if isinstance(items, list):
                for item in items:
                    if item.get('type', '').startswith('image/') or item.get('medium') == 'image':
                        return item.get('href') or item.get('url')
            elif isinstance(items, dict) and (items.get('type', '').startswith('image/') or items.get('medium') == 'image'):
                return items.get('href') or items.get('url')
    html_content = entry.get('content', [{}])[0].get('value') or entry.get('summary')
    if html_content and (img := BeautifulSoup(html_content, 'html.parser').find('img')):
        return img.get('src')
    return None

def parse_published_time(entry: feedparser.FeedParserDict) -> datetime:
    """Парсинг часу публікації та конвертація в Kyiv Time Zone."""
    if hasattr(entry, 'published_parsed') and entry.published_parsed:
        try:
            return datetime(*entry.published_parsed[:6], tzinfo=timezone.utc).astimezone(KYIV_TZ)
        except Exception:
            pass
    return datetime.now(KYIV_TZ)


# --- 4. ГЕНЕРАЦІЯ ХЕШТЕГІВ (ПРОФЕСІЙНА ВЕРСІЯ) ---

NORMALIZATION_MAP = {
    'києва': 'Київ', 'києві': 'Київ', 'києвом': 'Київ',
    'україни': 'Україна', 'україні': 'Україна', 'україн': 'Україна',
    'росії': 'Росія', 'росію': 'Росія', 'росією': 'Росія',
    'сша': 'США',
    'трампа': 'Трамп', 'трампу': 'Трамп', 'трампом': 'Трамп',
    'байдена': 'Байден', 'байдену': 'Байден', 'байденом': 'Байден',
    'зеленського': 'Зеленський', 'зеленському': 'Зеленський',
    'путіна': 'Путін', 'путіну': 'Путін', 'путіним': 'Путін',
    'пентагону': 'Пентагон', 'пентагоні': 'Пентагон',
    'сенату': 'Сенат', 'сенаті': 'Сенат',
    'китаю': 'Китай', 'китаєм': 'Китай',
    'тернопільщини': 'Тернопіль', 'тернополя': 'Тернопіль',
    'москви': 'Москва', 'москвою': 'Москва',
    'валенсії': 'Валенсія',
    'зсу': 'ЗСУ', 'сбу': 'СБУ', 'гур': 'ГУР', 'нато': 'НАТО', 'єс': 'ЄС', 'оон': 'ООН',
    'ради': 'Рада', 'радою': 'Рада',
    'кабміну': 'Кабмін', 'кабміном': 'Кабмін'
}

def generate_hashtags(title: str, source: str) -> str:
    """Генерує релевантні хештеги на основі заголовка та джерела."""
    stop_words = {'на', 'в', 'у', 'з', 'до', 'про', 'від', 'для', 'це', 'що', 'як', 'та', 'і', 'а', 'але', 'по', 'за', 'під', 'над', 'буде', 'було', 'є', 'він', 'вона', 'вони', 'ми', 'ви', 'тисяч', 'млн', 'млрд', 'може', 'через', 'проти', 'новини', 'головне', 'стало', 'відомо'}
    priority_keywords = {'ЗСУ', 'СБУ', 'ГУР', 'НАТО', 'ЄС', 'ООН', 'Рада', 'Кабмін', 'Президент', 'Зеленський', 'Сирський', 'Буданов', 'Путін', 'Байден', 'Трамп', 'США', 'Україна', 'Росія', 'Київ', 'Львів', 'Харків', 'Одеса', 'Дніпро', 'Херсон', 'війна'}
    
    hashtags = {"#Новини"}
    
    # Хештег джерела
    source_domain = urlparse(f"https://{source}").netloc.split('.')
    clean_source = source_domain[-2] if len(source_domain) > 1 else source_domain[0]
    hashtags.add(f"#{clean_source.capitalize()}")

    clean_title = re.sub(r'[^\w\s]', ' ', title).lower()
    words = clean_title.split()
    
    found_entities = set()
    i = 0
    while i < len(words):
        word = words[i]
        # Перевірка на багатослівні сутності (напр. Джо Байден)
        if i + 1 < len(words) and f"{word} {words[i+1]}" in NORMALIZATION_MAP:
            entity = NORMALIZATION_MAP[f"{word} {words[i+1]}"]
            if entity not in found_entities:
                hashtags.add(f"#{entity.replace(' ', '')}")
                found_entities.add(entity)
            i += 2
            continue
        
        # Перевірка по словнику нормалізації
        if word in NORMALIZATION_MAP:
            entity = NORMALIZATION_MAP[word]
            if entity not in found_entities:
                hashtags.add(f"#{entity}")
                found_entities.add(entity)
        # Перевірка на пріоритетні слова та слова з великої літери
        elif word not in stop_words and len(word) > 3:
            original_word_match = re.search(r'\b' + re.escape(word) + r'\b', title, re.IGNORECASE)
            if original_word_match:
                original_word = original_word_match.group(0)
                if original_word.capitalize() in priority_keywords or (original_word[0].isupper() and original_word.lower() not in stop_words):
                    normalized = original_word.capitalize().replace('-', '')
                    if normalized not in found_entities:
                        hashtags.add(f"#{normalized}")
                        found_entities.add(normalized)
        i += 1
        
    return " ".join(list(hashtags)[:7]) # Обмеження до 7 хештегів


# --- 5. ОСНОВНИЙ ПАРСИНГ ---

async def fetch_and_parse_source(session: aiohttp.ClientSession, rss_url: str) -> List[Dict[str, Any]]:
    """Отримує, парсить та фільтрує новини з одного RSS-джерела."""
    news_items = []
    source_domain = urlparse(rss_url).netloc.replace('www.', '')
    for attempt in range(Config.MAX_RETRIES):
        try:
            async with session.get(rss_url, timeout=Config.HTTP_TIMEOUT) as response:
                if response.status == 200:
                    await update_source_error_count(rss_url, is_error=False)
                    content = await response.text()
                    feed = feedparser.parse(content)
                    max_age = timedelta(minutes=Config.MAX_AGE_MIN)
                    for entry in feed.entries[:Config.FETCH_LIMIT]:
                        published_time = parse_published_time(entry)
                        if datetime.now(KYIV_TZ) - published_time > max_age:
                            continue
                        news_item = {
                            'source': source_domain,
                            'title': entry.title.strip(),
                            'url': entry.link,
                            'summary': normalize_summary(entry.get('summary') or entry.get('description')),
                            'image_url': extract_image_url(entry),
                            'published_at': published_time,
                        }
                        if is_valid_for_posting(news_item):
                            news_items.append(news_item)
                    return news_items
                else:
                    await update_source_error_count(rss_url, is_error=True, http_code=response.status)
                    if response.status in Config.BLOCKED_HTTP_CODES:
                        return [] # Не повторювати спроби для цих кодів
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.warning(f"❌ Помилка мережі ({attempt+1}/{Config.MAX_RETRIES}) для {rss_url}: {e}")
            if attempt == Config.MAX_RETRIES - 1:
                await update_source_error_count(rss_url, is_error=True, http_code=599)
        await asyncio.sleep(Config.RETRY_DELAY_SEC * (attempt + 1))
    return []

async def fetch_all_sources() -> Tuple[List[Dict[str, Any]], float]:
    """Запускає одночасний парсинг активних джерел."""
    start_time = datetime.now()
    active_sources = await get_active_sources_from_db()
    if not active_sources:
        logger.warning("Немає активних джерел для парсингу.")
        return [], 0
    
    num_to_fetch = min(Config.NUM_SOURCES_TO_FETCH, len(active_sources))
    selected_sources = random.sample(list(active_sources), num_to_fetch)
    logger.info(f"⏳ Парсинг {len(selected_sources)}/{len(active_sources)} випадкових активних джерел...")
    
    all_news = []
    connector = aiohttp.TCPConnector(limit_per_host=5, limit=Config.MAX_CONCURRENCY, ssl=False)
    async with aiohttp.ClientSession(headers=Config.DEFAULT_HEADERS, connector=connector) as session:
        tasks = [fetch_and_parse_source(session, url) for url in selected_sources]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for res in results:
            if isinstance(res, list):
                all_news.extend(res)
            elif isinstance(res, Exception):
                logger.error(f"Помилка під час виконання завдання парсингу: {res}")
    
    duration = (datetime.now() - start_time).total_seconds()
    return all_news, duration

# --- 6. ФОРМАТУВАННЯ ТА ПОСТИНГ ---

def format_news_post(news_item: Dict[str, Any]) -> str:
    """Форматує новину для Telegram."""
    title = news_item['title']
    summary = news_item['summary']
    url = news_item['url']
    source = news_item['source']
    hashtags = generate_hashtags(title, source)
    return (
        f"<b>⚡️ {title}</b>\n\n"
        f"{summary}\n\n"
        f"<a href='{url}'>Подробиці на {source}</a>\n\n"
        f"{hashtags}"
    )

async def send_news_to_channel(news_to_post: List[Dict[str, Any]]) -> int:
    """Надсилає відфільтровані та унікальні новини в Telegram-канал."""
    posted_urls = []
    posted_count = 0
    for news in news_to_post:
        try:
            caption = format_news_post(news)
            if news.get('image_url'):
                await bot.send_photo(
                    chat_id=CHANNEL_ID,
                    photo=news['image_url'],
                    caption=caption,
                    parse_mode=ParseMode.HTML
                )
            else:
                await bot.send_message(
                    chat_id=CHANNEL_ID,
                    text=caption,
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True
                )
            posted_urls.append(news['url'])
            posted_count += 1
            await asyncio.sleep(2) # Пауза для уникнення FloodWait
        except TelegramAPIError as e:
            logger.error(f"❌ Telegram API Error для '{news['title'][:30]}...': {e.message}")
            if "failed to get HTTP URL content" in e.message or "PHOTO_INVALID" in e.message:
                logger.warning("-> Проблема з URL зображення. Позначаємо як опубліковану, щоб не повторювати.")
                posted_urls.append(news['url']) # Уникаємо повторної спроби з битим фото
        except Exception as e:
            logger.error(f"❌ Невідома помилка відправки для '{news['title'][:30]}...': {e}", exc_info=True)
    
    if posted_urls:
        await mark_news_as_posted(posted_urls)
    return posted_count

# --- 7. ЦИКЛИ ТА КОМАНДИ АДМІНІСТРАТОРА ---

async def db_cleanup_loop():
    """Фоновий цикл для очищення бази даних."""
    while True:
        await asyncio.sleep(Config.CLEANUP_INTERVAL_HOURS * 3600)
        logger.info("--- ♻️ Запуск фонової очистки БД ---")
        await cleanup_db()

async def auto_posting_loop(bot_instance: Bot):
    """Головний цикл, який періодично перевіряє та публікує новини."""
    wait_time = Config.POSTING_INTERVAL_MIN * 60
    while True:
        try:
            logger.info("--- 🚀 Запуск циклу автопостингу ---")
            # 1. Парсинг і збереження
            fetched_news, parse_duration = await fetch_all_sources()
            new_count = await save_news_with_transaction(fetched_news)
            # 2. Отримання новин для публікації
            news_to_post = await get_unique_news_from_db(Config.MAX_NEWS_PER_CYCLE)
            # 3. Публікація
            if news_to_post:
                posted_count = await send_news_to_channel(news_to_post)
                logger.info(
                    f"--- ✅ Цикл завершено. Нових: {new_count}. Кандидатів: {len(news_to_post)}. Опубліковано: {posted_count}. Парсинг: {parse_duration:.2f}с ---"
                )
            else:
                 logger.info(
                    f"--- ✅ Цикл завершено. Нових: {new_count}. Немає унікальних новин для публікації. Парсинг: {parse_duration:.2f}с ---"
                )
        except Exception as e:
            logger.critical(f"❌ Критична помилка в циклі автопостингу: {e}", exc_info=True)
        
        await asyncio.sleep(wait_time)

# --- Адмін-команди ---
async def cmd_status(message: types.Message):
    """Показує поточний статус бота та конфігурацію."""
    active_sources = await get_active_sources_from_db()
    async with db_pool.acquire() as conn:
        total_news = await conn.fetchval("SELECT COUNT(*) FROM news;")
        posted_news = await conn.fetchval("SELECT COUNT(*) FROM news WHERE is_posted = TRUE;")
        unposted_news = await conn.fetchval("SELECT COUNT(*) FROM news WHERE is_posted = FALSE;")
    
    status_text = (
        "<b>🤖 Статус Платформи Новин</b>\n\n"
        "<b>⚙️ Конфігурація:</b>\n"
        f"  - Інтервал: <b>{Config.POSTING_INTERVAL_MIN} хв</b>\n"
        f"  - Макс. постів/цикл: <b>{Config.MAX_NEWS_PER_CYCLE}</b>\n"
        f"  - Макс. вік новини: <b>{Config.MAX_AGE_MIN} хв</b>\n"
        f"  - Поріг схожості: <b>{Config.SIMILARITY_THRESHOLD * 100}%</b>\n\n"
        "<b>📰 Джерела:</b>\n"
        f"  - Активних: <b>{len(active_sources)} / {len(Config.SOURCES)}</b>\n\n"
        "<b>📊 Статистика БД:</b>\n"
        f"  - Всього новин: <b>{total_news}</b>\n"
        f"  - Опубліковано: <b>{posted_news}</b>\n"
        f"  - У черзі: <b>{unposted_news}</b>"
    )
    await message.answer(status_text, parse_mode=ParseMode.HTML)

async def cmd_forcepost(message: types.Message):
    """Примусово запускає один цикл парсингу та постингу."""
    await message.answer("⏳ <b>Примусовий запуск циклу...</b>", parse_mode=ParseMode.HTML)
    try:
        fetched_news, parse_duration = await fetch_all_sources()
        new_count = await save_news_with_transaction(fetched_news)
        news_to_post = await get_unique_news_from_db(Config.MAX_NEWS_PER_CYCLE)
        if news_to_post:
            posted_count = await send_news_to_channel(news_to_post)
            result_msg = (
                f"✅ <b>Цикл завершено!</b>\n"
                f"  - Нових новин: {new_count}\n"
                f"  - Кандидатів на пост: {len(news_to_post)}\n"
                f"  - Опубліковано: {posted_count}\n"
                f"  - Час парсингу: {parse_duration:.2f} сек"
            )
        else:
            result_msg = "✅ <b>Цикл завершено!</b> Не знайдено нових унікальних новин для публікації."
    except Exception as e:
        logger.error(f"Помилка примусового постингу: {e}", exc_info=True)
        result_msg = f"❌ <b>Помилка:</b> {e}"
    await message.answer(result_msg, parse_mode=ParseMode.HTML)

# --- 8. ЗАПУСК БОТА (WEBHOOK) ---
async def on_startup(bot_instance: Bot):
    """Виконується при старті: ініціалізація, підключення до ДБ, встановлення вебхука."""
    logger.info("--- Ініціалізація бота ---")
    if not all([BOT_TOKEN, DATABASE_URL, CHANNEL_ID, WEBHOOK_HOST, WEBHOOK_SECRET, ADMIN_ID]):
        logger.critical("Критична помилка: Не задані всі необхідні змінні середовища.")
        return

    await connect_db()
    if not db_pool: return
    await init_db()

    # Запуск фонових циклів
    asyncio.create_task(auto_posting_loop(bot_instance))
    asyncio.create_task(db_cleanup_loop())

    await bot_instance.set_webhook(
        url=WEBHOOK_URL,
        secret_token=WEBHOOK_SECRET,
        drop_pending_updates=True
    )
    logger.info(f"✅ Webhook встановлено на: {WEBHOOK_URL}")

async def on_shutdown(bot_instance: Bot):
    """Виконується при зупинці: видалення вебхука, закриття з'єднань."""
    logger.info("--- Зупинка бота ---")
    await bot_instance.delete_webhook(drop_pending_updates=True)
    if db_pool:
        await db_pool.close()
    logger.info("Webhook вимкнено, пул ДБ закрито.")

def main():
    """Основна функція для запуску бота через Webhook."""
    global bot, dp
    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher()
    
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)
    
    # Реєстрація команд адміністратора
    admin_filter = F.from_user.id == ADMIN_ID
    dp.message.register(cmd_status, Command("status"), admin_filter)
    dp.message.register(cmd_forcepost, Command("forcepost"), admin_filter)

    app = web.Application()
    webhook_handler = SimpleRequestHandler(dispatcher=dp, bot=bot, secret_token=WEBHOOK_SECRET)
    webhook_handler.register(app, path=WEBHOOK_PATH)
    
    web.run_app(app, host=WEB_SERVER_HOST, port=WEB_SERVER_PORT)

if __name__ == "__main__":
    try:
        main()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Бот зупинено.")
    except Exception as e:
        logger.critical(f"❌ Критична помилка на верхньому рівні: {e}", exc_info=True)
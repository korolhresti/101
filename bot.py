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

# Завантаження змінних оточення (для локального тестування)
load_dotenv() 

# --- 1. НАЛАШТУВАННЯ І КОНСТАНТИ (Професійні параметри) ---

# Використовуйте Kyiv time zone (UTC+3)
KYIV_TZ = timezone(timedelta(hours=3), 'Europe/Kyiv')

class Config:
    """Конфігурація платформи, зібрана в одному місці."""
    
    # ⚙️ ОСНОВНІ ПАРАМЕТРИ ЦИКЛУ
    POSTING_INTERVAL_MIN = 5   # Кожні 5 хвилин (Вимога)
    MAX_NEWS_PER_CYCLE = 1     # СТРОГИЙ ЛІМІТ: До 1 новини за цикл (Виконання вимоги 1 пост / 5 хв)
    MAX_AGE_MIN = 20           # Не публікувати новини старше 20 хвилин (Топові/Свіжі)
    
    # 🛡️ ПАРАМЕТРИ НАДІЙНОСТІ ТА ПРОДУКТИВНОСТІ (Оптимізація Compute)
    FETCH_LIMIT = 50           # Макс. кількість записів для обробки з одного RSS-фіда
    NUM_SOURCES_TO_FETCH = 15  # Кількість випадкових джерел, які парсяться за цикл (ЗМЕНШЕНО для економії CPU)
    HTTP_TIMEOUT = 10          # Таймаут для HTTP-запитів
    MAX_CONCURRENCY = 15       # Макс. одночасних з'єднань для парсингу (ЗМЕНШЕНО відповідно до NUM_SOURCES_TO_FETCH)
    MAX_RETRIES = 3            # Макс. кількість повторних спроб для HTTP-запиту
    RETRY_DELAY_SEC = 2        # Початкова затримка повтору
    
    # 💾 ПАРАМЕТРИ ОБСЛУГОВУВАННЯ БАЗИ ДАНИХ (Оптимізація Neon Compute Time)
    DB_POOL_MIN = 1            # Мінімальний розмір пулу
    DB_POOL_MAX = 2            # Максимальний розмір пулу (ЗМЕНШЕНО для економії Compute Time Neon)
    DB_CLEANUP_DAYS = 7        # Видаляти записи новин старше 7 днів
    CLEANUP_INTERVAL_HOURS = 1 # Частота очистки БД
    
    # ❌ ПАРАМЕТРИ БЛОКУВАННЯ ДЖЕРЕЛ (Економія Compute Time)
    BLOCKED_HTTP_CODES = [403, 404, 500, 503]
    SOURCE_BLOCK_THRESHOLD = 5 # Кількість помилок, після яких джерело блокується
    SOURCE_BLOCK_DURATION_HOURS = 2 # На скільки годин блокувати джерело
    
    DEFAULT_HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36 Bot (+https://t.me/YourNewsBotChannel)', 
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'uk-UA,uk;q=0.9,en-US;q=0.8,en;q=0.7',
    }
    
    # 1. 📰 Джерела новин (Оновлений список: видалено zaxid.net, додано babel.ua)
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
        "https://ua.interfax.com.ua/news/ukraine.rss", 
        # ВИДАЛЕНО: "https://zaxid.net/rss",
        "https://hromadske.ua/feed/news", "https://biz.censor.net/rss",
        "https://slovoidilo.ua/rss/index.xml", "https://apostrophe.ua/rss",
        "https://babel.ua/rss" # ДОДАНО
    ]


# Налаштування логування
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(filename)s - %(levelname)s - %(message)s')
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

# Глобальні змінні для DB, Bot, Dispatcher
db_pool: asyncpg.Pool = None
dp: Dispatcher = None
bot: Bot = None
current_post_limit: int = 0


# --- 2. БАЗА ДАНИХ (POSTGRESQL/NEON) ---

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
            timeout=5,
            statement_cache_size=0 # Часто допомагає з динамічними запитами Neon
        )
        logger.info(f"✅ Успішно підключено до Neon PostgreSQL. Пул: {Config.DB_POOL_MIN}-{Config.DB_POOL_MAX}. (Оптимізація Compute Time)")
    except Exception as e:
        logger.error(f"❌ Критична помилка підключення до DB: {e}")
        await asyncio.sleep(60)
        exit(1)

async def init_db():
    """Створює таблиці 'news' та 'source_stats' для надійної роботи."""
    if not db_pool:
        return

    async with db_pool.acquire() as conn:
        # Таблиця для новин (дедуплікація, стан)
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
                is_posted BOOLEAN DEFAULT FALSE
            );
        """)
        # Таблиця для відстеження помилок джерел (для економії Compute Time)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS source_stats (
                source_url TEXT PRIMARY KEY,
                error_count INTEGER DEFAULT 0,
                last_error_at TIMESTAMP WITH TIME ZONE,
                is_blocked BOOLEAN DEFAULT FALSE
            );
        """)
        
        try:
            # Створення індексів для швидкого пошуку та запобігання дублікатів
            await conn.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS news_url_idx ON news (url);
                CREATE INDEX IF NOT EXISTS news_is_posted_idx ON news (is_posted, published_at);
            """)
        except Exception as e:
            logger.error(f"Помилка при створенні індексу: {e}")

    logger.info("Таблиці DB перевірені/оновлені.")

async def save_news_with_transaction(news_items: List[Dict[str, Any]]) -> int:
    """Виконує пакетну вставку новин в одній транзакції (найкраща економія Neon)."""
    if not news_items or not db_pool:
        return 0
    
    # Використовуємо UNNEST для пакетної вставки, що є найбільш ефективним методом
    sql = """
        INSERT INTO news (source, url, title, summary, image_url, published_at)
        SELECT * FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::timestamptz[])
        ON CONFLICT (url) DO NOTHING
        RETURNING id;
    """
    
    sources = [item['source'] for item in news_items]
    urls = [item['url'] for item in news_items]
    titles = [item['title'] for item in news_items]
    summaries = [item['summary'] for item in news_items]
    image_urls = [item['image_url'] for item in news_items]
    published_at_list = [item['published_at'] for item in news_items]
    
    try:
        async with db_pool.acquire() as conn:
            async with conn.transaction():
                result = await conn.fetch(sql, sources, urls, titles, summaries, image_urls, published_at_list)
                return len(result)
    except asyncpg.exceptions.PostgresError as e:
        logger.error(f"❌ Помилка транзакційної вставки в БД: {e}")
        return 0

async def get_active_sources_from_db() -> Set[str]:
    """Вибирає лише ті джерела, які не заблоковані або час блокування минув."""
    if not db_pool:
        return set(Config.SOURCES)
    
    # 1. Оновлюємо статус блокованих, якщо час блокування минув
    await update_source_block_status()

    # 2. Вибираємо всі не заблоковані джерела
    sql_get_blocked = "SELECT source_url FROM source_stats WHERE is_blocked = TRUE;"
    
    try:
        async with db_pool.acquire() as conn:
            blocked_records = await conn.fetch(sql_get_blocked)
            blocked_urls = {r['source_url'] for r in blocked_records}

            active_sources = set(Config.SOURCES) - blocked_urls
            
            # Додаткова перевірка, чи всі джерела з Config.SOURCES є в source_stats (для першого запуску)
            all_sources_in_db = {r['source_url'] for r in await conn.fetch("SELECT source_url FROM source_stats;")}
            new_sources = active_sources - all_sources_in_db
            
            # Додавання нових джерел в source_stats, якщо їх там немає
            if new_sources:
                 insert_sql = "INSERT INTO source_stats (source_url, error_count, is_blocked) VALUES ($1, 0, FALSE) ON CONFLICT (source_url) DO NOTHING;"
                 await conn.executemany(insert_sql, [(url,) for url in new_sources])
                 
            logger.info(f"Активних джерел: {len(active_sources)}. Заблокованих: {len(blocked_urls)}")
            return active_sources

    except asyncpg.exceptions.PostgresError as e:
        logger.error(f"❌ Помилка отримання активних джерел: {e}")
        return set(Config.SOURCES) 

async def update_source_error_count(source_url: str, is_error: bool, http_code: int = None):
    """Оновлює статистику джерела та блокує його при досягненні порога."""
    if not db_pool:
        return

    async with db_pool.acquire() as conn:
        if is_error and http_code in Config.BLOCKED_HTTP_CODES:
            # Збільшення лічильника
            await conn.execute("""
                INSERT INTO source_stats (source_url, error_count, last_error_at)
                VALUES ($1, 1, $2)
                ON CONFLICT (source_url) 
                DO UPDATE SET 
                    error_count = source_stats.error_count + 1, 
                    last_error_at = $2;
            """, source_url, datetime.now(KYIV_TZ))

            # Перевірка на блокування
            record = await conn.fetchrow("SELECT error_count FROM source_stats WHERE source_url = $1;", source_url)
            if record and record['error_count'] >= Config.SOURCE_BLOCK_THRESHOLD:
                await conn.execute("""
                    UPDATE source_stats 
                    SET is_blocked = TRUE 
                    WHERE source_url = $1;
                """, source_url)
                logger.warning(f"🚨 Джерело заблоковано: {source_url}. Помилок: {record['error_count']}.")
        
        elif not is_error:
            # Скидання лічильника при успіху
            await conn.execute("""
                INSERT INTO source_stats (source_url, error_count)
                VALUES ($1, 0)
                ON CONFLICT (source_url) 
                DO UPDATE SET 
                    error_count = 0,
                    is_blocked = FALSE; -- Знімаємо блокування при успішному парсингу
            """, source_url)
            
async def update_source_block_status():
    """Розблоковує джерела, час блокування яких минув, і очищає статистику."""
    if not db_pool:
        return
        
    unlock_time = datetime.now(KYIV_TZ) - timedelta(hours=Config.SOURCE_BLOCK_DURATION_HOURS)
    
    sql = """
        UPDATE source_stats
        SET is_blocked = FALSE, error_count = 0
        WHERE is_blocked = TRUE AND last_error_at < $1
        RETURNING source_url;
    """
    
    async with db_pool.acquire() as conn:
        records = await conn.fetch(sql, unlock_time)
        if records:
            urls = [r['source_url'] for r in records]
            logger.info(f"🔓 Розблоковано {len(urls)} джерел.")


async def get_unique_news_from_db(limit: int) -> List[Dict[str, Any]]:
    """Вибирає свіжі, неопубліковані новини, надаючи ПРІОРИТЕТ тим, що мають зображення."""
    if not db_pool or limit == 0:
        return []

    # Стратегія: пріоритет (image_url IS NOT NULL) > свіжість (published_at DESC)
    sql = """
        SELECT source, url, title, summary, image_url, published_at
        FROM news
        WHERE is_posted = FALSE
        ORDER BY 
            (CASE WHEN image_url IS NOT NULL THEN 0 ELSE 1 END), -- 1. Пріоритет фото
            published_at DESC                                    -- 2. Свіжість
        LIMIT $1;
    """
    
    try:
        async with db_pool.acquire() as conn:
            records = await conn.fetch(sql, limit)
            return [dict(r) for r in records]
    except Exception as e:
        logger.error(f"❌ Помилка отримання новин з БД: {e}")
        return []

async def mark_news_as_posted(urls: List[str]):
    """Пакетне оновлення статусу 'is_posted' для економії запитів (Neon)."""
    if not urls or not db_pool:
        return
        
    sql = """
        UPDATE news
        SET is_posted = TRUE
        WHERE url = ANY($1::text[]);
    """
    try:
        async with db_pool.acquire() as conn:
            await conn.execute(sql, urls)
    except Exception as e:
        logger.error(f"❌ Помилка пакетного оновлення статусу: {e}")

async def cleanup_db():
    """Видаляє старі записи новин для підтримки бази даних у робочому стані."""
    if not db_pool:
        return
        
    cutoff_time = datetime.now(KYIV_TZ) - timedelta(days=Config.DB_CLEANUP_DAYS)
    
    sql = """
        DELETE FROM news
        WHERE inserted_at < $1
        RETURNING id;
    """
    try:
        async with db_pool.acquire() as conn:
            result = await conn.execute(sql, cutoff_time)
            deleted_count = int(result.split()[-1])
            logger.info(f"🗑️ Видалено {deleted_count} старих записів новин (старше {Config.DB_CLEANUP_DAYS} днів).")
    except Exception as e:
        logger.error(f"❌ Помилка очищення БД: {e}")


async def get_db_stats() -> Dict[str, int]:
    """Отримує загальну статистику для команд адміністратора."""
    if not db_pool:
        return {}
        
    sql = """
        SELECT 
            COUNT(*) AS total_news,
            COUNT(*) FILTER (WHERE is_posted = TRUE) AS posted_news,
            COUNT(*) FILTER (WHERE is_posted = FALSE AND image_url IS NOT NULL) AS unposted_news
        FROM news;
    """
    try:
        async with db_pool.acquire() as conn:
            record = await conn.fetchrow(sql)
            if record:
                return dict(record)
            return {}
    except Exception as e:
        logger.error(f"❌ Помилка отримання статистики: {e}")
        return {}


# --- 3. ХЕЛПЕРИ ПАРСИНГУ ---

def is_news_relevant(title: str, summary: str) -> bool:
    """
    Фільтрація за ключовими словами: Блокування футболу, боксу, авто, кіно/серіалів та відомих персон (Вимога).
    """
    negative_keywords = [
        # Загальні/Реклама
        'гороскоп', 'астрологічний', 'реклама', 'прогноз погоди',
        
        # Спорт (Футбол, Бокс)
        'футбол', 'бокс', 'матч', 'гравець', 'спортсмен', 'удар', 'гол', 'нокаут', 'усик', 'кличко', 
        'чемпіон', 'ліга чемпіонів', 'євро-2024', 'кубок', 'спаринг', 'фінал',
        
        # Кіно/Серіали/Персони
        'серіал', 'кіно', 'прем\'єра', 'трейлер', 'оскар', 'netflix', 'актор', 'режисер', 
        'кінострічка', 'зірка екрану', 'відомих персон', 'знаменитості', 'шоу-бізнес', 
        'кохання', 'розлучення', 'весілля', 'стосунки', 'зірки',
        
        # Автомобілі (заборона новин про цивільні авто)
        'автомобіль', 'авто', 'тест-драйв', 'модельний ряд', 'автосалон', 'кросовер', 'мотоцикл', 'дрифт'
    ] 
    
    content = (title + ' ' + summary).lower()
    return not any(kw in content for kw in negative_keywords)


def normalize_summary(text: str) -> str:
    """Очищення та скорочення тексту для використання в Telegram."""
    if not text:
        return "Деталі за посиланням."
    
    # Видалення HTML-тегів
    soup = BeautifulSoup(text, 'html.parser')
    clean_text = soup.get_text()
    
    # Видалення зайвих пробілів
    clean_text = re.sub(r'\s+', ' ', clean_text).strip()
    
    # Скорочення до 300-400 символів, якщо занадто довгий
    if len(clean_text) > 400:
        clean_text = clean_text[:380]
        # Шукаємо останній розділовий знак
        last_dot = max(clean_text.rfind('.'), clean_text.rfind('!'), clean_text.rfind('?'))
        if last_dot > 100:
            clean_text = clean_text[:last_dot + 1]
        clean_text += "..."
        
    return clean_text

def extract_image_url(entry: feedparser.FeedParserDict) -> str | None:
    """Вилучення URL зображення з різних полів RSS-запису."""
    
    # 1. Шукаємо в enclosure
    if 'enclosures' in entry and entry.enclosures:
        for enc in entry.enclosures:
            if enc.get('type', '').startswith('image/'):
                return enc.get('href')

    # 2. Шукаємо в media_content
    if 'media_content' in entry and entry.media_content:
        for media in entry.media_content:
            if media.get('type', '').startswith('image/'):
                return media.get('url')

    # 3. Шукаємо в media_thumbnail
    if 'media_thumbnail' in entry and entry.media_thumbnail:
        if isinstance(entry.media_thumbnail, list) and entry.media_thumbnail:
            return entry.media_thumbnail[0].get('url')
        if isinstance(entry.media_thumbnail, dict):
             return entry.media_thumbnail.get('url')

    # 4. Парсимо HTML-контент або опис (як запасний варіант)
    html_content = entry.get('content', [{}])[0].get('value') or entry.get('summary') or entry.get('description')
    if html_content:
        soup = BeautifulSoup(html_content, 'html.parser')
        img = soup.find('img')
        if img and img.get('src'):
            return img['src']

    return None

def parse_published_time(entry: feedparser.FeedParserDict) -> datetime:
    """Парсинг часу публікації та конвертація в Kyiv Time Zone."""
    # feedparser автоматично намагається парсити publish_time
    if hasattr(entry, 'published_parsed'):
        try:
            dt_utc = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
            return dt_utc.astimezone(KYIV_TZ)
        except Exception:
            pass # Продовжуємо спроби
            
    # Якщо не вдалося, повертаємо поточний час як припущення
    return datetime.now(KYIV_TZ)


# --- 4. ГЕНЕРАЦІЯ ХЕШТЕГІВ (Покращено: Корекція відмінків та об'єднання імен) ---

# Словник для заміни відмінкових форм називним відмінком (ЛЕКСИЧНА НОРМАЛІЗАЦІЯ)
UKR_CASE_CORRECTIONS = {
    # Політика/Географія/Організації
    'києви': 'Київ', 'києва': 'Київ', 'києву': 'Київ', 'києвом': 'Київ', 'києві': 'Київ',
    'москви': 'Москва', 'москвою': 'Москва', 'москві': 'Москва',
    'росії': 'Росія', 'росією': 'Росія',
    'китаю': 'Китай', 'китаєм': 'Китай',
    'валенсії': 'Валенсія', 'валенсією': 'Валенсія',
    'пентагону': 'Пентагон', 'пентагоном': 'Пентагон',
    'сенату': 'Сенат', 'сенатом': 'Сенат',
    'тернопільщини': 'Тернопільщина', 'тернопільщиною': 'Тернопільщина',
    
    # Імена/Посади
    'трампа': 'Трамп', 'трампом': 'Трамп',
    'зеленського': 'Зеленський', 'зеленським': 'Зеленський',
    'байдена': 'Байден', 'байдену': 'Байден',
    'президента': 'Президент', 'міністра': 'Міністр',
    
    # Загальні слова, що часто відмінюються
    'військ': 'Військо', 'війні': 'Війна', 'війною': 'Війна',
}

def normalize_word_for_hashtag(word: str) -> str:
    """Нормалізує слово, застосовуючи корекції відмінків та очищення."""
    word = word.lower().strip()
    
    # 1. Спроба корекції відмінка за словником
    if word in UKR_CASE_CORRECTIONS:
        return UKR_CASE_CORRECTIONS[word]
    
    # 2. Видалення символів, окрім літер/цифр (для загальних слів)
    cleaned_word = re.sub(r'[^\w]', '', word)
    
    # 3. Нормалізація регіону (залишаємо тільки першу літеру великою)
    return cleaned_word.capitalize() if cleaned_word else ""

def generate_hashtags(title: str, source: str) -> str:
    """Генерує хештеги відповідно до вимог: корекція відмінків, об'єднання імен."""
    
    stop_words = set([
        'на', 'в', 'у', 'з', 'до', 'про', 'від', 'для', 'це', 'що', 'як', 'та', 'але', 
        'і', 'по', 'за', 'під', 'над', 'коли', 'буде', 'було', 'є', 'він', 'вона', 
        'воно', 'вони', 'ми', 'ви', 'тисяч', 'мільйонів', 'може', 'які', 'який', 
        'яка', 'щодо', 'зі', 'через', 'поки', 'подробиці', 'рік', 'року', 'день', 
        'місяць', 'тижня', 'сегодня'
    ])
    
    # Очистка та токенізація заголовка
    clean_title = re.sub(r'[^\w\s-]', '', title) 
    words = clean_title.split()
    
    hashtags: Set[str] = set()
    used_indices: Set[int] = set() # Щоб не використовувати одне слово двічі
    
    # 1. Обробка слів та об'єднання імен
    for i in range(len(words)):
        if i in used_indices:
            continue
            
        word = words[i]
        
        # Об'єднання імені та прізвища (Ім'я Прізвище -> #Ім'яПрізвище)
        if word[0].isupper() and i + 1 < len(words) and words[i+1][0].isupper():
            next_word = words[i+1]
            
            # Застосовуємо нормалізацію до обох слів
            normalized_name_part1 = normalize_word_for_hashtag(word)
            normalized_name_part2 = normalize_word_for_hashtag(next_word)
            
            if normalized_name_part1 and normalized_name_part2:
                # Об'єднання без пробілів
                hashtags.add(f"#{normalized_name_part1}{normalized_name_part2}") 
                used_indices.add(i)
                used_indices.add(i + 1)
                continue
                
        # Обробка одиничного слова
        lower_word = word.lower()
        
        if len(word) > 3 and lower_word not in stop_words:
            
            # Якщо це слово у словнику корекції або власне ім'я
            if word[0].isupper() or lower_word in UKR_CASE_CORRECTIONS:
                normalized_word = normalize_word_for_hashtag(word)
                if normalized_word:
                    hashtags.add(f"#{normalized_word}")
                    used_indices.add(i)

    # 2. Формування хештегу джерела
    source_domain = urlparse(f"https://{source}").netloc.replace('www.', '')
    source_parts = source_domain.split('.')
    # Беремо передостанню частину домену (наприклад, "pravda" з "www.pravda.com.ua")
    source_name = source_parts[-2].capitalize() if len(source_parts) >= 2 else source_parts[0].capitalize()
    source_tag = f"#{source_name}" if source_name else "#Джерело"
    
    # 3. Комбінування та фіналізація
    final_tags = ["#Новини"]
    if source_tag and source_tag not in final_tags:
        final_tags.append(source_tag)
    
    # Додаємо інші унікальні хештеги, обмежуючи загальну кількість до 6 (максимальна релевантність)
    for tag in sorted(list(hashtags)): 
        if tag not in final_tags and len(final_tags) < 6: 
            final_tags.append(tag)
            
    return " ".join(final_tags)


# --- 5. ОСНОВНИЙ ПАРСИНГ (З Backoff Retry) ---

async def fetch_and_parse_source(session: aiohttp.ClientSession, rss_url: str) -> List[Dict[str, Any]]:
    """Отримує, парсить та фільтрує новини з одного RSS-джерела з повторними спробами."""
    news_items = []
    source_domain = urlparse(rss_url).netloc.replace('www.', '')
    
    for attempt in range(Config.MAX_RETRIES):
        try:
            async with session.get(rss_url, headers=Config.DEFAULT_HEADERS, timeout=Config.HTTP_TIMEOUT) as response:
                
                # Обробка успіху
                if response.status == 200:
                    await update_source_error_count(rss_url, is_error=False)
                    content = await response.text(encoding=response.charset or 'utf-8')
                    
                    feed = feedparser.parse(content)
                    now_kyiv = datetime.now(KYIV_TZ)
                    max_age_dt = timedelta(minutes=Config.MAX_AGE_MIN) 

                    for entry in feed.entries[:Config.FETCH_LIMIT]:
                        try:
                            url = entry.link
                            title = entry.title
                            summary = normalize_summary(entry.get('summary') or entry.get('description') or entry.title)
                            
                            # Фільтрація
                            if not is_news_relevant(title, summary): continue
                            
                            image_url = extract_image_url(entry)
                            published_time = parse_published_time(entry)

                            # Фільтрація за часом (тільки свіжі новини)
                            if now_kyiv - published_time > max_age_dt: continue

                            news_items.append({
                                'source': source_domain, 'title': title, 'url': url, 
                                'summary': summary, 'image_url': image_url, 'published_at': published_time,
                            })
                        except Exception as e:
                            logger.warning(f"Помилка обробки запису з {rss_url}: {e}")
                            continue
                            
                    return news_items # Успішно завершено, виходимо
                
                # Обробка помилок, що ведуть до блокування
                elif response.status in Config.BLOCKED_HTTP_CODES:
                    logger.warning(f"⚠️ HTTP Помилка {response.status} ({attempt+1}/{Config.MAX_RETRIES}) для {rss_url}. Блокую...")
                    await update_source_error_count(rss_url, is_error=True, http_code=response.status)
                    return [] # Не повторюємо, якщо код помилки блокує
                
                # Обробка інших помилок (з повторною спробою)
                else:
                    logger.warning(f"⚠️ HTTP Помилка {response.status} ({attempt+1}/{Config.MAX_RETRIES}) для {rss_url}. Спроба...")
                    
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.warning(f"❌ Помилка мережі ({attempt+1}/{Config.MAX_RETRIES}) для {rss_url}: {type(e).__name__}. Спроба...")
        
        # Експоненційна затримка (Exponential Backoff)
        if attempt < Config.MAX_RETRIES - 1:
            await asyncio.sleep(Config.RETRY_DELAY_SEC * (2 ** attempt))
            
    # Якщо всі спроби вичерпані
    await update_source_error_count(rss_url, is_error=True, http_code=599) # Використовуємо 599 як загальну помилку
    return []

async def fetch_all_sources() -> Tuple[List[Dict[str, Any]], float]:
    """Запускає одночасний парсинг тільки активних джерел."""
    all_news = []
    start_time = datetime.now()

    # Отримуємо лише активні джерела
    active_sources_urls = await get_active_sources_from_db()
    
    # Вибираємо випадкову підмножину для рівномірного навантаження
    num_sources_to_fetch = min(Config.NUM_SOURCES_TO_FETCH, len(active_sources_urls)) 
    selected_sources = random.sample(list(active_sources_urls), num_sources_to_fetch)
    
    logger.info(f"⏳ Парсинг {len(selected_sources)} випадкових активних джерел...")

    # Встановлюємо високий ліміт конекторів для максимальної швидкості
    connector = aiohttp.TCPConnector(limit=Config.MAX_CONCURRENCY)
    async with aiohttp.ClientSession(headers=Config.DEFAULT_HEADERS, connector=connector) as session:
        # Одночасний запуск усіх завдань парсингу
        tasks = [fetch_and_parse_source(session, rss_url) for rss_url in selected_sources]
        results = await asyncio.gather(*tasks)

        for news_list in results:
            if news_list:
                all_news.extend(news_list)

    duration = (datetime.now() - start_time).total_seconds()
    
    return all_news, duration

# --- 6. ФОРМАТУВАННЯ ТА ПОСТИНГ ---

def format_news_post(news_item: Dict[str, Any]) -> str:
    """Форматує новину для Telegram, включаючи якісні хештеги."""
    source_display = news_item['source'].replace('https://', '').replace('http://', '')
    
    message = (
        f"<b>⚡️ {news_item['title']}</b>\n\n"
        f"{news_item['summary']}\n\n"
        f"<a href='{news_item['url']}'>Подробиці на {source_display}</a>"
    )

    hashtags = generate_hashtags(news_item['title'], source_display)
    message += f"\n\n{hashtags}" 
    
    return message

async def send_news_to_channel(news_to_post: List[Dict[str, Any]]) -> int:
    """Надсилає новини в Telegram-канал з безпечним оновленням статусу DB (Правильна перевірка перед постом)."""
    posted_urls = []
    
    for news in news_to_post: 
        try:
            caption = format_news_post(news)
            
            if news.get('image_url'):
                # Відправка з фото
                await bot.send_photo(
                    chat_id=CHANNEL_ID,
                    photo=news['image_url'],
                    caption=caption,
                    parse_mode=ParseMode.HTML,
                    disable_notification=False 
                )
            else:
                 # Відправка як тексту (якщо немає фото)
                 await bot.send_message(
                    chat_id=CHANNEL_ID,
                    text=caption,
                    parse_mode=ParseMode.HTML,
                    disable_notification=False 
                 )
            
            await asyncio.sleep(1.5) # Пауза, щоб уникнути FloodWait
            posted_urls.append(news['url'])
            
        except TelegramAPIError as e:
            # КЛЮЧОВА ПЕРЕВІРКА: При будь-якій помилці API (навіть при проблемі з фото),
            # новина маркується як опублікована, щоб не потрапити в нескінченний цикл.
            logger.error(f"❌ Telegram API Error для '{news['title'][:50]}...': {e.message}")
            if "Bad Request: failed to get HTTP URL content" in e.message or "Bad Request: PHOTO_INVALID" in e.message:
                logger.warning("-> Проблема з URL зображення або інша помилка. Новина маркується як опублікована.")
            
            posted_urls.append(news['url']) 
            continue
        except Exception as e:
            logger.error(f"❌ Невідома помилка відправки для '{news['title'][:50]}...': {e}")
            posted_urls.append(news['url']) # Також маркуємо як опубліковану на випадок загальної помилки
            continue 

    # Пакетне оновлення статусу в DB
    await mark_news_as_posted(posted_urls)
    return len(posted_urls)

# --- 7. ЦИКЛИ ТА КОМАНДИ АДМІНІСТРАТОРА ---

async def db_cleanup_loop():
    """Асинхронний цикл для періодичного очищення бази даних та оновлення джерел."""
    while True:
        await asyncio.sleep(Config.CLEANUP_INTERVAL_HOURS * 3600) 
        logger.info("--- ♻️ Запуск фонової очистки БД та оновлення джерел ---")
        await cleanup_db()
        await update_source_block_status()

async def auto_posting_loop(bot_instance: Bot):
    """Головний цикл, який періодично перевіряє та публікує новини з динамічним лімітом."""
    global current_post_limit
    wait_time = Config.POSTING_INTERVAL_MIN * 60
    
    current_post_limit = 0 
    
    while True:
        try:
            logger.info("--- 🚀 Запуск циклу автопостингу ---")
            
            # 1. Парсинг і збереження новин
            fetched_news, parse_duration = await fetch_all_sources()
            # Використовуємо транзакційну вставку (Ключ до економії Neon Compute Time)
            new_count = await save_news_with_transaction(fetched_news)
            logger.info(f"💾 Успішно вставлено {new_count} новин.")

            # 2. Оновлення ліміту постів (Обмеження до 1)
            # При MAX_NEWS_PER_CYCLE = 1, current_post_limit швидко досягне 1 і залишиться там.
            current_post_limit = min(current_post_limit + 1, Config.MAX_NEWS_PER_CYCLE)
            current_limit = current_post_limit
            
            # 3. Отримуємо новини для публікації
            news_to_post = await get_unique_news_from_db(current_limit)
            
            # 4. Публікація 
            post_start_time = datetime.now()
            posted_count = await send_news_to_channel(news_to_post)
            post_duration = (datetime.now() - post_start_time).total_seconds()
            
            logger.info(
                f"--- ✅ Цикл завершено. Нових: {new_count}. Поточний ліміт: {current_limit}. Опубліковано: {posted_count}. Таймінги: Парсинг={parse_duration:.2f}с, Постинг={post_duration:.2f}с ---"
            )
            
        except Exception as e:
            logger.critical(f"❌ Критична помилка в циклі автопостингу: {e}", exc_info=True)

        await asyncio.sleep(wait_time)
        logger.info(f"Очікування {Config.POSTING_INTERVAL_MIN} хвилин...")

# --- КОМАНДИ АДМІНІСТРАТОРА ---

async def cmd_status(message: types.Message):
    """Показує поточний статус бота та конфігурацію."""
    global current_post_limit
    active_sources = await get_active_sources_from_db()
    stats = await get_db_stats()
    
    config_msg = (
        "<b>🤖 Статус Платформи Новин:</b>\n\n"
        "<b>⚙️ Конфігурація:</b>\n"
        f"  ⏳ Інтервал: <b>{Config.POSTING_INTERVAL_MIN} хв</b>\n"
        f"  ⏱️ Макс. вік новини: {Config.MAX_AGE_MIN} хв\n"
        f"  📝 Макс. постів за цикл: <b>{Config.MAX_NEWS_PER_CYCLE}</b>\n"
        f"  🔄 **Поточний ліміт постів:** <b>{current_post_limit}</b>\n"
        f"  🛡️ Порог блокування джерела: {Config.SOURCE_BLOCK_THRESHOLD} помилок\n"
        f"  📰 Активних джерел: <b>{len(active_sources)}</b> (Всього: {len(Config.SOURCES)})\n"
        f"  💾 DB Пул: <b>{Config.DB_POOL_MIN}-{Config.DB_POOL_MAX}</b> (Оптимізація Neon)"
    )
    
    stats_msg = (
        "\n\n📊 <b>Статистика DB:</b>\n"
        f"• Всього новин у DB: {stats.get('total_news', 0)}\n"
        f"• Опубліковано: {stats.get('posted_news', 0)}\n"
        f"• У черзі (З ФОТО): {stats.get('unposted_news', 0)}"
    )
    
    await message.answer(config_msg + stats_msg, parse_mode=ParseMode.HTML)


async def cmd_forcepost(message: types.Message):
    """Примусово запускає цикл парсингу та постингу."""
    global current_post_limit
    await message.answer("♻️ Примусовий запуск циклу парсингу...")
    
    async def run_once(bot_instance):
        try:
            start_time = datetime.now()
            # 1. Парсинг
            fetched_news, parse_duration = await fetch_all_sources()
            new_count = await save_news_with_transaction(fetched_news)
            
            # 2. Оновлення ліміту
            current_post_limit = min(current_post_limit + 1, Config.MAX_NEWS_PER_CYCLE)
            current_limit = current_post_limit

            # 3. Вибірка для постингу
            news_to_post = await get_unique_news_from_db(current_limit) 
            
            # 4. Постинг
            post_start_time = datetime.now()
            posted_count = await send_news_to_channel(news_to_post)
            post_duration = (datetime.now() - post_start_time).total_seconds()
            
            result_msg = (
                "✅ <b>Цикл примусового постингу завершено!</b>\n"
                f"   • Знайдено нових новин: {new_count}\n"
                f"   • <b>Поточний ліміт постів: {current_limit}</b>\n"
                f"   • Опубліковано новин: {posted_count}\n"
                f"   • Таймінг (Парсинг): {parse_duration:.2f} сек\n"
                f"   • Таймінг (Постинг): {post_duration:.2f} сек"
            )
        except Exception as e:
            result_msg = f"❌ <b>Критична помилка примусового постингу:</b> {e}"
        
        await bot_instance.send_message(message.chat.id, result_msg, parse_mode=ParseMode.HTML)

    loop = asyncio.get_event_loop()
    loop.create_task(run_once(bot))


async def cmd_stats(message: types.Message):
    """Показує статистику бази даних."""
    stats = await get_db_stats()
    active_sources = await get_active_sources_from_db()
    
    if stats:
        stats_msg = (
            "📊 <b>Статистика Бази Даних:</b>\n\n"
            f"• 📝 Всього новин у DB: {stats.get('total_news', 0)}\n"
            f"• ✅ Опубліковано: {stats.get('posted_news', 0)}\n"
            f"• 📦 У черзі (З ФОТО): {stats.get('unposted_news', 0)}\n"
            f"• 📰 Активних джерел: {len(active_sources)} / {len(Config.SOURCES)}"
        )
    else:
        stats_msg = "❌ Не вдалося отримати статистику з бази даних."

    await message.answer(stats_msg, parse_mode=ParseMode.HTML)


# --- 8. ЗАПУСК БОТА (WEBHOOK) ---

async def main():
    """Основна функція для ініціалізації та запуску бота через Webhook."""
    
    if not all([BOT_TOKEN, DATABASE_URL, CHANNEL_ID, WEBHOOK_HOST, WEBHOOK_SECRET, ADMIN_ID]):
        logger.critical("Критична помилка: Не задані BOT_TOKEN, DATABASE_URL, CHANNEL_ID, WEBHOOK_HOST, WEBHOOK_SECRET або ADMIN_ID.")
        return

    await connect_db()
    if not db_pool:
        return

    await init_db()

    global bot
    bot = Bot(
        token=BOT_TOKEN, 
        default=DefaultBotProperties(parse_mode=ParseMode.HTML)
    )
    
    global dp
    dp = Dispatcher()
    
    # Реєстрація команд адміністратора
    dp.message.register(cmd_status, Command("status"), F.from_user.id == ADMIN_ID)
    dp.message.register(cmd_forcepost, Command("forcepost"), F.from_user.id == ADMIN_ID)
    dp.message.register(cmd_stats, Command("stats"), F.from_user.id == ADMIN_ID)

    # Запуск фонових циклів
    loop = asyncio.get_event_loop()
    loop.create_task(auto_posting_loop(bot))
    loop.create_task(db_cleanup_loop())
    logger.info("Бот запущено. Початок роботи (WEBHOOK MODE).")
    
    runner = None
    try:
        # 1. Встановлення Webhook
        await bot.set_webhook(
            url=WEBHOOK_URL,
            secret_token=WEBHOOK_SECRET,
            drop_pending_updates=True 
        )
        logger.info(f"✅ Webhook встановлено на: {WEBHOOK_URL}")

        # 2. Налаштовуємо AIOHTTP веб-сервер
        app = web.Application()
        
        # Реєструємо обробник для шляху вебхука
        webhook_request_handler = SimpleRequestHandler(
            dispatcher=dp,
            bot=bot,
            secret_token=WEBHOOK_SECRET,
        )
        webhook_request_handler.register(app, WEBHOOK_PATH)

        # 3. Запускаємо веб-сервер
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, host=WEB_SERVER_HOST, port=WEB_SERVER_PORT)
        
        await site.start()
        logger.info(f"🌐 Веб-сервер запущено на {WEB_SERVER_HOST}:{WEB_SERVER_PORT}")
        
        # Блокуємо головний потік, щоб він не завершився (поки працює веб-сервер)
        await asyncio.Future() 

    except Exception as e:
        logger.critical(f"❌ Критична помилка у головній функції: {e}", exc_info=True)

    finally:
        # 4. Очищення при завершенні роботи
        if bot:
            await bot.delete_webhook(drop_pending_updates=True) 
            await bot.session.close()
            logger.info("Webhook успішно вимкнено.")
        if db_pool:
            await db_pool.close()
        if runner:
            await runner.cleanup()


if __name__ == "__main__":
    try:
        # Запуск головної асинхронної функції
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Бот зупинено вручну (KeyboardInterrupt).")
    except Exception as e:
        logger.critical(f"❌ Головна помилка виконання: {e}")
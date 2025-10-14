import os
import asyncio
import logging
import re
import random
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse, urljoin
from typing import List, Dict, Any, Tuple

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

# --- 1. НАЛАШТУВАННЯ І КОНСТАНТИ ---

# Використовуйте Kyiv time zone (UTC+3)
KYIV_TZ = timezone(timedelta(hours=3), 'Europe/Kyiv')
# Формат часу для відображення
TIME_FORMAT = "%H:%M" 

class Config:
    """Конфігурація платформи, зібрана в одному місці."""
    
    # ⚙️ ОСНОВНІ ПАРАМЕТРИ ЦИКЛУ
    POSTING_INTERVAL_MIN = 5  # Кожні 5 хвилин
    MAX_NEWS_PER_CYCLE = 3   # СТРОГИЙ ЛІМІТ: До 3 новин за цикл (ТОП-3)
    MAX_AGE_MIN = 30          # Не публікувати новини старше 30 хвилин
    
    # 🛡️ ПАРАМЕТРИ НАДІЙНОСТІ ТА ПРОДУКТИВНОСТІ
    FETCH_LIMIT = 30          # Макс. кількість записів для обробки з одного RSS-фіда
    NUM_SOURCES_TO_FETCH = 24 # Кількість випадкових джерел, які парсяться за цикл
    HTTP_TIMEOUT = 15         # Таймаут для HTTP-запитів
    MAX_CONCURRENCY = 15      # Макс. одночасних з'єднань для парсингу
    
    # 💾 ПАРАМЕТРИ ОБСЛУГОВУВАННЯ БАЗИ ДАНИХ
    DB_CLEANUP_DAYS = 7       # Видаляти записи старше 7 днів
    CLEANUP_INTERVAL_HOURS = 1 # Частота очистки БД
    
    DEFAULT_HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36', 
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'uk-UA,uk;q=0.9,en-US;q=0.8,en;q=0.7',
    }
    
    # 1. 📰 Джерела новин (доповнений список)
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
        "https://ua.interfax.com.ua/news/ukraine.rss", "https://zaxid.net/rss",
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

# Webhook конфігурація
WEB_SERVER_HOST = os.getenv("WEB_SERVER_HOST", "0.0.0.0")
WEB_SERVER_PORT = int(os.getenv("PORT", 8080)) 
WEBHOOK_HOST = os.getenv("WEBHOOK_HOST") # Наприклад: https://my-bot.render.com
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "/webhook") 
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET") # Секретний токен

WEBHOOK_URL = urljoin(WEBHOOK_HOST, WEBHOOK_PATH) if WEBHOOK_HOST else None

try:
    ADMIN_ID = int(os.getenv("ADMIN_ID", 0))
except ValueError:
    ADMIN_ID = 0

# Глобальні змінні для DB
db_pool: asyncpg.Pool = None
dp: Dispatcher = None
bot: Bot = None

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
            min_size=1, 
            max_size=10,
            timeout=5 
        )
        logger.info("✅ Успішно підключено до Neon PostgreSQL.")
    except Exception as e:
        logger.error(f"❌ Критична помилка підключення до DB: {e}")
        await asyncio.sleep(60)
        # Використовуємо sys.exit(1) для PaaS-платформ
        # import sys; sys.exit(1) 
        exit(1)

async def init_db():
    """Створює таблицю 'news' та необхідні індекси."""
    if not db_pool:
        return

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
                is_posted BOOLEAN DEFAULT FALSE
            );
        """)
        
        # Перевірка та створення індексу (для уникнення конфліктів)
        try:
            await conn.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS news_url_idx ON news (url);
                CREATE INDEX IF NOT EXISTS news_is_posted_idx ON news (is_posted, published_at);
            """)
        except Exception as e:
            logger.error(f"Помилка при створенні індексу: {e}")

    logger.info("Таблиці DB перевірені/оновлені.")


async def save_news_to_db(news_items: List[Dict[str, Any]]) -> int:
    """Пакетна вставка нових новин, ігноруючи дублікати за url."""
    if not news_items or not db_pool:
        return 0
    
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
            result = await conn.fetch(sql, sources, urls, titles, summaries, image_urls, published_at_list)
            return len(result)
    except asyncpg.exceptions.PostgresError as e:
        logger.error(f"❌ Помилка пакетної вставки в БД: {e}")
        return 0

async def get_unique_news_from_db(limit: int) -> List[Dict[str, Any]]:
    """Отримує найновіші неопубліковані новини з фото."""
    if not db_pool:
        return []

    sql = """
        SELECT url, title, summary, image_url, source, published_at
        FROM news
        WHERE is_posted = FALSE
          AND image_url IS NOT NULL AND image_url != '' 
        ORDER BY 
            published_at DESC 
        LIMIT $1;
    """
    try:
        async with db_pool.acquire() as conn:
            records = await conn.fetch(sql, limit)
            return [dict(record) for record in records]
    except asyncpg.exceptions.PostgresError as e:
        logger.error(f"❌ Помилка вибірки з БД: {e}")
        return []

async def mark_news_as_posted(urls: List[str]):
    """Позначає список новин як опубліковані."""
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
    except asyncpg.exceptions.PostgresError as e:
        logger.error(f"❌ Помилка оновлення is_posted в БД: {e}")

async def cleanup_db():
    """Видаляє старі записи з БД."""
    if not db_pool:
        return
        
    cleanup_time = datetime.now(KYIV_TZ) - timedelta(days=Config.DB_CLEANUP_DAYS)
    
    sql = """
        DELETE FROM news
        WHERE inserted_at < $1;
    """
    try:
        async with db_pool.acquire() as conn:
            deleted_count = await conn.execute(sql, cleanup_time)
            match = re.search(r'DELETE (\d+)', deleted_count)
            count = int(match.group(1)) if match else 0
            logger.info(f"🧹 Обслуговування DB: Видалено {count} старих записів.")
            return count
    except asyncpg.exceptions.PostgresError as e:
        logger.error(f"❌ Помилка очистки БД: {e}")
        return 0

async def get_db_stats() -> Dict[str, Any]:
    """Повертає статистику по записах у БД."""
    if not db_pool:
        return {}
        
    sql = """
        SELECT 
            (SELECT count(*) FROM news) AS total_news,
            (SELECT count(*) FROM news WHERE is_posted = TRUE) AS posted_news,
            (SELECT count(*) FROM news WHERE is_posted = FALSE) AS unposted_news,
            (SELECT count(DISTINCT source) FROM news) AS total_sources;
    """
    try:
        async with db_pool.acquire() as conn:
            record = await conn.fetchrow(sql)
            return dict(record) if record else {}
    except asyncpg.exceptions.PostgresError as e:
        logger.error(f"❌ Помилка отримання статистики з БД: {e}")
        return {}

# --- 3. ХЕЛПЕРИ ПАРСИНГУ ---

def is_news_relevant(title: str, summary: str) -> bool:
    """Фільтрує новини за нерелевантними ключовими словами (Шоу-бізнес, Спорт)."""
    if not title and not summary:
        return False
        
    text = (title + " " + summary).lower()
    
    celebrity_keywords = [
        "зірок", "шоу-бізнес", "світське життя", "особисте життя", 
        "вагітність", "розлучення", "скандал", "мода", "гламур", "голлівуд"
    ]
    
    football_keywords = [
        "футбол", "матч", "ліга чемпіонів", "ліга європи", "динамо", "шахтар",
        "фк ", "борусія", "реал", "барселона"
    ]

    for keyword in celebrity_keywords + football_keywords:
        if keyword in text:
            logger.debug(f"Пропущено новину (ФІЛЬТР): {title[:50]}...")
            return False
            
    return True
    
def normalize_summary(text: str) -> str:
    """Очищує HTML та обрізає текст опису."""
    if not text:
        return ""
    # Використовуємо Beautiful Soup для очищення від HTML/XML тегів
    soup = BeautifulSoup(text, 'html.parser')
    clean_text = soup.get_text()
    # Видаляємо зайві пробіли та переноси рядків
    clean_text = ' '.join(clean_text.split())
    # Обрізаємо до ліміту (для Telegram)
    return clean_text[:800].strip()

def extract_image_url(entry: feedparser.FeedParserDict) -> str:
    """Намагається знайти дійсний URL зображення у різних полях RSS-запису."""
    image_url = ""

    # 1. Пошук у media:content (найпоширеніший стандарт)
    if 'media_content' in entry:
        for media in entry.media_content:
            if media.get('url') and ('image' in media.get('type', '') or 'image' in media.get('medium', '')):
                image_url = media['url']
                break
    
    # 2. Пошук у media:thumbnail
    if not image_url and 'media_thumbnail' in entry and entry.media_thumbnail:
        image_url = entry.media_thumbnail[0].get('url', '')
    
    # 3. Пошук у summary/description HTML
    if not image_url and entry.get('summary'):
        # Уникнення попередження, якщо summary виглядає як файл
        soup = BeautifulSoup(entry.summary, 'html.parser') 
        img = soup.find('img')
        if img and img.get('src'):
            image_url = img['src']
            
    if image_url:
        # Валідація та очищення URL
        if not image_url.startswith(('http://', 'https://')):
             return ""
             
        # Видалення параметрів запиту та хешів
        clean_url = image_url.split('?')[0].split('#')[0]

        # Перевірка на розширення файлу зображення
        if re.search(r'\.(jpe?g|png|gif|webp|tiff|svg|ico|bmp|avif)\b', clean_url.lower()):
            return image_url
            
    return ""

def parse_published_time(entry: feedparser.FeedParserDict) -> datetime:
    """Витягує час публікації та конвертує його в Київський час."""
    published = entry.get('published_parsed') or entry.get('updated_parsed')
    
    if published:
        try:
            # Створення datetime у UTC
            published_utc = datetime(*published[:6], tzinfo=timezone.utc)
            # Конвертація в Kyiv time zone
            return published_utc.astimezone(KYIV_TZ)
        except Exception:
            pass
            
    # Якщо час не знайдено, використовуємо поточний час
    return datetime.now(KYIV_TZ)

# --- 4. ГЕНЕРАЦІЯ ХЕШТЕГІВ ---

def generate_hashtags(title: str, source: str) -> str:
    """Генерує до 5 релевантних хештегів на основі заголовка та джерела."""
    
    # Стоп-слова (базовий набір)
    stop_words = set([
        'на', 'в', 'у', 'з', 'до', 'про', 'від', 'для', 'це', 'що', 'як',
        'та', 'але', 'і', 'по', 'за', 'під', 'над', 'коли', 'буде', 'було', 'є',
        'він', 'вона', 'воно', 'вони', 'ми', 'ви', 'тисяч', 'мільйонів', 'може' 
    ])
    
    # 1. Очистка та нормалізація заголовка
    clean_title = re.sub(r'[^\w\s]', '', title).lower()
    words = clean_title.split()
    
    # 2. Фільтрація слів
    # Залишаємо слова довше 3 символів та не стоп-слова
    filtered_words = [word for word in words if word not in stop_words and len(word) > 3]
    
    # 3. Формування хештегу джерела
    # Очищаємо домен (напр., pravda.com.ua -> Pravda)
    clean_source = source.split('.')[0].replace('-', '').replace('_', '')
    source_tag = f"#{clean_source.capitalize()}"

    # 4. Формування хештегів з заголовка
    # Беремо перші 3-4 унікальні слова
    unique_words = list(set(filtered_words))[:4]
    title_tags = [f"#{word.capitalize()}" for word in unique_words]
    
    # 5. Комбінування
    # Додаємо загальні хештеги в кінець
    all_tags = [source_tag] + title_tags
    all_tags = list(dict.fromkeys(all_tags)) # Зберігаємо порядок, видаляємо дублікати
    
    return " ".join(all_tags[:5]) + " #НовиниУкраїни #Новини"

# --- 5. ОСНОВНИЙ ПАРСИНГ ---

async def fetch_and_parse_source(session: aiohttp.ClientSession, rss_url: str) -> List[Dict[str, Any]]:
    """Отримує, парсить та фільтрує новини з одного RSS-джерела."""
    news_items = []
    source_domain = urlparse(rss_url).netloc.replace('www.', '')
    
    try:
        async with session.get(rss_url, headers=Config.DEFAULT_HEADERS, timeout=Config.HTTP_TIMEOUT) as response:
            if response.status != 200:
                logger.warning(f"⚠️ HTTP Помилка {response.status} при отриманні RSS для {rss_url}")
                return []
            
            content = await response.text(encoding=response.charset or 'utf-8')
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        logger.error(f"❌ Помилка мережі/таймауту для {rss_url}: {type(e).__name__} - {e}")
        return []

    feed = feedparser.parse(content)
    now_kyiv = datetime.now(KYIV_TZ)
    max_age_dt = timedelta(minutes=Config.MAX_AGE_MIN) 

    for entry in feed.entries[:Config.FETCH_LIMIT]:
        try:
            url = entry.link
            title = entry.title
            summary = normalize_summary(entry.get('summary') or entry.get('description') or entry.title)
            
            if not is_news_relevant(title, summary):
                continue

            image_url = extract_image_url(entry)
            published_time = parse_published_time(entry)

            # Фільтр за віком
            if now_kyiv - published_time > max_age_dt:
                continue

            news_items.append({
                'source': source_domain, 'title': title, 'url': url, 
                'summary': summary, 'image_url': image_url, 'published_at': published_time,
            })
        except Exception as e:
            logger.warning(f"Помилка обробки запису з {rss_url}: {e}")
            continue

    return news_items

async def fetch_all_sources() -> Tuple[List[Dict[str, Any]], float]:
    """Запускає одночасний парсинг вибраних випадкових джерел."""
    all_news = []
    start_time = datetime.now()

    num_sources_to_fetch = min(Config.NUM_SOURCES_TO_FETCH, len(Config.SOURCES)) 
    selected_sources = random.sample(Config.SOURCES, num_sources_to_fetch)
    
    logger.info(f"⏳ Парсинг {len(selected_sources)} випадкових джерел (Активних: {len(Config.SOURCES)})...")

    connector = aiohttp.TCPConnector(limit=Config.MAX_CONCURRENCY)
    async with aiohttp.ClientSession(headers=Config.DEFAULT_HEADERS, connector=connector) as session:
        tasks = [fetch_and_parse_source(session, rss_url) for rss_url in selected_sources]
        results = await asyncio.gather(*tasks)

        for news_list in results:
            if news_list:
                all_news.extend(news_list)

    duration = (datetime.now() - start_time).total_seconds()
    
    return all_news, duration

# --- 6. ФОРМАТУВАННЯ ТА ПОСТИНГ ---

def format_news_post(news_item: Dict[str, Any]) -> str:
    """Форматує новину для відправки в Telegram, включаючи хештеги."""
    source_display = news_item['source'].replace('https://', '').replace('http://', '')
    published_time_str = news_item['published_at'].strftime(TIME_FORMAT)
    
    # Основний текст
    message = (
        f"<b>⚡️ {news_item['title']}</b>\n\n"
        f"{news_item['summary']}\n\n"
        f"🕰️ {published_time_str} | <a href='{news_item['url']}'>Подробиці на {source_display}</a>"
    )

    # Генерація та додавання хештегів
    hashtags = generate_hashtags(news_item['title'], source_display)
    message += f"\n\n{hashtags}" 
    
    return message

async def send_news_to_channel(news_to_post: List[Dict[str, Any]]) -> int:
    """Надсилає новини в Telegram-канал."""
    posted_urls = []
    
    for news in news_to_post[:Config.MAX_NEWS_PER_CYCLE]:
        try:
            caption = format_news_post(news)
            
            if news.get('image_url'):
                # Публікація з фото (Telegram має автоматично завантажити фото з URL)
                await bot.send_photo(
                    chat_id=CHANNEL_ID,
                    photo=news['image_url'],
                    caption=caption,
                    parse_mode=ParseMode.HTML,
                    disable_notification=False 
                )
            
            await asyncio.sleep(1.5) 
            posted_urls.append(news['url'])
            
        except TelegramAPIError as e:
            logger.error(f"❌ Telegram API Error для '{news['title'][:50]}...': {e.message}")
            if "Bad Request: chat not found" in e.message and ADMIN_ID:
                 # Спроба надіслати повідомлення адміністратору
                 try:
                     await bot.send_message(ADMIN_ID, f"❌ Критична помилка: CHANNEL_ID ({CHANNEL_ID}) не знайдено або недійсний. Перевірте змінну оточення.", parse_mode=ParseMode.HTML)
                 except Exception:
                     pass
            
            # Позначаємо як опубліковану, щоб не спамити, якщо проблема з URL фото
            if "Bad Request: failed to get HTTP URL content" in e.message or "Bad Request: PHOTO_INVALID" in e.message:
                logger.warning("-> Проблема з URL зображення. Новина буде пропущена.")
                posted_urls.append(news['url']) 
                
            continue
        except Exception as e:
            logger.error(f"❌ Невідома помилка відправки для '{news['title'][:50]}...': {e}")
            continue 

    await mark_news_as_posted(posted_urls)
    return len(posted_urls)

# --- 7. ЦИКЛИ ТА КОМАНДИ АДМІНІСТРАТОРА ---

async def db_cleanup_loop():
    """Асинхронний цикл для періодичного очищення бази даних."""
    # Нескінченний цикл, який запускається після ініціалізації бота
    while True:
        await asyncio.sleep(Config.CLEANUP_INTERVAL_HOURS * 3600) 
        logger.info("--- ♻️ Запуск фонової очистки БД ---")
        await cleanup_db()

async def auto_posting_loop(bot_instance: Bot):
    """Головний цикл, який періодично перевіряє та публікує новини."""
    while True:
        try:
            logger.info("--- 🚀 Запуск циклу автопостингу ---")
            
            # 1. Парсинг і збереження новин
            fetched_news, parse_duration = await fetch_all_sources()
            new_count = await save_news_to_db(fetched_news)
            logger.info(f"💾 Успішно вставлено {new_count} новин.")

            # 2. Отримуємо новини для публікації
            news_to_post = await get_unique_news_from_db(Config.MAX_NEWS_PER_CYCLE)
            
            # 3. Публікація 
            post_start_time = datetime.now()
            posted_count = await send_news_to_channel(news_to_post)
            post_duration = (datetime.now() - post_start_time).total_seconds()
            
            logger.info(
                f"--- ✅ Цикл завершено. Нових: {new_count}. Постів: {posted_count}. Таймінги: Парсинг={parse_duration:.2f}с, Постинг={post_duration:.2f}с ---"
            )
            
        except Exception as e:
            logger.critical(f"❌ Критична помилка в циклі автопостингу: {e}", exc_info=True)

        await asyncio.sleep(Config.POSTING_INTERVAL_MIN * 60)
        logger.info(f"Очікування {Config.POSTING_INTERVAL_MIN} хвилин...")


# --- КОМАНДИ АДМІНІСТРАТОРА ---

async def cmd_status(message: types.Message):
    """Показує поточний статус бота та конфігурацію."""
    config_msg = (
        "<b>🤖 Статус Платформи Новин:</b>\n\n"
        "<b>⚙️ Конфігурація:</b>\n"
        f"  🌐 Режим: <b>WEBHOOK</b>\n"
        f"  ⏳ Інтервал: {Config.POSTING_INTERVAL_MIN} хв\n"
        f"  ⏱️ Макс. вік новини: {Config.MAX_AGE_MIN} хв\n"
        f"  📝 Макс. постів за цикл: <b>{Config.MAX_NEWS_PER_CYCLE}</b>\n"
        f"  📸 Вимога: Тільки пости <b>З ФОТО</b>\n"
        f"  🧹 Чистка DB: Раз на {Config.CLEANUP_INTERVAL_HOURS} год.\n"
        f"  📰 Джерел у списку: {len(Config.SOURCES)}\n\n"
        "<b>🔑 Сервісні параметри:</b>\n"
        f"  Керування: <code>/forcepost</code>, <code>/stats</code>\n"
        f"  📢 Channel ID: <code>{CHANNEL_ID}</code>"
    )
    await message.answer(config_msg, parse_mode=ParseMode.HTML)

async def cmd_forcepost(message: types.Message):
    """Примусово запускає цикл парсингу та постингу."""
    await message.answer("♻️ Примусовий запуск циклу парсингу...")
    
    async def run_once(bot_instance):
        """Одинарний запуск основної логіки."""
        try:
            start_time = datetime.now()
            fetched_news, parse_duration = await fetch_all_sources()
            new_count = await save_news_to_db(fetched_news)
            
            news_to_post = await get_unique_news_from_db(Config.MAX_NEWS_PER_CYCLE) 
            
            post_start_time = datetime.now()
            posted_count = await send_news_to_channel(news_to_post)
            post_duration = (datetime.now() - post_start_time).total_seconds()
            
            result_msg = (
                "✅ <b>Цикл примусового постингу завершено!</b>\n"
                f"   • Знайдено нових новин: {new_count}\n"
                f"   • Опубліковано новин: {posted_count}\n"
                f"   • Таймінг (Парсинг): {parse_duration:.2f} сек\n"
                f"   • Таймінг (Постинг): {post_duration:.2f} сек"
            )
        except Exception as e:
            result_msg = f"❌ <b>Критична помилка примусового постингу:</b> {e}"
        
        await bot_instance.send_message(message.chat.id, result_msg, parse_mode=ParseMode.HTML)

    # Створюємо та запускаємо завдання у головному циклі подій
    loop = asyncio.get_event_loop()
    loop.create_task(run_once(bot))


async def cmd_stats(message: types.Message):
    """Показує статистику бази даних."""
    stats = await get_db_stats()
    
    if stats:
        stats_msg = (
            "📊 <b>Статистика Бази Даних:</b>\n\n"
            f"• 📝 Всього новин у DB: {stats.get('total_news', 0)}\n"
            f"• ✅ Опубліковано: {stats.get('posted_news', 0)}\n"
            f"• 📦 У черзі (З ФОТО): {stats.get('unposted_news', 0)}\n"
            f"• 📰 Активних джерел: {stats.get('total_sources', 0)}"
        )
    else:
        stats_msg = "❌ Не вдалося отримати статистику з бази даних."

    await message.answer(stats_msg, parse_mode=ParseMode.HTML)


# --- 8. ЗАПУСК БОТА (WEBHOOK) ---

async def main():
    """Основна функція для ініціалізації та запуску бота через Webhook."""
    
    # Перевірка всіх необхідних змінних
    if not all([BOT_TOKEN, DATABASE_URL, CHANNEL_ID, WEBHOOK_HOST, WEBHOOK_SECRET]):
        logger.critical("Критична помилка: Не задані BOT_TOKEN, DATABASE_URL, CHANNEL_ID, WEBHOOK_HOST або WEBHOOK_SECRET.")
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
    
    # Реєстрація команд
    dp.message.register(cmd_status, Command("status"), F.from_user.id == ADMIN_ID)
    dp.message.register(cmd_forcepost, Command("forcepost"), F.from_user.id == ADMIN_ID)
    dp.message.register(cmd_stats, Command("stats"), F.from_user.id == ADMIN_ID)

    # Запускаємо фонові цикли (парсинг/постинг та очистка БД)
    loop = asyncio.get_event_loop()
    loop.create_task(auto_posting_loop(bot))
    loop.create_task(db_cleanup_loop())
    logger.info("Бот запущено. Початок роботи (WEBHOOK MODE).")
    
    runner = None
    try:
        # 1. Встановлюємо Webhook на сервері Telegram
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
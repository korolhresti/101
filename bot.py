import os
import asyncio
import logging
import re
import random
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse, urljoin

import asyncpg
import aiohttp
import feedparser
from bs4 import BeautifulSoup

from aiogram import Bot, Dispatcher, types, F
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.client.default import DefaultBotProperties 
from aiogram.exceptions import TelegramAPIError

# --- 1. НАЛАШТУВАННЯ І КОНСТАНТИ (ПРОФЕСІЙНА КОНФІГУРАЦІЯ) ---

# Використовуйте Kyiv time zone (UTC+3)
KYIV_TZ = timezone(timedelta(hours=3), 'Europe/Kyiv')
# Формат часу для відображення
TIME_FORMAT = "%H:%M" 

class Config:
    """Конфігурація платформи, зібрана в одному місці."""
    
    # ⚙️ ОСНОВНІ ПАРАМЕТРИ ЦИКЛУ (Встановлено згідно з фінальним запитом)
    POSTING_INTERVAL_MIN = 5  # Кожні 5 хвилин
    MAX_NEWS_PER_CYCLE = 3   # СТРОГИЙ ЛІМІТ: До 3 новин за цикл (ТОП-3)
    MAX_AGE_MIN = 30          # Не публікувати новини старше 30 хвилин (Симуляція Топ-новин за переглядами)
    
    # 🛡️ ПАРАМЕТРИ НАДІЙНОСТІ ТА ПРОДУКТИВНОСТІ
    FETCH_LIMIT = 30          # Макс. кількість записів для аналізу в кожному RSS-фіді (ЗБІЛЬШЕНО)
    NUM_SOURCES_TO_FETCH = 20 # Кількість випадкових джерел, які будуть перевірені за цикл (ЗБІЛЬШЕНО)
    HTTP_TIMEOUT = 15         # Таймаут HTTP-запиту в секундах
    MAX_CONCURRENCY = 15      # Макс. кількість одночасних HTTP-з'єднань
    
    # 💾 ПАРАМЕТРИ ОБСЛУГОВУВАННЯ БАЗИ ДАНИХ
    DB_CLEANUP_DAYS = 7       # Видаляти новини, старші за 7 днів
    CLEANUP_INTERVAL_HOURS = 1 # Інтервал очищення (кожна година)
    
    DEFAULT_HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36', 
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'uk-UA,uk;q=0.9,en-US;q=0.8,en;q=0.7',
    }

    # 1. 📰 Джерела новин 
    SOURCES = [
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
        "https://hromadske.ua/feed/news", "https://biz.censor.net/rss" # Додано нове джерело
    ]


# Налаштування логування
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Змінні середовища
BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
CHANNEL_ID = os.getenv("CHANNEL_ID")
try:
    ADMIN_ID = int(os.getenv("ADMIN_ID", 0))
except ValueError:
    ADMIN_ID = 0

# Глобальні змінні для DB
db_pool = None
dp: Dispatcher = None
bot: Bot = None

# --- 2. БАЗА ДАНИХ (POSTGRESQL/NEON) ---

async def connect_db():
    """Створює пул з'єднань до бази даних Neon (PostgreSQL)."""
    global db_pool
    try:
        # Встановлення параметрів пулу для стабільності
        db_pool = await asyncpg.create_pool(
            DATABASE_URL,
            min_size=1, 
            max_size=10,
            timeout=5 # Таймаут на отримання з'єднання
        )
        logger.info("✅ Успішно підключено до Neon PostgreSQL.")
    except Exception as e:
        logger.error(f"❌ Критична помилка підключення до DB: {e}")
        await asyncio.sleep(60)
        exit(1)

async def init_db():
    """Створює таблицю 'news', якщо вона не існує, та додає необхідні стовпці."""
    if not db_pool:
        return

    async with db_pool.acquire() as conn:
        # ... (Код створення таблиці та індексів залишено без змін)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS news (
                id SERIAL PRIMARY KEY,
                source VARCHAR(255) NOT NULL,
                url TEXT UNIQUE NOT NULL,
                title TEXT NOT NULL,
                summary TEXT,
                image_url TEXT,
                published_at TIMESTAMP WITH TIME ZONE NOT NULL,
                inserted_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        try:
            await conn.execute("""
                ALTER TABLE news ADD COLUMN is_posted BOOLEAN DEFAULT FALSE;
            """)
        except asyncpg.exceptions.DuplicateColumnError:
            pass 
        
        try:
            await conn.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS news_url_idx ON news (url);
            """)
        except Exception as e:
            logger.error(f"Помилка при створенні індексу: {e}")

    logger.info("Таблиця 'news' перевірена/оновлена.")


async def save_news_to_db(news_items: list):
    """Зберігає список новин у базу даних, використовуючи пакетну вставку."""
    if not news_items or not db_pool:
        return 0
    
    sql = """
        INSERT INTO news (source, url, title, summary, image_url, published_at)
        SELECT * FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::timestamptz[])
        ON CONFLICT (url) DO NOTHING
        RETURNING id;
    """
    
    # Використання генераторів для ефективного формування списків
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

async def get_unique_news_from_db(limit: int) -> list:
    """Вибирає ТОП-N найновіших, ще не опублікованих новин ТІЛЬКИ З КАРТИНКОЮ."""
    if not db_pool:
        return []

    # SQL-запит оптимізовано для швидкодії
    sql = """
        SELECT url, title, summary, image_url, source, published_at
        FROM news
        WHERE is_posted = FALSE
          AND image_url IS NOT NULL AND image_url != '' -- СТРОГИЙ ФІЛЬТР: Тільки пости з зображенням
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

async def mark_news_as_posted(urls: list):
    """Позначає новини, що були успішно опубліковані."""
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
    """Видаляє старі новини для обслуговування бази даних."""
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

async def get_db_stats():
    """Повертає статистику бази даних."""
    # ... (Код статистики залишено без змін)
    if not db_pool:
        return None
        
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
            return dict(record) if record else None
    except asyncpg.exceptions.PostgresError as e:
        logger.error(f"❌ Помилка отримання статистики з БД: {e}")
        return None

# --- 3. ХЕЛПЕРИ ПАРСИНГУ ---

def is_news_relevant(title: str, summary: str) -> bool:
    """Перевіряє, чи не стосується новина заблокованих тем (зірки, футбол)."""
    # ... (Код фільтрації залишено без змін)
    if not title and not summary:
        return False
        
    text = (title + " " + summary).lower()
    
    celebrity_keywords = [
        "зірок", "зірка", "шоу-бізнес", "світське життя", "особисте життя", 
        "відпочинок", "вагітність", "розлучення", "скандал", "тсн.особливе",
        "телебачення", "кіно", "мода", "гламур", "новини зірок", "зіркова пара",
        "голлівуд", "селебриті"
    ]
    
    football_keywords = [
        "футбол", "матч", "ліга чемпіонів", "ліга європи", "збірна україни з футболу", 
        "чемпіонат світу з футболу", "чемпіонат україни з футболу", "прем'єр-ліга",
        "динамо", "шахтар", "фк ", "борусія", "реал", "барселона"
    ]

    for keyword in celebrity_keywords:
        if keyword in text:
            logger.debug(f"Пропущено новину (ЗІРКИ/ШОУ): {title[:50]}...")
            return False

    for keyword in football_keywords:
        if keyword in text:
            logger.debug(f"Пропущено новину (ФУТБОЛ): {title[:50]}...")
            return False
            
    return True
    
def normalize_summary(text: str) -> str:
    """Очищає та нормалізує текст анотації."""
    if not text:
        return ""
    soup = BeautifulSoup(text, 'html.parser')
    clean_text = soup.get_text()
    clean_text = ' '.join(clean_text.split())
    return clean_text[:400].strip()

def extract_image_url(entry) -> str:
    """Витягує URL зображення та СТРОГО перевіряє його на валідність."""
    # ... (Код витягування зображення залишено без змін, оскільки він вже оптимізований)
    image_url = ""

    if 'media_content' in entry:
        for media in entry.media_content:
            if 'image' in media.get('type', '') or 'image' in media.get('medium', ''):
                image_url = media.get('url', '')
                break
    
    if not image_url and 'media_thumbnail' in entry and entry.media_thumbnail:
        image_url = entry.media_thumbnail[0].get('url', '')
    
    if not image_url and 'tags' in entry:
        for tag in entry.tags:
            if tag.get('term') == 'enclosure' and tag.get('url'):
                 image_url = tag['url']

    if not image_url and entry.get('summary'):
        soup = BeautifulSoup(entry.summary, 'html.parser')
        img = soup.find('img')
        if img and img.get('src'):
            image_url = img['src']
            
    if image_url:
        if not image_url.startswith(('http://', 'https://')):
             return ""
             
        clean_url = image_url.split('?')[0].split('#')[0]

        if re.search(r'\.(jpe?g|png|gif|webp|tiff|svg|ico|bmp|tga|avif)\b', clean_url.lower()):
            return image_url
            
    return ""

def parse_published_time(entry, rss_url) -> datetime:
    """Парсить та нормалізує час публікації до часового поясу Києва."""
    published = entry.get('published_parsed') or entry.get('updated_parsed')
    
    if published:
        try:
            published_utc = datetime(*published[:6], tzinfo=timezone.utc)
            return published_utc.astimezone(KYIV_TZ)
        except Exception:
            pass
            
    return datetime.now(KYIV_TZ)

# --- 4. ОСНОВНИЙ ПАРСИНГ ---

async def fetch_and_parse_source(session, rss_url: str):
    """Парсить одне джерело."""
    news_items = []
    source_domain = urlparse(rss_url).netloc.replace('www.', '')
    
    # Використовуємо async with для автоматичного закриття ресурсу
    try:
        async with session.get(rss_url, headers=Config.DEFAULT_HEADERS, timeout=Config.HTTP_TIMEOUT) as response:
            if response.status != 200:
                logger.warning(f"⚠️ HTTP Помилка {response.status} при отриманні RSS для {rss_url}")
                return []
            
            # Обробка кодування вмісту
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
            published_time = parse_published_time(entry, rss_url)

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

async def fetch_all_sources():
    """Асинхронно отримує новини з випадково обраних джерел."""
    all_news = []
    start_time = datetime.now()

    num_sources_to_fetch = min(Config.NUM_SOURCES_TO_FETCH, len(Config.SOURCES)) 
    selected_sources = random.sample(Config.SOURCES, num_sources_to_fetch)
    
    logger.info(f"⏳ Парсинг {len(selected_sources)} випадкових джерел...")

    # Використовуємо TCPConnector для обмеження одночасних з'єднань
    connector = aiohttp.TCPConnector(limit=Config.MAX_CONCURRENCY)
    async with aiohttp.ClientSession(headers=Config.DEFAULT_HEADERS, connector=connector) as session:
        tasks = [fetch_and_parse_source(session, rss_url) for rss_url in selected_sources]
        results = await asyncio.gather(*tasks)

        for news_list in results:
            if news_list:
                all_news.extend(news_list)

    duration = (datetime.now() - start_time).total_seconds()
    logger.info(f"📰 Знайдено {len(all_news)} новин з {len(selected_sources)} джерел за {duration:.2f} сек.")
    
    return all_news, duration

# --- 5. ФОРМАТУВАННЯ ТА ПОСТИНГ ---

def format_news_post(news_item: dict) -> str:
    """Форматує новину для публікації у Telegram (HTML) з часом публікації."""
    source_display = news_item['source'].replace('https://', '').replace('http://', '')
    
    # Використовуємо час публікації новини з бази
    published_time_str = news_item['published_at'].strftime(TIME_FORMAT)
    
    message = (
        f"<b>⚡️ {news_item['title']}</b>\n\n"
        f"{news_item['summary']}\n\n"
        f"🕰️ {published_time_str} | <a href='{news_item['url']}'>Подробиці на {source_display}</a>"
    )
    return message

async def send_news_to_channel(news_to_post: list):
    """Публікує новини у канал, використовуючи send_photo."""
    
    posted_urls = []
    
    for news in news_to_post[:Config.MAX_NEWS_PER_CYCLE]:
        try:
            caption = format_news_post(news)
            
            if news.get('image_url'):
                # Публікація з фото (MUST BE PHOTO)
                await bot.send_photo(
                    chat_id=CHANNEL_ID,
                    photo=news['image_url'],
                    caption=caption,
                    parse_mode=ParseMode.HTML,
                    disable_notification=False 
                )
            
            await asyncio.sleep(1.5) # Збільшена затримка для стабільності
            posted_urls.append(news['url'])
            
        except TelegramAPIError as e:
            # Обробка помилок Telegram (наприклад, невалідний URL фото, або занадто велике фото)
            logger.error(f"❌ Telegram API Error для '{news['title'][:50]}...': {e.message}")
            if "Bad Request: failed to get HTTP URL content" in e.message:
                logger.warning("-> Проблема з URL зображення. Новина не буде повторно опублікована.")
                posted_urls.append(news['url']) # Позначаємо як опубліковану, щоб не спамити
            continue
        except Exception as e:
            logger.error(f"❌ Невідома помилка відправки для '{news['title'][:50]}...': {e}")
            continue 

    await mark_news_as_posted(posted_urls)
    return len(posted_urls)

# --- 6. ОСНОВНИЙ ЦИКЛ АВТОПОСТИНГУ ---

async def db_cleanup_loop():
    """Асинхронний цикл для періодичного очищення бази даних."""
    while True:
        await asyncio.sleep(Config.CLEANUP_INTERVAL_HOURS * 3600) # Чекаємо 1 годину
        logger.info("--- ♻️ Запуск фонової очистки БД ---")
        await cleanup_db()

async def auto_posting_loop(bot: Bot):
    """Головний цикл, який періодично перевіряє та публікує новини."""
    while True:
        try:
            logger.info("--- 🚀 Запуск циклу автопостингу ---")
            
            # 1. Парсинг і збереження новин
            fetched_news, parse_duration = await fetch_all_sources()
            new_count = await save_news_to_db(fetched_news)
            logger.info(f"💾 Успішно вставлено {new_count} новин.")

            # 2. Отримуємо ТОП-3 новини
            news_to_post = await get_unique_news_from_db(Config.MAX_NEWS_PER_CYCLE)
            
            # 3. Публікація 
            post_start_time = datetime.now()
            posted_count = await send_news_to_channel(news_to_post)
            post_duration = (datetime.now() - post_start_time).total_seconds()
            
            logger.info(
                f"--- ✅ Цикл завершено. Постів: {posted_count}. Таймінги: Парсинг={parse_duration:.2f}с, Постинг={post_duration:.2f}с ---"
            )
            
        except Exception as e:
            logger.critical(f"❌ Критична помилка в циклі автопостингу: {e}")

        await asyncio.sleep(Config.POSTING_INTERVAL_MIN * 60)
        logger.info(f"Очікування {Config.POSTING_INTERVAL_MIN} хвилин...")

# --- 7. КОМАНДИ АДМІНІСТРАТОРА ---

async def cmd_status(message: types.Message):
    """Показує поточний статус бота та конфігурацію."""
    config_msg = (
        "<b>🤖 Статус Платформи Новин (Професійний режим):</b>\n\n"
        "<b>⚙️ Конфігурація:</b>\n"
        f"  ⏳ Інтервал: {Config.POSTING_INTERVAL_MIN} хв\n"
        f"  ⏱️ Макс. вік новини: {Config.MAX_AGE_MIN} хв\n"
        f"  📝 Макс. постів за цикл: <b>{Config.MAX_NEWS_PER_CYCLE} (ТОП-3)</b>\n"
        f"  📸 Вимога: Тільки пости <b>З ФОТО</b>\n"
        f"  🧹 Чистка DB: Раз на {Config.CLEANUP_INTERVAL_HOURS} год. (Видаляються записи старше {Config.DB_CLEANUP_DAYS} дн.)\n"
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


# --- 8. ЗАПУСК БОТА ---

async def main():
    """Основна функція для ініціалізації та запуску бота."""
    
    if not all([BOT_TOKEN, DATABASE_URL, CHANNEL_ID]):
        logger.critical("Критична помилка: Не задані BOT_TOKEN, DATABASE_URL або CHANNEL_ID.")
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
    
    dp.message.register(cmd_status, Command("status"), F.from_user.id == ADMIN_ID)
    dp.message.register(cmd_forcepost, Command("forcepost"), F.from_user.id == ADMIN_ID)
    dp.message.register(cmd_stats, Command("stats"), F.from_user.id == ADMIN_ID)

    loop = asyncio.get_event_loop()
    # Запускаємо основний цикл та фоновий цикл очистки БД
    loop.create_task(auto_posting_loop(bot))
    loop.create_task(db_cleanup_loop())
    logger.info("Бот запущено. Початок роботи.")

    try:
        for i in range(3):
            try:
                await bot.delete_webhook(drop_pending_updates=True) 
                logger.info("Webhook успішно вимкнено.")
                break
            except Exception as e:
                logger.warning(f"Помилка вимкнення Webhook: {e}. Затримка 5 сек...")
                await asyncio.sleep(5)

        await dp.start_polling(bot)
    finally:
        if bot:
            await bot.session.close()
        if db_pool:
            await db_pool.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Бот зупинено вручну.")
    except Exception as e:
        logger.critical(f"❌ Головна помилка виконання: {e}")
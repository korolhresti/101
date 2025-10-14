import os
import asyncio
import logging
import re
import random
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

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

class Config:
    """Конфігурація платформи, зібрана в одному місці."""
    
    # ⚙️ НОВІ ПАРАМЕТРИ ЦИКЛУ (ЕКОНОМІЯ РЕСУРСІВ)
    NEWS_GATHERING_INTERVAL_MIN = 20  # Інтервал збору новин (раз на 20 хв)
    POSTING_INTERVAL_MIN = 5         # Інтервал публікації (кожні 5 хв)
    
    MAX_NEWS_PER_POSTING_CYCLE = 1 # Публікувати по ОДНІЙ новині за раз
    MAX_AGE_MIN = 120                # Не публікувати новини старше 120 хвилин
    
    # 🛡️ ПАРАМЕТРИ НАДІЙНОСТІ ТА ПРОДУКТИВНОСТІ
    FETCH_LIMIT = 30
    NUM_SOURCES_TO_FETCH = 25
    HTTP_TIMEOUT = 15
    MAX_CONCURRENCY = 15
    
    # 💾 ПАРАМЕТРИ ОБСЛУГОВУВАННЯ БАЗИ ДАНИХ
    DB_CLEANUP_DAYS = 7
    CLEANUP_INTERVAL_HOURS = 1
    
    DEFAULT_HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'uk-UA,uk;q=0.9,en-US;q=0.8,en;q=0.7',
    }

    # 1. 📰 Джерела новин (З акцентом на фінанси)
    SOURCES = [
        # Фінанси та економіка
        "https://minfin.com.ua/rss/news/",
        "https://news.finance.ua/ua/rss",
        "https://biz.censor.net/rss",
        "https://www.epravda.com.ua/rss/", # Економічна правда
        "https://mind.ua/rss/news", # Mind.ua
        # Загальні авторитетні джерела
        "https://tsn.ua/rss/all.xml", "https://www.pravda.com.ua/rss/news/",
        "https://censor.net/rss/all_news", "https://www.rbc.ua/static/rss/all.xml",
        "https://www.ukrinform.ua/rss/all.xml", "https://www.liga.net/rss/news.xml",
        "https://www.obozrevatel.com/rss/main.xml", "https://focus.ua/rss/latest.xml",
        "https://ua.korrespondent.net/rss/all", "https://gazeta.ua/rss/all",
        "https://24tv.ua/rss/all.xml", "https://nv.ua/ukr/rss/all.xml",
        "https://delo.ua/rss/all.xml", "https://suspilne.media/feed/",
        "https://www.bbc.com/ukrainian/rss.xml", "https://www.unian.ua/rss/news.rss",
        "https://ua.interfax.com.ua/news/ukraine.rss", "https://zaxid.net/rss",
        "https://hromadske.ua/feed/news",
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
# (Код бази даних залишається без значних змін, оскільки він вже оптимізований)

async def connect_db():
    """Створює пул з'єднань до бази даних Neon (PostgreSQL)."""
    global db_pool
    try:
        db_pool = await asyncpg.create_pool(
            DATABASE_URL, min_size=1, max_size=10, timeout=5
        )
        logger.info("✅ Успішно підключено до Neon PostgreSQL.")
    except Exception as e:
        logger.error(f"❌ Критична помилка підключення до DB: {e}")
        await asyncio.sleep(60)
        exit(1)

async def init_db():
    """Створює таблицю 'news', якщо вона не існує."""
    if not db_pool: return
    async with db_pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS news (
                id SERIAL PRIMARY KEY, source VARCHAR(255) NOT NULL,
                url TEXT UNIQUE NOT NULL, title TEXT NOT NULL, summary TEXT,
                image_url TEXT, published_at TIMESTAMP WITH TIME ZONE NOT NULL,
                inserted_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                is_posted BOOLEAN DEFAULT FALSE
            );
            CREATE UNIQUE INDEX IF NOT EXISTS news_url_idx ON news (url);
        """)
        try:
            await conn.execute("ALTER TABLE news ADD COLUMN is_posted BOOLEAN DEFAULT FALSE;")
        except asyncpg.exceptions.DuplicateColumnError: pass
    logger.info("Таблиця 'news' перевірена/оновлена.")

async def save_news_to_db(news_items: list):
    """Зберігає список новин у базу даних, використовуючи пакетну вставку."""
    if not news_items or not db_pool: return 0
    sql = """
        INSERT INTO news (source, url, title, summary, image_url, published_at)
        SELECT * FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::timestamptz[])
        ON CONFLICT (url) DO NOTHING RETURNING id;
    """
    try:
        async with db_pool.acquire() as conn:
            result = await conn.fetch(sql,
                [item['source'] for item in news_items], [item['url'] for item in news_items],
                [item['title'] for item in news_items], [item['summary'] for item in news_items],
                [item['image_url'] for item in news_items], [item['published_at'] for item in news_items]
            )
            return len(result)
    except asyncpg.exceptions.PostgresError as e:
        logger.error(f"❌ Помилка пакетної вставки в БД: {e}")
        return 0

async def get_unique_news_from_db(limit: int) -> list:
    """Вибирає найновіші, ще не опубліковані новини з картинкою."""
    if not db_pool: return []
    sql = """
        SELECT url, title, summary, image_url, source, published_at FROM news
        WHERE is_posted = FALSE AND image_url IS NOT NULL AND image_url != ''
        ORDER BY published_at DESC LIMIT $1;
    """
    try:
        async with db_pool.acquire() as conn:
            return [dict(record) for record in await conn.fetch(sql, limit)]
    except asyncpg.exceptions.PostgresError as e:
        logger.error(f"❌ Помилка вибірки з БД: {e}")
        return []

async def mark_news_as_posted(urls: list):
    """Позначає новини, що були успішно опубліковані."""
    if not urls or not db_pool: return
    try:
        async with db_pool.acquire() as conn:
            await conn.execute("UPDATE news SET is_posted = TRUE WHERE url = ANY($1::text[]);", urls)
    except asyncpg.exceptions.PostgresError as e:
        logger.error(f"❌ Помилка оновлення is_posted в БД: {e}")

async def cleanup_db():
    """Видаляє старі новини для обслуговування бази даних."""
    if not db_pool: return
    cleanup_time = datetime.now(KYIV_TZ) - timedelta(days=Config.DB_CLEANUP_DAYS)
    try:
        async with db_pool.acquire() as conn:
            deleted_count_str = await conn.execute("DELETE FROM news WHERE inserted_at < $1;", cleanup_time)
            count = int(re.search(r'DELETE (\d+)', deleted_count_str).group(1))
            logger.info(f"🧹 Обслуговування DB: Видалено {count} старих записів.")
    except (asyncpg.exceptions.PostgresError, AttributeError) as e:
        logger.error(f"❌ Помилка очистки БД: {e}")

async def get_db_stats():
    """Повертає статистику бази даних."""
    if not db_pool: return None
    sql = """
        SELECT (SELECT count(*) FROM news) AS total,
               (SELECT count(*) FROM news WHERE is_posted) AS posted,
               (SELECT count(*) FROM news WHERE NOT is_posted) AS unposted,
               (SELECT count(DISTINCT source) FROM news) AS sources;
    """
    try:
        async with db_pool.acquire() as conn:
            return dict(await conn.fetchrow(sql))
    except asyncpg.exceptions.PostgresError as e:
        logger.error(f"❌ Помилка отримання статистики з БД: {e}")
        return None

# --- 3. ХЕЛПЕРИ ПАРСИНГУ ТА ФІЛЬТРАЦІЇ ---

def is_news_relevant(title: str, summary: str) -> bool:
    """Перевіряє, чи не стосується новина заблокованих тем."""
    if not title and not summary: return False
    text = (title + " " + summary).lower()
    
    # Розширений список стоп-слів
    stop_keywords = [
        # Шоу-бізнес
        "зірок", "шоу-бізнес", "світське життя", "особисте життя", "скандал",
        "голлівуд", "селебриті", "гламур", "мода", "королівська родина",
        # Спорт
        "футбол", "матч", "ліга чемпіонів", "ліга європи", "динамо", "шахтар",
        "чемпіонат", "спорт", "бокс", "теніс", "гонка",
        # Бойові дії та фронт (НОВИЙ ФІЛЬТР)
        "фронт", "боєзіткнення", "обстріл", "наступ", "зсу", "окупант", "ворог",
        "атака", "зведення генштабу", "лінія зіткнення", "плацдарм", "контрнаступ"
    ]

    for keyword in stop_keywords:
        if keyword in text:
            logger.debug(f"Пропущено новину (Стоп-слово: '{keyword}'): {title[:50]}...")
            return False
    return True

def normalize_summary(text: str) -> str:
    """Очищає та нормалізує текст анотації."""
    if not text: return ""
    soup = BeautifulSoup(text, 'html.parser')
    clean_text = ' '.join(soup.get_text().split())
    return clean_text[:400].strip()

def extract_image_url(entry) -> str:
    """Витягує URL зображення та перевіряє його на валідність."""
    image_url = ""
    if 'media_content' in entry:
        for media in entry.media_content:
            if 'image' in media.get('type', '') or 'image' in media.get('medium', ''):
                image_url = media.get('url', '')
                break
    if not image_url and 'media_thumbnail' in entry and entry.media_thumbnail:
        image_url = entry.media_thumbnail[0].get('url', '')
    if not image_url and 'summary' in entry:
        img = BeautifulSoup(entry.summary, 'html.parser').find('img')
        if img and img.get('src'): image_url = img['src']
            
    if image_url and image_url.startswith(('http://', 'https://')):
        clean_url = image_url.split('?')[0]
        if re.search(r'\.(jpe?g|png|gif|webp|avif)\b', clean_url.lower()):
            return image_url
    return ""

def parse_published_time(entry) -> datetime:
    """Парсить та нормалізує час публікації до часового поясу Києва."""
    published = entry.get('published_parsed') or entry.get('updated_parsed')
    if published:
        try:
            return datetime(*published[:6], tzinfo=timezone.utc).astimezone(KYIV_TZ)
        except Exception: pass
    return datetime.now(KYIV_TZ)

# --- 4. ОСНОВНИЙ ПАРСИНГ ---

async def fetch_and_parse_source(session, rss_url: str):
    """Парсить одне джерело."""
    news_items = []
    source_domain = urlparse(rss_url).netloc.replace('www.', '')
    try:
        async with session.get(rss_url, timeout=Config.HTTP_TIMEOUT) as response:
            if response.status != 200:
                logger.warning(f"⚠️ HTTP Помилка {response.status} для {rss_url}")
                return []
            content = await response.text(encoding=response.charset or 'utf-8')
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        logger.error(f"❌ Помилка мережі/таймауту для {rss_url}: {e}")
        return []

    feed = feedparser.parse(content)
    max_age_dt = timedelta(minutes=Config.MAX_AGE_MIN)

    for entry in feed.entries[:Config.FETCH_LIMIT]:
        published_time = parse_published_time(entry)
        if datetime.now(KYIV_TZ) - published_time > max_age_dt: continue

        title = entry.title
        summary = normalize_summary(entry.get('summary') or entry.get('description', ''))
        
        if not is_news_relevant(title, summary): continue

        news_items.append({
            'source': source_domain, 'title': title, 'url': entry.link,
            'summary': summary, 'image_url': extract_image_url(entry),
            'published_at': published_time,
        })
    return news_items

async def fetch_all_sources():
    """Асинхронно отримує новини з випадково обраних джерел."""
    start_time = datetime.now()
    selected_sources = random.sample(Config.SOURCES, min(Config.NUM_SOURCES_TO_FETCH, len(Config.SOURCES)))
    logger.info(f"⏳ Парсинг {len(selected_sources)} випадкових джерел...")

    connector = aiohttp.TCPConnector(limit=Config.MAX_CONCURRENCY)
    async with aiohttp.ClientSession(headers=Config.DEFAULT_HEADERS, connector=connector) as session:
        tasks = [fetch_and_parse_source(session, url) for url in selected_sources]
        results = await asyncio.gather(*tasks)
    
    all_news = [item for sublist in results for item in sublist]
    duration = (datetime.now() - start_time).total_seconds()
    logger.info(f"📰 Знайдено {len(all_news)} новин з {len(selected_sources)} джерел за {duration:.2f} сек.")
    return all_news, duration

# --- 5. ФОРМАТУВАННЯ ТА ПОСТИНГ (З ХЕШТЕГАМИ) ---

def generate_hashtags(text: str) -> str:
    """Генерує до 3 розумних хештегів на основі ключових слів."""
    text_lower = text.lower()
    found_hashtags = set()

    # Словник: {ключове_слово: #хештег}
    # Ключові слова перевіряються в нижньому регістрі.
    HASHTAG_MAP = {
        'зеленськ': '#ВолодимирЗеленський', 'президент': '#ПрезидентУкраїни',
        'залужн': '#ВалерійЗалужний', 'сирськ': '#ОлександрСирський',
        'верховна рада': '#ВерховнаРада', 'кабмін': '#Кабмін',
        'нбу': '#НБУ', 'нацбанк': '#НБУ', 'долар': '#КурсДолара', 'євро': '#КурсЄвро',
        'інфляція': '#інфляція', 'бюджет': '#БюджетУкраїни',
        'мвф': '#МВФ', 'міжнародний валютний фонд': '#МВФ',
        'пенсі': '#пенсії', 'зарплат': '#зарплати',
        'допомога': '#ФінансоваДопомога', 'кредит': '#кредити',
        'сша': '#США', 'єс': '#ЄвропейськийСоюз', 'євросоюз': '#ЄвропейськийСоюз',
        'саміт миру': '#СамітМиру', 'переговори': '#переговори',
        'дтек': '#ДТЕК', 'укренерго': '#Укренерго', 'світло': '#відключеннясвітла',
        'податки': '#податки', 'бізнес': '#бізнес', 'економіка': '#ЕкономікаУкраїни'
    }

    # Пошук ключових слів у тексті
    for keyword, hashtag in HASHTAG_MAP.items():
        if keyword in text_lower:
            found_hashtags.add(hashtag)
        if len(found_hashtags) >= 3:
            break
            
    return " ".join(found_hashtags)

def format_news_post(news_item: dict) -> str:
    """Форматує новину для публікації у Telegram (HTML) з хештегами."""
    source_display = news_item['source']
    hashtags = generate_hashtags(news_item['title'] + " " + news_item['summary'])
    
    message = (
        f"<b>⚡️ {news_item['title']}</b>\n\n"
        f"{news_item['summary']}\n\n"
        f"<a href='{news_item['url']}'>Подробиці на {source_display}</a>\n\n"
        f"{hashtags if hashtags else ''}"
    )
    return message.strip()

async def send_news_to_channel(news_to_post: list):
    """Публікує новини у канал."""
    posted_urls = []
    for news in news_to_post:
        try:
            caption = format_news_post(news)
            await bot.send_photo(
                chat_id=CHANNEL_ID, photo=news['image_url'],
                caption=caption, parse_mode=ParseMode.HTML,
            )
            await asyncio.sleep(1.5)
            posted_urls.append(news['url'])
        except TelegramAPIError as e:
            logger.error(f"❌ Telegram API Error для '{news['title'][:50]}...': {e.message}")
            if "Bad Request: failed to get HTTP URL content" in e.message:
                posted_urls.append(news['url']) # Позначити як опубліковану, щоб уникнути спаму
        except Exception as e:
            logger.error(f"❌ Невідома помилка відправки для '{news['title'][:50]}...': {e}")
    
    if posted_urls:
        await mark_news_as_posted(posted_urls)
    return len(posted_urls)

# --- 6. РОЗДІЛЕНІ ЦИКЛИ ЗБОРУ ТА ПОСТИНГУ ---

async def db_cleanup_loop():
    """Асинхронний цикл для періодичного очищення бази даних."""
    while True:
        await asyncio.sleep(Config.CLEANUP_INTERVAL_HOURS * 3600)
        logger.info("--- ♻️ Запуск фонової очистки БД ---")
        await cleanup_db()

async def news_gathering_loop():
    """Цикл, який збирає новини раз на 20 хвилин."""
    while True:
        try:
            logger.info("--- 🚀 Запуск циклу збору новин ---")
            fetched_news, parse_duration = await fetch_all_sources()
            if fetched_news:
                new_count = await save_news_to_db(fetched_news)
                logger.info(f"💾 Успішно збережено {new_count} нових новин. Тривалість: {parse_duration:.2f}с.")
        except Exception as e:
            logger.critical(f"❌ Критична помилка в циклі збору новин: {e}")

        logger.info(f"Очікування {Config.NEWS_GATHERING_INTERVAL_MIN} хвилин до наступного збору...")
        await asyncio.sleep(Config.NEWS_GATHERING_INTERVAL_MIN * 60)
        
async def news_posting_loop():
    """Цикл, який публікує одну новину кожні 5 хвилин."""
    while True:
        try:
            logger.info("--- ▶️ Запуск циклу постингу ---")
            news_to_post = await get_unique_news_from_db(Config.MAX_NEWS_PER_POSTING_CYCLE)
            
            if news_to_post:
                posted_count = await send_news_to_channel(news_to_post)
                logger.info(f"--- ✅ Опубліковано {posted_count} новин. ---")
            else:
                logger.info("--- 📭 Немає нових новин для публікації. ---")

        except Exception as e:
            logger.critical(f"❌ Критична помилка в циклі постингу: {e}")

        logger.info(f"Очікування {Config.POSTING_INTERVAL_MIN} хвилин до наступного посту...")
        await asyncio.sleep(Config.POSTING_INTERVAL_MIN * 60)

# --- 7. КОМАНДИ АДМІНІСТРАТОРА ---

async def cmd_status(message: types.Message):
    """Показує поточний статус бота та конфігурацію."""
    config_msg = (
        "<b>🤖 Статус Платформи Новин (Еко-режим):</b>\n\n"
        "<b>⚙️ Конфігурація:</b>\n"
        f"  📥 Збір новин: раз на {Config.NEWS_GATHERING_INTERVAL_MIN} хв\n"
        f"  📤 Постинг: кожні {Config.POSTING_INTERVAL_MIN} хв\n"
        f"  📝 Постів за раз: <b>{Config.MAX_NEWS_PER_POSTING_CYCLE}</b>\n"
        f"  📸 Вимога: Тільки пости <b>З ФОТО</b>\n"
        f"  🧹 Чистка DB: Раз на {Config.CLEANUP_INTERVAL_HOURS} год (старше {Config.DB_CLEANUP_DAYS} дн.)\n"
        f"  📰 Джерел у списку: {len(Config.SOURCES)}\n\n"
        f"📢 Channel ID: <code>{CHANNEL_ID}</code>"
    )
    await message.answer(config_msg)

async def cmd_forcepost(message: types.Message):
    """Примусово запускає один цикл постингу."""
    await message.answer("♻️ Примусовий запуск постингу однієї новини...")
    
    try:
        news_to_post = await get_unique_news_from_db(1)
        if not news_to_post:
            await message.answer("📪 Немає нових новин у черзі для публікації.")
            return
            
        posted_count = await send_news_to_channel(news_to_post)
        result_msg = f"✅ <b>Примусовий постинг завершено!</b>\nОпубліковано новин: {posted_count}"
    except Exception as e:
        result_msg = f"❌ <b>Помилка примусового постингу:</b> {e}"
        
    await message.answer(result_msg)

async def cmd_stats(message: types.Message):
    """Показує статистику бази даних."""
    stats = await get_db_stats()
    if stats:
        stats_msg = (
            "📊 <b>Статистика Бази Даних:</b>\n\n"
            f"• 📝 Всього у DB: {stats.get('total', 0)}\n"
            f"• ✅ Опубліковано: {stats.get('posted', 0)}\n"
            f"• 📦 У черзі (з фото): {stats.get('unposted', 0)}\n"
            f"• 📰 Активних джерел: {stats.get('sources', 0)}"
        )
    else:
        stats_msg = "❌ Не вдалося отримати статистику."
    await message.answer(stats_msg)

# --- 8. ЗАПУСК БОТА ---

async def main():
    """Основна функція для ініціалізації та запуску бота."""
    if not all([BOT_TOKEN, DATABASE_URL, CHANNEL_ID]):
        logger.critical("Критична помилка: Не задані BOT_TOKEN, DATABASE_URL або CHANNEL_ID.")
        return

    await connect_db()
    if not db_pool: return
    await init_db()

    global bot
    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    
    global dp
    dp = Dispatcher()
    
    # Реєстрація адмін-команд
    admin_filter = F.from_user.id == ADMIN_ID
    dp.message.register(cmd_status, Command("status"), admin_filter)
    dp.message.register(cmd_forcepost, Command("forcepost"), admin_filter)
    dp.message.register(cmd_stats, Command("stats"), admin_filter)

    loop = asyncio.get_event_loop()
    # Запускаємо нові розділені цикли
    loop.create_task(news_gathering_loop())
    loop.create_task(news_posting_loop())
    loop.create_task(db_cleanup_loop())
    logger.info("Бот запущено. Початок роботи в еко-режимі.")

    try:
        await bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(bot)
    finally:
        if bot: await bot.session.close()
        if db_pool: await db_pool.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Бот зупинено вручну.")
    except Exception as e:
        logger.critical(f"❌ Головна помилка виконання: {e}")
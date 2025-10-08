import os
import asyncio
import logging
import re
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

# --- 1. НАЛАШТУВАННЯ І КОНСТАНТИ ---

# Використовуйте Kyiv time zone (UTC+3)
KYIV_TZ = timezone(timedelta(hours=3), 'Europe/Kyiv')

# Налаштування логування
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Змінні середовища (читаються з Render Environment)
BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
CHANNEL_ID = os.getenv("CHANNEL_ID") # Приклад: -1002766273069
try:
    ADMIN_ID = int(os.getenv("ADMIN_ID", 0))
except ValueError:
    ADMIN_ID = 0

# Конфігурація бота
POSTING_INTERVAL_MIN = 5  # Кожні 5 хвилин
MAX_NEWS_PER_CYCLE = 100   # До 100 новин за цикл
MAX_AGE_MIN = 20          # Не публікувати новини старше 20 хвилин

# Додано User-Agent для обходу 403 помилок (ОНОВЛЕНО)
DEFAULT_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36', 
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language': 'uk-UA,uk;q=0.9,en-US;q=0.8,en;q=0.7',
}

# 1. 📰 Джерела новин (НОВИЙ, ПЕРЕВІРЕНИЙ СПИСОК ПОВНИХ RSS-ШЛЯХІВ)
SOURCES = [
    "https://tsn.ua/rss/all.xml",
    "https://www.pravda.com.ua/rss/news/",
    "https://censor.net/rss/all_news",
    "https://www.rbc.ua/static/rss/all.xml",
    "https://www.ukrinform.ua/rss/all.xml",
    "https://hromadske.ua/feed",
    "https://www.obozrevatel.com/rss/main.xml",
    "https://minfin.com.ua/rss/news/",
    "https://focus.ua/rss/latest.xml",
    "https://ua.korrespondent.net/rss/all",
    "https://apostrophe.ua/rss/all.xml",
    "https://www.liga.net/rss/news.xml",
    "https://gazeta.ua/rss/all",
    "https://24tv.ua/rss/all.xml",
    "https://nv.ua/rss/all.xml",
    "https://delo.ua/rss/all.xml",
    "https://suspilne.media/feed/",
    "https://uain.press/rss",
    # "https://www.segodnya.ua/rss/news.xml", # Видалено через постійну помилку DNS/блокування
    "https://www.bbc.com/ukrainian/rss.xml",
    "https://www.eurointegration.com.ua/rss/rss.xml", 
    "https://news.finance.ua/ua/rss", 
    "https://www.unian.ua/rss/news.rss", 
    "https://ua.interfax.com.ua/news/ukraine.rss", 
    "https://zaxid.net/rss",
]
FETCH_LIMIT = 15 # Кількість новин, які будемо намагатися парсити з кожного джерела


# Глобальні змінні для DB
db_pool = None
dp: Dispatcher = None
bot: Bot = None

# --- 2. БАЗА ДАНИХ (POSTGRESQL/NEON) ---

async def connect_db():
    """Створює пул з'єднань до бази даних Neon (PostgreSQL)."""
    global db_pool
    try:
        db_pool = await asyncpg.create_pool(DATABASE_URL)
        logger.info("Успішно підключено до Neon PostgreSQL.")
    except Exception as e:
        logger.error(f"Помилка підключення до DB: {e}")
        # Вихід із програми, якщо не вдалося підключитися
        exit(1)

# ВИПРАВЛЕНО: Додана логіка ALTER TABLE для оновлення структури бази
async def init_db():
    """Створює таблицю 'news', якщо вона не існує, та додає необхідні стовпці."""
    async with db_pool.acquire() as conn:
        # 1. Створення таблиці, якщо не існує
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
        
        # 2. ❗ МІГРАЦІЯ: Додавання стовпця 'is_posted' для зворотної сумісності/міграції
        try:
            await conn.execute("""
                ALTER TABLE news ADD COLUMN is_posted BOOLEAN DEFAULT FALSE;
            """)
            logger.info("Стовпець 'is_posted' успішно додано.")
        except asyncpg.exceptions.DuplicateColumnError:
            # Це очікувано, якщо стовпець вже існує
            pass 
        except Exception as e:
            logger.error(f"Помилка при спробі додати стовпець 'is_posted': {e}")
            
        # 3. Додавання унікального індексу на url, якщо його ще немає (для ON CONFLICT)
        try:
            await conn.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS news_url_idx ON news (url);
            """)
        except Exception as e:
            logger.error(f"Помилка при створенні унікального індексу на url: {e}")

    logger.info("Таблиця 'news' перевірена/оновлена.")


async def save_news_to_db(news_items: list):
    """Зберігає список новин у базу даних, уникаючи дублікатів."""
    if not news_items:
        return 0
    
    # Використовуємо UNNEST для масової вставки
    sql = """
        INSERT INTO news (source, url, title, summary, image_url, published_at)
        SELECT * FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::timestamptz[])
        ON CONFLICT (url) DO NOTHING
        RETURNING id;
    """
    
    # Підготовка даних для масової вставки
    sources = [item['source'] for item in news_items]
    urls = [item['url'] for item in news_items]
    titles = [item['title'] for item in news_items]
    summaries = [item['summary'] for item in news_items]
    image_urls = [item['image_url'] for item in news_items]
    published_at_list = [item['published_at'] for item in news_items]
    
    async with db_pool.acquire() as conn:
        result = await conn.fetch(sql, sources, urls, titles, summaries, image_urls, published_at_list)
        return len(result) # Кількість успішно вставлених записів

async def get_unique_news_from_db(limit: int) -> list:
    """Вибирає найновіші, ще не опубліковані новини з бази."""
    sql = """
        SELECT url, title, summary, image_url, source
        FROM news
        WHERE is_posted = FALSE
        ORDER BY published_at DESC
        LIMIT $1;
    """
    async with db_pool.acquire() as conn:
        records = await conn.fetch(sql, limit)
        return [dict(record) for record in records]

async def mark_news_as_posted(urls: list):
    """Позначає новини, що були успішно опубліковані."""
    if not urls:
        return
    sql = """
        UPDATE news
        SET is_posted = TRUE
        WHERE url = ANY($1::text[]);
    """
    async with db_pool.acquire() as conn:
        await conn.execute(sql, urls)

async def get_db_stats():
    """Повертає статистику бази даних."""
    sql = """
        SELECT 
            (SELECT count(*) FROM news) AS total_news,
            (SELECT count(*) FROM news WHERE is_posted = TRUE) AS posted_news,
            (SELECT count(*) FROM news WHERE is_posted = FALSE) AS unposted_news,
            (SELECT count(DISTINCT source) FROM news) AS total_sources;
    """
    async with db_pool.acquire() as conn:
        record = await conn.fetchrow(sql)
        return dict(record) if record else None

# --- 3. ХЕЛПЕРИ ПАРСИНГУ ---

def normalize_summary(text: str) -> str:
    """Очищає та нормалізує текст анотації, видаляючи HTML/зайві символи."""
    if not text:
        return ""
    # Видалення HTML-тегів
    soup = BeautifulSoup(text, 'html.parser')
    clean_text = soup.get_text()
    # Видалення зайвих пробілів/переносів
    clean_text = ' '.join(clean_text.split())
    # Обмеження довжини (наприклад, 400 символів)
    return clean_text[:400].strip()

def extract_image_url(entry) -> str:
    """Витягує URL зображення з різних полів RSS-запису."""
    # 1. media_content (найкращий варіант)
    if 'media_content' in entry:
        for media in entry.media_content:
            # Перевірка на "image" у type або medium
            if 'image' in media.get('type', '') or 'image' in media.get('medium', ''):
                return media.get('url', '')
    
    # 2. media_thumbnail
    if 'media_thumbnail' in entry and entry.media_thumbnail:
        return entry.media_thumbnail[0].get('url', '')
    
    # 3. tags (для деяких фідів)
    if 'tags' in entry:
        for tag in entry.tags:
            if tag.get('term') == 'enclosure' and tag.get('url'):
                 return tag['url']

    # 4. Витяг з summary/description за допомогою BeautifulSoup
    if entry.get('summary'):
        soup = BeautifulSoup(entry.summary, 'html.parser')
        img = soup.find('img')
        if img and img.get('src'):
            return img['src']
            
    return ""

def parse_published_time(entry, rss_url) -> datetime:
    """Парсить та нормалізує час публікації до часового поясу Києва."""
    published = entry.get('published_parsed') or entry.get('updated_parsed')
    
    if published:
        try:
            # Створення datetime object з UTC, якщо часовий пояс не вказано (за замовчуванням feedparser)
            published_utc = datetime(*published[:6], tzinfo=timezone.utc)
            # Конвертація в часовий пояс Києва
            return published_utc.astimezone(KYIV_TZ)
        except Exception:
            # Fallback
            pass
            
    # Якщо не вдалося спарсити, використовуємо поточний час (з попередженням)
    logger.warning(f"Не вдалося спарсити час публікації для {entry.get('title', '')} з {rss_url}. Використано поточний час.")
    return datetime.now(KYIV_TZ)

# --- 4. ОСНОВНИЙ ПАРСИНГ ---

async def fetch_and_parse_source(session, rss_url: str):
    """
    Парсить одне джерело (повний RSS URL) та повертає список нових, актуальних новин.
    """
    news_items = []
    
    # Визначаємо домен для збереження в базу
    source_domain = urlparse(rss_url).netloc.replace('www.', '')

    # 1. Запит
    try:
        # Використовуємо заголовки DEFAULT_HEADERS для обходу 403
        async with session.get(rss_url, headers=DEFAULT_HEADERS, timeout=10) as response:
            if response.status != 200:
                logger.warning(f"Помилка {response.status} при отриманні RSS для {rss_url}")
                return []
            content = await response.text()
    except Exception as e:
        # Видалено Segodnya, але залишаємо загальний лог для інших проблем
        logger.error(f"Помилка AIOHTTP для {rss_url}: {e}")
        return []

    # 2. Парсинг RSS
    feed = feedparser.parse(content)
    now_kyiv = datetime.now(KYIV_TZ)
    max_age_dt = timedelta(minutes=MAX_AGE_MIN)

    for entry in feed.entries[:FETCH_LIMIT]:
        try:
            url = entry.link
            title = entry.title
            summary = normalize_summary(entry.get('summary') or entry.get('description') or entry.title)
            image_url = extract_image_url(entry)
            published_time = parse_published_time(entry, rss_url)

            # 3. 🕒 Логіка актуальності (≤ MAX_AGE_MIN)
            if now_kyiv - published_time > max_age_dt:
                logger.debug(f"Пропущено стару новину: {title[:50]}... ({now_kyiv - published_time})")
                continue

            news_items.append({
                'source': source_domain,
                'title': title,
                'url': url,
                'summary': summary,
                'image_url': image_url,
                'published_at': published_time,
            })
        except Exception as e:
            logger.warning(f"Помилка обробки запису з {rss_url}: {e}")
            continue

    return news_items

async def fetch_all_sources():
    """Асинхронно отримує новини з усіх джерел."""
    all_news = []
    start_time = asyncio.get_event_loop().time()

    async with aiohttp.ClientSession(headers=DEFAULT_HEADERS) as session:
        # Створюємо список задач для одночасного виконання
        tasks = [fetch_and_parse_source(session, rss_url) for rss_url in SOURCES]
        
        # Запускаємо всі задачі паралельно
        results = await asyncio.gather(*tasks)

        # Об'єднуємо результати
        for news_list in results:
            if news_list:
                all_news.extend(news_list)

    end_time = asyncio.get_event_loop().time()
    logger.info(f"Всього знайдено {len(all_news)} актуальних новин з усіх джерел за {end_time - start_time:.2f} сек.")
    
    return all_news

# --- 5. ФОРМАТУВАННЯ ТА ПОСТИНГ ---

def format_news_post(news_item: dict) -> str:
    """Форматує новину для публікації у Telegram (HTML)."""
    
    # Видалення протоколу для чистішого відображення
    source_display = news_item['source'].replace('https://', '').replace('http://', '')
    
    # Використовуємо HTML для форматування
    message = (
        f"<b>{news_item['title']}</b>\n\n"
        f"{news_item['summary']}\n\n"
        f"<a href='{news_item['url']}'>Подробиці на {source_display}</a>"
    )
    return message

async def send_news_to_channel(news_to_post: list):
    """Публікує новини у канал, використовуючи метод send_photo або send_message."""
    
    posted_urls = []
    
    # Сортуємо новини, щоб найстаріші з актуальних були опубліковані першими
    news_to_post.reverse() 
    
    for news in news_to_post[:MAX_NEWS_PER_CYCLE]:
        try:
            caption = format_news_post(news)
            
            if news.get('image_url'):
                # 1. Постинг з фото
                await bot.send_photo(
                    chat_id=CHANNEL_ID,
                    photo=news['image_url'],
                    caption=caption,
                    parse_mode=ParseMode.HTML
                )
            else:
                # 2. Постинг лише тексту
                await bot.send_message(
                    chat_id=CHANNEL_ID,
                    text=caption,
                    parse_mode=ParseMode.HTML
                )
            
            # Затримка між постами, щоб уникнути спам-блокування
            await asyncio.sleep(1) 
            posted_urls.append(news['url'])
            
        except Exception as e:
            logger.error(f"Помилка відправки новини '{news['title'][:50]}...': {e}")
            # Не додаємо URL до posted_urls, щоб спробувати пізніше

    # Позначаємо успішно опубліковані новини у базі
    await mark_news_as_posted(posted_urls)
    return len(posted_urls)

# --- 6. ОСНОВНИЙ ЦИКЛ АВТОПОСТИНГУ ---

async def auto_posting_loop(bot: Bot):
    """Головний цикл, який періодично перевіряє та публікує новини."""
    while True:
        try:
            # 1. Запуск парсингу
            logger.info("--- Запуск циклу автопостингу ---")
            start_time = datetime.now()
            
            fetched_news = await fetch_all_sources()
            
            # 2. Збереження у базу
            new_count = await save_news_to_db(fetched_news)
            logger.info(f"Успішно вставлено (нових) {new_count} новин у базу.")

            # 3. Отримання новин для публікації
            news_to_post = await get_unique_news_from_db(MAX_NEWS_PER_CYCLE)
            
            # 4. Публікація у канал
            posted_count = await send_news_to_channel(news_to_post)
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            logger.info(
                f"--- Цикл завершено. Опубліковано: {posted_count} новин за {duration:.2f} сек. ---"
            )
            
        except Exception as e:
            # Ця критична помилка була виправлена у init_db
            logger.error(f"Критична помилка в циклі автопостингу: {e}")

        # 5. Очікування наступного циклу
        await asyncio.sleep(POSTING_INTERVAL_MIN * 60)
        logger.info(f"Очікування {POSTING_INTERVAL_MIN} хвилин...")

# --- 7. КОМАНДИ АДМІНІСТРАТОРА ---

async def cmd_status(message: types.Message):
    """Показує поточний статус бота та конфігурацію."""
    config_msg = (
        "<b>🤖 Статус Бота та Конфігурація:</b>\n\n"
        f"⏳ Інтервал: {POSTING_INTERVAL_MIN} хв\n"
        f"⏱️ Макс. вік новини: {MAX_AGE_MIN} хв\n"
        f"📝 Макс. постів за цикл: {MAX_NEWS_PER_CYCLE}\n"
        f"📰 Джерел: {len(SOURCES)}\n"
        f"⚙️ Webhook: Вимкнено (Polling Mode)\n"
        f"🔑 Admin ID: <code>{ADMIN_ID}</code>\n"
        f"📢 Channel ID: <code>{CHANNEL_ID}</code>"
    )
    await message.answer(config_msg, parse_mode=ParseMode.HTML)

async def cmd_forcepost(message: types.Message):
    """Примусово запускає цикл парсингу та постингу."""
    await message.answer("♻️ Примусовий запуск циклу парсингу...")
    
    # Використовуємо окрему асинхронну функцію для виконання циклу один раз
    async def run_once(bot_instance):
        try:
            start_time = datetime.now()
            fetched_news = await fetch_all_sources()
            new_count = await save_news_to_db(fetched_news)
            news_to_post = await get_unique_news_from_db(MAX_NEWS_PER_CYCLE)
            posted_count = await send_news_to_channel(news_to_post)
            duration = (datetime.now() - start_time).total_seconds()
            
            result_msg = (
                "✅ <b>Цикл примусового постингу завершено!</b>\n"
                f"   • Знайдено нових новин: {new_count}\n"
                f"   • Опубліковано новин: {posted_count}\n"
                f"   • Час виконання: {duration:.2f} сек"
            )
        except Exception as e:
            result_msg = f"❌ <b>Помилка примусового постингу:</b> {e}"
        
        await bot_instance.send_message(message.chat.id, result_msg, parse_mode=ParseMode.HTML)

    # Створення та запуск задачі
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
            f"• 📦 У черзі: {stats.get('unposted_news', 0)}\n"
            f"• 📰 Активних джерел: {stats.get('total_sources', 0)}"
        )
    else:
        stats_msg = "❌ Не вдалося отримати статистику з бази даних."

    await message.answer(stats_msg, parse_mode=ParseMode.HTML)


# --- 8. ЗАПУСК БОТА ---

async def main():
    """Основна функція для ініціалізації та запуску бота."""
    
    # Перевірка наявності змінних середовища
    if not all([BOT_TOKEN, DATABASE_URL, CHANNEL_ID]):
        logger.error("Критична помилка: Не задані BOT_TOKEN, DATABASE_URL або CHANNEL_ID.")
        return

    # 1. Підключення до бази даних
    await connect_db()
    await init_db()

    # 2. Налаштування та запуск бота
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

    # 3. Запуск циклу автопостингу
    loop = asyncio.get_event_loop()
    loop.create_task(auto_posting_loop(bot))
    logger.info("Бот запущено. Початок роботи.")

    try:
        # 🚨 АВТОМАТИЧНЕ ВИМКНЕННЯ WEBHOOK з повторними спробами
        for i in range(3):
            try:
                logger.info(f"Спроба {i+1}/3: Примусове вимкнення Webhook...")
                # drop_pending_updates=True гарантує, що бот почне з чистого аркуша
                await bot.delete_webhook(drop_pending_updates=True) 
                logger.info("Webhook успішно вимкнено.")
                break
            except Exception as e:
                logger.warning(f"Помилка вимкнення Webhook: {e}. Затримка 5 сек...")
                await asyncio.sleep(5)

        # 4. Запуск Polling
        await dp.start_polling(bot)
    finally:
        # 5. Закриття сесій
        await bot.session.close()
        if db_pool:
            await db_pool.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Бот зупинено вручну.")
    except Exception as e:
        logger.error(f"Головна помилка виконання: {e}")
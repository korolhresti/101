import os
import asyncio
import logging
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
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
from aiohttp import web

# --- 1. НАЛАШТУВАННЯ І КОНСТАНТИ ---

# Використовуйте Kyiv time zone (UTC+3)
KYIV_TZ = timezone(timedelta(hours=3), 'Europe/Kyiv')

# Налаштування логування
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Змінні середовища (читаються з Render Environment)
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
CHANNEL_ID = os.getenv("TELEGRAM_CHANNEL_ID")
RENDER_EXTERNAL_URL = os.getenv("RENDER_EXTERNAL_URL")
WEBHOOK_PATH_ENV = os.getenv("WEBHOOK_PATH") # /webhook
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET")
PORT = int(os.getenv("PORT", 8080))
try:
    ADMIN_ID = int(os.getenv("ADMIN_ID", 0))
except ValueError:
    ADMIN_ID = 0

# Конфігурація бота
POSTING_INTERVAL_MIN = 5  # Кожні 5 хвилин
MAX_NEWS_PER_CYCLE = 100   # До 100 новин за цикл
MAX_AGE_MIN = 20          # Не публікувати новини старше 20 хвилин

# Налаштування Webhook
WEBHOOK_PATH = f"{WEBHOOK_PATH_ENV}/{BOT_TOKEN}"
WEBHOOK_URL = f"{RENDER_EXTERNAL_URL}{WEBHOOK_PATH}"

# Додано User-Agent для обходу 403 помилок
DEFAULT_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36', 
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language': 'uk-UA,uk;q=0.9,en-US;q=0.8,en;q=0.7',
}

# 1. 📰 Джерела новин (ФІНАЛЬНА ОПТИМІЗАЦІЯ)
SOURCES = [
    "https://epravda.com.ua/",
    "https://news.liga.net/ua/",
    "https://www.eurointegration.com.ua/",
    "https://www.rbc.ua/",
    "https://www.ukrinform.ua/",
    "https://tsn.ua/",
    "http://feeds.bbci.co.uk/ukrainian/rss.xml", 
    "https://ua.korrespondent.net/",
    "https://www.obozrevatel.com/",
    "https://news.finance.ua/",
    "https://suspilne.media/",
    "https://www.unian.ua/",
    "https://ua.interfax.com.ua/",
    "https://nv.ua/",
    "https://zaxid.net/",
    "https://hromadske.ua/",
    "https://censor.net/",
    "https://minfin.com.ua/",
    "https://gazeta.ua/",
    "https://focus.ua/",
    "https://apostrophe.ua/",
]
FETCH_LIMIT = 15

# Глобальний пул підключень до бази даних
db_pool = None
# Ініціалізація Бота та Диспетчера
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()


# --- 2. БАЗА ДАНИХ (Neon PostgreSQL) ---

async def init_db_pool():
    """Створює пул підключень до Neon PostgreSQL та створює таблицю."""
    global db_pool
    if not DATABASE_URL:
        logger.error("DATABASE_URL не встановлено.")
        return
    try:
        # Використовуємо ssl='require' якщо воно не вказано в DATABASE_URL
        db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=10)
        logger.info("Успішно підключено до Neon PostgreSQL.")
        await create_news_table()
    except Exception as e:
        logger.critical(f"Помилка підключення до БД: {e}")
        db_pool = None 

async def create_news_table():
    """Створює таблицю 'news', якщо вона не існує."""
    CREATE_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS news (
        id SERIAL PRIMARY KEY,
        source TEXT,
        title TEXT,
        url TEXT UNIQUE,
        summary TEXT,
        image_url TEXT,
        published_at TIMESTAMP WITH TIME ZONE,
        posted_at TIMESTAMP WITH TIME ZONE DEFAULT NULL
    );
    """
    if db_pool:
        async with db_pool.acquire() as conn:
            await conn.execute(CREATE_TABLE_SQL)
        logger.info("Таблиця 'news' перевірена/створена.")

async def insert_news(news_list):
    """Вставляє список новин у базу даних, ігноруючи дублікати."""
    if not news_list or not db_pool:
        return []

    INSERT_SQL = """
    INSERT INTO news (source, title, url, summary, image_url, published_at)
    VALUES ($1, $2, $3, $4, $5, $6)
    ON CONFLICT (url) DO NOTHING
    RETURNING url;
    """
    inserted_urls = []
    
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            for news_item in news_list:
                # Обробка випадку, коли published_at може бути None
                published_at = news_item.get('published_at', datetime.now(KYIV_TZ) - timedelta(minutes=MAX_AGE_MIN + 1))
                if not isinstance(published_at, datetime):
                    published_at = datetime.now(KYIV_TZ) - timedelta(minutes=MAX_AGE_MIN + 1)
                
                try:
                    result = await conn.fetchval(INSERT_SQL,
                        news_item.get('source'),
                        news_item.get('title'),
                        news_item.get('url'),
                        news_item.get('summary'),
                        news_item.get('image_url'),
                        published_at
                    )
                    if result is not None:
                        inserted_urls.append(result)
                except Exception as e:
                    logger.warning(f"Помилка вставки новини {news_item.get('url')}: {e}")

    logger.info(f"Успішно вставлено (нових) {len(inserted_urls)} новин у базу.")
    return inserted_urls

async def update_posted_at(urls_list):
    """Оновлює поле posted_at для успішно опублікованих новин."""
    if not urls_list or not db_pool:
        return 0

    UPDATE_SQL = """
    UPDATE news
    SET posted_at = NOW()
    WHERE url = ANY($1::TEXT[]);
    """
    async with db_pool.acquire() as conn:
        return await conn.execute(UPDATE_SQL, urls_list)


# --- 3. ПАРСИНГ НОВИН (Виправлення 404/403) ---

def parse_published_time(entry, source_url: str) -> datetime:
    """Намагається отримати та нормалізувати час публікації."""
    try:
        parsed_time = None
        if hasattr(entry, 'published_parsed') and entry.published_parsed:
            parsed_time = entry.published_parsed
        elif hasattr(entry, 'updated_parsed') and entry.updated_parsed:
            parsed_time = entry.updated_parsed

        if parsed_time:
            published_time = datetime(*parsed_time[:6], tzinfo=timezone.utc)
            return published_time.astimezone(KYIV_TZ)
    except Exception as e:
        logger.warning(f"Помилка парсингу часу для {source_url}: {e}. Використано час UTC-0.")

    return datetime.now(KYIV_TZ) - timedelta(minutes=MAX_AGE_MIN + 1)

def extract_image_url(entry):
    """Намагається знайти URL зображення з різних полів RSS."""
    # (Логіка пошуку зображення залишена з попереднього кроку для кращої якості постів)
    if hasattr(entry, 'media_content'):
        for media in entry.media_content:
            if 'url' in media and media.get('type', '').startswith('image/'):
                return media['url']
    if hasattr(entry, 'enclosures'):
        for enclosure in entry.enclosures:
            if enclosure.get('type', '').startswith('image/'):
                return enclosure['href']
    if hasattr(entry, 'media_thumbnail'):
        for thumbnail in entry.media_thumbnail:
            if 'url' in thumbnail:
                return thumbnail['url']
    return None

def normalize_summary(summary: str) -> str:
    """Очищає та нормалізує текст summary від HTML."""
    if not summary:
        return ""
    soup = BeautifulSoup(summary, 'html.parser')
    text = soup.get_text().strip()
    return text[:700] + ('...' if len(text) > 700 else '')

async def fetch_and_parse_source(session, source_url: str):
    """Парсить одне джерело (з оптимізованими шляхами) та повертає список новин."""
    news_items = []
    rss_path = '/rss'
    
    # Спеціальні випадки (ФІНАЛЬНА КОРЕКЦІЯ RSS-ШЛЯХІВ)
    if "epravda.com.ua" in source_url:
        rss_path = "/rss/"
    elif "eurointegration.com.ua" in source_url:
        rss_path = "/rss/news.xml" 
    elif "liga.net" in source_url:
        rss_path = "/rss.xml" 
    elif "rbc.ua" in source_url:
        rss_path = "/all/rss" 
    elif "ukrinform.ua" in source_url:
        rss_path = "/rss/all.rss"
    elif "tsn.ua" in source_url:
        rss_path = "/rss"
    elif "bbci.co.uk" in source_url:
        rss_url = source_url
        source_domain = "bbc.com/ukrainian" 
        pass 
    elif "korrespondent.net" in source_url:
        rss_path = "/rss/all_news" 
    elif "obozrevatel.com" in source_url:
        rss_path = "/rss/news.rss" 
    elif "news.finance.ua" in source_url:
        rss_path = "/ua/rss"
    elif "suspilne.media" in source_url:
        rss_path = "/rss/all"
    elif "unian.ua" in source_url:
        rss_url = source_url.replace("www.", "rss.") + "/rss/news/ukr/feed"
        source_domain = "unian.ua"
        pass
    elif "interfax.com.ua" in source_url: 
        rss_path = "/news/ukraine.rss"
    elif "nv.ua" in source_url:
        rss_path = "/ukr/rss/all.xml"
    elif "hromadske.ua" in source_url:
        rss_path = "/feeds/all.xml"
    elif "censor.net" in source_url:
        rss_path = "/news/rss.xml"
    elif "minfin.com.ua" in source_url:
        rss_path = "/rss/feed/"
    elif "gazeta.ua" in source_url:
        rss_path = "/rss/all.rss"
    elif "focus.ua" in source_url:
        rss_path = "/rss"
    elif "apostrophe.ua" in source_url:
        rss_path = "/rss/feed"
    
    # Формування URL
    if "bbci.co.uk" not in source_url and "unian.ua" not in source_url:
        rss_url = source_url.rstrip('/') + rss_path
        source_domain = urlparse(source_url).netloc
    
    # Запит
    try:
        async with session.get(rss_url, headers=DEFAULT_HEADERS, timeout=10) as response:
            if response.status != 200:
                logger.warning(f"Помилка {response.status} при отриманні RSS для {rss_url}")
                return []
            content = await response.text()
    except Exception as e:
        logger.error(f"Помилка AIOHTTP для {rss_url}: {e}")
        return []

    # Парсинг RSS
    feed = feedparser.parse(content)
    now_kyiv = datetime.now(KYIV_TZ)
    max_age_dt = timedelta(minutes=MAX_AGE_MIN)

    for entry in feed.entries[:FETCH_LIMIT]:
        try:
            url = entry.link
            title = entry.title
            summary = normalize_summary(entry.summary if hasattr(entry, 'summary') else (entry.description if hasattr(entry, 'description') else entry.title))
            image_url = extract_image_url(entry)
            published_time = parse_published_time(entry, source_url)

            # Логіка актуальності (≤ 20 хв)
            if now_kyiv - published_time > max_age_dt:
                logger.debug(f"Пропущено стару новину: {title[:50]}...")
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
            logger.warning(f"Помилка обробки запису з {source_url}: {e}")
            continue

    return news_items

async def collect_all_news():
    """Паралельно парсить усі джерела."""
    all_news = []
    timeout = aiohttp.ClientTimeout(total=45) 
    async with aiohttp.ClientSession(timeout=timeout) as session:
        tasks = [fetch_and_parse_source(session, source) for source in SOURCES]
        results = await asyncio.gather(*tasks)

        for news_list in results:
            all_news.extend(news_list)

    all_news.sort(key=lambda x: x['published_at'], reverse=True)
    logger.info(f"Всього знайдено {len(all_news)} актуальних новин з усіх джерел.")
    return all_news

# --- 4. АВТОМАТИЧНИЙ ПОСТИНГ ТА ЛОГІКА ---

async def post_news_cycle(bot: Bot):
    """Основний цикл парсингу, фільтрації, постингу та збереження."""
    start_time = datetime.now()
    logger.info("--- Запуск циклу автопостингу ---")

    all_news = await collect_all_news()
    inserted_urls = await insert_news(all_news)

    inserted_urls_set = set(inserted_urls)
    # Фільтруємо: беремо тільки щойно вставлені (нові) новини
    news_to_post = [
        news for news in all_news
        if news['url'] in inserted_urls_set
    ][:MAX_NEWS_PER_CYCLE]

    posted_count = 0
    urls_posted_successfully = []

    for news in news_to_post:
        # Форматування джерела
        source_domain = news['source']
        source_name = source_domain.replace('www.', '').replace('.com', '').replace('.ua', '').replace('.net', '').replace('.org', '').title()
        
        # НОВИЙ ФОРМАТ
        text = (
            f"⚡️ <b>{news['title']}</b>\n\n"
            f"{news['summary']}\n\n"
            f"—\n"
            f"🗞️ <a href='{news['url']}'>{source_name}</a>"
        )

        try:
            if news['image_url']:
                await bot.send_photo(
                    chat_id=CHANNEL_ID,
                    photo=news['image_url'],
                    caption=text,
                    parse_mode=ParseMode.HTML
                )
            else:
                await bot.send_message(
                    chat_id=CHANNEL_ID,
                    text=text,
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True
                )
            urls_posted_successfully.append(news['url'])
            posted_count += 1
            await asyncio.sleep(0.5) 
        except Exception as e:
            logger.error(f"Помилка постингу новини {news['url']}: {e}")

    # Маркуємо новини як опубліковані
    if urls_posted_successfully:
        await update_posted_at(urls_posted_successfully)

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    logger.info(f"--- Цикл завершено. Опубліковано: {posted_count} новин за {duration:.2f} сек. ---")


# --- 5. КОМАНДИ АДМІНІСТРАТОРА (aiogram 3.x) ---

@dp.message(Command("status"), F.from_user.id == ADMIN_ID)
async def cmd_status(message: types.Message):
    """Показує статистику бота."""
    total_news = 0
    unposted_news = 0
    last_posted = "Немає даних"
    try:
        if db_pool:
            async with db_pool.acquire() as conn:
                total_news = await conn.fetchval("SELECT COUNT(*) FROM news")
                unposted_news = await conn.fetchval("SELECT COUNT(*) FROM news WHERE posted_at IS NULL")
                last_posted_dt = await conn.fetchval("SELECT MAX(posted_at) FROM news WHERE posted_at IS NOT NULL")
                if last_posted_dt:
                    last_posted = last_posted_dt.astimezone(KYIV_TZ).strftime("%d.%m.%Y %H:%M:%S")

    except Exception as e:
        logger.error(f"Помилка отримання статусу БД: {e}")
        total_news = "Помилка"
        unposted_news = "Помилка"
        last_posted = "Помилка БД"

    status_message = (
        "🤖 **Статус NewsAutoPoster UA (Webhook)**\n\n"
        f"🔸 **Новин у базі (Всього):** `{total_news}`\n"
        f"🔸 **Новин у черзі (Неопубл.):** `{unposted_news}`\n"
        f"🔸 **Кількість джерел:** `{len(SOURCES)}`\n"
        f"🔸 **Час циклу:** кожні `{POSTING_INTERVAL_MIN}` хв\n"
        f"🔸 **Час останньої публікації:** `{last_posted}`\n"
        f"🔸 **Актуальність:** не старше `{MAX_AGE_MIN}` хв"
    )
    await message.answer(status_message, parse_mode=ParseMode.MARKDOWN)

@dp.message(Command("forcepost"), F.from_user.id == ADMIN_ID)
async def cmd_forcepost(message: types.Message):
    """Запускає позачергову перевірку і постинг новин."""
    await message.answer("🔄 Запускаю позачерговий цикл парсингу та постингу...")
    await post_news_cycle(bot)
    await message.answer("✅ Позачерговий цикл завершено.")

@dp.message(Command("stats"), F.from_user.id == ADMIN_ID)
async def cmd_stats(message: types.Message):
    """Показує кількість опублікованих новин за добу."""
    news_24h = 0
    try:
        if db_pool:
            yesterday = datetime.now(KYIV_TZ) - timedelta(hours=24)
            async with db_pool.acquire() as conn:
                news_24h = await conn.fetchval(
                    "SELECT COUNT(*) FROM news WHERE posted_at IS NOT NULL AND posted_at >= $1", yesterday
                )
    except Exception as e:
        logger.error(f"Помилка отримання статистики: {e}")
        news_24h = "Помилка"

    stats_message = (
        "📊 **Статистика публікацій**\n\n"
        f"🔸 **Опубліковано за останні 24 год:** `{news_24h}`"
    )
    await message.answer(stats_message, parse_mode=ParseMode.MARKDOWN)

@dp.message(Command("start"))
async def start_cmd(message: types.Message):
    """Обробник команди /start."""
    await message.answer(
        "👋 Привіт! Це новинний бот 🇺🇦\n\n"
        "Він автоматично публікує найсвіжіші новини з провідних джерел України 🗞️",
        parse_mode=ParseMode.HTML
    )

# --- 6. ОСНОВНИЙ ЦИКЛ АВТОПОСТИНГУ ---

async def autopost_loop():
    """Безкінечний цикл для автопостингу кожні 5 хвилин."""
    logger.info("Старт фонового циклу автопостингу.")
    await asyncio.sleep(10) # Даємо час для ініціалізації
    while True:
        try:
            await post_news_cycle(bot)
        except Exception as e:
            logger.error(f"Критична помилка в циклі автопостингу: {e}")
        finally:
            logger.info(f"Очікування {POSTING_INTERVAL_MIN} хвилин...")
            await asyncio.sleep(POSTING_INTERVAL_MIN * 60)

# --- 7. AIOHTTP WEB SERVER & WEBHOOK ---

async def on_startup(app: web.Application):
    """Виконується при запуску Aiohttp сервера."""
    if not all([BOT_TOKEN, DATABASE_URL, CHANNEL_ID, RENDER_EXTERNAL_URL]) or ADMIN_ID == 0:
        logger.critical("Не всі змінні середовища встановлені.")
        return

    # Ініціалізація бази даних
    await init_db_pool()

    # Встановлення Webhook
    try:
        await bot.set_webhook(
            WEBHOOK_URL,
            secret=WEBHOOK_SECRET,
            drop_pending_updates=True
        )
        logger.info(f"Webhook успішно встановлено: {WEBHOOK_URL}")
    except Exception as e:
        logger.critical(f"Помилка встановлення Webhook: {e}")
        
    # Запуск циклу автопостингу як фонової задачі
    app['background_task'] = asyncio.create_task(autopost_loop())


async def on_shutdown(app: web.Application):
    """Виконується при зупинці Aiohttp сервера."""
    logger.info("Завершення фонової задачі...")
    app['background_task'].cancel()
    
    logger.info("Видалення webhook...")
    await bot.delete_webhook()
    
    logger.info("Закриття з'єднань...")
    await bot.session.close()
    if db_pool:
        await db_pool.close()

# === Aiohttp webserver ===
def main():
    """Основна функція запуску Web-сервера."""
    
    if not BOT_TOKEN:
        logger.critical("BOT_TOKEN не знайдено. Завершення.")
        return

    app = web.Application()
    
    # Реєстрація Webhook-обробника
    webhook_request_handler = SimpleRequestHandler(
        dispatcher=dp, 
        bot=bot, 
        secret=WEBHOOK_SECRET
    )
    webhook_request_handler.register(app, path=WEBHOOK_PATH)
    
    # Додавання диспетчера до Aiohttp
    setup_application(app, dp, bot=bot)
    
    # Реєстрація функцій запуску/зупинки
    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)

    # Запуск сервера
    logger.info(f"Запуск Web-сервера на 0.0.0.0:{PORT}")
    web.run_app(app, host="0.0.0.0", port=PORT)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Програма зупинена користувачем.")
    except Exception:
        # Catch-all для неочікуваних помилок при запуску
        pass
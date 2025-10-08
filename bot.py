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
# Перетворюємо ADMIN_ID на int. Якщо змінної немає, встановлюємо 0.
try:
    ADMIN_ID = int(os.getenv("ADMIN_ID", 0))
except ValueError:
    ADMIN_ID = 0

# Конфігурація бота
POSTING_INTERVAL_MIN = 5  # Кожні 5 хвилин
MAX_NEWS_PER_CYCLE = 50   # До 50 новин за цикл
MAX_AGE_MIN = 20          # Не публікувати новини старше 20 хвилин

# 1. 📰 Джерела новин
SOURCES = [
    "https://finance.ua/",
    "https://www.ukrinform.ua/",
    "https://epravda.com.ua/",
    "https://ua.korrespondent.net/",
    "https://www.obozrevatel.com/",
    "https://www.eurointegration.com.ua/",
    "https://minprom.ua/",
    "https://tsn.ua/",
    "https://forbes.ua/news",
    "https://www.bbc.com/ukrainian"
]

# Кількість новин, які будемо намагатися парсити з кожного джерела
# для обробки ліміту в 50 і перевірки актуальності 20хв
FETCH_LIMIT = 15

# Глобальний пул підключень до бази даних
db_pool = None
# Диспетчер для aiogram
dp = None

# --- 2. БАЗА ДАНИХ (Neon PostgreSQL) ---

async def init_db_pool():
    """Створює пул підключень до Neon PostgreSQL та створює таблицю."""
    global db_pool
    if not DATABASE_URL:
        logger.error("DATABASE_URL не встановлено.")
        return
    try:
        # Використовуємо 10 підключень (достатньо для невеликого бота)
        db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=10)
        logger.info("Успішно підключено до Neon PostgreSQL.")
        await create_news_table()
    except Exception as e:
        logger.critical(f"Помилка підключення до БД: {e}")
        db_pool = None # Забезпечуємо, що пул буде None у разі помилки

async def create_news_table():
    """Створює таблицю 'news', якщо вона не існує."""
    # Таблиця з UNIQUE(url) для контролю дублікатів.
    CREATE_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS news (
        id SERIAL PRIMARY KEY,
        source TEXT,
        title TEXT,
        url TEXT UNIQUE,
        summary TEXT,
        image_url TEXT,
        published_at TIMESTAMP WITH TIME ZONE,
        posted_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
    );
    """
    if db_pool:
        async with db_pool.acquire() as conn:
            await conn.execute(CREATE_TABLE_SQL)
        logger.info("Таблиця 'news' перевірена/створена.")

async def insert_news(news_list):
    """
    Вставляє список новин у базу даних.
    Використовує ON CONFLICT DO NOTHING (запобігає дублюванню URL).
    Повертає список URL, які були успішно ВСТАВЛЕНІ.
    """
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
        # Використовуємо транзакцію для пакетного виконання
        async with conn.transaction():
            for news_item in news_list:
                # Конвертуємо published_at у python datetime з timezone
                published_at = news_item.get('published_at')
                # Якщо час не знайдено, використовуємо час, що гарантує, що новина буде проігнорована
                # у фільтрі актуальності, але вставиться в базу для контролю дублікатів.
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


# --- 3. ПАРСИНГ НОВИН ---

def parse_published_time(entry, source_url: str) -> datetime:
    """Намагається отримати та нормалізувати час публікації."""
    try:
        # Використовуємо .get('published_parsed') або 'updated_parsed'
        parsed_time = None
        if hasattr(entry, 'published_parsed') and entry.published_parsed:
            parsed_time = entry.published_parsed
        elif hasattr(entry, 'updated_parsed') and entry.updated_parsed:
            parsed_time = entry.updated_parsed

        if parsed_time:
            # feedparser надає час у UTC
            published_time = datetime(*parsed_time[:6], tzinfo=timezone.utc)
            return published_time.astimezone(KYIV_TZ)
    except Exception as e:
        logger.warning(f"Помилка парсингу часу для {source_url}: {e}. Використано час UTC-0.")

    # Якщо час не знайдено, повертаємо час, що гарантує, що новина буде проігнорована
    # у фільтрі актуальності, але дозволить вставити її для контролю дублікатів URL.
    return datetime.now(KYIV_TZ) - timedelta(minutes=MAX_AGE_MIN + 1)

def extract_image_url(entry):
    """Намагається знайти URL зображення з різних полів RSS."""
    # 1. media:content
    if hasattr(entry, 'media_content'):
        for media in entry.media_content:
            if 'url' in media and media.get('type', '').startswith('image/'):
                return media['url']
    # 2. enclosures
    if hasattr(entry, 'enclosures'):
        for enclosure in entry.enclosures:
            if enclosure.get('type', '').startswith('image/'):
                return enclosure['href']
    # 3. media:thumbnail
    if hasattr(entry, 'media_thumbnail'):
        for thumbnail in entry.media_thumbnail:
            if 'url' in thumbnail:
                return thumbnail['url']

    return None

def normalize_summary(summary: str) -> str:
    """Очищає та нормалізує текст summary від HTML."""
    if not summary:
        return ""
    # Видаляємо HTML-теги
    soup = BeautifulSoup(summary, 'html.parser')
    text = soup.get_text().strip()
    # Обмежуємо довжину
    return text[:700] + ('...' if len(text) > 700 else '')

async def fetch_and_parse_source(session, source_url: str):
    """
    Парсить одне джерело (зазвичай через RSS) та повертає список нових, актуальних новин.
    """
    news_items = []
    logger.info(f"Парсинг: {source_url}")

    # 1. Визначення URL для RSS (налаштування для деяких джерел)
    rss_url = source_url.rstrip('/') + '/rss'
    if source_url.endswith("/news"): # Forbes
        rss_url = "https://forbes.ua/rss"
    elif source_url.startswith("https://ua.korrespondent.net/"):
        rss_url = "https://ua.korrespondent.net/rss"

    # 2. Запит
    try:
        async with session.get(rss_url, timeout=10) as response:
            if response.status != 200:
                logger.warning(f"Помилка {response.status} при отриманні RSS для {source_url}")
                return []
            content = await response.text()
    except Exception as e:
        logger.error(f"Помилка AIOHTTP для {rss_url}: {e}")
        return []

    # 3. Парсинг RSS
    feed = feedparser.parse(content)
    now_kyiv = datetime.now(KYIV_TZ)
    max_age_dt = timedelta(minutes=MAX_AGE_MIN)
    source_domain = urlparse(source_url).netloc

    for entry in feed.entries[:FETCH_LIMIT]: # Беремо лише перші N новин
        try:
            url = entry.link
            title = entry.title
            # Використовуємо description, якщо summary відсутній
            summary = normalize_summary(entry.summary if hasattr(entry, 'summary') else (entry.description if hasattr(entry, 'description') else entry.title))
            image_url = extract_image_url(entry)
            published_time = parse_published_time(entry, source_url)

            # 5. 🕒 Логіка актуальності
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
            logger.warning(f"Помилка обробки запису з {source_url}: {e}")
            continue

    logger.info(f"Знайдено {len(news_items)} актуальних новин у {source_domain}")
    return news_items

async def collect_all_news():
    """
    Паралельно парсить усі джерела і збирає всі нові та актуальні новини.
    """
    all_news = []
    # Створюємо асинхронну сесію для ефективного керування підключеннями
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_and_parse_source(session, source) for source in SOURCES]
        # Запускаємо всі парсери одночасно
        results = await asyncio.gather(*tasks)

        for news_list in results:
            all_news.extend(news_list)

    # Сортуємо новини за часом публікації (найновіші перші)
    all_news.sort(key=lambda x: x['published_at'], reverse=True)

    logger.info(f"Всього знайдено {len(all_news)} актуальних новин з усіх джерел.")
    return all_news

# --- 4. АВТОМАТИЧНИЙ ПОСТИНГ ТА ЛОГІКА ---

async def post_news_cycle(bot: Bot):
    """
    Основний цикл парсингу, фільтрації, постингу та збереження.
    """
    start_time = datetime.now()
    logger.info("--- Запуск циклу автопостингу ---")

    # 1. Парсинг усіх сайтів
    all_news = await collect_all_news()

    # 2. Відбір нових новин (зберігання в базу з унікальною URL)
    # Зберігаємо всі знайдені новини, які пройшли фільтр актуальності.
    # 'inserted_urls' містить лише URL, які були ВСТАВЛЕНІ (тобто нові, не дублікати)
    inserted_urls = await insert_news(all_news)

    # 3. Фільтруємо список новин, щоб постити лише ті, що були щойно вставлені
    news_to_post = [
        news for news in all_news
        if news['url'] in inserted_urls
    ]
    # Обмежуємо до 50 новин
    news_to_post = news_to_post[:MAX_NEWS_PER_CYCLE]

    posted_count = 0
    urls_posted_successfully = []

    for news in news_to_post:
        # 4. Формування та публікація повідомлення
        text = f"📰 <b>{news['title']}</b>\n\n{news['summary']}\n\n🔗 Джерело: {news['url']}"

        try:
            if news['image_url']:
                # Публікація з фото (якщо є)
                await bot.send_photo(
                    chat_id=CHANNEL_ID,
                    photo=news['image_url'],
                    caption=text,
                    parse_mode=ParseMode.HTML
                )
            else:
                # Публікація без фото
                await bot.send_message(
                    chat_id=CHANNEL_ID,
                    text=text,
                    parse_mode=ParseMode.HTML
                )
            urls_posted_successfully.append(news['url'])
            posted_count += 1
            # Затримка між постами, щоб уникнути спаму та лімітів Telegram
            await asyncio.sleep(0.5) 
        except Exception as e:
            logger.error(f"Помилка постингу новини {news['url']}: {e}")
            # Якщо постингу не відбулося, не оновлюємо posted_at

    # 5. Оновлення posted_at для успішно опублікованих новин
    # Це гарантує, що posted_at відображає фактичний час публікації в Telegram
    await update_posted_at(urls_posted_successfully)


    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    logger.info(f"--- Цикл завершено. Опубліковано: {posted_count} новин за {duration:.2f} сек. ---")


# --- 5. КОМАНДИ АДМІНІСТРАТОРА (aiogram) ---

# /status
@Command("status")
@F.from_user.id.in_({ADMIN_ID}) # Обмежуємо доступ лише для ADMIN_ID
async def cmd_status(message: types.Message):
    """Показує статистику бота."""
    total_news = 0
    last_posted = "Немає даних"
    try:
        if db_pool:
            async with db_pool.acquire() as conn:
                total_news = await conn.fetchval("SELECT COUNT(*) FROM news")
                # Знаходимо час останнього успішного постингу (де posted_at не NULL, якщо була змінена схема)
                last_posted_dt = await conn.fetchval("SELECT MAX(posted_at) FROM news")
                if last_posted_dt:
                    # Приводимо час до Київського для відображення
                    last_posted = last_posted_dt.astimezone(KYIV_TZ).strftime("%d.%m.%Y %H:%M:%S")

    except Exception as e:
        logger.error(f"Помилка отримання статусу БД: {e}")
        total_news = "Помилка"
        last_posted = "Помилка БД"

    status_message = (
        "🤖 **Статус NewsAutoPoster UA**\n\n"
        f"🔸 **Новин у базі:** `{total_news}`\n"
        f"🔸 **Кількість джерел:** `{len(SOURCES)}`\n"
        f"🔸 **Час циклу:** кожні `{POSTING_INTERVAL_MIN}` хв\n"
        f"🔸 **Час останньої публікації:** `{last_posted}`\n"
        f"🔸 **Актуальність:** не старше `{MAX_AGE_MIN}` хв"
    )
    await message.answer(status_message, parse_mode=ParseMode.MARKDOWN)

# /forcepost
@Command("forcepost")
@F.from_user.id.in_({ADMIN_ID})
async def cmd_forcepost(message: types.Message, bot: Bot):
    """Запускає позачергову перевірку і постинг новин."""
    await message.answer("🔄 Запускаю позачерговий цикл парсингу та постингу...")
    await post_news_cycle(bot)
    await message.answer("✅ Позачерговий цикл завершено.")

# /stats
@Command("stats")
@F.from_user.id.in_({ADMIN_ID})
async def cmd_stats(message: types.Message):
    """Показує кількість опублікованих новин за добу."""
    news_24h = 0
    try:
        if db_pool:
            yesterday = datetime.now(KYIV_TZ) - timedelta(hours=24)
            async with db_pool.acquire() as conn:
                # Рахуємо новини, опубліковані за останні 24 години
                news_24h = await conn.fetchval(
                    "SELECT COUNT(*) FROM news WHERE posted_at >= $1", yesterday
                )
    except Exception as e:
        logger.error(f"Помилка отримання статистики: {e}")
        news_24h = "Помилка"

    stats_message = (
        "📊 **Статистика публікацій**\n\n"
        f"🔸 **Опубліковано за останні 24 год:** `{news_24h}`"
    )
    await message.answer(stats_message, parse_mode=ParseMode.MARKDOWN)

# --- 6. ОСНОВНИЙ ЦИКЛ (asyncio) ---

async def auto_posting_loop(bot: Bot):
    """Безкінечний цикл для автопостингу кожні 5 хвилин."""
    # Чекаємо 10 секунд після запуску, щоб ініціалізація завершилась
    await asyncio.sleep(10)
    while True:
        try:
            await post_news_cycle(bot)
        except Exception as e:
            logger.error(f"Критична помилка в циклі автопостингу: {e}")
        finally:
            logger.info(f"Очікування {POSTING_INTERVAL_MIN} хвилин...")
            await asyncio.sleep(POSTING_INTERVAL_MIN * 60)

async def main():
    """Ініціалізація та запуск бота."""
    if not all([BOT_TOKEN, DATABASE_URL, CHANNEL_ID]) or ADMIN_ID == 0:
        logger.critical("Не всі змінні середовища встановлені. Перевірте BOT_TOKEN, DATABASE_URL, CHANNEL_ID та ADMIN_ID.")
        return

    # Ініціалізація бази даних
    await init_db_pool()
    if not db_pool:
        logger.critical("Не вдалося підключитися до бази даних. Завершення.")
        return

    # Ініціалізація Telegram
    bot = Bot(token=BOT_TOKEN, parse_mode=ParseMode.HTML)
    global dp
    dp = Dispatcher()
    
    # Реєстрація команд адміністратора
    dp.message.register(cmd_status, Command("status"), F.from_user.id == ADMIN_ID)
    dp.message.register(cmd_forcepost, Command("forcepost"), F.from_user.id == ADMIN_ID)
    dp.message.register(cmd_stats, Command("stats"), F.from_user.id == ADMIN_ID)

    # Запуск безкінечного циклу автопостингу
    loop = asyncio.get_event_loop()
    loop.create_task(auto_posting_loop(bot))
    logger.info("Бот запущено. Початок роботи.")

    try:
        # Запускаємо polling (оскільки Render добре підходить для постійних процесів)
        await dp.start_polling(bot)
    finally:
        # Закриття ресурсів
        await bot.session.close()
        if db_pool:
            await db_pool.close()
        logger.info("Бот зупинено.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Програма зупинена користувачем.")
    except Exception as e:
        logger.critical(f"Загальна помилка при запуску: {e}")
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
# До 100 новин за цикл
MAX_NEWS_PER_CYCLE = 100   
MAX_AGE_MIN = 20          # Не публікувати новини старше 20 хвилин

# Додано User-Agent для обходу 403 помилок
DEFAULT_HEADERS = {
    # ВИПРАВЛЕНО: Більш надійний User-Agent, що імітує Chrome
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36', 
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language': 'uk-UA,uk;q=0.9,en-US;q=0.8,en;q=0.7',
}

# 1. 📰 Джерела новин (ФІНАЛЬНА ОПТИМІЗАЦІЯ)
SOURCES = [
    "https://tsn.ua/rss/all.xml",
    "https://www.pravda.com.ua/rss/news/",
    "https://censor.net/rss/all_news",
    "https://www.bbc.com/ukrainian/index.xml",
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
    "https://uain.press/rss",
    "https://suspilne.media/feed/",
    "https://delo.ua/rss/all.xml",
    "https://www.segodnya.ua/rss/all.xml"
]

FETCH_LIMIT = 500

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
        db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=10)
        logger.info("Успішно підключено до Neon PostgreSQL.")
        await create_news_table()
    except Exception as e:
        logger.critical(f"Помилка підключення до БД: {e}")
        db_pool = None 

async def create_news_table():
    """Створює таблицю 'news', якщо вона не існує."""
    # Таблиця 'news' слугує для унікальності та історії
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
    """
    Вставляє список новин у базу даних.
    ON CONFLICT (url) DO NOTHING забезпечує, що новина ніколи не буде вставлена двічі.
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
        async with conn.transaction():
            for news_item in news_list:
                published_at = news_item.get('published_at')
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
    """
    Парсить одне джерело (зазвичай через RSS) та повертає список нових, актуальних новин.
    """
    news_items = []

    # 1. Визначення URL для RSS
    rss_path = '/rss'
    
    # Спеціальні випадки (ФІНАЛЬНА КОРЕКЦІЯ RSS-ШЛЯХІВ)
    
    if "epravda.com.ua" in source_url:
        rss_path = "/rss/"
    elif "eurointegration.com.ua" in source_url:
        # НОВА СПРОБА: більш явний шлях
        rss_path = "/rss/news.xml" 
    elif "liga.net" in source_url:
        rss_path = "/rss.xml" 
    elif "rbc.ua" in source_url:
        # НОВА СПРОБА: Загальний фід
        rss_path = "/all/rss" 
    elif "ukrinform.ua" in source_url:
        # НОВА СПРОБА: Загальний фід
        rss_path = "/rss/all.rss"
    elif "tsn.ua" in source_url:
        rss_path = "/rss"
    elif "bbci.co.uk" in source_url:
        # Фід вже є повним URL, не додаємо шлях
        rss_url = source_url
        source_domain = "bbc.com/ukrainian" 
        pass 
    elif "korrespondent.net" in source_url:
        # НОВА СПРОБА: загальний фід
        rss_path = "/rss/all_news" 
    elif "obozrevatel.com" in source_url:
        # НОВА СПРОБА: Фід новин
        rss_path = "/rss/news.rss" 
    elif "news.finance.ua" in source_url:
        rss_path = "/ua/rss"
    elif "suspilne.media" in source_url:
        # ПОМИЛКА 403: залишаємо як є, сподіваємося на User-Agent
        rss_path = "/rss/all"
    elif "unian.ua" in source_url:
        # НОВА СПРОБА: З піддоменом (більш надійний)
        rss_url = source_url.replace("www.", "rss.") + "/rss/news/ukr/feed"
        source_domain = "unian.ua"
        pass
    elif "interfax.com.ua" in source_url: 
        # НОВА СПРОБА: змінено .xml на .rss
        rss_path = "/news/ukraine.rss"
    elif "nv.ua" in source_url:
        rss_path = "/ukr/rss/all.xml"
    elif "zaxid.net" in source_url:
        rss_path = "/rss"
    elif "hromadske.ua" in source_url:
        # НОВА СПРОБА: змінено .rss на .xml
        rss_path = "/feeds/all.xml"
    elif "censor.net" in source_url:
        # ПОМИЛКА 403: залишаємо як є, сподіваємося на User-Agent
        rss_path = "/news/rss.xml"
    elif "minfin.com.ua" in source_url:
        # НОВА СПРОБА: /rss/feed/
        rss_path = "/rss/feed/"
    elif "gazeta.ua" in source_url:
        # НОВА СПРОБА: /rss/all.rss
        rss_path = "/rss/all.rss"
    elif "focus.ua" in source_url:
        # НОВА СПРОБА: загальний /rss
        rss_path = "/rss"
    elif "apostrophe.ua" in source_url:
        # НОВА СПРОБА: /rss/feed
        rss_path = "/rss/feed"
    
    # Якщо це не BBC або UNIAN, формуємо URL зі шляху
    if "bbci.co.uk" not in source_url and "unian.ua" not in source_url:
        rss_url = source_url.rstrip('/') + rss_path
        source_domain = urlparse(source_url).netloc
    
    # 2. Запит
    try:
        # Використовуємо заголовки DEFAULT_HEADERS для обходу 403
        async with session.get(rss_url, headers=DEFAULT_HEADERS, timeout=10) as response:
            if response.status != 200:
                logger.warning(f"Помилка {response.status} при отриманні RSS для {rss_url}")
                return []
            content = await response.text()
    except Exception as e:
        logger.error(f"Помилка AIOHTTP для {rss_url}: {e}")
        return []

    # 3. Парсинг RSS
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

            # 5. 🕒 Логіка актуальності (≤ 20 хв)
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

    return news_items

async def collect_all_news():
    """
    Паралельно парсить усі джерела і збирає всі нові та актуальні новини.
    """
    all_news = []
    # Збільшено таймаут на випадок повільних джерел
    timeout = aiohttp.ClientTimeout(total=45) 
    async with aiohttp.ClientSession(timeout=timeout) as session:
        tasks = [fetch_and_parse_source(session, source) for source in SOURCES]
        results = await asyncio.gather(*tasks)

        for news_list in results:
            all_news.extend(news_list)

    # Сортуємо: найновіші перші (забезпечує постинг у порядку появи з усіх каналів)
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

    all_news = await collect_all_news()
    # Вставляємо нові новини у базу. inserted_urls містить тільки ті, яких ще не було.
    inserted_urls = await insert_news(all_news)

    inserted_urls_set = set(inserted_urls)
    # Фільтруємо: беремо тільки щойно вставлені (нові) новини, до 100 штук.
    news_to_post = [
        news for news in all_news
        if news['url'] in inserted_urls_set
    ][:MAX_NEWS_PER_CYCLE]

    posted_count = 0
    urls_posted_successfully = []

    for news in news_to_post:
        # 1. Форматування джерела для чистого вигляду
        source_domain = news['source']
        source_name = source_domain.replace('www.', '').replace('.com', '').replace('.ua', '').replace('.net', '').replace('.org', '').title()
        
        # 2. НОВИЙ ФОРМАТ: як у популярних TG-каналах
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
            # Невелика затримка, щоб уникнути спам-блокування від Telegram
            await asyncio.sleep(0.5) 
        except Exception as e:
            logger.error(f"Помилка постингу новини {news['url']}: {e}")

    # ОНОВЛЕННЯ: Маркуємо новини як опубліковані (збереження історії)
    if urls_posted_successfully:
        await update_posted_at(urls_posted_successfully)

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    logger.info(f"--- Цикл завершено. Опубліковано: {posted_count} новин за {duration:.2f} сек. ---")


# --- 5. КОМАНДИ АДМІНІСТРАТОРА (aiogram) ---

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
        "🤖 **Статус NewsAutoPoster UA**\n\n"
        f"🔸 **Новин у базі (Всього):** `{total_news}`\n"
        f"🔸 **Новин у черзі (Неопубл.):** `{unposted_news}`\n"
        f"🔸 **Кількість джерел:** `{len(SOURCES)}`\n"
        f"🔸 **Час циклу:** кожні `{POSTING_INTERVAL_MIN}` хв\n"
        f"🔸 **Час останньої публікації:** `{last_posted}`\n"
        f"🔸 **Актуальність:** не старше `{MAX_AGE_MIN}` хв"
    )
    await message.answer(status_message, parse_mode=ParseMode.MARKDOWN)

async def cmd_forcepost(message: types.Message, bot: Bot):
    """Запускає позачергову перевірку і постинг новин."""
    await message.answer("🔄 Запускаю позачерговий цикл парсингу та постингу...")
    await post_news_cycle(bot)
    await message.answer("✅ Позачерговий цикл завершено.")

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

# --- 6. ОСНОВНИЙ ЦИКЛ (asyncio) ---

async def auto_posting_loop(bot: Bot):
    """Безкінечний цикл для автопостингу кожні 5 хвилин."""
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

    await init_db_pool()
    if not db_pool:
        logger.critical("Не вдалося підключитися до бази даних. Завершення.")
        return

    # Ініціалізація Bot з DefaultBotProperties (виправлення aiogram 3.x помилки)
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

    loop = asyncio.get_event_loop()
    loop.create_task(auto_posting_loop(bot))
    logger.info("Бот запущено. Початок роботи.")

    try:
        # 🚨 АВТОМАТИЧНЕ ВИМКНЕННЯ WEBHOOK з повторними спробами
        for i in range(3):
            try:
                logger.info(f"Спроба {i+1}/3: Примусове вимкнення Webhook...")
                await bot.delete_webhook(drop_pending_updates=True)
                logger.info("Webhook успішно вимкнено.")
                break
            except Exception as e:
                logger.warning(f"Помилка вимкнення Webhook: {e}. Затримка 5 сек...")
                await asyncio.sleep(5)

        await dp.start_polling(bot)
    finally:
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
        pass
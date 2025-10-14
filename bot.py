import os
import asyncio
import logging
import re
import random
import sys
import json
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse, urljoin
from typing import Dict, Any, List, Optional, Tuple

import asyncpg
import aiohttp
from aiohttp import ClientSession, web
import feedparser
from bs4 import BeautifulSoup
from PIL import Image, ImageDraw, ImageFont # Модуль, який був відсутній (Pillow)

from aiogram import Bot, Dispatcher, types, F
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.client.default import DefaultBotProperties
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, BotCommand
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

# --- 1. НАЛАШТУВАННЯ СЕРЕДОВИЩА ТА ЛОГУВАННЯ ---

KYIV_TZ = timezone(timedelta(hours=3), 'Europe/Kyiv')

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    stream=sys.stdout)
logger = logging.getLogger(__name__)
# Встановлення рівня логування для aiogram
logging.getLogger('aiogram').setLevel(logging.WARNING)

# Змінні оточення
BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")

# Нові змінні для Webhook
WEBHOOK_HOST = os.getenv("WEBHOOK_HOST") # Ваш публічний домен (обов'язково HTTPS)
WEB_SERVER_HOST = os.getenv("WEB_SERVER_HOST", "0.0.0.0") # Хост для сервера aiohttp
WEB_SERVER_PORT = int(os.getenv("PORT", 8080)) # Порт для сервера aiohttp

# Шляхи та URL для Webhook
WEBHOOK_PATH = f"/webhook/{BOT_TOKEN}"
WEBHOOK_URL = f"{WEBHOOK_HOST}{WEBHOOK_PATH}" if WEBHOOK_HOST else None

# Параметри публікації та збору
NEWS_CHANNEL_ID = os.getenv("NEWS_CHANNEL_ID", "@your_channel_username_or_id")
POST_INTERVAL_MINUTES = int(os.getenv("POST_INTERVAL_MINUTES", 5))
COLLECTION_INTERVAL_MINUTES = int(os.getenv("COLLECTION_INTERVAL_MINUTES", 20))

# --- 2. КОНФІГУРАЦІЯ RSS ДЖЕРЕЛ ---
# Додано більше джерел та оновлено неробочі посилання (за винятком тих, які не зміг знайти)

RSS_FEEDS = {
    "Ukrinform": "https://www.ukrinform.ua/rss/all.xml", # 404 - залишив
    "Korrespondent": "https://ua.korrespondent.net/rss/all", # 404 - залишив
    "UNIAN": "https://www.unian.ua/rss/news.rss", # 404 - залишив
    "Obozrevatel": "https://www.obozrevatel.com/rss/main.xml", # 404 - залишив
    "Hromadske": "https://hromadske.ua/rss", # Оновлено
    "Mind.ua": "https://mind.ua/rss/all", # Оновлено
    "Delo.ua": "https://delo.ua/rss/", # Оновлено
    "Censor.net": "https://censor.net/ua/rss", # Оновлено
    "BBC Ukraine": "https://www.bbc.com/ukrainian/rss.xml", # 404 - залишив
    "Suspіlne": "https://suspilne.media/rss/all.xml", # Оновлено
    "Liga.net": "https://www.liga.net/rss/all.xml",
    "NV": "https://nv.ua/rss/all.xml",
    "Glavcom": "https://glavcom.ua/xml/news.xml",
    "TCH": "https://tsn.ua/rss/full.rss",
    "Dzerkalo Tyzhnya": "https://zn.ua/rss/ukr/all.xml",
}


# --- 3. СТАТУСИ ТА ДОПОМІЖНІ КЛАСИ ---

class UserStates(StatesGroup):
    """Статуси для FSM"""
    waiting_for_config = State()


class EconomicEngine:
    """Заглушка для класу, який, ймовірно, використовується в middleware"""
    def __init__(self, pool):
        self.pool = pool
        logger.info("EconomicEngine initialized.")

    async def get_user_balance(self, user_id):
        return random.randint(100, 1000)


# --- 4. ФУНКЦІЇ БАЗИ ДАНИХ (DB) ---

async def create_db_pool() -> asyncpg.Pool:
    """Створення пулу підключень до PostgreSQL"""
    if not DATABASE_URL:
        logger.error("DATABASE_URL is not set!")
        raise ValueError("DATABASE_URL is required.")

    # Обробка URL для Heroku/Render, де використовується 'postgres://' замість 'postgresql://'
    fixed_url = DATABASE_URL.replace("postgres://", "postgresql://", 1)

    pool = await asyncpg.create_pool(fixed_url)
    logger.info("Database pool created successfully.")
    
    # Створення таблиці, якщо вона не існує
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS news (
                id SERIAL PRIMARY KEY,
                title TEXT NOT NULL,
                link TEXT UNIQUE NOT NULL,
                source TEXT NOT NULL,
                published_at TIMESTAMP WITH TIME ZONE NOT NULL,
                saved_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS posted_news (
                news_link TEXT PRIMARY KEY,
                posted_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        """)
        logger.info("Database schema checked/updated.")

    return pool

async def get_last_posted_news(pool: asyncpg.Pool) -> List[str]:
    """Отримання списку посилань на останні опубліковані новини"""
    async with pool.acquire() as conn:
        # Отримати 100 останніх опублікованих посилань
        result = await conn.fetch("SELECT news_link FROM posted_news ORDER BY posted_at DESC LIMIT 100")
        return [row['news_link'] for row in result]

async def save_new_news(pool: asyncpg.Pool, news_items: List[Dict[str, Any]]):
    """Зберігання нових новин у БД"""
    if not news_items:
        return 0

    unique_items = []
    # Фільтрація дублікатів по link перед вставкою
    existing_links = await pool.fetchval("SELECT array_agg(link) FROM news WHERE link IN ($1::text[])", [item['link'] for item in news_items])
    existing_links = set(existing_links) if existing_links else set()

    for item in news_items:
        if item['link'] not in existing_links:
            unique_items.append(item)

    if not unique_items:
        logger.info("No genuinely new news items to save after filtering existing links.")
        return 0

    # Використання copy_records_to_table для швидкої масової вставки
    columns = ('title', 'link', 'source', 'published_at')
    records = [(item['title'], item['link'], item['source'], item['published_at']) for item in unique_items]
    
    # Виконання вставки (використовуємо `copy_records_to_table` в asyncpg)
    async with pool.acquire() as conn:
        result = await conn.copy_records_to_table('news', records=records, columns=columns)
        count = len(records)
        logger.info(f"💾 Успішно збережено {count} нових новин.")
        return count


async def get_next_news_for_posting(pool: asyncpg.Pool) -> Optional[Dict[str, Any]]:
    """Отримання наступної новини для публікації"""
    async with pool.acquire() as conn:
        # Вибірка найстарішої новини, яка ще не була опублікована
        query = """
        SELECT n.title, n.link, n.source
        FROM news n
        LEFT JOIN posted_news pn ON n.link = pn.news_link
        WHERE pn.news_link IS NULL
        ORDER BY n.published_at ASC
        LIMIT 1;
        """
        record = await conn.fetchrow(query)
        if record:
            return dict(record)
        return None

async def mark_news_as_posted(pool: asyncpg.Pool, news_link: str):
    """Позначення новини як опублікованої"""
    async with pool.acquire() as conn:
        # Використовуємо INSERT INTO ... ON CONFLICT DO NOTHING для уникнення дублікатів
        await conn.execute(
            "INSERT INTO posted_news (news_link) VALUES ($1) ON CONFLICT (news_link) DO NOTHING",
            news_link
        )


# --- 5. ФУНКЦІЇ RSS ТА ПАРСИНГУ ---

def parse_rss_feed(feed_url: str, source_name: str, kyiv_tz: timezone) -> List[Dict[str, Any]]:
    """Синхронний парсинг одного RSS-фіда (блокуючий виклик)"""
    try:
        # Використовуємо 'lxml' для швидшого парсингу, якщо він доступний
        feed = feedparser.parse(feed_url, response_headers={'content-type': 'text/xml'}, response_type='xml')
    except Exception as e:
        logger.error(f"Помилка парсингу RSS для {source_name} ({feed_url}): {e}")
        return []

    news_list = []
    for entry in feed.entries:
        try:
            title = getattr(entry, 'title', 'Без заголовка').strip()
            link = getattr(entry, 'link', None)
            published_time = getattr(entry, 'published_parsed', None)

            if not link or not title or not published_time:
                continue

            # Конвертація часу
            published_dt = datetime(*published_time[:6], tzinfo=timezone.utc).astimezone(kyiv_tz)

            news_list.append({
                'title': title,
                'link': link,
                'source': source_name,
                'published_at': published_dt,
            })
        except Exception as e:
            logger.warning(f"Помилка обробки запису з {source_name}: {e}")
            continue

    return news_list

async def fetch_rss_feed(session: ClientSession, feed_url: str, source_name: str) -> List[Dict[str, Any]]:
    """Асинхронне отримання та парсинг RSS-фіда з обробкою HTTP помилок"""
    logger.debug(f"Завантаження RSS: {source_name} ({feed_url})")
    try:
        async with session.get(feed_url, timeout=10, ssl=False) as response:
            if response.status in (404, 403):
                logger.warning(f"⚠️ HTTP Помилка {response.status} для {feed_url}")
                return []
            
            # Читання контенту як тексту
            content = await response.text()
            
            # Виконання блокуючого парсингу в окремому потоці
            news_items = await asyncio.to_thread(feedparser.parse, content)

            parsed_list = []
            for entry in news_items.entries:
                try:
                    title = getattr(entry, 'title', 'Без заголовка').strip()
                    link = getattr(entry, 'link', None)
                    published_time = getattr(entry, 'published_parsed', None)

                    if not link or not title or not published_time:
                        continue

                    published_dt = datetime(*published_time[:6], tzinfo=timezone.utc).astimezone(KYIV_TZ)
                    
                    parsed_list.append({
                        'title': title,
                        'link': link,
                        'source': source_name,
                        'published_at': published_dt,
                    })
                except Exception as e:
                    logger.warning(f"Помилка обробки запису з {source_name}: {e}")

            return parsed_list

    except aiohttp.ClientConnectorError as e:
        logger.warning(f"⚠️ Помилка з'єднання для {feed_url}: {e}")
    except asyncio.TimeoutError:
        logger.warning(f"⚠️ Час очікування минув для {feed_url}")
    except Exception as e:
        logger.error(f"Непередбачена помилка при завантаженні RSS {feed_url}: {e}", exc_info=True)

    return []


async def collect_news(pool: asyncpg.Pool, session: ClientSession):
    """Основний цикл збору новин"""
    start_time = datetime.now()
    all_news_items = []

    # Створення асинхронних завдань для всіх RSS-фідів
    fetch_tasks = [
        fetch_rss_feed(session, url, name)
        for name, url in RSS_FEEDS.items()
    ]

    # Виконання всіх завдань паралельно
    results = await asyncio.gather(*fetch_tasks)
    
    # Зведення результатів
    total_found = 0
    for news_list in results:
        if news_list:
            all_news_items.extend(news_list)
            total_found += len(news_list)

    duration = (datetime.now() - start_time).total_seconds()
    logger.info(f"📰 Знайдено {total_found} новин з {len(RSS_FEEDS)} джерел за {duration:.2f} сек.")

    # Збереження унікальних новин у БД
    saved_count = await save_new_news(pool, all_news_items)
    logger.info(f"💾 Успішно збережено {saved_count} нових новин. Тривалість: {duration:.2f}с.")
    logger.info(f"Очікування {COLLECTION_INTERVAL_MINUTES} хвилин до наступного збору...")

    # Встановлення нового таймера збору
    asyncio.create_task(
        schedule_next_collection(pool, session, COLLECTION_INTERVAL_MINUTES)
    )

def format_news_message(news: Dict[str, Any]) -> str:
    """Форматування повідомлення для публікації"""
    # HTML формат для кращої читабельності
    message = (
        f"<b>📰 НОВА НОВИНА!</b>\n\n"
        f"<b>{news['title']}</b>\n\n"
        f"🔗 <a href='{news['link']}'>Читати повністю на {news['source']}</a>\n\n"
        f"<i>#Новини #Україна #Політика</i>"
    )
    return message


# --- 6. ФУНКЦІЇ ДЛЯ ГЕНЕРАЦІЇ ЗОБРАЖЕНЬ (PIL/Pillow) ---

# Ця функція використовує PIL, який був відсутній.
# Її потрібно реалізувати, якщо ви плануєте публікувати новини з зображеннями.
def create_news_image(news: Dict[str, Any]) -> bytes:
    """
    Створення зображення з заголовком новини
    ПОВИННО БУТИ РЕАЛІЗОВАНО! Зараз повертає заглушку.
    """
    try:
        # Приклад використання PIL
        img = Image.new('RGB', (800, 400), color = (255, 255, 255))
        d = ImageDraw.Draw(img)
        
        # Використання шрифту (потрібно мати .ttf файл, або використовувати вбудований)
        # font = ImageFont.truetype("Arial.ttf", 30) 
        d.text((10,10), f"Новина від {news['source']}:\n{news['title']}", fill=(0,0,0))

        # Збереження в байтах
        import io
        byte_arr = io.BytesIO()
        img.save(byte_arr, format='PNG')
        return byte_arr.getvalue()
    except Exception as e:
        logger.error(f"Помилка при генерації зображення новини: {e}")
        # Повертаємо порожні байти, щоб уникнути помилки відправки
        return b''

async def post_news_to_channel(pool: asyncpg.Pool, bot: Bot):
    """Публікація наступної новини в канал"""
    news_item = await get_next_news_for_posting(pool)

    if not news_item:
        logger.info("--- 📭 Немає нових новин для публікації. ---")
    else:
        # 1. Форматування повідомлення
        message_text = format_news_message(news_item)
        
        # 2. Генерація зображення (опціонально)
        # image_bytes = await asyncio.to_thread(create_news_image, news_item)
        
        try:
            # Якщо ви використовуєте фото, використовуйте `bot.send_photo`
            # Якщо просто текст, використовуйте `bot.send_message`
            await bot.send_message(
                chat_id=NEWS_CHANNEL_ID,
                text=message_text,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True # Краще вимкнути для чистоти, якщо надсилаєте фото
            )
            
            await mark_news_as_posted(pool, news_item['link'])
            logger.info("--- ✅ Опубліковано 1 новин. ---")
            
        except Exception as e:
            logger.error(f"Помилка публікації новини {news_item['link']}: {e}", exc_info=True)

    logger.info(f"Очікування {POST_INTERVAL_MINUTES} хвилин до наступного посту...")
    asyncio.create_task(
        schedule_next_post(pool, bot, POST_INTERVAL_MINUTES)
    )


# --- 7. ПЛАНУВАЛЬНИКИ ЗАВДАНЬ ---

async def schedule_next_post(pool: asyncpg.Pool, bot: Bot, delay_minutes: int):
    """Планувальник для циклу постингу"""
    await asyncio.sleep(delay_minutes * 60)
    logger.info("--- ▶️ Запуск циклу постингу ---")
    await post_news_to_channel(pool, bot)

async def schedule_next_collection(pool: asyncpg.Pool, session: ClientSession, delay_minutes: int):
    """Планувальник для циклу збору новин"""
    await asyncio.sleep(delay_minutes * 60)
    logger.info("--- 🔄 Запуск циклу збору новин ---")
    await collect_news(pool, session)


# --- 8. ХЕНДЛЕРИ КОМАНД (ЗАГЛУШКИ) ---

@Command("start")
async def handle_start(message: types.Message, bot: Bot, pool: asyncpg.Pool):
    """Хендлер команди /start"""
    user_id = message.from_user.id
    username = message.from_user.username or user_id
    
    # Використовуйте ParseMode.HTML для форматування
    await message.answer(
        f"Привіт, <b>@{username}</b>! Я ваш бот новин.",
        parse_mode=ParseMode.HTML
    )

@Command("help")
async def handle_help(message: types.Message):
    """Хендлер команди /help"""
    await message.answer(
        "Я збираю та публікую останні новини в канал. Зверніться до адміністратора для налаштування.",
        parse_mode=ParseMode.HTML
    )


# --- 9. ФУНКЦІЇ ЗАПУСКУ (WEBHOOK/POLLING) ---

async def handle_webhook(request: web.Request):
    """Хендлер вхідних вебхуків"""
    bot: Bot = request.app['bot']
    dispatcher: Dispatcher = request.app['dp']

    if request.match_info.get('token') == BOT_TOKEN:
        # Отримання JSON тіла запиту
        data = await request.json()
        telegram_update = types.Update.model_validate(data, context={"bot": bot})
        
        # Обробка оновлення
        await dispatcher.feed_update(bot, telegram_update)
        return web.Response(status=200)
    
    # Для невірного токена
    return web.Response(status=403)

async def on_startup(app: web.Application):
    """Функція, що виконується при запуску Webhook сервера"""
    bot: Bot = app["bot"]
    pool: asyncpg.Pool = app["pool"]
    session: ClientSession = app["session"]
    
    # 1. Встановлення вебхука
    if WEBHOOK_URL:
        await bot.set_webhook(WEBHOOK_URL, allowed_updates=app["dp"].resolve_used_update_types())
        logger.info(f"Webhook встановлено на: {WEBHOOK_URL}")
    
    # 2. Запуск циклів збору та постингу
    # Запускаємо перший пост через 5 секунд, перший збір через 10 секунд
    asyncio.create_task(schedule_next_post(pool, bot, 5 / 60))
    asyncio.create_task(schedule_next_collection(pool, session, 10 / 60))


async def on_shutdown(app: web.Application):
    """Функція, що виконується при вимкненні Webhook сервера"""
    bot: Bot = app["bot"]
    pool: asyncpg.Pool = app["pool"]
    session: ClientSession = app["session"]
    
    # Видалення вебхука
    if WEBHOOK_URL:
        await bot.delete_webhook()
        logger.info("Webhook видалено.")

    # Закриття ресурсів
    await session.close()
    await pool.close()
    logger.info("Закриття з'єднань БД та aiohttp.")


async def main():
    """Головна функція запуску"""
    if not BOT_TOKEN:
        logger.error("BOT_TOKEN is not set!")
        return

    # Ініціалізація основних компонентів
    default_props = DefaultBotProperties(parse_mode=ParseMode.HTML)
    bot = Bot(token=BOT_TOKEN, default=default_props)
    dp = Dispatcher()

    # Реєстрація хендлерів
    dp.message.register(handle_start, Command("start"))
    dp.message.register(handle_help, Command("help"))

    # Створення пулу БД та сесії aiohttp
    try:
        pool = await create_db_pool()
        session = ClientSession(timeout=aiohttp.ClientTimeout(total=60))
        economic_engine = EconomicEngine(pool) # Використовується в оригіналі
    except Exception as e:
        logger.error(f"Критична помилка ініціалізації: {e}")
        return

    # Реєстрація залежностей для хендлерів DP (винесено в окремі функції)
    # Це забезпечує, що всі хендлери матимуть доступ до ресурсів
    dp.update.middleware.register(lambda handler, event, data: {
        **data, 
        'session': session, 
        'pool': pool, 
        'bot': bot, 
        'economic_engine': economic_engine
    })
    
    # --- ВИБІР РЕЖИМУ ЗАПУСКУ (FIX for TelegramConflictError) ---

    if WEBHOOK_HOST:
        # Режим Webhook (для Render/продакшену)
        app = web.Application()
        app["bot"] = bot
        app["dp"] = dp
        app["pool"] = pool
        app["session"] = session
        app["economic_engine"] = economic_engine

        # Реєстрація маршруту та функцій запуску/вимкнення
        app.router.add_post(WEBHOOK_PATH, handle_webhook) # Використовуємо WEBHOOK_PATH без параметра токена в шляху
        
        # Реєстрація функцій запуску/вимкнення
        app.on_startup.append(on_startup)
        app.on_shutdown.append(on_shutdown)
        
        logger.info(f"Запуск Webhook сервера на {WEB_SERVER_HOST}:{WEB_SERVER_PORT}")
        
        # Запуск сервера
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, host=WEB_SERVER_HOST, port=WEB_SERVER_PORT)
        await site.start()

        # Тримаємо main() увімкненою, поки працює Webhook
        await asyncio.Event().wait() 

    else:
        # Режим Polling (для локальної розробки)
        logger.info(f"Run polling for bot @{await bot.get_me()}.username id={bot.id} - '{await bot.get_me()}.first_name}'")
        
        # Запуск циклів збору та постингу
        asyncio.create_task(schedule_next_post(pool, bot, 1)) # Почати швидко
        asyncio.create_task(schedule_next_collection(pool, session, 5)) # Почати швидко

        # Запуск поллінгу - це блокуюча функція
        await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by KeyboardInterrupt.")
    except Exception as e:
        logger.critical(f"Fatal error in main execution: {e}", exc_info=True)

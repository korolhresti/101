import os
import asyncio
import logging
import re
import random
import sys
import json
import base64
import time # Додано time для роботи з time.struct_time
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse, urljoin
from typing import Dict, Any, List, Optional, Tuple

import asyncpg
import aiohttp
from aiohttp import ClientSession, web
import feedparser
from bs4 import BeautifulSoup

from aiogram import Bot, Dispatcher, types, F
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.client.default import DefaultBotProperties
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

# --- 1. НАЛАШТУВАННЯ СЕРЕДОВИЩА ТА ЛОГУВАННЯ ---

KYIV_TZ = timezone(timedelta(hours=3), 'Europe/Kyiv')

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    stream=sys.stdout)
logger = logging.getLogger(__name__)

# Змінні оточення
BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")

# Нові змінні для Webhook
WEBHOOK_HOST = os.getenv("WEBHOOK_HOST") # Ваш публічний домен (обов'язково HTTPS)
WEB_SERVER_HOST = os.getenv("WEB_SERVER_HOST", "0.0.0.0")
WEB_SERVER_PORT = int(os.getenv("PORT", 8080))

WEBHOOK_PATH = f"/webhook/{BOT_TOKEN}"
WEBHOOK_URL = urljoin(WEBHOOK_HOST, WEBHOOK_PATH) if WEBHOOK_HOST else None

if not all([BOT_TOKEN, DATABASE_URL, WEBHOOK_HOST]):
    logger.error("Необхідні змінні оточення (BOT_TOKEN, DATABASE_URL, WEBHOOK_HOST) не встановлені.")
    sys.exit(1)

# --- 2. ДОПОМІЖНІ КЛАСИ ТА ФУНКЦІЇ ---

class NewsFeeds:
    """Клас для зберігання RSS-посилань"""
    # Це список фідів, які були виявлені у вашому лозі.
    FEEDS = [
        "https://www.rbc.ua/static/rss/news.ukr.rss.xml",
        "https://www.liga.net/index.rss",
        "https://ua.interfax.com.ua/rss.xml",
        "https://hromadske.ua/rss",
        "https://minfin.com.ua/rss/news/all",
        "https://gazeta.ua/rss/life.rss",
        "https://focus.ua/rss",
        "https://delo.ua/rss/",
        "https://www.ukrinform.ua/rss/main.rss",
        "https://censor.net/ua/rss/all",
        "https://biz.censor.net/ru/rss/all",
        "https://www.obozrevatel.com/rss.xml",
        "https://www.unian.ua/rss/news.xml",
        "https://ua.korrespondent.net/rss",
        "https://www.pravda.com.ua/rus/rss_news/",
    ]

# Стани для FSM (якщо використовуються)
class UserState(StatesGroup):
    idle = State()

# --- 3. ФУНКЦІЇ ДЛЯ РОБОТИ З БАЗОЮ ДАНИХ (asyncpg) ---

async def init_db(pool: asyncpg.Pool):
    """Створює необхідні таблиці в базі даних."""
    async with pool.acquire() as conn:
        # Таблиця для підписників
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS subscribers (
                chat_id BIGINT PRIMARY KEY,
                is_active BOOLEAN DEFAULT TRUE,
                subscribed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        """)
        # Таблиця для новин (для уникнення дублікатів і очищення)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS news_articles (
                link TEXT PRIMARY KEY,
                title TEXT,
                published_at TIMESTAMP WITH TIME ZONE,
                source TEXT,
                published_to_telegram BOOLEAN DEFAULT FALSE,
                fetched_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        """)
    logger.info("Таблиці 'subscribers' та 'news_articles' перевірені/створені.")

async def add_subscriber(pool: asyncpg.Pool, chat_id: int):
    """Додає або активує підписника."""
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO subscribers (chat_id, is_active)
            VALUES ($1, TRUE)
            ON CONFLICT (chat_id) DO UPDATE SET is_active = TRUE;
        """, chat_id)

async def remove_subscriber(pool: asyncpg.Pool, chat_id: int):
    """Деактивує підписника."""
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE subscribers SET is_active = FALSE WHERE chat_id = $1;
        """, chat_id)

async def get_active_subscribers(pool: asyncpg.Pool) -> List[int]:
    """Отримує список активних chat_id."""
    async with pool.acquire() as conn:
        records = await conn.fetch("SELECT chat_id FROM subscribers WHERE is_active = TRUE;")
        return [r['chat_id'] for r in records]

async def save_article(pool: asyncpg.Pool, link: str, title: str, published_at: datetime, source: str) -> bool:
    """Зберігає нову статтю в БД і повертає True, якщо вона нова."""
    async with pool.acquire() as conn:
        # Перевіряємо, чи стаття вже існує
        existing = await conn.fetchval("SELECT link FROM news_articles WHERE link = $1", link)
        if existing:
            return False

        # Зберігаємо нову статтю
        await conn.execute("""
            INSERT INTO news_articles (link, title, published_at, source, published_to_telegram)
            VALUES ($1, $2, $3, $4, FALSE);
        """, link, title, published_at, source)
        return True

async def get_unpublished_articles(pool: asyncpg.Pool, limit: int = 5) -> List[Dict[str, Any]]:
    """Отримує список неопублікованих статей, сортуючи за датою."""
    async with pool.acquire() as conn:
        records = await conn.fetch("""
            SELECT link, title, published_at, source
            FROM news_articles
            WHERE published_to_telegram = FALSE
            ORDER BY published_at DESC, fetched_at ASC
            LIMIT $1;
        """, limit)
        return [dict(r) for r in records]

async def mark_article_as_published(pool: asyncpg.Pool, link: str):
    """Позначає статтю як опубліковану."""
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE news_articles SET published_to_telegram = TRUE WHERE link = $1;
        """, link)

async def cleanup_old_articles(pool: asyncpg.Pool, days: int = 7) -> int:
    """Видаляє старі опубліковані статті."""
    cutoff_date = datetime.now(KYIV_TZ) - timedelta(days=days)
    async with pool.acquire() as conn:
        result = await conn.execute("""
            DELETE FROM news_articles
            WHERE published_to_telegram = TRUE AND published_at < $1;
        """, cutoff_date)
        deleted_count = int(result.split()[-1])
        return deleted_count


# --- ФУНКЦІЯ ДЛЯ ВИПРАВЛЕННЯ ПОМИЛКИ З ДАТОЮ ---

def convert_parsed_time_to_datetime(parsed_time: Optional[time.struct_time]) -> datetime:
    """
    Конвертує time.struct_time, отриманий від feedparser (entry.published_parsed), у datetime.
    
    Це усуває помилку 'module 'feedparser' has no attribute '_time_parse'',
    використовуючи стандартний та публічний API feedparser.
    
    :param parsed_time: time.struct_time або None.
    :return: datetime в часовій зоні Києва або поточна дата/час, якщо парсинг невдалий.
    """
    if parsed_time is None:
        # Якщо feedparser не зміг розпізнати дату, використовуємо поточний час
        return datetime.now(KYIV_TZ)
    try:
        # 1. Конвертуємо time.struct_time у datetime (без урахування часового поясу)
        dt_naive = datetime(*parsed_time[:6])
        
        # 2. Робимо його усвідомленим UTC, оскільки feedparser.parsed_time зазвичай є UTC
        dt_utc_aware = dt_naive.replace(tzinfo=timezone.utc)
        
        # 3. Конвертуємо в часову зону Києва
        dt_kyiv = dt_utc_aware.astimezone(KYIV_TZ)
        return dt_kyiv
        
    except Exception as e:
        # Логування помилки, але повернення поточного часу для продовження роботи
        logger.warning(f"Помилка конвертації time.struct_time {parsed_time}: {e}. Використовується поточний час.")
        return datetime.now(KYIV_TZ)

# --- 4. ОСНОВНА ЛОГІКА ЗБОРУ НОВИН ---

async def fetch_news_articles(pool: asyncpg.Pool, session: ClientSession):
    """Асинхронно збирає нові статті з RSS-фідів."""
    logger.info("Початок процесу пошуку та зберігання нових статей.")
    # new_articles_with_photo_count = 0 # Залишив, але для повної логіки потрібно реалізувати пошук фото
    
    for feed_url in NewsFeeds.FEEDS:
        source_domain = urlparse(feed_url).netloc
        added_count_for_feed = 0
        
        try:
            # Використовуємо сесію aiohttp для отримання вмісту
            async with session.get(feed_url, timeout=15) as response:
                # Обов'язково вказуємо кодування, якщо воно не визначено коректно
                content = await response.text(encoding='utf-8')
                
            # Використовуємо feedparser для парсингу вмісту
            feed = feedparser.parse(content)
            
            for entry in feed.entries:
                link = entry.get('link')
                title = entry.get('title')
                
                # --- ВИКОРИСТАННЯ ВИПРАВЛЕНОЇ ЛОГІКИ ДАТИ ---
                published_parsed = entry.get('published_parsed')
                published_at = convert_parsed_time_to_datetime(published_parsed)
                # ---------------------------------------------
                
                # Приклад дуже спрощеної логіки визначення наявності фото
                has_photo = bool(re.search(r'<img', entry.get('summary', ''), re.IGNORECASE))
                
                if link and title:
                    is_new = await save_article(pool, link, title, published_at, source_domain)
                    if is_new:
                        added_count_for_feed += 1
                
        except Exception as e:
            logger.error(f"Помилка при обробці фідa {feed_url}: {e}")
            continue
            
        logger.info(f"Оброблено фід {source_domain}. Додано нових статей: {added_count_for_feed}")


# --- 5. ХЕНДЛЕРИ TELEGRAM (СКОРОЧЕНІ) ---

async def start_command(message: types.Message, pool: asyncpg.Pool):
    """Хендлер для команди /start."""
    await add_subscriber(pool, message.chat.id)
    await message.answer("Ласкаво просимо! Ви успішно підписалися на розсилку новин. Новини будуть надходити регулярно.")

async def stop_command(message: types.Message, pool: asyncpg.Pool):
    """Хендлер для команди /stop."""
    await remove_subscriber(pool, message.chat.id)
    await message.answer("Ви відписалися від розсилки новин. На все добре.")

# --- 6. ЦИКЛИ ФОНОВИХ ЗАВДАНЬ ---

async def news_search_and_publish_loop(pool: asyncpg.Pool, bot: Bot, session: ClientSession):
    """Головний цикл пошуку та публікації новин."""
    SEARCH_INTERVAL = 20 * 60  # 20 хвилин
    PUBLISH_INTERVAL = 5 * 60 # 5 хвилин
    
    logger.info(f"Запуск запланованого циклу новин. Початковий інтервал пошуку {SEARCH_INTERVAL//60} хв, публікації {PUBLISH_INTERVAL//60} хв.")

    while True:
        # --- Пошук новин ---
        logger.info(f"Настав час для пошуку нових статей. Інтервал: {SEARCH_INTERVAL//60} хв.")
        try:
            await fetch_news_articles(pool, session)
        except Exception as e:
            logger.error(f"Помилка у циклі пошуку новин: {e}")
            
        # --- Публікація новин (повторюється частіше) ---
        next_search_time = asyncio.get_event_loop().time() + SEARCH_INTERVAL
        
        while asyncio.get_event_loop().time() < next_search_time:
            logger.info(f"Настав час для публікації нових статей. Інтервал: {PUBLISH_INTERVAL//60} хв.")
            try:
                articles = await get_unpublished_articles(pool, limit=3) # Публікуємо по 3
                if articles:
                    subscribers = await get_active_subscribers(pool)
                    for article in articles:
                        # Створення повідомлення
                        published_date_str = article['published_at'].strftime("%H:%M, %d.%m")
                        message_text = (
                            f"📰 **{article['title']}**\n"
                            f"_({published_date_str} - Джерело: {article['source']})_\n\n"
                            f"🌐 [Читати повністю]({article['link']})"
                        )
                        
                        # Розсилка підписникам
                        for chat_id in subscribers:
                            try:
                                await bot.send_message(
                                    chat_id=chat_id,
                                    text=message_text,
                                    parse_mode=ParseMode.MARKDOWN
                                )
                                # Додаткова затримка між повідомленнями для уникнення FloodWait
                                await asyncio.sleep(0.05) 
                            except Exception as e:
                                logger.warning(f"Помилка відправки статті {article['link']} до {chat_id}: {e}")
                        
                        # Позначення як опублікованої
                        await mark_article_as_published(pool, article['link'])
                        
                else:
                    logger.info("Немає нових статей для публікації.")
                        
            except Exception as e:
                logger.error(f"Помилка у циклі публікації новин: {e}")
            
            await asyncio.sleep(PUBLISH_INTERVAL)

async def db_cleanup_loop(pool: asyncpg.Pool):
    """Цикл для очищення старих записів у базі даних."""
    CLEANUP_INTERVAL = 24 * 60 * 60 # 24 години
    DAYS_TO_KEEP = 7
    logger.info("Запуск циклу очищення БД.")
    
    while True:
        await asyncio.sleep(CLEANUP_INTERVAL)
        logger.info("Початок очищення БД.")
        try:
            deleted_count = await cleanup_old_articles(pool, days=DAYS_TO_KEEP)
            logger.info(f"Очищення БД завершено. Видалено старих опублікованих статей: {deleted_count}")
        except Exception as e:
            logger.error(f"Помилка у циклі очищення БД: {e}")


# --- 7. КОНФІГУРАЦІЯ WEBHOOK/DISPATCHER ---

async def handle_webhook(request: web.Request):
    """Обробник вхідних вебхуків Telegram."""
    bot: Bot = request.app['bot']
    dp: Dispatcher = request.app['dp']
    
    # Отримання вмісту тіла запиту
    update = await request.json()
    
    # Обробка оновлення
    try:
        await dp.feed_raw_update(bot, update)
        return web.Response(status=200)
    except Exception as e:
        logger.error(f"Помилка обробки webhook: {e}")
        return web.Response(status=500)

async def on_startup(app: web.Application):
    """Виконується при запуску web-сервера."""
    logger.info("Запуск on_startup...")
    bot: Bot = app['bot']
    pool: asyncpg.Pool = app['pool']
    session: ClientSession = app['session']

    # Ініціалізація БД
    await init_db(pool)

    # Встановлення Webhook
    try:
        await bot.set_webhook(WEBHOOK_URL)
        logger.info(f"Встановлення Webhook на URL: {WEBHOOK_URL} - Успішно.")
    except Exception as e:
        logger.error(f"Помилка встановлення Webhook: {e}")

    # Запуск фонових завдань
    # Створення та запуск фонових задач як завдань aiohttp
    app['news_task'] = asyncio.create_task(news_search_and_publish_loop(pool, bot, session))
    app['cleanup_task'] = asyncio.create_task(db_cleanup_loop(pool))
    
    logger.info(f"Запуск on_startup... Успішно. Запущено 2 фонові задачі.")

async def on_shutdown(app: web.Application):
    """Виконується при вимкненні web-сервера."""
    logger.info("Запуск on_shutdown...")
    bot: Bot = app['bot']
    pool: asyncpg.Pool = app['pool']
    session: ClientSession = app['session']

    # Скасування Webhook
    try:
        await bot.delete_webhook()
    except Exception as e:
        logger.error(f"Помилка скасування Webhook: {e}")

    # Скасування фонових завдань
    app['news_task'].cancel()
    app['cleanup_task'].cancel()
    
    # Закриття сесій
    await session.close()
    await pool.close()
    
    logger.info("Запуск on_shutdown... Успішно.")

# --- 8. ЗАПУСК ДОДАТКУ ---

async def main():
    """Основна функція запуску додатку."""
    # 1. Ініціалізація компонентів
    default = DefaultBotProperties(parse_mode=ParseMode.HTML)
    bot = Bot(token=BOT_TOKEN, default=default)
    dp = Dispatcher()
    
    # Реєстрація хендлерів
    dp.message.register(start_command, Command("start"))
    dp.message.register(stop_command, Command("stop"))

    # Створення пулу з'єднань до БД
    pool = await asyncpg.create_pool(DATABASE_URL)
    session = ClientSession() # Створення єдиної сесії aiohttp

    # 2. Налаштування aiohttp Web
    app = web.Application()
    
    # 3. Зберігання ресурсів у додатку
    app["bot"] = bot
    app["dp"] = dp
    app["pool"] = pool
    app["session"] = session

    # 4. Реєстрація залежностей для хендлерів DP
    # Використання lambda для інжекції залежностей (видалено 'economic_engine' та 'conn', які були зайвими)
    dp.message.outer_middleware.register(lambda handler, event, data: {**data, 'session': session, 'pool': pool, 'bot': bot})
    dp.callback_query.outer_middleware.register(lambda handler, event, data: {**data, 'pool': pool, 'bot': bot})
    
    # 5. Реєстрація маршруту для вебхука
    app.router.add_post(WEBHOOK_PATH, handle_webhook, name="webhook_handler")
    
    # 6. Реєстрація функцій запуску/вимкнення
    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)
    
    # 7. Запуск сервера
    logger.info(f"Запуск Webhook сервера на {WEB_SERVER_HOST}:{WEB_SERVER_PORT}")
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host=WEB_SERVER_HOST, port=WEB_SERVER_PORT)
    
    # Запуск сервера
    await site.start()
    
    # Утримання основного циклу aiohttp
    await asyncio.Event().wait()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Бот зупинено вручну.")
    except Exception as e:
        logger.error(f"Критична помилка запуску: {e}")

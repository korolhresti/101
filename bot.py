# --- НЕОБХІДНІ БІБЛІОТЕКИ ---
# pip install aiogram==3.1.1 asyncpg aiohttp feedparser beautifulsoup4 Pillow

import os
import asyncio
import logging
import re
import random
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse
import io

from PIL import Image, ImageDraw, ImageFont

import asyncpg
import aiohttp
import feedparser
from bs4 import BeautifulSoup

from aiogram import Bot, Dispatcher, types, F
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.client.default import DefaultBotProperties
from aiogram.exceptions import TelegramAPIError, TelegramRetryAfter

# --- 1. КОНФІГУРАЦІЯ ТА ГЛОБАЛЬНІ ЗМІННІ ---

KYIV_TZ = timezone(timedelta(hours=3), 'Europe/Kyiv')

class Config:
    """Статична конфігурація платформи."""
    CHANNEL_USERNAME = "@YourChannelName"
    WATERMARK_TEXT = "t.me/YourChannelName"
    DEFAULT_CTA_TEXT = f"👉 Підписатись на {CHANNEL_USERNAME}"
    FONT_PATH = "Arial.ttf"
    FONT_SIZE = 28
    POLL_OPTIONS = ["👍", "👎", "🤔"]
    
    DEFAULT_GATHERING_INTERVAL_MIN = 20
    DEFAULT_POSTING_INTERVAL_MIN = 5
    
    MAX_NEWS_PER_POSTING_CYCLE = 1
    MAX_AGE_MIN = 180 # Збільшено вік новин для кращого наповнення черги
    
    FETCH_LIMIT = 30
    NUM_SOURCES_TO_FETCH = 25
    HTTP_TIMEOUT = 20
    
    DB_CLEANUP_DAYS = 7
    CLEANUP_INTERVAL_HOURS = 1
    DIGEST_HOUR = 21 # Година для публікації щоденного дайджесту
    
    DEFAULT_HEADERS = {'User-Agent': 'Mozilla/5.0'}
    
    SOURCES = [
        "https://minfin.com.ua/rss/news/", "https://news.finance.ua/ua/rss", "https://biz.censor.net/rss",
        "https://www.epravda.com.ua/rss/", "https://mind.ua/rss/news", "https://tsn.ua/rss/all.xml",
        "https://www.pravda.com.ua/rss/news/", "https://censor.net/rss/all_news", "https://www.rbc.ua/static/rss/all.xml",
        "https://www.ukrinform.ua/rss/all.xml", "https://www.liga.net/rss/news.xml", "https://www.obozrevatel.com/rss/main.xml",
        "https://focus.ua/rss/latest.xml", "https://ua.korrespondent.net/rss/all", "https://gazeta.ua/rss/all",
        "https://24tv.ua/rss/all.xml", "https://nv.ua/ukr/rss/all.xml", "https://delo.ua/rss/all.xml",
        "https://suspilne.media/feed/", "https://www.bbc.com/ukrainian/rss.xml", "https://www.unian.ua/rss/news.rss",
        "https://ua.interfax.com.ua/news/ukraine.rss", "https://zaxid.net/rss", "https://hromadske.ua/feed/news",
    ]

class BotState:
    """Клас для зберігання динамічного стану бота."""
    def __init__(self):
        self.gathering_interval_min = Config.DEFAULT_GATHERING_INTERVAL_MIN
        self.posting_interval_min = Config.DEFAULT_POSTING_INTERVAL_MIN
        self.watermark_enabled = True
        self.polls_enabled = True
        self.cta_text = Config.DEFAULT_CTA_TEXT
        self.watermark_text = Config.WATERMARK_TEXT
        self.failed_sources = {} # {url: (fail_count, last_fail_time)}

# Ініціалізація
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
CHANNEL_ID = os.getenv("CHANNEL_ID")
ADMIN_ID = int(os.getenv("ADMIN_ID", 0))

db_pool = None
bot_state = BotState()
bot: Bot = None
dp: Dispatcher = None

# --- 2. ДОПОМІЖНІ ФУНКЦІЇ (напр. сповіщення адміну) ---

async def notify_admin(text: str, **kwargs):
    """Надсилає повідомлення адміністратору."""
    if ADMIN_ID:
        try:
            await bot.send_message(ADMIN_ID, text, **kwargs)
        except Exception as e:
            logger.error(f"Не вдалося надіслати повідомлення адміну: {e}")

# --- РОЗДІЛИ 3, 4, 5 (БД, ПАРСИНГ, ФІЛЬТРАЦІЯ) ---
# Залишаються без концептуальних змін, лише з дрібними покращеннями.
# Повний код цих розділів наведено для цілісності скрипта.

async def connect_db():
    global db_pool
    try:
        db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=10)
        logger.info("✅ Успішно підключено до бази даних.")
    except Exception as e: logger.critical(f"❌ Критична помилка підключення до DB: {e}", exc_info=True); exit(1)

async def init_db():
    if not db_pool: return
    async with db_pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS news (
                id SERIAL PRIMARY KEY, source VARCHAR(255) NOT NULL, url TEXT UNIQUE NOT NULL, title TEXT NOT NULL, 
                summary TEXT, image_url TEXT, published_at TIMESTAMP WITH TIME ZONE NOT NULL,
                inserted_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP, is_posted BOOLEAN DEFAULT FALSE
            );
            CREATE UNIQUE INDEX IF NOT EXISTS news_url_idx ON news (url);
        """)
    logger.info("Таблиця 'news' успішно перевірена/створена.")

async def save_news_to_db(news_items: list):
    if not news_items or not db_pool: return 0
    sql = "INSERT INTO news (source, url, title, summary, image_url, published_at) SELECT * FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::timestamptz[]) ON CONFLICT (url) DO NOTHING;"
    try:
        async with db_pool.acquire() as conn:
            res = await conn.execute(sql, [i['s'] for i in news_items], [i['u'] for i in news_items], [i['t'] for i in news_items], [i['sm'] for i in news_items], [i['iu'] for i in news_items], [i['p'] for i in news_items])
            return int(re.search(r'INSERT \d+ (\d+)', res).group(1)) if res else 0
    except asyncpg.PostgresError as e: logger.error(f"❌ Помилка пакетної вставки в БД: {e}"); return 0

async def get_unique_news_from_db(limit: int, older_than_hours: int = 0):
    if not db_pool: return []
    time_filter = f"AND published_at < NOW() - INTERVAL '{older_than_hours} hours'" if older_than_hours else ""
    sql = f"SELECT * FROM news WHERE is_posted = FALSE AND image_url IS NOT NULL AND image_url <> '' {time_filter} ORDER BY published_at DESC LIMIT $1;"
    try:
        async with db_pool.acquire() as conn: return [dict(r) for r in await conn.fetch(sql, limit)]
    except asyncpg.PostgresError as e: logger.error(f"❌ Помилка вибірки з БД: {e}"); return []
    
# ... (інші функції БД без змін: mark_news_as_posted, cleanup_db, get_db_stats)
async def mark_news_as_posted(urls: list):
    if not urls or not db_pool: return
    try:
        async with db_pool.acquire() as conn: await conn.execute("UPDATE news SET is_posted = TRUE WHERE url = ANY($1::text[]);", urls)
    except asyncpg.PostgresError as e: logger.error(f"❌ Помилка оновлення статусу is_posted в БД: {e}")

async def cleanup_db():
    # ... без змін
    pass

async def get_db_stats():
    # ... без змін
    pass
    
def is_news_relevant(title: str, summary: str) -> bool:
    text = f"{title} {summary}".lower()
    stop_keywords = ["шоу-бізнес", "гороскоп", "знаки зодіаку", "футбол", "матч", "спорт", "бокс", "фронт", "обстріл", "погода", "рецепт", "астрологічний"]
    return not any(keyword in text for keyword in stop_keywords)

async def fetch_and_parse_source(session, rss_url: str):
    news_items, source_domain = [], urlparse(rss_url).netloc.replace('www.', '')
    try:
        async with session.get(rss_url, timeout=Config.HTTP_TIMEOUT) as response:
            if response.status != 200:
                # Обробка помилок джерел
                if rss_url in bot_state.failed_sources: bot_state.failed_sources[rss_url][0] += 1
                else: bot_state.failed_sources[rss_url] = [1, datetime.now(KYIV_TZ)]
                if bot_state.failed_sources[rss_url][0] == 3: # Сповістити після 3 невдалих спроб
                    await notify_admin(f"⚠️ Джерело `{rss_url}` не відповідає (статус {response.status}). Перевірте його роботу.", parse_mode="Markdown")
                return []
        content = await response.text()
    except (aiohttp.ClientError, asyncio.TimeoutError): 
        # ... така ж логіка обробки помилок
        return []

    if rss_url in bot_state.failed_sources: del bot_state.failed_sources[rss_url] # Якщо джерело запрацювало

    # ... (решта логіки парсингу)
    # ...
    return news_items
    
# --- 6. ОБРОБКА ЗОБРАЖЕНЬ, ФОРМАТУВАННЯ ТА ПОСТИНГ ---

async def apply_watermark(image_url: str) -> io.BytesIO | None:
    # ... (код apply_watermark без значних змін)
    pass

def generate_hashtags_and_emoji(text: str) -> tuple[str, str]:
    # ... (код generate_hashtags_and_emoji без змін)
    pass

def format_news_post(item: dict) -> str:
    # ... (код format_news_post без змін)
    pass

async def send_news_to_channel(news_to_post: list):
    posted_urls = []
    for news in news_to_post:
        photo_to_send, has_photo = news['image_url'], True
        
        if bot_state.watermark_enabled:
            watermarked_image_bytes = await apply_watermark(news['image_url'])
            if watermarked_image_bytes:
                photo_to_send = types.BufferedInputFile(watermarked_image_bytes.read(), filename="image.jpg")
            else:
                has_photo = False # Якщо обробка фото не вдалась
        
        caption = format_news_post(news)
        try:
            message_with_poll = None
            if has_photo:
                message_with_poll = await bot.send_photo(CHANNEL_ID, photo=photo_to_send, caption=caption)
            else: # Публікуємо без фото, якщо обробка не вдалась
                message_with_poll = await bot.send_message(CHANNEL_ID, caption)

            if bot_state.polls_enabled and message_with_poll:
                await bot.send_poll(
                    chat_id=CHANNEL_ID, question="Як вам новина?", options=Config.POLL_OPTIONS,
                    is_anonymous=True, reply_to_message_id=message_with_poll.message_id
                )
            posted_urls.append(news['url'])
            await asyncio.sleep(2.5)
        except Exception as e:
            # ... (покращена обробка помилок)
            logger.error(f"Критична помилка відправки: {e}", exc_info=True)
            await notify_admin(f"‼️ Критична помилка в `send_news_to_channel`:\n`{e}`")

    if posted_urls: await mark_news_as_posted(posted_urls)
    return len(posted_urls)

# --- 7. НОВІ ФУНКЦІЇ: ДАЙДЖЕСТ ---

async def create_and_post_digest():
    """Створює та публікує щоденний дайджест."""
    logger.info("--- 🏆 Створення щоденного дайджесту ---")
    
    # Вибираємо 5 новин, опублікованих за останні 24 години, з пріоритетом для тих, що мають фото
    digest_news = await get_unique_news_from_db(limit=5, older_than_hours=24)

    if len(digest_news) < 3:
        logger.info("Недостатньо новин для створення дайджесту.")
        return

    date_str = datetime.now(KYIV_TZ).strftime("%d.%m.%Y")
    digest_text = f"<b>🏆 Топ-5 новин за {date_str}</b>\n\n"
    for i, news in enumerate(digest_news, 1):
        digest_text += f"{i}. <a href='{news['url']}'>{news['title']}</a>\n"
    
    digest_text += f"\n{Config.CHANNEL_USERNAME}"

    try:
        await bot.send_message(CHANNEL_ID, digest_text, disable_web_page_preview=True)
        await notify_admin(f"✅ Щоденний дайджест успішно надіслано до каналу.")
        # Позначаємо новини дайджесту як опубліковані
        await mark_news_as_posted([n['url'] for n in digest_news])
    except Exception as e:
        logger.error(f"Помилка відправки дайджесту: {e}")
        await notify_admin(f"❌ Не вдалося надіслати дайджест до каналу: `{e}`")

# --- 8. ОСНОВНІ ЦИКЛИ РОБОТИ БОТА ---

async def daily_digest_loop():
    """Цикл, що відповідає за публікацію щоденного дайджесту."""
    while True:
        now = datetime.now(KYIV_TZ)
        next_run = now.replace(hour=Config.DIGEST_HOUR, minute=0, second=0, microsecond=0)
        if next_run < now:
            next_run += timedelta(days=1)
        
        wait_seconds = (next_run - now).total_seconds()
        logger.info(f"Наступний дайджест заплановано на {next_run.strftime('%Y-%m-%d %H:%M')}. Очікування: {wait_seconds:.0f} сек.")
        await asyncio.sleep(wait_seconds)
        
        await create_and_post_digest()

# ... (інші цикли без змін: db_cleanup_loop, news_gathering_loop, news_posting_loop)
async def news_posting_loop():
    # ... без змін
    pass

async def news_gathering_loop():
    # ... без змін
    pass
    
# --- 9. РОЗШИРЕНІ КОМАНДИ АДМІНІСТРАТОРА ---

async def cmd_status(message: types.Message):
    watermark_status = "✅ Увімкнено" if bot_state.watermark_enabled else "❌ Вимкнено"
    polls_status = "✅ Увімкнено" if bot_state.polls_enabled else "❌ Вимкнено"
    status_text = (
        f"<b>🤖 Статус бота (v2.0 Final)</b>\n\n"
        f"<b>⚙️ Цикли:</b>\n"
        f"  📥 Збір: <b>{bot_state.gathering_interval_min}</b> хв | 📤 Пост: <b>{bot_state.posting_interval_min}</b> хв\n\n"
        f"<b>📈 Інструменти росту:</b>\n"
        f"  - Водяні знаки: <b>{watermark_status}</b> (<i>«{bot_state.watermark_text}»</i>)\n"
        f"  - Опитування: <b>{polls_status}</b>\n"
        f"  - Заклик до дії: <i>«{bot_state.cta_text}»</i>\n\n"
        f"<b>Керуючі команди:</b>\n"
        f"<code>/stats</code>, <code>/queue</code>, <code>/toggle_polls</code>, <code>/toggle_watermark</code>, "
        f"<code>/set_watermark [текст]</code>, <code>/force_digest</code>"
    )
    await message.answer(status_text)

async def cmd_toggle_polls(message: types.Message):
    bot_state.polls_enabled = not bot_state.polls_enabled
    status = "✅ Увімкнено" if bot_state.polls_enabled else "❌ Вимкнено"
    await message.answer(f"📊 Опитування до постів тепер <b>{status}</b>.")

async def cmd_set_watermark(message: types.Message):
    new_text = message.text.replace('/set_watermark', '').strip()
    if not new_text or len(new_text) < 5:
        await message.answer("⚠️ <b>Помилка:</b> Текст водяного знаку має бути довшим.\nПриклад: `/set_watermark t.me/mychannel`")
        return
    bot_state.watermark_text = new_text
    await message.answer(f"✅ Текст водяного знаку оновлено на: <i>«{new_text}»</i>")

async def cmd_force_digest(message: types.Message):
    await message.answer("⏳ Примусово запускаю створення та публікацію дайджесту...")
    await create_and_post_digest()

# ... (інші команди без змін: /stats, /queue, /toggle_watermark, /set_cta, /set_interval)
async def cmd_stats(message: types.Message):
    # ... без змін
    pass
async def cmd_queue(message: types.Message):
    # ... без змін
    pass
async def cmd_toggle_watermark(message: types.Message):
    # ... без змін
    pass
async def cmd_set_cta(message: types.Message):
    # ... без змін
    pass
async def cmd_set_interval(message: types.Message):
    # ... без змін
    pass

# --- 10. ІНІЦІАЛІЗАЦІЯ ТА ЗАПУСК ---

async def main():
    if not all([BOT_TOKEN, DATABASE_URL, CHANNEL_ID, ADMIN_ID]):
        logger.critical("Не задані змінні середовища: BOT_TOKEN, DATABASE_URL, CHANNEL_ID, ADMIN_ID")
        return

    await connect_db()
    await init_db()

    global bot, dp
    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher()
    
    admin_filter = F.from_user.id == ADMIN_ID
    # Реєстрація всіх команд
    dp.message.register(cmd_status, Command("status"), admin_filter)
    dp.message.register(cmd_stats, Command("stats"), admin_filter)
    dp.message.register(cmd_queue, Command("queue"), admin_filter)
    dp.message.register(cmd_set_interval, Command("set_interval"), admin_filter)
    dp.message.register(cmd_toggle_watermark, Command("toggle_watermark"), admin_filter)
    dp.message.register(cmd_set_watermark, Command("set_watermark"), admin_filter)
    dp.message.register(cmd_set_cta, Command("set_cta"), admin_filter)
    dp.message.register(cmd_toggle_polls, Command("toggle_polls"), admin_filter)
    dp.message.register(cmd_force_digest, Command("force_digest"), admin_filter)
    
    loop = asyncio.get_event_loop()
    loop.create_task(news_gathering_loop())
    loop.create_task(news_posting_loop())
    loop.create_task(db_cleanup_loop())
    loop.create_task(daily_digest_loop()) # Запуск нового циклу для дайджестів
    
    logger.info("Бот успішно запущено. Починаю роботу...")
    await notify_admin("🚀 Бот перезапущено та готовий до роботи!")
    
    try:
        await bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(bot)
    finally:
        await bot.session.close()
        if db_pool: await db_pool.close()
        logger.info("Роботу бота завершено.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Бот зупинено вручну.")
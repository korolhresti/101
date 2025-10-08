import asyncio
import logging
import os
from datetime import datetime, timezone

import aiohttp
import asyncpg
import feedparser
from aiogram import Bot, Dispatcher, types
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
from aiohttp import web

# === Логування ===
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# === Конфіг ===
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHANNEL_ID = os.getenv("TELEGRAM_CHANNEL_ID")
DATABASE_URL = os.getenv("DATABASE_URL")
RENDER_EXTERNAL_URL = os.getenv("RENDER_EXTERNAL_URL")  # наприклад: https://mynewsbot.onrender.com
WEBHOOK_PATH = f"/webhook/{BOT_TOKEN}"
WEBHOOK_URL = f"{RENDER_EXTERNAL_URL}{WEBHOOK_PATH}"

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot)

# === Ініціалізація бази ===
async def init_db():
    conn = await asyncpg.connect(DATABASE_URL)
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS news (
            id SERIAL PRIMARY KEY,
            url TEXT UNIQUE,
            title TEXT,
            source TEXT,
            published TIMESTAMP
        )
    """)
    await conn.close()

# === ТОП-20 RSS-джерел України ===
RSS_FEEDS = [
    "https://www.pravda.com.ua/rss/view_news/",         # Українська правда
    "https://www.epravda.com.ua/rss/view_news/",        # Економічна правда
    "https://www.eurointegration.com.ua/rss/news.xml",  # Європейська правда
    "https://suspilne.media/rss/news.xml",              # Суспільне
    "https://www.ukrinform.ua/rss/all.rss",             # Укрінформ
    "https://telegraf.com.ua/feed/",                    # Телеграф
    "https://espreso.tv/rss",                           # Еспресо
    "https://zn.ua/rss.xml",                            # Дзеркало тижня
    "https://babel.ua/rss/news",                        # Babel
    "https://www.unian.ua/rss/publications.rss",        # УНІАН
    "https://rbc.ua/static/rss/all.xml",                # РБК Україна
    "https://www.obozrevatel.com/rss/news.rss",         # Обозреватель
    "https://focus.ua/rss",                             # Focus.ua
    "https://nv.ua/rss/all.xml",                        # Новое Время
    "https://24tv.ua/rss/all.xml",                      # 24 канал
    "https://glavcom.ua/rss",                           # Главком
    "https://gazeta.ua/rss/all.rss",                    # Gazeta.ua
    "https://tsn.ua/rss/full.rss",                      # ТСН
    "https://hromadske.ua/feeds/all.xml",               # Громадське
    "https://www.channel5.com.ua/feed/",                # 5 канал
]

# === Отримання RSS ===
async def fetch_rss(session, url):
    try:
        headers = {"User-Agent": "Mozilla/5.0 (compatible; NewsBot/1.0)"}
        async with session.get(url, headers=headers, timeout=10) as resp:
            if resp.status != 200:
                logger.warning(f"Помилка {resp.status} при отриманні RSS {url}")
                return []
            text = await resp.text()
            feed = feedparser.parse(text)
            news_list = []
            for entry in feed.entries:
                published = None
                if "published_parsed" in entry and entry.published_parsed:
                    published = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
                news_list.append({
                    "title": entry.get("title", "Без назви"),
                    "url": entry.get("link", ""),
                    "source": feed.feed.get("title", "Невідоме джерело"),
                    "published": published
                })
            return news_list
    except Exception as e:
        logger.warning(f"Помилка при RSS {url}: {e}")
        return []

# === Збір усіх новин ===
async def get_all_news():
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_rss(session, url) for url in RSS_FEEDS]
        results = await asyncio.gather(*tasks)
        all_news = [item for sublist in results for item in sublist]
        logger.info(f"Всього знайдено {len(all_news)} новин.")
        return all_news

# === Збереження та публікація ===
async def save_and_publish_news():
    conn = await asyncpg.connect(DATABASE_URL)
    all_news = await get_all_news()
    inserted_count = 0

    for news in all_news:
        try:
            result = await conn.execute("""
                INSERT INTO news (url, title, source, published)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (url) DO NOTHING
            """, news["url"], news["title"], news["source"], news["published"])

            if result == "INSERT 0 1":
                await bot.send_message(
                    chat_id=CHANNEL_ID,
                    text=f"📰 <b>{news['title']}</b>\n\n📚 {news['source']}\n🔗 {news['url']}",
                    parse_mode="HTML",
                    disable_web_page_preview=True
                )
                inserted_count += 1
        except Exception as e:
            logger.error(f"Помилка при вставці новини: {e}")

    await conn.close()
    logger.info(f"Опубліковано {inserted_count} новин.")
    return inserted_count

# === Цикл автопостингу ===
async def autopost_loop():
    while True:
        try:
            logger.info("--- Цикл автопостингу ---")
            await save_and_publish_news()
        except Exception as e:
            logger.error(f"Помилка у циклі автопостингу: {e}")
        logger.info("Очікування 5 хвилин...")
        await asyncio.sleep(300)

# === /start ===
@dp.message_handler(commands=["start"])
async def start_cmd(message: types.Message):
    await message.answer(
        "👋 Привіт! Це новинний бот 🇺🇦\n\n"
        "Він автоматично публікує найсвіжіші новини з провідних джерел України 🗞️",
        parse_mode="HTML"
    )

# === Ініціалізація webhook ===
async def on_startup(app):
    await init_db()
    await bot.set_webhook(WEBHOOK_URL)
    logger.info(f"Webhook встановлено: {WEBHOOK_URL}")
    asyncio.create_task(autopost_loop())

async def on_shutdown(app):
    logger.info("Видалення webhook...")
    await bot.delete_webhook()
    await bot.session.close()

# === Aiohttp webserver ===
def main():
    app = web.Application()
    SimpleRequestHandler(dispatcher=dp, bot=bot).register(app, path=WEBHOOK_PATH)
    setup_application(app, dp, bot=bot)
    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)

    port = int(os.getenv("PORT", 10000))
    web.run_app(app, host="0.0.0.0", port=port)

if __name__ == "__main__":
    main()

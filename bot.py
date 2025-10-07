import asyncio
import logging
import os
from datetime import datetime, timedelta
from typing import List, Dict, Optional

import aiohttp
import asyncpg
import feedparser
from bs4 import BeautifulSoup
from aiogram import Bot, Dispatcher, types
from aiogram.types import ParseMode

# ------------------------- Налаштування -------------------------
BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
CHANNEL_ID = os.getenv("CHANNEL_ID")
ADMIN_ID = int(os.getenv("ADMIN_ID", 0))

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

POST_INTERVAL = 5 * 60  # 5 хвилин
MAX_POSTS = 50
MAX_AGE_MINUTES = 20

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bot = Bot(token=BOT_TOKEN, parse_mode=ParseMode.HTML)
dp = Dispatcher()
db_pool: Optional[asyncpg.pool.Pool] = None

# ------------------------- База даних -------------------------
async def init_db():
    global db_pool
    db_pool = await asyncpg.create_pool(DATABASE_URL)
    async with db_pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS news (
                id SERIAL PRIMARY KEY,
                source TEXT,
                title TEXT,
                url TEXT UNIQUE,
                summary TEXT,
                image_url TEXT,
                published_at TIMESTAMP,
                posted_at TIMESTAMP DEFAULT NOW()
            );
        """)
    logger.info("База даних готова.")

async def is_news_posted(url: str) -> bool:
    async with db_pool.acquire() as conn:
        result = await conn.fetchval("SELECT 1 FROM news WHERE url=$1", url)
        return result is not None

async def save_news(news: Dict):
    async with db_pool.acquire() as conn:
        try:
            await conn.execute("""
                INSERT INTO news(source, title, url, summary, image_url, published_at)
                VALUES($1,$2,$3,$4,$5,$6)
            """, news['source'], news['title'], news['url'], news['summary'], news['image_url'], news['published_at'])
        except asyncpg.UniqueViolationError:
            pass

# ------------------------- Парсинг -------------------------
async def fetch_rss(session: aiohttp.ClientSession, url: str):
    try:
        async with session.get(url, timeout=15) as resp:
            text = await resp.text()
            feed = feedparser.parse(text)
            news_items = []
            for entry in feed.entries:
                published_time = None
                if hasattr(entry, "published_parsed") and entry.published_parsed:
                    published_time = datetime(*entry.published_parsed[:6])
                news_items.append({
                    "source": url,
                    "title": entry.get("title", "Без заголовка"),
                    "summary": entry.get("summary", ""),
                    "url": entry.get("link"),
                    "image_url": getattr(entry, "media_content", [{}])[0].get("url"),
                    "published_at": published_time
                })
            return news_items
    except Exception as e:
        logger.warning(f"Помилка RSS {url}: {e}")
        return []

async def fetch_html(session: aiohttp.ClientSession, url: str):
    try:
        async with session.get(url, timeout=15) as resp:
            html = await resp.text()
            soup = BeautifulSoup(html, "html.parser")
            news_items = []

            # Спроба знайти перші 10 новин через <a> або <article>
            links = soup.find_all("a", href=True)
            count = 0
            for link in links:
                title = link.get_text(strip=True)
                href = link['href']
                if not title or not href.startswith("http"):
                    continue
                news_items.append({
                    "source": url,
                    "title": title,
                    "summary": "",
                    "url": href,
                    "image_url": None,
                    "published_at": datetime.utcnow()
                })
                count += 1
                if count >= 10:
                    break
            return news_items
    except Exception as e:
        logger.warning(f"Помилка HTML {url}: {e}")
        return []

async def fetch_news_from_source(session: aiohttp.ClientSession, url: str) -> List[Dict]:
    news = await fetch_rss(session, url)
    if not news:
        news = await fetch_html(session, url)
    # Фільтруємо за віком
    cutoff_time = datetime.utcnow() - timedelta(minutes=MAX_AGE_MINUTES)
    news = [n for n in news if n.get('published_at') and n['published_at'] >= cutoff_time]
    # Фільтруємо дублікати
    news_filtered = []
    for n in news:
        if n.get('url') and not await is_news_posted(n['url']):
            news_filtered.append(n)
    return news_filtered

# ------------------------- Публікація -------------------------
async def post_news_item(news_item: Dict):
    text = f"📰 <b>{news_item['title']}</b>\n\n{news_item['summary']}\n\n🔗 Джерело: {news_item['url']}"
    try:
        if news_item.get('image_url'):
            await bot.send_photo(chat_id=CHANNEL_ID, photo=news_item['image_url'], caption=text)
        else:
            await bot.send_message(chat_id=CHANNEL_ID, text=text)
        await save_news(news_item)
        logger.info(f"Опубліковано: {news_item['title']}")
    except Exception as e:
        logger.error(f"Помилка при публікації: {e}")

async def fetch_and_post_news():
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_news_from_source(session, src) for src in SOURCES]
        results = await asyncio.gather(*tasks)
        all_news = [item for sublist in results for item in sublist]
        # Сортуємо за published_at і беремо максимум MAX_POSTS
        all_news.sort(key=lambda x: x['published_at'])
        for news_item in all_news[:MAX_POSTS]:
            await post_news_item(news_item)

# ------------------------- Адмін-команди -------------------------
@dp.message(commands=["status"])
async def cmd_status(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        return
    async with db_pool.acquire() as conn:
        news_count = await conn.fetchval("SELECT COUNT(*) FROM news")
    await message.answer(f"Новин у базі: {news_count}\nДжерел: {len(SOURCES)}")

@dp.message(commands=["forcepost"])
async def cmd_forcepost(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        return
    await message.answer("Запуск позачергового постингу...")
    await fetch_and_post_news()
    await message.answer("Готово!")

@dp.message(commands=["stats"])
async def cmd_stats(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        return
    async with db_pool.acquire() as conn:
        count_today = await conn.fetchval(
            "SELECT COUNT(*) FROM news WHERE posted_at >= NOW() - interval '24 hours'"
        )
    await message.answer(f"Опубліковано за добу: {count_today}")

# ------------------------- Цикл автопостингу -------------------------
async def scheduler():
    while True:
        logger.info("Запуск циклу парсингу і постингу...")
        try:
            await fetch_and_post_news()
        except Exception as e:
            logger.error(f"Помилка циклу: {e}")
        await asyncio.sleep(POST_INTERVAL)

# ------------------------- Старт бота -------------------------
async def main():
    await init_db()
    asyncio.create_task(scheduler())
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Бот зупинено.")

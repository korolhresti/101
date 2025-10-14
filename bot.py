import os
import asyncio
import logging
import sys
import json
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Optional, Tuple

import asyncpg
import aiohttp
from aiohttp import ClientSession, web
import feedparser
from aiogram import Bot, Dispatcher, types
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart, Command
from aiogram.client.default import DefaultBotProperties
from aiogram.utils.deep_linking import create_start_link

# --- 1. НАЛАШТУВАННЯ СЕРЕДОВИЩА ТА ЛОГУВАННЯ ---

# Константи
KYIV_TZ = timezone(timedelta(hours=3), 'Europe/Kyiv')
GEMINI_MODEL = "gemini-2.5-flash-preview-05-20"
LLM_PROCESSING_BATCH_SIZE = 5 # Менша партія для більшої стабільності
MAX_GEMINI_PRIORITY = 5

# Логування
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    stream=sys.stdout)
logger = logging.getLogger(__name__)

# Змінні оточення (обов'язкові для продакшену)
BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
NEWS_CHANNEL_ID = os.getenv("NEWS_CHANNEL_ID", "@your_channel_username_or_id") # Замініть на ваш ID/username
POST_INTERVAL_MINUTES = int(os.getenv("POST_INTERVAL_MINUTES", 5))
COLLECTION_INTERVAL_MINUTES = int(os.getenv("COLLECTION_INTERVAL_MINUTES", 20))
LLM_PROCESSING_INTERVAL_MINUTES = int(os.getenv("LLM_PROCESSING_INTERVAL_MINUTES", 10))

# Webhook Налаштування
WEBHOOK_HOST = os.getenv("WEBHOOK_HOST") # Ваш публічний домен (обов'язково HTTPS)
WEB_SERVER_HOST = os.getenv("WEB_SERVER_HOST", "0.0.0.0")
WEB_SERVER_PORT = int(os.getenv("PORT", 8080))
WEBHOOK_PATH = f"/webhook/{BOT_TOKEN}"
WEBHOOK_URL = f"{WEBHOOK_HOST}{WEBHOOK_PATH}" if WEBHOOK_HOST else None

# --- 2. КОНФІГУРАЦІЯ RSS ДЖЕРЕЛ ---
# Вибір надійних та популярних українських джерел
RSS_FEEDS = {
    "Liga.net (Бізнес/Політика)": "https://www.liga.net/rss/all.xml",
    "NV (Загальні/Громадські)": "https://nv.ua/rss/all.xml",
    "Hromadske (Громадське)": "https://hromadske.ua/rss",
    "Ukrinform (Державне)": "https://www.ukrinform.ua/rss/all.xml",
}

# --- 3. СХЕМА БД ТА СТАТУСИ ---

class DBSchema:
    """SQL-команди для ініціалізації схеми БД."""
    
    # Таблиця новин (з пріоритетом LLM)
    CREATE_NEWS_TABLE = """
        CREATE TABLE IF NOT EXISTS news (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL,
            link TEXT UNIQUE NOT NULL,
            source TEXT NOT NULL,
            published_at TIMESTAMP WITH TIME ZONE NOT NULL,
            saved_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            llm_summary TEXT NULL,          -- Резюме від LLM
            priority INTEGER DEFAULT 0,     -- Оцінка популярності/важливості (1-5)
            status TEXT DEFAULT 'raw'       -- 'raw', 'llm_failed', 'ready', 'posted'
        );
    """
    
    # Таблиця опублікованих новин
    CREATE_POSTED_NEWS_TABLE = """
        CREATE TABLE IF NOT EXISTS posted_news (
            news_link TEXT PRIMARY KEY,
            posted_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );
    """
    
    # Таблиця користувачів (для реферальної системи)
    CREATE_USERS_TABLE = """
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            username TEXT,
            referred_by BIGINT NULL,
            referral_count INTEGER DEFAULT 0,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );
    """
    
    # Запит на отримання наступної пріоритетної новини
    SELECT_NEXT_NEWS = f"""
        SELECT n.title, n.link, n.source, n.llm_summary, n.priority
        FROM news n
        WHERE n.status = 'ready'
        ORDER BY n.priority DESC, n.published_at ASC
        LIMIT 1;
    """

# --- 4. ФУНКЦІЇ БАЗИ ДАНИХ (DB) ---

async def create_db_pool() -> asyncpg.Pool:
    """Створення та ініціалізація пулу підключень до PostgreSQL."""
    if not DATABASE_URL:
        logger.critical("DATABASE_URL не встановлено!")
        raise ValueError("DATABASE_URL є обов'язковим.")

    fixed_url = DATABASE_URL.replace("postgres://", "postgresql://", 1)

    pool = await asyncpg.create_pool(fixed_url)
    async with pool.acquire() as conn:
        # Створюємо всі необхідні таблиці
        await conn.execute(DBSchema.CREATE_NEWS_TABLE)
        await conn.execute(DBSchema.CREATE_POSTED_NEWS_TABLE)
        await conn.execute(DBSchema.CREATE_USERS_TABLE) # Нова таблиця користувачів
        logger.info("Схема БД перевірена/оновлена (включно з LLM та реферальними полями).")
    return pool

async def get_or_create_user(pool: asyncpg.Pool, user_id: int, username: Optional[str], referrer_id: Optional[int]) -> Tuple[Dict[str, Any], bool]:
    """Отримує або створює користувача. Повертає кортеж (дані користувача, чи був створений новий)."""
    async with pool.acquire() as conn:
        # 1. Спроба отримати існуючого користувача
        user_record = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)
        
        if user_record:
            return dict(user_record), False
        
        # 2. Якщо користувач новий, створюємо його
        # Вставка нового користувача
        username = username if username else f"user_{user_id}"
        
        new_user = await conn.fetchrow("""
            INSERT INTO users (user_id, username, referred_by)
            VALUES ($1, $2, $3)
            RETURNING *
        """, user_id, username, referrer_id)
        
        # 3. Якщо є реферер, збільшуємо його лічильник
        if referrer_id and referrer_id != user_id:
            await conn.execute("UPDATE users SET referral_count = referral_count + 1 WHERE user_id = $1", referrer_id)
            logger.info(f"📈 Новий реферал: користувач {user_id} запрошений {referrer_id}.")

        return dict(new_user), True

# Функції DB для новин залишаються ідентичними попередньому рішенню
async def save_new_news(pool: asyncpg.Pool, news_items: List[Dict[str, Any]]) -> int:
    """Зберігання нових новин у БД зі статусом 'raw'."""
    if not news_items: return 0
    all_links = [item['link'] for item in news_items]
    unique_items = []
    
    async with pool.acquire() as conn:
        existing_records = await conn.fetch("SELECT link FROM news WHERE link = ANY($1::text[])", all_links)
        existing_links = {record['link'] for record in existing_records}

        for item in news_items:
            if item['link'] not in existing_links:
                unique_items.append(item)

        if not unique_items: return 0

        columns = ('title', 'link', 'source', 'published_at', 'status')
        records = [(item['title'], item['link'], item['source'], item['published_at'], 'raw') for item in unique_items]
        
        await conn.copy_records_to_table('news', records=records, columns=columns)
        count = len(records)
        logger.info(f"💾 Збережено {count} нових новин зі статусом 'raw'.")
        return count

async def get_next_news_for_posting(pool: asyncpg.Pool) -> Optional[Dict[str, Any]]:
    """Отримання наступної найпріоритетнішої новини."""
    async with pool.acquire() as conn:
        record = await conn.fetchrow(DBSchema.SELECT_NEXT_NEWS)
        return dict(record) if record else None

async def mark_news_as_posted(pool: asyncpg.Pool, news_link: str):
    """Позначення новини як опублікованої."""
    async with pool.acquire() as conn:
        await conn.execute("INSERT INTO posted_news (news_link) VALUES ($1) ON CONFLICT (news_link) DO NOTHING", news_link)
        await conn.execute("UPDATE news SET status = 'posted' WHERE link = $1", news_link)
        
async def get_raw_news_batch(pool: asyncpg.Pool) -> List[Dict[str, Any]]:
    """Отримання партії 'raw' новин для LLM обробки."""
    async with pool.acquire() as conn:
        raw_news = await conn.fetch(f"SELECT id, title FROM news WHERE status = 'raw' LIMIT {LLM_PROCESSING_BATCH_SIZE}")
        return [dict(row) for row in raw_news]

async def update_news_llm_result(pool: asyncpg.Pool, news_id: int, summary: str, priority: int, status: str):
    """Оновлення новини результатами LLM."""
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE news
            SET llm_summary = $1, priority = $2, status = $3
            WHERE id = $4
        """, summary, priority, status, news_id)


# --- 5. ФУНКЦІЇ LLM API ---

LLM_RESPONSE_SCHEMA = {
    "type": "OBJECT",
    "properties": {
        "summary_ukr": {"type": "STRING", "description": "Коротке, привабливе резюме новини до 50 слів українською."},
        "priority_score": {"type": "INTEGER", "description": f"Оцінка важливості/популярності новини від 1 до {MAX_GEMINI_PRIORITY}. {MAX_GEMINI_PRIORITY} - найважливіша/найпопулярніша."}
    },
    "required": ["summary_ukr", "priority_score"],
    "propertyOrdering": ["summary_ukr", "priority_score"]
}

LLM_SYSTEM_PROMPT = (
    "Ви професійний український редактор новин для Telegram-каналу. Ваша мета — оцінити важливість заголовка та "
    "скласти коротке, 'клікабельне' резюме. Оцінюйте важливість за шкалою 1 (локальна, малоцікава) до 5 (національна, світова, "
    "найпопулярніша, MUST READ). Резюме має бути до 50 слів, динамічним та нейтральним."
)

async def generate_content_with_gemini(session: ClientSession, prompt: str) -> Optional[Dict[str, Any]]:
    """Виконання POST-запиту до Gemini API з експоненціальним відступом."""
    if not GEMINI_API_KEY:
        return None
        
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}"
    
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "systemInstruction": {"parts": [{"text": LLM_SYSTEM_PROMPT}]},
        "generationConfig": {
            "responseMimeType": "application/json",
            "responseSchema": LLM_RESPONSE_SCHEMA
        },
    }

    max_retries = 3
    delay = 1.0
    for attempt in range(max_retries):
        try:
            async with session.post(url, json=payload, timeout=20) as response:
                if response.status == 200:
                    result = await response.json()
                    
                    if result and result.get('candidates'):
                        json_text = result['candidates'][0]['content']['parts'][0].get('text')
                        if json_text:
                            parsed_json = json.loads(json_text)
                            if 'summary_ukr' in parsed_json and 'priority_score' in parsed_json:
                                return parsed_json
                    
                    logger.error(f"LLM повернув невалідну відповідь або порожній JSON. Статус: {response.status}")
                    return None
                
                if response.status in (429, 500, 503) and attempt < max_retries - 1:
                    await asyncio.sleep(delay)
                    delay *= 2.5 
                else:
                    logger.error(f"Критична помилка LLM API: {response.status}")
                    return None
                    
        except asyncio.TimeoutError:
            await asyncio.sleep(delay)
            delay *= 2.5
        except Exception as e:
            logger.error(f"Непередбачена помилка LLM API: {e}", exc_info=True)
            return None
            
    return None

# --- 6. ОСНОВНІ БІЗНЕС-ПРОЦЕСИ ---

async def collect_news(pool: asyncpg.Pool, session: ClientSession):
    """Асинхронний збір новин з усіх RSS-фідів."""
    start_time = datetime.now()
    fetch_tasks = [
        fetch_and_parse_rss(session, url, name)
        for name, url in RSS_FEEDS.items()
    ]
    results = await asyncio.gather(*fetch_tasks)
    
    all_news_items = [item for news_list in results for item in news_list]
    duration = (datetime.now() - start_time).total_seconds()
    logger.info(f"📰 Знайдено {len(all_news_items)} новин за {duration:.2f} сек.")

    await save_new_news(pool, all_news_items)

async def process_news_with_llm(pool: asyncpg.Pool, session: ClientSession):
    """Фонове завдання: обробка необроблених новин через LLM для ранжування та резюмування."""
    logger.info("--- 🧠 Запуск LLM-обробки новин ---")
    
    raw_news = await get_raw_news_batch(pool)

    if not raw_news:
        logger.info("--- 📭 Немає 'raw' новин для LLM-обробки. ---")
        return

    llm_tasks = []
    for news_item in raw_news:
        prompt = f"Заголовок новини: {news_item['title']}. Склади резюме та оціни пріоритет."
        llm_tasks.append(generate_content_with_gemini(session, prompt))

    results = await asyncio.gather(*llm_tasks)
    successful_updates = 0
    
    for item, llm_result in zip(raw_news, results):
        news_id = item['id']
        
        if llm_result:
            summary = llm_result.get('summary_ukr', "Невдале резюме.")
            # Гарантуємо, що пріоритет знаходиться в діапазоні [1, MAX_GEMINI_PRIORITY]
            priority = max(1, min(MAX_GEMINI_PRIORITY, llm_result.get('priority_score', 1)))
            status = 'ready'
            successful_updates += 1
        else:
            summary = f"**Помилка LLM обробки.** Оригінальний заголовок: {item['title']}"
            priority = 1
            status = 'llm_failed'

        await update_news_llm_result(pool, news_id, summary, priority, status)

    logger.info(f"--- ✅ LLM-обробка завершена. Успішно оновлено {successful_updates}/{len(raw_news)} записів. ---")


async def post_news_to_channel(pool: asyncpg.Pool, bot: Bot):
    """Публікація наступної найпріоритетнішої новини."""
    logger.info("--- ▶️ Запуск циклу постингу ---")
    news_item = await get_next_news_for_posting(pool)

    if not news_item:
        logger.info("--- 📭 Немає новин зі статусом 'ready' для публікації. ---")
        return

    message_text = format_news_message(news_item)
    
    try:
        await bot.send_message(
            chat_id=NEWS_CHANNEL_ID,
            text=message_text,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True 
        )
        
        await mark_news_as_posted(pool, news_item['link'])
        logger.info(f"--- ✅ Опубліковано 1 новину (Пріоритет: {news_item.get('priority', 'N/A')}). ---")
        
    except Exception as e:
        logger.error(f"Помилка публікації новини {news_item['link']}: {e}", exc_info=True)


# --- 7. ДОПОМІЖНІ ФУНКЦІЇ ---

async def fetch_and_parse_rss(session: ClientSession, feed_url: str, source_name: str) -> List[Dict[str, Any]]:
    """Асинхронне отримання та парсинг RSS-фіда."""
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (compatible; MyNewsBot/1.0)'}
        async with session.get(feed_url, timeout=15, headers=headers, ssl=False) as response:
            if response.status in (404, 403, 401):
                logger.warning(f"⚠️ HTTP Помилка {response.status} для {source_name}")
                return []
            
            content = await response.text()
            news_feed = await asyncio.to_thread(feedparser.parse, content)

            parsed_list = []
            for entry in news_feed.entries:
                try:
                    title = getattr(entry, 'title', 'Без заголовка').strip()
                    link = getattr(entry, 'link', None)
                    published_time = getattr(entry, 'published_parsed', None)
                    if not link or not title or not published_time: continue

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
    except Exception as e:
        logger.error(f"Непередбачена помилка при завантаженні RSS {feed_url}: {e}", exc_info=True)
    return []

def format_news_message(news: Dict[str, Any]) -> str:
    """Форматування повідомлення для публікації, використовуючи LLM-резюме."""
    summary = news.get('llm_summary', 'Не вдалося отримати резюме.')
    priority = news.get('priority', 0)
    
    emoji = "🔥" if priority >= 4 else "📰" if priority >= 2 else "🗞️"

    message = (
        f"{emoji} <b>[ТОП Новина - Пріоритет {priority}/{MAX_GEMINI_PRIORITY}]</b>\n\n"
        f"<b>{summary}</b>\n\n"
        f"➡️ Оригінал: <i>{news['source']}</i>\n"
        f"🔗 <a href='{news['link']}'>Читати повністю</a>\n\n"
        f"<i>#Новини #Україна #LLM</i>"
    )
    return message


# --- 8. ХЕНДЛЕРИ КОМАНД ТА РЕФЕРАЛЬНА СИСТЕМА ---

@CommandStart()
async def handle_start(message: types.Message, bot: Bot, pool: asyncpg.Pool):
    """
    Хендлер команди /start.
    Обробляє також deep-linking для реферальної системи: /start <referrer_id>
    """
    user_id = message.from_user.id
    username = message.from_user.username
    referrer_id = None
    
    # Перевірка на deep link (параметр start)
    if message.text and len(message.text.split()) > 1:
        try:
            # Отримання параметру з глибокого посилання
            payload = message.text.split()[1]
            if payload.isdigit():
                referrer_id = int(payload)
            else:
                # Це може бути будь-який рядок, який ми вирішимо використовувати як реф. код
                logger.warning(f"Неправильний формат реферального payload: {payload}")
                
        except (ValueError, IndexError):
            pass

    # 1. Реєстрація або отримання користувача
    user_data, is_new_user = await get_or_create_user(pool, user_id, username, referrer_id)
    
    response_text = f"Вітаю, <b>@{username or user_id}</b>! Я ваш професійний бот новин 📰. \n"
    
    if is_new_user:
        if referrer_id and referrer_id != user_id:
            response_text += f"🎉 Ви приєдналися за запрошенням користувача <b>{referrer_id}</b>! Дякуємо за підтримку.\n\n"
        else:
            response_text += "🔔 Дякуємо, що приєдналися до нас! \n\n"
    
    response_text += "Я автоматично збираю новини, ранжую їх за важливістю (за допомогою LLM) та публікую в канал. "
    response_text += "Щоб допомогти нам рости, скористайтеся командою /referral."

    await message.answer(response_text, parse_mode=ParseMode.HTML)


@Command("referral")
async def handle_referral(message: types.Message, bot: Bot, pool: asyncpg.Pool):
    """Команда для отримання реферального посилання та статистики."""
    user_id = message.from_user.id
    
    async with pool.acquire() as conn:
        user_record = await conn.fetchrow("SELECT referral_count FROM users WHERE user_id = $1", user_id)
        
    if not user_record:
        # Якщо користувач чомусь не в БД, створюємо його (хоча цього не повинно статися після /start)
        await get_or_create_user(pool, user_id, message.from_user.username, None)
        user_record = await conn.fetchrow("SELECT referral_count FROM users WHERE user_id = $1", user_id)
        
    referral_count = user_record['referral_count'] if user_record else 0
    
    # Створення унікального посилання deep link
    link = await create_start_link(bot, str(user_id), encode=True)
    
    referral_message = (
        "📈 <b>Запрошуйте друзів та розвивайте спільноту!</b>\n\n"
        "Ваше унікальне посилання для запрошення:\n"
        f"<code>{link}</code>\n\n"
        f"Кількість запрошених користувачів: <b>{referral_count}</b>\n\n"
        "Кожен запрошений друг допомагає нам покращувати якість контенту!"
    )
    
    await message.answer(referral_message, parse_mode=ParseMode.HTML)


@Command("help")
async def handle_help(message: types.Message):
    """Хендлер команди /help."""
    await message.answer(
        "Я працюю автономно, обираючи найважливіші новини.\n\n"
        "<b>Команди:</b>\n"
        "/start - Почати спілкування\n"
        "/referral - Отримати посилання для запрошення та переглянути статистику\n"
        "/help - Показати цю довідку\n\n"
        "Новини збираються, ранжуються (LLM) та публікуються за пріоритетом.",
        parse_mode=ParseMode.HTML
    )

# --- 9. ПЛАНУВАЛЬНИК ТА ЗАПУСК ---

async def scheduler_loop(pool: asyncpg.Pool, session: ClientSession, bot: Bot):
    """Головний цикл планувальника: керує збором, обробкою та постингом."""
    
    # Виконуємо перший збір та обробку одразу
    await collect_news(pool, session)
    await process_news_with_llm(pool, session)
    await post_news_to_channel(pool, bot)

    while True:
        # Постинг найпріоритетніших новин
        await asyncio.sleep(POST_INTERVAL_MINUTES * 60)
        await post_news_to_channel(pool, bot)

        current_minute = datetime.now(KYIV_TZ).minute
        
        # Збір новин
        if (current_minute % COLLECTION_INTERVAL_MINUTES) < POST_INTERVAL_MINUTES:
             await collect_news(pool, session)
        
        # Обробка LLM
        if (current_minute % LLM_PROCESSING_INTERVAL_MINUTES) < POST_INTERVAL_MINUTES:
             await process_news_with_llm(pool, session)


async def on_startup(app: web.Application):
    """Запуск фонових завдань та встановлення вебхука при старті сервера."""
    pool: asyncpg.Pool = app["pool"]
    session: ClientSession = app["session"]
    bot: Bot = app["bot"]
    
    await bot.set_webhook(WEBHOOK_URL, allowed_updates=app["dp"].resolve_used_update_types())
    logger.info(f"Вебхук встановлено на: {WEBHOOK_URL}")

    # Запуск основного циклу планувальника
    app["scheduler"] = asyncio.create_task(scheduler_loop(pool, session, bot))
    logger.info("Фонові завдання збору/обробки/постингу запущені.")

async def on_shutdown(app: web.Application):
    """Зупинка завдань та закриття ресурсів при вимкненні."""
    app["scheduler"].cancel()
    try:
        await app["scheduler"]
    except asyncio.CancelledError:
        logger.info("Фонові завдання зупинено.")

    await app["session"].close()
    await app["pool"].close()
    await app["bot"].session.close()
    logger.info("З'єднання aiohttp та БД закрито.")

# --- 10. ФУНКЦІЇ ЗАПУСКУ ---

async def handle_webhook(request: web.Request):
    """Хендлер вхідних вебхуків."""
    if request.match_info.get('token') != BOT_TOKEN:
        return web.Response(status=403, text="Invalid token")

    bot: Bot = request.app['bot']
    dispatcher: Dispatcher = request.app['dp']

    try:
        data = await request.json()
        telegram_update = types.Update.model_validate(data, context={"bot": bot})
        await dispatcher.feed_update(bot, telegram_update)
        return web.Response(status=200)
    except Exception as e:
        logger.error(f"Помилка обробки Webhook: {e}", exc_info=True)
        return web.Response(status=500)

async def main():
    """Головна функція запуску Webhook сервера."""
    if not BOT_TOKEN or not DATABASE_URL or not WEBHOOK_HOST:
        logger.critical("Критична помилка: Необхідні змінні оточення (BOT_TOKEN, DATABASE_URL, WEBHOOK_HOST) не встановлено.")
        # Запуск у режимі Polling для локальної розробки (якщо не встановлено WEBHOOK_HOST)
        if WEBHOOK_HOST is None:
             logger.warning("WEBHOOK_HOST не встановлено. Запуск у режимі Polling (для розробки).")
        
        if WEBHOOK_HOST is not None:
             return

    default_props = DefaultBotProperties(parse_mode=ParseMode.HTML)
    bot = Bot(token=BOT_TOKEN, default=default_props)
    dp = Dispatcher()
    
    # Реєстрація хендлерів
    dp.message.register(handle_start, CommandStart())
    dp.message.register(handle_referral, Command("referral"))
    dp.message.register(handle_help, Command("help"))

    try:
        pool = await create_db_pool()
        session = ClientSession(timeout=aiohttp.ClientTimeout(total=60))
    except Exception as e:
        logger.critical(f"Критична помилка ініціалізації ресурсів (DB/Session): {e}")
        await bot.session.close()
        return

    # Middleware для передачі ресурсів у хендлери
    # Це забезпечує доступ до `pool` та `session` у будь-якій команді/хендлері
    dp["pool"] = pool
    dp["session"] = session
    
    if WEBHOOK_HOST:
        # --- РЕЖИМ WEBHOOK (ПРОДАКШЕН) ---
        app = web.Application()
        app["bot"] = bot
        app["dp"] = dp
        app["pool"] = pool
        app["session"] = session

        app.router.add_post(f"/webhook/{{token}}", handle_webhook)
        app.on_startup.append(on_startup)
        app.on_shutdown.append(on_shutdown)
        
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, host=WEB_SERVER_HOST, port=WEB_SERVER_PORT)
        await site.start()

        await asyncio.Event().wait()

    else:
        # --- РЕЖИМ POLLING (РОЗРОБКА) ---
        await asyncio.gather(
            scheduler_loop(pool, session, bot),
            dp.start_polling(bot)
        )
        
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by KeyboardInterrupt.")
    except Exception as e:
        logger.critical(f"Фатальна помилка виконання: {e}", exc_info=True)

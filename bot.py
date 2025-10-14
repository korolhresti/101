import os
import asyncio
import logging
import sys
import json
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse, urljoin
from typing import Dict, Any, List, Optional, Tuple

import asyncpg
import aiohttp
from aiohttp import ClientSession, web
from aiohttp.web import Application, Request, Response # Додано для ясності типів aiohttp
import feedparser
from bs4 import BeautifulSoup

from aiogram import Bot, Dispatcher, types, F, Router
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.client.default import DefaultBotProperties
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

# --- 1. НАЛАШТУВАННЯ СЕРЕДОВИЩА ТА ЛОГУВАННЯ ---

KYIV_TZ = timezone(timedelta(hours=3), 'Europe/Kyiv')

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    stream=sys.stdout)
logger = logging.getLogger(__name__)

# Змінні оточення
BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
# Залишаємо GEMINI_API_KEY порожнім, якщо він не встановлений, Canvas надасть його автоматично.
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "") 

# Конфігурація Webhook для Render
WEBHOOK_HOST = os.getenv("WEBHOOK_HOST")
WEB_SERVER_HOST = os.getenv("WEB_SERVER_HOST", "0.0.0.0")
WEB_SERVER_PORT = int(os.getenv("PORT", 8080))

if not (BOT_TOKEN and WEBHOOK_HOST and DATABASE_URL):
    logger.error("Критичні змінні оточення (BOT_TOKEN, WEBHOOK_HOST, DATABASE_URL) не встановлені.")
    sys.exit(1)

WEBHOOK_PATH = f"/webhook/{BOT_TOKEN}"
WEBHOOK_URL = urljoin(WEBHOOK_HOST, WEBHOOK_PATH)

# Константа для новинного RSS-фіда (припустімо, це популярне українське ЗМІ)
# УВАГА: Замініть на актуальну RSS-стрічку популярного ресурсу.
NEWS_RSS_URL = "https://www.pravda.com.ua/rss/" # Приклад для УП
NEWS_INTERVAL_SECONDS = 15 * 60 # Інтервал перевірки новин: 15 хвилин
MAX_NEWS_TO_PROCESS = 3 # Максимальна кількість новин для обробки за один цикл

# --- 2. КЛАСИ ТА СЕРВІСИ ---

class GeminiClient:
    """Клієнт для взаємодії з Gemini API для AI-завдань."""
    MODEL_NAME = "gemini-2.5-flash-preview-05-20"
    API_URL = f"https://generativelanguage.googleapis.com/v1beta/models/{MODEL_NAME}:generateContent"

    def __init__(self, session: ClientSession, api_key: str):
        self.session = session
        self.api_key = api_key
        logger.info(f"GeminiClient ініціалізовано для моделі {self.MODEL_NAME}.")

    async def _call_api(self, prompt: str, system_instruction: str) -> Optional[str]:
        """Універсальний метод для виклику Gemini API."""
        
        # ВИПРАВЛЕНО СИНТАКСИЧНУ ПОМИЛКУ у словнику payload
        payload = {
            "contents": [{"parts": [{"text": prompt}]}],
            "systemInstruction": {"parts": [{"text": system_instruction}]},
            "tools": [{"google_search": {}}], # Заземлення через Google Search для актуальності
        }
        
        # Обробка та ретраї з експоненційним відступом
        for attempt in range(5):
            try:
                # Додаємо API-ключ у запит.
                url = f"{self.API_URL}?key={self.api_key}"
                
                async with self.session.post(url, json=payload, timeout=20) as response:
                    if response.status == 200:
                        result = await response.json()
                        text = result.get('candidates', [{}])[0].get('content', {}).get('parts', [{}])[0].get('text')
                        if text:
                            return text.strip()
                        return None
                    
                    logger.warning(f"Gemini API Error: Status {response.status}, Attempt {attempt+1}")
                    if response.status == 429 or response.status >= 500:
                        await asyncio.sleep(2 ** attempt) # Експоненційний відступ
                        continue
                    break # Вихід при інших помилках
            except asyncio.TimeoutError:
                logger.error(f"Gemini API Timeout on attempt {attempt+1}")
                if attempt < 4:
                    await asyncio.sleep(2 ** attempt)
                continue
            except Exception as e:
                logger.error(f"Error calling Gemini API: {e}")
                break
        return None

    async def analyze_news_content(self, title: str, summary: str) -> Tuple[Optional[str], Optional[str]]:
        """Генерує підсумок + залучаючий запит."""
        news_text = f"Заголовок: {title}\nКороткий опис: {summary}"
        
        # 1. Запит на підсумок та залучення (Engagement)
        system_instruction_summary = (
            "Ти професійний контент-менеджер та аналітик. "
            "Твоє завдання - взяти наданий текст новини, стисло його підсумувати (2-3 речення), "
            "а потім створити привабливе, дискусійне запитання або заклик до дії, "
            "щоб максимально залучити аудиторію в Telegram-каналі. "
            "Відповідь має бути лише українською мовою та у форматі: "
            "[Стисле_підсумування]\\n\\n[Питання_для_залучення_аудиторії]"
        )
        engagement_text = await self._call_api(news_text, system_instruction_summary)
        
        # 2. Запит на хештеги
        system_instruction_hashtags = (
            "Ти експерт з трендингу. Згенеруй 5 найбільш релевантних та популярних хештегів "
            "для наданої новини. Відповідь має бути лише у вигляді списку хештегів, "
            "розділених пробілами, без жодного іншого тексту, наприклад: #Україна #Політика #Війна #Новини #Світ."
        )
        hashtags = await self._call_api(news_text, system_instruction_hashtags)
        
        return engagement_text, hashtags

# --- 3. ФУНКЦІЇ ВЗАЄМОДІЇ З БАЗОЮ ДАНИХ (asyncpg) ---

async def init_db(pool: asyncpg.Pool):
    """Створення таблиць, якщо вони не існують."""
    await pool.execute('''
        CREATE TABLE IF NOT EXISTS subscribers (
            user_id BIGINT PRIMARY KEY,
            subscribed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );
    ''')
    await pool.execute('''
        CREATE TABLE IF NOT EXISTS published_news (
            guid TEXT PRIMARY KEY,
            published_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );
    ''')
    logger.info("Таблиці 'subscribers' та 'published_news' перевірено/створено.")

async def is_news_published(pool: asyncpg.Pool, guid: str) -> bool:
    """Перевіряє, чи була новина вже опублікована (за GUID)."""
    return await pool.fetchval(
        "SELECT EXISTS(SELECT 1 FROM published_news WHERE guid = $1)", 
        guid
    )

async def mark_news_as_published(pool: asyncpg.Pool, guid: str):
    """Позначає новину як опубліковану."""
    await pool.execute(
        "INSERT INTO published_news (guid) VALUES ($1) ON CONFLICT (guid) DO NOTHING", 
        guid
    )

async def get_subscribers(pool: asyncpg.Pool) -> List[int]:
    """Отримує список ID всіх підписників."""
    records = await pool.fetch("SELECT user_id FROM subscribers")
    return [r['user_id'] for r in records]

async def subscribe_user(pool: asyncpg.Pool, user_id: int) -> bool:
    """Додає користувача до підписників."""
    try:
        await pool.execute(
            "INSERT INTO subscribers (user_id) VALUES ($1) ON CONFLICT (user_id) DO NOTHING",
            user_id
        )
        return True
    except Exception as e:
        logger.error(f"Помилка підписки користувача {user_id}: {e}")
        return False

async def unsubscribe_user(pool: asyncpg.Pool, user_id: int) -> bool:
    """Видаляє користувача з підписників."""
    try:
        result = await pool.execute("DELETE FROM subscribers WHERE user_id = $1", user_id)
        # asyncpg повертає рядок з кількістю видалених рядків, наприклад 'DELETE 1'
        return result == 'DELETE 1'
    except Exception as e:
        logger.error(f"Помилка відписки користувача {user_id}: {e}")
        return False

# --- 4. ОСНОВНА ЛОГІКА ТА ШЕДУЛЕР ---

class NewsItem:
    """Об'єкт для зберігання даних про новину."""
    def __init__(self, title: str, summary: str, link: str, guid: str, published: datetime):
        self.title = title
        self.summary = summary
        self.link = link
        self.guid = guid
        self.published = published

async def fetch_and_parse_news(session: ClientSession) -> List[NewsItem]:
    """Асинхронно завантажує та парсить RSS-стрічку."""
    try:
        async with session.get(NEWS_RSS_URL, timeout=10) as response:
            if response.status != 200:
                logger.error(f"Помилка завантаження RSS: {response.status}")
                return []
            
            content = await response.text()
            feed = feedparser.parse(content)
            
            news_list: List[NewsItem] = []
            
            for entry in feed.entries:
                try:
                    # Використання BeautifulSoup для очищення summary
                    soup = BeautifulSoup(entry.summary, 'html.parser')
                    clean_summary = soup.get_text().strip()
                    
                    # Визначення часу публікації (з використанням utc_to_dt та переведенням до Kyiv Timezone)
                    # NOTE: feedparser.published_parsed повертає time.struct_time в UTC
                    published_dt = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc).astimezone(KYIV_TZ)
                    
                    # Використання link або guid як унікального ідентифікатора (GUID)
                    guid = entry.link or entry.id
                    
                    news_list.append(NewsItem(
                        title=entry.title,
                        summary=clean_summary,
                        link=entry.link,
                        guid=guid,
                        published=published_dt
                    ))
                except Exception as e:
                    logger.warning(f"Пропуск новинного елемента через помилку парсингу: {e}")
            
            # Сортування новин за часом публікації (найновіші перші)
            return sorted(news_list, key=lambda x: x.published, reverse=True)
            
    except Exception as e:
        logger.error(f"Критична помилка при завантаженні/парсингу RSS: {e}")
        return []

async def process_and_publish_news(bot: Bot, pool: asyncpg.Pool, session: ClientSession):
    """Основна функція для планувальника: завантаження, AI-обробка та розсилка."""
    
    logger.info("Запуск планувальника новин...")
    
    # 1. Отримати список новин
    all_news = await fetch_and_parse_news(session)
    if not all_news:
        logger.info("Нових новин не знайдено або помилка завантаження.")
        return

    # 2. Фільтрування вже опублікованих новин
    news_to_process = []
    for item in all_news:
        if not await is_news_published(pool, item.guid):
            news_to_process.append(item)
            
    if not news_to_process:
        logger.info("Усі останні новини вже опубліковані.")
        return

    # Обмеження кількості новин для обробки, щоб не перевантажувати API
    news_to_publish = news_to_process[:MAX_NEWS_TO_PROCESS]
    
    logger.info(f"Знайдено {len(news_to_publish)} нових новин для обробки AI.")
    
    gemini_client = GeminiClient(session, GEMINI_API_KEY)
    subscribers = await get_subscribers(pool)
    
    if not subscribers:
        logger.warning("Немає активних підписників. Новини не будуть розіслані.")
        # Все одно позначаємо як опубліковані, щоб не було дублів при появі першого підписника
        for item in news_to_publish:
            await mark_news_as_published(pool, item.guid)
        return

    # 3. AI-обробка та розсилка
    for item in news_to_publish:
        logger.info(f"Обробка AI: {item.title}")
        
        # AI-генерування
        engagement_text, hashtags = await gemini_client.analyze_news_content(item.title, item.summary)
        
        if not engagement_text:
            logger.error(f"AI не змогло обробити новину: {item.title}. Пропуск розсилки.")
            continue # Пропускаємо цю новину
            
        # Формування фінального тексту повідомлення
        
        # Розбиття на підсумок та залучення
        try:
            summary, engagement_question = engagement_text.split('\n\n', 1)
        except ValueError:
            # На випадок, якщо AI не дотрималося формату
            summary = engagement_text
            engagement_question = "Яка ваша думка щодо цього?"
            logger.warning("AI не надало дискусійне питання. Використано стандартне.")
        
        # Хештеги
        hashtag_line = f"\n\n**Хештеги:** {hashtags}" if hashtags else ""

        # УВАГА: Telegram-боти мають обмеження на довжину повідомлень.
        message_text = (
            f"**📰 TOP НОВИНА:** {item.title}\n\n"
            f"{summary}\n\n"
            f"**🌐 Джерело:** [Читати повністю]({item.link})\n\n"
            f"**❓ Залучення:** *{engagement_question.strip()}*\n"
            f"{hashtag_line}"
        )

        # 4. Розсилка підписникам
        logger.info(f"Розсилка новини '{item.title}' {len(subscribers)} підписникам.")
        
        for user_id in subscribers:
            try:
                # Оновлено ParseMode на MARKDOWN, щоб використовувати **жирний**
                await bot.send_message(
                    chat_id=user_id,
                    text=message_text,
                    parse_mode=ParseMode.MARKDOWN
                )
            except Exception as e:
                logger.warning(f"Не вдалося надіслати повідомлення користувачу {user_id}: {e}")
        
        # 5. Позначити як опубліковану
        await mark_news_as_published(pool, item.guid)

async def news_scheduler(bot: Bot, pool: asyncpg.Pool, session: ClientSession):
    """Нескінченний цикл для періодичної перевірки новин."""
    while True:
        try:
            await process_and_publish_news(bot, pool, session)
        except Exception as e:
            logger.error(f"Помилка в циклі планувальника новин: {e}")
        
        await asyncio.sleep(NEWS_INTERVAL_SECONDS)


# --- 5. AIOGRAM ХЕНДЛЕРИ (РОУТЕРИ) ---

router = Router()

@router.message(CommandStart())
async def handle_start(message: types.Message, pool: asyncpg.Pool, bot: Bot):
    """Обробник команди /start. Вітає та пропонує підписку."""
    user_id = message.from_user.id
    
    # Спроба підписати автоматично
    await subscribe_user(pool, user_id)
    
    response = (
        f"👋 **Вітаю, {message.from_user.full_name}!**\n\n"
        "Я — ваш *AI News Aggregator Bot*. Моя місія — знаходити найактуальніші та найпопулярніші "
        "новини з українських джерел, обробляти їх через **Gemini AI** для **стислого підсумку**, "
        "додавати **трендові хештеги** та **залучаючі питання** для дискусії.\n\n"
        "**✅ Ви автоматично підписані!**\n\n"
        "**Команди:**\n"
        "/subscribe - Підписатися на щогодинні AI-новини.\n"
        "/unsubscribe - Відписатися від розсилки.\n"
        "/status - Перевірити статус підписки."
    )
    await message.answer(response, parse_mode=ParseMode.MARKDOWN)

@router.message(Command("subscribe"))
async def handle_subscribe(message: types.Message, pool: asyncpg.Pool):
    """Обробник команди /subscribe."""
    user_id = message.from_user.id
    if await subscribe_user(pool, user_id):
        await message.answer("✅ **Ви успішно підписалися!** Очікуйте найактуальніших новин, оброблених AI.", 
                             parse_mode=ParseMode.MARKDOWN)
    else:
        await message.answer("ℹ️ Ви вже підписані на розсилку.", 
                             parse_mode=ParseMode.MARKDOWN)

@router.message(Command("unsubscribe"))
async def handle_unsubscribe(message: types.Message, pool: asyncpg.Pool):
    """Обробник команди /unsubscribe."""
    user_id = message.from_user.id
    if await unsubscribe_user(pool, user_id):
        await message.answer("❌ **Ви успішно відписалися.** Ви більше не отримуватимете AI-новини.", 
                             parse_mode=ParseMode.MARKDOWN)
    else:
        await message.answer("ℹ️ Ви не були підписані.", 
                             parse_mode=ParseMode.MARKDOWN)

@router.message(Command("status"))
async def handle_status(message: types.Message, pool: asyncpg.Pool):
    """Перевіряє статус підписки користувача."""
    user_id = message.from_user.id
    is_subscribed = await pool.fetchval("SELECT EXISTS(SELECT 1 FROM subscribers WHERE user_id = $1)", user_id)
    
    if is_subscribed:
        response = "✅ **Ваш статус:** Ви підписані на розсилку AI-новин."
    else:
        response = "❌ **Ваш статус:** Ви не підписані. Скористайтеся /subscribe, щоб отримувати новини."
        
    await message.answer(response, parse_mode=ParseMode.MARKDOWN)

@router.message(F.text)
async def handle_general_text(message: types.Message):
    """Обробник будь-якого іншого тексту."""
    response = (
        "🤖 Я бот-агрегатор новин. Моя основна функція — розсилка AI-оброблених новин.\n"
        "Скористайтеся командами:\n"
        "/subscribe\n"
        "/unsubscribe\n"
        "/status"
    )
    await message.answer(response)

# --- 6. WEBHOOK ТА ЗАПУСК ДОДАТКУ ---

async def handle_webhook(request: web.Request):
    """Обробник вхідних POST-запитів від Telegram (webhook)."""
    # Отримати ресурси, які були збережені в app
    bot: Bot = request.app["bot"]
    dp: Dispatcher = request.app["dp"]
    
    # Telegram надсилає оновлення як JSON
    update = types.Update.model_validate(await request.json())
    
    # Обробка оновлення диспетчером
    await dp.feed_update(bot, update)
    
    # Обов'язкова відповідь 200 OK
    return web.Response()

async def on_startup(app: web.Application):
    """Виконується при запуску Aiohttp сервера."""
    bot: Bot = app["bot"]
    pool: asyncpg.Pool = app["pool"]
    session: ClientSession = app["session"]

    logger.info("Запуск on_startup...")
    
    # 1. Ініціалізація бази даних
    await init_db(pool)
    
    # 2. Встановлення Webhook
    logger.info(f"Встановлення Webhook на URL: {WEBHOOK_URL}")
    await bot.set_webhook(WEBHOOK_URL, drop_pending_updates=True)

    # 3. Запуск фонового планувальника новин
    # Створюємо завдання, яке буде виконуватися у фоні
    app["news_task"] = asyncio.create_task(news_scheduler(bot, pool, session))
    logger.info("Фоновий планувальник новин запущено.")

async def on_shutdown(app: web.Application):
    """Виконується при зупинці Aiohttp сервера."""
    bot: Bot = app["bot"]
    session: ClientSession = app["session"]
    pool: asyncpg.Pool = app["pool"]

    logger.info("Запуск on_shutdown...")
    
    # 1. Відміна Webhook
    logger.info("Видалення Webhook...")
    await bot.delete_webhook()

    # 2. Зупинка фонового завдання
    # Перевірка, чи існує завдання перед скасуванням
    if "news_task" in app and not app["news_task"].done():
        app["news_task"].cancel()
        logger.info("Фонове завдання планувальника новин скасовано.")

    # 3. Закриття HTTP-сесії
    await session.close()
    
    # 4. Закриття підключення до БД
    await pool.close()
    logger.info("Підключення до БД закрито.")

async def main():
    """Основна функція запуску програми."""
    
    # Ініціалізація Bot, Dispatcher
    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher()
    dp.include_router(router) # Додавання роутера

    # Створення пулу підключень до PostgreSQL
    try:
        # asyncpg вимагає, щоб query-параметри були передані окремо від URL, 
        # особливо якщо URL з Render містить SSL-параметри
        pool = await asyncpg.create_pool(DATABASE_URL)
    except Exception as e:
        logger.critical(f"Не вдалося підключитися до бази даних: {e}")
        return

    # Створення Aiohttp сесії
    session = ClientSession()

    # 2. Налаштування aiohttp Web
    app = web.Application()
    
    # 3. Зберігання ресурсів у додатку
    app["bot"] = bot
    app["dp"] = dp
    app["pool"] = pool
    app["session"] = session

    # 4. Реєстрація залежностей для хендлерів DP
    # Використовуємо .update.middleware.register для ін'єкції ресурсів у всі хендлери
    dp.update.middleware.register(lambda handler, event, data: {
        **data, 
        'pool': pool, 
        'session': session, 
        'bot': bot
    })
    
    # 5. Реєстрація маршруту для вебхука
    app.router.add_post(WEBHOOK_PATH, handle_webhook)
    
    # 6. Реєстрація функцій запуску/вимкнення
    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)
    
    # 7. Запуск сервера
    logger.info(f"Запуск Webhook сервера на {WEB_SERVER_HOST}:{WEB_SERVER_PORT}")
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host=WEB_SERVER_HOST, port=WEB_SERVER_PORT)
    
    await site.start()

    # Нескінченний цикл для підтримки роботи сервера
    await asyncio.Event().wait() 

if __name__ == '__main__':
    # Використання asyncio.run() для запуску головної асинхронної функції
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Бот зупинено користувачем.")
    except Exception as e:
        logger.critical(f"Загальна помилка виконання: {e}")

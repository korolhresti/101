import os
import asyncio
import logging
import re
import random
import sys
from datetime import datetime, timedelta, timezone
from urllib.parse import urljoin
from typing import List, Optional

import asyncpg
import aiohttp
from aiohttp import ClientSession, web
from aiogram import Bot, Dispatcher, Router, types
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.client.default import DefaultBotProperties
from aiogram.types import WebhookInfo, InlineKeyboardMarkup, InlineKeyboardButton # <-- ДОДАНО: Кнопки

# --- 1. НАЛАШТУВАННЯ СЕРЕДОВИЩА ТА ЛОГУВАННЯ ---

# Встановлюємо часовий пояс для коректного планування
KYIV_TZ = timezone(timedelta(hours=3), 'Europe/Kyiv')

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    stream=sys.stdout)
logger = logging.getLogger(__name__)

# Змінні оточення
BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0")) # ID адміністратора для команди /stats

# Webhook конфігурація
WEBHOOK_HOST = os.getenv("WEBHOOK_HOST") # !!! КРИТИЧНО: Ваш публічний домен (обов'язково HTTPS)
WEB_SERVER_HOST = os.getenv("WEB_SERVER_HOST", "0.0.0.0") 
WEB_SERVER_PORT = int(os.getenv("PORT", 8080))

if BOT_TOKEN:
    WEBHOOK_PATH = f"/webhook/{BOT_TOKEN}"
    WEBHOOK_URL = urljoin(WEBHOOK_HOST or "https://placeholder-host.com/", WEBHOOK_PATH)
else:
     WEBHOOK_PATH = "/webhook/placeholder"
     WEBHOOK_URL = ""

# Константи для preferred стилів
EXPERT_POLITICAL = "portnikov"
EXPERT_ECONOMIC = "libsits"
EXPERT_MIXED = "mixed"


# --- 2. СТРУКТУРА БАЗИ ДАНИХ ---

async def create_db_tables(pool: asyncpg.Pool):
    """Створює необхідні таблиці в базі даних. Додано expert_preference."""
    await pool.execute("""
        CREATE TABLE IF NOT EXISTS subscribers (
            chat_id BIGINT PRIMARY KEY,
            subscribed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            expert_preference TEXT DEFAULT 'mixed' NOT NULL  -- НОВЕ ПОЛЕ для preferred
        );
    """)

# --- 3. ХЕНДЛЕРИ БОТА ТА ІНТЕРАКТИВНІСТЬ ---

router = Router() 

def get_preference_keyboard() -> InlineKeyboardMarkup:
    """Створює інлайн-клавіатуру для вибору preferred стилю новин."""
    buttons = [
        [
            InlineKeyboardButton(text="Політика (Портников)", callback_data=f"set_expert_{EXPERT_POLITICAL}"),
        ],
        [
            InlineKeyboardButton(text="Економіка (Лібсіц)", callback_data=f"set_expert_{EXPERT_ECONOMIC}"),
        ],
        [
            InlineKeyboardButton(text="Змішаний (Рандом)", callback_data=f"set_expert_{EXPERT_MIXED}"),
        ]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)


@router.message(Command("start"))
async def command_start_handler(message: types.Message, pool: asyncpg.Pool) -> None:
    """Обробляє команду /start, підписує користувача та пропонує обрати preferred стиль."""
    chat_id = message.chat.id
    try:
        # Вставка або оновлення підписки
        await pool.execute(
            "INSERT INTO subscribers (chat_id) VALUES ($1) ON CONFLICT (chat_id) DO NOTHING",
            chat_id
        )
        await message.answer(
            "Ласкаво просимо! Ви успішно підписалися на щогодинну аналітику від експертів.\n"
            "Оберіть ваш preferred стиль новин:",
            reply_markup=get_preference_keyboard()
        )
    except Exception as e:
        logger.error(f"Помилка підписки користувача {chat_id}: {e}")
        await message.answer("Виникла помилка при оформленні підписки. Спробуйте пізніше.")


@router.callback_query(lambda c: c.data and c.data.startswith('set_expert_'))
async def process_expert_choice(callback_query: types.CallbackQuery, pool: asyncpg.Pool):
    """Обробляє вибір preferred стилю новин користувачем через інлайн-клавіатуру."""
    chat_id = callback_query.message.chat.id
    # Витягуємо preferred: 'set_expert_portnikov' -> 'portnikov'
    preference = callback_query.data.split('_')[-1] 
    
    # Використовуємо словник для відображення назв
    preference_map = {
        EXPERT_POLITICAL: "Політика (Портников)",
        EXPERT_ECONOMIC: "Економіка (Лібсіц)",
        EXPERT_MIXED: "Змішаний (Рандом)"
    }
    preference_title = preference_map.get(preference, preference.capitalize())
    
    try:
        await pool.execute(
            "UPDATE subscribers SET expert_preference = $1 WHERE chat_id = $2",
            preference,
            chat_id
        )
        await callback_query.answer(f"Ваш preferred стиль встановлено: {preference_title}")
        
        # Редагуємо повідомлення, щоб прибрати клавіатуру і показати результат
        await callback_query.message.edit_text(
            f"✅ Ви підписані. Ваш preferred стиль: **{preference_title}**. "
            f"Щогодини ви отримуватимете нову аналітику.",
            parse_mode=ParseMode.MARKDOWN_V2
        )
    except Exception as e:
        logger.error(f"Помилка оновлення preferred користувача {chat_id}: {e}")
        await callback_query.answer("Виникла помилка. Спробуйте пізніше.", show_alert=True)


async def get_user_preference(pool: asyncpg.Pool, chat_id: int) -> Optional[str]:
    """Отримує preferred стиль новин користувача."""
    preference = await pool.fetchval(
        "SELECT expert_preference FROM subscribers WHERE chat_id = $1", 
        chat_id
    )
    # Повертає preferred або None, якщо користувач не підписаний/не має preference
    return preference

@router.message(Command("news"))
async def command_news_handler(message: types.Message, pool: asyncpg.Pool, bot: Bot):
    """Обробляє команду /news та генерує новину негайно, використовуючи preferred користувача."""
    chat_id = message.chat.id
    
    preference = await get_user_preference(pool, chat_id)
    if not preference:
        await message.answer("Ви не підписані або не обрали preferred стиль. Виконайте команду /start.")
        return

    # Імітуємо набір тексту, поки триває генерація
    await bot.send_chat_action(chat_id, "typing")
    await message.answer(f"🔄 Генерую аналітику ({preference.capitalize()})...")

    try:
        # Генерація новини на основі preferred
        news_message = await generate_expert_news(preference) 
        
        if news_message:
            await message.answer(news_message, parse_mode=ParseMode.MARKDOWN_V2)
        else:
            await message.answer("❌ Не вдалося згенерувати аналітику. Спробуйте пізніше або перевірте API ключ.")
            
    except Exception as e:
        logger.error(f"Помилка обробки /news для {chat_id}: {e}")
        await message.answer("Виникла критична помилка під час генерації. Спробуйте пізніше.")


@router.message(Command("stop"))
async def command_stop_handler(message: types.Message, pool: asyncpg.Pool) -> None:
    """Обробляє команду /stop та відписує користувача."""
    chat_id = message.chat.id
    try:
        result = await pool.execute(
            "DELETE FROM subscribers WHERE chat_id = $1",
            chat_id
        )
        if result.split()[-1] == '1':
            await message.answer("Ви успішно відписалися від розсилки.")
        else:
            await message.answer("Ви не були підписані на розсилку.")
    except Exception as e:
        logger.error(f"Помилка відписки користувача {chat_id}: {e}")
        await message.answer("Виникла помилка при скасуванні підписки. Спробуйте пізніше.")

@router.message(Command("stats"))
async def command_stats_handler(message: types.Message, pool: asyncpg.Pool) -> None:
    """Обробляє команду /stats (для адміна) та показує статистику підписників."""
    chat_id = message.chat.id
    
    # Перевірка на адміністратора (порівняння ID)
    if chat_id != ADMIN_ID:
        await message.answer("Ця команда доступна лише адміністратору.")
        return

    try:
        count = await pool.fetchval("SELECT COUNT(*) FROM subscribers")
        # Використовуємо MarkdownV2 та екрануємо символи
        await message.answer(
            f"\\*\\*📊 Статистика Підписників\\*\\*\n"
            f"Всього активних підписників: `{count}`",
            parse_mode=ParseMode.MARKDOWN_V2
        )
    except Exception as e:
        logger.error(f"Помилка отримання статистики: {e}")
        await message.answer("Виникла помилка при отриманні статистики.")


# --- 4. ФУНКЦІЇ ДЛЯ ГЕНЕРАЦІЇ ТА РОЗСИЛКИ НОВИН (GEMINI API) ---

async def get_subscribers(pool: asyncpg.Pool) -> List[int]:
    """Отримує список усіх підписаних chat ID."""
    try:
        records = await pool.fetch("SELECT chat_id FROM subscribers")
        return [r['chat_id'] for r in records]
    except Exception as e:
        logger.error(f"Помилка отримання підписників з DB: {e}")
        return []

async def call_gemini_api(system_prompt: str, user_query: str) -> Optional[str]:
    """Викликає Gemini API з експоненційним відступом та Google Search grounding."""
    if not GEMINI_API_KEY:
        logger.warning("GEMINI_API_KEY не встановлено. Новини не генеруються.")
        return None

    model_name = "gemini-2.5-flash-preview-05-20" 
    api_url = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent?key={GEMINI_API_KEY}"
    
    payload = {
        "contents": [{ "parts": [{ "text": user_query }] }],
        "tools": [{ "google_search": {} }], # Google Search для актуальності
        "systemInstruction": { "parts": [{ "text": system_prompt }] },
    }

    max_retries = 5
    base_delay = 1

    for attempt in range(max_retries):
        try:
            # Створюємо тимчасову ClientSession для API-дзвінка
            async with aiohttp.ClientSession() as session:
                async with session.post(api_url, json=payload, timeout=30) as response:
                    if response.status == 200:
                        result = await response.json()
                        candidate = result.get('candidates', [{}])[0]
                        text = candidate.get('content', {}).get('parts', [{}])[0].get('text')
                        
                        sources_list = []
                        grounding_metadata = candidate.get('groundingMetadata')
                        if grounding_metadata and grounding_metadata.get('groundingAttributions'):
                            # Форматуємо посилання, екрануючи заголовок для MarkdownV2
                            sources_list = [
                                f"[{re.sub(r'([_*[\]()~`>#+\-=|{}.!])', r'\\\1', attr['web']['title'])}]({attr['web']['uri']})"
                                for attr in grounding_metadata['groundingAttributions']
                                if attr.get('web', {}).get('uri') and attr.get('web', {}).get('title')
                            ]
                        
                        full_text = text
                        if sources_list:
                            # Додаємо джерела в кінці повідомлення, екрануючи роздільник
                            full_text += "\n\n\\*\\*\\*\\n\\*\\*Джерела\\*\\*:\n" + "\n".join(sources_list)
                            
                        return full_text

                    elif response.status in (429, 500, 503) and attempt < max_retries - 1:
                        delay = base_delay * (2 ** attempt) + (random.random() * 0.5)
                        await asyncio.sleep(delay)
                        continue
                    else:
                        logger.error(f"Gemini API returned status {response.status}: {await response.text()}")
                        return None
        except asyncio.TimeoutError:
            logger.error(f"Gemini API call timed out on attempt {attempt + 1}")
        except Exception as e:
            logger.error(f"Непередбачувана помилка під час API-дзвінка: {e}")
            break
            
    logger.error("Не вдалося викликати Gemini API після кількох спроб.")
    return None

async def generate_expert_news(expert_choice: Optional[str] = None) -> Optional[str]:
    """Генерує новину-аналітику, використовуючи обраний або випадковий стиль."""
    
    if expert_choice is None or expert_choice == EXPERT_MIXED:
        # Для scheduled розсилки або коли обрано "mixed", вибираємо випадково (або mixed)
        expert_choice = random.choice([EXPERT_POLITICAL, EXPERT_ECONOMIC, EXPERT_MIXED])
    
    if expert_choice == EXPERT_POLITICAL:
        system_prompt = "Ви — глибокий політичний оглядач, в стилі Віталія Портникова. Створіть стислу, аналітичну новину про актуальні політичні події в Україні чи світі. Тон має бути серйозним, прогнозуючим, з акцентом на історичні паралелі та геополітичні наслідки. Відповідь має бути лише текстом новини, відформатованим за правилами **MarkdownV2** (використовуйте \\*\\*жирний шрифт\\*\\* для виділення). Не використовуйте заголовки."
        user_query = "Напиши одну аналітичну новину на основі останніх політичних подій."
        title = "Політична Аналітика"
    elif expert_choice == EXPERT_ECONOMIC:
        system_prompt = "Ви — провідний економічний експерт та професор, в стилі Ігоря Лібсіца. Створіть стислу, але ґрунтовну економічну новину-прогноз для України, використовуючи економічні терміни та реальні дані. Тон має бути прагматичним та трохи критичним. Відповідь має бути лише текстом новини, відформатованим за правилами **MarkdownV2** (використовуйте \\*\\*жирний шрифт\\*\\* для виділення). Не використовуйте заголовки."
        user_query = "Напиши одну економічну новину-прогноз на основі поточних економічних тенденцій в Україні."
        title = "Економічний Прогноз"
    else: # mixed (якщо викликано з preference, але тут він вже має бути або political, або economic, це запасний варіант)
        system_prompt = "Ви — аналітик, що поєднує політичну глибину Віталія Портникова та економічний прагматизм Ігоря Лібсіца. Створіть одну, комплексну новину, яка аналізує політичні рішення через призму їхніх економічних наслідків. Відповідь має бути лише текстом новини, відформатованим за правилами **MarkdownV2** (використовуйте \\*\\*жирний шрифт\\*\\* для виділення). Не використовуйте заголовки."
        user_query = "Напиши одну комплексну новину-аналітичну статтю, що поєднує політику та економіку."
        title = "Комплексний Огляд"

    raw_news_text = await call_gemini_api(system_prompt, user_query)

    if raw_news_text:
        # Екрануємо заголовок для MarkdownV2
        escaped_title = re.sub(r'([_*[\]()~`>#+\-=|{}.!])', r'\\\1', title)
        
        # Додаємо заголовок та дату (в MarkdownV2, тому використовуємо екранування)
        current_time = datetime.now(KYIV_TZ).strftime("%d\\.%m\\.%Y %H:%M")
        
        return (
            f"📰 \\*\\*{escaped_title} (Станом на {current_time})\\*\\*\n\n"
            f"{raw_news_text}"
        )
    
    return None

async def news_poster(app: web.Application):
    """Фонове завдання, що генерує та публікує новини щогодини."""
    bot: Bot = app['bot']
    pool: asyncpg.Pool = app['pool']
    
    logger.info("Запуск завдання щогодинної розсилки новин.")
    
    # Затримка для завершення ініціалізації
    await asyncio.sleep(5) 

    while True:
        try:
            # 1. Розрахунок часу до наступної повної години
            now = datetime.now(KYIV_TZ)
            # Встановлюємо час на початок наступної години
            next_hour = (now + timedelta(hours=1)).replace(minute=0, second=5, microsecond=0, tzinfo=KYIV_TZ)
            wait_seconds = (next_hour - now).total_seconds()
            
            if wait_seconds < 0:
                 # Якщо минула година, чекаємо до наступної години
                 next_hour = (now + timedelta(hours=2)).replace(minute=0, second=5, microsecond=0, tzinfo=KYIV_TZ)
                 wait_seconds = (next_hour - now).total_seconds()
            
            logger.info(f"Очікування {wait_seconds:.0f} секунд до наступного запуску ({next_hour.strftime('%H:%M:%S')}).")
            await asyncio.sleep(wait_seconds)

            # 2. Generate News (без preferred, буде обрано випадково)
            logger.info("Час прийшов. Запуск генерації новин...")
            news_message = await generate_expert_news(expert_choice=None) # Вибір випадковий/змішаний
            
            if news_message:
                # 3. Get Subscribers
                subscribers = await get_subscribers(pool)
                logger.info(f"Новина згенерована. Знайдено {len(subscribers)} підписників.")

                # 4. Send News
                for chat_id in subscribers:
                    try:
                        await bot.send_message(
                            chat_id=chat_id, 
                            text=news_message, 
                            parse_mode=ParseMode.MARKDOWN_V2 # Використовуємо MarkdownV2
                        )
                        # Невелика затримка, щоб уникнути обмежень Telegram
                        await asyncio.sleep(0.05) 
                    except Exception as e:
                        logger.error(f"Не вдалося надіслати новину користувачу {chat_id}: {e}")
                
            else:
                logger.warning("Генерація новин не вдалася або повернула порожній вміст.")

        except asyncio.CancelledError:
            logger.info("Завдання розсилки новин скасовано.")
            break
        except Exception as e:
            logger.critical(f"Критична помилка в циклі news_poster: {e}")
            await asyncio.sleep(60) # Пауза перед наступною спробою


# --- 5. КОНФІГУРАЦІЯ WEBHOOK I SERVER ---

async def handle_webhook(request: web.Request) -> web.Response:
    """Обробляє вхідні оновлення від Telegram."""
    bot: Bot = request.app['bot']
    dp: Dispatcher = request.app['dp']
    
    # Передаємо оновлення диспетчеру
    try:
        update = types.Update.model_validate(await request.json(), context={"bot": bot})
        await dp.feed_update(bot, update)
    except Exception as e:
        logger.error(f"Помилка обробки Webhook: {e}")
    
    # Telegram очікує 200 OK
    return web.Response(status=200)

async def on_startup(app: web.Application):
    """Дії, що виконуються при запуску додатка."""
    pool: asyncpg.Pool = app['pool']
    bot: Bot = app['bot']
    dp: Dispatcher = app['dp']
    
    logger.info("Запуск on_startup...")
    
    # 1. Створення/перевірка таблиць в DB
    await create_db_tables(pool)
    logger.info("Таблиці DB перевірено/створено.")

    # 2. Встановлення Webhook
    if not WEBHOOK_HOST or WEBHOOK_HOST == "https://placeholder-host.com/":
        logger.critical("WEBHOOK_HOST environment variable is NOT set correctly. Webhook не буде налаштовано.")
        return 
        
    try:
        webhook_info: WebhookInfo = await bot.get_webhook_info()
        
        if webhook_info.url != WEBHOOK_URL:
            success = await bot.set_webhook(
                url=WEBHOOK_URL,
                allowed_updates=dp.resolve_used_update_types(),
                drop_pending_updates=True
            )
            if success:
                 logger.info(f"Встановлення Webhook на URL: {WEBHOOK_URL} - Успішно.")
            else:
                logger.critical(f"Помилка встановлення Webhook: Telegram server returned False.")
        else:
            logger.info(f"Webhook вже встановлено на URL: {WEBHOOK_URL}")

    except Exception as e:
        logger.critical(f"Критична помилка встановлення Webhook: {e}")

    # 3. Запуск фонового завдання для розсилки новин
    app['news_task'] = asyncio.create_task(news_poster(app))
    
    
async def on_shutdown(app: web.Application):
    """Дії, що виконуються при зупинці додатка."""
    bot: Bot = app['bot']
    session: ClientSession = app['session']
    pool: asyncpg.Pool = app['pool']
    
    logger.info("Запуск on_shutdown...")
    
    # 1. Вимкнення фонового завдання
    if 'news_task' in app:
        app['news_task'].cancel()
        await asyncio.gather(app['news_task'], return_exceptions=True)
        logger.info("Фонове завдання news_poster скасовано.")
    
    # 2. Видалення Webhook
    try:
        await bot.delete_webhook()
        logger.info("Webhook успішно видалено.")
    except Exception as e:
        logger.warning(f"Помилка видалення Webhook: {e}")
        
    # 3. Закриття HTTP сесії та пулу DB
    await session.close()
    await pool.close()
    logger.info("HTTP сесія та пул DB закриті.")


# --- 6. ОСНОВНА ФУНКЦІЯ ЗАПУСКУ ---

async def main():
    """Налаштування пулу DB, бота, диспетчера та aiohttp сервера."""
    if not BOT_TOKEN or not DATABASE_URL:
        logger.critical("Не встановлено BOT_TOKEN або DATABASE_URL.")
        sys.exit(1)

    # 1. Ініціалізація компонентів
    pool = await asyncpg.create_pool(DATABASE_URL)
    session = ClientSession()
    
    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher()
    
    # Реєстрація головного роутера з хендлерами
    dp.include_router(router) 

    # 2. Налаштування aiohttp Web
    app = web.Application()
    
    # 3. Зберігання ресурсів у додатку
    app["bot"] = bot
    app["dp"] = dp
    app["pool"] = pool
    app["session"] = session

    # 4. Реєстрація залежностей для хендлерів (тепер passed 'bot' to message handlers is crucial for /news)
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
    
    await site.start()
    
    # Тримаємо main() активним, поки aiohttp керує процесом
    while True:
        await asyncio.sleep(3600) 

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Бот зупинено вручну.")
    except Exception as e:
        logger.critical(f"Глобальна помилка виконання: {e}")

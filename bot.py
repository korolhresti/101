import os
import asyncio
import logging
import re
import random
import sys
from datetime import datetime, timedelta, timezone
from urllib.parse import urljoin
from typing import List, Optional, Dict, Any

import asyncpg
import aiohttp
from aiohttp import ClientSession, web
from aiogram import Bot, Dispatcher, Router, types # Router імпортовано
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.client.default import DefaultBotProperties
from aiogram.types import WebhookInfo, InlineKeyboardMarkup, InlineKeyboardButton
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
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))

# Webhook конфігурація
WEBHOOK_HOST = os.getenv("WEBHOOK_HOST")
WEB_SERVER_HOST = os.getenv("WEB_SERVER_HOST", "0.0.0.0") 
WEB_SERVER_PORT = int(os.getenv("PORT", 8080))

if BOT_TOKEN:
    WEBHOOK_PATH = f"/webhook/{BOT_TOKEN}"
    # Використовуємо WEBHOOK_HOST, який повинен бути публічним HTTPS-доменом
    WEBHOOK_URL = urljoin(WEBHOOK_HOST or "https://placeholder-host.com/", WEBHOOK_PATH)
else:
     WEBHOOK_PATH = "/webhook/placeholder"
     WEBHOOK_URL = ""

# Константи для preferred стилів
EXPERT_POLITICAL = "portnikov"
EXPERT_ECONOMIC = "libsits"
EXPERT_MIXED = "mixed"


# --- 2. МОДУЛЬ АНАЛІТИКИ: MOCK ECONOMIC ENGINE ---

class EconomicEngine:
    """Мок-клас для симуляції доступу до економічних даних або API."""
    
    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool
        # Імітація актуальних економічних показників
        self.mock_data = {
            "gdp_growth": "Попередній квартал: +0.8%; Прогноз: +1.2%",
            "inflation": "Поточний рівень: 8.5%; Ціль НБУ: 5%",
            "bond_yields": "ОВДП (1 рік): 18.5%",
            "currency_rate": "Офіційний курс НБУ: 40.5 UAH/USD"
        }

    async def get_latest_report(self) -> Dict[str, str]:
        """Симулює отримання актуальних економічних даних."""
        # У професійній версії тут був би виклик API чи DB
        await asyncio.sleep(0.1) # Імітація затримки
        return self.mock_data
        
    async def get_report_summary(self, style: str) -> str:
        """Створює короткий звіт у заданому стилі на основі мок-даних."""
        data = await self.get_latest_report()
        # Форматуємо дані для вставки в prompt (екранування для MarkdownV2 відбувається пізніше)
        summary = (
            f"Останні економічні показники:\n"
            f"Зростання ВВП: {data['gdp_growth']}\n"
            f"Інфляція: {data['inflation']}\n"
            f"Дохідність держоблігацій: {data['bond_yields']}\n"
            f"Курс НБУ: {data['currency_rate']}\n"
        )
        return summary


# --- 3. СТРУКТУРА БАЗИ ДАНИХ ТА FSM СТАНИ ---

async def create_db_tables(pool: asyncpg.Pool):
    """Створює необхідні таблиці в базі даних. Додано expert_preference та last_active."""
    await pool.execute("""
        CREATE TABLE IF NOT EXISTS subscribers (
            chat_id BIGINT PRIMARY KEY,
            subscribed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            expert_preference TEXT DEFAULT 'mixed' NOT NULL,
            last_active TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );
    """)

class NewsChat(StatesGroup):
    """Стани для керування багатоходовим діалоговим режимом з експертом-ШІ."""
    waiting_for_question = State() # Очікування питання від користувача


# --- 4. MIDDLEWARE ДЛЯ ПРОФЕСІЙНОЇ ПЛАТФОРМИ ---

# Middleware для оновлення часу останньої активності користувача
async def update_last_active_middleware(handler, event: types.Update, data: dict):
    """Оновлює поле last_active у DB для кожного вхідного повідомлення."""
    pool = data.get('pool')
    
    # Визначаємо chat_id для повідомлень, callback_query тощо
    chat_id = event.message.chat.id if event.message else (
        event.callback_query.message.chat.id if event.callback_query and event.callback_query.message else None
    )

    if pool and chat_id:
        try:
             await pool.execute(
                "UPDATE subscribers SET last_active = NOW() WHERE chat_id = $1",
                chat_id
            )
        except Exception as e:
            logger.debug(f"Помилка оновлення last_active для {chat_id}: {e}")
            
    return await handler(event, data)


# --- 5. ХЕНДЛЕРИ БОТА ТА ІНТЕРАКТИВНІСТЬ ---

# ВИПРАВЛЕННЯ: Створюємо інстанс Router() безпосередньо.
router = Router() 
router.message.middleware.register(update_last_active_middleware)
router.callback_query.middleware.register(update_last_active_middleware)


def get_preference_keyboard() -> InlineKeyboardMarkup:
    """Створює інлайн-клавіатуру для вибору preferred стилю новин."""
    buttons = [
        [
            InlineKeyboardButton(text="📰 Політика (Портников)", callback_data=f"set_expert_{EXPERT_POLITICAL}"),
        ],
        [
            InlineKeyboardButton(text="📈 Економіка (Лібсіц)", callback_data=f"set_expert_{EXPERT_ECONOMIC}"),
        ],
        [
            InlineKeyboardButton(text="⚖️ Змішаний (Рандом)", callback_data=f"set_expert_{EXPERT_MIXED}"),
        ]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)


async def get_user_preference(pool: asyncpg.Pool, chat_id: int) -> Optional[str]:
    """Отримує preferred стиль новин користувача."""
    preference = await pool.fetchval(
        "SELECT expert_preference FROM subscribers WHERE chat_id = $1", 
        chat_id
    )
    return preference

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
            "**👋 Ласкаво просимо до Професійної Аналітики!**\n\n"
            "Ви успішно підписалися на щогодинну аналітику від експертів\\-ШІ\\.\n"
            "Оберіть ваш preferred стиль новин:",
            reply_markup=get_preference_keyboard(),
            parse_mode=ParseMode.MARKDOWN_V2
        )
    except Exception as e:
        logger.error(f"Помилка підписки користувача {chat_id}: {e}")
        await message.answer("❌ Виникла помилка при оформленні підписки\\. Спробуйте пізніше\\.", parse_mode=ParseMode.MARKDOWN_V2)

@router.message(Command("settings"))
async def command_settings_handler(message: types.Message) -> None:
    """Дозволяє користувачу змінити свій preferred стиль."""
    await message.answer(
        "**⚙️ Налаштування Експерта**\n\n"
        "Оберіть preferred стиль новин для щогодинної розсилки та миттєвих запитів \\(/news, /ask\\):",
        reply_markup=get_preference_keyboard(),
        parse_mode=ParseMode.MARKDOWN_V2
    )


@router.callback_query(lambda c: c.data and c.data.startswith('set_expert_'))
async def process_expert_choice(callback_query: types.CallbackQuery, pool: asyncpg.Pool):
    """Обробляє вибір preferred стилю новин користувачем через інлайн-клавіатуру."""
    chat_id = callback_query.message.chat.id
    preference = callback_query.data.split('_')[-1] 
    
    preference_map = {
        EXPERT_POLITICAL: "Політика (Портников)",
        EXPERT_ECONOMIC: "Економіка (Лібсіц)",
        EXPERT_MIXED: "Змішаний (Рандом)"
    }
    preference_title = preference_map.get(preference, preference.capitalize())
    
    try:
        # Оновлення preference та часу останньої активності
        await pool.execute(
            "UPDATE subscribers SET expert_preference = $1, last_active = NOW() WHERE chat_id = $2",
            preference,
            chat_id
        )
        await callback_query.answer(f"Ваш preferred стиль встановлено: {preference_title}")
        
        # Редагуємо повідомлення, щоб прибрати клавіатуру і показати результат
        await callback_query.message.edit_text(
            f"✅ Налаштування оновлено\\. Ваш preferred стиль: **{preference_title}**\\.",
            parse_mode=ParseMode.MARKDOWN_V2
        )
    except Exception as e:
        logger.error(f"Помилка оновлення preferred користувача {chat_id}: {e}")
        await callback_query.answer("❌ Виникла помилка\\. Спробуйте пізніше\\.", show_alert=True)


@router.message(Command("news"))
async def command_news_handler(message: types.Message, pool: asyncpg.Pool, bot: Bot, session: ClientSession, economic_engine: 'EconomicEngine'):
    """Обробляє команду /news та генерує новину негайно, використовуючи preferred користувача."""
    chat_id = message.chat.id
    
    preference = await get_user_preference(pool, chat_id)
    if not preference:
        await message.answer("Ви не підписані або не обрали preferred стиль\\. Виконайте команду /start\\.", parse_mode=ParseMode.MARKDOWN_V2)
        return

    await bot.send_chat_action(chat_id, "typing")
    await message.answer(f"🔄 **Генерую аналітику ({preference.capitalize()})**\\.\\.\\.", parse_mode=ParseMode.MARKDOWN_V2)

    try:
        # Передаємо глобальну aiohttp сесію та economic_engine
        news_message = await generate_expert_news(session, economic_engine, preference) 
        
        if news_message:
            await message.answer(news_message, parse_mode=ParseMode.MARKDOWN_V2)
        else:
            await message.answer("❌ Не вдалося згенерувати аналітику\\. Спробуйте пізніше або перевірте API ключ\\.", parse_mode=ParseMode.MARKDOWN_V2)
            
    except Exception as e:
        logger.error(f"Помилка обробки /news для {chat_id}: {e}")
        await message.answer("❌ Виникла критична помилка під час генерації\\. Спробуйте пізніше\\.", parse_mode=ParseMode.MARKDOWN_V2)

@router.message(Command("ask"))
async def command_ask_handler(message: types.Message, state: FSMContext, pool: asyncpg.Pool):
    """Розпочинає багатоходовий діалоговий режим Q&A з експертом-ШІ."""
    chat_id = message.chat.id
    preference = await get_user_preference(pool, chat_id)
    
    if not preference:
        await message.answer("Ви не підписані\\. Виконайте команду /start, щоб розпочати.", parse_mode=ParseMode.MARKDOWN_V2)
        return
    
    # Скидаємо будь-яку стару історію, щоб почати нову консультацію
    await state.set_data({'chat_history': []})

    expert_title = f"Експерт ({preference.capitalize()})"
    
    await message.answer(
        f"**🎙️ Консультація з {expert_title}**\n\n"
        "Задайте ваше перше питання\\. Це **багатоходовий діалог**, тому ви можете ставити додаткові питання, і експерт пам'ятатиме контекст\\.\n\n"
        "Надішліть питання, або /cancel, щоб вийти з режиму консультації\\.",
        parse_mode=ParseMode.MARKDOWN_V2
    )
    # Встановлюємо FSM стан
    await state.set_state(NewsChat.waiting_for_question)


@router.message(NewsChat.waiting_for_question)
async def process_expert_question(message: types.Message, state: FSMContext, pool: asyncpg.Pool, bot: Bot, session: ClientSession):
    """Обробляє питання користувача в багатоходовому діалоговому режимі."""
    user_question = message.text
    chat_id = message.chat.id
    
    # 0. Перевірка на /cancel
    if user_question.lower() == "/cancel":
        await state.clear()
        await message.answer("✅ **Консультацію скасовано**\\. Щоб розпочати нову, використайте команду /ask\\.", parse_mode=ParseMode.MARKDOWN_V2)
        return

    # 1. Отримання preferred стилю та історії
    preference = await get_user_preference(pool, chat_id)
    state_data = await state.get_data()
    
    # Історія чату: list of {'role': 'user'/'model', 'parts': [{'text': '...'}]}
    chat_history: List[Dict[str, Any]] = state_data.get('chat_history', [])
    
    # 2. Визначення system_prompt (надсилаємо завжди для стійкості)
    system_prompt = "Ви — експерт-аналітик. Дайте чітку, стислу відповідь на питання користувача. Ваш тон і стиль мають відповідати обраному профілю. Використовуйте **MarkdownV2** для форматування. Відповідь має бути лише текстом, без заголовків."
    if preference == EXPERT_POLITICAL:
        system_prompt += " (Політичний оглядач, стиль Портникова: глибокий, геополітичний, прогнозуючий)."
    elif preference == EXPERT_ECONOMIC:
        system_prompt += " (Економічний експерт, стиль Лібсіца: прагматичний, критичний, з даними)."
    else:
        system_prompt += " (Змішаний, збалансований аналіз)."

    # 3. Додаємо нове питання користувача до історії
    new_user_message = {
        "role": "user",
        "parts": [{ "text": user_question }]
    }
    # Для API-дзвінка ми використовуємо поточну історію + нове повідомлення користувача
    contents_for_api = chat_history + [new_user_message] 

    await bot.send_chat_action(chat_id, "typing")
    await message.answer("🤔 **Обмірковую відповідь\\.\\.\\.**", parse_mode=ParseMode.MARKDOWN_V2)

    # 4. Виклик Gemini API з повною історією
    response_text = await call_gemini_api(session, contents_for_api, system_prompt)
    
    if response_text:
        # 5. Надсилаємо відповідь
        await message.answer(
            f"**Відповідь Експерта ({preference.capitalize()})**:\n\n"
            f"{response_text}",
            parse_mode=ParseMode.MARKDOWN_V2
        )
        
        # 6. Додаємо відповідь моделі до історії для наступного запиту
        # Для коректної історії чату, ми повинні додати повідомлення користувача і відповідь моделі
        chat_history.append(new_user_message)
        
        # Важливо: Gemini API повертає text з джерелами. Для історії чату ми зберігаємо лише текст моделі.
        model_text_only = response_text.split("***")[0].strip() 
        new_model_response = {
            "role": "model",
            "parts": [{ "text": model_text_only }]
        }
        chat_history.append(new_model_response)
        
        # 7. Зберігаємо оновлену історію в FSM контексті
        await state.update_data(chat_history=chat_history)
        
        await message.answer(
            "Продовжуйте консультацію або використайте /cancel, щоб завершити\\.",
            parse_mode=ParseMode.MARKDOWN_V2
        )
        # Стан NewsChat.waiting_for_question залишається активним

    else:
        # 8. Обробка помилки
        await message.answer("❌ Вибачте, сталася помилка під час генерації відповіді\\. Історію чату збережено\\. Спробуйте інше питання або /cancel\\.", parse_mode=ParseMode.MARKDOWN_V2)


@router.message(Command("cancel"))
async def command_cancel_handler(message: types.Message, state: FSMContext):
    """Скасовує будь-яку поточну FSM операцію."""
    current_state = await state.get_state()
    if current_state is None:
        await message.answer("Немає активних операцій для скасування.")
        return
    
    await state.clear()
    await message.answer("Операцію скасовано\\. Я знову готовий до роботи\\.", parse_mode=ParseMode.MARKDOWN_V2)


@router.message(Command("help"))
async def command_help_handler(message: types.Message) -> None:
    """Обробляє команду /help та надає довідку про функціонал."""
    help_text = (
        "**🤖 Професійна Аналітика — Ваш Експертний Бот**\n\n"
        "Цей бот надає глибоку аналітику, змодельовану на основі стилів відомих експертів \\(Портников/Лібсіц\\) із залученням актуальних даних \\(Google Search Grounding\\) та економічних показників\\.\n\n"
        "**Доступні команди:**\n"
        "• `/start` \\- Підписатися та обрати preferred стиль.\n"
        "• `/news` \\- Отримати свіжу аналітику негайно \\(відповідно до вашого preferred стилю та останніх економічних даних\\).\n"
        "• `/settings` \\- Змінити ваш preferred стиль новин.\n"
        "• `/ask` \\- Розпочати багатоходову консультацію з експертом \\(діалог з пам'яттю контексту\\).\n"
        "• `/stop` \\- Скасувати підписку на щогодинну розсилку.\n"
        "• `/help` \\- Показати цю довідку.\n"
        "• `/cancel` \\- Скасувати поточну операцію \\(наприклад, під час `/ask`\\)."
    )
    # Екрануємо символи для MarkdownV2
    help_text = re.sub(r'([_*[\]()~`>#+\-=|{}.!])', r'\\\1', help_text)
    await message.answer(help_text, parse_mode=ParseMode.MARKDOWN_V2)


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
    
    if chat_id != ADMIN_ID:
        await message.answer("Ця команда доступна лише адміністратору.")
        return

    try:
        count = await pool.fetchval("SELECT COUNT(*) FROM subscribers")
        # Активні - ті, хто взаємодіяв за останні 7 днів
        active_count = await pool.fetchval(
            "SELECT COUNT(*) FROM subscribers WHERE last_active >= NOW() - INTERVAL '7 days'"
        )
        
        await message.answer(
            f"\\*\\*📊 Статистика Платформи\\*\\*\n"
            f"Всього підписників: `{count}`\n"
            f"Активних за 7 днів: `{active_count}`",
            parse_mode=ParseMode.MARKDOWN_V2
        )
    except Exception as e:
        logger.error(f"Помилка отримання статистики: {e}")
        await message.answer("Виникла помилка при отриманні статистики.")


# --- 6. ФУНКЦІЇ ДЛЯ ГЕНЕРАЦІЇ ТА РОЗСИЛКИ НОВИН (GEMINI API) ---

async def get_subscribers(pool: asyncpg.Pool) -> List[int]:
    """Отримує список усіх підписаних chat ID."""
    try:
        records = await pool.fetch("SELECT chat_id FROM subscribers")
        return [r['chat_id'] for r in records]
    except Exception as e:
        logger.error(f"Помилка отримання підписників з DB: {e}")
        return []

async def call_gemini_api(session: ClientSession, contents_list: List[Dict[str, Any]], system_prompt: Optional[str] = None) -> Optional[str]:
    """
    Викликає Gemini API з експоненційним відступом та Google Search grounding,
    використовуючи повну історію чату.
    """
    if not GEMINI_API_KEY:
        logger.warning("GEMINI_API_KEY не встановлено. Новини/відповіді не генеруються.")
        return None

    model_name = "gemini-2.5-flash-preview-05-20" 
    api_url = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent?key={GEMINI_API_KEY}"
    
    payload = {
        "contents": contents_list,
        "tools": [{ "google_search": {} }], 
    }
    
    if system_prompt:
        payload["systemInstruction"] = { "parts": [{ "text": system_prompt }] }

    max_retries = 5
    base_delay = 1

    for attempt in range(max_retries):
        try:
            # Використовуємо передану сесію
            async with session.post(api_url, json=payload, timeout=45) as response:
                if response.status == 200:
                    result = await response.json()
                    candidate = result.get('candidates', [{}])[0]
                    text = candidate.get('content', {}).get('parts', [{}])[0].get('text')
                    
                    if not text:
                         return "Вибачте, експерт не зміг сформувати відповідь. Спробуйте інше питання."
                        
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
                        # Додаємо розділювач, щоб відокремити джерела від основного тексту
                        full_text += "\n\n\\*\\*\\*\\n\\*\\*Джерела\\*\\*:\n" + "\n".join(sources_list)
                        
                    return full_text

                elif response.status in (429, 500, 503) and attempt < max_retries - 1:
                    delay = base_delay * (2 ** attempt) + (random.random() * 0.5)
                    logger.warning(f"Gemini API: Помилка {response.status}. Спроба {attempt + 1}/{max_retries}. Очікування {delay:.2f}с.")
                    await asyncio.sleep(delay)
                    continue
                else:
                    logger.error(f"Gemini API повернув статус {response.status}: {await response.text()}")
                    return None
        except asyncio.TimeoutError:
            logger.error(f"Gemini API: Час очікування вичерпано на спробі {attempt + 1}")
        except Exception as e:
            logger.error(f"Непередбачувана помилка під час API-дзвінка: {e}")
            break
            
    logger.error("Не вдалося викликати Gemini API після кількох спроб.")
    return None

async def generate_expert_news(session: ClientSession, economic_engine: 'EconomicEngine', expert_choice: Optional[str] = None) -> Optional[str]:
    """Генерує новину-аналітику, використовуючи обраний або випадковий стиль та економічні дані."""
    
    if expert_choice is None or expert_choice == EXPERT_MIXED:
        expert_choice = random.choice([EXPERT_POLITICAL, EXPERT_ECONOMIC, EXPERT_MIXED])
    
    # Отримуємо звіт з мок-аналітичного двигуна
    economic_report = await economic_engine.get_report_summary(expert_choice)

    if expert_choice == EXPERT_POLITICAL:
        system_prompt = "Ви — глибокий політичний оглядач, в стилі Віталія Портникова. Створіть стислу, аналітичну новину про актуальні політичні події в Україні чи світі. Тон має бути серйозним, прогнозуючим, з акцентом на історичні паралелі та геополітичні наслідки. Використовуйте **MarkdownV2** для форматування. Не використовуйте заголовки."
        user_query = f"Напиши одну аналітичну новину, яка включає оцінку політичної ситуації на основі наступних економічних даних:\n{economic_report}"
        title = "Політична Аналітика"
    elif expert_choice == EXPERT_ECONOMIC:
        system_prompt = "Ви — провідний економічний експерт та професор, в стилі Ігоря Лібсіца. Створіть стислу, але ґрунтовну економічну новину-прогноз для України, використовуючи економічні терміни. Тон має бути прагматичним та трохи критичним. Використовуйте **MarkdownV2** для форматування. Не використовуйте заголовки."
        user_query = f"Напиши один детальний економічний прогноз, аналізуючи наступні дані:\n{economic_report}"
        title = "Економічний Прогноз"
    else: 
        system_prompt = "Ви — аналітик, що поєднує політичну глибину Портникова та економічний прагматизм Лібсіца. Створіть одну, комплексну новину, яка аналізує політичні рішення через призму їхніх економічних наслідків. Використовуйте **MarkdownV2** для форматування. Не використовуйте заголовки."
        user_query = f"Напиши одну комплексну новину-аналітичну статтю, що поєднує політику та економіку, на основі даних:\n{economic_report}"
        title = "Комплексний Огляд"

    # Створюємо contents_list для LLM (одноразовий запит)
    contents_list = [{ "parts": [{ "text": user_query }] }]
    raw_news_text = await call_gemini_api(session, contents_list, system_prompt)

    if raw_news_text:
        # Екрануємо заголовок та дату для MarkdownV2
        escaped_title = re.sub(r'([_*[\]()~`>#+\-=|{}.!])', r'\\\1', title)
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
    session: ClientSession = app['session']
    economic_engine: 'EconomicEngine' = app['economic_engine'] # Отримуємо інстанс EconomicEngine
    
    logger.info("Запуск завдання щогодинної розсилки новин.")
    
    await asyncio.sleep(5) 

    while True:
        try:
            # 1. Розрахунок часу
            now = datetime.now(KYIV_TZ)
            next_hour = (now + timedelta(hours=1)).replace(minute=0, second=5, microsecond=0, tzinfo=KYIV_TZ)
            wait_seconds = (next_hour - now).total_seconds()
            
            if wait_seconds < 0:
                 next_hour = (now + timedelta(hours=2)).replace(minute=0, second=5, microsecond=0, tzinfo=KYIV_TZ)
                 wait_seconds = (next_hour - now).total_seconds()
            
            logger.info(f"Очікування {wait_seconds:.0f} секунд до наступного запуску ({next_hour.strftime('%H:%M:%S')}).")
            await asyncio.sleep(wait_seconds)

            # 2. Generate News (передаємо сесію та двигун)
            logger.info("Час прийшов. Запуск генерації новин...")
            news_message = await generate_expert_news(session, economic_engine, expert_choice=None) 
            
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
                            parse_mode=ParseMode.MARKDOWN_V2 
                        )
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
            await asyncio.sleep(60) 


# --- 7. КОНФІГУРАЦІЯ WEBHOOK I SERVER ---

async def handle_webhook(request: web.Request) -> web.Response:
    """Обробляє вхідні оновлення від Telegram."""
    bot: Bot = request.app['bot']
    dp: Dispatcher = request.app['dp']
    
    try:
        # Впевнюємося, що ми передаємо об'єкти bot та dp для обробки
        update = types.Update.model_validate(await request.json(), context={"bot": bot})
        await dp.feed_update(bot, update)
    except Exception as e:
        logger.error(f"Помилка обробки Webhook: {e}")
    
    return web.Response(status=200)

async def on_startup(app: web.Application):
    """Дії, що виконуються при запуску додатка."""
    pool: asyncpg.Pool = app['pool']
    bot: Bot = app['bot']
    dp: Dispatcher = app['dp']
    
    logger.info("Запуск on_startup...")
    
    # 1. Створення/перевірка таблиць в DB
    await create_db_tables(pool)
    # ЛОГ: оновлено для консистентності
    logger.info("Таблиця 'subscribers' перевірена/створена.")

    # 2. Встановлення Webhook
    
    # КРИТИЧНА ПЕРЕВІРКА: WEBHOOK_HOST має бути публічним HTTPS-доменом
    if not WEBHOOK_HOST or "placeholder-host" in WEBHOOK_HOST or not WEBHOOK_HOST.startswith("https://"):
        logger.critical(
            "WEBHOOK_HOST environment variable is NOT set correctly. "
            "Він має бути публічною HTTPS-адресою (домен або IP), а не внутрішньою (як 0.0.34.96)."
        )
        # Продовжуємо, але Telegram поверне помилку, якщо WEBHOOK_HOST недійсний
    
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
        logger.critical(f"Критична помилка встановлення Webhook: Telegram server says - {e}")

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


# --- 8. ОСНОВНА ФУНКЦІЯ ЗАПУСКУ ---

async def main():
    """Налаштування пулу DB, бота, диспетчера, EconomicEngine та aiohttp сервера."""
    if not BOT_TOKEN or not DATABASE_URL:
        logger.critical("Не встановлено BOT_TOKEN або DATABASE_URL.")
        sys.exit(1)

    # 1. Ініціалізація компонентів
    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.MARKDOWN_V2))
    dp = Dispatcher()
    
    try:
        pool = await asyncpg.create_pool(DATABASE_URL)
    except Exception as e:
        logger.critical(f"Помилка підключення до бази даних: {e}")
        sys.exit(1)
        
    session = ClientSession()
    
    # НОВИЙ КОМПОНЕНТ: Ініціалізація аналітичного двигуна
    economic_engine = EconomicEngine(pool)
    
    # Реєстрація головного роутера з хендлерами
    dp.include_router(router) 

    # 2. Налаштування aiohttp Web
    app = web.Application()
    
    # 3. Зберігання ресурсів у додатку для Dependency Injection
    app["bot"] = bot
    app["dp"] = dp
    app["pool"] = pool
    app["session"] = session
    app["economic_engine"] = economic_engine # Передаємо EconomicEngine

    # 4. Реєстрація залежностей через outer_middleware
    dp.message.outer_middleware.register(lambda handler, event, data: {**data, 'session': session, 'pool': pool, 'bot': bot, 'economic_engine': economic_engine})
    dp.callback_query.outer_middleware.register(lambda handler, event, data: {**data, 'session': session, 'pool': pool, 'bot': bot, 'economic_engine': economic_engine})
    
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
    
    while True:
        await asyncio.sleep(3600) 

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Бот зупинено вручну.")
    except Exception as e:
        logger.critical(f"Глобальна помилка виконання: {e}")

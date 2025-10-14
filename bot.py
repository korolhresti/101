import os
import asyncio
import logging
import re
import random
import sys
import json
import base64
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
from aiogram.methods.set_webhook import SetWebhook

# --- 1. НАЛАШТУВАННЯ СЕРЕДОВИЩА ТА ЛОГУВАННЯ ---

# Встановлюємо часовий пояс для коректної роботи з часом новин
KYIV_TZ = timezone(timedelta(hours=3), 'Europe/Kyiv')

# Налаштування логування
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    stream=sys.stdout)
logger = logging.getLogger(__name__)

# Змінні оточення
BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
# Використовуємо GEMINI_API_KEY для генерації хештегів
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "") 

# Нові змінні для Webhook
WEBHOOK_HOST = os.getenv("WEBHOOK_HOST") # Ваш публічний домен (обов'язково HTTPS)
WEB_SERVER_HOST = os.getenv("WEB_SERVER_HOST", "0.0.0.0")
WEB_SERVER_PORT = int(os.getenv("PORT", 8080))

# Секретний токен для URL вебхука
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "super-secret-key")

# Формування повного URL для вебхука
WEBHOOK_PATH = f"/webhook/{BOT_TOKEN}"
WEBHOOK_URL = f"{WEBHOOK_HOST}{WEBHOOK_PATH}"

# --- 2. КОНФІГУРАЦІЯ БОТА ---

class Config:
    """Конфігурація платформи, зібрана в одному місці."""
    
    # ⚙️ ОСНОВНІ ПАРАМЕТРИ ЦИКЛУ (ОНОВЛЕНО)
    NEWS_FETCH_INTERVAL_MIN = 20 # Кожні 20 хвилин - ШУКАЄМО ТОП-НОВИНИ
    NEWS_POST_INTERVAL_MIN = 5   # Кожні 5 хвилин - ПОСТИМО НОВИНИ З ЧЕРГИ
    MAX_NEWS_PER_CYCLE = 3       # СТРОГИЙ ЛІМІТ: До 3 новин за цикл (ТОП-3)
    MAX_AGE_MIN = 30             # Не публікувати новини старше 30 хвилин
    
    # 🛡️ ПАРАМЕТРИ НАДІЙНОСТІ ТА ПРОДУКТИВНОСТІ
    FETCH_LIMIT = 30             # Макс. кількість записів для аналізу в кожному RSS-фіді
    NUM_SOURCES_TO_FETCH = 20    # Кількість випадкових джерел, які будуть перевірені за цикл
    HTTP_TIMEOUT = 15            # Таймаут HTTP-запиту в секундах
    MAX_CONCURRENCY = 15         # Макс. кількість одночасних HTTP-з'єднань
    
    # 💾 ПАРАМЕТРИ ОБСЛУГОВУВАННЯ БАЗИ ДАНИХ
    DB_CLEANUP_DAYS = 7          # Видаляти новини, старші за 7 днів
    CLEANUP_INTERVAL_HOURS = 1   # Інтервал очищення (кожна година)
    
    DEFAULT_HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36', 
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'uk-UA,uk;q=0.9,en-US;q=0.8,en;q=0.7',
    }

    # 1. 📰 Джерела новин 
    SOURCES = [
        "https://tsn.ua/rss/all.xml", "https://www.pravda.com.ua/rss/news/", 
        "https://censor.net/rss/all_news", "https://www.rbc.ua/static/rss/all.xml",
        "https://www.ukrinform.ua/rss/all.xml", "https://www.liga.net/rss/news.xml",
        "https://www.obozrevatel.com/rss/main.xml", "https://minfin.com.ua/rss/news/",
        "https://focus.ua/rss/latest.xml", "https://ua.korrespondent.net/rss/all",
        "https://gazeta.ua/rss/all", "https://24tv.ua/rss/all.xml",
        "https://nv.ua/ukr/rss/all.xml", "https://delo.ua/rss/all.xml",
        "https://suspilne.media/feed/", "https://www.bbc.com/ukrainian/rss.xml",
        "https://news.finance.ua/ua/rss", "https://www.unian.ua/rss/news.rss", 
        "https://ua.interfax.com.ua/news/ukraine.rss", "https://zaxid.net/rss",
        "https://hromadske.ua/feed/news", "https://biz.censor.net/rss"
    ]


# --- 3. ФУНКЦІЇ ДЛЯ РОБОТИ З БАЗОЮ ДАНИХ ---

async def setup_db_schema(pool: asyncpg.Pool):
    """Створення необхідних таблиць та перевірка схеми."""
    async with pool.acquire() as conn:
        # Таблиця для підписників
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS subscribers (
                id SERIAL PRIMARY KEY,
                chat_id BIGINT UNIQUE NOT NULL,
                subscribed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        """)
        # Таблиця для черги новин (must_post=true) та вже опублікованих
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS news_articles (
                id SERIAL PRIMARY KEY,
                link TEXT UNIQUE NOT NULL,
                title TEXT,
                summary TEXT,
                image_url TEXT,
                published_time TIMESTAMP WITH TIME ZONE,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                is_posted BOOLEAN DEFAULT FALSE,
                posted_at TIMESTAMP WITH TIME ZONE NULL
            );
        """)
    logger.info("Таблиці 'subscribers' та 'news_articles' перевірені/створені.")

async def is_article_exists(pool: asyncpg.Pool, link: str) -> bool:
    """Перевіряє, чи існує стаття з цим посиланням у БД."""
    async with pool.acquire() as conn:
        count = await conn.fetchval(
            "SELECT count(*) FROM news_articles WHERE link = $1", link
        )
        return count > 0

async def store_article(pool: asyncpg.Pool, article_data: Dict[str, Any]):
    """Зберігає нову статтю в БД."""
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO news_articles (link, title, summary, image_url, published_time)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (link) DO NOTHING
        """,
            article_data['link'],
            article_data['title'],
            article_data['summary'],
            article_data['image_url'],
            article_data['published_time']
        )
    logger.debug(f"Збережено статтю: {article_data['title']}")

# --- 4. ФУНКЦІЇ ДЛЯ СКРАПІНГУ ТА ПАРСИНГУ ---

def parse_date(date_str: str) -> Optional[datetime]:
    """Парсить дату з RSS та перетворює її на datetime з часовим поясом Києва."""
    try:
        # feedparser повертає дату як time.struct_time, перетворюємо її
        dt = datetime.fromtimestamp(feedparser._time_parse(date_str), tz=KYIV_TZ)
        return dt
    except Exception as e:
        logger.warning(f"Неможливо розпізнати дату '{date_str}': {e}")
        return None

def find_image_url(soup: BeautifulSoup, base_url: str) -> Optional[str]:
    """Шукає посилання на зображення в HTML за мета-тегами."""
    
    # 1. Шукаємо OpenGraph та Twitter Card мета-теги
    for prop in ['og:image', 'twitter:image']:
        meta = soup.find('meta', property=prop)
        if meta and meta.get('content'):
            img_url = urljoin(base_url, meta['content'])
            if img_url:
                return img_url
    
    # 2. Якщо не знайдено, шукаємо перше велике зображення в body
    # Це менш надійно, але може спрацювати.
    body = soup.find('body')
    if body:
        img_tag = body.find('img', {'loading': 'lazy'}) or body.find('img')
        if img_tag and img_tag.get('src'):
            # Спробуємо відфільтрувати маленькі зображення (наприклад, іконки)
            width = img_tag.get('width')
            height = img_tag.get('height')
            if width and height and int(width) < 200 and int(height) < 200:
                return None
            
            img_url = urljoin(base_url, img_tag['src'])
            # Проста перевірка, що URL не є іконкою
            if not any(ext in img_url.lower() for ext in ['.ico', 'logo', 'icon', 'sprite']):
                return img_url
            
    return None

def find_summary_text(soup: BeautifulSoup) -> Optional[str]:
    """Шукає детальний опис (summary) за мета-тегами."""
    
    # 1. OpenGraph та стандартний description
    for prop in ['og:description', 'description']:
        meta = soup.find('meta', property=prop) or soup.find('meta', attrs={'name': prop})
        if meta and meta.get('content') and len(meta['content']) > 50:
            return meta['content']
            
    return None

async def fetch_page_metadata(link: str, session: ClientSession) -> Tuple[Optional[str], Optional[str]]:
    """Завантажує сторінку статті та скрапить URL зображення та опис."""
    image_url = None
    summary = None
    
    try:
        async with session.get(link, timeout=Config.HTTP_TIMEOUT) as response:
            # Перевіряємо, що ми отримали HTML (або імітуємо його)
            if 'text/html' not in response.headers.get('Content-Type', ''):
                 logger.debug(f"URL {link} не повернув HTML.")
                 return None, None
            
            html = await response.text()
            soup = BeautifulSoup(html, 'html.parser')
            
            # Шукаємо зображення
            image_url = find_image_url(soup, link)
            
            # Шукаємо кращий опис
            summary = find_summary_text(soup)

    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        logger.warning(f"Помилка завантаження сторінки {link}: {e}")
    except Exception as e:
        logger.error(f"Непередбачувана помилка при скрапінгу {link}: {e}", exc_info=True)
        
    return image_url, summary

async def fetch_feed(url: str, pool: asyncpg.Pool, session: ClientSession):
    """Завантажує, парсить та обробляє один RSS-фід."""
    try:
        async with session.get(url, timeout=Config.HTTP_TIMEOUT) as response:
            content = await response.text()
            feed = feedparser.parse(content)
            
            source_domain = urlparse(url).netloc
            new_articles_count = 0

            for entry in feed.entries[:Config.FETCH_LIMIT]:
                link = entry.get('link')
                if not link:
                    continue
                
                # 1. Перевірка дублікатів у БД
                if await is_article_exists(pool, link):
                    logger.debug(f"Пропущено дублікат: {link}")
                    continue

                # 2. Перевірка віку
                published_time_str = entry.get('published')
                published_dt = parse_date(published_time_str)
                
                if published_dt is None:
                    # Якщо дату не можна визначити, вважаємо, що стаття стара, або пропускаємо
                    logger.debug(f"Пропущено статтю без дати: {link}")
                    continue
                
                age_minutes = (datetime.now(KYIV_TZ) - published_dt).total_seconds() / 60
                
                if age_minutes > Config.MAX_AGE_MIN:
                    logger.debug(f"Пропущено стару статтю ({age_minutes:.0f} хв): {link}")
                    continue
                
                # 3. Скрапінг для пошуку фото та кращого опису
                article_title = entry.get('title', 'Без заголовка')
                
                # Використовуємо summary з RSS як fallback
                rss_summary = BeautifulSoup(entry.get('summary', ''), 'html.parser').get_text().strip()[:500] 
                
                image_url, scraped_summary = await fetch_page_metadata(link, session)
                
                # 4. Формування даних та збереження
                article_data = {
                    'link': link,
                    'title': article_title,
                    'summary': scraped_summary if scraped_summary else rss_summary,
                    'image_url': image_url,
                    'published_time': published_dt,
                }
                
                if image_url:
                    await store_article(pool, article_data)
                    new_articles_count += 1
                else:
                    # Пропускаємо статті без зображень, як вимагає користувач ("обов'язково з фото")
                    logger.debug(f"Пропущено статтю без фото: {link}")

            logger.info(f"Оброблено фід {source_domain}. Додано нових статей з фото: {new_articles_count}")

    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        logger.warning(f"Помилка завантаження RSS-фіду {url}: {e}")
    except Exception as e:
        logger.error(f"Критична помилка при обробці фіду {url}: {e}", exc_info=True)


async def fetch_and_store_new_articles(pool: asyncpg.Pool, session: ClientSession):
    """
    Асинхронно завантажує нові статті з RSS-джерел.
    """
    logger.info("Початок процесу пошуку та зберігання нових статей.")
    
    # 1. Вибираємо випадкові джерела
    sources_to_fetch = random.sample(Config.SOURCES, min(Config.NUM_SOURCES_TO_FETCH, len(Config.SOURCES)))
    
    # 2. Обмежуємо одночасні запити
    semaphore = asyncio.Semaphore(Config.MAX_CONCURRENCY)
    
    async def limited_fetch(url):
        async with semaphore:
            await fetch_feed(url, pool, session)

    # 3. Запускаємо паралельне завантаження
    tasks = [limited_fetch(url) for url in sources_to_fetch]
    
    await asyncio.gather(*tasks)
    
    logger.info("Пошук нових статей завершено.")

# --- 4.5. ФУНКЦІЯ ГЕНЕРАЦІЇ ХЕШТЕГІВ ЗА ДОПОМОГОЮ GEMINI API ---

async def extract_hashtags_with_gemini(title: str, summary: str, session: ClientSession) -> List[str]:
    """
    Використовує Gemini API для виділення ключових осіб та місць
    зі статті та генерує відповідні хештеги.
    """
    # Хоча GEMINI_API_KEY може бути порожнім, в Canvas він зазвичай вводиться під час виконання.
    if not GEMINI_API_KEY and not os.getenv("GEMINI_API_KEY"):
        logger.warning("GEMINI_API_KEY не знайдено. Пропускаю генерацію хештегів.")
        return []

    logger.debug("Запит до Gemini API для генерації хештегів...")
    
    # 1. API Конфігурація
    api_key = GEMINI_API_KEY
    api_url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-preview-05-20:generateContent?key={api_key}"
    
    # 2. Системна інструкція та запит
    system_prompt = (
        "Ти — експерт з аналізу новин. Твоє завдання — виділити ключові іменовані сутності "
        "(особи, організації, значущі місця, геополітичні події) з наданого тексту та повернути їх "
        "як список хештегів. Максимум 5 хештегів. "
        "Хештеги мають бути багатослівними, без пробілів, у форматі CamelCase (наприклад, 'ВолодимирЗеленський', 'КиївськаОбласть'). "
        "Повертай результат виключно у форматі JSON відповідно до наданої схеми."
    )
    user_query = f"Проаналізуй наступний заголовок та опис для новинного посту та поверни до 5 найважливіших хештегів, що відповідають ключовим особам та місцям:\n\nЗаголовок: \"{title}\"\nОпис: \"{summary}\""

    # 3. JSON Схема
    response_schema = {
        "type": "OBJECT",
        "properties": {
            "hashtags": {
                "type": "ARRAY",
                "description": "List of key entities (people, organizations, locations) extracted from the text, formatted as multi-word hashtags (e.g., 'ВолодимирЗеленський', 'КиївськаОбласть'). Maximum 5 hashtags.",
                "items": { "type": "STRING" }
            }
        },
        "required": ["hashtags"]
    }

    payload = {
        "contents": [{ "parts": [{ "text": user_query }] }],
        "systemInstruction": { "parts": [{ "text": system_prompt }] },
        "generationConfig": {
            "responseMimeType": "application/json",
            "responseSchema": response_schema
        }
    }
    
    retries = 3
    delay = 1.0
    
    for attempt in range(retries):
        try:
            async with session.post(api_url, 
                                    json=payload, 
                                    timeout=Config.HTTP_TIMEOUT) as response:
                
                if response.status != 200:
                    logger.warning(f"Gemini API - Спроба {attempt+1}: Отримано статус {response.status}")
                    await asyncio.sleep(delay)
                    delay *= 2
                    continue
                    
                result = await response.json()
                
                # Перевірка структури відповіді
                candidate = result.get('candidates', [{}])[0]
                text_part = candidate.get('content', {}).get('parts', [{}])[0].get('text')
                
                if not text_part:
                    logger.warning(f"Gemini API - Спроба {attempt+1}: Порожня відповідь.")
                    await asyncio.sleep(delay)
                    delay *= 2
                    continue
                
                # Парсинг JSON
                try:
                    # Припускаємо, що модель повернула коректний JSON рядок
                    parsed_json = json.loads(text_part)
                    raw_hashtags: List[str] = parsed_json.get('hashtags', [])
                    
                    # Фінальне очищення та форматування в Python
                    final_hashtags = []
                    for h in raw_hashtags:
                        # Прибираємо пробіли та інші неалфавітно-цифрові символи, додаємо #
                        cleaned = re.sub(r'[^а-яА-Яa-zA-Z0-9]', '', h.strip().replace(' ', ''))
                        if cleaned:
                            final_hashtags.append(f"#{cleaned}")

                    return final_hashtags[:5] # Обмеження до 5 хештегів

                except json.JSONDecodeError:
                    logger.error(f"Gemini API - Помилка декодування JSON: {text_part[:100]}...")
                    return []
                    
        except asyncio.TimeoutError:
            logger.warning(f"Gemini API - Спроба {attempt+1}: Таймаут запиту.")
        except aiohttp.ClientError as e:
            logger.error(f"Gemini API - Спроба {attempt+1}: Помилка клієнта: {e}")
        except Exception as e:
            logger.error(f"Непередбачувана помилка при запиті до Gemini: {e}")
            
        await asyncio.sleep(delay)
        delay *= 2 # Експоненційна затримка

    logger.error(f"Gemini API - Не вдалося отримати хештеги після {retries} спроб.")
    return []

async def post_top_unposted_news(bot: Bot, pool: asyncpg.Pool, session: ClientSession) -> int:
    """
    Вибирає та публікує до MAX_NEWS_PER_CYCLE (3) найновіших статей з черги,
    які мають is_posted=FALSE та посилання на фото.
    """
    posted_count = 0
    
    # 1. Вибрати статті з БД
    async with pool.acquire() as conn:
        articles = await conn.fetch("""
            SELECT link, title, summary, image_url, created_at FROM news_articles
            WHERE is_posted = FALSE AND image_url IS NOT NULL
            ORDER BY published_time DESC
            LIMIT $1
        """, Config.MAX_NEWS_PER_CYCLE)

    if not articles:
        logger.info("Черга публікації порожня або немає новин з фото.")
        return 0

    # 2. Отримати список чатів для публікації (УСІ ПІДПИСНИКИ)
    async with pool.acquire() as conn:
        subscriber_ids = [r['chat_id'] for r in await conn.fetch("SELECT chat_id FROM subscribers")]
    
    if not subscriber_ids:
        logger.warning("Немає активних підписників для публікації новин.")
        return 0

    # 3. Публікація
    for article in articles:
        try:
            # Використовуємо MarkdownV2 для коректного форматування в Telegram
            
            # Очищення заголовка та посилання від символів, які можуть порушити Markdown
            def escape_markdown_v2(text):
                # Символи: _, *, [, ], (, ), ~, `, >, #, +, -, =, |, {, }, ., !
                return re.sub(r'([_*\[\]()~`>#+\-=|{}.!])', r'\\\1', text)

            safe_title = escape_markdown_v2(article['title'])
            
            # --- НОВА ЛОГІКА: ГЕНЕРАЦІЯ ХЕШТЕГІВ ---
            # Використовуємо необроблені title/summary для кращого аналізу
            hashtags = await extract_hashtags_with_gemini(article['title'], article['summary'] or '', session)
            hashtags_str = " ".join(hashtags)
            
            safe_summary = escape_markdown_v2(article['summary'] or '...')
            
            # Початковий підпис
            caption_template = (
                f"*{safe_title}*\n\n"
                f"{{summary}}\n\n"
                f"[Читати повністю]({article['link']})\n\n"
                f"{hashtags_str}"
            )
            
            # Обмеження на довжину caption: 1024 символи
            max_caption_len = 1024
            
            # Фіксована довжина нижнього колонтитула (без summary)
            footer_len = len(caption_template.format(summary=''))
            
            # Допустима довжина summary
            max_summary_len = max_caption_len - footer_len - len(safe_title) - 40 
            
            if len(safe_summary) > max_summary_len:
                # Скорочуємо summary
                shortened_summary = safe_summary[:max_summary_len].strip() + '...'
                if len(shortened_summary) < 50: # Забезпечуємо мінімальну довжину
                     shortened_summary = safe_summary[:50].strip() + '...'
            else:
                shortened_summary = safe_summary

            caption = caption_template.format(summary=shortened_summary)


            # Публікація в усі чати
            for chat_id in subscriber_ids:
                await bot.send_photo(
                    chat_id=chat_id,
                    photo=article['image_url'],
                    caption=caption,
                    parse_mode=ParseMode.MARKDOWN_V2
                )
            
            # Оновлення статусу в БД після успішної публікації
            async with pool.acquire() as conn:
                await conn.execute("""
                    UPDATE news_articles SET is_posted = TRUE, posted_at = $1 WHERE link = $2
                """, datetime.now(KYIV_TZ), article['link'])
            
            posted_count += 1
            await asyncio.sleep(1) # Невеликий таймаут між постами для уникнення FloodWait

        except Exception as e:
            logger.error(f"Помилка публікації статті {article['link']}: {e}", exc_info=True)

    return posted_count

async def db_cleanup_cycle(pool: asyncpg.Pool):
    """
    Фонова задача для очищення старих новин з бази даних.
    """
    logger.info("Запуск циклу очищення БД.")
    
    while True:
        try:
            # Визначаємо пороговий час (7 днів тому)
            threshold_time = datetime.now(KYIV_TZ) - timedelta(days=Config.DB_CLEANUP_DAYS)
            
            async with pool.acquire() as conn:
                deleted_count = await conn.execute("""
                    DELETE FROM news_articles WHERE published_time < $1 AND is_posted = TRUE
                """, threshold_time)
                
            match = re.search(r'DELETE (\d+)', deleted_count)
            count = int(match.group(1)) if match else 0
            
            logger.info(f"Очищення БД завершено. Видалено старих опублікованих статей: {count}")

        except asyncio.CancelledError:
            logger.info("Цикл очищення БД скасовано.")
            raise
        except Exception as e:
            logger.error(f"Критична помилка в циклі очищення БД: {e}", exc_info=True)
            
        # Пауза на 1 годину
        await asyncio.sleep(Config.CLEANUP_INTERVAL_HOURS * 3600)


async def scheduled_news_cycle(bot: Bot, pool: asyncpg.Pool, session: ClientSession):
    """
    Фонова задача, яка керує пошуком (20 хв) та публікацією (5 хв) новин.
    """
    logger.info("Запуск запланованого циклу новин. Початковий інтервал пошуку 20 хв, публікації 5 хв.")
    
    # Встановлюємо час останнього пошуку на минуле, щоб змусити перший пошук
    last_fetch_time = datetime.now(KYIV_TZ) - timedelta(minutes=Config.NEWS_FETCH_INTERVAL_MIN + 1)
    
    while True:
        try:
            current_time = datetime.now(KYIV_TZ)
            
            # --- 1. ЛОГІКА ПОШУКУ (КОЖНІ 20 ХВИЛИН) ---
            if current_time >= last_fetch_time + timedelta(minutes=Config.NEWS_FETCH_INTERVAL_MIN):
                logger.info(f"Настав час для пошуку нових статей. Інтервал: {Config.NEWS_FETCH_INTERVAL_MIN} хв.")
                await fetch_and_store_new_articles(pool, session)
                last_fetch_time = datetime.now(KYIV_TZ)
                logger.info("Пошук нових статей завершено.")
                
            # --- 2. ЛОГІКА ПУБЛІКАЦІЇ (КОЖНІ 5 ХВИЛИН) ---
            logger.info(f"Настав час для публікації. Інтервал: {Config.NEWS_POST_INTERVAL_MIN} хв.")
            posted_count = await post_top_unposted_news(bot, pool, session)
            logger.info(f"Цикл публікації завершено. Опубліковано новин: {posted_count}")

        except asyncio.CancelledError:
            # Обробка скасування задачі при зупинці сервера
            logger.info("Запланований цикл новин скасовано.")
            raise
        except Exception as e:
            logger.error(f"Критична помилка в запланованому циклі новин: {e}", exc_info=True)
            
        # Пауза на 5 хвилин (інтервал публікації)
        await asyncio.sleep(Config.NEWS_POST_INTERVAL_MIN * 60)

# --- 5. ХЕНДЛЕРИ ТА ЛОГІКА WEBHOOK ---

async def handle_webhook(request):
    """Обробка вхідного вебхука від Telegram."""
    try:
        data = await request.json()
        dp: Dispatcher = request.app["dp"]
        await dp.feed_raw_update(request.app["bot"], data)
        return web.Response()
    except Exception as e:
        logger.error(f"Помилка обробки вебхука: {e}")
        return web.Response(status=200) # Telegram очікує 200 ОК


# --- 6. ХЕНДЛЕРИ КОМАНД (ПРИКЛАД) ---

# Додайте хендлери для команд тут, наприклад /start
async def command_start_handler(message: types.Message, pool: asyncpg.Pool):
    """Обробляє команду /start та додає користувача до підписників."""
    chat_id = message.chat.id
    try:
        # Перевіряємо, чи це приватний чат чи канал/група
        if message.chat.type in ['group', 'supergroup', 'channel']:
            # Для каналів або груп ми не підписуємо їх, бо бот поститиме туди сам.
            await message.answer("Ця команда призначена для підписки в приватних чатах. Якщо ви хочете, щоб бот публікував новини в цьому каналі/групі, його потрібно додати як адміністратора.")
            return

        async with pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO subscribers (chat_id) VALUES ($1)
                ON CONFLICT (chat_id) DO NOTHING
            """, chat_id)
        await message.answer(
            "Привіт! 👋 Ви успішно підписалися на розсилку ТОП-новин. "
            "Я буду надсилати вам до 3 найсвіжіших статей кожні 5 хвилин (за наявності). "
            "Використовуйте /stop, щоб скасувати підписку."
        )
    except Exception as e:
        logger.error(f"Помилка при обробці /start для {chat_id}: {e}", exc_info=True)
        await message.answer("Виникла помилка при підписці. Спробуйте пізніше.")

async def command_stop_handler(message: types.Message, pool: asyncpg.Pool):
    """Обробляє команду /stop та видаляє користувача з підписників."""
    chat_id = message.chat.id
    try:
        async with pool.acquire() as conn:
            result = await conn.execute("""
                DELETE FROM subscribers WHERE chat_id = $1
            """, chat_id)
        
        if result == 'DELETE 1':
            await message.answer("Ви успішно відписалися від розсилки новин. Дякуємо!")
        else:
             await message.answer("Ви вже не були підписані на розсилку.")
             
    except Exception as e:
        logger.error(f"Помилка при обробці /stop для {chat_id}: {e}", exc_info=True)
        await message.answer("Виникла помилка при відписці. Спробуйте пізніше.")


# --- 7. ФУНКЦІЇ ЗАПУСКУ ТА ВИМКНЕННЯ ---

async def on_startup(app: web.Application):
    """Виконується при запуску Webhook сервера."""
    logger.info("Запуск on_startup...")
    bot: Bot = app["bot"]
    pool: asyncpg.Pool = app["pool"]
    session: ClientSession = app["session"]

    # 1. Створення/перевірка схеми БД
    await setup_db_schema(pool)
    
    # 2. Встановлення вебхука
    try:
        await bot.set_webhook(url=WEBHOOK_URL, secret_token=WEBHOOK_SECRET)
        logger.info(f"Встановлення Webhook на URL: {WEBHOOK_URL} - Успішно.")
    except Exception as e:
        logger.error(f"Помилка встановлення вебхука: {e}")
        
    # 3. Запуск запланованих циклів (ВАЖЛИВО!)
    # Фонова задача публікації/пошуку новин
    app["news_task"] = asyncio.create_task(scheduled_news_cycle(bot, pool, session))
    # Фонова задача очищення БД
    app["cleanup_task"] = asyncio.create_task(db_cleanup_cycle(pool))
    
    logger.info("Запуск on_startup... Успішно. Запущено 2 фонові задачі.")


async def on_shutdown(app: web.Application):
    """Виконується при зупинці Webhook сервера."""
    logger.info("Запуск on_shutdown...")
    bot: Bot = app["bot"]
    pool: asyncpg.Pool = app["pool"]
    session: ClientSession = app["session"]

    # 1. Відміна фонових задач (ВАЖЛИВО!)
    if "news_task" in app:
        app["news_task"].cancel()
    if "cleanup_task" in app:
        app["cleanup_task"].cancel()
        
    # Чекаємо завершення скасованих задач
    tasks_to_wait = []
    if "news_task" in app: tasks_to_wait.append(app["news_task"])
    if "cleanup_task" in app: tasks_to_wait.append(app["cleanup_task"])
    
    if tasks_to_wait:
        await asyncio.gather(*tasks_to_wait, return_exceptions=True)
        logger.info("Всі фонові задачі скасовано.")
        
    # 2. Видалення вебхука
    await bot.delete_webhook()
    logger.info("Webhook видалено.")

    # 3. Закриття пулу з'єднань з БД
    await pool.close()
    logger.info("Пул з'єднань з БД закрито.")
    
    # 4. Закриття HTTP сесії
    await session.close()
    logger.info("HTTP сесію закрито.")
    
    logger.info("Запуск on_shutdown... Успішно.")


async def main():
    """Основна точка входу для застосунку Webhook."""
    if not BOT_TOKEN or not DATABASE_URL or not WEBHOOK_HOST:
        logger.error("Необхідні змінні оточення (BOT_TOKEN, DATABASE_URL, WEBHOOK_HOST) не встановлені.")
        sys.exit(1)

    # Ініціалізація компонентів
    bot = Bot(
        token=BOT_TOKEN,
        # Встановлюємо ParseMode.MARKDOWN_V2 для кращого контролю форматування
        default=DefaultBotProperties(parse_mode=ParseMode.MARKDOWN_V2) 
    )
    dp = Dispatcher()
    
    # Реєстрація хендлерів
    dp.message.register(command_start_handler, Command("start"))
    dp.message.register(command_stop_handler, Command("stop")) # Додано /stop

    # Створення пулу з'єднань з БД
    pool = await asyncpg.create_pool(DATABASE_URL)
    
    # Створення HTTP сесії
    # trust_env=True дозволяє використовувати налаштування проксі з середовища (важливо для деяких хостингів)
    session = aiohttp.ClientSession(headers=Config.DEFAULT_HEADERS, trust_env=True)
    
    # 2. Налаштування aiohttp Web
    app = web.Application()
    
    # 3. Зберігання ресурсів у додатку
    app["bot"] = bot
    app["dp"] = dp
    app["pool"] = pool
    app["session"] = session
    
    # 4. Реєстрація залежностей для хендлерів DP
    # Впровадження залежностей (pool та session) у хендлери
    # Додаємо session, pool, bot в контекст
    dp.message.outer_middleware.register(lambda handler, event, data: {**data, 'session': session, 'pool': pool})
    dp.callback_query.outer_middleware.register(lambda handler, event, data: {**data, 'session': session, 'pool': pool})
    
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
    
    # Тримаємо основний потік відкритим, поки не буде сигналу зупинки
    await asyncio.Event().wait()


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Бот зупинено вручну.")
    except Exception as e:
        logger.error(f"Головна помилка: {e}", exc_info=True)

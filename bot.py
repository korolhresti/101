import os
import asyncio
import logging
import re
import random
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse, urljoin

# Необхідні бібліотеки
import asyncpg
import aiohttp
import feedparser
from bs4 import BeautifulSoup
from PIL import Image, ImageDraw, ImageFont # 🎨 НОВА БІБЛІОТЕКА ДЛЯ WATERMARK
from io import BytesIO # Для роботи з зображеннями в пам'яті

from aiogram import Bot, Dispatcher, types, F
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.client.default import DefaultBotProperties 
from aiogram.exceptions import TelegramAPIError

# --- 1. НАЛАШТУВАННЯ І КОНСТАНТИ (ПРОФЕСІЙНА КОНФІГУРАЦІЯ) ---

# Використовуйте Kyiv time zone (UTC+3)
KYIV_TZ = timezone(timedelta(hours=3), 'Europe/Kyiv')
# Формат часу для відображення
TIME_FORMAT = "%H:%M" 
# Шлях до шрифту для Watermark (ВИ ПОВИННІ ЙОГО ДОДАТИ! Наприклад, Arial.ttf)
# Якщо не буде знайдено, буде використовуватись дефолтний PIL-шрифт.
FONT_PATH = "arial.ttf" 

class Config:
    """Конфігурація платформи, зібрана в одному місці."""
    
    # ⚙️ ОСНОВНІ ПАРАМЕТРИ ЦИКЛУ (Рекомендовані для стабільної роботи)
    POSTING_INTERVAL_MIN = 5  # Кожні 5 хвилин
    FETCH_INTERVAL_MIN = 5    # Кожні 5 хвилин (винесено окремо для динаміки)
    MAX_NEWS_PER_CYCLE = 3   # СТРОГИЙ ЛІМІТ: До 3 новин за цикл (ТОП-3)
    MAX_AGE_MIN = 45          # Не публікувати новини старше 45 хвилин (Збільшено для більшої вибірки)
    DAILY_DIGEST_HOUR = 21    # Година публікації Дайджесту (21:00)
    DAILY_DIGEST_LIMIT = 5    # Кількість новин у Дайджесті

    # 🛡️ ПАРАМЕТРИ НАДІЙНОСТІ ТА ПРОДУКТИВНОСТІ
    FETCH_LIMIT = 50          # Макс. кількість записів для аналізу в кожному RSS-фіді (Збільшено)
    NUM_SOURCES_TO_FETCH = 25 # Кількість випадкових джерел, які будуть перевірені за цикл (Збільшено)
    HTTP_TIMEOUT = 15         # Таймаут HTTP-запиту в секундах
    MAX_CONCURRENCY = 20      # Макс. кількість одночасних HTTP-з'єднань (Збільшено для швидкості)
    TELEGRAM_POST_DELAY = 2   # Затримка між постами в Telegram (сек)

    # 💾 ПАРАМЕТРИ ОБСЛУГОВУВАННЯ БАЗИ ДАНИХ
    DB_CLEANUP_DAYS = 14      # Видаляти новини, старші за 14 днів (Збільшено для статистики дайджесту)
    CLEANUP_INTERVAL_HOURS = 2 # Інтервал очищення (кожні 2 години)
    
    # 🖼️ ПАРАМЕТРИ WATERMARK
    DEFAULT_WATERMARK = "@YourChannelName" # СТАНДАРТНИЙ ТЕКСТ
    DEFAULT_CTA = "👉 Підписатись на @YourChannelName" # СТАНДАРТНИЙ ЗАКЛИК ДО ДІЇ
    
    DEFAULT_HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36 Bot/1.0', 
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'uk-UA,uk;q=0.9,en-US;q=0.8,en;q=0.7',
    }

    # 1. 📰 Джерела новин (Розширено)
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
        "https://hromadske.ua/feed/news", "https://biz.censor.net/rss",
        "https://apostrophe.ua/rss/all", "https://espreso.tv/rss.xml" # Додано нові джерела
    ]

class BotState:
    """Динамічні налаштування та стан бота."""
    def __init__(self):
        self.watermark_text = Config.DEFAULT_WATERMARK
        self.cta_text = Config.DEFAULT_CTA
        self.watermark_enabled = True
        self.posting_interval_min = Config.POSTING_INTERVAL_MIN
        self.fetch_interval_min = Config.FETCH_INTERVAL_MIN
        self.disabled_sources = set()
        self.last_digest_date = datetime.now(KYIV_TZ).date() - timedelta(days=1) # Щоб запустився при першому старті

# Ініціалізація глобального стану
bot_state = BotState()

# Налаштування логування
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Змінні середовища
BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
CHANNEL_ID = os.getenv("CHANNEL_ID")
try:
    ADMIN_ID = int(os.getenv("ADMIN_ID", 0))
except ValueError:
    ADMIN_ID = 0

# Глобальні змінні для DB
db_pool = None
dp: Dispatcher = None
bot: Bot = None

# --- 2. БАЗА ДАНИХ (POSTGRESQL/NEON) ---
# ... (connect_db, init_db, save_news_to_db, mark_news_as_posted, cleanup_db, get_db_stats залишено без змін,
# крім додавання is_digested до init_db та оновлення get_unique_news_from_db для дайджесту)

async def connect_db():
    """Створює пул з'єднань до бази даних Neon (PostgreSQL)."""
    global db_pool
    try:
        db_pool = await asyncpg.create_pool(
            DATABASE_URL,
            min_size=1, 
            max_size=10,
            timeout=5
        )
        logger.info("✅ Успішно підключено до Neon PostgreSQL.")
    except Exception as e:
        logger.error(f"❌ Критична помилка підключення до DB: {e}")
        await asyncio.sleep(60)
        exit(1)

async def init_db():
    """Створює таблицю 'news', якщо вона не існує, та додає необхідні стовпці."""
    if not db_pool:
        return

    async with db_pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS news (
                id SERIAL PRIMARY KEY,
                source VARCHAR(255) NOT NULL,
                url TEXT UNIQUE NOT NULL,
                title TEXT NOT NULL,
                summary TEXT,
                image_url TEXT,
                published_at TIMESTAMP WITH TIME ZONE NOT NULL,
                inserted_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        try:
            await conn.execute("ALTER TABLE news ADD COLUMN is_posted BOOLEAN DEFAULT FALSE;")
        except asyncpg.exceptions.DuplicateColumnError:
            pass 
        
        try:
            await conn.execute("ALTER TABLE news ADD COLUMN is_digested BOOLEAN DEFAULT FALSE;") # НОВЕ: для дайджесту
        except asyncpg.exceptions.DuplicateColumnError:
            pass 
        
        try:
            await conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS news_url_idx ON news (url);")
        except Exception as e:
            logger.error(f"Помилка при створенні індексу: {e}")

    logger.info("Таблиця 'news' перевірена/оновлена.")


async def save_news_to_db(news_items: list):
    """Зберігає список новин у базу даних, використовуючи пакетну вставку."""
    if not news_items or not db_pool:
        return 0
    
    # Додано стовпець is_digested зі значенням FALSE
    sql = """
        INSERT INTO news (source, url, title, summary, image_url, published_at, is_digested)
        SELECT * FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::timestamptz[], $7::boolean[])
        ON CONFLICT (url) DO NOTHING
        RETURNING id;
    """
    
    sources = [item['source'] for item in news_items]
    urls = [item['url'] for item in news_items]
    titles = [item['title'] for item in news_items]
    summaries = [item['summary'] for item in news_items]
    image_urls = [item['image_url'] for item in news_items]
    published_at_list = [item['published_at'] for item in news_items]
    is_digested_list = [False] * len(news_items) # Початкове значення
    
    try:
        async with db_pool.acquire() as conn:
            result = await conn.fetch(sql, sources, urls, titles, summaries, image_urls, published_at_list, is_digested_list)
            return len(result)
    except asyncpg.exceptions.PostgresError as e:
        logger.error(f"❌ Помилка пакетної вставки в БД: {e}")
        return 0

async def get_unique_news_from_db(limit: int) -> list:
    """Вибирає ТОП-N найновіших, ще не опублікованих новин ТІЛЬКИ З КАРТИНКОЮ."""
    if not db_pool:
        return []

    sql = """
        SELECT url, title, summary, image_url, source, published_at
        FROM news
        WHERE is_posted = FALSE
          AND image_url IS NOT NULL AND image_url != '' 
        ORDER BY 
            published_at DESC 
        LIMIT $1;
    """
    try:
        async with db_pool.acquire() as conn:
            records = await conn.fetch(sql, limit)
            return [dict(record) for record in records]
    except asyncpg.exceptions.PostgresError as e:
        logger.error(f"❌ Помилка вибірки з БД: {e}")
        return []

async def get_daily_digest_news(limit: int) -> list:
    """Вибирає N найкращих новин за останню добу, які ще НЕ були у дайджесті."""
    if not db_pool:
        return []

    # Вибираємо найновіші, ще не опубліковані у дайджесті, за останню добу
    start_time = datetime.now(KYIV_TZ) - timedelta(hours=24)
    
    sql = """
        SELECT title, url
        FROM news
        WHERE published_at >= $1
          AND is_digested = FALSE
        ORDER BY 
            published_at DESC -- Можна змінити на ORDER BY RANDOM() або складніший рейтинг
        LIMIT $2;
    """
    try:
        async with db_pool.acquire() as conn:
            records = await conn.fetch(sql, start_time, limit)
            return [dict(record) for record in records]
    except asyncpg.exceptions.PostgresError as e:
        logger.error(f"❌ Помилка вибірки дайджесту з БД: {e}")
        return []

async def mark_news_as_posted(urls: list, is_digested=False):
    """Позначає новини, що були успішно опубліковані (або у дайджесті)."""
    if not urls or not db_pool:
        return
        
    field_to_update = "is_digested" if is_digested else "is_posted"
    sql = f"""
        UPDATE news
        SET {field_to_update} = TRUE
        WHERE url = ANY($1::text[]);
    """
    try:
        async with db_pool.acquire() as conn:
            await conn.execute(sql, urls)
    except asyncpg.exceptions.PostgresError as e:
        logger.error(f"❌ Помилка оновлення {field_to_update} в БД: {e}")

async def cleanup_db():
    # ... (Cleanup без змін)
    if not db_pool:
        return
        
    cleanup_time = datetime.now(KYIV_TZ) - timedelta(days=Config.DB_CLEANUP_DAYS)
    
    sql = """
        DELETE FROM news
        WHERE inserted_at < $1;
    """
    try:
        async with db_pool.acquire() as conn:
            deleted_count = await conn.execute(sql, cleanup_time)
            match = re.search(r'DELETE (\d+)', deleted_count)
            count = int(match.group(1)) if match else 0
            logger.info(f"🧹 Обслуговування DB: Видалено {count} старих записів.")
            return count
    except asyncpg.exceptions.PostgresError as e:
        logger.error(f"❌ Помилка очистки БД: {e}")
        return 0

async def get_db_stats():
    # ... (Статистика оновлена)
    if not db_pool:
        return None
        
    sql = """
        SELECT 
            (SELECT count(*) FROM news) AS total_news,
            (SELECT count(*) FROM news WHERE is_posted = TRUE) AS posted_news,
            (SELECT count(*) FROM news WHERE is_posted = FALSE AND image_url IS NOT NULL AND image_url != '') AS unposted_photo_news,
            (SELECT count(*) FROM news WHERE is_digested = TRUE) AS digested_news,
            (SELECT count(DISTINCT source) FROM news) AS total_sources;
    """
    try:
        async with db_pool.acquire() as conn:
            record = await conn.fetchrow(sql)
            return dict(record) if record else None
    except asyncpg.exceptions.PostgresError as e:
        logger.error(f"❌ Помилка отримання статистики з БД: {e}")
        return None

# --- 3. ХЕЛПЕРИ ПАРСИНГУ ---

# Розширений список стоп-слів
def is_news_relevant(title: str, summary: str) -> bool:
    """Перевіряє, чи не стосується новина заблокованих тем (зірки, футбол, гороскопи, тощо)."""
    if not title and not summary:
        return False
        
    text = (title + " " + summary).lower()
    
    irrelevant_keywords = [
        "зірок", "зірка", "шоу-бізнес", "світське життя", "особисте життя", 
        "вагітність", "розлучення", "скандал", "тсн.особливе", "телебачення", 
        "кіно", "мода", "гламур", "голлівуд", "селебриті", # Зірки
        "футбол", "матч", "ліга чемпіонів", "ліга європи", "прем'єр-ліга",
        "динамо", "шахтар", "фк ", "борусія", "реал", "барселона", # Спорт
        "гороскоп", "прогноз погоди", "рецепт", "порада", "кулінарія", "догляд", # Сміття
        "прикмета", "сонник" # Містика
    ]
    
    for keyword in irrelevant_keywords:
        # Перевірка на ціле слово для уникнення хибних спрацювань усередині слів
        if re.search(r'\b' + re.escape(keyword) + r'\b', text):
            logger.debug(f"Пропущено новину (НЕРЕЛЕВАНТ): {title[:50]}... ({keyword})")
            return False
            
    return True
    
def normalize_summary(text: str) -> str:
    """Очищає та нормалізує текст анотації."""
    if not text:
        return ""
    soup = BeautifulSoup(text, 'html.parser')
    clean_text = soup.get_text()
    clean_text = ' '.join(clean_text.split())
    return clean_text[:600].strip() # Збільшено до 600 символів

# extract_image_url та parse_published_time залишено без змін

# --- 4. ОСНОВНИЙ ПАРСИНГ ---

# Оновлення логіки для відстеження недоступності стрічок
async def fetch_and_parse_source(session, rss_url: str):
    """Парсить одне джерело."""
    news_items = []
    source_domain = urlparse(rss_url).netloc.replace('www.', '')
    
    if source_domain in bot_state.disabled_sources:
        logger.debug(f"⚠️ Пропущено вимкнене джерело: {source_domain}")
        return []

    try:
        async with session.get(rss_url, headers=Config.DEFAULT_HEADERS, timeout=Config.HTTP_TIMEOUT) as response:
            if response.status != 200:
                await send_admin_notification(
                    f"🔔 **Попередження RSS**: Джерело `{source_domain}` повернуло HTTP-помилку {response.status}.", is_error=False
                )
                return []
            
            content = await response.text(encoding=response.charset or 'utf-8')
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        await send_admin_notification(
            f"❌ **Помилка RSS**: Джерело `{source_domain}` недоступне: {type(e).__name__}", is_error=True
        )
        return []

    feed = feedparser.parse(content)
    now_kyiv = datetime.now(KYIV_TZ)
    max_age_dt = timedelta(minutes=Config.MAX_AGE_MIN) 

    for entry in feed.entries[:Config.FETCH_LIMIT]:
        # ... (логіка парсингу запису залишена без змін)
        try:
            url = entry.link
            title = entry.title
            summary = normalize_summary(entry.get('summary') or entry.get('description') or entry.title)
            
            if not is_news_relevant(title, summary):
                continue

            image_url = extract_image_url(entry)
            published_time = parse_published_time(entry, rss_url)

            if now_kyiv - published_time > max_age_dt:
                continue

            news_items.append({
                'source': source_domain, 'title': title, 'url': url, 
                'summary': summary, 'image_url': image_url, 'published_at': published_time,
            })
        except Exception as e:
            logger.warning(f"Помилка обробки запису з {rss_url}: {e}")
            continue
            
    return news_items

async def fetch_all_sources():
    """Асинхронно отримує новини з випадково обраних джерел."""
    all_news = []
    start_time = datetime.now()

    # Оновлення: вибираємо тільки активні джерела
    active_sources = [url for url in Config.SOURCES if urlparse(url).netloc.replace('www.', '') not in bot_state.disabled_sources]
    
    num_sources_to_fetch = min(Config.NUM_SOURCES_TO_FETCH, len(active_sources)) 
    selected_sources = random.sample(active_sources, num_sources_to_fetch)
    
    logger.info(f"⏳ Парсинг {len(selected_sources)} випадкових джерел (Активних: {len(active_sources)})...")

    connector = aiohttp.TCPConnector(limit=Config.MAX_CONCURRENCY)
    async with aiohttp.ClientSession(headers=Config.DEFAULT_HEADERS, connector=connector) as session:
        tasks = [fetch_and_parse_source(session, rss_url) for rss_url in selected_sources]
        results = await asyncio.gather(*tasks)

        for news_list in results:
            if news_list:
                all_news.extend(news_list)

    duration = (datetime.now() - start_time).total_seconds()
    logger.info(f"📰 Знайдено {len(all_news)} новин з {len(selected_sources)} джерел за {duration:.2f} сек.")
    
    return all_news, duration

# --- 5. ХЕЛПЕРИ ДЛЯ ЗОБРАЖЕНЬ ТА ТЕКСТУ ---

def generate_hashtags(title: str, summary: str) -> str:
    """Генерує до 3-х релевантних хештегів на основі ключових слів."""
    text = (title + " " + summary).lower()
    
    # Розширений словник для хештегів: 'ключове_слово': 'хештег_без_пробілів'
    keyword_map = {
        'війна': '#Війна', 'рф': '#Війна', 'росія': '#Війна', 'обстріл': '#Війна', 'фронт': '#Фронт',
        'зеленський': '#Зеленський', 'президент': '#Політика', 'парламент': '#Політика', 'рада': '#Політика',
        'долар': '#Фінанси', 'гривня': '#Фінанси', 'банк': '#Фінанси', 'економіка': '#Економіка', 'ціни': '#Економіка',
        'єс': '#Європа', 'сша': '#США', 'україна': '#Україна', 'київ': '#Київ',
        'суд': '#Право', 'кримінал': '#Право', 'поліція': '#Право', 'закон': '#Право',
        'технології': '#Техно', 'наука': '#Наука', 'медицина': '#Медицина'
    }
    
    # Використовуємо set для уникнення дублікатів і гарантії унікальності
    found_hashtags = set()
    
    for keyword, hashtag in keyword_map.items():
        # Регулярний вираз для пошуку слова, що закінчується на ключове слово (у різних відмінках)
        # Наприклад, 'війні', 'війною' тощо.
        if re.search(r'\b' + re.escape(keyword) + r'\w*\b', text): 
            found_hashtags.add(hashtag)
        
        if len(found_hashtags) >= 3:
            break
            
    return " ".join(list(found_hashtags))

def get_post_emoji(hashtags: str) -> str:
    """Вибирає тематичний емодзі для початку посту."""
    if '#Війна' in hashtags: return '🛡️'
    if '#Фінанси' in hashtags or '#Економіка' in hashtags: return '💰'
    if '#Політика' in hashtags: return '🏛️'
    if '#Європа' in hashtags or '#США' in hashtags: return '🌍'
    return '📰'

async def apply_watermark(image_url: str, text: str) -> BytesIO | None:
    """Завантажує зображення, додає водяний знак і повертає його у форматі BytesIO."""
    if not bot_state.watermark_enabled:
        return None
        
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(image_url, headers=Config.DEFAULT_HEADERS, timeout=Config.HTTP_TIMEOUT) as response:
                response.raise_for_status()
                image_bytes = await response.read()

        img = Image.open(BytesIO(image_bytes)).convert("RGBA")
        width, height = img.size

        # Визначаємо розмір шрифту (1/20 від висоти зображення)
        font_size = max(18, height // 20)
        
        try:
            # Спроба завантажити професійний шрифт
            font = ImageFont.truetype(FONT_PATH, font_size)
        except IOError:
            # Використання дефолтного шрифту, якщо професійний не знайдено
            font = ImageFont.load_default()
            logger.warning("❌ Не знайдено FONT_PATH. Використано дефолтний шрифт.")
        
        # Створення прозорого шару для водяного знаку
        watermark_layer = Image.new('RGBA', (width, height), (255, 255, 255, 0))
        draw = ImageDraw.Draw(watermark_layer)

        # Визначення позиції (правий нижній кут)
        text_bbox = draw.textbbox((0, 0), text, font=font)
        text_width = text_bbox[2] - text_bbox[0]
        text_height = text_bbox[3] - text_bbox[1]
        
        margin = 10
        position = (width - text_width - margin, height - text_height - margin)

        # Малюємо контур (тінь) для кращої читабельності
        shadow_color = (0, 0, 0, 150) # Напівпрозорий чорний
        main_color = (255, 255, 255, 255) # Білий
        
        # Додавання тіні
        for offset in [(1, 1), (-1, 1), (1, -1), (-1, -1)]:
            draw.text((position[0] + offset[0], position[1] + offset[1]), text, font=font, fill=shadow_color)
            
        # Додавання основного тексту
        draw.text(position, text, font=font, fill=main_color)

        # Комбінування зображення та водяного знаку
        watermarked_img = Image.alpha_composite(img.convert('RGBA'), watermark_layer)
        watermarked_img = watermarked_img.convert("RGB") # Telegram не підтримує PNG з альфа-каналом для photo

        # Збереження у BytesIO
        output = BytesIO()
        watermarked_img.save(output, format="JPEG", quality=90)
        output.seek(0)
        return output

    except Exception as e:
        logger.error(f"❌ Помилка обробки Watermark для {image_url}: {e}")
        return None

# --- 6. ФОРМАТУВАННЯ ТА ПОСТИНГ ---

def format_news_post(news_item: dict) -> str:
    """Форматує новину для публікації у Telegram (HTML) з часом публікації, CTA та хештегами."""
    source_display = news_item['source'].replace('https://', '').replace('http://', '')
    published_time_str = news_item['published_at'].strftime(TIME_FORMAT)
    
    hashtags = generate_hashtags(news_item['title'], news_item['summary'])
    emoji = get_post_emoji(hashtags)
    
    # Використання f-рядків для кращої читабельності
    message = (
        f"{emoji} <b>{news_item['title']}</b>\n\n"
        f"{news_item['summary']}\n\n"
        f"🕰️ {published_time_str} | <a href='{news_item['url']}'>Подробиці на {source_display}</a>\n"
        f"{hashtags}\n\n"
        f"<i>{bot_state.cta_text}</i>"
    )
    return message

def format_digest_post(news_list: list) -> str:
    """Форматує щоденний дайджест."""
    today = datetime.now(KYIV_TZ).strftime("%d.%m.%Y")
    
    header = f"🏆 **ТОП-{len(news_list)} НОВИН ЗА ДОБУ** ({today}) 🏆\n\n"
    
    items = []
    for i, news in enumerate(news_list):
        items.append(f"{i+1}. <a href='{news['url']}'>{news['title']}</a>")
        
    footer = f"\n\n*Професійна платформа новин. {bot_state.cta_text}*"
    
    return header + "\n".join(items) + footer

async def send_news_to_channel(news_to_post: list):
    """Публікує новини у канал з обробкою водяного знаку та помилок."""
    
    posted_urls = []
    
    for news in news_to_post[:Config.MAX_NEWS_PER_CYCLE]:
        caption = format_news_post(news)
        image_url = news.get('image_url')
        post_successful = False
        
        try:
            # 1. Спроба обробки зображення (Watermark)
            watermarked_photo = None
            if image_url:
                watermarked_photo = await apply_watermark(image_url, bot_state.watermark_text)

            # 2. Публікація (з Watermark або без)
            if watermarked_photo:
                await bot.send_photo(
                    chat_id=CHANNEL_ID,
                    photo=types.BufferedInputFile(watermarked_photo.getvalue(), filename="news_photo.jpg"),
                    caption=caption,
                    parse_mode=ParseMode.HTML,
                    disable_notification=False 
                )
                post_successful = True
            elif image_url:
                # Якщо Watermark не вдалося накласти, спробуємо оригінальний URL
                await bot.send_photo(
                    chat_id=CHANNEL_ID,
                    photo=image_url,
                    caption=caption,
                    parse_mode=ParseMode.HTML,
                    disable_notification=False
                )
                post_successful = True
            
            # Якщо немає фото, або обидві спроби з фото провалилися, надсилаємо ТЕКСТ
            if not post_successful and (not image_url or image_url): # Використовуємо останній 'else' для текстового посту
                 await bot.send_message(
                    chat_id=CHANNEL_ID,
                    text=caption,
                    parse_mode=ParseMode.HTML,
                    disable_notification=False
                )
                 post_successful = True
                 logger.warning(f"-> Опубліковано як ТЕКСТ через відсутність/помилку фото: {news['title'][:50]}...")
                
            posted_urls.append(news['url'])
            
            # Затримка для уникнення FloodWait
            await asyncio.sleep(Config.TELEGRAM_POST_DELAY) 
            
        except TelegramAPIError as e:
            logger.error(f"❌ Telegram API Error для '{news['title'][:50]}...': {e.message}")
            if "Too Many Requests" in e.message:
                # Автоматичне очікування, якщо Telegram просить
                wait_time = int(re.search(r'retry after (\d+)', e.message).group(1)) if re.search(r'retry after (\d+)', e.message) else 30
                logger.warning(f"-> Telegram попросив зачекати {wait_time} сек. Чекаємо...")
                await asyncio.sleep(wait_time)
            
            # Позначаємо як опубліковану (навіть при помилці фото), щоб не спамити.
            if "Bad Request: failed to get HTTP URL content" in e.message or "Bad Request: PHOTO_INVALID" in e.message:
                logger.warning("-> Проблема з URL/форматом зображення. Позначаємо як опубліковану.")
                posted_urls.append(news['url']) 
                continue # Переходимо до наступної новини
            
            continue # Для інших помилок (не flood wait), просто переходимо до наступної новини
            
        except Exception as e:
            await send_admin_notification(
                f"❌ Критична помилка постингу: {type(e).__name__} для '{news['title'][:50]}...'", is_error=True
            )
            continue 

    await mark_news_as_posted(posted_urls)
    return len(posted_urls)

async def send_daily_digest():
    """Публікує щоденний дайджест о 21:00."""
    
    # 1. Отримуємо новини для дайджесту
    news_list = await get_daily_digest_news(Config.DAILY_DIGEST_LIMIT)
    
    if not news_list:
        logger.info("ℹ️ Немає новин для щоденного дайджесту.")
        return

    # 2. Форматуємо та надсилаємо
    digest_message = format_digest_post(news_list)
    try:
        await bot.send_message(
            chat_id=CHANNEL_ID,
            text=digest_message,
            parse_mode=ParseMode.HTML,
            disable_notification=False 
        )
        # 3. Позначаємо як опубліковані
        digest_urls = [news['url'] for news in news_list]
        await mark_news_as_posted(digest_urls, is_digested=True)
        
        await send_admin_notification(
            f"✅ **Дайджест успішно надіслано**: Опубліковано {len(news_list)} новин.", is_error=False
        )
        logger.info(f"🏆 Щоденний дайджест успішно надіслано ({len(news_list)} новин).")
        
    except TelegramAPIError as e:
        logger.error(f"❌ Помилка Telegram при відправці дайджесту: {e.message}")
        await send_admin_notification(
            f"❌ Критична помилка при відправці дайджесту: {e.message}", is_error=True
        )
    except Exception as e:
        logger.critical(f"❌ Критична помилка в логіці дайджесту: {e}")
        await send_admin_notification(
            f"❌ Критична помилка в логіці дайджесту: {type(e).__name__}", is_error=True
        )


async def send_admin_notification(message: str, is_error: bool = False):
    """Надсилає повідомлення адміністратору."""
    if not ADMIN_ID or not bot:
        return
        
    icon = "🔥 КРИТИЧНА ПОМИЛКА: " if is_error else "💡 ЗВІТ БОТА: "
    full_message = f"{icon}{message}"
    
    try:
        await bot.send_message(ADMIN_ID, full_message, parse_mode=ParseMode.MARKDOWN)
    except Exception as e:
        logger.error(f"Не вдалося надіслати сповіщення адміну: {e}")

# --- 7. ОСНОВНИЙ ЦИКЛ АВТОПОСТИНГУ ---

async def db_cleanup_loop():
    """Асинхронний цикл для періодичного очищення бази даних."""
    while True:
        await asyncio.sleep(Config.CLEANUP_INTERVAL_HOURS * 3600) 
        logger.info("--- ♻️ Запуск фонової очистки БД ---")
        await cleanup_db()

async def auto_posting_loop(bot: Bot):
    """Головний цикл, який періодично перевіряє та публікує новини."""
    while True:
        now_kyiv = datetime.now(KYIV_TZ)
        
        # 1. Перевірка та запуск щоденного дайджесту
        if now_kyiv.hour == Config.DAILY_DIGEST_HOUR and now_kyiv.date() > bot_state.last_digest_date:
            logger.info("--- 🏆 Час щоденного дайджесту ---")
            await send_daily_digest()
            bot_state.last_digest_date = now_kyiv.date()
        
        try:
            logger.info("--- 🚀 Запуск циклу автопостингу ---")
            
            # 2. Парсинг і збереження новин
            fetched_news, parse_duration = await fetch_all_sources()
            new_count = await save_news_to_db(fetched_news)
            logger.info(f"💾 Успішно вставлено {new_count} новин.")

            # 3. Отримуємо ТОП-3 новини
            news_to_post = await get_unique_news_from_db(Config.MAX_NEWS_PER_CYCLE)
            
            # 4. Публікація 
            post_start_time = datetime.now()
            posted_count = await send_news_to_channel(news_to_post)
            post_duration = (datetime.now() - post_start_time).total_seconds()
            
            logger.info(
                f"--- ✅ Цикл завершено. Постів: {posted_count}. Таймінги: Парсинг={parse_duration:.2f}с, Постинг={post_duration:.2f}с ---"
            )
            
        except Exception as e:
            logger.critical(f"❌ Критична помилка в циклі автопостингу: {e}")
            await send_admin_notification(
                f"❌ Критична помилка в циклі автопостингу: {type(e).__name__}", is_error=True
            )

        # 5. Очікування на основі динамічного інтервалу
        await asyncio.sleep(bot_state.posting_interval_min * 60)
        logger.info(f"Очікування {bot_state.posting_interval_min} хвилин...")

# --- 8. КОМАНДИ АДМІНІСТРАТОРА (ДИНАМІЧНЕ КЕРУВАННЯ) ---

async def cmd_status(message: types.Message):
    """Показує поточний статус бота та конфігурацію."""
    stats = await get_db_stats()
    stats_text = (
        f"• 📝 Всього новин у DB: {stats.get('total_news', 0)}\n"
        f"• ✅ Опубліковано: {stats.get('posted_news', 0)}\n"
        f"• 🏆 У дайджесті: {stats.get('digested_news', 0)}\n"
        f"• 📦 У черзі (З ФОТО): {stats.get('unposted_photo_news', 0)}\n"
        f"• 📰 Активних джерел: {stats.get('total_sources', 0)}"
    ) if stats else "❌ Не вдалося отримати статистику з бази даних."
    
    disabled_sources_list = "\n".join([f"   - {d}" for d in bot_state.disabled_sources]) if bot_state.disabled_sources else "   - (Немає)"

    config_msg = (
        "<b>🤖 Статус Платформи Новин (Професійний режим):</b>\n\n"
        "<b>⚙️ Конфігурація (Динамічна):</b>\n"
        f"  ⏳ Інтервал Парсингу: {bot_state.fetch_interval_min} хв\n"
        f"  ⏳ Інтервал Постингу: <b>{bot_state.posting_interval_min} хв</b>\n"
        f"  ⏱️ Макс. вік новини: {Config.MAX_AGE_MIN} хв\n"
        f"  📝 Макс. постів за цикл: <b>{Config.MAX_NEWS_PER_CYCLE} (ТОП-3)</b>\n"
        f"  🏆 Дайджест: {Config.DAILY_DIGEST_HOUR}:00 ({Config.DAILY_DIGEST_LIMIT} новин)\n"
        f"  🖼️ Watermark: {'✅ УВІМКНЕНО' if bot_state.watermark_enabled else '❌ ВИМКНЕНО'} (Текст: <code>{bot_state.watermark_text}</code>)\n"
        f"  ✍️ CTA: <code>{bot_state.cta_text}</code>\n"
        f"  🚫 Вимкнені джерела:\n{disabled_sources_list}\n\n"
        "<b>📊 Статистика Бази Даних:</b>\n"
        f"{stats_text}\n\n"
        "<b>🔑 Сервісні параметри:</b>\n"
        f"  📢 Channel ID: <code>{CHANNEL_ID}</code>"
    )
    await message.answer(config_msg, parse_mode=ParseMode.HTML)

async def cmd_forcepost(message: types.Message):
    """Примусово запускає цикл парсингу та постингу."""
    await message.answer("♻️ Примусовий запуск циклу парсингу...")
    
    # Використовуємо окрему функцію для запуску в окремому завданні
    async def run_once(bot_instance):
        try:
            start_time = datetime.now()
            fetched_news, parse_duration = await fetch_all_sources()
            new_count = await save_news_to_db(fetched_news)
            
            news_to_post = await get_unique_news_from_db(Config.MAX_NEWS_PER_CYCLE) 
            
            post_start_time = datetime.now()
            posted_count = await send_news_to_channel(news_to_post)
            post_duration = (datetime.now() - post_start_time).total_seconds()
            
            result_msg = (
                "✅ <b>Цикл примусового постингу завершено!</b>\n"
                f"   • Знайдено нових новин: {new_count}\n"
                f"   • Опубліковано новин: {posted_count}\n"
                f"   • Таймінг (Парсинг): {parse_duration:.2f} сек\n"
                f"   • Таймінг (Постинг): {post_duration:.2f} сек"
            )
        except Exception as e:
            result_msg = f"❌ <b>Критична помилка примусового постингу:</b> {e}"
            logger.critical(result_msg)
        
        await bot_instance.send_message(message.chat.id, result_msg, parse_mode=ParseMode.HTML)

    asyncio.create_task(run_once(bot)) # Запуск як окреме завдання


async def cmd_set_watermark(message: types.Message):
    """/set_watermark <новий текст> - Змінює текст водяного знаку."""
    try:
        new_text = message.text.split(maxsplit=1)[1].strip()
        if not new_text:
            await message.answer("❌ Будь ласка, вкажіть текст для водяного знаку.")
            return

        bot_state.watermark_text = new_text
        await message.answer(f"✅ Текст водяного знаку змінено на: <code>{new_text}</code>", parse_mode=ParseMode.HTML)
    except IndexError:
        await message.answer("❌ Використання: <code>/set_watermark @NewChannelName</code>", parse_mode=ParseMode.HTML)

async def cmd_toggle_watermark(message: types.Message):
    """/toggle_watermark - Вмикає/вимикає водяні знаки."""
    bot_state.watermark_enabled = not bot_state.watermark_enabled
    status = "УВІМКНЕНО" if bot_state.watermark_enabled else "ВИМКНЕНО"
    await message.answer(f"✅ Водяні знаки тепер: <b>{status}</b>", parse_mode=ParseMode.HTML)

async def cmd_set_cta(message: types.Message):
    """/set_cta <новий текст> - Змінює текст заклику до дії."""
    try:
        new_text = message.text.split(maxsplit=1)[1].strip()
        if not new_text:
            await message.answer("❌ Будь ласка, вкажіть текст для заклику до дії (CTA).")
            return

        bot_state.cta_text = new_text
        await message.answer(f"✅ Текст заклику до дії (CTA) змінено на: <i>{new_text}</i>", parse_mode=ParseMode.HTML)
    except IndexError:
        await message.answer("❌ Використання: <code>/set_cta 👉 Підписатись на @MyChannel</code>", parse_mode=ParseMode.HTML)
        
async def cmd_set_interval(message: types.Message):
    """/set_interval <постинг> - Змінює інтервал постингу (у хвилинах)."""
    try:
        args = message.text.split()
        if len(args) < 2:
            await message.answer("❌ Використання: <code>/set_interval 5</code> (встановлює інтервал постингу 5 хв)", parse_mode=ParseMode.HTML)
            return

        posting_interval = int(args[1])
        
        if posting_interval < 1:
            await message.answer("❌ Інтервал має бути не менше 1 хвилини.")
            return

        # bot_state.fetch_interval_min = fetch_interval # Парсинг лишаємо в config
        bot_state.posting_interval_min = posting_interval

        await message.answer(
            f"✅ Інтервал постингу змінено на: <b>{posting_interval} хв</b>\n"
            f"   (Зміни набудуть чинності після завершення поточного циклу)", 
            parse_mode=ParseMode.HTML
        )
    except ValueError:
        await message.answer("❌ Інтервал має бути цілим числом.", parse_mode=ParseMode.HTML)

async def cmd_toggle_source(message: types.Message):
    """/toggle_source <домен_джерела> - Вимикає/вмикає джерело."""
    try:
        source_domain_raw = message.text.split(maxsplit=1)[1].strip()
        source_domain = source_domain_raw.replace('https://', '').replace('http://', '').replace('www.', '').split('/')[0]
        
        if not source_domain:
            await message.answer("❌ Вкажіть домен джерела (наприклад, pravda.com.ua).")
            return

        if source_domain in bot_state.disabled_sources:
            bot_state.disabled_sources.remove(source_domain)
            status = "УВІМКНЕНО"
        else:
            bot_state.disabled_sources.add(source_domain)
            status = "ВИМКНЕНО"
            
        await message.answer(f"✅ Джерело <b>{source_domain}</b> тепер: <b>{status}</b>", parse_mode=ParseMode.HTML)
    except IndexError:
        await message.answer("❌ Використання: <code>/toggle_source pravda.com.ua</code>", parse_mode=ParseMode.HTML)

async def cmd_queue(message: types.Message):
    """/queue - Показує кількість новин з фото, що очікують на публікацію."""
    stats = await get_db_stats()
    unposted_count = stats.get('unposted_photo_news', 0) if stats else 0
    await message.answer(f"📦 <b>У черзі</b> на публікацію (з фото): <b>{unposted_count}</b> новин.", parse_mode=ParseMode.HTML)


# --- 9. ЗАПУСК БОТА ---

async def main():
    """Основна функція для ініціалізації та запуску бота."""
    
    if not all([BOT_TOKEN, DATABASE_URL, CHANNEL_ID]):
        logger.critical("Критична помилка: Не задані BOT_TOKEN, DATABASE_URL або CHANNEL_ID.")
        return

    await connect_db()
    
    if not db_pool:
        return

    await init_db()

    global bot
    bot = Bot(
        token=BOT_TOKEN, 
        default=DefaultBotProperties(parse_mode=ParseMode.HTML)
    )
    
    global dp
    dp = Dispatcher()
    
    # Реєстрація команд адміністратора
    admin_filter = F.from_user.id == ADMIN_ID
    dp.message.register(cmd_status, Command("status"), admin_filter)
    dp.message.register(cmd_forcepost, Command("forcepost"), admin_filter)
    dp.message.register(cmd_stats, Command("stats"), admin_filter)
    dp.message.register(cmd_set_watermark, Command("set_watermark"), admin_filter)
    dp.message.register(cmd_toggle_watermark, Command("toggle_watermark"), admin_filter)
    dp.message.register(cmd_set_cta, Command("set_cta"), admin_filter)
    dp.message.register(cmd_set_interval, Command("set_interval"), admin_filter)
    dp.message.register(cmd_toggle_source, Command("toggle_source"), admin_filter)
    dp.message.register(cmd_queue, Command("queue"), admin_filter)

    asyncio.create_task(auto_posting_loop(bot))
    asyncio.create_task(db_cleanup_loop())
    logger.info("Бот запущено. Початок роботи.")

    try:
        await bot.delete_webhook(drop_pending_updates=True) 
        await dp.start_polling(bot)
    finally:
        if bot:
            await bot.session.close()
        if db_pool:
            await db_pool.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Бот зупинено вручну.")
    except Exception as e:
        logger.critical(f"❌ Головна помилка виконання: {e}")
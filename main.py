import asyncio
import logging
import os
import re
import sys
import psycopg2
import gspread
from datetime import datetime
from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import FSInputFile
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder
from playwright.async_api import async_playwright

# === 1. ЗАГРУЗКА КОНФИГУРАЦИИ ===
# Явно указываем кодировку, чтобы избежать UnicodeDecodeError
load_dotenv(encoding="utf-8")

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_CHANNEL_ID = os.getenv("ADMIN_CHANNEL_ID")
DEFAULT_PHOTO_PATH = os.path.join("img", "default_photo.jpeg")

# Настройки БД PostgreSQL
DB_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASS"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT")
}

# Настройки Google
GOOGLE_KEY_FILE = os.getenv("GOOGLE_KEY_PATH", "google_key.json")
GOOGLE_SHEET_NAME = os.getenv("GOOGLE_SHEET_NAME", "Список тендеров")

# Настройки Парсера
MAX_PAGES_PER_RUN = 5
MIN_PRICE_LIMIT = 5000000  # 5 МЛН СУМ

TARGET_KEYWORDS = [
    "Услуги печатные", "звуко- и видеозаписей", "программных средств",
    "Оборудование компьютерное", "электронное и оптическое", "Оборудование электрическое",
    "Машины и оборудование", "Услуги телекоммуникационные", "Продукты программные",
    "разработке программного обеспечения", "информационных технологий", "Услуги головных офисов",
    "услуги консультативные", "научными исследованиями", "экспериментальными разработками",
    "Услуги рекламные", "исследованию конъюнктуры рынка", "Услуги профессиональные, научные и технические",
    "государственного управления", "военной безопасности", "социального обеспечения",
    "Услуги в области образования", "Услуги в области здравоохранения"
]

REGIONS_LIST = [
    "Respublika Karakalpakstan", "Andijan", "Bukhara", "Jizzakh", "Qashqadaryo",
    "Navoiy", "Namangan", "Samarkand", "Surxondaryo", "Sirdaryo", "Tashkent",
    "Fergana", "Xorazm", "Toshkent shahri", "Бухарская", "Ташкентская",
    "Самаркандская", "Ферганская", "Андижанская", "Наманганская", "Джизакская",
    "Кашкадарьинская", "Навоийская", "Сырдарьинская", "Сурхандарьинская",
    "Хорезмская", "Республика Каракалпакстан", "г.Ташкент", "г. Ташкент",
    "Toshkent viloyati", "Tashkent region"
]

TOPIC_MAP = {
    "Xarid.uz": 2, "IT-Market": 4, "Etender": 6, "Cooperation": 8, "XT-Xarid": 10
}

# Инициализация бота
logging.basicConfig(level=logging.INFO)
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# ==========================================
# === 2. БЛОК РАБОТЫ С БАЗОЙ ДАННЫХ (PostgreSQL) ===
# ==========================================

def get_connection():
    try:
        return psycopg2.connect(**DB_CONFIG)
    except psycopg2.OperationalError as e:
        print(f"❌ Ошибка подключения к БД! Проверьте, запущен ли PostgreSQL и создан ли DB_NAME.\nТекст ошибки: {e}")
        raise e

def init_db():
    conn = get_connection()
    cursor = conn.cursor()
    # Таблица тендеров
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tenders (
            id SERIAL PRIMARY KEY,
            source TEXT, title TEXT, description TEXT, price TEXT,
            start_date TEXT, end_date TEXT, link TEXT UNIQUE,
            date_added TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    ''')
    # Таблица пользователей
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            phone TEXT, username TEXT
        );
    ''')
    # Таблица избранного
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS favorites (
            user_id BIGINT,
            tender_id INTEGER,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (user_id, tender_id),
            FOREIGN KEY (tender_id) REFERENCES tenders(id) ON DELETE CASCADE
        );
    ''')
    conn.commit()
    cursor.close()
    conn.close()

def check_exists(link):
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM tenders WHERE link = %s", (link,))
        result = cursor.fetchone()
        conn.close()
        return result is not None
    except: return False

def add_tender_direct(source, title, description, price, start_date, end_date, link):
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO tenders (source, title, description, price, start_date, end_date, link)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (link) DO NOTHING
        """, (source, title, description, price, start_date, end_date, link))
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        print(f"DB Error: {e}")
        return False

def get_next_tender(user_id, source):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        query = """
            SELECT id, title, description, price, start_date, end_date, link 
            FROM tenders 
            WHERE source = %s 
            AND id NOT IN (SELECT tender_id FROM favorites WHERE user_id = %s)
            ORDER BY id DESC LIMIT 1
        """
        cursor.execute(query, (source, user_id))
        return cursor.fetchone()
    finally:
        cursor.close()
        conn.close()

def add_favorite(user_id, tender_id):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("INSERT INTO favorites (user_id, tender_id) VALUES (%s, %s) ON CONFLICT DO NOTHING", (user_id, tender_id))
        conn.commit()
    finally:
        cursor.close()
        conn.close()

def delete_favorite(user_id, tender_id):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM favorites WHERE user_id = %s AND tender_id = %s", (user_id, tender_id))
        conn.commit()
    finally:
        cursor.close()
        conn.close()

def get_user_favorites(user_id):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        query = """
            SELECT t.id, t.title, t.price, t.link, t.source 
            FROM favorites f
            JOIN tenders t ON f.tender_id = t.id
            WHERE f.user_id = %s
            ORDER BY f.timestamp DESC
        """
        cursor.execute(query, (user_id,))
        return cursor.fetchall()
    finally:
        cursor.close()
        conn.close()

def get_tender_link(tender_id):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT link FROM tenders WHERE id = %s", (tender_id,))
        res = cursor.fetchone()
        return res[0] if res else None
    finally:
        cursor.close()
        conn.close()

# ==========================================
# === 3. ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ПАРСЕРА ===
# ==========================================

def parse_price_to_number(price_str):
    if not price_str: return 0.0
    try:
        clean = price_str.replace(" ", "").replace("\xa0", "").replace(",", ".")
        clean = re.sub(r'[^\d.]', '', clean)
        return float(clean)
    except: return 0.0

def format_price_str(price_raw):
    if not price_raw or "нет" in price_raw.lower() or "hali" in price_raw.lower(): return "Нет"
    try:
        val = parse_price_to_number(price_raw)
        return "{:,.2f} сум".format(val).replace(",", " ").replace(".", ",")
    except: return price_raw

def save_to_google_sheet(source_name, row_data):
    if not os.path.exists(GOOGLE_KEY_FILE): return
    try:
        client = gspread.service_account(filename=GOOGLE_KEY_FILE)
        sheet = client.open(GOOGLE_SHEET_NAME)
        try:
            worksheet = sheet.worksheet(source_name)
        except gspread.WorksheetNotFound:
            cols = "6" if source_name == "IT-Market" else "20"
            worksheet = sheet.add_worksheet(title=source_name, rows="1000", cols=cols)
            headers = ["Дата парсинга", "Заказчик", "Статус", "Задача", "Бюджет", "Ссылка"] if source_name == "IT-Market" else [
                "Дата парсинга", "Тип анкеты", "Номер лота", "Описание товаров", 
                "Название/Квалификация", "Заказчик", "Начальная цена", "Текущая цена", 
                "Регион", "Дата начала", "Срок окончания", "Срок доставки", 
                "Участников", "Контакты", "Ссылка"
            ]
            worksheet.append_row(headers)
        
        worksheet.append_row(row_data)
        print(f"✅ [Google] Записано в лист '{source_name}'")
    except Exception as e:
        print(f"⚠️ Ошибка Google Sheets: {e}")

async def send_notification_to_channel(text, source_name, photo_path=None):
    if not ADMIN_CHANNEL_ID: return
    thread_id = TOPIC_MAP.get(source_name)
    try:
        if photo_path and os.path.exists(photo_path):
            photo = FSInputFile(photo_path)
            await bot.send_photo(chat_id=ADMIN_CHANNEL_ID, photo=photo, caption=text, parse_mode="HTML", message_thread_id=thread_id)
        else:
            await bot.send_message(chat_id=ADMIN_CHANNEL_ID, text=text, parse_mode="HTML", message_thread_id=thread_id, disable_web_page_preview=True)
    except Exception as e:
        print(f"⚠️ Ошибка отправки TG: {e}")

# ==========================================
# === 4. ЛОГИКА ПАРСИНГА (PLAYWRIGHT) ===
# ==========================================

async def get_xarid_details(page, link):
    data = {
        "customer": "Не указан", "contact": "Не указан", "participants": "0", 
        "start_date": "Не указана", "end_date": "Не указана", 
        "delivery_term": "Не указан", "items_desc": "Не указано"
    }
    try:
        await page.goto(link, timeout=45000, wait_until="domcontentloaded")
        await page.wait_for_timeout(2500)
        raw_text = await page.inner_text("body")
        
        found_items = []
        raw_items = re.findall(r"(?:^|\n)\s*(?:\d+[.\s]*)?([^\n]+?)\s*\(\d{2}\.\d{2}\.\d{2}[\.\d-]*\)", raw_text)
        if raw_items:
            for idx, item_name in enumerate(raw_items):
                item_clean = item_name.strip()
                if len(item_clean) > 2:
                    found_items.append(f"{idx + 1}. {item_clean}")
            if found_items: data["items_desc"] = "\n".join(found_items)

        for word in ["Texnik yordam", "Call-markaz", "Ishonch telefoni", "Техническая поддержка"]:
            if word in raw_text: raw_text = raw_text.split(word)[0]
        clean_oneline = " ".join(raw_text.split())

        cust_match = re.search(r"(Buyurtmachining\s*nomi|Наименование\s*заказчика)\s*:?\s*(.*?)(Boshlanish|Start|Дата|Manzil|Адрес)", clean_oneline, re.IGNORECASE)
        if cust_match: data["customer"] = cust_match.group(2).strip()

        phone_match = re.search(r"Bog.?lanish\s*uchun\s*:?\s*([+\d\(\)\s-]{7,25})", clean_oneline, re.IGNORECASE)
        if phone_match and any(c.isdigit() for c in phone_match.group(1)): data["contact"] = phone_match.group(1).strip()
        
        deliv_match = re.search(r"(Yetkazib\s*berish\s*muddati|Срок\s*поставки)\s*:?\s*(.*?)(Fayl|Status|Статус)", clean_oneline, re.IGNORECASE)
        if deliv_match: data["delivery_term"] = deliv_match.group(2).strip()

        start_match = re.search(r"(Boshlanish\s*sanasi|Дата\s*начала).*?(\d{2}\.\d{2}\.\d{4}\s*\d{2}:\d{2}(?::\d{2})?)", clean_oneline, re.IGNORECASE)
        if start_match: data["start_date"] = start_match.group(2)
        
        end_match = re.search(r"(Tugash\s*sanasi|Дата\s*окончания).*?(\d{2}\.\d{2}\.\d{4}\s*\d{2}:\d{2}(?::\d{2})?)", clean_oneline, re.IGNORECASE)
        if end_match: data["end_date"] = end_match.group(2)

        part_match = re.search(r"(Ishtirokchilar\s*soni|Участники).*?(\d+)", clean_oneline, re.IGNORECASE)
        if part_match: data["participants"] = part_match.group(2)
    except: pass
    return data

async def parse_xarid_uz(page):
    url = "https://xarid.uzex.uz/auction"
    source_name = "Xarid.uz"
    print(f"🔸 Проверяю {source_name}...")
    try:
        await page.goto(url, timeout=90000, wait_until="domcontentloaded")
        page_num = 1
        while page_num <= MAX_PAGES_PER_RUN:
            try: await page.wait_for_selector(".lot-item", timeout=15000)
            except: break
            items = page.locator(".lot-item")
            count = await items.count()
            if count == 0: break
            new_items = 0
            
            for i in range(count):
                item = items.nth(i)
                try:
                    full_text = await item.inner_text()
                    clean_text = " ".join(full_text.split())

                    is_target = any(k.lower() in clean_text.lower() for k in TARGET_KEYWORDS)
                    if not is_target: continue

                    match_id = re.search(r'Lot\s*raqami:\s*(\d+)', clean_text, re.IGNORECASE)
                    lot_id = match_id.group(1) if match_id else "00000"
                    
                    # === 1. НАЧАЛЬНАЯ ЦЕНА ===
                    start_pattern = r"(?:Boshlang.?ich\s*narx|Начальная\s*стоимость|Стартовая\s*стоимость|Начальная\s*цена)[^\d]*([\d\s,.]+)"
                    match_p = re.search(start_pattern, clean_text, re.IGNORECASE)
                    start_price_raw = match_p.group(1) if match_p else "0"
                    
                    start_price_num = parse_price_to_number(start_price_raw)
                    if start_price_num < MIN_PRICE_LIMIT: continue 
                    start_price_str = format_price_str(start_price_raw)

                    full_link = f"https://xarid.uzex.uz/auction/detail/{lot_id[-6:]}"
                    if check_exists(full_link): continue

                    # === 2. ТЕКУЩАЯ ЦЕНА (ИСПРАВЛЕНО ДЛЯ EXCEL) ===
                    curr_pattern = r"(?:Joriy\s*narx|Текущая\s*цена|Лучшее\s*предложение)[^\d]*([\d\s,.]+)"
                    match_c = re.search(curr_pattern, clean_text, re.IGNORECASE)
                    
                    # Создаем две переменные: одну для бота (строка), другую для Excel (число)
                    if match_c:
                        raw_curr = match_c.group(1)
                        current_price_num = parse_price_to_number(raw_curr) # Чистое число
                        current_price_str = format_price_str(raw_curr)      # Красивая строка
                    else:
                        current_price_num = 0.0
                        current_price_str = "Нет ставок"

                    # Глубокий парсинг
                    detail_page = await page.context.new_page()
                    details = await get_xarid_details(detail_page, full_link)
                    await detail_page.close()
                    
                    toifa = "Не указана"
                    if "Toifa:" in full_text:
                        toifa = full_text.split("Toifa:")[1].split("\n")[0].strip()

                    region = "Не указан"
                    for reg in REGIONS_LIST:
                        if reg.lower() in clean_text.lower(): region = reg; break

                    real_end = details['end_date'] if details['end_date'] != "Не указана" else "-"
                    real_start = details['start_date'] if details['start_date'] != "Не указана" else "-"
                    
                    full_desc = f"{toifa}||{region}||{current_price_str}"
                    add_tender_direct(source_name, f"Лот №{lot_id}", full_desc, start_price_str, real_start, real_end, full_link)
                    print(f"🔥 [Xarid] Новый: {lot_id} | Тек: {current_price_num}")

                    msg = (
                        f"<b>Тип анкеты: Аукцион</b>\nИсточник: xarid.uz\n\n"
                        f"🔢 <b>Номер лота:</b> {lot_id}\n📂 <b>Квалификация:</b> {toifa}\n"
                        f"📍 <b>Район:</b> {region}\n📅 <b>Дата начала:</b> {real_start}\n"
                        f"⏳ <b>Срок окончания:</b> {real_end}\n🚚 <b>Срок доставки:</b> {details['delivery_term']}\n"
                        f"💰 <b>Начальная цена:</b> {start_price_str}\n📉 <b>Текущая цена:</b> {current_price_str}\n"
                        f"🔗 <b>Ссылка:</b> {full_link}\n\n🏢 <b>Заказчик:</b> {details['customer']}\n"
                        f"📞 <b>Контакты:</b> {details['contact']}\n👥 <b>Участников:</b> {details['participants']}\n"
                        f"📦 <b>Товары:</b>\n{details['items_desc'][:300]}..."
                    )
                    await send_notification_to_channel(msg, source_name, DEFAULT_PHOTO_PATH)

                    # ЗАПИСЬ В EXCEL (Используем чистые числа)
                    sheet_row = [
                        datetime.now().strftime("%d.%m.%Y %H:%M"), 
                        "Аукцион", 
                        lot_id, 
                        details['items_desc'], 
                        toifa, 
                        details['customer'], 
                        start_price_num,   # Число
                        current_price_num, # Число (раньше тут была ошибка)
                        region, 
                        real_start, 
                        real_end, 
                        details['delivery_term'], 
                        details['participants'], 
                        details['contact'], 
                        full_link
                    ]
                    save_to_google_sheet("Xarid.uz", sheet_row)
                    new_items += 1
                except Exception as e: 
                    continue

            if new_items == 0 and page_num > 1: break
            try:
                next_btn = page.locator(".pagination-next, .ui-paginator-next").first
                if await next_btn.is_visible(): await next_btn.click(); await page.wait_for_timeout(3000); page_num += 1
                else: break
            except: break
    except: pass

    
async def parse_it_market(page):
    url = "https://it-market.uz/order/"
    source_name = "IT-Market"
    print(f"🔹 Проверяю {source_name}...")
    try:
        await page.goto(url, timeout=60000, wait_until="networkidle")
        cards = page.locator(".animated-card")
        if await cards.count() == 0: return

        for i in range(await cards.count()):
            card = cards.nth(i)
            try:
                link_loc = card.locator(".stretched-link")
                if await link_loc.count() > 0:
                    href = await link_loc.get_attribute("href")
                    full_link = f"https://it-market.uz{href}"
                else: full_link = url

                if check_exists(full_link): continue

                lines = (await card.inner_text()).split('\n')
                lines = [l.strip() for l in lines if l.strip()]
                if len(lines) < 3: continue

                company, status, title = lines[0], (lines[1] if len(lines) > 1 else ""), (lines[2] if len(lines) > 2 else "Без названия")
                price_str = "Договорная"
                
                for k, line in enumerate(lines):
                    if "Бюджет" in line and k+3 < len(lines):
                        price_str = format_price_str(lines[k+3])
                        break
                
                add_tender_direct(source_name, title, company, price_str, "-", "-", full_link)
                print(f"🔥 [IT-Market] Новый: {title}")

                msg = (f"<b>Тип анкеты: IT Заказ</b>\n\n🏢 <b>Заказчик:</b> {company}\nℹ️ <b>Статус:</b> {status}\n🛠 <b>Задача:</b> {title}\n💰 <b>Бюджет:</b> {price_str}\n🔗 <b>Ссылка:</b> {full_link}")
                await send_notification_to_channel(msg, source_name, DEFAULT_PHOTO_PATH)
                
                save_to_google_sheet("IT-Market", [datetime.now().strftime("%d.%m.%Y %H:%M"), company, status, title, price_str, full_link])
            except: continue
    except: pass

async def parser_loop():
    print("🚀 Парсер запущен в фоне...")
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(viewport={'width': 1920, 'height': 1080})
        page = await context.new_page()
        
        while True:
            await parse_xarid_uz(page)
            await parse_it_market(page)
            print("💤 Пауза парсинга 5 минут...")
            await asyncio.sleep(300)

# ==========================================
# === 5. ЛОГИКА ТЕЛЕГРАМ БОТА ===
# ==========================================

def get_source_menu():
    kb = InlineKeyboardBuilder()
    kb.button(text="🏛 Xarid.uz", callback_data="source_Xarid.uz")
    kb.button(text="🏗 Etender", callback_data="source_Etender")
    kb.button(text="🤝 Cooperation", callback_data="source_Cooperation")
    kb.button(text="🏫 XT-Xarid", callback_data="source_XT-Xarid")
    kb.button(text="💻 IT-Market", callback_data="source_IT-Market")
    kb.adjust(2, 2, 1) 
    return kb.as_markup()

def get_bottom_menu():
    kb = ReplyKeyboardBuilder()
    kb.button(text="🔙 Выбрать источник")
    kb.button(text="❤️ Мои лайки")
    return kb.as_markup(resize_keyboard=True)

def get_tinder_keyboard(tender_id, source):
    kb = InlineKeyboardBuilder()
    kb.button(text="👎 Пропустить", callback_data=f"dislike_{tender_id}_{source}")
    kb.button(text="❤️ Лайк", callback_data=f"like_{tender_id}_{source}")
    kb.adjust(2)
    return kb.as_markup()

def format_caption(source, title, desc, price, start, end, link):
    if source == "Xarid.uz":
        toifa, region, current_price = "Не указана", "Не указан", "Нет"
        if desc and "||" in desc:
            parts = desc.split("||")
            if len(parts) >= 3: toifa, region, current_price = parts[0], parts[1], parts[2]
        lot_number = title.replace("Лот №", "").replace("Лот ", "")
        return (
            f"📢 *Новый лот на Xarid.uz*\n\n"
            f"1. *Номер лота:* {lot_number}\n2. *Квалификация:* {toifa}\n"
            f"3. *Район:* {region}\n4. *Срок окончания:* {end}\n"
            f"5. *Начальная цена:* {price}\n6. *Текущая цена:* {current_price}\n"
            f"7. *Ссылка:* {link}"
        )
    else:
        display_title = title if title else "Без названия"
        company, status = desc, "-"
        if desc and "(" in desc and desc.strip().endswith(")"):
            try: company, status = desc.rsplit(" (", 1); status = status.replace(")", "")
            except: pass
        return (
            f"📢 *Новый заказ на {source}*\n\n"
            f"🏢 *Заказчик:* {company}\nℹ️ *Статус:* {status}\n"
            f"🛠 *Задача:* {display_title}\n\n💰 *Бюджет:* {price}\n"
            f"📅 *Начало:* {start}\n🏁 *Дедлайн:* {end}\n\n"
            f"🔗 [Посмотреть подробнее]({link})"
        )

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    await message.answer("👋 Привет! Я агрегатор тендеров.", reply_markup=get_bottom_menu())
    await message.answer("🔎 Откуда брать тендеры?", reply_markup=get_source_menu())

@dp.message(F.text == "🔙 Выбрать источник")
async def menu_button_handler(message: types.Message):
    await message.answer("🔎 Выберите площадку:", reply_markup=get_source_menu())

@dp.message(F.text == "❤️ Мои лайки")
async def favorites_button_handler(message: types.Message):
    user_id = message.from_user.id
    favorites = get_user_favorites(user_id)
    if not favorites:
        await message.answer("💔 Вы пока ничего не добавили в избранное.")
        return
    await message.answer(f"📋 *Ваши избранные ({len(favorites)} шт):*", parse_mode="Markdown")
    for row in favorites:
        t_id, title, price, link, source = row[:5]
        display_title = title if title else "Без названия"
        text = f"🔢 *{display_title}*\n🏛 {source}\n💰 {price}\n🔗 {link}" if source == "Xarid.uz" else f"🛠 *{display_title}*\n🏛 {source}\n💰 {price}\n🔗 [Открыть]({link})"
        kb = InlineKeyboardBuilder()
        kb.button(text="🗑 Удалить", callback_data=f"del_fav_{t_id}")
        await message.answer(text, parse_mode="Markdown", disable_web_page_preview=True, reply_markup=kb.as_markup())

@dp.callback_query(F.data.startswith("source_"))
async def start_swiping(callback: types.CallbackQuery):
    source_name = callback.data.split("_")[1]
    await callback.answer(f"Загружаю {source_name}...")
    await show_next_card(callback.message, callback.from_user.id, source_name)

async def show_next_card(message: types.Message, user_id, source_name):
    tender = get_next_tender(user_id, source_name)
    if not tender:
        await message.answer(f"🎉 На площадке *{source_name}* всё просмотрено!", parse_mode="Markdown")
        return
    t_id, title, desc, price, start, end, link = tender
    caption_text = format_caption(source_name, title, desc, price, start, end, link)
    
    if os.path.exists(DEFAULT_PHOTO_PATH):
        photo = FSInputFile(DEFAULT_PHOTO_PATH)
        await message.answer_photo(photo=photo, caption=caption_text, parse_mode="Markdown", reply_markup=get_tinder_keyboard(t_id, source_name))
    else:
        await message.answer(text=caption_text, parse_mode="Markdown", reply_markup=get_tinder_keyboard(t_id, source_name), disable_web_page_preview=True)

@dp.callback_query(F.data.startswith("like_"))
async def handle_like(callback: types.CallbackQuery):
    _, t_id, source = callback.data.split("_", 2)
    add_favorite(callback.from_user.id, t_id)
    
    old_text = callback.message.caption or callback.message.text
    link = get_tender_link(t_id)
    restored_text = old_text
    if link:
        if source == "Xarid.uz":
             if "7. Ссылка:" in old_text and link not in old_text: restored_text = old_text.replace("7. Ссылка:", f"7. Ссылка: {link}")
        else: restored_text = old_text.replace("🔗 Посмотреть подробнее", f"🔗 [Посмотреть подробнее]({link})")
    
    new_text = f"{restored_text}\n\n✅ *В ИЗБРАННОМ*"
    if callback.message.photo: await callback.message.edit_caption(caption=new_text, parse_mode="Markdown", reply_markup=None)
    else: await callback.message.edit_text(text=new_text, parse_mode="Markdown", reply_markup=None, disable_web_page_preview=True)
    await callback.answer("❤️ Сохранено")
    await show_next_card(callback.message, callback.from_user.id, source)

@dp.callback_query(F.data.startswith("dislike_"))
async def handle_dislike(callback: types.CallbackQuery):
    _, t_id, source = callback.data.split("_", 2)
    old_text = callback.message.caption or callback.message.text
    new_text = f"{old_text}\n\n❌ *ПРОПУЩЕНО*"
    if callback.message.photo: await callback.message.edit_caption(caption=new_text, parse_mode="Markdown", reply_markup=None)
    else: await callback.message.edit_text(text=new_text, parse_mode="Markdown", reply_markup=None, disable_web_page_preview=True)
    await callback.answer("👎 Пропущено")
    await show_next_card(callback.message, callback.from_user.id, source)

@dp.callback_query(F.data.startswith("del_fav_"))
async def delete_favorite_handler(callback: types.CallbackQuery):
    tender_id = callback.data.split("_")[2]
    delete_favorite(callback.from_user.id, tender_id)
    await callback.message.delete()
    await callback.answer("🗑 Удалено!")

# ==========================================
# === 6. MAIN (ЗАПУСК ВСЕГО) ===
# ==========================================

async def main():
    print("🚀 Запуск инициализации...")
    try:
        init_db()
    except Exception as e:
        print(f"❌ КРИТИЧЕСКАЯ ОШИБКА БД: {e}")
        return

    print("🤖 Запуск Бота и Парсера...")
    asyncio.create_task(parser_loop())
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        print("Bot stopped!")
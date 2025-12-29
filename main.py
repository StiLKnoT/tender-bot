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

# === 1. CONFIGURATION ===
load_dotenv(encoding="utf-8")

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_CHANNEL_ID = os.getenv("ADMIN_CHANNEL_ID")
DEFAULT_PHOTO_PATH = os.path.join("img", "default_photo.jpeg")

# Database Configuration
DB_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASS"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT")
}

# Google Sheets Configuration
GOOGLE_KEY_FILE = os.getenv("GOOGLE_KEY_PATH", "google_key.json")
GOOGLE_SHEET_NAME = os.getenv("GOOGLE_SHEET_NAME", "Список тендеров")

# Parser Settings
MAX_PAGES_PER_RUN = 5
MIN_PRICE_LIMIT = 5000000  # 5 Million SUM

TARGET_KEYWORDS = [
    "Услуги печатные", "звуко- и видеозаписей", "программных средств",
    "Оборудование компьютерное", "электронное и оптическое", "Оборудование электрическое",
    "Услуги телекоммуникационные", "Продукты программные",
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

# Initialize Bot
logging.basicConfig(level=logging.INFO)
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# ==========================================
# === 2. DATABASE MANAGEMENT (PostgreSQL) ===
# ==========================================

def get_connection():
    try:
        return psycopg2.connect(**DB_CONFIG)
    except psycopg2.OperationalError as e:
        print(f"❌ DB Connection Error: {e}")
        raise e

def init_db():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tenders (
            id SERIAL PRIMARY KEY,
            source TEXT, title TEXT, description TEXT, price TEXT,
            start_date TEXT, end_date TEXT, link TEXT UNIQUE,
            date_added TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            phone TEXT, username TEXT
        );
    ''')
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
# === 3. HELPER FUNCTIONS ===
# ==========================================

def parse_price_to_number(price_str):
    if not price_str: return 0.0
    try:
        clean = price_str.replace(" ", "").replace("\xa0", "")
        # Remove trailing .00
        if clean.endswith(".00") or clean.endswith(",00"): clean = clean[:-3]
        
        if "," in clean and "." in clean: 
            if clean.find(",") < clean.find("."): clean = clean.replace(",", "") 
            else: clean = clean.replace(".", "").replace(",", ".")
        elif "," in clean: 
            if len(clean) - clean.rfind(",") == 4: clean = clean.replace(",", "")
            else: clean = clean.replace(",", ".")
            
        clean = re.sub(r'[^\d.]', '', clean)
        return float(clean)
    except: return 0.0

def format_price_str(price_raw):
    if not price_raw or "нет" in str(price_raw).lower(): return "Не указано"
    try:
        val = parse_price_to_number(str(price_raw))
        if val == 0: return "Не указано"
        return "{:,.2f}".format(val).replace(",", " ").replace(".", ",")
    except: return "Не указано"

def save_to_google_sheet(source_name, row_data):
    if not os.path.exists(GOOGLE_KEY_FILE): return
    try:
        client = gspread.service_account(filename=GOOGLE_KEY_FILE)
        sheet = client.open(GOOGLE_SHEET_NAME)
        
        try:
            worksheet = sheet.worksheet(source_name)
        except gspread.WorksheetNotFound:
            # === ЗАГОЛОВКИ ДЛЯ ETENDER (БЕЗ "Текущая цена" и "Участников") ===
            if source_name == "Etender":
                cols = "20"
                headers = [
                    "Дата парсинга", 
                    "Тип анкеты", 
                    "Номер лота", 
                    "Описание товаров", 
                    "Квалификация (Toifa)", 
                    "Заказчик", 
                    "ИНН Заказчика", 
                    "Начальная цена", 
                    "Валюта", 
                    # "Текущая цена" - УБРАНО
                    "Регион", 
                    "Дата начала", 
                    "Срок окончания", 
                    "Срок доставки", 
                    # "Участников" - УБРАНО
                    "Контакты", 
                    "Ссылка"
                ]
            elif source_name == "IT-Market":
                cols = "6"
                headers = ["Дата парсинга", "Заказчик", "Статус", "Задача", "Бюджет", "Ссылка"]
            else:
                # Xarid.uz (ОСТАВЛЯЕМ КАК ЕСТЬ)
                cols = "20"
                headers = [
                    "Дата парсинга", "Тип анкеты", "Номер лота", "Описание товаров", 
                    "Название/Квалификация", "Заказчик", "Начальная цена", "Текущая цена", 
                    "Регион", "Дата начала", "Срок окончания", "Срок доставки", 
                    "Участников", "Контакты", "Ссылка"
                ]
            
            worksheet = sheet.add_worksheet(title=source_name, rows="1000", cols=cols)
            worksheet.append_row(headers)
            try:
                body = {"requests": [{"repeatCell": {"range": {"sheetId": worksheet.id, "startRowIndex": 0, "endRowIndex": 1}, "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}}, "fields": "userEnteredFormat.textFormat.bold"}}]}
                sheet.batch_update(body)
            except: pass
        
        # Преобразуем все данные в строки
        safe_row = [str(x) if x is not None else "" for x in row_data]
        worksheet.append_row(safe_row)
        print(f"✅ [Google] Записано в лист '{source_name}'")
        
    except Exception as e:
        print(f"⚠️ Ошибка Google Sheets ({source_name}): {e}")

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
        print(f"⚠️ Telegram Error: {e}")

# ==========================================
# === 4. PARSING LOGIC: ETENDER ===
# ==========================================

# ==========================================
# === 3. ФУНКЦИЯ ЗАПИСИ В GOOGLE SHEETS ===
# ==========================================

def save_to_google_sheet(source_name, row_data):
    if not os.path.exists(GOOGLE_KEY_FILE): return
    try:
        client = gspread.service_account(filename=GOOGLE_KEY_FILE)
        sheet = client.open(GOOGLE_SHEET_NAME)
        
        try:
            worksheet = sheet.worksheet(source_name)
        except gspread.WorksheetNotFound:
            # === СОЗДАНИЕ ЛИСТА С ПРАВИЛЬНЫМИ ЗАГОЛОВКАМИ ===
            if source_name == "Etender":
                cols = "20"
                headers = [
                    "Дата парсинга", 
                    "Тип анкеты", 
                    "Номер лота", 
                    "Описание товаров", 
                    "Квалификация (Toifa)", 
                    "ИНН Заказчика",  # <--- ТЕПЕРЬ СНАЧАЛА ИНН
                    "Заказчик",       # <--- ПОТОМ ЗАКАЗЧИК
                    "Начальная цена", 
                    "Валюта", 
                    # "Текущая цена" - УБРАНО
                    "Регион", 
                    "Дата начала", 
                    "Срок окончания", 
                    "Срок доставки", 
                    # "Участников" - УБРАНО
                    "Контакты", 
                    "Ссылка"
                ]
            elif source_name == "IT-Market":
                cols = "6"
                headers = ["Дата парсинга", "Заказчик", "Статус", "Задача", "Бюджет", "Ссылка"]
            else:
                # Xarid.uz (стандарт)
                cols = "20"
                headers = [
                    "Дата парсинга", "Тип анкеты", "Номер лота", "Описание товаров", 
                    "Название/Квалификация", "Заказчик", "Начальная цена", "Текущая цена", 
                    "Регион", "Дата начала", "Срок окончания", "Срок доставки", 
                    "Участников", "Контакты", "Ссылка"
                ]
            
            worksheet = sheet.add_worksheet(title=source_name, rows="1000", cols=cols)
            worksheet.append_row(headers)
            try:
                body = {"requests": [{"repeatCell": {"range": {"sheetId": worksheet.id, "startRowIndex": 0, "endRowIndex": 1}, "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}}, "fields": "userEnteredFormat.textFormat.bold"}}]}
                sheet.batch_update(body)
            except: pass
        
        # Преобразуем все данные (int оставляем int, остальное str)
        safe_row = []
        for x in row_data:
            if isinstance(x, float) and x.is_integer():
                safe_row.append(int(x))
            elif x is None:
                safe_row.append("")
            else:
                safe_row.append(x)

        worksheet.append_row(safe_row, value_input_option="USER_ENTERED")
        print(f"✅ [Google] Записано в лист '{source_name}'")
        
    except Exception as e:
        print(f"⚠️ Ошибка Google Sheets ({source_name}): {e}")

# ==========================================
# === 4. ПАРСИНГ ETENDER (ФИНАЛЬНЫЙ) ===
# ==========================================

async def get_etender_details(page, link):
    """
    Детальный парсинг Etender (Строгий фильтр: только названия лотов "1 - Название")
    """
    data = {
        "customer": "Не указан", "inn": "Не указан", "contact": "Не указан", 
        "start_date": "Не указана", "end_date": "Не указана", 
        "delivery_term": "Не указан", "items_desc": "Тендер", "toifa": "Тендер",
        "participants": "0" 
    }
    
    try:
        # === 1. ОПИСАНИЕ (СТРОГИЙ РЕЖИМ) ===
        items_list = []
        try:
            # Ищем все возможные контейнеры с названиями
            # .lot__products__item - это наиболее точный класс для списка товаров на Etender
            # h4, .lot-title - запасные варианты
            titles = page.locator(".lot__products__item, h4, h5, .card-title, .lot-title")
            count = await titles.count()
            
            for i in range(count):
                raw_text = await titles.nth(i).inner_text()
                
                # Разбиваем текст на отдельные строки (так как в одном блоке может быть и название, и цена)
                lines = raw_text.split('\n')
                
                for line in lines:
                    clean_line = line.strip()
                    # === ГЛАВНЫЙ ФИЛЬТР ===
                    # Ищем строки, которые начинаются строго с "Цифра" + " - "
                    # Пример: "1 - Запасные части..."
                    # ^ - начало строки, \d+ - цифры, \s* - пробелы, - - тире
                    if re.match(r'^\d+\s*-\s+', clean_line):
                        # Дополнительная защита: если строка слишком короткая (менее 5 симв), это мусор
                        if len(clean_line) > 5:
                            items_list.append(clean_line)
            
            # Если нашли товары по шаблону - сохраняем
            if items_list: 
                # Убираем дубликаты (dict.fromkeys) и берем первые 10
                unique_items = list(dict.fromkeys(items_list))
                data["items_desc"] = "\n".join(unique_items[:10])
            else:
                # ЗАПАСНОЙ ВАРИАНТ: Если шаблона "1 - ..." нет, берем заголовок H1
                h1 = page.locator("h1").first
                if await h1.count() > 0: 
                    data["items_desc"] = (await h1.inner_text()).strip()

        except Exception as e:
            # print(f"Error parsing desc: {e}")
            pass

        # === 2. КВАЛИФИКАЦИЯ (TOIFA) ===
        try:
            toifa_el = page.locator("td:nth-child(4)").first
            if await toifa_el.count() > 0: 
                data["toifa"] = (await toifa_el.inner_text()).strip()
        except: pass

        # Получаем весь текст для остальных полей
        raw_text = await page.inner_text("body")
        clean_text = " ".join(raw_text.split())

        # === 3. ЗАКАЗЧИК ===
        cust_match = re.search(
            r"(?:Buyurtmachi nomi|Name of the customer|Наименование заказчика)[\s:]+([^\n\r]+?)(?:Buyurtmachi|Telefon|Manzil|Address|Stir|Rasmiylashtirish|Takliflarni|Ishtirokchi|Eng yaxshi|$)", 
            clean_text, 
            re.IGNORECASE
        )
        if cust_match: 
            data["customer"] = cust_match.group(1).strip()[:150]

        # 4. ИНН
        inn_match = re.search(r"(?:STIR|INN|ИНН)[\s:]+(\d{9})", clean_text, re.IGNORECASE)
        if inn_match: data["inn"] = inn_match.group(1)

        # 5. ДАТЫ
        start_match = re.search(r"(?:Boshlanish|Start|Начало)[\w\W]{0,60}?(\d{2}[.-]\d{2}[.-]\d{4}(?:\s*\d{2}:\d{2})?)", clean_text, re.IGNORECASE)
        if start_match: data["start_date"] = start_match.group(1)
        
        end_match = re.search(r"(?:Tugash|End|Окончани|Muddat)[\w\W]{0,60}?(\d{2}[.-]\d{2}[.-]\d{4}(?:\s*\d{2}:\d{2})?)", clean_text, re.IGNORECASE)
        if end_match: data["end_date"] = end_match.group(1)

        # 6. КОНТАКТЫ
        phone_match = re.search(r"(?:Telefon|Phone|Телефон)[\s:]+([+\d\(\)\s-]{9,20})", clean_text, re.IGNORECASE)
        if phone_match: data["contact"] = phone_match.group(1).strip()

        # 7. СРОК ПОСТАВКИ
        deliv_match = re.search(r"(?:Muddati|Yetkazib|Delivery|Срок)[\s:]+([\w\d\s]+?)(?:kun|day|oy|мес|$)", clean_text, re.IGNORECASE)
        if deliv_match: data["delivery_term"] = deliv_match.group(0).strip()

    except Exception as e: pass
    return data

async def parse_etender(page):
    url = "https://etender.uzex.uz/lots/1/0"
    source_name = "Etender"
    print(f"🔸 Проверяю {source_name}...")
    
    # === СПИСОК РАЗРЕШЕННЫХ КАТЕГОРИЙ ===
    ALLOWED_TOIFA = [
        "Оборудование компьютерное, электронное и оптическое",
        "Оборудование электрическое",
        "Продукты программные", 
        "услуги по разработке программного обеспечения",
        "Консультационные и аналогические услуги в области информационных технологий",
        "Услуги в области информационных технологий"
    ]
    
    try:
        await page.goto(url, timeout=90000, wait_until="networkidle")
        try: await page.wait_for_selector("a[href^='/lot/']", timeout=20000)
        except: return

        page_num = 1
        while page_num <= MAX_PAGES_PER_RUN:
            lot_links = page.locator("a[href^='/lot/']")
            count = await lot_links.count()
            print(f"🔎 Etender: Страница {page_num}, найдено ссылок: {count}")
            if count == 0: break
            new_items = 0
            
            all_links = []
            for i in range(count):
                href = await lot_links.nth(i).get_attribute("href")
                all_links.append(f"https://etender.uzex.uz{href}")

            for full_link in all_links:
                try:
                    if check_exists(full_link): continue

                    detail_page = await page.context.new_page()
                    await detail_page.goto(full_link, wait_until="networkidle")
                    
                    full_page_text = await detail_page.inner_text("body")
                    clean_page_text = " ".join(full_page_text.split())
                    
                    details = await get_etender_details(detail_page, full_link)

                    # === ФИЛЬТР ПО КАТЕГОРИЯМ (TOIFA) ===
                    # Проверяем, содержит ли 'toifa' одну из разрешенных фраз
                    current_toifa = details['toifa'].lower()
                    is_allowed_category = any(cat.lower() in current_toifa for cat in ALLOWED_TOIFA)

                    if not is_allowed_category:
                        # Если категория не подходит, закрываем и пропускаем
                        # print(f"🚫 Пропуск (Категория): {details['toifa']}") 
                        await detail_page.close()
                        continue
                    # ====================================

                    start_price_raw = "0"
                    currency_code = "UZS"
                    price_regex = r"(\d[\d\s,.]+)\s*(UZS|USD|RUB|EUR|so.?m|сум|ye)"
                    
                    context_match = re.search(r"(?:Boshlang|Start|Начальная|Бюджет)[\w\W]{0,50}?" + price_regex, clean_page_text, re.IGNORECASE)
                    if context_match:
                        start_price_raw = context_match.group(1).strip()
                        currency_code = context_match.group(2).upper().strip()
                    else:
                        simple_match = re.search(price_regex, clean_page_text)
                        if simple_match:
                            start_price_raw = simple_match.group(1).strip()
                            currency_code = simple_match.group(2).upper().strip()

                    if "SO" in currency_code or "СУМ" in currency_code: currency_code = "UZS"
                    if "YE" in currency_code: currency_code = "USD"

                    start_price_num = parse_price_to_number(start_price_raw)
                    if len(str(int(start_price_num))) > 15: start_price_num = 0.0
                    
                    limit = MIN_PRICE_LIMIT
                    if currency_code != "UZS": limit = 100

                    if start_price_num < limit: 
                        await detail_page.close(); continue

                    await detail_page.close()

                    lot_id = full_link.split("/")[-1]
                    region = next((r for r in REGIONS_LIST if r.lower() in clean_page_text.lower()), "Не указан")
                    
                    sheet_start_price = int(start_price_num) if start_price_num > 0 else 0
                    
                    full_price_db = f"{start_price_num} {currency_code}"
                    full_desc = f"Tender||{region}||{currency_code}"
                    add_tender_direct(source_name, f"Лот №{lot_id}", full_desc, full_price_db, details['start_date'], details['end_date'], full_link)
                    
                    print(f"🔥 [Etender] Новый: {lot_id} | {start_price_num} {currency_code}")

                    msg = (
                        f"<b>Тип анкеты: Тендер</b>\nИсточник: etender.uzex.uz\n\n"
                        f"🔢 <b>Номер лота:</b> {lot_id}\n"
                        f"📂 <b>Описание:</b> {details['items_desc'][:200]}...\n"
                        f"📁 <b>Квалификация:</b> {details['toifa']}\n"
                        f"📍 <b>Район:</b> {region}\n"
                        f"📅 <b>Дата начала:</b> {details['start_date']}\n"
                        f"⏳ <b>Срок окончания:</b> {details['end_date']}\n"
                        f"💰 <b>Бюджет:</b> {format_price_str(str(start_price_num))} {currency_code}\n"
                        f"🔗 <b>Ссылка:</b> {full_link}\n\n"
                        f"🏢 <b>Заказчик:</b> {details['customer']}\n"
                        f"🔢 <b>ИНН:</b> {details['inn']}\n"
                        f"📞 <b>Контакты:</b> {details['contact']}"
                    )
                    await send_notification_to_channel(msg, source_name, DEFAULT_PHOTO_PATH)
                    
                    # === ЗАПИСЬ В GOOGLE SHEETS ===
                    save_to_google_sheet("Etender", [
                        datetime.now().strftime("%d.%m.%Y %H:%M"), 
                        "Тендер", 
                        lot_id, 
                        details['items_desc'], 
                        details['toifa'], 
                        details['inn'],      
                        details['customer'], 
                        sheet_start_price, 
                        currency_code, 
                        region, 
                        details['start_date'], 
                        details['end_date'], 
                        details['delivery_term'], 
                        details['contact'], 
                        full_link
                    ])
                    new_items += 1

                except Exception as e:
                    continue
            
            if new_items == 0 and page_num > 1: break
            try:
                next_btn = page.locator("li.pagination-next a").first
                if await next_btn.is_visible(): await next_btn.click(); await page.wait_for_timeout(5000); page_num += 1
                else: break
            except: break
    except: pass

# ==========================================
# === 5. PARSING LOGIC: XARID.UZ (ORIGINAL) ===
# ==========================================

async def get_xarid_details(page, link):
    data = {"customer": "Не указан", "contact": "Не указан", "participants": "0", "start_date": "Не указана", "end_date": "Не указана", "delivery_term": "Не указан", "items_desc": "Не указано"}
    try:
        await page.goto(link, timeout=45000, wait_until="domcontentloaded"); await page.wait_for_timeout(2500)
        raw_text = await page.inner_text("body")
        found_items = []; raw_items = re.findall(r"(?:^|\n)\s*(?:\d+[.\s]*)?([^\n]+?)\s*\(\d{2}\.\d{2}\.\d{2}[\.\d-]*\)", raw_text)
        if raw_items:
            for idx, item_name in enumerate(raw_items):
                if len(item_name.strip()) > 2: found_items.append(f"{idx + 1}. {item_name.strip()}")
            if found_items: data["items_desc"] = "\n".join(found_items)
        
        for word in ["Texnik yordam", "Call-markaz", "Ishonch telefoni", "Техническая поддержка"]:
            if word in raw_text: raw_text = raw_text.split(word)[0]
        clean_oneline = " ".join(raw_text.split())

        # ИСПРАВЛЕНО: Теперь group(1) - это значение, group(0) - все совпадение
        cust_match = re.search(r"(?:Buyurtmachining\s*nomi|Наименование\s*заказчика)\s*:?\s*(.*?)(?:Boshlanish|Start|Дата|Manzil|Адрес)", clean_oneline, re.IGNORECASE)
        if cust_match: data["customer"] = cust_match.group(1).strip()

        phone_match = re.search(r"Bog.?lanish\s*uchun\s*:?\s*([+\d\(\)\s-]{7,25})", clean_oneline, re.IGNORECASE)
        if phone_match and any(c.isdigit() for c in phone_match.group(1)): data["contact"] = phone_match.group(1).strip()
        
        # ИСПРАВЛЕНО: Теперь group(1) - это значение
        deliv_match = re.search(r"(?:Yetkazib\s*berish\s*muddati|Срок\s*поставки)\s*:?\s*(.*?)(?:Fayl|Status|Статус)", clean_oneline, re.IGNORECASE)
        if deliv_match: data["delivery_term"] = deliv_match.group(1).strip()

        start_match = re.search(r"(?:Boshlanish\s*sanasi|Дата\s*начала).*?(\d{2}\.\d{2}\.\d{4}\s*\d{2}:\d{2}(?::\d{2})?)", clean_oneline, re.IGNORECASE)
        if start_match: data["start_date"] = start_match.group(1)
        end_match = re.search(r"(?:Tugash\s*sanasi|Дата\s*окончания).*?(\d{2}\.\d{2}\.\d{4}\s*\d{2}:\d{2}(?::\d{2})?)", clean_oneline, re.IGNORECASE)
        if end_match: data["end_date"] = end_match.group(1)
        part_match = re.search(r"(?:Ishtirokchilar\s*soni|Участники).*?(\d+)", clean_oneline, re.IGNORECASE)
        if part_match: data["participants"] = part_match.group(1)
    except: pass
    return data


async def parse_xarid_uz(page):
    url = "https://xarid.uzex.uz/auction"
    source_name = "Xarid.uz"
    print(f"🔸 Checking {source_name}...")
    try:
        await page.goto(url, timeout=90000, wait_until="domcontentloaded")
        page_num = 1
        while page_num <= MAX_PAGES_PER_RUN:
            try: await page.wait_for_selector(".lot-item", timeout=15000); items = page.locator(".lot-item"); count = await items.count()
            except: break
            if count == 0: break
            new_items = 0
            for i in range(count):
                item = items.nth(i)
                try:
                    full_text = await item.inner_text(); clean_text = " ".join(full_text.split())
                    if not any(k.lower() in clean_text.lower() for k in TARGET_KEYWORDS): continue
                    match_id = re.search(r'Lot\s*raqami:\s*(\d+)', clean_text, re.IGNORECASE); lot_id = match_id.group(1) if match_id else "00000"
                    
                    # === 1. НАЧАЛЬНАЯ ЦЕНА ===
                    start_pattern = r"(?:Boshlang.?ich\s*narx|Начальная\s*стоимость|Стартовая\s*стоимость|Начальная\s*цена)[^\d]*([\d\s,.]+)"
                    match_p = re.search(start_pattern, clean_text, re.IGNORECASE)
                    start_price_raw = match_p.group(1) if match_p else "0"
                    start_price_num = parse_price_to_number(start_price_raw)
                    if start_price_num < MIN_PRICE_LIMIT: continue 
                    start_price_str = format_price_str(start_price_raw)

                    full_link = f"https://xarid.uzex.uz/auction/detail/{lot_id[-6:]}"
                    if check_exists(full_link): continue

                    # === 2. ТЕКУЩАЯ ЦЕНА (ИСПРАВЛЕНО: не захватывать даты) ===
                    # Ищем цену только в пределах 20 символов после слов "Текущая цена", чтобы не улететь на дату
                    curr_pattern = r"(?:Joriy\s*narx|Текущая\s*цена|Лучшее\s*предложение)[^\d\n]{0,20}([\d\s,.]+)"
                    match_c = re.search(curr_pattern, clean_text, re.IGNORECASE)
                    
                    current_price_num = 0.0
                    current_price_str = "Нет ставок"
                    
                    if match_c: 
                        raw_curr = match_c.group(1)
                        # Доп. проверка: если в строке больше одной точки, это скорее всего дата (25.12.2025)
                        if raw_curr.count('.') < 2:
                            current_price_num = parse_price_to_number(raw_curr)
                            current_price_str = format_price_str(raw_curr)

                    # ПОДГОТОВКА ДЛЯ EXCEL (INT, без .0)
                    sheet_start_price = int(start_price_num) if start_price_num > 0 else 0
                    sheet_current_price = int(current_price_num) if current_price_num > 0 else 0

                    detail_page = await page.context.new_page()
                    details = await get_xarid_details(detail_page, full_link)
                    await detail_page.close()
                    
                    toifa = "Не указана"
                    if "Toifa:" in full_text: toifa = full_text.split("Toifa:")[1].split("\n")[0].strip()
                    region = "Не указан"
                    for reg in REGIONS_LIST:
                        if reg.lower() in clean_text.lower(): region = reg; break

                    real_end = details['end_date'] if details['end_date'] != "Не указана" else "-"
                    real_start = details['start_date'] if details['start_date'] != "Не указана" else "-"
                    
                    full_desc = f"{toifa}||{region}||{current_price_str}"
                    add_tender_direct(source_name, f"Лот №{lot_id}", full_desc, f"{start_price_num} UZS", real_start, real_end, full_link)
                    print(f"🔥 [Xarid] New: {lot_id}")
                    
                    msg = (f"<b>Тип анкеты: Аукцион</b>\nИсточник: xarid.uz\n\n🔢 <b>Номер лота:</b> {lot_id}\n📂 <b>Квалификация:</b> {toifa}\n📍 <b>Район:</b> {region}\n📅 <b>Дата начала:</b> {real_start}\n⏳ <b>Срок окончания:</b> {real_end}\n🚚 <b>Срок доставки:</b> {details['delivery_term']}\n💰 <b>Начальная цена:</b> {start_price_str} UZS\n📉 <b>Текущая цена:</b> {current_price_str}\n🔗 <b>Ссылка:</b> {full_link}\n\n🏢 <b>Заказчик:</b> {details['customer']}\n📞 <b>Контакты:</b> {details['contact']}\n👥 <b>Участников:</b> {details['participants']}\n📦 <b>Товары:</b>\n{details['items_desc'][:300]}...")
                    await send_notification_to_channel(msg, source_name, DEFAULT_PHOTO_PATH)
                    
                    save_to_google_sheet("Xarid.uz", [
                        datetime.now().strftime("%d.%m.%Y %H:%M"), "Аукцион", lot_id, 
                        details['items_desc'], toifa, details['customer'], 
                        sheet_start_price,   # Исправлено на INT
                        sheet_current_price, # Исправлено на INT
                        region, real_start, real_end, 
                        details['delivery_term'], details['participants'], details['contact'], full_link
                    ])
                    new_items += 1
                except: continue
            if new_items == 0 and page_num > 1: break
            try:
                next_btn = page.locator(".pagination-next, .ui-paginator-next").first
                if await next_btn.is_visible(): await next_btn.click(); await page.wait_for_timeout(3000); page_num += 1
                else: break
            except: break
    except: pass

# ==========================================
# === 6. PARSING LOGIC: IT-MARKET ===
# ==========================================

async def parse_it_market(page):
    url = "https://it-market.uz/order/"
    source_name = "IT-Market"
    print(f"🔹 Checking {source_name}...")
    try:
        await page.goto(url, timeout=60000, wait_until="networkidle")
        cards = page.locator(".animated-card")
        if await cards.count() == 0: return
        for i in range(await cards.count()):
            card = cards.nth(i)
            try:
                link_loc = card.locator(".stretched-link")
                if await link_loc.count() > 0: href = await link_loc.get_attribute("href"); full_link = f"https://it-market.uz{href}"
                else: full_link = url
                if check_exists(full_link): continue
                lines = (await card.inner_text()).split('\n'); lines = [l.strip() for l in lines if l.strip()]
                if len(lines) < 3: continue
                company, status, title = lines[0], (lines[1] if len(lines) > 1 else ""), (lines[2] if len(lines) > 2 else "Без названия")
                price_str = "Договорная"
                for k, line in enumerate(lines):
                    if "Бюджет" in line and k+3 < len(lines): price_str = format_price_str(lines[k+3]); break
                add_tender_direct(source_name, title, company, price_str, "-", "-", full_link)
                print(f"🔥 [IT-Market] New: {title}")
                msg = (f"<b>Тип анкеты: IT Заказ</b>\n\n🏢 <b>Заказчик:</b> {company}\nℹ️ <b>Статус:</b> {status}\n🛠 <b>Задача:</b> {title}\n💰 <b>Бюджет:</b> {price_str}\n🔗 <b>Ссылка:</b> {full_link}")
                await send_notification_to_channel(msg, source_name, DEFAULT_PHOTO_PATH)
                save_to_google_sheet("IT-Market", [datetime.now().strftime("%d.%m.%Y %H:%M"), company, status, title, price_str, full_link])
            except: continue
    except: pass

async def parser_loop():
    print("🚀 Parser started in background...")
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(viewport={'width': 1920, 'height': 1080})
        page = await context.new_page()
        while True:
            await parse_xarid_uz(page)
            await parse_etender(page)
            await parse_it_market(page)
            print("💤 Parsing paused for 5 minutes...")
            await asyncio.sleep(300)

# ==========================================
# === 7. TELEGRAM BOT LOGIC ===
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
        return (f"📢 *Новый лот на Xarid.uz*\n\n1. *Номер лота:* {lot_number}\n2. *Квалификация:* {toifa}\n3. *Район:* {region}\n4. *Срок окончания:* {end}\n5. *Начальная цена:* {price}\n6. *Текущая цена:* {current_price}\n7. *Ссылка:* {link}")
    
    elif source == "Etender":
        region = "Не указан"
        currency = ""
        if desc and "||" in desc:
             parts = desc.split("||")
             if len(parts) >= 2: region = parts[1]
             if len(parts) >= 3: currency = parts[2]
        lot_number = title.replace("Лот №", "").replace("Лот ", "")
        
        display_price = price if " " in price else f"{price} {currency}"
        return (f"📢 *Новый Тендер на Etender*\n\n1. *Номер лота:* {lot_number}\n2. *Район:* {region}\n3. *Начало:* {start}\n4. *Конец:* {end}\n5. *Бюджет:* {display_price}\n6. *Ссылка:* {link}")

    else:
        display_title = title if title else "Без названия"
        company, status = desc, "-"
        if desc and "(" in desc and desc.strip().endswith(")"):
            try: company, status = desc.rsplit(" (", 1); status = status.replace(")", "")
            except: pass
        return (f"📢 *Новый заказ на {source}*\n\n🏢 *Заказчик:* {company}\nℹ️ *Статус:* {status}\n🛠 *Задача:* {display_title}\n\n💰 *Бюджет:* {price}\n📅 *Начало:* {start}\n🏁 *Дедлайн:* {end}\n\n🔗 [Посмотреть подробнее]({link})")

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
        text = f"🔢 *{display_title}*\n🏛 {source}\n💰 {price}\n🔗 {link}" if source in ["Xarid.uz", "Etender"] else f"🛠 *{display_title}*\n🏛 {source}\n💰 {price}\n🔗 [Открыть]({link})"
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
        elif source == "Etender":
             if "6. Ссылка:" in old_text and link not in old_text: restored_text = old_text.replace("6. Ссылка:", f"6. Ссылка: {link}")
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
# === 8. MAIN (STARTUP) ===
# ==========================================

async def main():
    print("🚀 Starting initialization...")
    try: init_db()
    except Exception as e:
        print(f"❌ CRITICAL DB ERROR: {e}")
        return
    print("🤖 Starting Bot and Parser...")
    asyncio.create_task(parser_loop())
    await dp.start_polling(bot)

if __name__ == "__main__":
    try: asyncio.run(main())
    except (KeyboardInterrupt, SystemExit): print("Bot stopped!")
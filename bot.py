# bot.py - ВЕРСІЯ 20 PRODUCTION
# ВИПРАВЛЕННЯ: rate_student_menu тепер показує всі completed уроки з оцінками - ТЕСТОВА ВЕРСІЯ З ОКРЕМОЮ БД
import sqlite3
import re
import logging
import os
from datetime import datetime, timedelta
from contextlib import contextmanager
from io import BytesIO

from telegram import Update, ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import (
    ApplicationBuilder, 
    CommandHandler, 
    MessageHandler, 
    ContextTypes, 
    CallbackQueryHandler,
    filters
)
import pytz
from openpyxl import Workbook
from openpyxl.styles import Font, Alignment, PatternFill

# ==================== PRODUCTION КОНФІГУРАЦІЯ ====================
# PRODUCTION БОТ TOKEN
TOKEN = "8337801301:AAGXhZVzyoqjED_taA2qqgtaxg8eGeFqiWQ"
ADMIN_ID = 669706811  # Твій Telegram ID
TIMEZONE = "Europe/Kyiv"

# БАЗА ДАНИХ НА PERSISTENT DISK
# Якщо є диск /var/data - використовуємо його, якщо ні - локально
import os
if os.path.exists("/var/data"):
    DB_NAME = "/var/data/driving_school.db"  # На Render з Persistent Disk
    print("✅ Використовую Persistent Disk: /var/data/driving_school.db")
else:
    DB_NAME = "driving_school.db"  # Локально для розробки
    print("⚠️ Persistent Disk не знайдено, використовую локальну БД")
# ==================================================================

# Робочі години
WORK_HOURS_START = 8
WORK_HOURS_END = 18
# Ціни за годину
PRICES = {
    "1 година": 400,
    "2 години": 800
}
from database import (
    init_db, 
    init_lessons_table, 
    init_students_table,
    migrate_database,
    get_instructors_by_transmission,
    get_instructor_by_name,
    get_instructor_by_telegram_id,
    get_instructor_rating,
    get_db as _original_get_db,  # Імпортуємо оригінальний
    init_schedule_blocks_table,
    get_instructor_stats_period,
    get_admin_report_by_instructors,
    get_all_instructors,
    register_student,
    get_student_by_telegram_id,
    update_lesson,
    add_lesson_rating
)

# ======================= HELPER FUNCTIONS =======================
def add_instructor_rating(lesson_id, rating, feedback=""):
    """Додати оцінку та коментар інструктора для учня"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE lessons
                SET instructor_rating = ?, instructor_feedback = ?
                WHERE id = ?
            """, (rating, feedback, lesson_id))
            conn.commit()
            return True
    except Exception as e:
        logger.error(f"Error in add_instructor_rating: {e}", exc_info=True)
        return False

# Перевизначаємо get_db для тестової БД
@contextmanager
def get_db():
    """Context manager для роботи з тестовою БД"""
    conn = sqlite3.connect(DB_NAME)
    try:
        yield conn
    finally:
        conn.close()

# Monkey patch для database модуля щоб використовував тестову БД
import database
database.DB_NAME = DB_NAME
database.get_db = get_db

# Налаштування логування
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[
        logging.FileHandler('bot.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

TZ = pytz.timezone(TIMEZONE)

# ======================= AUTO-ADD INSTRUCTORS =======================
def ensure_instructors_exist():
    """Автоматично додає інструкторів якщо їх немає в базі"""
    instructors = [
        (646703680, 'Мартович Владислав', '+380684232133', 'Автомат', 450),
        (5077103081, 'Фірсов Артур', '+38666619757', 'Механіка', 550),
        (197658460, 'Урядко Артур', '+380502380725', 'Механіка', 550),
        (669706811, 'Будункевич Віктор', '+380936879999', 'Автомат', 450),
        (2042857396, 'Будункевич Мирослав', '+380982534001', 'Механіка', 450),
        (5140435045, 'Блажевський Ігор', '+380664009381', 'Механіка', 550),
        (1846725989, 'Рекетчук Богдан', '+380501591448', 'Механіка', 550),
        (831664827, 'Данилишин Святослав', '+380960755539', 'Механіка', 550)
    ]
    
    with get_db() as conn:
        cursor = conn.cursor()
        added = 0
        
        for telegram_id, name, phone, transmission, price in instructors:
            cursor.execute("SELECT id FROM instructors WHERE telegram_id = ?", (telegram_id,))
            if not cursor.fetchone():
                cursor.execute("""
                    INSERT INTO instructors (telegram_id, name, phone, transmission_type, price_per_hour, is_active, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (telegram_id, name, phone, transmission, price, 1, datetime.now()))
                logger.info(f"✅ Додано інструктора: {name} ({transmission})")
                added += 1
        
        if added > 0:
            conn.commit()
            logger.info(f"🎉 Автоматично додано {added} інструкторів")
        else:
            logger.info("ℹ️ Всі інструктори вже є в базі")

def is_instructor(telegram_id):
    """Перевіряє чи є користувач інструктором"""
    instructor = get_instructor_by_telegram_id(telegram_id)
    return instructor is not None

# ======================= HELPERS =======================
def get_next_dates(days=14, instructor_name=None):
    """Генерує список дат на найближчі N днів з кількістю вільних годин"""
    dates = []
    today = datetime.now().date()
    
    for i in range(days):
        date = today + timedelta(days=i)
        date_formatted = date.strftime('%d.%m.%Y')
        
        # Форматуємо дату: "Пн 13.12.2024"
        weekday = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Нд"][date.weekday()]
        
        # ✅ ДОДАНО: Виділення вихідних кольоровими блоками
        if date.weekday() == 5:  # Субота
            weekday_display = f"🟦 {weekday}"
        elif date.weekday() == 6:  # Неділя
            weekday_display = f"🟥 {weekday}"
        else:
            weekday_display = weekday
        
        # Якщо передано інструктора - рахуємо вільні години
        if instructor_name:
            free_slots = get_available_time_slots(instructor_name, date_formatted)
            free_count = len(free_slots)
            
            # Показуємо тільки дні з вільними годинами
            if free_count > 0:
                formatted = f"{weekday_display} {date.strftime('%d.%m')} ({free_count})"
                dates.append(formatted)
        else:
            # Без інструктора - просто дата
            formatted = f"{weekday_display} {date.strftime('%d.%m.%Y')}"
            dates.append(formatted)
    
    return dates

def get_available_time_slots(instructor_name, date_str):
    """Отримати вільні часові слоти для інструктора"""
    try:
        instructor_data = get_instructor_by_name(instructor_name)
        if not instructor_data:
            return []
        
        instructor_id = instructor_data[0]
        
        # Перевіряємо чи це сьогодні (з правильною таймзоною)
        date_obj = datetime.strptime(date_str, "%d.%m.%Y")
        now = datetime.now(TZ)
        is_today = date_obj.date() == now.date()
        
        # Всі можливі слоти
        all_slots = []
        
        # Якщо це сьогодні - слоти мають бути мінімум через 1 годину
        if is_today:
            # Поточний час + 1 година
            min_time = now + timedelta(hours=1)
            min_hour = min_time.hour
            
            # Якщо є хвилини - починаємо з наступної години
            if min_time.minute > 0:
                min_hour += 1
            
            start_hour = max(min_hour, WORK_HOURS_START)
        else:
            start_hour = WORK_HOURS_START
        
        hour = start_hour
        while hour < WORK_HOURS_END:
            all_slots.append(f"{hour:02d}:00")
            hour += 1
        
        # Перевіряємо які зайняті
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT time, duration FROM lessons
                WHERE instructor_id = ? AND date = ? AND status = 'active'
            """, (instructor_id, date_str))
            booked = cursor.fetchall()
        
        # Створюємо список зайнятих годин з урахуванням тривалості
        blocked_hours = set()
        for booked_time, duration in booked:
            if ':' not in booked_time:
                continue
            
            start_h = int(booked_time.split(':')[0])
            
            # Визначаємо скільки годин займає заняття
            if "1.5" in duration:
                hours_blocked = 2  # 1.5 години блокує 2 слоти
            elif "2" in duration:
                hours_blocked = 2
            else:
                hours_blocked = 1
            
            # Блокуємо всі години заняття
            for i in range(hours_blocked):
                blocked_hours.add(f"{start_h + i:02d}:00")
        
        # Перевіряємо заблоковані інструктором
        from database import is_time_blocked
        date_formatted = date_obj.strftime("%Y-%m-%d")
        
        free_slots = [
            slot for slot in all_slots 
            if slot not in blocked_hours
            and not is_time_blocked(instructor_id, date_formatted, slot)
        ]
        
        return free_slots
        
    except Exception as e:
        logger.error(f"Помилка get_available_time_slots: {e}")
        return []

# ======================= VALIDATORS =======================
def validate_phone(phone):
    """Валідація українського номера"""
    clean = re.sub(r'[\s\-\(\)]', '', phone)
    patterns = [
        r'^(\+?38)?0\d{9}$',
        r'^\d{10}$'
    ]
    return any(re.match(p, clean) for p in patterns)

def validate_date_format(date_str):
    """Валідація формату дати"""
    try:
        datetime.strptime(date_str, "%d.%m.%Y")
        return True
    except ValueError:
        return False

def is_admin(user_id):
    """Перевірка чи користувач є адміном"""
    return user_id == ADMIN_ID

# ======================= START =======================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Головне меню"""
    user_id = update.message.from_user.id
    logger.info(f"🟢 START викликано! User: {user_id}, Args: {context.args}")
    
    # Обробка deep links для реєстрації
    if context.args:
        command = context.args[0]
        logger.info(f"🔗 Deep link виявлено: {command}")
        if command == "register450":
            logger.info("➡️ Перенаправлення на register_450")
            await register_450(update, context)
            return
        elif command == "register550":
            logger.info("➡️ Перенаправлення на register_550")
            await register_550(update, context)
            return
    
    context.user_data.clear()

    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT id FROM instructors WHERE telegram_id = ?", (user_id,))
            is_instructor = cursor.fetchone() is not None

        # Для інструкторів - показуємо панель
        if is_instructor:
            keyboard = [
                [KeyboardButton("🚗 Автомат"), KeyboardButton("🚙 Механіка")],
                [KeyboardButton("📅 Мій розклад")],
                [KeyboardButton("⚙️ Управління графіком")],
                [KeyboardButton("📊 Моя статистика")],
                [KeyboardButton("❌ Історія скасувань")],
                [KeyboardButton("⭐ Оцінити учня")]
            ]
            text = "Привіт! 👋 Я бот *Автоінструктор*.\n\n👨‍🏫 *Панель інструктора*\n\nОберіть дію:"
            
            if is_admin(user_id):
                keyboard.append([KeyboardButton("🔐 Панель адміна")])
                text += "\n🔐 *Панель адміністратора*"
            
            context.user_data["state"] = "waiting_for_transmission"
            
            await update.message.reply_text(
                text,
                reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True),
                parse_mode="Markdown"
            )
        else:
            # Для учнів - спочатку перевіряємо чи зареєстрований
            student = get_student_by_telegram_id(user_id)
            
            if student:
                # Вже зареєстрований - показуємо меню
                context.user_data["student_name"] = student[1]
                context.user_data["student_phone"] = student[2]
                context.user_data["student_tariff"] = student[3]
                
                keyboard = [
                    [KeyboardButton("🚀 Записатися на заняття")],
                    [KeyboardButton("📋 Мої записи")],
                    [KeyboardButton("❌ Скасувати запис")],
                    [KeyboardButton("📊 Моя статистика")]
                ]
                
                await update.message.reply_text(
                    f"Привіт, {student[1]}! 👋\n\n"
                    f"💰 Ваш тариф: {student[3]} грн/год\n\n"
                    f"Що бажаєте зробити?",
                    reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
                )
            else:
                # Не зареєстрований - пропонуємо зареєструватися через посилання
                await update.message.reply_text(
                    "⚠️ *Для запису на заняття потрібна реєстрація*\n\n"
                    "Зверніться до адміністратора за посиланням для реєстрації.\n\n"
                    "📞 Контакт: @ваш\\_адмін",
                    parse_mode="Markdown"
                )
        
    except Exception as e:
        logger.error(f"Error in start: {e}", exc_info=True)
        await update.message.reply_text("❌ Виникла помилка. Спробуйте /start")

# ======================= REGISTRATION COMMANDS =======================
async def register_450(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Реєстрація учня з тарифом 450 грн"""
    logger.info("🔵 register_450 викликано!")
    try:
        await register_student_with_tariff(update, context, 450)
    except Exception as e:
        logger.error(f"❌ Помилка в register_450: {e}", exc_info=True)

async def register_550(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Реєстрація учня з тарифом 550 грн"""
    await register_student_with_tariff(update, context, 550)

async def register_student_with_tariff(update: Update, context: ContextTypes.DEFAULT_TYPE, tariff: int):
    """Загальна функція реєстрації учня"""
    user = update.message.from_user
    user_id = user.id
    logger.info(f"🟡 register_student_with_tariff викликано! User: {user_id}, Tariff: {tariff}")
    
    # Перевіряємо чи вже зареєстрований
    student = get_student_by_telegram_id(user_id)
    
    if student:
        logger.info(f"✅ Учень вже зареєстрований: {student[1]}")
        await update.message.reply_text(
            f"✅ Ви вже зареєстровані!\n\n"
            f"👤 Ім'я: {student[1]}\n"
            f"💰 Тариф: {student[3]} грн/год\n\n"
            f"Використовуйте /start для запису на заняття."
        )
        return
    
    # Автозаповнення імені та номера телефону
    auto_name = f"{user.first_name or ''} {user.last_name or ''}".strip()
    auto_phone = user.username if user.username else ""
    logger.info(f"📝 Автозаповнення: name={auto_name}, username={auto_phone}")
    
    context.user_data["registration_tariff"] = tariff
    context.user_data["auto_name"] = auto_name
    context.user_data["state"] = "registration_name"
    
    keyboard = []
    if auto_name:
        keyboard.append([KeyboardButton(f"✅ {auto_name}")])
    keyboard.append([KeyboardButton("🔙 Скасувати")])
    
    logger.info(f"💬 Відправляю запит на введення імені")
    await update.message.reply_text(
        f"🎓 *Реєстрація учня*\n"
        f"💰 Тариф: *{tariff} грн/год*\n\n"
        f"Введіть ваше ім'я та прізвище:",
        reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True),
        parse_mode="Markdown"
    )

# ======================= HANDLE MESSAGE =======================
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Головний обробник"""
    text = update.message.text
    state = context.user_data.get("state", "")
    user_id = update.message.from_user.id
    
    logger.info(f"📥 Message: '{text}' | State: '{state}'")
    
    try:
        # === ОЦІНЮВАННЯ ІНСТРУКТОРА УЧНЕМ ===
        # Крок 1: Отримання оцінки
        if text in ["⭐", "⭐⭐", "⭐⭐⭐", "⭐⭐⭐⭐", "⭐⭐⭐⭐⭐"]:
            lesson_data = context.bot_data.get(f"rating_lesson_{user_id}")
            
            if lesson_data:
                # Визначаємо оцінку
                rating_map = {
                    "⭐": 1,
                    "⭐⭐": 2,
                    "⭐⭐⭐": 3,
                    "⭐⭐⭐⭐": 4,
                    "⭐⭐⭐⭐⭐": 5
                }
                rating = rating_map.get(text, 5)
                
                # Зберігаємо оцінку в БД
                with get_db() as conn:
                    cursor = conn.cursor()
                    cursor.execute("""
                        UPDATE lessons
                        SET rating = ?
                        WHERE id = ?
                    """, (rating, lesson_data['lesson_id']))
                    conn.commit()
                
                # Зберігаємо оцінку в контексті для можливого коментаря
                context.bot_data[f"rating_feedback_{user_id}"] = {
                    'lesson_id': lesson_data['lesson_id'],
                    'instructor_name': lesson_data['instructor_name'],
                    'rating': rating
                }
                
                # Видаляємо попередній контекст
                del context.bot_data[f"rating_lesson_{user_id}"]
                
                # Встановлюємо стан для коментаря
                context.user_data["state"] = "rating_feedback"
                
                # Запитуємо коментар
                keyboard = [
                    [KeyboardButton("✍️ Написати коментар")],
                    [KeyboardButton("⏭️ Пропустити")]
                ]
                
                await update.message.reply_text(
                    f"✅ *Дякуємо за оцінку!*\n"
                    f"⭐ Оцінка: {rating}/5\n\n"
                    f"💬 Хочете залишити коментар?",
                    reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True),
                    parse_mode="Markdown"
                )
                
                logger.info(f"✅ Учень {user_id} оцінив урок {lesson_data['lesson_id']}: {rating}/5")
                return
            else:
                logger.warning(f"⚠️ Учень {user_id} надіслав оцінку але немає lesson_data")
        
        # Пропуск оцінювання
        if text == "⏭️ Пропустити" and f"rating_lesson_{user_id}" in context.bot_data:
            lesson_data = context.bot_data.get(f"rating_lesson_{user_id}")
            del context.bot_data[f"rating_lesson_{user_id}"]
            
            await update.message.reply_text(
                f"✅ Дякуємо!\n\n"
                f"📅 {lesson_data['date']} {lesson_data['time']}\n"
                f"👨‍🏫 {lesson_data['instructor_name']}"
            )
            
            logger.info(f"⏭️ Учень {user_id} пропустив оцінювання уроку {lesson_data['lesson_id']}")
            await start(update, context)
            return
        
        # Крок 2: Обробка запиту на коментар
        if state == "rating_feedback":
            feedback_data = context.bot_data.get(f"rating_feedback_{user_id}")
            
            if text == "✍️ Написати коментар":
                # Просимо ввести коментар
                context.user_data["state"] = "rating_feedback_input"
                
                keyboard = [[KeyboardButton("⏭️ Пропустити")]]
                
                await update.message.reply_text(
                    "💬 Введіть ваш коментар:",
                    reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
                )
                return
            
            elif text == "⏭️ Пропустити":
                # Пропускаємо коментар
                if feedback_data:
                    del context.bot_data[f"rating_feedback_{user_id}"]
                    context.user_data.clear()
                    
                    await update.message.reply_text(
                        f"✅ *Дякуємо за відгук!*\n\n"
                        f"👨‍🏫 {feedback_data['instructor_name']}\n"
                        f"⭐ Оцінка: {feedback_data['rating']}/5",
                        parse_mode="Markdown"
                    )
                    
                    logger.info(f"⏭️ Учень {user_id} пропустив коментар для уроку {feedback_data['lesson_id']}")
                    await start(update, context)
                    return
        
        # Крок 3: Збереження коментаря
        if state == "rating_feedback_input":
            feedback_data = context.bot_data.get(f"rating_feedback_{user_id}")
            
            if text == "⏭️ Пропустити":
                # Пропускаємо коментар
                if feedback_data:
                    del context.bot_data[f"rating_feedback_{user_id}"]
                    context.user_data.clear()
                    
                    await update.message.reply_text(
                        f"✅ *Дякуємо за відгук!*\n\n"
                        f"👨‍🏫 {feedback_data['instructor_name']}\n"
                        f"⭐ Оцінка: {feedback_data['rating']}/5",
                        parse_mode="Markdown"
                    )
                    
                    logger.info(f"⏭️ Учень {user_id} пропустив коментар для уроку {feedback_data['lesson_id']}")
                    await start(update, context)
                    return
            else:
                # Зберігаємо коментар
                feedback_text = text
                
                with get_db() as conn:
                    cursor = conn.cursor()
                    cursor.execute("""
                        UPDATE lessons
                        SET feedback = ?
                        WHERE id = ?
                    """, (feedback_text, feedback_data['lesson_id']))
                    conn.commit()
                
                del context.bot_data[f"rating_feedback_{user_id}"]
                context.user_data.clear()
                
                await update.message.reply_text(
                    f"✅ *Дякуємо за відгук!*\n\n"
                    f"👨‍🏫 {feedback_data['instructor_name']}\n"
                    f"⭐ Оцінка: {feedback_data['rating']}/5\n"
                    f"💬 \"{feedback_text}\"",
                    parse_mode="Markdown"
                )
                
                logger.info(f"✅ Учень {user_id} залишив коментар для уроку {feedback_data['lesson_id']}")
                await start(update, context)
                return
        
        # === РЕЄСТРАЦІЯ УЧНЯ ===
        if state == "registration_name":
            if text == "🔙 Скасувати":
                await update.message.reply_text("❌ Реєстрацію скасовано.")
                return
            
            context.user_data["student_name"] = text
            context.user_data["state"] = "registration_phone"
            
            # Запит на номер телефону
            keyboard = [[KeyboardButton("📱 Надати номер", request_contact=True)]]
            keyboard.append([KeyboardButton("🔙 Скасувати")])
            
            await update.message.reply_text(
                "📱 Тепер надайте ваш номер телефону:\n"
                "(натисніть кнопку нижче або введіть вручну)",
                reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=True)
            )
            return
        
        if state == "registration_phone":
            if text == "🔙 Скасувати":
                await update.message.reply_text("❌ Реєстрацію скасовано.")
                return
            
            # Обробка контакту або текстового номера
            phone = None
            if update.message.contact:
                phone = update.message.contact.phone_number
            elif validate_phone(text):
                phone = text
            else:
                await update.message.reply_text("⚠️ Невірний формат номера. Спробуйте ще раз:")
                return
            
            # Зберігаємо учня
            user_id = update.message.from_user.id
            name = context.user_data["student_name"]
            tariff = context.user_data["registration_tariff"]
            
            if register_student(name, phone, user_id, tariff, f"link_{tariff}"):
                # Кнопка для переходу до бронювання
                keyboard = [
                    [KeyboardButton("🚀 Записатися на заняття")],
                    [KeyboardButton("📋 Мої записи")]
                ]
                
                await update.message.reply_text(
                    f"✅ *Реєстрацію завершено!*\n\n"
                    f"👤 Ім'я: {name}\n"
                    f"📱 Телефон: {phone}\n"
                    f"💰 Ваш тариф: *{tariff} грн/год* (фіксований)\n\n"
                    f"ℹ️ Тариф закріплений за вами і не змінюється.\n\n"
                    f"Натисніть кнопку нижче, щоб записатися на заняття:",
                    reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True),
                    parse_mode="Markdown"
                )
            else:
                await update.message.reply_text("❌ Помилка реєстрації. Спробуйте пізніше.")
            
            context.user_data.clear()
            return

        # === ПАНЕЛЬ АДМІНА ===
        if text == "🔐 Панель адміна":
            if not is_admin(update.message.from_user.id):
                await update.message.reply_text("❌ У вас немає доступу.")
                return
            await show_admin_panel(update, context)
            return
        
        if text == "📥 Експорт в Excel":
            await show_export_period_menu(update, context)
            return
        
        if state == "admin_panel":
            await handle_admin_report(update, context)
            return
        
        if state == "admin_report_period":
            await handle_admin_report(update, context)
            return
        
        # === ЕКСПОРТ З ВИБОРОМ ПЕРІОДУ ===
        if state == "export_period":
            await handle_export_period_choice(update, context)
            return
        
        if state == "export_custom_period":
            await handle_export_custom_period(update, context)
            return

        # === МЕНЮ ІНСТРУКТОРА ===
        # Перевірка кнопки Назад для інструктора
        if text == "🔙 Назад":
            # Якщо інструктор - повертаємо в головне меню
            with get_db() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT id FROM instructors WHERE telegram_id = ?", (user_id,))
                is_instructor = cursor.fetchone() is not None
            
            if is_instructor:
                await start(update, context)
                return
        
        if text == "📅 Мій розклад":
            await show_instructor_schedule(update, context)
            return
        elif text == "⚙️ Управління графіком":
            await manage_schedule(update, context)
            return
        elif text == "📊 Моя статистика":
            # Перевіряємо чи це інструктор
            with get_db() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT id FROM instructors WHERE telegram_id = ?", (user_id,))
                is_instructor = cursor.fetchone() is not None
            
            if is_instructor:
                await show_instructor_stats_menu(update, context)
            else:
                await show_student_statistics(update, context)
            return
        elif text == "❌ Історія скасувань":
            await show_cancellation_history(update, context)
            return
        elif text == "⭐ Оцінити учня":
            await rate_student_menu(update, context)
            return

        # === СТАТИСТИКА ЗА ПЕРІОД ===
        if state == "stats_period":
            await handle_stats_period(update, context)
            return
        
        if state == "stats_custom_period":
            await handle_stats_custom_period(update, context)
            return
        
        # === ОЦІНЮВАННЯ УЧНЯ ===
        if state in ["rating_select_lesson", "rating_give_score", "rating_give_feedback"]:
            await handle_rating_flow(update, context)
            return

        # === КОРИГУВАННЯ ГРАФІКУ ===
        if state in ["edit_schedule_select", "edit_schedule_date", "edit_schedule_time"]:
            await handle_edit_schedule(update, context)
            return

        # === УПРАВЛІННЯ ГРАФІКОМ ===
        if state in ["schedule_menu", "block_choose_date", "block_choose_time_start", 
                     "block_choose_time_end", "block_choose_reason", "unblock_choose_date"]:
            await handle_schedule_management(update, context)
            return

        # === МЕНЮ СТУДЕНТА ===
        if text == "🚀 Записатися на заняття":
            # Показати вибір типу коробки
            keyboard = [
                [KeyboardButton("🚗 Автомат"), KeyboardButton("🚙 Механіка")]
            ]
            context.user_data["state"] = "waiting_for_transmission"
            
            await update.message.reply_text(
                "🚗 Оберіть тип коробки передач:",
                reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
            )
            return
        
        if text == "📖 Мої записи" or text == "📋 Мої записи":
            await show_student_lessons(update, context)
            return
        
        if text == "❌ Скасувати запис":
            await show_lessons_to_cancel(update, context)
            return
        
        # === СКАСУВАННЯ ЗАПИСУ ===
        if state == "cancel_lesson_select":
            await handle_cancel_lesson(update, context)
            return
        
        if state == "cancel_lesson_confirm":
            await handle_cancel_confirmation(update, context)
            return
        
        # === ПІДТВЕРДЖЕННЯ ===
        if state == "waiting_for_confirmation":
            if text == "✅ Підтвердити":
                await save_lesson(update, context)
                return
            elif text in ["💬 Додати коментар", "✏️ Змінити коментар"]:
                # Переходимо в режим введення коментаря
                context.user_data["state"] = "waiting_for_booking_comment"
                
                keyboard = [
                    [KeyboardButton("⏭️ Пропустити")],
                    [KeyboardButton("🔙 Назад")]
                ]
                
                await update.message.reply_text(
                    "💬 *Введіть коментар для інструктора:*\n\n"
                    "_Наприклад:_\n"
                    "• \"Перше заняття\"\n"
                    "• \"буду чекати в Тисмениці\"\n"
                    "• \"практичний іспиту\"",
                    reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True),
                    parse_mode="Markdown"
                )
                return
            elif text == "🔙 Скасувати":
                await update.message.reply_text("❌ Запис скасовано.")
                await start(update, context)
                return
        
        # === ВВЕДЕННЯ КОМЕНТАРЯ ===
        if state == "waiting_for_booking_comment":
            if text == "🔙 Назад":
                # Повертаємось до підтвердження
                context.user_data["state"] = "waiting_for_confirmation"
                await show_booking_confirmation(update, context)
                return
            elif text == "⏭️ Пропустити":
                # Видаляємо коментар якщо був
                context.user_data["booking_comment"] = ""
                await show_booking_confirmation(update, context)
                return
            else:
                # Зберігаємо коментар
                context.user_data["booking_comment"] = text
                
                await update.message.reply_text(
                    f"✅ Коментар збережено!\n\n"
                    f"💬 \"{text}\""
                )
                
                # Повертаємось до підтвердження
                await show_booking_confirmation(update, context)
                return
        
        # === ВИБІР КОРОБКИ ===
        if state == "waiting_for_transmission":
            if text not in ["🚗 Автомат", "🚙 Механіка"]:
                await update.message.reply_text("⚠️ Оберіть коробку передач із меню.")
                return
            
            transmission = "Автомат" if text == "🚗 Автомат" else "Механіка"
            context.user_data["transmission"] = transmission
            context.user_data["state"] = "waiting_for_instructor"

            instructors = get_instructors_by_transmission(transmission)
            if not instructors:
                await update.message.reply_text("😔 Немає інструкторів для цього типу.")
                return

            keyboard = []
            for instructor in instructors:
                rating = get_instructor_rating(instructor)
                if rating > 0:
                    stars = "⭐" * int(rating)
                    keyboard.append([f"{instructor} {stars} ({rating:.1f})"])
                else:
                    keyboard.append([f"{instructor} 🆕"])
            
            keyboard.append([KeyboardButton("🔙 Назад")])
            
            await update.message.reply_text(
                "👨‍🏫 Оберіть інструктора:",
                reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
            )
            return
        
        # === ВИБІР ІНСТРУКТОРА ===
        if state == "waiting_for_instructor":
            logger.info(f"👨‍🏫 Обробка вибору інструктора: {text}")
            
            if text == "🔙 Назад":
                await start(update, context)
                return
            
            instructor_name = text.split(" ⭐")[0].split(" 🆕")[0]
            context.user_data["instructor"] = instructor_name
            context.user_data["state"] = "waiting_for_date"
            
            logger.info(f"✅ Інструктор обраний: {instructor_name}")
            logger.info(f"🔄 Стан змінено на: waiting_for_date")
            
            # Генеруємо дати на 14 днів вперед з кількістю вільних годин
            dates = get_next_dates(14, instructor_name)
            
            # Робимо кнопки по 2 в рядку
            keyboard = []
            for i in range(0, len(dates), 2):
                row = [KeyboardButton(dates[i])]
                if i + 1 < len(dates):
                    row.append(KeyboardButton(dates[i + 1]))
                keyboard.append(row)
            
            keyboard.append([KeyboardButton("🔙 Назад")])
            
            await update.message.reply_text(
                f"📅 Оберіть дату заняття:",
                reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
            )
            return
        
        # === ВИБІР ДАТИ ===
        if state == "waiting_for_date":
            logger.info(f"🔵 Обробка дати: {text}")
            
            if text == "🔙 Назад":
                # Повертаємось до вибору інструктора
                transmission = context.user_data.get("transmission")
                instructors = get_instructors_by_transmission(transmission)
                
                context.user_data["state"] = "waiting_for_instructor"
                
                keyboard = []
                for instructor in instructors:
                    rating = get_instructor_rating(instructor)
                    if rating > 0:
                        stars = "⭐" * int(rating)
                        keyboard.append([f"{instructor} {stars} ({rating:.1f})"])
                    else:
                        keyboard.append([f"{instructor} 🆕"])
                
                keyboard.append([KeyboardButton("🔙 Назад")])
                
                await update.message.reply_text(
                    "👨‍🏫 Оберіть інструктора:",
                    reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
                )
                return
            
            # ПЕРЕВІРКА: чи це кнопка з датою, а не вручну введена дата
            valid_date_markers = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Нд", "🟦", "🟥"]
            if not any(marker in text for marker in valid_date_markers):
                logger.warning(f"⚠️ Спроба ввести дату вручну: {text}")
                await update.message.reply_text(
                    "⚠️ Будь ласка, оберіть дату з кнопок нижче.\n\n"
                    "Якщо потрібної дати немає у списку - зверніться до адміністратора або оберіть іншого інструктора."
                )
                return
            
            # Витягуємо дату з формату "Пн 13.12 (3)", "🟦 Сб 13.12 (3)" або "Пн 13.12.2024"
            date_parts = text.split()
            
            # Перевіряємо чи є емодзі на початку (🟦 або 🟥)
            if len(date_parts) >= 3 and date_parts[0] in ["🟦", "🟥"]:
                # Формат: "🟦 Сб 10.01 (5)" → беремо date_parts[2]
                date_candidate = date_parts[2].replace("(", "").replace(")", "")  # "10.01"
            elif len(date_parts) >= 2:
                # Формат: "Пн 13.12 (3)" або "Пн 13.12.2024" → беремо date_parts[1]
                date_candidate = date_parts[1]  # "13.12" або "13.12.2024"
            else:
                date_str = text  # Якщо ввели вручну "13.12.2024"
                date_candidate = None
            
            if date_candidate:
                # Якщо короткий формат "13.12" - додаємо рік
                if date_candidate.count('.') == 1:
                    current_year = datetime.now().year
                    date_str = f"{date_candidate}.{current_year}"
                else:
                    date_str = date_candidate
            
            logger.info(f"📆 Витягнута дата: {date_str}")
            
            if not validate_date_format(date_str):
                logger.warning(f"⚠️ Невірний формат дати: {date_str}")
                await update.message.reply_text(
                    "⚠️ Невірний формат дати. Оберіть дату з меню."
                )
                return
            
            date_obj = datetime.strptime(date_str, "%d.%m.%Y")
            today = datetime.now(TZ).date()  # З ТАЙМЗОНОЮ!
            if date_obj.date() < today:
                logger.warning(f"⚠️ Минула дата: {date_str} (сьогодні: {today})")
                await update.message.reply_text("⚠️ Неможливо записатися на минулу дату.")
                return
            
            context.user_data["date"] = date_str
            instructor = context.user_data["instructor"]
            logger.info(f"✅ Дата валідна: {date_str}, інструктор: {instructor}")
            
            logger.info(f"🔍 Шукаю вільні слоти...")
            free_slots = get_available_time_slots(instructor, date_str)
            logger.info(f"📊 Знайдено {len(free_slots)} вільних слотів: {free_slots}")
            
            if not free_slots:
                logger.warning(f"⚠️ Немає вільних місць на {date_str}")
                await update.message.reply_text(
                    "😔 На цю дату немає вільних місць.\n"
                    "Оберіть іншу дату:"
                )
                return
            
            context.user_data["state"] = "waiting_for_time"
            
            # Робимо кнопки часу по 3 в рядку
            keyboard = []
            for i in range(0, len(free_slots), 3):
                row = []
                for j in range(3):
                    if i + j < len(free_slots):
                        row.append(KeyboardButton(free_slots[i + j]))
                keyboard.append(row)
            
            keyboard.append([KeyboardButton("🔙 Назад")])
            
            logger.info(f"💬 Відправляю список часів")
            await update.message.reply_text(
                "🕐 Оберіть час заняття:",
                reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
            )
            return
        
        # === ВИБІР ЧАСУ ===
        if state == "waiting_for_time":
            if text == "🔙 Назад":
                context.user_data["state"] = "waiting_for_date"
                await update.message.reply_text("📅 Введіть іншу дату (ДД.ММ.РРРР):")
                return
            
            # ПЕРЕВІРКА: чи це час з кнопок (формат HH:MM)
            if not re.match(r'^([0-1][0-9]|2[0-3]):[0-5][0-9]$', text):
                logger.warning(f"⚠️ Невірний формат часу: {text}")
                await update.message.reply_text(
                    "⚠️ Будь ласка, оберіть час з кнопок нижче.\n\n"
                    "Якщо потрібного часу немає - оберіть іншу дату або інструктора."
                )
                return
            
            # ПЕРЕВІРКА: чи час є серед вільних слотів
            instructor = context.user_data.get("instructor")
            date = context.user_data.get("date")
            free_slots = get_available_time_slots(instructor, date)
            
            if text not in free_slots:
                logger.warning(f"⚠️ Час {text} не входить у вільні слоти: {free_slots}")
                await update.message.reply_text(
                    "⚠️ Цей час недоступний. Будь ласка, оберіть час з доступних варіантів."
                )
                return
            
            context.user_data["time"] = text
            context.user_data["state"] = "waiting_for_duration"
            
            keyboard = [
                [KeyboardButton("1 година")],
                [KeyboardButton("2 години")],
                [KeyboardButton("🔙 Назад")]
            ]
            
            await update.message.reply_text(
                "⏱ Оберіть тривалість заняття:",
                reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
            )
            return
        
        # === ВИБІР ТРИВАЛОСТІ ===
        if state == "waiting_for_duration":
            if text == "🔙 Назад":
                instructor = context.user_data["instructor"]
                date = context.user_data["date"]
                free_slots = get_available_time_slots(instructor, date)
                
                context.user_data["state"] = "waiting_for_time"
                
                # Робимо кнопки часу по 3 в рядку
                keyboard = []
                for i in range(0, len(free_slots), 3):
                    row = []
                    for j in range(3):
                        if i + j < len(free_slots):
                            row.append(KeyboardButton(free_slots[i + j]))
                    keyboard.append(row)
                
                keyboard.append([KeyboardButton("🔙 Назад")])
                
                await update.message.reply_text(
                    "🕐 Оберіть час заняття:",
                    reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
                )
                return
            
            if text not in ["1 година", "2 години"]:
                await update.message.reply_text("⚠️ Оберіть тривалість із меню.")
                return
            
            # Перевірка чи вільні наступні години (для 2 годин)
            if text == "2 години":
                selected_time = context.user_data["time"]
                instructor = context.user_data["instructor"]
                date = context.user_data["date"]
                
                # Перевіряємо чи вільна наступна година
                selected_hour = int(selected_time.split(':')[0])
                next_hour = f"{selected_hour + 1:02d}:00"
                
                free_slots = get_available_time_slots(instructor, date)
                
                if next_hour not in free_slots and next_hour != f"{WORK_HOURS_END:02d}:00":
                    await update.message.reply_text(
                        "⚠️ Наступна година зайнята. Оберіть інший час або 1 годину."
                    )
                    return
            
            context.user_data["duration"] = text
            
            # Перевірка чи учень зареєстрований
            user = update.message.from_user
            student = get_student_by_telegram_id(user.id)
            
            if student:
                # Учень зареєстрований - показуємо підтвердження
                context.user_data["student_name"] = student[1]
                context.user_data["student_phone"] = student[2]
                context.user_data["student_tariff"] = student[3]
                
                await show_booking_confirmation(update, context)
            else:
                # Не зареєстрований - не даємо записатися
                await update.message.reply_text(
                    "⚠️ *Помилка!*\n\n"
                    "Для запису потрібна реєстрація через спеціальне посилання.\n"
                    "Зверніться до адміністратора.",
                    parse_mode="Markdown"
                )
                await start(update, context)
            return
        
        # === ІМ'Я СТУДЕНТА ===
        if state == "waiting_for_name":
            if text == "🔙 Назад":
                # Якщо це була реєстрація через /start (не через тривалість)
                if "duration" not in context.user_data:
                    await start(update, context)
                    return
                
                # Якщо це після вибору тривалості
                context.user_data["state"] = "waiting_for_duration"
                keyboard = [
                    [KeyboardButton("1 година")],
                    [KeyboardButton("2 години")],
                    [KeyboardButton("🔙 Назад")]
                ]
                await update.message.reply_text(
                    "⏱ Оберіть тривалість:",
                    reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
                )
                return
            
            # Якщо натиснули автозаповнення
            if text.startswith("✅ "):
                text = text[2:]
            
            context.user_data["student_name"] = text
            context.user_data["state"] = "waiting_for_phone"
            
            # Пропонуємо поділитися контактом
            keyboard = [[KeyboardButton("📱 Надати номер", request_contact=True)]]
            keyboard.append([KeyboardButton("🔙 Назад")])
            
            await update.message.reply_text(
                "📱 Введіть номер телефону або натисніть кнопку нижче:",
                reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=True)
            )
            return
        
        # === ТЕЛЕФОН СТУДЕНТА ===
        if state == "waiting_for_phone":
            if text == "🔙 Назад":
                user = update.message.from_user
                auto_name = f"{user.first_name or ''} {user.last_name or ''}".strip()
                keyboard = []
                if auto_name:
                    keyboard.append([KeyboardButton(f"✅ {auto_name}")])
                keyboard.append([KeyboardButton("🔙 Назад")])
                
                context.user_data["state"] = "waiting_for_name"
                await update.message.reply_text(
                    "👤 Введіть ваше ім'я:",
                    reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
                )
                return
            
            # Обробка контакту або текстового номера
            phone = None
            if update.message.contact:
                phone = update.message.contact.phone_number
            elif validate_phone(text):
                phone = text
            else:
                await update.message.reply_text("⚠️ Невірний формат номера. Спробуйте ще раз:")
                return
            
            context.user_data["student_phone"] = phone
            
            # Якщо це реєстрація через /start (не через бронювання)
            if "duration" not in context.user_data:
                # Переходимо до вибору коробки
                context.user_data["state"] = "waiting_for_transmission"
                
                keyboard = [
                    [KeyboardButton("🚗 Автомат"), KeyboardButton("🚙 Механіка")]
                ]
                
                await update.message.reply_text(
                    "✅ Дякую! Тепер оберіть тип коробки передач:",
                    reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
                )
            else:
                # Якщо це під час бронювання - показуємо підтвердження
                await show_booking_confirmation(update, context)
            return
        
    except Exception as e:
        logger.error(f"Error in handle_message: {e}", exc_info=True)
        await update.message.reply_text("❌ Виникла помилка. Спробуйте /start")

async def show_booking_confirmation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показати підтвердження бронювання"""
    instructor = context.user_data["instructor"]
    date = context.user_data["date"]
    time = context.user_data["time"]
    duration = context.user_data["duration"]
    name = context.user_data.get("student_name", "")
    phone = context.user_data.get("student_phone", "")
    student_tariff = context.user_data.get("student_tariff", 0)
    booking_comment = context.user_data.get("booking_comment", "")
    
    # Розрахунок вартості на основі тарифу учня
    if student_tariff > 0:
        # Якщо є тариф учня - використовуємо його
        if "2" in duration:
            price = student_tariff * 2
        else:
            price = student_tariff
    else:
        # Якщо немає тарифу - стандартні ціни
        price = PRICES.get(duration, 400)
    
    context.user_data["state"] = "waiting_for_confirmation"
    
    # Формуємо текст
    text = (
        f"📋 *Підтвердження запису*\n\n"
        f"👨‍🏫 Інструктор: {instructor}\n"
        f"📅 Дата: {date}\n"
        f"🕐 Час: {time}\n"
        f"⏱ Тривалість: {duration}\n"
        f"💰 Вартість: {price:.0f} грн\n"
    )
    
    # Додаємо коментар якщо є
    if booking_comment:
        text += f"\n💬 Коментар:\n\"{booking_comment}\"\n"
    
    text += "\nВсе вірно?"
    
    # Кнопки
    keyboard = [
        [KeyboardButton("✅ Підтвердити")]
    ]
    
    # Якщо коментар є - показуємо "Змінити", якщо немає - "Додати"
    if booking_comment:
        keyboard.append([KeyboardButton("✏️ Змінити коментар")])
    else:
        keyboard.append([KeyboardButton("💬 Додати коментар")])
    
    keyboard.append([KeyboardButton("🔙 Скасувати")])
    
    await update.message.reply_text(
        text,
        reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True),
        parse_mode="Markdown"
    )

# ======================= INSTRUCTOR FUNCTIONS =======================
async def show_instructor_schedule(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показати розклад інструктора"""
    user_id = update.message.from_user.id
    
    try:
        instructor_data = get_instructor_by_telegram_id(user_id)
        if not instructor_data:
            await update.message.reply_text("❌ Ви не зареєстровані як інструктор.")
            return
        
        instructor_id, instructor_name = instructor_data
        
        # Поточна дата та час у форматі БД (ДД.ММ.РРРР ГГ:ХХ)
        now = datetime.now(TZ)
        
        with get_db() as conn:
            cursor = conn.cursor()
            # Спочатку отримуємо всі активні заняття
            cursor.execute("""
                SELECT date, time, duration, student_name, student_phone, status, booking_comment
                FROM lessons
                WHERE instructor_id = ? 
                AND status = 'active'
                ORDER BY date, time
            """, (instructor_id,))
            
            all_lessons = cursor.fetchall()
        
        # Фільтруємо майбутні заняття в Python
        lessons = []
        for date, time, duration, student_name, student_phone, status, booking_comment in all_lessons:
            try:
                # Парсимо дату з БД (ДД.ММ.РРРР)
                lesson_datetime = datetime.strptime(f"{date} {time}", "%d.%m.%Y %H:%M")
                lesson_datetime = TZ.localize(lesson_datetime)
                
                # Порівнюємо
                if lesson_datetime >= now:
                    lessons.append((date, time, duration, student_name, student_phone, status, booking_comment))
            except:
                # Якщо не вдалося розпарсити - показуємо всі
                lessons.append((date, time, duration, student_name, student_phone, status, booking_comment))
        
        # Обмежуємо 20 записами
        lessons = lessons[:20]
        
        if not lessons:
            await update.message.reply_text("📋 У вас поки немає запланованих занять.")
            return
        
        text = f"📅 *Ваш розклад:*\n\n"
        current_date = None
        
        for date, time, duration, student_name, student_phone, status, booking_comment in lessons:
            if date != current_date:
                text += f"\n📆 *{date}*\n"
                current_date = date
            
            text += f"🕐 {time} ({duration})\n"
            text += f"👤 {student_name}\n"
            if student_phone:
                text += f"📱 {student_phone}\n"
            if booking_comment:
                text += f"💬 \"{booking_comment}\"\n"
            text += "\n"
        
        # Додаємо тільки кнопку Назад
        keyboard = [
            [KeyboardButton("🔙 Назад")]
        ]
        
        await update.message.reply_text(
            text,
            reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True),
            parse_mode="Markdown"
        )
        
    except Exception as e:
        logger.error(f"Error in show_instructor_schedule: {e}", exc_info=True)
        await update.message.reply_text("❌ Помилка завантаження розкладу.")

async def show_instructor_stats_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Меню вибору періоду для статистики"""
    keyboard = [
        [KeyboardButton("📊 За сьогодні")],
        [KeyboardButton("📊 За тиждень")],
        [KeyboardButton("📊 За місяць")],
        [KeyboardButton("📊 Свій період")],
        [KeyboardButton("🔙 Назад")]
    ]
    
    await update.message.reply_text(
        "📊 Статистика\n\nОберіть період:",
        reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    )
    
    context.user_data["state"] = "stats_period"

async def handle_stats_period(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обробка вибору періоду статистики"""
    text = update.message.text
    user_id = update.message.from_user.id
    
    logger.info(f"📊 handle_stats_period: User {user_id}, Text: '{text}'")
    
    if text == "🔙 Назад":
        await start(update, context)
        return
    
    instructor_data = get_instructor_by_telegram_id(user_id)
    if not instructor_data:
        logger.error(f"❌ Instructor not found for user {user_id}")
        await update.message.reply_text("❌ Помилка.")
        return
    
    instructor_id, instructor_name = instructor_data
    logger.info(f"✅ Instructor found: {instructor_name} (ID: {instructor_id})")
    
    today = datetime.now().date()
    
    if text == "📊 За сьогодні":
        # ВАЖЛИВО: використовуємо ДД.ММ.РРРР бо в lessons.date зберігається саме так!
        date_from = today.strftime("%d.%m.%Y")
        date_to = today.strftime("%d.%m.%Y")
        period_text = "сьогодні"
    elif text == "📊 За тиждень":
        date_from = (today - timedelta(days=7)).strftime("%d.%m.%Y")
        date_to = today.strftime("%d.%m.%Y")
        period_text = "за тиждень"
        logger.info(f"📅 Period: {date_from} - {date_to}")
    elif text == "📊 За місяць":
        date_from = (today - timedelta(days=30)).strftime("%d.%m.%Y")
        date_to = today.strftime("%d.%m.%Y")
        period_text = "за місяць"
        logger.info(f"📅 Period: {date_from} - {date_to}")
    elif text == "📊 Свій період":
        context.user_data["state"] = "stats_custom_period"
        await update.message.reply_text(
            "📅 Введіть період у форматі:\n"
            "ДД.ММ.РРРР - ДД.ММ.РРРР\n\n"
            "Наприклад: 01.11.2024 - 30.11.2024"
        )
        return
    else:
        logger.warning(f"⚠️ Unknown period button: '{text}'")
        await update.message.reply_text("⚠️ Оберіть період із меню.")
        return
    
    logger.info(f"🔄 Calling show_instructor_stats...")
    await show_instructor_stats(update, context, instructor_id, date_from, date_to, period_text)

async def handle_stats_custom_period(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обробка введеного користувачем періоду"""
    text = update.message.text
    user_id = update.message.from_user.id
    
    if text == "🔙 Назад":
        await show_instructor_stats_menu(update, context)
        return
    
    try:
        # Парсимо період: "01.12.2025 - 22.12.2025"
        import re
        match = re.match(r'(\d{2}\.\d{2}\.\d{4})\s*-\s*(\d{2}\.\d{2}\.\d{4})', text)
        
        if not match:
            await update.message.reply_text(
                "⚠️ Невірний формат!\n\n"
                "Використовуйте: ДД.ММ.РРРР - ДД.ММ.РРРР\n"
                "Наприклад: 01.12.2024 - 31.12.2024"
            )
            return
        
        date_from = match.group(1)
        date_to = match.group(2)
        
        # Перевіряємо що дати валідні
        from datetime import datetime
        try:
            datetime.strptime(date_from, "%d.%m.%Y")
            datetime.strptime(date_to, "%d.%m.%Y")
        except ValueError:
            await update.message.reply_text("⚠️ Невірна дата! Перевірте формат.")
            return
        
        instructor_data = get_instructor_by_telegram_id(user_id)
        if not instructor_data:
            await update.message.reply_text("❌ Помилка.")
            return
        
        instructor_id = instructor_data[0]
        period_text = f"{date_from} - {date_to}"
        
        await show_instructor_stats(update, context, instructor_id, date_from, date_to, period_text)
        
    except Exception as e:
        logger.error(f"Error in handle_stats_custom_period: {e}", exc_info=True)
        await update.message.reply_text("❌ Помилка обробки періоду.")

async def show_instructor_stats(update: Update, context: ContextTypes.DEFAULT_TYPE, instructor_id, date_from, date_to, period_text):
    """Показати статистику інструктора"""
    try:
        stats = get_instructor_stats_period(instructor_id, date_from, date_to)
        
        if not stats:
            await update.message.reply_text("❌ Помилка отримання статистики.")
            return
        
        text = f"📊 Статистика {period_text}\n\n"
        text += f"📝 Занять проведено: {stats['total_lessons']}\n"
        text += f"⏱ Годин відпрацьовано: {stats['total_hours']}\n"
        text += f"💰 Заробіток: {stats['earnings']:.0f} грн\n"
        text += f"⭐ Середній рейтинг: {stats['avg_rating']}\n"
        text += f"❌ Скасовано: {stats['cancelled']}\n"
        
        await update.message.reply_text(text)
        
        # Повертаємось в меню статистики
        await show_instructor_stats_menu(update, context)
        
    except Exception as e:
        logger.error(f"Error in show_instructor_stats: {e}", exc_info=True)
        await update.message.reply_text("❌ Помилка.")

async def show_cancellation_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Історія скасувань"""
    user_id = update.message.from_user.id
    
    try:
        instructor_data = get_instructor_by_telegram_id(user_id)
        if not instructor_data:
            await update.message.reply_text("❌ Помилка.")
            return
        
        instructor_id = instructor_data[0]
        
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT date, time, student_name, cancelled_by, cancelled_at
                FROM lessons
                WHERE instructor_id = ? AND status = 'cancelled'
                ORDER BY cancelled_at DESC
                LIMIT 10
            """, (instructor_id,))
            
            cancellations = cursor.fetchall()
        
        if not cancellations:
            await update.message.reply_text("📋 Немає скасованих занять.")
            return
        
        text = "❌ *Історія скасувань:*\n\n"
        
        for date, time, student_name, cancelled_by, cancelled_at in cancellations:
            text += f"📅 {date} {time}\n"
            text += f"👤 {student_name}\n"
            text += f"🚫 Скасував: {cancelled_by}\n"
            if cancelled_at:
                text += f"🕐 {cancelled_at[:16]}\n"
            text += "\n"
        
        await update.message.reply_text(text, parse_mode="Markdown")
        
    except Exception as e:
        logger.error(f"Error in show_cancellation_history: {e}", exc_info=True)
        await update.message.reply_text("❌ Помилка.")

# ======================= RATING FUNCTIONS =======================
async def rate_student_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Меню оцінювання учнів - ВИПРАВЛЕНА ВЕРСІЯ v17"""
    user_id = update.message.from_user.id
    
    try:
        instructor_data = get_instructor_by_telegram_id(user_id)
        if not instructor_data:
            await update.message.reply_text("❌ Ви не інструктор.")
            return
        
        instructor_id = instructor_data[0]
        
        # Отримуємо завершені заняття БЕЗ оцінки ІНСТРУКТОРА
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id, date, time, student_name, rating, feedback
                FROM lessons
                WHERE instructor_id = ? 
                  AND status = 'completed'
                  AND instructor_rating IS NULL
                ORDER BY date DESC, time DESC
                LIMIT 10
            """, (instructor_id,))
            
            lessons = cursor.fetchall()
        
        if not lessons:
            await update.message.reply_text("📋 Немає занять для оцінювання.")
            return
        
        # Зберігаємо в context
        context.user_data["lessons_to_rate"] = lessons
        context.user_data["state"] = "rating_select_lesson"
        
        text = "⭐ *Оберіть заняття для оцінювання:*\n\n"
        keyboard = []
        
        # Показуємо оцінку УЧНЯ якщо є
        for i, (lesson_id, date, time, student_name, rating, feedback) in enumerate(lessons, 1):
            text += f"{i}. {date} {time} - {student_name}\n"
            
            # Показуємо оцінку учня якщо є
            if rating and rating > 0:
                stars = "⭐" * rating
                text += f"   Учень оцінив: {stars} ({rating}/5)\n"
                if feedback:
                    text += f"   💬 \"{feedback}\"\n"
            
            text += "\n"
            keyboard.append([KeyboardButton(f"{i}")])
        
        keyboard.append([KeyboardButton("🔙 Назад")])
        
        await update.message.reply_text(
            text,
            reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True),
            parse_mode="Markdown"
        )
        
    except Exception as e:
        logger.error(f"Error in rate_student_menu: {e}", exc_info=True)
        await update.message.reply_text("❌ Помилка.")


async def handle_rating_flow(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обробка процесу оцінювання"""
    state = context.user_data.get("state")
    text = update.message.text
    
    if text == "🔙 Назад":
        await start(update, context)
        return
    
    if state == "rating_select_lesson":
        try:
            lesson_index = int(text) - 1
            lessons = context.user_data.get("lessons_to_rate", [])
            
            if lesson_index < 0 or lesson_index >= len(lessons):
                await update.message.reply_text("⚠️ Невірний номер. Спробуйте ще раз:")
                return
            
            selected_lesson = lessons[lesson_index]
            context.user_data["rating_lesson_id"] = selected_lesson[0]
            context.user_data["rating_student_name"] = selected_lesson[3]
            context.user_data["state"] = "rating_give_score"
            
            keyboard = [[KeyboardButton(str(i))] for i in range(1, 6)]
            keyboard.append([KeyboardButton("🔙 Назад")])
            
            await update.message.reply_text(
                f"⭐ Оцініть учня *{selected_lesson[3]}*\n\n"
                f"Виберіть оцінку від 1 до 5:",
                reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True),
                parse_mode="Markdown"
            )
            
        except ValueError:
            await update.message.reply_text("⚠️ Введіть номер заняття:")
            return
    
    elif state == "rating_give_score":
        try:
            rating = int(text)
            if rating < 1 or rating > 5:
                await update.message.reply_text("⚠️ Оцінка має бути від 1 до 5:")
                return
            
            context.user_data["rating_score"] = rating
            context.user_data["state"] = "rating_give_feedback"
            
            keyboard = [
                [KeyboardButton("➡️ Пропустити")],
                [KeyboardButton("🔙 Назад")]
            ]
            
            await update.message.reply_text(
                "💬 Додайте коментар (або натисніть 'Пропустити'):",
                reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
            )
            
        except ValueError:
            await update.message.reply_text("⚠️ Введіть число від 1 до 5:")
            return
    
    elif state == "rating_give_feedback":
        feedback = "" if text == "➡️ Пропустити" else text
        
        lesson_id = context.user_data.get("rating_lesson_id")
        rating = context.user_data.get("rating_score")
        student_name = context.user_data.get("rating_student_name")
        
        if add_instructor_rating(lesson_id, rating, feedback):
            await update.message.reply_text(
                f"✅ Оцінку додано!\n\n"
                f"👤 {student_name}\n"
                f"⭐ Оцінка: {rating}/5"
            )
        else:
            await update.message.reply_text("❌ Помилка збереження оцінки.")
        
        context.user_data.clear()
        await start(update, context)

# ======================= EDIT SCHEDULE =======================
async def handle_edit_schedule(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Коригування графіку інструктором"""
    state = context.user_data.get("state")
    text = update.message.text
    
    if text == "🔙 Назад":
        await start(update, context)
        return
    
    if text == "✏️ Коригувати графік":
        user_id = update.message.from_user.id
        instructor_data = get_instructor_by_telegram_id(user_id)
        
        if not instructor_data:
            await update.message.reply_text("❌ Помилка.")
            return
        
        instructor_id = instructor_data[0]
        
        # Отримуємо активні заняття
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id, date, time, student_name
                FROM lessons
                WHERE instructor_id = ? AND status = 'active'
                ORDER BY date, time
                LIMIT 10
            """, (instructor_id,))
            
            lessons = cursor.fetchall()
        
        if not lessons:
            await update.message.reply_text("📋 Немає занять для коригування.")
            return
        
        context.user_data["lessons_to_edit"] = lessons
        context.user_data["state"] = "edit_schedule_select"
        
        text = "✏️ *Оберіть заняття для зміни:*\n\n"
        keyboard = []
        
        for i, (lesson_id, date, time, student_name) in enumerate(lessons, 1):
            text += f"{i}. {date} {time} - {student_name}\n"
            keyboard.append([KeyboardButton(f"{i}")])
        
        keyboard.append([KeyboardButton("🔙 Назад")])
        
        await update.message.reply_text(
            text,
            reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True),
            parse_mode="Markdown"
        )
        return
    
    if state == "edit_schedule_select":
        try:
            lesson_index = int(text) - 1
            lessons = context.user_data.get("lessons_to_edit", [])
            
            if lesson_index < 0 or lesson_index >= len(lessons):
                await update.message.reply_text("⚠️ Невірний номер:")
                return
            
            selected_lesson = lessons[lesson_index]
            context.user_data["edit_lesson_id"] = selected_lesson[0]
            context.user_data["state"] = "edit_schedule_date"
            
            await update.message.reply_text(
                f"📅 Введіть нову дату у форматі *ДД.ММ.РРРР*\n"
                f"Поточна: {selected_lesson[1]}",
                parse_mode="Markdown"
            )
            
        except ValueError:
            await update.message.reply_text("⚠️ Введіть номер:")
            return
    
    elif state == "edit_schedule_date":
        if not validate_date_format(text):
            await update.message.reply_text("⚠️ Невірний формат. Використовуйте ДД.ММ.РРРР:")
            return
        
        context.user_data["edit_new_date"] = text
        context.user_data["state"] = "edit_schedule_time"
        
        await update.message.reply_text("🕐 Введіть новий час у форматі *ГГ:ХХ*", parse_mode="Markdown")
    
    elif state == "edit_schedule_time":
        if not re.match(r'^\d{1,2}:\d{2}$', text):
            await update.message.reply_text("⚠️ Невірний формат. Використовуйте ГГ:ХХ:")
            return
        
        lesson_id = context.user_data.get("edit_lesson_id")
        new_date = context.user_data.get("edit_new_date")
        new_time = text
        
        if update_lesson(lesson_id, date=new_date, time=new_time):
            await update.message.reply_text(
                f"✅ Графік оновлено!\n\n"
                f"📅 Нова дата: {new_date}\n"
                f"🕐 Новий час: {new_time}"
            )
        else:
            await update.message.reply_text("❌ Помилка оновлення.")
        
        context.user_data.clear()
        await start(update, context)

# ======================= SCHEDULE MANAGEMENT =======================
async def manage_schedule(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Управління графіком"""
    keyboard = [
        [KeyboardButton("🔴 Заблокувати час")],
        [KeyboardButton("🟢 Розблокувати час")],
        [KeyboardButton("📋 Мої блокування")],
        [KeyboardButton("🔙 Назад")]
    ]
    
    context.user_data["state"] = "schedule_menu"
    
    await update.message.reply_text(
        "⚙️ *Управління графіком*\n\nОберіть дію:",
        reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True),
        parse_mode="Markdown"
    )

async def handle_schedule_management(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обробка управління графіком"""
    text = update.message.text
    state = context.user_data.get("state")
    
    logger.info(f"🔧 handle_schedule_management: text='{text}', state='{state}'")
    
    if text == "🔙 Назад":
        logger.info("⬅️ Назад натиснуто")
        if state == "schedule_menu":
            await start(update, context)
        else:
            await manage_schedule(update, context)
        return
    
    # ВАЖЛИВО: перевірка кнопок меню має бути перед перевіркою стану!
    if text == "🔴 Заблокувати час":
        logger.info("🔴 Кнопка 'Заблокувати час' натиснута - показую календар")
        context.user_data["state"] = "block_choose_date"
        
        # Генеруємо дати на 30 днів (місяць)
        dates = get_next_dates(30)
        
        # Робимо кнопки по 2 в рядку
        keyboard = []
        for i in range(0, len(dates), 2):
            row = [KeyboardButton(dates[i])]
            if i + 1 < len(dates):
                row.append(KeyboardButton(dates[i + 1]))
            keyboard.append(row)
        
        keyboard.append([KeyboardButton("🔙 Назад")])
        
        await update.message.reply_text(
            "📅 Оберіть дату для блокування (доступно на місяць вперед):",
            reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        )
        return
    
    elif text == "🟢 Розблокувати час":
        logger.info("🟢 Розблокувати час")
        await show_blocks_to_unblock(update, context)
        return
    
    elif text == "📋 Мої блокування":
        logger.info("📋 Мої блокування")
        await show_all_blocks(update, context)
        return
    
    # Тепер обробка станів
    logger.info(f"📍 Перевірка стану: {state}")
    if state == "block_choose_date":
        # Витягуємо дату з формату "Пн 13.12.2024", "🟦 Сб 13.12.2024" тощо
        date_parts = text.split()
        
        # Перевіряємо чи є емодзі на початку (🟦 або 🟥)
        if len(date_parts) >= 3 and date_parts[0] in ["🟦", "🟥"]:
            # Формат: "🟦 Сб 10.01.2024" → беремо date_parts[2]
            date_str = date_parts[2]
        elif len(date_parts) >= 2:
            # Формат: "Пн 13.12.2024" → беремо date_parts[1]
            date_str = date_parts[1]  # "13.12.2024"
        else:
            date_str = text  # Якщо ввели вручну
        
        logger.info(f"📆 Обробка дати блокування: {date_str}")
        
        if not validate_date_format(date_str):
            logger.warning(f"⚠️ Невірний формат дати: {date_str}")
            await update.message.reply_text("⚠️ Невірний формат. Оберіть дату з меню.")
            return
        
        context.user_data["block_date"] = date_str
        context.user_data["state"] = "block_choose_time_start"
        
        # Перевіряємо чи це сьогодні (з правильною таймзоною)
        date_obj = datetime.strptime(date_str, "%d.%m.%Y")
        now = datetime.now(TZ)
        is_today = date_obj.date() == now.date()
        current_hour = now.hour
        
        # Показуємо години для вибору
        keyboard = []
        for hour in range(WORK_HOURS_START, WORK_HOURS_END):
            # Якщо сьогодні - пропускаємо минулі години
            if is_today and hour <= current_hour:
                continue
            keyboard.append([KeyboardButton(f"{hour:02d}:00")])
        
        keyboard.append([KeyboardButton("🔙 Назад")])
        
        logger.info(f"💬 Відправляю вибір часу початку")
        await update.message.reply_text(
            "🕐 Оберіть час початку блокування:",
            reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        )
        return
    
    elif state == "block_choose_time_start":
        logger.info(f"🕐 Обробка часу початку: {text}")
        
        if not re.match(r'^\d{1,2}:\d{2}$', text):
            await update.message.reply_text("⚠️ Невірний формат. Оберіть час з меню.")
            return
        
        context.user_data["block_time_start"] = text
        context.user_data["state"] = "block_choose_time_end"
        
        # Показуємо години для кінця (від початку до 18:00)
        start_hour = int(text.split(':')[0])
        keyboard = []
        for hour in range(start_hour + 1, WORK_HOURS_END + 1):
            keyboard.append([KeyboardButton(f"{hour:02d}:00")])
        
        keyboard.append([KeyboardButton("🔙 Назад")])
        
        logger.info(f"💬 Відправляю вибір часу кінця")
        await update.message.reply_text(
            "🕐 Оберіть час кінця блокування:",
            reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        )
        return
    
    elif state == "block_choose_time_end":
        logger.info(f"🕐 Обробка часу кінця: {text}")
        
        if not re.match(r'^\d{1,2}:\d{2}$', text):
            await update.message.reply_text("⚠️ Невірний формат. Оберіть час з меню.")
            return
        
        context.user_data["block_time_end"] = text
        context.user_data["state"] = "block_choose_reason"
        
        keyboard = [
            [KeyboardButton("➡️ Пропустити")],
            [KeyboardButton("🔙 Назад")]
        ]
        
        logger.info(f"💬 Запитую причину")
        await update.message.reply_text(
            "💬 Введіть причину блокування (або пропустіть):",
            reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        )
        return
    
    elif state == "block_choose_reason":
        reason = "" if text == "➡️ Пропустити" else text
        
        user_id = update.message.from_user.id
        instructor_data = get_instructor_by_telegram_id(user_id)
        
        if not instructor_data:
            await update.message.reply_text("❌ Помилка.")
            return
        
        instructor_id = instructor_data[0]
        block_date = context.user_data["block_date"]
        time_start = context.user_data["block_time_start"]
        time_end = context.user_data["block_time_end"]
        
        
        # НЕ конвертуємо дату - в БД вона як ДД.ММ.РРРР
        date_formatted = block_date
        
        # ПЕРЕВІРКА КОНФЛІКТІВ: чи є уроки в цей час
        def time_to_minutes(time_str):
            h, m = map(int, time_str.split(':'))
            return h * 60 + m
        
        block_start_min = time_to_minutes(time_start)
        block_end_min = time_to_minutes(time_end)
        
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT student_name, student_phone, time, duration, student_tariff
                FROM lessons
                WHERE instructor_id = ? AND date = ? AND status = 'active'
            """, (instructor_id, date_formatted))
            
            lessons = cursor.fetchall()
        
        # Перевіряємо конфлікти
        conflicting_lessons = []
        for student_name, student_phone, lesson_time, duration, tariff in lessons:
            if ':' not in lesson_time:
                continue
            
            lesson_start_min = time_to_minutes(lesson_time)
            
            # Визначаємо тривалість уроку
            if "1.5" in duration:
                lesson_duration = 90
            elif "2" in duration:
                lesson_duration = 120
            else:
                lesson_duration = 60
            
            lesson_end_min = lesson_start_min + lesson_duration
            
            # Перевірка на перетин
            if not (block_end_min <= lesson_start_min or block_start_min >= lesson_end_min):
                conflicting_lessons.append({
                    'name': student_name,
                    'phone': student_phone or "немає",
                    'time': lesson_time,
                    'duration': duration,
                    'tariff': tariff or 0
                })
        
        # Якщо є конфлікти - показуємо попередження
        if conflicting_lessons:
            message = f"❌ Не можна заблокувати!\n\n"
            
            for lesson in conflicting_lessons:
                # Визначаємо час закінчення
                start_h, start_m = map(int, lesson['time'].split(':'))
                if "1.5" in lesson['duration']:
                    end_h, end_m = start_h + 1, start_m + 30
                elif "2" in lesson['duration']:
                    end_h, end_m = start_h + 2, start_m
                else:
                    end_h, end_m = start_h + 1, start_m
                
                message += f"📅 {block_date}, 🕐 {lesson['time']}-{end_h:02d}:{end_m:02d}\n"
                message += f"👤 {lesson['name']} ({lesson['phone']})\n"
                message += f"💵 {lesson['tariff']} грн, {lesson['duration']}\n\n"
            
            message += "Зв'яжіться з учнем для перенесення."
            
            await update.message.reply_text(message)
            context.user_data.clear()
            await manage_schedule(update, context)
            return
        
        # Конвертуємо дату для schedule_blocks (там РРРР-ММ-ДД)
        date_for_block = datetime.strptime(block_date, "%d.%m.%Y").strftime("%Y-%m-%d")

        
        from database import add_schedule_block
        
        if add_schedule_block(instructor_id, date_for_block, time_start, time_end, "blocked", reason):
            await update.message.reply_text(
                f"✅ Час заблоковано!\n\n"
                f"📅 {block_date}\n"
                f"🕐 {time_start} - {time_end}"
            )
        else:
            await update.message.reply_text("❌ Помилка блокування.")
        
        context.user_data.clear()
        await manage_schedule(update, context)

async def show_blocks_to_unblock(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показати блокування для видалення"""
    user_id = update.message.from_user.id
    
    try:
        instructor_data = get_instructor_by_telegram_id(user_id)
        if not instructor_data:
            await update.message.reply_text("❌ Помилка.")
            return
        
        instructor_id = instructor_data[0]
        
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id, date, time_start, time_end, reason
                FROM schedule_blocks
                WHERE instructor_id = ?
                ORDER BY date, time_start
                LIMIT 10
            """, (instructor_id,))
            
            blocks = cursor.fetchall()
        
        if not blocks:
            await update.message.reply_text("📋 Немає блокувань.")
            return
        
        text = "🟢 *Оберіть блокування для видалення:*\n\n"
        buttons = []
        
        for block_id, date, time_start, time_end, reason in blocks:
            text += f"📅 {date} | 🕐 {time_start}-{time_end}\n"
            if reason:
                text += f"💬 {reason}\n"
            text += "\n"
            
            buttons.append([InlineKeyboardButton(
                f"❌ {date} {time_start}-{time_end}",
                callback_data=f"unblock_{block_id}"
            )])
        
        await update.message.reply_text(
            text,
            reply_markup=InlineKeyboardMarkup(buttons),
            parse_mode="Markdown"
        )
        
    except Exception as e:
        logger.error(f"Error in show_blocks_to_unblock: {e}", exc_info=True)
        await update.message.reply_text("❌ Помилка.")

async def show_all_blocks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показати всі блокування"""
    user_id = update.message.from_user.id
    
    try:
        instructor_data = get_instructor_by_telegram_id(user_id)
        if not instructor_data:
            await update.message.reply_text("❌ Помилка.")
            return
        
        instructor_id = instructor_data[0]
        
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT date, time_start, time_end, reason
                FROM schedule_blocks
                WHERE instructor_id = ?
                ORDER BY date, time_start
            """, (instructor_id,))
            
            blocks = cursor.fetchall()
        
        if not blocks:
            await update.message.reply_text("📋 У вас немає заблокованих годин.")
            return
        
        text = "🔴 *Ваші блокування:*\n\n"
        current_date = None
        
        for date, time_start, time_end, reason in blocks:
            if date != current_date:
                text += f"\n📅 *{date}*\n"
                current_date = date
            
            text += f"🕐 {time_start} - {time_end}"
            if reason:
                text += f" | {reason}"
            text += "\n"
        
        await update.message.reply_text(text, parse_mode="Markdown")
        
    except Exception as e:
        logger.error(f"Error in show_all_blocks: {e}", exc_info=True)
        await update.message.reply_text("❌ Помилка.")

# ======================= ADMIN FUNCTIONS =======================
async def show_admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Панель адміністратора"""
    keyboard = [
        [KeyboardButton("📊 Звіт по інструкторах")],
        [KeyboardButton("👥 Список інструкторів")],
        [KeyboardButton("📥 Експорт в Excel")],
        [KeyboardButton("🔙 Назад")]
    ]
    
    await update.message.reply_text(
        "🔐 Панель адміністратора\n\nОберіть дію:",
        reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    )
    
    context.user_data["state"] = "admin_panel"

async def handle_admin_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обробка звітів адміна"""
    text = update.message.text
    
    if text == "🔙 Назад":
        context.user_data.clear()  # Очищаємо стан
        await start(update, context)  # Повертаємось в головне меню
        return
    
    if text == "📊 Звіт по інструкторах":
        keyboard = [
            [KeyboardButton("📊 За тиждень")],
            [KeyboardButton("📊 За місяць")],
            [KeyboardButton("📊 Свій період")],
            [KeyboardButton("🔙 Назад")]
        ]
        
        context.user_data["state"] = "admin_report_period"
        
        await update.message.reply_text(
            "📊 Оберіть період для звіту:",
            reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        )
        return
    
    if text == "👥 Список інструкторів":
        instructors = get_all_instructors()
        
        text = "👥 Список інструкторів:\n\n"
        for i, (inst_id, name, transmission, telegram_id) in enumerate(instructors, 1):
            text += f"{i}. {name} ({transmission})\n"
            text += f"   ID: {telegram_id}\n\n"
        
        await update.message.reply_text(text)
        await show_admin_panel(update, context)
        await update.message.reply_text(text)
        await show_admin_panel(update, context)
        return
    
    # Обробка періоду
    today = datetime.now().date()
    
    if text == "📊 За тиждень":
        date_from = (today - timedelta(days=7)).strftime("%Y-%m-%d")
        date_to = today.strftime("%Y-%m-%d")
        period_text = "за тиждень"
    elif text == "📊 За місяць":
        date_from = (today - timedelta(days=30)).strftime("%Y-%m-%d")
        date_to = today.strftime("%Y-%m-%d")
        period_text = "за місяць"
    elif text == "📊 Свій період":
        await update.message.reply_text(
            "📅 Введіть період у форматі:\n"
            "ДД.ММ.РРРР - ДД.ММ.РРРР\n\n"
            "Наприклад: 01.11.2024 - 30.11.2024"
        )
        context.user_data["state"] = "admin_custom_period"
        return
    else:
        return
    
    await generate_admin_report(update, context, date_from, date_to, period_text)

async def generate_admin_report(update: Update, context: ContextTypes.DEFAULT_TYPE, date_from, date_to, period_text):
    """Генерація звіту для адміна"""
    try:
        report_data = get_admin_report_by_instructors(date_from, date_to)
        
        if not report_data:
            await update.message.reply_text("📋 Немає даних за цей період.")
            return
        
        text = f"📊 Звіт по інструкторах {period_text}\n\n"
        text += f"📅 Період: {date_from} - {date_to}\n\n"
        
        total_lessons = 0
        total_hours = 0
        total_earnings = 0
        
        for name, lessons, hours, avg_rating, cancelled in report_data:
            if lessons > 0:
                hours = hours or 0
                earnings = hours * 400
                
                text += f"👨‍🏫 {name}\n"
                text += f"   📝 Занять: {lessons}\n"
                text += f"   ⏱ Годин: {hours:.1f}\n"
                text += f"   💰 Заробіток: {earnings:.0f} грн\n"
                text += f"   ⭐ Рейтинг: {avg_rating:.1f if avg_rating else 0}\n"
                text += f"   ❌ Скасовано: {cancelled}\n\n"
                
                total_lessons += lessons
                total_hours += hours
                total_earnings += earnings
        
        text += f"\n📊 ЗАГАЛОМ:\n"
        text += f"📝 Занять: {total_lessons}\n"
        text += f"⏱ Годин: {total_hours:.1f}\n"
        text += f"💰 Заробіток: {total_earnings:.0f} грн\n"
        
        await update.message.reply_text(text)
        await show_admin_panel(update, context)
        
    except Exception as e:
        logger.error(f"Error in generate_admin_report: {e}", exc_info=True)
        await update.message.reply_text("❌ Помилка генерації звіту.")

# ======================= STUDENT FUNCTIONS =======================
async def show_student_lessons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показати записи студента"""
    user_id = update.message.from_user.id
    
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT l.date, l.time, l.duration, i.name, i.phone, l.status, l.booking_comment
                FROM lessons l
                JOIN instructors i ON l.instructor_id = i.id
                WHERE l.student_telegram_id = ? AND l.status = 'active'
                ORDER BY l.date, l.time
                LIMIT 10
            """, (user_id,))
            
            lessons = cursor.fetchall()
        
        if not lessons:
            await update.message.reply_text("📋 У вас поки немає записів на заняття.")
            return
        
        text = "📖 Ваші записи:\n\n"
        
        for date, time, duration, instructor_name, instructor_phone, status, booking_comment in lessons:
            text += f"📅 {date} о {time} ({duration})\n"
            text += f"👨‍🏫 {instructor_name} | 📱 {instructor_phone}\n"
            if booking_comment:
                text += f"💬 Ваш коментар: \"{booking_comment}\"\n"
            text += "\n"
        
        await update.message.reply_text(text)
        
    except Exception as e:
        logger.error(f"Error in show_student_lessons: {e}", exc_info=True)
        await update.message.reply_text("❌ Помилка завантаження записів.")

async def show_student_statistics(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показати статистику учня"""
    user_id = update.message.from_user.id
    
    try:
        from datetime import datetime, timedelta
        
        now = datetime.now(TZ)
        today_str = now.strftime("%d.%m.%Y")
        
        with get_db() as conn:
            cursor = conn.cursor()
            
            # ========== ЗАПЛАНОВАНО ==========
            cursor.execute("""
                SELECT COUNT(*), 
                       SUM(CASE 
                           WHEN duration LIKE '%2%' THEN 2
                           WHEN duration LIKE '%1.5%' THEN 1.5
                           ELSE 1
                       END),
                       SUM(CASE 
                           WHEN duration LIKE '%2%' THEN student_tariff * 2
                           ELSE student_tariff
                       END)
                FROM lessons
                WHERE student_telegram_id = ? 
                AND status = 'active'
                AND date >= ?
            """, (user_id, today_str))
            
            planned = cursor.fetchone()
            planned_count = planned[0] or 0
            planned_hours = planned[1] or 0
            planned_cost = planned[2] or 0
            
            # ========== ЗАВЕРШЕНО ==========
            cursor.execute("""
                SELECT COUNT(*), 
                       SUM(CASE 
                           WHEN duration LIKE '%2%' THEN 2
                           WHEN duration LIKE '%1.5%' THEN 1.5
                           ELSE 1
                       END),
                       SUM(CASE 
                           WHEN duration LIKE '%2%' THEN student_tariff * 2
                           ELSE student_tariff
                       END)
                FROM lessons
                WHERE student_telegram_id = ? 
                AND status = 'completed'
            """, (user_id,))
            
            completed = cursor.fetchone()
            completed_count = completed[0] or 0
            completed_hours = completed[1] or 0
            completed_cost = completed[2] or 0
            
            # ========== ПРОГРЕС (НА ОСНОВІ ОЦІНОК) ==========
            cursor.execute("""
                SELECT AVG(rating), COUNT(rating)
                FROM lessons
                WHERE student_telegram_id = ?
                AND status = 'completed'
                AND rating IS NOT NULL
            """, (user_id,))
            
            rating_data = cursor.fetchone()
            avg_rating = rating_data[0] or 0
            rated_lessons = rating_data[1] or 0
            
            # ========== ІНСТРУКТОРИ ==========
            cursor.execute("""
                SELECT i.name, COUNT(*)
                FROM lessons l
                JOIN instructors i ON l.instructor_id = i.id
                WHERE l.student_telegram_id = ?
                AND l.status = 'completed'
                GROUP BY i.name
                ORDER BY COUNT(*) DESC
            """, (user_id,))
            
            instructors = cursor.fetchall()
        
        # ========== ФОРМУВАННЯ ПОВІДОМЛЕННЯ ==========
        text = "📊 Статистика\n\n"
        
        # Заплановано
        text += "▶️ ЗАПЛАНОВАНО\n"
        if planned_count > 0:
            text += f"   {planned_count} {'урок' if planned_count == 1 else 'уроки' if planned_count < 5 else 'уроків'} "
            text += f"({planned_hours:.1f} год) → {planned_cost:,.0f} грн\n\n"
        else:
            text += "   Немає запланованих уроків\n\n"
        
        # Завершено
        text += "✅ ЗАВЕРШЕНО\n"
        if completed_count > 0:
            text += f"   {completed_count} {'урок' if completed_count == 1 else 'уроки' if completed_count < 5 else 'уроків'} "
            text += f"({completed_hours:.1f} год) → {completed_cost:,.0f} грн\n\n"
        else:
            text += "   Поки немає завершених уроків\n\n"
        
        # Прогрес (на основі оцінок)
        if rated_lessons > 0:
            text += "📈 ПРОГРЕС\n"
            text += f"   ⭐ Середня оцінка: {avg_rating:.1f}/5 (за {rated_lessons} {'урок' if rated_lessons == 1 else 'уроки' if rated_lessons < 5 else 'уроків'})\n\n"
        
        # Інструктори
        if instructors:
            text += "👨‍🏫 ІНСТРУКТОРИ\n"
            instructor_names = []
            for name, count in instructors:
                short_name = name.split()[0]  # Тільки ім'я
                instructor_names.append(f"{short_name}: {count}")
            text += f"   {' | '.join(instructor_names)}\n"
        
        await update.message.reply_text(text)
        
    except Exception as e:
        logger.error(f"Error in show_student_statistics: {e}", exc_info=True)
        await update.message.reply_text("❌ Помилка завантаження статистики.")

# ======================= CANCEL LESSON FUNCTIONS =======================
async def show_lessons_to_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показати уроки які можна скасувати"""
    user_id = update.message.from_user.id
    
    try:
        now = datetime.now(TZ)
        
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT l.id, l.date, l.time, l.duration, i.name
                FROM lessons l
                JOIN instructors i ON l.instructor_id = i.id
                WHERE l.student_telegram_id = ? AND l.status = 'active'
                ORDER BY l.date, l.time
                LIMIT 10
            """, (user_id,))
            
            lessons = cursor.fetchall()
        
        if not lessons:
            await update.message.reply_text("📋 У вас немає активних записів на заняття.")
            return
        
        # Фільтруємо уроки - залишаємо тільки ті що >24 години
        cancelable_lessons = []
        
        for lesson_id, date, time, duration, instructor_name in lessons:
            try:
                # Парсимо дату і час уроку
                lesson_datetime = datetime.strptime(f"{date} {time}", "%d.%m.%Y %H:%M")
                lesson_datetime = TZ.localize(lesson_datetime)
                
                # Скільки годин до уроку
                hours_until = (lesson_datetime - now).total_seconds() / 3600
                
                if hours_until >= 24:
                    cancelable_lessons.append((lesson_id, date, time, duration, instructor_name, hours_until))
            except Exception as e:
                logger.error(f"Error parsing lesson time: {e}")
                continue
        
        if not cancelable_lessons:
            await update.message.reply_text(
                "⚠️ Немає уроків які можна скасувати\n\n"
                "Скасування можливе мінімум за 24 години до уроку."
            )
            return
        
        # Зберігаємо в context
        context.user_data["cancelable_lessons"] = cancelable_lessons
        context.user_data["state"] = "cancel_lesson_select"
        
        text = "❌ *Скасування запису*\n\n"
        text += "Оберіть урок для скасування:\n\n"
        
        keyboard = []
        
        for i, (lesson_id, date, time, duration, instructor_name, hours_until) in enumerate(cancelable_lessons, 1):
            text += f"{i}. {date} {time} ({duration})\n"
            text += f"   👨‍🏫 {instructor_name}\n"
            text += f"   ⏰ Залишилось {int(hours_until)} год\n\n"
            keyboard.append([KeyboardButton(f"{i}")])
        
        keyboard.append([KeyboardButton("🔙 Назад")])
        
        await update.message.reply_text(
            text,
            reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True),
            parse_mode="Markdown"
        )
        
    except Exception as e:
        logger.error(f"Error in show_lessons_to_cancel: {e}", exc_info=True)
        await update.message.reply_text("❌ Помилка завантаження записів.")

async def handle_cancel_lesson(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обробка вибору уроку для скасування"""
    text = update.message.text
    
    if text == "🔙 Назад":
        context.user_data.clear()
        await start(update, context)
        return
    
    try:
        lesson_index = int(text) - 1
        lessons = context.user_data.get("cancelable_lessons", [])
        
        if lesson_index < 0 or lesson_index >= len(lessons):
            await update.message.reply_text("⚠️ Невірний номер. Спробуйте ще раз:")
            return
        
        selected = lessons[lesson_index]
        lesson_id, date, time, duration, instructor_name, hours_until = selected
        
        # Зберігаємо вибраний урок
        context.user_data["cancel_lesson_id"] = lesson_id
        context.user_data["cancel_lesson_date"] = date
        context.user_data["cancel_lesson_time"] = time
        context.user_data["cancel_lesson_instructor"] = instructor_name
        context.user_data["state"] = "cancel_lesson_confirm"
        
        keyboard = [
            [KeyboardButton("✅ Так, скасувати")],
            [KeyboardButton("🔙 Ні, залишити")]
        ]
        
        await update.message.reply_text(
            f"⚠️ *Підтвердіть скасування*\n\n"
            f"📅 Дата: {date}\n"
            f"🕐 Час: {time}\n"
            f"⏱ Тривалість: {duration}\n"
            f"👨‍🏫 Інструктор: {instructor_name}\n\n"
            f"Скасувати урок?",
            reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True),
            parse_mode="Markdown"
        )
        
    except ValueError:
        await update.message.reply_text("⚠️ Введіть номер уроку:")
        return
    except Exception as e:
        logger.error(f"Error in handle_cancel_lesson: {e}", exc_info=True)
        await update.message.reply_text("❌ Помилка.")

async def handle_cancel_confirmation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обробка підтвердження скасування"""
    text = update.message.text
    
    if text == "🔙 Ні, залишити":
        context.user_data.clear()
        await update.message.reply_text("✅ Запис залишено без змін.")
        await start(update, context)
        return
    
    if text != "✅ Так, скасувати":
        await update.message.reply_text("⚠️ Оберіть дію з меню:")
        return
    
    try:
        lesson_id = context.user_data.get("cancel_lesson_id")
        date = context.user_data.get("cancel_lesson_date")
        time = context.user_data.get("cancel_lesson_time")
        instructor_name = context.user_data.get("cancel_lesson_instructor")
        
        user_id = update.message.from_user.id
        
        # Отримуємо дані учня та інструктора
        with get_db() as conn:
            cursor = conn.cursor()
            
            # Дані уроку
            cursor.execute("""
                SELECT student_name, student_phone, student_tariff, duration, instructor_id
                FROM lessons
                WHERE id = ?
            """, (lesson_id,))
            
            lesson_data = cursor.fetchone()
            
            if not lesson_data:
                await update.message.reply_text("❌ Урок не знайдено.")
                return
            
            student_name, student_phone, student_tariff, duration, instructor_id = lesson_data
            
            # Оновлюємо статус уроку
            cursor.execute("""
                UPDATE lessons
                SET status = 'cancelled',
                    cancelled_by = 'student',
                    cancelled_at = CURRENT_TIMESTAMP
                WHERE id = ?
            """, (lesson_id,))
            
            # Отримуємо telegram_id інструктора
            cursor.execute("SELECT telegram_id FROM instructors WHERE id = ?", (instructor_id,))
            instructor_telegram_id = cursor.fetchone()[0]
            
            conn.commit()
        
        # Повідомлення учню
        await update.message.reply_text(
            f"✅ *Урок скасовано!*\n\n"
            f"📅 {date} {time}\n"
            f"👨‍🏫 {instructor_name}",
            parse_mode="Markdown"
        )
        
        # Повідомлення інструктору
        if instructor_telegram_id:
            try:
                # Розрахунок вартості
                if student_tariff and "2" in duration:
                    price = student_tariff * 2
                elif student_tariff:
                    price = student_tariff
                else:
                    price = PRICES.get(duration, 400)
                
                await context.bot.send_message(
                    chat_id=instructor_telegram_id,
                    text=f"🔔 *Урок скасовано учнем*\n\n"
                         f"👤 Учень: {student_name}\n"
                         f"📱 Телефон: {student_phone}\n"
                         f"📅 Дата: {date}\n"
                         f"🕐 Час: {time}\n"
                         f"⏱ Тривалість: {duration}\n"
                         f"💰 Сума: {price:.0f} грн",
                    parse_mode="Markdown"
                )
            except Exception as e:
                logger.error(f"Failed to notify instructor: {e}")
        
        context.user_data.clear()
        await start(update, context)
        
    except Exception as e:
        logger.error(f"Error in handle_cancel_confirmation: {e}", exc_info=True)
        await update.message.reply_text("❌ Помилка скасування.")


async def save_lesson(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Зберегти заняття в БД"""
    try:
        instructor_name = context.user_data["instructor"]
        date = context.user_data["date"]
        time = context.user_data["time"]
        duration = context.user_data["duration"]
        student_name = context.user_data.get("student_name", "")
        student_phone = context.user_data.get("student_phone", "")
        student_telegram_id = update.message.from_user.id
        student_tariff = context.user_data.get("student_tariff", 0)
        
        instructor_data = get_instructor_by_name(instructor_name)
        if not instructor_data:
            await update.message.reply_text("❌ Помилка: інструктор не знайдений.")
            return
        
        instructor_id, instructor_telegram_id = instructor_data
        
        # ========== ПЕРЕВІРКИ ПЕРЕД ЗАПИСОМ ==========
        
        # Розрахунок часу закінчення уроку
        start_hour = int(time.split(':')[0])
        if "2" in duration:
            lesson_hours = 2
        elif "1.5" in duration:
            lesson_hours = 1.5
        else:
            lesson_hours = 1
        
        end_hour = start_hour + lesson_hours
        
        with get_db() as conn:
            cursor = conn.cursor()
            
            # ПЕРЕВІРКА 1: Чи учень вже має урок в цей час (у будь-якого інструктора)
            cursor.execute("""
                SELECT i.name, l.time, l.duration
                FROM lessons l
                JOIN instructors i ON l.instructor_id = i.id
                WHERE l.student_telegram_id = ? AND l.date = ? AND l.status = 'active'
            """, (student_telegram_id, date))
            
            existing_lessons = cursor.fetchall()
            
            for existing_instructor, existing_time, existing_duration in existing_lessons:
                existing_start = int(existing_time.split(':')[0])
                if "2" in existing_duration:
                    existing_hours = 2
                elif "1.5" in existing_duration:
                    existing_hours = 1.5
                else:
                    existing_hours = 1
                existing_end = existing_start + existing_hours
                
                # Перевірка перетину часу
                if not (end_hour <= existing_start or start_hour >= existing_end):
                    await update.message.reply_text(
                        f"❌ *Не можна записатись!*\n\n"
                        f"У вас вже є урок в цей час:\n"
                        f"👨‍🏫 {existing_instructor}\n"
                        f"📅 {date}\n"
                        f"🕐 {existing_time} ({existing_duration})\n\n"
                        f"Оберіть інший час.",
                        parse_mode="Markdown"
                    )
                    return
            
            # ПЕРЕВІРКА 2: Чи не перевищує ліміт 2 години в день
            cursor.execute("""
                SELECT SUM(
                    CASE 
                        WHEN duration LIKE '%2%' THEN 2
                        WHEN duration LIKE '%1.5%' THEN 1.5
                        ELSE 1
                    END
                )
                FROM lessons
                WHERE student_telegram_id = ? AND date = ? AND status = 'active'
            """, (student_telegram_id, date))
            
            total_hours_today = cursor.fetchone()[0] or 0
            
            if total_hours_today + lesson_hours > 2:
                await update.message.reply_text(
                    f"❌ *Ліміт перевищено!*\n\n"
                    f"Ви вже маєте *{total_hours_today:.1f} год* на {date}\n"
                    f"Максимум: *2 години на день*\n\n"
                    f"Залишилось: *{2 - total_hours_today:.1f} год*",
                    parse_mode="Markdown"
                )
                return
            
            # ПЕРЕВІРКА 3: Чи не перевищує ліміт 6 годин в тиждень
            # Визначаємо початок і кінець тижня
            from datetime import datetime, timedelta
            date_obj = datetime.strptime(date, "%d.%m.%Y")
            # Понеділок поточного тижня
            week_start = date_obj - timedelta(days=date_obj.weekday())
            # Неділя поточного тижня
            week_end = week_start + timedelta(days=6)
            
            week_start_str = week_start.strftime("%d.%m.%Y")
            week_end_str = week_end.strftime("%d.%m.%Y")
            
            cursor.execute("""
                SELECT SUM(
                    CASE 
                        WHEN duration LIKE '%2%' THEN 2
                        WHEN duration LIKE '%1.5%' THEN 1.5
                        ELSE 1
                    END
                )
                FROM lessons
                WHERE student_telegram_id = ? 
                AND date BETWEEN ? AND ?
                AND status = 'active'
            """, (student_telegram_id, week_start_str, week_end_str))
            
            total_hours_week = cursor.fetchone()[0] or 0
            
            if total_hours_week + lesson_hours > 6:
                await update.message.reply_text(
                    f"❌ *Ліміт перевищено!*\n\n"
                    f"Ви вже маєте *{total_hours_week:.1f} год* цього тижня\n"
                    f"Максимум: *6 годин на тиждень*\n\n"
                    f"Залишилось: *{6 - total_hours_week:.1f} год*",
                    parse_mode="Markdown"
                )
                return
            
            # ========== ВСІ ПЕРЕВІРКИ ПРОЙШЛИ - ЗБЕРІГАЄМО ==========
            
            booking_comment = context.user_data.get("booking_comment", "")
            
            cursor.execute("""
                INSERT INTO lessons 
                (instructor_id, student_name, student_telegram_id, student_phone, student_tariff, date, time, duration, status, booking_comment)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'active', ?)
            """, (instructor_id, student_name, student_telegram_id, student_phone, student_tariff, date, time, duration, booking_comment))
            conn.commit()
        
        # Повідомлення учню (БЕЗ особистих даних)
        await update.message.reply_text(
            f"✅ *Заняття заброньовано!*\n\n"
            f"👨‍🏫 Інструктор: {instructor_name}\n"
            f"📅 Дата: {date}\n"
            f"🕐 Час: {time}\n"
            f"⏱ Тривалість: {duration}",
            parse_mode="Markdown"
        )
        
        # Розрахунок вартості для інструктора
        if student_tariff > 0:
            if "2" in duration:
                price = student_tariff * 2
            else:
                price = student_tariff
        else:
            price = PRICES.get(duration, 400)
        
        # Повідомлення інструктору (З особистими даними ТА сумою)
        booking_comment = context.user_data.get("booking_comment", "")
        
        if instructor_telegram_id:
            try:
                message_text = (
                    f"🔔 *Новий запис!*\n\n"
                    f"👤 Учень: {student_name}\n"
                    f"📱 Телефон: {student_phone}\n"
                    f"📅 Дата: {date}\n"
                    f"🕐 Час: {time}\n"
                    f"⏱ Тривалість: {duration}\n"
                    f"💰 Вартість: *{price:.0f} грн*"
                )
                
                # Додаємо коментар якщо є
                if booking_comment:
                    message_text += f"\n\n💬 Коментар учня:\n\"{booking_comment}\""
                
                await context.bot.send_message(
                    chat_id=instructor_telegram_id,
                    text=message_text,
                    parse_mode="Markdown"
                )
            except Exception as e:
                logger.error(f"Не вдалося надіслати повідомлення інструктору: {e}")
        
        await start(update, context)
        
    except Exception as e:
        logger.error(f"Error in save_lesson: {e}", exc_info=True)
        await update.message.reply_text("❌ Помилка збереження запису.")

# ======================= CALLBACKS =======================
async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обробка callback кнопок"""
    query = update.callback_query
    await query.answer()
    
    try:
        if query.data.startswith("unblock_"):
            block_id = int(query.data.split("_")[1])
            await handle_unblock_callback(query, context, block_id)
            
    except Exception as e:
        logger.error(f"Error in handle_callback: {e}", exc_info=True)
        await query.edit_message_text("❌ Помилка.")

async def handle_unblock_callback(query, context, block_id):
    """Розблокування часу"""
    try:
        from database import remove_schedule_block
        
        if remove_schedule_block(block_id):
            await query.edit_message_text("✅ Час розблоковано!")
        else:
            await query.edit_message_text("❌ Помилка розблокування.")
            
    except Exception as e:
        logger.error(f"Error in handle_unblock_callback: {e}", exc_info=True)
        await query.edit_message_text("❌ Помилка.")

# ======================= REMINDERS =======================
async def send_reminders(context: ContextTypes.DEFAULT_TYPE):
    """Відправка нагадувань про заняття"""
    try:
        now = datetime.now(TZ)
        logger.info(f"🔔 send_reminders запущено! Зараз: {now.strftime('%d.%m.%Y %H:%M')}")
        
        with get_db() as conn:
            cursor = conn.cursor()
            
            # === НАГАДУВАННЯ ЗА 24 ГОДИНИ ===
            # Отримуємо всі активні уроки
            cursor.execute("""
                SELECT l.id, l.student_telegram_id, i.name, l.date, l.time
                FROM lessons l
                JOIN instructors i ON l.instructor_id = i.id
                WHERE l.status = 'active' 
                AND l.reminder_24h_sent = 0
            """)
            
            all_lessons = cursor.fetchall()
            logger.info(f"📋 Знайдено {len(all_lessons)} активних уроків (без нагадування 24h)")
            lessons_24h = []
            
            # Перевіряємо кожен урок
            for lesson_id, student_id, instructor, date_str, time_str in all_lessons:
                try:
                    # Конвертуємо дату з ДД.ММ.РРРР в datetime
                    lesson_datetime = datetime.strptime(f"{date_str} {time_str}", "%d.%m.%Y %H:%M")
                    lesson_datetime = TZ.localize(lesson_datetime)
                    
                    # Перевіряємо чи урок через 24 години (±30 хвилин)
                    time_diff = (lesson_datetime - now).total_seconds() / 3600
                    
                    logger.info(f"  📝 Урок #{lesson_id}: {date_str} {time_str}, різниця: {time_diff:.1f} год")
                    
                    if 23.5 <= time_diff <= 24.5:
                        lessons_24h.append((lesson_id, student_id, instructor, date_str, time_str))
                        logger.info(f"    ✅ Додано до нагадувань 24h!")
                except Exception as e:
                    logger.error(f"Error parsing lesson date {date_str} {time_str}: {e}")
            
            for lesson_id, student_id, instructor, date, time in lessons_24h:
                try:
                    await context.bot.send_message(
                        chat_id=student_id,
                        text=f"⏰ *Нагадування!*\n\nУ вас заняття завтра:\n"
                             f"👨‍🏫 {instructor}\n📅 {date}\n🕐 {time}",
                        parse_mode="Markdown"
                    )
                    
                    cursor.execute("UPDATE lessons SET reminder_24h_sent = 1 WHERE id = ?", (lesson_id,))
                    conn.commit()
                except Exception as e:
                    logger.error(f"Failed to send 24h reminder: {e}")
            
            # === НАГАДУВАННЯ ЗА 2 ГОДИНИ ===
            # Отримуємо всі активні уроки
            cursor.execute("""
                SELECT l.id, l.student_telegram_id, i.name, l.date, l.time
                FROM lessons l
                JOIN instructors i ON l.instructor_id = i.id
                WHERE l.status = 'active' 
                AND l.reminder_2h_sent = 0
            """)
            
            all_lessons_2h = cursor.fetchall()
            logger.info(f"📋 Знайдено {len(all_lessons_2h)} активних уроків (без нагадування 2h)")
            lessons_2h = []
            
            # Перевіряємо кожен урок
            for lesson_id, student_id, instructor, date_str, time_str in all_lessons_2h:
                try:
                    # Конвертуємо дату з ДД.ММ.РРРР в datetime
                    lesson_datetime = datetime.strptime(f"{date_str} {time_str}", "%d.%m.%Y %H:%M")
                    lesson_datetime = TZ.localize(lesson_datetime)
                    
                    # Перевіряємо чи урок через 2 години (±30 хвилин)
                    time_diff = (lesson_datetime - now).total_seconds() / 3600
                    
                    logger.info(f"  📝 Урок #{lesson_id}: {date_str} {time_str}, різниця: {time_diff:.1f} год")
                    
                    if 1.5 <= time_diff <= 2.5:
                        lessons_2h.append((lesson_id, student_id, instructor, date_str, time_str))
                        logger.info(f"    ✅ Додано до нагадувань 2h!")
                except Exception as e:
                    logger.error(f"Error parsing lesson date {date_str} {time_str}: {e}")
            
            for lesson_id, student_id, instructor, date, time in lessons_2h:
                try:
                    logger.info(f"📤 Відправляю нагадування 2h учню {student_id}: {date} {time}")
                    await context.bot.send_message(
                        chat_id=student_id,
                        text=f"🔔 *Нагадування!*\n\nУ вас заняття через 2 години:\n"
                             f"👨‍🏫 {instructor}\n📅 {date}\n🕐 {time}\n\n"
                             f"⏰ Не забудьте підготуватися!",
                        parse_mode="Markdown"
                    )
                    
                    cursor.execute("UPDATE lessons SET reminder_2h_sent = 1 WHERE id = ?", (lesson_id,))
                    conn.commit()
                    logger.info(f"✅ Нагадування 2h відправлено успішно!")
                except Exception as e:
                    logger.error(f"Failed to send 2h reminder: {e}")
        
        logger.info("✅ Reminders sent successfully")
        
    except Exception as e:
        logger.error(f"Error in send_reminders: {e}", exc_info=True)

async def send_rating_request_to_student(context, student_tg_id, lesson_id, date, time, instructor_name):
    """Відправити запит на оцінювання інструктора учню"""
    try:
        keyboard = [
            [KeyboardButton("⭐"), KeyboardButton("⭐⭐"), KeyboardButton("⭐⭐⭐")],
            [KeyboardButton("⭐⭐⭐⭐"), KeyboardButton("⭐⭐⭐⭐⭐")],
            [KeyboardButton("⏭️ Пропустити")]
        ]
        
        await context.bot.send_message(
            chat_id=student_tg_id,
            text=f"✅ *Урок завершено!*\n\n"
                 f"📅 {date} {time}\n"
                 f"👨‍🏫 {instructor_name}\n\n"
                 f"⭐ Оцініть інструктора:",
            reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True),
            parse_mode="Markdown"
        )
        
        # Зберігаємо дані уроку для подальшого збереження оцінки
        context.bot_data[f"rating_lesson_{student_tg_id}"] = {
            'lesson_id': lesson_id,
            'instructor_name': instructor_name,
            'date': date,
            'time': time
        }
        
        logger.info(f"📤 Відправлено запит на оцінювання учню {student_tg_id} за урок {lesson_id}")
        
    except Exception as e:
        logger.error(f"Error sending rating request: {e}", exc_info=True)

async def check_completed_lessons(context: ContextTypes.DEFAULT_TYPE):
    """Перевірка завершених занять"""
    try:
        now = datetime.now(TZ)
        
        with get_db() as conn:
            cursor = conn.cursor()
            
            # Отримуємо всі активні уроки з даними про учня та інструктора
            cursor.execute("""
                SELECT l.id, l.date, l.time, l.student_telegram_id, 
                       l.instructor_id, i.name as instructor_name
                FROM lessons l
                JOIN instructors i ON l.instructor_id = i.id
                WHERE l.status = 'active'
            """)
            
            lessons_to_complete = []
            
            for lesson_id, date_str, time_str, student_tg_id, instructor_id, instructor_name in cursor.fetchall():
                try:
                    # Конвертуємо дату з ДД.ММ.РРРР в datetime
                    lesson_datetime = datetime.strptime(f"{date_str} {time_str}", "%d.%m.%Y %H:%M")
                    lesson_datetime = TZ.localize(lesson_datetime)
                    
                    # Якщо урок вже минув
                    if lesson_datetime < now:
                        lessons_to_complete.append({
                            'id': lesson_id,
                            'date': date_str,
                            'time': time_str,
                            'student_tg_id': student_tg_id,
                            'instructor_id': instructor_id,
                            'instructor_name': instructor_name
                        })
                except Exception as e:
                    logger.error(f"Error parsing lesson date {date_str} {time_str}: {e}")
            
            # Оновлюємо статус
            for lesson in lessons_to_complete:
                cursor.execute("""
                    UPDATE lessons
                    SET status = 'completed', completed_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                """, (lesson['id'],))
            
            conn.commit()
            
            if lessons_to_complete:
                logger.info(f"Completed {len(lessons_to_complete)} lessons")
                
                # Відправляємо запит на оцінювання учням
                for lesson in lessons_to_complete:
                    try:
                        await send_rating_request_to_student(
                            context, 
                            lesson['student_tg_id'],
                            lesson['id'],
                            lesson['date'],
                            lesson['time'],
                            lesson['instructor_name']
                        )
                    except Exception as e:
                        logger.error(f"Failed to send rating request for lesson {lesson['id']}: {e}")
        
    except Exception as e:
        logger.error(f"Error in check_completed_lessons: {e}", exc_info=True)

# ======================= EXPORT WITH PERIOD SELECTION =======================
async def show_export_period_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Меню вибору періоду для експорту"""
    keyboard = [
        [KeyboardButton("📊 За тиждень")],
        [KeyboardButton("📊 За місяць")],
        [KeyboardButton("📊 За весь час")],
        [KeyboardButton("📊 Свій період")],
        [KeyboardButton("🔙 Назад")]
    ]
    
    context.user_data["state"] = "export_period"
    
    await update.message.reply_text(
        "📥 *Експорт в Excel*\n\n"
        "Оберіть період для експорту:",
        reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True),
        parse_mode="Markdown"
    )

async def handle_export_period_choice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обробка вибору періоду"""
    text = update.message.text
    
    if text == "🔙 Назад":
        await show_admin_panel(update, context)
        return
    
    from datetime import datetime, timedelta
    today = datetime.now().date()
    
    if text == "📊 За тиждень":
        date_from = (today - timedelta(days=7)).strftime("%d.%m.%Y")
        date_to = (today + timedelta(days=7)).strftime("%d.%m.%Y")  # +7 днів вперед
        period_name = "тиждень"
        
    elif text == "📊 За місяць":
        date_from = (today - timedelta(days=30)).strftime("%d.%m.%Y")
        date_to = (today + timedelta(days=30)).strftime("%d.%m.%Y")  # +30 днів вперед
        period_name = "місяць"
        
    elif text == "📊 За весь час":
        date_from = "01.01.2020"
        # Додаємо +1 рік щоб захопити майбутні уроки
        date_to = (today + timedelta(days=365)).strftime("%d.%m.%Y")
        period_name = "весь час"
        
    elif text == "📊 Свій період":
        context.user_data["state"] = "export_custom_period"
        
        keyboard = [[KeyboardButton("🔙 Назад")]]
        
        await update.message.reply_text(
            "📅 *Введіть період у форматі:*\n"
            "`ДД.ММ.РРРР - ДД.ММ.РРРР`\n\n"
            "Наприклад: `01.12.2025 - 31.12.2025`",
            reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True),
            parse_mode="Markdown"
        )
        return
    else:
        await update.message.reply_text("⚠️ Оберіть період з меню.")
        return
    
    # Генеруємо Excel
    await export_to_excel_with_period(update, context, date_from, date_to, period_name)

async def handle_export_custom_period(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обробка введеного періоду"""
    text = update.message.text
    
    if text == "🔙 Назад":
        await show_export_period_menu(update, context)
        return
    
    try:
        import re
        match = re.match(r'(\d{2}\.\d{2}\.\d{4})\s*-\s*(\d{2}\.\d{2}\.\d{4})', text)
        
        if not match:
            await update.message.reply_text(
                "⚠️ Невірний формат!\n\n"
                "Використовуйте: `ДД.ММ.РРРР - ДД.ММ.РРРР`\n"
                "Наприклад: `01.12.2025 - 31.12.2025`",
                parse_mode="Markdown"
            )
            return
        
        date_from = match.group(1)
        date_to = match.group(2)
        
        # Перевіряємо валідність дат
        from datetime import datetime
        try:
            datetime.strptime(date_from, "%d.%m.%Y")
            datetime.strptime(date_to, "%d.%m.%Y")
        except ValueError:
            await update.message.reply_text("⚠️ Невірна дата! Перевірте формат.")
            return
        
        period_name = f"{date_from} - {date_to}"
        
        # Генеруємо Excel
        await export_to_excel_with_period(update, context, date_from, date_to, period_name)
        
    except Exception as e:
        logger.error(f"Error in handle_export_custom_period: {e}", exc_info=True)
        await update.message.reply_text("❌ Помилка обробки періоду.")

async def export_to_excel_with_period(update: Update, context: ContextTypes.DEFAULT_TYPE, date_from: str, date_to: str, period_name: str):
    """Експорт даних в Excel за період"""
    user_id = update.message.from_user.id
    
    try:
        await update.message.reply_text("⏳ Генерую Excel файл... Зачекайте...")
        
        from openpyxl import Workbook
        from openpyxl.styles import Font, PatternFill, Alignment
        from io import BytesIO
        
        # Створюємо Excel workbook
        wb = Workbook()
        
        # ============ ЛИСТ 1: УРОКИ ============
        ws1 = wb.active
        ws1.title = "Уроки"
        
        headers1 = ["ID", "Дата", "Час", "Інструктор", "Учень", "Телефон", "Тариф", "Тривалість", "Вартість", "Статус", "⭐ Оцінка учня", "💬 Коментар учня", "⭐ Оцінка інструктора", "💬 Коментар інструктора"]
        ws1.append(headers1)
        
        # Стилізація
        header_fill = PatternFill(start_color="366092", end_color="366092", fill_type="solid")
        header_font = Font(bold=True, color="FFFFFF")
        
        for cell in ws1[1]:
            cell.fill = header_fill
            cell.font = header_font
            cell.alignment = Alignment(horizontal="center", vertical="center")
        
        # Дані уроків - ОТРИМУЄМО ВСІ, ФІЛЬТРУЄМО В PYTHON
        with get_db() as conn:
            cursor = conn.cursor()
            
            # Перевіряємо чи є нові колонки в БД
            cursor.execute("PRAGMA table_info(lessons)")
            columns = {row[1] for row in cursor.fetchall()}
            has_instructor_rating = 'instructor_rating' in columns
            has_instructor_feedback = 'instructor_feedback' in columns
            
            logger.info(f"🔍 Колонки БД: instructor_rating={has_instructor_rating}, instructor_feedback={has_instructor_feedback}")
            
            # SQL запит залежно від наявності колонок
            if has_instructor_rating and has_instructor_feedback:
                # Нова БД з колонками
                cursor.execute("""
                    SELECT 
                        l.id,
                        l.date,
                        l.time,
                        i.name as instructor_name,
                        s.name as student_name,
                        s.phone as student_phone,
                        s.tariff,
                        l.duration,
                        CASE 
                            WHEN l.duration LIKE '%2%' THEN s.tariff * 2
                            WHEN l.duration LIKE '%1.5%' THEN s.tariff * 1.5
                            ELSE s.tariff * 1
                        END as earnings,
                        l.status,
                        l.rating,
                        l.feedback,
                        l.instructor_rating,
                        l.instructor_feedback
                    FROM lessons l
                    LEFT JOIN instructors i ON l.instructor_id = i.id
                    LEFT JOIN students s ON l.student_telegram_id = s.telegram_id
                    ORDER BY l.date DESC, l.time DESC
                """)
            else:
                # Стара БД без нових колонок - додаємо NULL
                cursor.execute("""
                    SELECT 
                        l.id,
                        l.date,
                        l.time,
                        i.name as instructor_name,
                        s.name as student_name,
                        s.phone as student_phone,
                        s.tariff,
                        l.duration,
                        CASE 
                            WHEN l.duration LIKE '%2%' THEN s.tariff * 2
                            WHEN l.duration LIKE '%1.5%' THEN s.tariff * 1.5
                            ELSE s.tariff * 1
                        END as earnings,
                        l.status,
                        l.rating,
                        l.feedback,
                        NULL as instructor_rating,
                        NULL as instructor_feedback
                    FROM lessons l
                    LEFT JOIN instructors i ON l.instructor_id = i.id
                    LEFT JOIN students s ON l.student_telegram_id = s.telegram_id
                    ORDER BY l.date DESC, l.time DESC
                """)
            
            all_lessons = cursor.fetchall()
        
        # === ДІАГНОСТИКА ===
        logger.info(f"📊 ДІАГНОСТИКА ЕКСПОРТУ")
        logger.info(f"📅 Період: {date_from} - {date_to}")
        logger.info(f"🔍 Всього уроків в БД: {len(all_lessons)}")
        
        if len(all_lessons) > 0:
            # Показуємо перші 5 дат
            sample_dates = [lesson[1] for lesson in all_lessons[:5]]
            logger.info(f"📝 Перші 5 дат в БД: {sample_dates}")
        else:
            logger.warning(f"⚠️ БД порожня! Уроків: 0")
        # === КІНЕЦЬ ДІАГНОСТИКИ ===
        
        # Фільтруємо уроки в Python по датах
        from datetime import datetime as dt
        date_from_obj = dt.strptime(date_from, "%d.%m.%Y")
        date_to_obj = dt.strptime(date_to, "%d.%m.%Y")
        
        logger.info(f"🔄 Початок фільтрації...")
        logger.info(f"📆 date_from_obj: {date_from_obj}")
        logger.info(f"📆 date_to_obj: {date_to_obj}")
        
        lessons = []
        filtered_out = 0
        parse_errors = 0
        
        for lesson in all_lessons:
            try:
                lesson_date = dt.strptime(lesson[1], "%d.%m.%Y")
                if date_from_obj <= lesson_date <= date_to_obj:
                    lessons.append(lesson)
                else:
                    filtered_out += 1
            except (ValueError, TypeError) as e:
                # Пропускаємо уроки з невірним форматом дати
                parse_errors += 1
                logger.warning(f"❌ Помилка парсингу дати: {lesson[1]} - {e}")
                continue
        
        # === ПІДСУМОК ДІАГНОСТИКИ ===
        logger.info(f"✅ Відфільтровано (в період): {len(lessons)}")
        logger.info(f"🚫 Відфільтровано (поза періодом): {filtered_out}")
        logger.info(f"❌ Помилки парсингу дат: {parse_errors}")
        # === КІНЕЦЬ ПІДСУМКУ ===
        
        # Статистика для повідомлення
        total_lessons = len(lessons)
        total_earnings = 0
        unique_students = set()
        
        for lesson in lessons:
                ws1.append(lesson)
                if lesson[8]:  # earnings
                    total_earnings += lesson[8]
                if lesson[4]:  # student_name
                    unique_students.add(lesson[4])
        
        # Автоширина
        for column in ws1.columns:
            max_length = 0
            column = [cell for cell in column]
            for cell in column:
                try:
                    if len(str(cell.value)) > max_length:
                        max_length = len(str(cell.value))
                except:
                    pass
            adjusted_width = min(max_length + 2, 50)
            ws1.column_dimensions[column[0].column_letter].width = adjusted_width
        
        # ============ ЛИСТ 2: УЧНІ ============
        ws2 = wb.create_sheet(title="Учні")
        
        headers2 = ["Учень", "Телефон", "Тариф", "Уроків", "Годин", "Витрачено", "⭐ Середній рейтинг"]
        ws2.append(headers2)
        
        for cell in ws2[1]:
            cell.fill = header_fill
            cell.font = header_font
            cell.alignment = Alignment(horizontal="center", vertical="center")
        
        # Дані учнів - використовуємо відфільтровані уроки
        # Групуємо по учням вручну в Python
        students_stats = {}
        
        for lesson in lessons:
            student_name = lesson[4]  # s.name
            student_phone = lesson[5]  # s.phone
            student_tariff = lesson[6]  # s.tariff
            duration = lesson[7]  # l.duration
            earnings = lesson[8]  # earnings
            instructor_rating = lesson[12]  # l.instructor_rating (НОВЕ!)
            
            if not student_name or not student_tariff:
                continue
            
            if student_name not in students_stats:
                students_stats[student_name] = {
                    'phone': student_phone,
                    'tariff': student_tariff,
                    'lessons': 0,
                    'hours': 0,
                    'spent': 0,
                    'ratings': []  # НОВЕ: список оцінок
                }
            
            students_stats[student_name]['lessons'] += 1
            
            # Рахуємо години
            if "1.5" in duration:
                students_stats[student_name]['hours'] += 1.5
            elif "2" in duration:
                students_stats[student_name]['hours'] += 2
            else:
                students_stats[student_name]['hours'] += 1
            
            # Рахуємо витрати
            if earnings:
                students_stats[student_name]['spent'] += earnings
            
            # Збираємо оцінки (НОВЕ!)
            if instructor_rating and instructor_rating > 0:
                students_stats[student_name]['ratings'].append(instructor_rating)
        
        # Конвертуємо в список для Excel
        students = []
        for name, stats in sorted(students_stats.items(), key=lambda x: x[1]['lessons'], reverse=True):
            # Рахуємо середній рейтинг
            avg_rating = sum(stats['ratings']) / len(stats['ratings']) if stats['ratings'] else None
            if avg_rating:
                avg_rating = round(avg_rating, 1)
            
            students.append((
                name,
                stats['phone'],
                stats['tariff'],
                stats['lessons'],
                stats['hours'],
                stats['spent'],
                avg_rating if avg_rating else '-'  # НОВЕ: рейтинг
            ))
        
        for student in students:
                ws2.append(student)
        
        # Автоширина
        for column in ws2.columns:
            max_length = 0
            column = [cell for cell in column]
            for cell in column:
                try:
                    if len(str(cell.value)) > max_length:
                        max_length = len(str(cell.value))
                except:
                    pass
            adjusted_width = min(max_length + 2, 50)
            ws2.column_dimensions[column[0].column_letter].width = adjusted_width
        
        # ============ ЛИСТ 3: ІНСТРУКТОРИ ============
        ws3 = wb.create_sheet(title="Інструктори")
        
        headers3 = ["Інструктор", "Тариф", "Уроків", "Годин", "Заробіток", "Рейтинг"]
        ws3.append(headers3)
        
        for cell in ws3[1]:
            cell.fill = header_fill
            cell.font = header_font
            cell.alignment = Alignment(horizontal="center", vertical="center")
        
        # Дані інструкторів - використовуємо відфільтровані уроки
        # Спочатку отримуємо дані інструкторів
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id, name, price_per_hour
                FROM instructors
            """)
            all_instructors = {row[0]: (row[1], row[2]) for row in cursor.fetchall()}
        
        # Групуємо статистику по інструкторах
        instructors_stats = {}
        
        for lesson in lessons:
            # Знаходимо instructor_id через instructor_name
            instructor_name = lesson[3]  # i.name
            if not instructor_name:
                continue
            
            # Шукаємо instructor_id
            instructor_id = None
            instructor_price = 0
            for iid, (iname, iprice) in all_instructors.items():
                if iname == instructor_name:
                    instructor_id = iid
                    instructor_price = iprice
                    break
            
            if not instructor_id:
                continue
            
            if instructor_id not in instructors_stats:
                instructors_stats[instructor_id] = {
                    'name': instructor_name,
                    'price': instructor_price,
                    'lessons': 0,
                    'hours': 0,
                    'earnings': 0,
                    'ratings': []
                }
            
            instructors_stats[instructor_id]['lessons'] += 1
            
            # Рахуємо години
            duration = lesson[7]
            if "1.5" in duration:
                instructors_stats[instructor_id]['hours'] += 1.5
            elif "2" in duration:
                instructors_stats[instructor_id]['hours'] += 2
            else:
                instructors_stats[instructor_id]['hours'] += 1
            
            # Рахуємо заробіток
            earnings = lesson[8]
            if earnings:
                instructors_stats[instructor_id]['earnings'] += earnings
            
            # Збираємо рейтинги
            rating = lesson[10]
            if rating and rating > 0:
                instructors_stats[instructor_id]['ratings'].append(rating)
        
        # Конвертуємо в список для Excel
        instructors = []
        for iid, stats in sorted(instructors_stats.items(), key=lambda x: x[1]['lessons'], reverse=True):
            avg_rating = sum(stats['ratings']) / len(stats['ratings']) if stats['ratings'] else 0
            instructors.append((
                stats['name'],
                stats['price'],
                stats['lessons'],
                stats['hours'],
                stats['earnings'],
                avg_rating
            ))
        
        for instructor in instructors:
                row = list(instructor)
                # Форматуємо рейтинг
                if len(row) > 5 and row[5]:
                    row[5] = round(row[5], 1)
                ws3.append(row)
        
        # Автоширина
        for column in ws3.columns:
            max_length = 0
            column = [cell for cell in column]
            for cell in column:
                try:
                    if len(str(cell.value)) > max_length:
                        max_length = len(str(cell.value))
                except:
                    pass
            adjusted_width = min(max_length + 2, 50)
            ws3.column_dimensions[column[0].column_letter].width = adjusted_width
        

        # ============ ЛИСТ 4: ЗАБЛОКОВАНІ ЧАСИ ============
        ws4 = wb.create_sheet(title="Заблоковані часи")
        
        headers4 = ["Інструктор", "Дата", "Час початку", "Час кінця", "Причина", "Створено"]
        ws4.append(headers4)
        
        for cell in ws4[1]:
            cell.fill = header_fill
            cell.font = header_font
            cell.alignment = Alignment(horizontal="center", vertical="center")
        
        # Отримуємо заблоковані часи
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    i.name AS instructor_name,
                    sb.date,
                    sb.time_start,
                    sb.time_end,
                    sb.reason,
                    sb.created_at
                FROM schedule_blocks sb
                JOIN instructors i ON sb.instructor_id = i.id
                ORDER BY sb.date DESC, sb.time_start
            """)
            blocked_times = cursor.fetchall()
        
        if blocked_times:
            for block in blocked_times:
                instructor_name = block[0]
                date = block[1]  # YYYY-MM-DD
                time_start = block[2]
                time_end = block[3]
                reason = block[4] or "Не вказано"
                created_at = block[5]
                
                # Форматуємо дату в DD.MM.YYYY
                try:
                    date_obj = datetime.strptime(date, "%Y-%m-%d")
                    date_formatted = date_obj.strftime("%d.%m.%Y")
                    weekday = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Нд"][date_obj.weekday()]
                    date_display = f"{weekday} {date_formatted}"
                except:
                    date_display = date
                
                # Форматуємо час
                time_display = f"{time_start} - {time_end}"
                
                # Форматуємо created_at
                try:
                    created_obj = datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S")
                    created_display = created_obj.strftime("%d.%m.%Y %H:%M")
                except:
                    created_display = created_at
                
                ws4.append([
                    instructor_name,
                    date_display,
                    time_start,
                    time_end,
                    reason,
                    created_display
                ])
        else:
            # Якщо немає заблокованих часів
            ws4.append(["Немає заблокованих часів", "", "", "", "", ""])
        
        # Автоширина
        for column in ws4.columns:
            max_length = 0
            column = [cell for cell in column]
            for cell in column:
                try:
                    if len(str(cell.value)) > max_length:
                        max_length = len(str(cell.value))
                except:
                    pass
            adjusted_width = min(max_length + 2, 50)
            ws4.column_dimensions[column[0].column_letter].width = adjusted_width
        
        # ============ ЗБЕРІГАЄМО ФАЙЛ ============
        excel_file = BytesIO()
        wb.save(excel_file)
        excel_file.seek(0)
        
        # Назва файлу
        filename = f"export_{period_name.replace(' ', '_').replace(':', '-')}.xlsx"
        
        # Відправляємо файл
        await context.bot.send_document(
            chat_id=update.message.chat_id,
            document=excel_file,
            filename=filename,
            caption=f"📊 *Експорт завершено!*\n\n"
                   f"📅 Період: {period_name}\n"
                   f"📝 Уроків: {total_lessons}\n"
                   f"👥 Учнів: {len(unique_students)}\n"
                   f"💰 Загальний заробіток: {total_earnings:.0f} грн",
            parse_mode="Markdown"
        )
        
        logger.info(f"✅ Excel exported for period: {period_name}")
        
        # Повертаємось в меню адміна
        await show_admin_panel(update, context)
        
    except Exception as e:
        logger.error(f"Error in export_to_excel_with_period: {e}", exc_info=True)
        await update.message.reply_text(
            "❌ Помилка експорту.\n\n"
            f"Деталі: {str(e)}"
        )
        await show_admin_panel(update, context)

# ======================= EXPORT TO EXCEL (OLD - DEPRECATED) =======================
async def export_to_excel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Експорт всіх даних в Excel"""
    user_id = update.message.from_user.id
    
    # Перевірка прав доступу (тільки адміни)
    if not is_admin(user_id):
        await update.message.reply_text("❌ У вас немає доступу до цієї функції.")
        return
    
    try:
        await update.message.reply_text("⏳ Генерую Excel файл... Зачекайте...")
        
        # Створюємо Excel workbook
        wb = Workbook()
        
        # ============ ЛИСТ 1: УРОКИ ============
        ws1 = wb.active
        ws1.title = "Уроки"
        
        # Заголовки
        headers1 = ["ID", "Дата", "Час", "Інструктор", "Учень", "Телефон", "Тариф", "Тривалість", "Вартість", "Статус", "Оцінка учня", "Коментар"]
        ws1.append(headers1)
        
        # Стилізація заголовків
        header_fill = PatternFill(start_color="366092", end_color="366092", fill_type="solid")
        header_font = Font(bold=True, color="FFFFFF")
        
        for cell in ws1[1]:
            cell.fill = header_fill
            cell.font = header_font
            cell.alignment = Alignment(horizontal="center", vertical="center")
        
        # Дані уроків
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    l.id,
                    l.date,
                    l.time,
                    i.name as instructor_name,
                    s.name as student_name,
                    s.phone as student_phone,
                    s.tariff,
                    l.duration,
                    CASE 
                        WHEN l.duration LIKE '%2%' THEN s.tariff * 2
                        WHEN l.duration LIKE '%1.5%' THEN s.tariff * 1.5
                        ELSE s.tariff * 1
                    END as earnings,
                    l.status,
                    l.rating,
                    l.feedback
                FROM lessons l
                LEFT JOIN instructors i ON l.instructor_id = i.id
                LEFT JOIN students s ON l.student_telegram_id = s.telegram_id
                ORDER BY l.date DESC, l.time DESC
            """)
            lessons = cursor.fetchall()
            
            for lesson in lessons:
                ws1.append(lesson)
        
        # Автоширина колонок
        for column in ws1.columns:
            max_length = 0
            column = [cell for cell in column]
            for cell in column:
                try:
                    if len(str(cell.value)) > max_length:
                        max_length = len(str(cell.value))
                except:
                    pass
            adjusted_width = min(max_length + 2, 50)
            ws1.column_dimensions[column[0].column_letter].width = adjusted_width
        
        # ============ ЛИСТ 2: УЧНІ ============
        ws2 = wb.create_sheet(title="Учні")
        
        headers2 = ["ID", "Ім'я", "Телефон", "Тариф", "Telegram ID", "Дата реєстрації"]
        ws2.append(headers2)
        
        # Стилізація
        for cell in ws2[1]:
            cell.fill = header_fill
            cell.font = header_font
            cell.alignment = Alignment(horizontal="center", vertical="center")
        
        # Дані учнів
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id, name, phone, tariff, telegram_id, created_at
                FROM students
                ORDER BY created_at DESC
            """)
            students = cursor.fetchall()
            
            for student in students:
                ws2.append(student)
        
        # Автоширина
        for column in ws2.columns:
            max_length = 0
            column = [cell for cell in column]
            for cell in column:
                try:
                    if len(str(cell.value)) > max_length:
                        max_length = len(str(cell.value))
                except:
                    pass
            adjusted_width = min(max_length + 2, 50)
            ws2.column_dimensions[column[0].column_letter].width = adjusted_width
        
        # ============ ЛИСТ 3: СТАТИСТИКА ІНСТРУКТОРІВ ============
        ws3 = wb.create_sheet(title="Статистика інструкторів")
        
        headers3 = ["Інструктор", "Тариф", "Уроків (всього)", "Годин", "Заробіток", "Рейтинг"]
        ws3.append(headers3)
        
        # Стилізація
        for cell in ws3[1]:
            cell.fill = header_fill
            cell.font = header_font
            cell.alignment = Alignment(horizontal="center", vertical="center")
        
        # Дані інструкторів
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    i.name,
                    i.price_per_hour,
                    COUNT(l.id) as total_lessons,
                    SUM(
                        CASE 
                            WHEN l.duration LIKE '%2%' THEN 2
                            WHEN l.duration LIKE '%1.5%' THEN 1.5
                            ELSE 1
                        END
                    ) as total_hours,
                    SUM(
                        CASE 
                            WHEN l.duration LIKE '%2%' THEN s.tariff * 2
                            WHEN l.duration LIKE '%1.5%' THEN s.tariff * 1.5
                            ELSE s.tariff * 1
                        END
                    ) as total_earnings,
                    COALESCE(AVG(CASE WHEN l.rating > 0 THEN l.rating END), 0) as avg_rating
                FROM instructors i
                LEFT JOIN lessons l ON i.id = l.instructor_id AND l.status = 'completed'
                LEFT JOIN students s ON l.student_telegram_id = s.telegram_id
                GROUP BY i.id
                ORDER BY total_lessons DESC
            """)
            instructors = cursor.fetchall()
            
            for instructor in instructors:
                row = list(instructor)
                # Форматуємо рейтинг (тепер це індекс 5, не 6!)
                if len(row) > 5 and row[5]:
                    row[5] = round(row[5], 1)
                ws3.append(row)
        
        # Автоширина
        for column in ws3.columns:
            max_length = 0
            column = [cell for cell in column]
            for cell in column:
                try:
                    if len(str(cell.value)) > max_length:
                        max_length = len(str(cell.value))
                except:
                    pass
            adjusted_width = min(max_length + 2, 50)
            ws3.column_dimensions[column[0].column_letter].width = adjusted_width
        
        # Зберігаємо в BytesIO
        excel_file = BytesIO()
        wb.save(excel_file)
        excel_file.seek(0)
        
        # Відправляємо файл
        now = datetime.now(TZ)
        filename = f"driving_school_export_{now.strftime('%Y%m%d_%H%M%S')}.xlsx"
        
        await update.message.reply_document(
            document=excel_file,
            filename=filename,
            caption=f"📊 **Експорт даних автошколи**\n\n"
                   f"📅 Дата: {now.strftime('%d.%m.%Y %H:%M')}\n"
                   f"📋 Листів: 3 (Уроки, Учні, Статистика)\n"
                   f"✅ Готово!",
            parse_mode="Markdown"
        )
        
        logger.info(f"✅ Excel експорт створено користувачем {user_id}")
        
    except Exception as e:
        logger.error(f"Error in export_to_excel: {e}", exc_info=True)
        await update.message.reply_text(
            f"❌ Помилка при створенні Excel файлу:\n{str(e)}"
        )

# ======================= ОБРОБКА ДОКУМЕНТІВ =======================
# ======================= MAIN =======================
def main():
    try:
        # Встановлюємо DB_NAME в environment для database.py
        os.environ["DB_NAME"] = DB_NAME
        
        logger.info("🚀 PRODUCTION ВЕРСІЯ БОТА")
        logger.info(f"🔑 TOKEN: {TOKEN[:20]}...")
        logger.info(f"💾 БД: {DB_NAME}")
        
        init_db()
        init_lessons_table()
        init_students_table()
        migrate_database()
        init_schedule_blocks_table()
        
        # Автоматично додаємо інструкторів якщо їх немає
        ensure_instructors_exist()

        # Створюємо application з job_queue
        from telegram.ext import JobQueue
        app = (
            ApplicationBuilder()
            .token(TOKEN)
            .build()
        )

        # Команди
        app.add_handler(CommandHandler("start", start))
        app.add_handler(CommandHandler("register450", register_450))
        app.add_handler(CommandHandler("register550", register_550))
        
        # Обробники
        app.add_handler(CallbackQueryHandler(handle_callback))
        app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), handle_message))
        app.add_handler(MessageHandler(filters.CONTACT, handle_message))

        # Нагадування кожні 30 хв (тільки якщо job_queue існує)
        if app.job_queue:
            app.job_queue.run_repeating(send_reminders, interval=1800, first=10)
            app.job_queue.run_repeating(check_completed_lessons, interval=900, first=60)
            logger.info("✅ Job queue налаштовано")
        else:
            logger.warning("⚠️ Job queue недоступна - нагадування вимкнено")

        logger.info("🚀 Бот запущено!")
        print("🚀 Бот запущено і слухає...")
        print("\n📝 Посилання для реєстрації учнів:")
        print(f"   450 грн: https://t.me/InstructorIFBot?start=register450")
        print(f"   550 грн: https://t.me/InstructorIFBot?start=register550")
        
        # Запускаємо polling в окремому потоці
        import threading
        from http.server import HTTPServer, BaseHTTPRequestHandler
        
        # Простий HTTP сервер для Render
        class HealthCheckHandler(BaseHTTPRequestHandler):
            def do_GET(self):
                self.send_response(200)
                self.send_header('Content-type', 'text/plain')
                self.end_headers()
                self.wfile.write(b'Bot is running!')
            
            def log_message(self, format, *args):
                pass  # Вимикаємо логи HTTP сервера
        
        # Запускаємо бота в окремому потоці з новим event loop
        def run_bot():
            import asyncio
            # Створюємо новий event loop для цього потоку
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                # Запускаємо без signal handlers (вони не працюють в потоках)
                app.run_polling(drop_pending_updates=True, stop_signals=None)
            finally:
                loop.close()
        
        bot_thread = threading.Thread(target=run_bot, daemon=True)
        bot_thread.start()
        
        # Запускаємо HTTP сервер на порту 8080 (для Render)
        port = int(os.environ.get('PORT', 8080))
        server = HTTPServer(('0.0.0.0', port), HealthCheckHandler)
        logger.info(f"🌐 HTTP сервер запущено на порту {port}")
        print(f"🌐 HTTP сервер запущено на порту {port}")
        
        # Блокуємо головний потік HTTP сервером
        server.serve_forever()
    
    except Exception as e:
        logger.error(f"Critical error: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    main()

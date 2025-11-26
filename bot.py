# bot.py - ВИПРАВЛЕНА ВЕРСІЯ (ЕТАП 1)
# Виправлення:
# 1. ✅ Валідація часу: мінімум +1 година від поточного
# 2. ✅ Заборона блокування зайнятого часу
# 3. ✅ Заборона запису на той самий час у різних інструкторів
# 4. ✅ Обмеження: максимум 2 години на день

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

# Читаємо TOKEN з environment або з config.py
try:
    TOKEN = os.environ.get('BOT_TOKEN')
    if not TOKEN:
        from config import TOKEN
    ADMIN_ID = int(os.environ.get('ADMIN_ID', '0'))
    if ADMIN_ID == 0:
        from config import ADMIN_ID
    TIMEZONE = os.environ.get('TIMEZONE', 'Europe/Kyiv')
except ImportError:
    TOKEN = os.environ['BOT_TOKEN']
    ADMIN_ID = int(os.environ['ADMIN_ID'])
    TIMEZONE = os.environ.get('TIMEZONE', 'Europe/Kyiv')

# Робочі години
WORK_HOURS_START = 8
WORK_HOURS_END = 18

# Ціни за годину
PRICES = {
    "1 година": 400,
    "2 години": 800
}

# Обмеження
MAX_LESSONS_PER_DAY = 2  # НОВЕ: максимум 2 години на день

from database import (
    init_db, 
    init_lessons_table, 
    init_students_table,
    migrate_database,
    get_instructors_by_transmission,
    get_instructor_by_name,
    get_instructor_by_telegram_id,
    get_instructor_rating,
    get_db,
    init_schedule_blocks_table,
    get_instructor_stats_period,
    get_admin_report_by_instructors,
    get_all_instructors,
    register_student,
    get_student_by_telegram_id,
    update_lesson,
    add_lesson_rating,
    check_student_has_booking_at_time,  # НОВЕ
    count_student_bookings_on_date,     # НОВЕ
    can_block_time_slot                 # НОВЕ
)

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
        (662748304, 'Гошовська Інна', '+380000000000', 'Автомат', 490),
        (666619757, 'Фірсов Артур', '+380000000000', 'Механіка', 550),
        (982534001, 'Будункевич Мирослав', '+380000000000', 'Механіка', 550),
        (669706811, 'Будункевич Віктор', '+380936879999', 'Автомат', 490),
        (6640009381, 'Блажевський Ігор', '+380000000000', 'Механіка', 550),
        (501591448, 'Рекетчук Богдан', '+380000000000', 'Механіка', 550),
        (960755539, 'Данилишин Святослав', '+380000000000', 'Механіка', 550)
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
def get_next_dates(days=14):
    """Генерує список дат на найближчі N днів"""
    dates = []
    today = datetime.now(TZ).date()
    
    for i in range(days):
        date = today + timedelta(days=i)
        weekday = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Нд"][date.weekday()]
        formatted = f"{weekday} {date.strftime('%d.%m.%Y')}"
        dates.append(formatted)
    
    return dates

def get_available_time_slots(instructor_name, date_str):
    """
    ✅ ВИПРАВЛЕНО: Отримати вільні часові слоти з валідацією часу
    - Не показує минулі дати
    - Мінімум +1 година від поточного часу
    - Враховує хвилини
    """
    try:
        instructor_data = get_instructor_by_name(instructor_name)
        if not instructor_data:
            return []
        
        instructor_id = instructor_data[0]
        
        # Поточний час в правильній timezone
        now = datetime.now(TZ)
        
        # Парсимо дату
        date_obj = datetime.strptime(date_str, "%d.%m.%Y")
        
        # ✅ ВИПРАВЛЕННЯ 1: Якщо дата в минулому - повертаємо порожній список
        if date_obj.date() < now.date():
            logger.info(f"Дата {date_str} в минулому, слоти недоступні")
            return []
        
        # Перевіряємо чи це сьогодні
        is_today = date_obj.date() == now.date()
        
        # ✅ ВИПРАВЛЕННЯ 2: Мінімальний час для запису = поточний + 1 година
        if is_today:
            min_booking_time = now + timedelta(hours=1)
            
            # Округлюємо до наступної години
            min_hour = min_booking_time.hour
            if min_booking_time.minute > 0:
                min_hour += 1
            
            # Якщо округлена година виходить за межі робочого дня - немає слотів
            if min_hour >= WORK_HOURS_END:
                logger.info(f"Сьогодні вже пізно для запису (мінімум {min_hour}:00)")
                return []
            
            start_hour = max(min_hour, WORK_HOURS_START)
            logger.info(f"Сьогодні, мінімальна година для запису: {start_hour}:00")
        else:
            start_hour = WORK_HOURS_START
        
        # Всі можливі слоти
        all_slots = []
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
                hours_blocked = 2
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
        
        logger.info(f"Доступні слоти для {instructor_name} на {date_str}: {len(free_slots)}")
        return free_slots
        
    except Exception as e:
        logger.error(f"Помилка get_available_time_slots: {e}", exc_info=True)
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

# ======================= STATE MANAGEMENT =======================
async def get_or_create_state(context: ContextTypes.DEFAULT_TYPE):
    """Отримати або створити стан користувача"""
    if not hasattr(context.user_data, 'get') or 'state' not in context.user_data:
        context.user_data['state'] = {}
    return context.user_data['state']

async def clear_state(context: ContextTypes.DEFAULT_TYPE):
    """Очистити стан користувача"""
    context.user_data['state'] = {}

# ======================= КОМАНДИ =======================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Стартова команда"""
    user_id = update.effective_user.id
    
    # Перевірка чи це реєстрація через посилання
    if context.args:
        command = context.args[0]
        if command == "register490":
            await register_490(update, context)
            return
        elif command == "register550":
            await register_550(update, context)
            return
    
    await clear_state(context)
    
    # Перевірка ролі
    if user_id == ADMIN_ID:
        keyboard = [
            ["👨‍🏫 Інструктори", "📊 Звіти"],
            ["📋 Всі заняття", "⚙️ Налаштування"]
        ]
        text = "👋 Вітаю, Адміністратор!\n\nОберіть дію:"
    elif is_instructor(user_id):
        keyboard = [
            ["📅 Мій графік", "📊 Моя статистика"],
            ["🔒 Блокувати час", "🔓 Мої блокування"],
            ["📋 Мої заняття"]
        ]
        instructor = get_instructor_by_telegram_id(user_id)
        text = f"👋 Вітаю, {instructor[1]}!\n\nОберіть дію:"
    else:
        # Перевіряємо чи учень зареєстрований
        student = get_student_by_telegram_id(user_id)
        if student:
            keyboard = [
                ["🚗 Записатися", "📋 Мої записи"],
                ["📊 Моя статистика", "ℹ️ Інформація"]
            ]
            text = f"👋 Вітаю, {student[1]}!\n\nОберіть дію:"
        else:
            keyboard = [
                ["📝 Реєстрація"]
            ]
            text = ("👋 Вітаємо в боті автошколи!\n\n"
                   "Для початку роботи потрібно зареєструватися.\n"
                   "Натисніть кнопку нижче:")
    
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    await update.message.reply_text(text, reply_markup=reply_markup)

async def register_490(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Реєстрація учня з тарифом 490 грн"""
    user_id = update.effective_user.id
    
    # Перевіряємо чи вже зареєстрований
    student = get_student_by_telegram_id(user_id)
    if student:
        await update.message.reply_text(
            f"✅ Ви вже зареєстровані з тарифом {student[3]} грн!\n"
            "Використовуйте /start для роботи з ботом."
        )
        return
    
    # Зберігаємо тариф в контексті
    state = await get_or_create_state(context)
    state['registration_tariff'] = 490
    state['awaiting'] = 'registration_name'
    
    await update.message.reply_text(
        "📝 Реєстрація з тарифом 490 грн\n\n"
        "Введіть ваше повне ім'я:"
    )

async def register_550(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Реєстрація учня з тарифом 550 грн"""
    user_id = update.effective_user.id
    
    # Перевіряємо чи вже зареєстрований
    student = get_student_by_telegram_id(user_id)
    if student:
        await update.message.reply_text(
            f"✅ Ви вже зареєстровані з тарифом {student[3]} грн!\n"
            "Використовуйте /start для роботи з ботом."
        )
        return
    
    # Зберігаємо тариф в контексті
    state = await get_or_create_state(context)
    state['registration_tariff'] = 550
    state['awaiting'] = 'registration_name'
    
    await update.message.reply_text(
        "📝 Реєстрація з тарифом 550 грн\n\n"
        "Введіть ваше повне ім'я:"
    )

# Далі йде решта коду з bot.py...
# (Через обмеження довжини, показую тільки ключові виправлення)

# ДОДАТИ ДО ОБРОБНИКА ПОВІДОМЛЕНЬ:
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Головний обробник повідомлень"""
    text = update.message.text
    user_id = update.effective_user.id
    state = await get_or_create_state(context)
    
    # ... (існуючий код)
    
    # ✅ НОВЕПЕРЕВІРКА при записі на урок
    if state.get('awaiting') == 'booking_time':
        selected_time = text
        
        # Перевіряємо формат
        if not re.match(r'^\d{2}:\d{2}$', selected_time):
            await update.message.reply_text("❌ Невірний формат часу. Оберіть зі списку:")
            return
        
        instructor_name = state['instructor']
        date_str = state['booking_date']
        student_telegram_id = user_id
        
        # ✅ ВИПРАВЛЕННЯ 3: Перевірка чи учень вже має запис на цей час
        if check_student_has_booking_at_time(student_telegram_id, date_str, selected_time):
            await update.message.reply_text(
                "❌ У вас вже є запис на цей час!\n"
                "Оберіть інший час або скасуйте попередній запис."
            )
            return
        
        # ✅ ВИПРАВЛЕННЯ 4: Перевірка кількості записів на день
        booking_count = count_student_bookings_on_date(student_telegram_id, date_str)
        if booking_count >= MAX_LESSONS_PER_DAY:
            await update.message.reply_text(
                f"❌ Ви вже маєте {booking_count} записів на цей день!\n"
                f"Максимум {MAX_LESSONS_PER_DAY} години на день."
            )
            return
        
        # Продовжуємо з вибором тривалості...
        state['booking_time'] = selected_time
        state['awaiting'] = 'booking_duration'
        
        keyboard = [["1 година"], ["2 години"], ["🔙 Назад"]]
        reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        await update.message.reply_text("Оберіть тривалість:", reply_markup=reply_markup)

# ✅ ВИПРАВЛЕННЯ 2: Перевірка при блокуванні часу інструктором
async def handle_instructor_block_time(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Блокування часу інструктором"""
    state = await get_or_create_state(context)
    
    if state.get('awaiting') == 'block_time_confirm':
        instructor = get_instructor_by_telegram_id(update.effective_user.id)
        instructor_id = instructor[0]
        
        date = state['block_date']
        time_start = state['block_time_start']
        time_end = state['block_time_end']
        reason = state.get('block_reason', 'Особисті справи')
        
        # ✅ ПЕРЕВІРКА: Чи немає записів на цей час
        if not can_block_time_slot(instructor_id, date, time_start, time_end):
            await update.message.reply_text(
                "❌ Неможливо заблокувати цей час!\n"
                "На цей період вже є записи учнів.\n\n"
                "Спочатку потрібно перенести або скасувати існуючі заняття."
            )
            await clear_state(context)
            return
        
        # Блокуємо час
        from database import add_schedule_block
        if add_schedule_block(instructor_id, date, time_start, time_end, 'manual', reason):
            await update.message.reply_text(
                f"✅ Час заблоковано!\n\n"
                f"📅 Дата: {date}\n"
                f"🕐 Час: {time_start} - {time_end}\n"
                f"📝 Причина: {reason}"
            )
        else:
            await update.message.reply_text("❌ Помилка блокування часу.")
        
        await clear_state(context)

# ... (решта коду залишається без змін)

if __name__ == "__main__":
    main()

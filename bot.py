# bot.py - ОНОВЛЕНА ВЕРСІЯ З РОЗШИРЕНИМ ЛОГУВАННЯМ
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

WORK_HOURS_START = 8
WORK_HOURS_END = 18
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
    get_db,
    init_schedule_blocks_table,
    get_instructor_stats_period,
    get_admin_report_by_instructors,
    get_all_instructors,
    register_student,
    get_student_by_telegram_id,
    update_lesson,
    add_lesson_rating,
    init_reschedule_requests_table,
    create_reschedule_request,
    get_pending_reschedule_by_student,
    accept_reschedule_request,
    reject_reschedule_request,
    get_lesson_by_instructor_datetime
)

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

def ensure_instructors_exist():
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
    instructor = get_instructor_by_telegram_id(telegram_id)
    return instructor is not None

def get_next_dates(days=14):
    dates = []
    today = datetime.now().date()
    
    for i in range(days):
        date = today + timedelta(days=i)
        weekday = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Нд"][date.weekday()]
        formatted = f"{weekday} {date.strftime('%d.%m.%Y')}"
        dates.append(formatted)
    
    return dates

def get_available_time_slots(instructor_name, date_str):
    try:
        instructor_data = get_instructor_by_name(instructor_name)
        if not instructor_data:
            return []
        
        instructor_id = instructor_data[0]
        date_obj = datetime.strptime(date_str, "%d.%m.%Y")
        now = datetime.now(TZ)
        is_today = date_obj.date() == now.date()
        current_hour = now.hour
        current_minute = now.minute
        
        all_slots = []
        start_hour = WORK_HOURS_START
        
        if is_today:
            if current_minute > 0:
                start_hour = max(current_hour + 2, WORK_HOURS_START)
            else:
                start_hour = max(current_hour + 1, WORK_HOURS_START)
            
            if start_hour >= WORK_HOURS_END:
                return []
        
        hour = start_hour
        while hour < WORK_HOURS_END:
            all_slots.append(f"{hour:02d}:00")
            hour += 1
        
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT time, duration FROM lessons
                WHERE instructor_id = ? AND date = ? AND status = 'active'
            """, (instructor_id, date_str))
            booked = cursor.fetchall()
        
        blocked_hours = set()
        for booked_time, duration in booked:
            if ':' not in booked_time:
                continue
            
            start_h = int(booked_time.split(':')[0])
            
            if "1.5" in duration:
                hours_blocked = 2
            elif "2" in duration:
                hours_blocked = 2
            else:
                hours_blocked = 1
            
            for i in range(hours_blocked):
                blocked_hours.add(f"{start_h + i:02d}:00")
        
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

def validate_phone(phone):
    clean = re.sub(r'[\s\-\(\)]', '', phone)
    patterns = [
        r'^(\+?38)?0\d{9}$',
        r'^\d{10}$'
    ]
    return any(re.match(p, clean) for p in patterns)

def validate_date_format(date_str):
    try:
        datetime.strptime(date_str, "%d.%m.%Y")
        return True
    except ValueError:
        return False

def is_admin(user_id):
    return user_id == ADMIN_ID

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    logger.info(f"🟢 START викликано! User: {user_id}, Args: {context.args}")
    
    if context.args:
        command = context.args[0]
        logger.info(f"🔗 Deep link виявлено: {command}")
        if command == "register490":
            logger.info("➡️ Перенаправлення на register_490")
            await register_490(update, context)
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
            student = get_student_by_telegram_id(user_id)
            
            if student:
                context.user_data["student_name"] = student[1]
                context.user_data["student_phone"] = student[2]
                context.user_data["student_tariff"] = student[3]
                
                keyboard = [
                    [KeyboardButton("🚀 Записатися на заняття")],
                    [KeyboardButton("📋 Мої записи")]
                ]
                
                await update.message.reply_text(
                    f"Привіт, {student[1]}! 👋\n\n"
                    f"💰 Ваш тариф: {student[3]} грн/год\n\n"
                    f"Що бажаєте зробити?",
                    reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
                )
            else:
                await update.message.reply_text(
                    "⚠️ *Для запису на заняття потрібна реєстрація*\n\n"
                    "Зверніться до адміністратора за посиланням для реєстрації.\n\n"
                    "📞 Контакт: @ваш_адмін",
                    parse_mode="Markdown"
                )
        
    except Exception as e:
        logger.error(f"Error in start: {e}", exc_info=True)
        await update.message.reply_text("❌ Виникла помилка. Спробуйте /start")

async def register_490(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.info("🔵 register_490 викликано!")
    try:
        await register_student_with_tariff(update, context, 490)
    except Exception as e:
        logger.error(f"❌ Помилка в register_490: {e}", exc_info=True)

async def register_550(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await register_student_with_tariff(update, context, 550)

async def register_student_with_tariff(update: Update, context: ContextTypes.DEFAULT_TYPE, tariff: int):
    user = update.message.from_user
    user_id = user.id
    logger.info(f"🟡 register_student_with_tariff викликано! User: {user_id}, Tariff: {tariff}")
    
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
    
    async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Головний обробник"""
    text = update.message.text
    state = context.user_data.get("state", "")
    user_id = update.message.from_user.id
    
    # 🔥 РОЗШИРЕНЕ ЛОГУВАННЯ - ПОКАЗУЄ ВСІ ДАНІ
    logger.info(f"📥 Message: '{text}' | State: '{state}' | UserData: {context.user_data}")
    
    try:
        # === ПЕРЕВІРКА НА ЗАПИТ ПЕРЕНЕСЕННЯ (пріоритет!) ===
        if text in ["✅ Так, обрати новий час", "❌ Ні, залишити як є"] and not state:
            request_data = get_pending_reschedule_by_student(user_id)
            if request_data:
                context.user_data["state"] = "reschedule_response"
                await handle_reschedule_response(update, context)
                return
        
        # === РЕЄСТРАЦІЯ УЧНЯ ===
        if state == "registration_name":
            if text == "🔙 Скасувати":
                await update.message.reply_text("❌ Реєстрацію скасовано.")
                return
            
            context.user_data["student_name"] = text
            context.user_data["state"] = "registration_phone"
            
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
            
            phone = None
            if update.message.contact:
                phone = update.message.contact.phone_number
            elif validate_phone(text):
                phone = text
            else:
                await update.message.reply_text("⚠️ Невірний формат номера. Спробуйте ще раз:")
                return
            
            user_id = update.message.from_user.id
            name = context.user_data["student_name"]
            tariff = context.user_data["registration_tariff"]
            
            if register_student(name, phone, user_id, tariff, f"link_{tariff}"):
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
        
        if state == "admin_panel":
            await handle_admin_report(update, context)
            return
        
        if state == "admin_report_period":
            await handle_admin_report(update, context)
            return

        # === МЕНЮ ІНСТРУКТОРА ===
        if text == "🔙 Назад":
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
            await show_instructor_stats_menu(update, context)
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
        
        # === ОЦІНЮВАННЯ УЧНЯ ===
        if state in ["rating_select_lesson", "rating_give_score", "rating_give_feedback"]:
            await handle_rating_flow(update, context)
            return

        # === КОРИГУВАННЯ ГРАФІКУ ===
        if state in ["edit_schedule_select", "edit_schedule_date", "edit_schedule_time"]:
            await handle_edit_schedule(update, context)
            return

        # === УПРАВЛІННЯ ГРАФІКОМ ===
        # 🔥 ДОДАНО "block_with_conflict" В СПИСОК
        if state in ["schedule_menu", "block_choose_date", "block_choose_time_start", 
                     "block_choose_time_end", "block_choose_reason", "unblock_choose_date", "block_with_conflict"]:
            await handle_schedule_management(update, context)
            return

        # === ПЕРЕНЕСЕННЯ ЗАНЯТЬ (УЧЕНЬ) ===
        if state == "reschedule_response":
            await handle_reschedule_response(update, context)
            return
        
        if state == "reschedule_choose_date":
            await handle_reschedule_date_selection(update, context)
            return
        
        if state == "reschedule_choose_time":
            await handle_reschedule_time_selection(update, context)
            return
        
        if state == "reschedule_confirm":
            await handle_reschedule_confirmation(update, context)
            return

        # === МЕНЮ СТУДЕНТА ===
        if text == "🚀 Записатися на заняття":
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
        
        # === ПІДТВЕРДЖЕННЯ ===
        if state == "waiting_for_confirmation":
            if text == "✅ Підтвердити":
                await save_lesson(update, context)
                return
            elif text == "🔙 Скасувати":
                await update.message.reply_text("❌ Запис скасовано.")
                await start(update, context)
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
            
            dates = get_next_dates(14)
            
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
            
            date_parts = text.split()
            if len(date_parts) == 2:
                date_str = date_parts[1]
            else:
                date_str = text
            
            logger.info(f"📆 Витягнута дата: {date_str}")
            
            if not validate_date_format(date_str):
                logger.warning(f"⚠️ Невірний формат дати: {date_str}")
                await update.message.reply_text(
                    "⚠️ Невірний формат дати. Оберіть дату з меню."
                )
                return
            
            date_obj = datetime.strptime(date_str, "%d.%m.%Y")
            if date_obj.date() < datetime.now().date():
                logger.warning(f"⚠️ Минула дата: {date_str}")
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
            
            selected_date = context.user_data.get("date")
            selected_time = text
            
            try:
                date_obj = datetime.strptime(selected_date, "%d.%m.%Y")
                time_obj = datetime.strptime(selected_time, "%H:%M")
                
                selected_datetime = datetime(
                    date_obj.year, date_obj.month, date_obj.day,
                    time_obj.hour, time_obj.minute,
                    tzinfo=TZ
                )
                
                now = datetime.now(TZ)
                min_allowed_time = now + timedelta(hours=1)
                
                if selected_datetime < min_allowed_time:
                    await update.message.reply_text(
                        "⚠️ *Запис має бути мінімум за 1 годину!*\n\n"
                        "Будь ласка, оберіть інший час.\n\n"
                        f"Зараз: {now.strftime('%H:%M')}\n"
                        f"Мінімальний час: {min_allowed_time.strftime('%H:%M')}",
                        parse_mode="Markdown"
                    )
                    return
                    
            except Exception as e:
                logger.error(f"Error checking past time: {e}")
            
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
            
            if text == "2 години":
                selected_time = context.user_data["time"]
                instructor = context.user_data["instructor"]
                date = context.user_data["date"]
                
                selected_hour = int(selected_time.split(':')[0])
                next_hour = f"{selected_hour + 1:02d}:00"
                
                free_slots = get_available_time_slots(instructor, date)
                
                if next_hour not in free_slots and next_hour != f"{WORK_HOURS_END:02d}:00":
                    await update.message.reply_text(
                        "⚠️ Наступна година зайнята. Оберіть інший час або 1 годину."
                    )
                    return
            
            context.user_data["duration"] = text
            
            user = update.message.from_user
            student = get_student_by_telegram_id(user.id)
            
            if student:
                context.user_data["student_name"] = student[1]
                context.user_data["student_phone"] = student[2]
                context.user_data["student_tariff"] = student[3]
                
                await show_booking_confirmation(update, context)
            else:
                await update.message.reply_text(
                    "⚠️ *Помилка!*\n\n"
                    "Для запису потрібна реєстрація через спеціальне посилання.\n"
                    "Зверніться до адміністратора.",
                    parse_mode="Markdown"
                )
                await start(update, context)
            return
        
    except Exception as e:
        logger.error(f"Error in handle_message: {e}", exc_info=True)
        await update.message.reply_text("❌ Виникла помилка. Спробуйте /start")

async def show_booking_confirmation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    instructor = context.user_data["instructor"]
    date = context.user_data["date"]
    time = context.user_data["time"]
    duration = context.user_data["duration"]
    name = context.user_data.get("student_name", "")
    phone = context.user_data.get("student_phone", "")
    student_tariff = context.user_data.get("student_tariff", 0)
    
    if student_tariff > 0:
        if "2" in duration:
            price = student_tariff * 2
        else:
            price = student_tariff
    else:
        price = PRICES.get(duration, 400)
    
    context.user_data["state"] = "waiting_for_confirmation"
    
    keyboard = [
        [KeyboardButton("✅ Підтвердити")],
        [KeyboardButton("🔙 Скасувати")]
    ]
    
    await update.message.reply_text(
        f"📋 *Підтвердження запису*\n\n"
        f"👨‍🏫 Інструктор: {instructor}\n"
        f"📅 Дата: {date}\n"
        f"🕐 Час: {time}\n"
        f"⏱ Тривалість: {duration}\n"
        f"💰 Вартість: {price:.0f} грн\n\n"
        f"Все вірно?",
        reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True),
        parse_mode="Markdown"
    )async def show_instructor_schedule(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    
    try:
        instructor_data = get_instructor_by_telegram_id(user_id)
        if not instructor_data:
            await update.message.reply_text("❌ Ви не зареєстровані як інструктор.")
            return
        
        instructor_id, instructor_name = instructor_data
        now = datetime.now(TZ)
        
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT date, time, duration, student_name, student_phone, status
                FROM lessons
                WHERE instructor_id = ? 
                AND status = 'active'
                ORDER BY date, time
            """, (instructor_id,))
            
            all_lessons = cursor.fetchall()
        
        lessons = []
        for date, time, duration, student_name, student_phone, status in all_lessons:
            try:
                lesson_datetime = datetime.strptime(f"{date} {time}", "%d.%m.%Y %H:%M")
                lesson_datetime = TZ.localize(lesson_datetime)
                
                if lesson_datetime >= now:
                    lessons.append((date, time, duration, student_name, student_phone, status))
            except:
                lessons.append((date, time, duration, student_name, student_phone, status))
        
        lessons = lessons[:20]
        
        if not lessons:
            await update.message.reply_text("📋 У вас поки немає запланованих занять.")
            return
        
        text = f"📅 *Ваш розклад:*\n\n"
        current_date = None
        
        for date, time, duration, student_name, student_phone, status in lessons:
            if date != current_date:
                text += f"\n📆 *{date}*\n"
                current_date = date
            
            text += f"🕐 {time} ({duration})\n"
            text += f"👤 {student_name}\n"
            if student_phone:
                text += f"📱 {student_phone}\n"
            text += "\n"
        
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
    keyboard = [
        [KeyboardButton("📊 За сьогодні")],
        [KeyboardButton("📊 За тиждень")],
        [KeyboardButton("📊 За місяць")],
        [KeyboardButton("📊 Свій період")],
        [KeyboardButton("🔙 Назад")]
    ]
    
    await update.message.reply_text(
        "📊 *Статистика*\n\nОберіть період:",
        reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True),
        parse_mode="Markdown"
    )
    
    context.user_data["state"] = "stats_period"

async def handle_stats_period(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    user_id = update.message.from_user.id
    
    if text == "🔙 Назад":
        await start(update, context)
        return
    
    instructor_data = get_instructor_by_telegram_id(user_id)
    if not instructor_data:
        await update.message.reply_text("❌ Помилка.")
        return
    
    instructor_id, instructor_name = instructor_data
    today = datetime.now().date()
    
    if text == "📊 За сьогодні":
        date_from = today.strftime("%Y-%m-%d")
        date_to = today.strftime("%Y-%m-%d")
        period_text = "сьогодні"
    elif text == "📊 За тиждень":
        date_from = (today - timedelta(days=7)).strftime("%Y-%m-%d")
        date_to = today.strftime("%Y-%m-%d")
        period_text = "за тиждень"
    elif text == "📊 За місяць":
        date_from = (today - timedelta(days=30)).strftime("%Y-%m-%d")
        date_to = today.strftime("%Y-%m-%d")
        period_text = "за місяць"
    elif text == "📊 Свій період":
        context.user_data["state"] = "stats_custom_period"
        await update.message.reply_text(
            "📅 Введіть період у форматі:\n"
            "*ДД.ММ.РРРР - ДД.ММ.РРРР*\n\n"
            "Наприклад: 01.11.2024 - 30.11.2024",
            parse_mode="Markdown"
        )
        return
    else:
        await update.message.reply_text("⚠️ Оберіть період із меню.")
        return
    
    await show_instructor_stats(update, context, instructor_id, date_from, date_to, period_text)

async def show_instructor_stats(update: Update, context: ContextTypes.DEFAULT_TYPE, instructor_id, date_from, date_to, period_text):
    try:
        stats = get_instructor_stats_period(instructor_id, date_from, date_to)
        
        if not stats:
            await update.message.reply_text("❌ Помилка отримання статистики.")
            return
        
        text = f"📊 *Статистика {period_text}*\n\n"
        text += f"📝 Занять проведено: {stats['total_lessons']}\n"
        text += f"⏱ Годин відпрацьовано: {stats['total_hours']}\n"
        text += f"💰 Заробіток: {stats['earnings']:.0f} грн\n"
        text += f"⭐ Середній рейтинг: {stats['avg_rating']}\n"
        text += f"❌ Скасовано: {stats['cancelled']}\n"
        
        await update.message.reply_text(text, parse_mode="Markdown")
        await start(update, context)
        
    except Exception as e:
        logger.error(f"Error in show_instructor_stats: {e}", exc_info=True)
        await update.message.reply_text("❌ Помилка.")

async def show_cancellation_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
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

async def rate_student_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    
    try:
        instructor_data = get_instructor_by_telegram_id(user_id)
        if not instructor_data:
            await update.message.reply_text("❌ Ви не інструктор.")
            return
        
        instructor_id = instructor_data[0]
        
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id, date, time, student_name
                FROM lessons
                WHERE instructor_id = ? 
                  AND status = 'completed'
                  AND rating IS NULL
                ORDER BY date DESC, time DESC
                LIMIT 10
            """, (instructor_id,))
            
            lessons = cursor.fetchall()
        
        if not lessons:
            await update.message.reply_text("📋 Немає занять для оцінювання.")
            return
        
        context.user_data["lessons_to_rate"] = lessons
        context.user_data["state"] = "rating_select_lesson"
        
        text = "⭐ *Оберіть заняття для оцінювання:*\n\n"
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
        
    except Exception as e:
        logger.error(f"Error in rate_student_menu: {e}", exc_info=True)
        await update.message.reply_text("❌ Помилка.")

async def handle_rating_flow(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
        
        if add_lesson_rating(lesson_id, rating, feedback):
            await update.message.reply_text(
                f"✅ Оцінку додано!\n\n"
                f"👤 {student_name}\n"
                f"⭐ Оцінка: {rating}/5"
            )
        else:
            await update.message.reply_text("❌ Помилка збереження оцінки.")
        
        context.user_data.clear()
        await start(update, context)

async def handle_edit_schedule(update: Update, context: ContextTypes.DEFAULT_TYPE):
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

async def manage_schedule(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
    text = update.message.text
    state = context.user_data.get("state")
    
    logger.info(f"🔧 handle_schedule_management: text='{text}', state='{state}'")
    # 🔥 РОЗШИРЕНЕ ЛОГУВАННЯ - ПОКАЗУЄ ВСІ ДАНІ
    logger.info(f"📦 Full context.user_data: {context.user_data}")
    
    if text == "🔙 Назад":
        logger.info("⬅️ Назад натиснуто")
        if state == "schedule_menu":
            await start(update, context)
        else:
            await manage_schedule(update, context)
        return
    
    if text == "🔴 Заблокувати час":
        logger.info("🔴 Кнопка 'Заблокувати час' натиснута - показую календар")
        context.user_data["state"] = "block_choose_date"
        
        dates = get_next_dates(30)
        
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
    
    # === ОБРОБКА КОНФЛІКТУ ПРИ БЛОКУВАННІ ===
    if state == "block_with_conflict":
        logger.info(f"🔧 Обробка block_with_conflict, text='{text}'")
        if text == "🔄 Запропонувати перенести":
            logger.info("✅ Кнопка перенесення натиснута")
            user_id = update.message.from_user.id
            instructor_data = get_instructor_by_telegram_id(user_id)
            
            if not instructor_data:
                await update.message.reply_text("❌ Помилка.")
                return
            
            instructor_id, instructor_name = instructor_data[0], instructor_data[1]
            
            lesson_id = context.user_data.get("temp_lesson_id")
            student_name = context.user_data.get("temp_student_name")
            student_telegram_id = context.user_data.get("temp_student_telegram_id")
            date = context.user_data.get("temp_block_date")
            time = context.user_data.get("temp_lesson_time")
            duration = context.user_data.get("temp_duration")
            
            logger.info(f"📋 Дані для запиту: lesson={lesson_id}, student={student_name}, date={date}, time={time}")
            
            request_id = create_reschedule_request(
                lesson_id, 
                instructor_id, 
                instructor_name,
                student_telegram_id,
                student_name,
                date, 
                time,
                duration,
                "Інструктор хоче заблокувати цей час"
            )
            
            logger.info(f"💾 Запит створено: request_id={request_id}")
            
            if request_id:
                try:
                    keyboard = [
                        [KeyboardButton("✅ Так, обрати новий час")],
                        [KeyboardButton("❌ Ні, залишити як є")]
                    ]
                    
                    date_obj = datetime.strptime(date, "%Y-%m-%d")
                    date_display = date_obj.strftime("%d.%m.%Y")
                    
                    logger.info(f"📨 Відправляю повідомлення учню {student_telegram_id}")
                    
                    await context.bot.send_message(
                        chat_id=student_telegram_id,
                        text=f"🔔 *Запит на перенесення*\n\n"
                             f"👨‍🏫 Інструктор: {instructor_name}\n\n"
                             f"Ваше заняття:\n"
                             f"📅 {date_display} о {time}\n"
                             f"⏱ Тривалість: {duration}\n\n"
                             f"⚠️ Інструктор просить перенести це заняття на інший час.\n\n"
                             f"Ви згодні перенести?",
                        reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=True),
                        parse_mode="Markdown"
                    )
                    
                    logger.info("✅ Повідомлення учню відправлено успішно!")
                    
                    await update.message.reply_text(
                        "✅ *Запит відправлено!*\n\n"
                        "📨 Учень отримає повідомлення з проханням перенести заняття.\n\n"
                        "⏳ Очікуємо підтвердження...\n\n"
                        "Після підтвердження учня ви зможете заблокувати цей час.",
                        parse_mode="Markdown"
                    )
                    
                except Exception as e:
                    logger.error(f"Failed to send reschedule request: {e}", exc_info=True)
                    await update.message.reply_text("❌ Помилка відправки запиту.")
            else:
                await update.message.reply_text("❌ Помилка створення запиту.")
            
            context.user_data.clear()
            await manage_schedule(update, context)
            return
        
        elif text == "🔙 Назад":
            context.user_data.clear()
            await manage_schedule(update, context)
            return
    
    logger.info(f"📍 Перевірка стану: {state}")
    if state == "block_choose_date":
        date_parts = text.split()
        if len(date_parts) == 2:
            date_str = date_parts[1]
        else:
            date_str = text
        
        logger.info(f"📆 Обробка дати блокування: {date_str}")
        
        if not validate_date_format(date_str):
            logger.warning(f"⚠️ Невірний формат дати: {date_str}")
            await update.message.reply_text("⚠️ Невірний формат. Оберіть дату з меню.")
            return
        
        context.user_data["block_date"] = date_str
        context.user_data["state"] = "block_choose_time_start"
        
        keyboard = []
        for hour in range(WORK_HOURS_START, WORK_HOURS_END):
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
        
        date_obj = datetime.strptime(block_date, "%d.%m.%Y")
        date_formatted = date_obj.strftime("%Y-%m-%d")
        
        logger.info(f"🔍 Перевіряю конфлікти для інструктора {instructor_id}, дата {date_formatted}, час {time_start}-{time_end}")
        
        start_hour = int(time_start.split(':')[0])
        end_hour = int(time_end.split(':')[0])
        
        conflicting_lessons = []
        for hour in range(start_hour, end_hour):
            time_slot = f"{hour:02d}:00"
            logger.info(f"  🔎 Перевіряю час {time_slot}...")
            lesson = get_lesson_by_instructor_datetime(instructor_id, date_formatted, time_slot)
            if lesson:
                logger.info(f"  ⚠️ ЗНАЙДЕНО КОНФЛІКТ: {lesson}")
                conflicting_lessons.append((time_slot, lesson))
            else:
                logger.info(f"  ✅ Час {time_slot} вільний")
        
        logger.info(f"📊 Всього конфліктів: {len(conflicting_lessons)}")
        
        if conflicting_lessons:
            time_slot, lesson_data = conflicting_lessons[0]
            lesson_id, student_name, student_telegram_id, duration, student_tariff = lesson_data
            
            context.user_data["temp_block_date"] = date_formatted
            context.user_data["temp_block_date_display"] = block_date
            context.user_data["temp_block_time_start"] = time_start
            context.user_data["temp_block_time_end"] = time_end
            context.user_data["temp_block_reason"] = reason
            context.user_data["temp_lesson_id"] = lesson_id
            context.user_data["temp_student_name"] = student_name
            context.user_data["temp_student_telegram_id"] = student_telegram_id
            context.user_data["temp_lesson_time"] = time_slot
            context.user_data["temp_duration"] = duration
            context.user_data["state"] = "block_with_conflict"
            
            keyboard = [
                [KeyboardButton("🔄 Запропонувати перенести")],
                [KeyboardButton("🔙 Назад")]
            ]
            
            await update.message.reply_text(
                f"⚠️ *На цей час є запис!*\n\n"
                f"📋 Деталі:\n"
                f"👤 Учень: {student_name}\n"
                f"📅 Дата: {block_date}\n"
                f"🕐 Час: {time_slot}\n"
                f"⏱ Тривалість: {duration}\n\n"
                f"Що бажаєте зробити?",
                reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True),
                parse_mode="Markdown"
            )
            return
        
        from database import add_schedule_block
        
        if add_schedule_block(instructor_id, date_formatted, time_start, time_end, "blocked", reason):
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

async def show_admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [KeyboardButton("📊 Звіт по інструкторах")],
        [KeyboardButton("👥 Список інструкторів")],
        [KeyboardButton("🔙 Назад")]
    ]
    
    await update.message.reply_text(
        "🔐 *Панель адміністратора*\n\nОберіть дію:",
        reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True),
        parse_mode="Markdown"
    )
    
    context.user_data["state"] = "admin_panel"

async def handle_admin_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    
    if text == "🔙 Назад":
        await show_admin_panel(update, context)
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
        
        text = "👥 *Список інструкторів:*\n\n"
        for i, (inst_id, name, transmission, telegram_id) in enumerate(instructors, 1):
            text += f"{i}. {name} ({transmission})\n"
            text += f"   ID: {telegram_id}\n\n"
        
        await update.message.reply_text(text, parse_mode="Markdown")
        return
    
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
            "*ДД.ММ.РРРР - ДД.ММ.РРРР*\n\n"
            "Наприклад: 01.11.2024 - 30.11.2024",
            parse_mode="Markdown"
        )
        context.user_data["state"] = "admin_custom_period"
        return
    else:
        return
    
    await generate_admin_report(update, context, date_from, date_to, period_text)

async def generate_admin_report(update: Update, context: ContextTypes.DEFAULT_TYPE, date_from, date_to, period_text):
    try:
        report_data = get_admin_report_by_instructors(date_from, date_to)
        
        if not report_data:
            await update.message.reply_text("📋 Немає даних за цей період.")
            return
        
        text = f"📊 *Звіт по інструкторах {period_text}*\n\n"
        text += f"📅 Період: {date_from} - {date_to}\n\n"
        
        total_lessons = 0
        total_hours = 0
        total_earnings = 0
        
        for name, lessons, hours, avg_rating, cancelled in report_data:
            if lessons > 0:
                hours = hours or 0
                earnings = hours * 400
                
                text += f"👨‍🏫 *{name}*\n"
                text += f"   📝 Занять: {lessons}\n"
                text += f"   ⏱ Годин: {hours:.1f}\n"
                text += f"   💰 Заробіток: {earnings:.0f} грн\n"
                text += f"   ⭐ Рейтинг: {avg_rating:.1f if avg_rating else 0}\n"
                text += f"   ❌ Скасовано: {cancelled}\n\n"
                
                total_lessons += lessons
                total_hours += hours
                total_earnings += earnings
        
        text += f"\n📊 *ЗАГАЛОМ:*\n"
        text += f"📝 Занять: {total_lessons}\n"
        text += f"⏱ Годин: {total_hours:.1f}\n"
        text += f"💰 Заробіток: {total_earnings:.0f} грн\n"
        
        await update.message.reply_text(text, parse_mode="Markdown")
        
    except Exception as e:
        logger.error(f"Error in generate_admin_report: {e}", exc_info=True)
        await update.message.reply_text("❌ Помилка генерації звіту.")

async def show_student_lessons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT l.date, l.time, l.duration, i.name, l.status
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
        
        text = "📖 *Ваші записи:*\n\n"
        
        for date, time, duration, instructor_name, status in lessons:
            text += f"📅 {date} | 🕐 {time} ({duration})\n"
            text += f"👨‍🏫 {instructor_name}\n\n"
        
        await update.message.reply_text(text, parse_mode="Markdown")
        
    except Exception as e:
        logger.error(f"Error in show_student_lessons: {e}", exc_info=True)
        await update.message.reply_text("❌ Помилка завантаження записів.")

async def save_lesson(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
        
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO lessons 
                (instructor_id, student_name, student_telegram_id, student_phone, student_tariff, date, time, duration, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'active')
            """, (instructor_id, student_name, student_telegram_id, student_phone, student_tariff, date, time, duration))
            conn.commit()
        
        await update.message.reply_text(
            f"✅ *Заняття заброньовано!*\n\n"
            f"👨‍🏫 Інструктор: {instructor_name}\n"
            f"📅 Дата: {date}\n"
            f"🕐 Час: {time}\n"
            f"⏱ Тривалість: {duration}",
            parse_mode="Markdown"
        )
        
        if student_tariff > 0:
            if "2" in duration:
                price = student_tariff * 2
            else:
                price = student_tariff
        else:
            price = PRICES.get(duration, 400)
        
        if instructor_telegram_id:
            try:
                await context.bot.send_message(
                    chat_id=instructor_telegram_id,
                    text=f"🔔 *Новий запис!*\n\n"
                         f"👤 Учень: {student_name}\n"
                         f"📱 Телефон: {student_phone}\n"
                         f"📅 Дата: {date}\n"
                         f"🕐 Час: {time}\n"
                         f"⏱ Тривалість: {duration}\n"
                         f"💰 Вартість: *{price:.0f} грн*",
                    parse_mode="Markdown"
                )
            except Exception as e:
                logger.error(f"Не вдалося надіслати повідомлення інструктору: {e}")
        
        await start(update, context)
        
    except Exception as e:
        logger.error(f"Error in save_lesson: {e}", exc_info=True)
        await update.message.reply_text("❌ Помилка збереження запису.")

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
    try:
        from database import remove_schedule_block
        
        if remove_schedule_block(block_id):
            await query.edit_message_text("✅ Час розблоковано!")
        else:
            await query.edit_message_text("❌ Помилка розблокування.")
            
    except Exception as e:
        logger.error(f"Error in handle_unblock_callback: {e}", exc_info=True)
        await query.edit_message_text("❌ Помилка.")

async def send_reminders(context: ContextTypes.DEFAULT_TYPE):
    try:
        now = datetime.now(TZ)
        tomorrow = now + timedelta(hours=24)
        
        with get_db() as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT l.id, l.student_telegram_id, i.name, l.date, l.time
                FROM lessons l
                JOIN instructors i ON l.instructor_id = i.id
                WHERE l.status = 'active' 
                AND l.reminder_24h_sent = 0
                AND datetime(l.date || ' ' || l.time) <= ?
                AND datetime(l.date || ' ' || l.time) > ?
            """, (tomorrow.strftime("%Y-%m-%d %H:%M"), now.strftime("%Y-%m-%d %H:%M")))
            
            lessons_24h = cursor.fetchall()
            
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
            
            in_2_hours = now + timedelta(hours=2)
            
            cursor.execute("""
                SELECT l.id, l.student_telegram_id, i.name, l.date, l.time
                FROM lessons l
                JOIN instructors i ON l.instructor_id = i.id
                WHERE l.status = 'active' 
                AND l.reminder_2h_sent = 0
                AND datetime(l.date || ' ' || l.time) <= ?
                AND datetime(l.date || ' ' || l.time) > ?
            """, (in_2_hours.strftime("%Y-%m-%d %H:%M"), now.strftime("%Y-%m-%d %H:%M")))
            
            lessons_2h = cursor.fetchall()
            
            for lesson_id, student_id, instructor, date, time in lessons_2h:
                try:
                    await context.bot.send_message(
                        chat_id=student_id,
                        text=f"🔔 *Нагадування!*\n\nУ вас заняття через 2 години:\n"
                             f"👨‍🏫 {instructor}\n📅 {date}\n🕐 {time}\n\n"
                             f"⏰ Не забудьте підготуватися!",
                        parse_mode="Markdown"
                    )
                    
                    cursor.execute("UPDATE lessons SET reminder_2h_sent = 1 WHERE id = ?", (lesson_id,))
                    conn.commit()
                except Exception as e:
                    logger.error(f"Failed to send 2h reminder: {e}")
        
        logger.info("Reminders sent successfully")
        
    except Exception as e:
        logger.error(f"Error in send_reminders: {e}", exc_info=True)

async def check_completed_lessons(context: ContextTypes.DEFAULT_TYPE):
    try:
        now = datetime.now(TZ)
        
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE lessons
                SET status = 'completed', completed_at = CURRENT_TIMESTAMP
                WHERE status = 'active'
                AND datetime(date || ' ' || l.time) < ?
            """, (now.strftime("%Y-%m-%d %H:%M"),))
            
            conn.commit()
            
        logger.info("Completed lessons checked")
        
    except Exception as e:
        logger.error(f"Error in check_completed_lessons: {e}", exc_info=True)

async def handle_reschedule_response(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    user_id = update.message.from_user.id
    
    request_data = get_pending_reschedule_by_student(user_id)
    
    if not request_data:
        await update.message.reply_text("❌ Активних запитів на перенесення немає.")
        return
    
    request_id, lesson_id, instructor_name, old_date, old_time, duration, created_at, instructor_id = request_data
    
    if text == "✅ Так, обрати новий час":
        context.user_data["reschedule_request_id"] = request_id
        context.user_data["reschedule_lesson_id"] = lesson_id
        context.user_data["reschedule_instructor_name"] = instructor_name
        context.user_data["reschedule_instructor_id"] = instructor_id
        context.user_data["reschedule_old_date"] = old_date
        context.user_data["reschedule_old_time"] = old_time
        context.user_data["reschedule_duration"] = duration
        context.user_data["state"] = "reschedule_choose_date"
        
        dates = get_next_dates(14)
        keyboard = []
        for i in range(0, len(dates), 2):
            row = [KeyboardButton(dates[i])]
            if i + 1 < len(dates):
                row.append(KeyboardButton(dates[i + 1]))
            keyboard.append(row)
        
        keyboard.append([KeyboardButton("🔙 Скасувати")])
        
        await update.message.reply_text(
            "📅 Оберіть нову дату для заняття:",
            reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        )
        return
        
    elif text == "❌ Ні, залишити як є":
        if reject_reschedule_request(request_id):
            try:
                await context.bot.send_message(
                    chat_id=instructor_id,
                    text=f"❌ *Учень відмовився від перенесення*\n\n"
                         f"👤 Учень не хоче переносити заняття.\n\n"
                         f"Запис залишається:\n"
                         f"📅 {old_date} о {old_time}\n\n"
                         f"Цей час заблокувати неможливо.",
                    parse_mode="Markdown"
                )
            except Exception as e:
                logger.error(f"Failed to notify instructor: {e}")
            
            await update.message.reply_text(
                "✅ Ваш вибір збережено.\n\n"
                "Заняття залишається на початковому часі.",
                reply_markup=ReplyKeyboardMarkup([[KeyboardButton("📋 Мої записи")]], resize_keyboard=True)
            )
        else:
            await update.message.reply_text("❌ Помилка збереження відповіді.")
        
        context.user_data.clear()
        return

async def handle_reschedule_date_selection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    
    if text == "🔙 Скасувати":
        await update.message.reply_text(
            "❌ Перенесення скасовано.",
            reply_markup=ReplyKeyboardMarkup([[KeyboardButton("📋 Мої записи")]], resize_keyboard=True)
        )
        
        request_id = context.user_data.get("reschedule_request_id")
        if request_id:
            reject_reschedule_request(request_id)
        
        context.user_data.clear()
        return
    
    try:
        parts = text.split()
        date_part = parts[1]
        date_obj = datetime.strptime(date_part, "%d.%m.%Y")
        
        context.user_data["reschedule_new_date"] = date_part
        context.user_data["reschedule_new_date_obj"] = date_obj
        context.user_data["state"] = "reschedule_choose_time"
        
        instructor_name = context.user_data.get("reschedule_instructor_name")
        free_slots = get_available_time_slots(instructor_name, date_part)
        
        if not free_slots:
            await update.message.reply_text(
                "😔 На цю дату немає вільних місць.\n\nОберіть іншу дату:"
            )
            return
        
        keyboard = []
        for i in range(0, len(free_slots), 3):
            row = [KeyboardButton(free_slots[j]) for j in range(i, min(i + 3, len(free_slots)))]
            keyboard.append(row)
        
        keyboard.append([KeyboardButton("🔙 Назад")])
        
        await update.message.reply_text(
            f"🕐 Оберіть новий час на {date_part}:",
            reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        )
        
    except Exception as e:
        logger.error(f"Error parsing reschedule date: {e}")
        await update.message.reply_text("❌ Невірний формат дати. Спробуйте ще раз.")

async def handle_reschedule_time_selection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    
    if text == "🔙 Назад":
        context.user_data["state"] = "reschedule_choose_date"
        dates = get_next_dates(14)
        keyboard = []
        for i in range(0, len(dates), 2):
            row = [KeyboardButton(dates[i])]
            if i + 1 < len(dates):
                row.append(KeyboardButton(dates[i + 1]))
            keyboard.append(row)
        keyboard.append([KeyboardButton("🔙 Скасувати")])
        
        await update.message.reply_text(
            "📅 Оберіть нову дату:",
            reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        )
        return
    
    new_time = text
    new_date = context.user_data.get("reschedule_new_date")
    old_date = context.user_data.get("reschedule_old_date")
    old_time = context.user_data.get("reschedule_old_time")
    duration = context.user_data.get("reschedule_duration")
    instructor_name = context.user_data.get("reschedule_instructor_name")
    
    keyboard = [
        [KeyboardButton("✅ Підтвердити перенесення")],
        [KeyboardButton("🔙 Назад")]
    ]
    
    await update.message.reply_text(
        f"📋 *Підтвердження перенесення*\n\n"
        f"🗓 *Старий час:*\n"
        f"📅 {old_date} о {old_time}\n\n"
        f"🆕 *Новий час:*\n"
        f"📅 {new_date} о {new_time}\n\n"
        f"👨‍🏫 Інструктор: {instructor_name}\n"
        f"⏱ Тривалість: {duration}\n\n"
        f"Підтверджуєте перенесення?",
        reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True),
        parse_mode="Markdown"
    )
    
    context.user_data["reschedule_new_time"] = new_time
    context.user_data["state"] = "reschedule_confirm"

async def handle_reschedule_confirmation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    
    if text == "🔙 Назад":
        context.user_data["state"] = "reschedule_choose_time"
        
        date_part = context.user_data.get("reschedule_new_date")
        instructor_name = context.user_data.get("reschedule_instructor_name")
        free_slots = get_available_time_slots(instructor_name, date_part)
        
        keyboard = []
        for i in range(0, len(free_slots), 3):
            row = [KeyboardButton(free_slots[j]) for j in range(i, min(i + 3, len(free_slots)))]
            keyboard.append(row)
        keyboard.append([KeyboardButton("🔙 Назад")])
        
        await update.message.reply_text(
            f"🕐 Оберіть новий час на {date_part}:",
            reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        )
        return
    
    if text == "✅ Підтвердити перенесення":
        request_id = context.user_data.get("reschedule_request_id")
        new_date = context.user_data.get("reschedule_new_date")
        new_time = context.user_data.get("reschedule_new_time")
        
        date_obj = datetime.strptime(new_date, "%d.%m.%Y")
        new_date_db = date_obj.strftime("%Y-%m-%d")
        
        if accept_reschedule_request(request_id, new_date_db, new_time):
            old_date = context.user_data.get("reschedule_old_date")
            old_time = context.user_data.get("reschedule_old_time")
            instructor_name = context.user_data.get("reschedule_instructor_name")
            duration = context.user_data.get("reschedule_duration")
            instructor_id = context.user_data.get("reschedule_instructor_id")
            
            await update.message.reply_text(
                f"✅ *Заняття успішно перенесено!*\n\n"
                f"🗓 *Старий час:*\n"
                f"📅 {old_date} о {old_time}\n\n"
                f"🆕 *Новий час:*\n"
                f"📅 {new_date} о {new_time}\n\n"
                f"👨‍🏫 Інструктор: {instructor_name}\n"
                f"⏱ Тривалість: {duration}",
                reply_markup=ReplyKeyboardMarkup([[KeyboardButton("📋 Мої записи")]], resize_keyboard=True),
                parse_mode="Markdown"
            )
            
            try:
                await context.bot.send_message(
                    chat_id=instructor_id,
                    text=f"✅ *Учень погодився на перенесення!*\n\n"
                         f"🗓 Старий час: {old_date} о {old_time}\n"
                         f"🆕 Новий час: {new_date} о {new_time}\n\n"
                         f"Тепер ви можете заблокувати старий час через\n"
                         f"⚙️ Управління графіком",
                    parse_mode="Markdown"
                )
            except Exception as e:
                logger.error(f"Failed to notify instructor: {e}")
        else:
            await update.message.reply_text("❌ Помилка перенесення. Спробуйте пізніше.")
        
        context.user_data.clear()

def main():
    try:
        init_db()
        init_lessons_table()
        init_students_table()
        migrate_database()
        init_schedule_blocks_table()
        init_reschedule_requests_table()
        
        ensure_instructors_exist()

        from telegram.ext import JobQueue
        app = (
            ApplicationBuilder()
            .token(TOKEN)
            .build()
        )

        app.add_handler(CommandHandler("start", start))
        app.add_handler(CommandHandler("register490", register_490))
        app.add_handler(CommandHandler("register550", register_550))
        
        app.add_handler(CallbackQueryHandler(handle_callback))
        app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), handle_message))
        app.add_handler(MessageHandler(filters.CONTACT, handle_message))

        if app.job_queue:
            app.job_queue.run_repeating(send_reminders, interval=1800, first=10)
            app.job_queue.run_repeating(check_completed_lessons, interval=900, first=60)
            logger.info("✅ Job queue налаштовано")
        else:
            logger.warning("⚠️ Job queue недоступна - нагадування вимкнено")

        logger.info("🚀 Бот запущено!")
        print("🚀 Бот запущено і слухає...")
        print("\n📝 Посилання для реєстрації учнів:")
        print(f"   490 грн: https://t.me/InstructorIFBot?start=register490")
        print(f"   550 грн: https://t.me/InstructorIFBot?start=register550")
        
        import threading
        from http.server import HTTPServer, BaseHTTPRequestHandler
        
        class HealthCheckHandler(BaseHTTPRequestHandler):
            def do_GET(self):
                self.send_response(200)
                self.send_header('Content-type', 'text/plain')
                self.end_headers()
                self.wfile.write(b'Bot is running!')
            
            def log_message(self, format, *args):
                pass
        
        def run_bot():
            import asyncio
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                app.run_polling(drop_pending_updates=True, stop_signals=None)
            finally:
                loop.close()
        
        bot_thread = threading.Thread(target=run_bot, daemon=True)
        bot_thread.start()
        
        port = int(os.environ.get('PORT', 10000))
        server = HTTPServer(('0.0.0.0', port), HealthCheckHandler)
        logger.info(f"🌐 HTTP сервер запущено на порту {port}")
        print(f"🌐 HTTP сервер запущено на порту {port}")
        
        server.serve_forever()
    
    except Exception as e:
        logger.error(f"Critical error: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    main()

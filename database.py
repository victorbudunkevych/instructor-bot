# database.py - ВИПРАВЛЕНА ВЕРСІЯ (ЕТАП 1)
# Додано нові функції:
# 1. check_student_has_booking_at_time() - перевірка дублювання записів
# 2. count_student_bookings_on_date() - підрахунок записів на день
# 3. can_block_time_slot() - перевірка можливості блокування часу

import sqlite3
import logging
from contextlib import contextmanager
from datetime import datetime

logger = logging.getLogger(__name__)

# ======================= ВАЛІДАЦІЯ =======================
def validate_time_format(time_str):
    """Перевірка формату часу HH:MM"""
    try:
        h, m = map(int, time_str.split(':'))
        return 0 <= h <= 23 and 0 <= m <= 59
    except:
        return False

def validate_date_format(date_str):
    """Перевірка формату дати YYYY-MM-DD"""
    try:
        datetime.strptime(date_str, '%Y-%m-%d')
        return True
    except ValueError:
        return False

# ======================= ПІДКЛЮЧЕННЯ =======================
@contextmanager
def get_db():
    """Context manager для безпечної роботи з БД"""
    conn = sqlite3.connect("instructors.db")
    try:
        yield conn
    except Exception as e:
        conn.rollback()
        logger.error(f"Database error: {e}")
        raise
    finally:
        conn.close()

# ======================= ІНІЦІАЛІЗАЦІЯ =======================
def init_db():
    """Створення таблиці інструкторів"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS instructors (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    transmission_type TEXT NOT NULL,
                    telegram_id INTEGER UNIQUE,
                    phone TEXT,
                    price_per_hour INTEGER DEFAULT 400,
                    is_active INTEGER DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.commit()
        logger.info("✅ Таблиця instructors готова")
    except Exception as e:
        logger.error(f"Помилка init_db: {e}")
        raise

def init_lessons_table():
    """Створення таблиці занять"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS lessons (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    instructor_id INTEGER NOT NULL,
                    student_name TEXT NOT NULL,
                    student_telegram_id INTEGER,
                    student_phone TEXT,
                    student_tariff INTEGER DEFAULT 0,
                    date TEXT NOT NULL,
                    time TEXT NOT NULL,
                    duration TEXT NOT NULL,
                    status TEXT DEFAULT 'active',
                    rating INTEGER,
                    feedback TEXT,
                    cancelled_by TEXT,
                    cancelled_at TIMESTAMP,
                    reminder_24h_sent INTEGER DEFAULT 0,
                    reminder_2h_sent INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    completed_at TIMESTAMP,
                    FOREIGN KEY (instructor_id) REFERENCES instructors(id)
                )
            """)
            
            # Індекси для швидкодії
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_lessons_instructor 
                ON lessons(instructor_id)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_lessons_date 
                ON lessons(date)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_lessons_student 
                ON lessons(student_telegram_id)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_lessons_status 
                ON lessons(status)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_lessons_datetime 
                ON lessons(date, time)
            """)
            
            conn.commit()
        logger.info("✅ Таблиця lessons готова")
    except Exception as e:
        logger.error(f"Помилка init_lessons_table: {e}")
        raise

def init_schedule_blocks_table():
    """Створення таблиці для блокування часу інструкторами"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS schedule_blocks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    instructor_id INTEGER NOT NULL,
                    date TEXT NOT NULL,
                    time_start TEXT NOT NULL,
                    time_end TEXT NOT NULL,
                    block_type TEXT NOT NULL,
                    reason TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (instructor_id) REFERENCES instructors(id)
                )
            """)
            
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_schedule_blocks_instructor 
                ON schedule_blocks(instructor_id)
            """)
            
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_schedule_blocks_date 
                ON schedule_blocks(date)
            """)
            
            conn.commit()
        logger.info("✅ Таблиця schedule_blocks готова")
    except Exception as e:
        logger.error(f"Помилка init_schedule_blocks_table: {e}")
        raise

def init_students_table():
    """Створення таблиці учнів"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS students (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    phone TEXT,
                    telegram_id INTEGER UNIQUE,
                    tariff INTEGER NOT NULL,
                    registered_via TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_students_telegram 
                ON students(telegram_id)
            """)
            
            conn.commit()
        logger.info("✅ Таблиця students готова")
    except Exception as e:
        logger.error(f"Помилка init_students_table: {e}")
        raise

def migrate_database():
    """Додавання нових полів до існуючої БД"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            
            # Перевіряємо які поля є в lessons
            cursor.execute("PRAGMA table_info(lessons)")
            existing_cols = {row[1] for row in cursor.fetchall()}
            
            # Додаємо відсутні поля
            new_cols = {
                'student_telegram_id': 'INTEGER',
                'student_phone': 'TEXT',
                'student_tariff': 'INTEGER DEFAULT 0',
                'status': "TEXT DEFAULT 'active'",
                'rating': 'INTEGER',
                'feedback': 'TEXT',
                'cancelled_by': 'TEXT',
                'cancelled_at': 'TIMESTAMP',
                'reminder_24h_sent': 'INTEGER DEFAULT 0',
                'reminder_2h_sent': 'INTEGER DEFAULT 0',
                'created_at': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP',
                'completed_at': 'TIMESTAMP'
            }
            
            for col, col_type in new_cols.items():
                if col not in existing_cols:
                    try:
                        cursor.execute(f"ALTER TABLE lessons ADD COLUMN {col} {col_type}")
                        logger.info(f"✅ Додано поле: {col}")
                    except sqlite3.OperationalError as e:
                        logger.debug(f"Поле {col} вже існує або помилка: {e}")
            
            # Оновлюємо старі записи
            cursor.execute("UPDATE lessons SET status = 'active' WHERE status IS NULL")
            conn.commit()
            
        logger.info("✅ Міграція завершена")
    except Exception as e:
        logger.error(f"Помилка migrate_database: {e}")

# ======================= ЗАПИТИ - ІНСТРУКТОРИ =======================
def get_instructors_by_transmission(transmission_type):
    """Отримати список інструкторів за типом трансмісії"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id, name, phone, price_per_hour
                FROM instructors
                WHERE transmission_type = ? AND is_active = 1
                ORDER BY name
            """, (transmission_type,))
            return cursor.fetchall()
    except Exception as e:
        logger.error(f"Помилка get_instructors_by_transmission: {e}")
        return []

def get_instructor_by_name(name):
    """Отримати інструктора за іменем"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id, name, phone, transmission_type, price_per_hour
                FROM instructors
                WHERE name = ?
            """, (name,))
            return cursor.fetchone()
    except Exception as e:
        logger.error(f"Помилка get_instructor_by_name: {e}")
        return None

def get_instructor_by_telegram_id(telegram_id):
    """Отримати інструктора за Telegram ID"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id, name, phone, transmission_type, price_per_hour
                FROM instructors
                WHERE telegram_id = ?
            """, (telegram_id,))
            return cursor.fetchone()
    except Exception as e:
        logger.error(f"Помилка get_instructor_by_telegram_id: {e}")
        return None

def get_all_instructors():
    """Отримати всіх інструкторів"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id, name, transmission_type, phone, price_per_hour
                FROM instructors
                WHERE is_active = 1
                ORDER BY name
            """)
            return cursor.fetchall()
    except Exception as e:
        logger.error(f"Помилка get_all_instructors: {e}")
        return []

def get_instructor_rating(instructor_id):
    """Отримати середню оцінку інструктора"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT AVG(rating), COUNT(*)
                FROM lessons
                WHERE instructor_id = ? AND rating IS NOT NULL
            """, (instructor_id,))
            result = cursor.fetchone()
            avg_rating = result[0] if result[0] else 0
            count = result[1]
            return round(avg_rating, 1), count
    except Exception as e:
        logger.error(f"Помилка get_instructor_rating: {e}")
        return 0, 0

# ======================= ЗАПИТИ - БЛОКУВАННЯ ЧАСУ =======================
def add_schedule_block(instructor_id, date, time_start, time_end, block_type='manual', reason=''):
    """Додати блокування часу в графіку"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO schedule_blocks 
                (instructor_id, date, time_start, time_end, block_type, reason)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (instructor_id, date, time_start, time_end, block_type, reason))
            conn.commit()
            return True
    except Exception as e:
        logger.error(f"Помилка add_schedule_block: {e}")
        return False

def remove_schedule_block(block_id):
    """Видалити блокування часу"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM schedule_blocks WHERE id = ?", (block_id,))
            conn.commit()
            return True
    except Exception as e:
        logger.error(f"Помилка remove_schedule_block: {e}")
        return False

def get_instructor_blocks(instructor_id):
    """Отримати всі блокування інструктора"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id, date, time_start, time_end, reason
                FROM schedule_blocks
                WHERE instructor_id = ?
                ORDER BY date DESC, time_start
            """, (instructor_id,))
            return cursor.fetchall()
    except Exception as e:
        logger.error(f"Помилка get_instructor_blocks: {e}")
        return []

def is_time_blocked(instructor_id, date, time):
    """Перевірка чи заблокований час"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT COUNT(*) FROM schedule_blocks
                WHERE instructor_id = ?
                AND date = ?
                AND time_start <= ?
                AND time_end > ?
            """, (instructor_id, date, time, time))
            count = cursor.fetchone()[0]
            return count > 0
    except Exception as e:
        logger.error(f"Помилка is_time_blocked: {e}")
        return False

def can_block_time_slot(instructor_id, date, time_start, time_end):
    """
    ✅ НОВА ФУНКЦІЯ: Перевірка чи можна заблокувати час
    Повертає False якщо на цей час вже є записи учнів
    """
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            
            # Конвертуємо дату в формат який зберігається в БД
            # Припускаємо що date в форматі YYYY-MM-DD, а в lessons - DD.MM.YYYY
            try:
                date_obj = datetime.strptime(date, '%Y-%m-%d')
                date_formatted = date_obj.strftime('%d.%m.%Y')
            except:
                # Якщо дата вже в форматі DD.MM.YYYY
                date_formatted = date
            
            # Перевіряємо чи є активні заняття в цьому проміжку
            cursor.execute("""
                SELECT COUNT(*) FROM lessons
                WHERE instructor_id = ? 
                  AND date = ?
                  AND status = 'active'
                  AND time >= ?
                  AND time < ?
            """, (instructor_id, date_formatted, time_start, time_end))
            
            count = cursor.fetchone()[0]
            
            if count > 0:
                logger.warning(f"Не можна заблокувати {date} {time_start}-{time_end}: є {count} активних записів")
                return False
            
            return True
            
    except Exception as e:
        logger.error(f"Помилка can_block_time_slot: {e}", exc_info=True)
        return False

# ======================= ЗАПИТИ - ЗАНЯТТЯ =======================
def is_time_slot_available(instructor_id, date, start_time, duration):
    """Перевірка чи вільний часовий слот"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT time, duration FROM lessons
                WHERE instructor_id = ? AND date = ? AND status = 'active'
            """, (instructor_id, date))
            booked = cursor.fetchall()
    except Exception as e:
        logger.error(f"Помилка is_time_slot_available: {e}")
        return False
    
    def time_to_minutes(time_str):
        """Конвертація часу в хвилини"""
        h, m = map(int, time_str.split(':'))
        return h * 60 + m
    
    def duration_to_minutes(dur_str):
        """Конвертація тривалості в хвилини"""
        if "1.5" in dur_str:
            return 90
        elif "2" in dur_str:
            return 120
        return 60
    
    new_start = time_to_minutes(start_time)
    new_end = new_start + duration_to_minutes(duration)
    
    for booked_time, booked_duration in booked:
        if ':' not in booked_time:
            continue
        booked_start = time_to_minutes(booked_time)
        booked_end = booked_start + duration_to_minutes(booked_duration)
        
        # Якщо є перетин
        if not (new_end <= booked_start or new_start >= booked_end):
            return False
    
    return True

def check_student_has_booking_at_time(student_telegram_id, date, time):
    """
    ✅ НОВА ФУНКЦІЯ: Перевірка чи учень вже має запис на цей час
    Повертає True якщо учень вже записаний на цей час у будь-якого інструктора
    """
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT COUNT(*) FROM lessons
                WHERE student_telegram_id = ?
                  AND date = ?
                  AND time = ?
                  AND status = 'active'
            """, (student_telegram_id, date, time))
            
            count = cursor.fetchone()[0]
            
            if count > 0:
                logger.info(f"Учень {student_telegram_id} вже має запис на {date} {time}")
                return True
            
            return False
            
    except Exception as e:
        logger.error(f"Помилка check_student_has_booking_at_time: {e}")
        return False

def count_student_bookings_on_date(student_telegram_id, date):
    """
    ✅ НОВА ФУНКЦІЯ: Підрахунок кількості записів учня на конкретну дату
    Повертає кількість активних записів
    """
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT COUNT(*) FROM lessons
                WHERE student_telegram_id = ?
                  AND date = ?
                  AND status = 'active'
            """, (student_telegram_id, date))
            
            count = cursor.fetchone()[0]
            logger.info(f"Учень {student_telegram_id} має {count} записів на {date}")
            return count
            
    except Exception as e:
        logger.error(f"Помилка count_student_bookings_on_date: {e}")
        return 0

def update_lesson(lesson_id, **kwargs):
    """Оновити дані заняття"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            
            # Формуємо SQL запит динамічно
            set_clause = ", ".join([f"{key} = ?" for key in kwargs.keys()])
            values = list(kwargs.values()) + [lesson_id]
            
            cursor.execute(f"""
                UPDATE lessons
                SET {set_clause}
                WHERE id = ?
            """, values)
            
            conn.commit()
            return True
    except Exception as e:
        logger.error(f"Помилка update_lesson: {e}")
        return False

def add_lesson_rating(lesson_id, rating, feedback=""):
    """Додати оцінку після завершення уроку"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE lessons
                SET rating = ?, feedback = ?, completed_at = CURRENT_TIMESTAMP
                WHERE id = ?
            """, (rating, feedback, lesson_id))
            conn.commit()
            return True
    except Exception as e:
        logger.error(f"Помилка add_lesson_rating: {e}")
        return False

# ======================= ЗАПИТИ - СТАТИСТИКА =======================
def get_instructor_stats_period(instructor_id, date_from, date_to):
    """Статистика інструктора за період"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            
            # Загальна кількість занять
            cursor.execute("""
                SELECT COUNT(*), 
                       SUM(CASE WHEN duration LIKE '%1.5%' THEN 1.5
                                WHEN duration LIKE '%2%' THEN 2
                                ELSE 1 END) as total_hours
                FROM lessons
                WHERE instructor_id = ? 
                  AND date BETWEEN ? AND ?
                  AND status IN ('active', 'completed')
            """, (instructor_id, date_from, date_to))
            
            total_lessons, total_hours = cursor.fetchone()
            total_hours = total_hours or 0
            
            # Середній рейтинг
            cursor.execute("""
                SELECT AVG(rating)
                FROM lessons
                WHERE instructor_id = ? 
                  AND date BETWEEN ? AND ?
                  AND rating IS NOT NULL
            """, (instructor_id, date_from, date_to))
            
            avg_rating = cursor.fetchone()[0] or 0
            
            # Скасовані заняття
            cursor.execute("""
                SELECT COUNT(*)
                FROM lessons
                WHERE instructor_id = ? 
                  AND date BETWEEN ? AND ?
                  AND status = 'cancelled'
            """, (instructor_id, date_from, date_to))
            
            cancelled = cursor.fetchone()[0]
            
            # Заробіток (400 грн/год)
            earnings = total_hours * 400
            
            return {
                'total_lessons': total_lessons or 0,
                'total_hours': round(total_hours, 1),
                'earnings': earnings,
                'avg_rating': round(avg_rating, 1),
                'cancelled': cancelled
            }
    except Exception as e:
        logger.error(f"Помилка get_instructor_stats_period: {e}")
        return None

def get_admin_report_by_instructors(date_from, date_to):
    """Звіт для адміна по всіх інструкторах за період"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    i.name,
                    COUNT(l.id) as total_lessons,
                    SUM(CASE WHEN l.duration LIKE '%1.5%' THEN 1.5
                             WHEN l.duration LIKE '%2%' THEN 2
                             ELSE 1 END) as total_hours,
                    AVG(l.rating) as avg_rating,
                    SUM(CASE WHEN l.status = 'cancelled' THEN 1 ELSE 0 END) as cancelled
                FROM instructors i
                LEFT JOIN lessons l ON i.id = l.instructor_id 
                    AND l.date BETWEEN ? AND ?
                    AND l.status IN ('active', 'completed', 'cancelled')
                GROUP BY i.id, i.name
                ORDER BY total_hours DESC
            """, (date_from, date_to))
            
            return cursor.fetchall()
    except Exception as e:
        logger.error(f"Помилка get_admin_report_by_instructors: {e}")
        return []

# ======================= ЗАПИТИ - УЧНІ =======================
def register_student(name, phone, telegram_id, tariff, registered_via="direct"):
    """Реєстрація учня"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            
            # Перевіряємо чи вже існує
            cursor.execute("""
                SELECT id FROM students WHERE telegram_id = ?
            """, (telegram_id,))
            
            if cursor.fetchone():
                # Оновлюємо існуючого
                cursor.execute("""
                    UPDATE students
                    SET name = ?, phone = ?, tariff = ?
                    WHERE telegram_id = ?
                """, (name, phone, tariff, telegram_id))
            else:
                # Додаємо нового
                cursor.execute("""
                    INSERT INTO students (name, phone, telegram_id, tariff, registered_via)
                    VALUES (?, ?, ?, ?, ?)
                """, (name, phone, telegram_id, tariff, registered_via))
            
            conn.commit()
            return True
    except Exception as e:
        logger.error(f"Помилка register_student: {e}")
        return False

def get_student_by_telegram_id(telegram_id):
    """Отримати дані учня"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id, name, phone, tariff
                FROM students
                WHERE telegram_id = ?
            """, (telegram_id,))
            return cursor.fetchone()
    except Exception as e:
        logger.error(f"Помилка get_student_by_telegram_id: {e}")
        return None

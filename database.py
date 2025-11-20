# database.py - ОНОВЛЕНА ВЕРСІЯ З НОВИМИ ФУНКЦІЯМИ
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
    """НОВА: Створення таблиці учнів"""
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

def init_reschedule_requests_table():
    """НОВА: Створення таблиці запитів на перенесення"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS reschedule_requests (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    lesson_id INTEGER NOT NULL,
                    instructor_id INTEGER NOT NULL,
                    instructor_name TEXT NOT NULL,
                    student_telegram_id INTEGER NOT NULL,
                    student_name TEXT NOT NULL,
                    old_date TEXT NOT NULL,
                    old_time TEXT NOT NULL,
                    duration TEXT NOT NULL,
                    new_date TEXT,
                    new_time TEXT,
                    status TEXT DEFAULT 'pending',
                    reason TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    responded_at TIMESTAMP,
                    FOREIGN KEY (lesson_id) REFERENCES lessons(id),
                    FOREIGN KEY (instructor_id) REFERENCES instructors(id)
                )
            """)
            
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_reschedule_lesson 
                ON reschedule_requests(lesson_id)
            """)
            
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_reschedule_status 
                ON reschedule_requests(status)
            """)
            
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_reschedule_student 
                ON reschedule_requests(student_telegram_id)
            """)
            
            conn.commit()
        logger.info("✅ Таблиця reschedule_requests готова")
    except Exception as e:
        logger.error(f"Помилка init_reschedule_requests_table: {e}")
        raise

def migrate_database():
    """Додавання нових полів до існуючої БД"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            
            # === МІГРАЦІЯ INSTRUCTORS ===
            cursor.execute("PRAGMA table_info(instructors)")
            existing_instructors_cols = {row[1] for row in cursor.fetchall()}
            
            instructors_new_cols = {
                'price_per_hour': 'INTEGER DEFAULT 400',
                'is_active': 'INTEGER DEFAULT 1'
            }
            
            for col, col_type in instructors_new_cols.items():
                if col not in existing_instructors_cols:
                    try:
                        cursor.execute(f"ALTER TABLE instructors ADD COLUMN {col} {col_type}")
                        logger.info(f"✅ Додано поле instructors.{col}")
                    except sqlite3.OperationalError as e:
                        logger.debug(f"Поле {col} вже існує або помилка: {e}")
            
            # === МІГРАЦІЯ LESSONS ===
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
                        logger.info(f"✅ Додано поле lessons.{col}")
                    except sqlite3.OperationalError as e:
                        logger.debug(f"Поле {col} вже існує або помилка: {e}")
            
            # Оновлюємо старі записи
            cursor.execute("UPDATE lessons SET status = 'active' WHERE status IS NULL")
            conn.commit()
            
        logger.info("✅ Міграція БД завершена")
    except Exception as e:
        logger.error(f"Помилка migrate_database: {e}")

# ======================= ЗАПИТИ - ІНСТРУКТОРИ =======================
def get_instructors_by_transmission(transmission_type):
    """Отримати інструкторів за типом коробки"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT DISTINCT name FROM instructors WHERE transmission_type = ? ORDER BY name",
                (transmission_type,)
            )
            return [row[0] for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"Помилка get_instructors_by_transmission: {e}")
        return []

def get_instructor_by_name(name):
    """Отримати ID та telegram_id інструктора"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT id, telegram_id FROM instructors WHERE name = ?",
                (name,)
            )
            return cursor.fetchone()
    except Exception as e:
        logger.error(f"Помилка get_instructor_by_name: {e}")
        return None

def get_instructor_by_telegram_id(telegram_id):
    """Отримати дані інструктора за telegram_id"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT id, name FROM instructors WHERE telegram_id = ?",
                (telegram_id,)
            )
            return cursor.fetchone()
    except Exception as e:
        logger.error(f"Помилка get_instructor_by_telegram_id: {e}")
        return None

def get_instructor_rating(instructor_name):
    """Отримати середній рейтинг інструктора"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT AVG(rating)
                FROM lessons l
                JOIN instructors i ON l.instructor_id = i.id
                WHERE i.name = ? AND l.rating IS NOT NULL
            """, (instructor_name,))
            result = cursor.fetchone()
            return round(result[0], 1) if result and result[0] else 0
    except Exception as e:
        logger.error(f"Помилка get_instructor_rating: {e}")
        return 0

def get_all_instructors():
    """НОВА: Отримати всіх інструкторів для звітності адміна"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id, name, transmission_type, telegram_id
                FROM instructors
                ORDER BY name
            """)
            return cursor.fetchall()
    except Exception as e:
        logger.error(f"Помилка get_all_instructors: {e}")
        return []

# ======================= ЗАПИТИ - БЛОКУВАННЯ РОЗКЛАДУ =======================
def add_schedule_block(instructor_id, date, time_start, time_end, block_type, reason=""):
    """Додати блокування часу"""
    if not validate_date_format(date):
        logger.error(f"Невірний формат дати: {date}")
        return False
    
    if not validate_time_format(time_start) or not validate_time_format(time_end):
        logger.error(f"Невірний формат часу: {time_start} - {time_end}")
        return False
    
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
    """Видалити блокування"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM schedule_blocks WHERE id = ?", (block_id,))
            conn.commit()
            return True
    except Exception as e:
        logger.error(f"Помилка remove_schedule_block: {e}")
        return False

def get_schedule_blocks(instructor_id, date):
    """Отримати всі блокування для дати"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id, time_start, time_end, block_type, reason
                FROM schedule_blocks
                WHERE instructor_id = ? AND date = ?
                ORDER BY time_start
            """, (instructor_id, date))
            return cursor.fetchall()
    except Exception as e:
        logger.error(f"Помилка get_schedule_blocks: {e}")
        return []

def is_time_blocked(instructor_id, date, time_slot):
    """Перевірити чи заблокований час"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id FROM schedule_blocks
                WHERE instructor_id = ? AND date = ? 
                AND time_start <= ? AND time_end > ?
            """, (instructor_id, date, time_slot, time_slot))
            return cursor.fetchone() is not None
    except Exception as e:
        logger.error(f"Помилка is_time_blocked: {e}")
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

def update_lesson(lesson_id, **kwargs):
    """НОВА: Оновити дані заняття (для коригування графіку)"""
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
    """НОВА: Додати оцінку після завершення уроку"""
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
    """НОВА: Статистика інструктора за період"""
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
    """НОВА: Звіт для адміна по всіх інструкторах за період"""
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
    """НОВА: Реєстрація учня"""
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
    """НОВА: Отримати дані учня"""
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

# ======================= ЗАПИТИ - ПЕРЕНЕСЕННЯ =======================
def create_reschedule_request(lesson_id, instructor_id, instructor_name, student_telegram_id, student_name, old_date, old_time, duration, reason=""):
    """Створити запит на перенесення заняття"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO reschedule_requests 
                (lesson_id, instructor_id, instructor_name, student_telegram_id, student_name, old_date, old_time, duration, reason, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending')
            """, (lesson_id, instructor_id, instructor_name, student_telegram_id, student_name, old_date, old_time, duration, reason))
            conn.commit()
            return cursor.lastrowid
    except Exception as e:
        logger.error(f"Помилка create_reschedule_request: {e}")
        return None

def get_pending_reschedule_by_student(student_telegram_id):
    """Отримати активний запит на перенесення для учня"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id, lesson_id, instructor_name, old_date, old_time, duration, created_at, instructor_id
                FROM reschedule_requests
                WHERE student_telegram_id = ? AND status = 'pending'
                ORDER BY created_at DESC
                LIMIT 1
            """, (student_telegram_id,))
            return cursor.fetchone()
    except Exception as e:
        logger.error(f"Помилка get_pending_reschedule_by_student: {e}")
        return None

def accept_reschedule_request(request_id, new_date, new_time):
    """Прийняти запит на перенесення"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            
            # Оновлюємо запит
            cursor.execute("""
                UPDATE reschedule_requests
                SET status = 'accepted', new_date = ?, new_time = ?, responded_at = CURRENT_TIMESTAMP
                WHERE id = ?
            """, (new_date, new_time, request_id))
            
            # Отримуємо lesson_id
            cursor.execute("SELECT lesson_id FROM reschedule_requests WHERE id = ?", (request_id,))
            result = cursor.fetchone()
            if not result:
                return False
            
            lesson_id = result[0]
            
            # Оновлюємо урок
            cursor.execute("""
                UPDATE lessons
                SET date = ?, time = ?
                WHERE id = ?
            """, (new_date, new_time, lesson_id))
            
            conn.commit()
            return True
    except Exception as e:
        logger.error(f"Помилка accept_reschedule_request: {e}")
        return False

def reject_reschedule_request(request_id):
    """Відхилити запит на перенесення"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE reschedule_requests
                SET status = 'rejected', responded_at = CURRENT_TIMESTAMP
                WHERE id = ?
            """, (request_id,))
            conn.commit()
            return True
    except Exception as e:
        logger.error(f"Помилка reject_reschedule_request: {e}")
        return False

def get_lesson_by_instructor_datetime(instructor_id, date, time):
    """Отримати урок за інструктором, датою і часом
    date може бути в форматі YYYY-MM-DD або DD.MM.YYYY
    """
    try:
        # Конвертуємо дату в формат DD.MM.YYYY якщо потрібно
        if '-' in date:  # Формат YYYY-MM-DD
            from datetime import datetime
            date_obj = datetime.strptime(date, "%Y-%m-%d")
            date_formatted = date_obj.strftime("%d.%m.%Y")
        else:  # Вже в форматі DD.MM.YYYY
            date_formatted = date
        
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id, student_name, student_telegram_id, duration, student_tariff
                FROM lessons
                WHERE instructor_id = ? AND date = ? AND time = ? AND status = 'active'
            """, (instructor_id, date_formatted, time))
            return cursor.fetchone()
    except Exception as e:
        logger.error(f"Помилка get_lesson_by_instructor_datetime: {e}")
        return None

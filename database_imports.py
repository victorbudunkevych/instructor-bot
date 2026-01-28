# database_imports.py
# Helper functions to import instructors, students and lessons into the existing SQLite DB
# These functions are written to be used together with the project's existing database.py get_db/contextmanager

import logging
from typing import List, Dict, Any

logger = logging.getLogger(__name__)

from database import get_db


def import_instructors(instructors: List[Dict[str, Any]], clear: bool = False) -> bool:
    """
    Import instructors.

    instructors: list of dicts with possible keys:
      id, name, transmission_type, telegram_id, phone, price_per_hour, is_active, created_at
    If clear=True, clears lessons, students and instructors before importing.
    """
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            if clear:
                cursor.execute("DELETE FROM lessons")
                cursor.execute("DELETE FROM students")
                cursor.execute("DELETE FROM instructors")
                conn.commit()

            for ins in instructors:
                cursor.execute(
                    """INSERT OR REPLACE INTO instructors
                    (id, name, transmission_type, telegram_id, phone, price_per_hour, is_active, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        ins.get('id'),
                        ins.get('name'),
                        ins.get('transmission_type'),
                        ins.get('telegram_id'),
                        ins.get('phone'),
                        ins.get('price_per_hour'),
                        ins.get('is_active', 1),
                        ins.get('created_at')
                    )
                )
            conn.commit()
        return True
    except Exception as e:
        logger.error(f"Error in import_instructors: {e}", exc_info=True)
        return False


def import_students(students: List[Dict[str, Any]]) -> bool:
    """Import students. students list of dicts with keys id, name, phone, telegram_id, tariff, registered_via, created_at"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            for s in students:
                cursor.execute(
                    """INSERT OR REPLACE INTO students
                    (id, name, phone, telegram_id, tariff, registered_via, price_per_hour, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        s.get('id'),
                        s.get('name'),
                        s.get('phone'),
                        s.get('telegram_id'),
                        s.get('tariff') or s.get('student_tariff', 0),
                        s.get('registered_via'),
                        s.get('price_per_hour'),
                        s.get('created_at')
                    )
                )
            conn.commit()
        return True
    except Exception as e:
        logger.error(f"Error in import_students: {e}", exc_info=True)
        return False


def import_lessons(lessons: List[Dict[str, Any]]) -> bool:
    """
    Import lessons. Will attempt to resolve instructor_id by name if missing and student's telegram id by looking up students.

    Expected lesson dict keys (best-effort):
      id, instructor_id, instructor_name, student_name, student_telegram_id, student_phone,
      student_tariff, date, time, duration, status, rating, feedback, cancelled_by,
      cancelled_at, reminder_24h_sent, reminder_2h_sent, created_at, completed_at
    """
    try:
        with get_db() as conn:
            cursor = conn.cursor()

            for l in lessons:
                instr_id = l.get('instructor_id')
                if not instr_id and l.get('instructor_name'):
                    cursor.execute("SELECT id FROM instructors WHERE name = ?", (l.get('instructor_name'),))
                    r = cursor.fetchone()
                    if r:
                        instr_id = r[0]

                student_tid = l.get('student_telegram_id')
                if not student_tid:
                    # try by exact match on students table by name or phone
                    if l.get('student_name'):
                        cursor.execute("SELECT telegram_id FROM students WHERE name = ? LIMIT 1", (l.get('student_name'),))
                        r = cursor.fetchone()
                        if r and r[0]:
                            student_tid = r[0]
                    if not student_tid and l.get('student_phone'):
                        cursor.execute("SELECT telegram_id FROM students WHERE phone = ? LIMIT 1", (l.get('student_phone'),))
                        r = cursor.fetchone()
                        if r and r[0]:
                            student_tid = r[0]

                cursor.execute(
                    """INSERT OR REPLACE INTO lessons
                    (id, instructor_id, student_name, student_telegram_id, student_phone,
                     student_tariff, date, time, duration, status, rating, feedback,
                     cancelled_by, cancelled_at, reminder_24h_sent, reminder_2h_sent, created_at, completed_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        l.get('id'),
                        instr_id,
                        l.get('student_name'),
                        student_tid,
                        l.get('student_phone'),
                        l.get('student_tariff'),
                        l.get('date'),
                        l.get('time'),
                        l.get('duration'),
                        l.get('status'),
                        l.get('rating'),
                        l.get('feedback'),
                        l.get('cancelled_by'),
                        l.get('cancelled_at'),
                        l.get('reminder_24h_sent', 0),
                        l.get('reminder_2h_sent', 0),
                        l.get('created_at'),
                        l.get('completed_at')
                    )
                )
            conn.commit()
        return True
    except Exception as e:
        logger.error(f"Error in import_lessons: {e}", exc_info=True)
        return False
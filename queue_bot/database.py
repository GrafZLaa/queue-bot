import contextlib
import os
import random
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Optional

import aiosqlite

DB = os.getenv("DB_PATH", "queue.db")
DEFAULT_DURATION_MINUTES = 90

SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    telegram_id    INTEGER UNIQUE NOT NULL,
    username       TEXT,
    full_name      TEXT NOT NULL,
    group_name     TEXT,
    name_confirmed INTEGER DEFAULT 0,
    rating         INTEGER DEFAULT 50,
    category       TEXT DEFAULT 'middle',
    on_time        INTEGER DEFAULT 0,
    late           INTEGER DEFAULT 0,
    no_show        INTEGER DEFAULT 0
);
CREATE TABLE IF NOT EXISTS subjects (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    name       TEXT NOT NULL,
    group_name TEXT
);
CREATE TABLE IF NOT EXISTS classes (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    subject_id       INTEGER NOT NULL,
    dt               TEXT NOT NULL,
    duration_minutes INTEGER DEFAULT 90,
    room             TEXT,
    teacher          TEXT,
    class_type       TEXT DEFAULT 'ПР',
    opens_at         TEXT,
    closes_at        TEXT,
    FOREIGN KEY (subject_id) REFERENCES subjects(id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS assignments (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    class_id    INTEGER,
    subject_id  INTEGER NOT NULL,
    title       TEXT NOT NULL,
    description TEXT,
    deadline    TEXT,
    url         TEXT,
    FOREIGN KEY (class_id) REFERENCES classes(id) ON DELETE CASCADE,
    FOREIGN KEY (subject_id) REFERENCES subjects(id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS queues (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    class_id INTEGER UNIQUE NOT NULL,
    status   TEXT DEFAULT 'pending',
    FOREIGN KEY (class_id) REFERENCES classes(id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS entries (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    queue_id    INTEGER NOT NULL,
    user_id     INTEGER NOT NULL,
    position    INTEGER,
    q_category  TEXT,
    submitted   INTEGER DEFAULT 0,
    on_time     INTEGER DEFAULT 0,
    UNIQUE(queue_id, user_id),
    FOREIGN KEY (queue_id) REFERENCES queues(id) ON DELETE CASCADE,
    FOREIGN KEY (user_id)  REFERENCES users(id) ON DELETE CASCADE
);
"""


@contextlib.asynccontextmanager
async def connect_db():
    async with aiosqlite.connect(DB) as conn:
        conn.row_factory = aiosqlite.Row
        await conn.execute("PRAGMA foreign_keys = ON")
        yield conn


def _clean_group(group_name: Optional[str]) -> Optional[str]:
    if group_name is None:
        return None
    value = group_name.strip()
    return value or None


def category(rating: int) -> str:
    if rating >= 65:
        return "good"
    if rating <= 35:
        return "poor"
    return "middle"


def class_end_time(dt: str, duration_minutes: Optional[int] = None) -> datetime:
    start = datetime.fromisoformat(dt)
    minutes = duration_minutes or DEFAULT_DURATION_MINUTES
    return start + timedelta(minutes=minutes)


async def _columns(table: str) -> set[str]:
    async with connect_db() as db:
        cur = await db.execute(f"PRAGMA table_info({table})")
        return {row["name"] for row in await cur.fetchall()}


async def _migrate() -> None:
    user_columns = await _columns("users")
    class_columns = await _columns("classes")
    async with connect_db() as db:
        if "group_name" not in user_columns:
            await db.execute("ALTER TABLE users ADD COLUMN group_name TEXT")
        if "name_confirmed" not in user_columns:
            await db.execute("ALTER TABLE users ADD COLUMN name_confirmed INTEGER DEFAULT 0")
        if "duration_minutes" not in class_columns:
            await db.execute(
                "ALTER TABLE classes ADD COLUMN duration_minutes INTEGER DEFAULT 90"
            )
        if "class_type" not in class_columns:
            await db.execute("ALTER TABLE classes ADD COLUMN class_type TEXT DEFAULT 'ПР'")
        if "opens_at" not in class_columns:
            await db.execute("ALTER TABLE classes ADD COLUMN opens_at TEXT")
        if "closes_at" not in class_columns:
            await db.execute("ALTER TABLE classes ADD COLUMN closes_at TEXT")
        await db.execute(
            "UPDATE classes SET duration_minutes=? WHERE duration_minutes IS NULL",
            (DEFAULT_DURATION_MINUTES,),
        )
        await db.commit()


async def init() -> None:
    async with connect_db() as db:
        await db.executescript(SCHEMA)
        await db.commit()
    await _migrate()


async def ensure_user(
    tg_id: int,
    username: Optional[str],
    full_name: str,
    group_name: Optional[str] = None,
) -> dict:
    group_name = _clean_group(group_name)
    async with connect_db() as db:
        await db.execute(
            """
            INSERT OR IGNORE INTO users (telegram_id, username, full_name, group_name)
            VALUES (?, ?, ?, ?)
            """,
            (tg_id, username, full_name, group_name),
        )
        if group_name is None:
            await db.execute(
                "UPDATE users SET username=?, full_name=? WHERE telegram_id=?",
                (username, full_name, tg_id),
            )
        else:
            await db.execute(
                """
                UPDATE users
                SET username=?, full_name=?, group_name=?
                WHERE telegram_id=?
                """,
                (username, full_name, group_name, tg_id),
            )
        await db.commit()
        cur = await db.execute("SELECT * FROM users WHERE telegram_id=?", (tg_id,))
        return dict(await cur.fetchone())


async def get_user_by_tg(tg_id: int) -> Optional[dict]:
    async with connect_db() as db:
        cur = await db.execute("SELECT * FROM users WHERE telegram_id=?", (tg_id,))
        row = await cur.fetchone()
        return dict(row) if row else None


async def get_user(user_id: int) -> Optional[dict]:
    async with connect_db() as db:
        cur = await db.execute("SELECT * FROM users WHERE id=?", (user_id,))
        row = await cur.fetchone()
        return dict(row) if row else None


async def all_users(group_name: Optional[str] = None) -> list[dict]:
    async with connect_db() as db:
        if group_name:
            cur = await db.execute(
                "SELECT * FROM users WHERE group_name=? ORDER BY rating DESC, full_name",
                (_clean_group(group_name),),
            )
        else:
            cur = await db.execute("SELECT * FROM users ORDER BY rating DESC, full_name")
        return [dict(r) for r in await cur.fetchall()]


async def set_rating(user_id: int, rating: int) -> None:
    r = max(0, min(100, int(rating)))
    async with connect_db() as db:
        await db.execute(
            "UPDATE users SET rating=?, category=? WHERE id=?",
            (r, category(r), user_id),
        )
        await db.commit()


async def set_full_name(user_id: int, full_name: str) -> None:
    async with connect_db() as db:
        await db.execute("UPDATE users SET full_name=? WHERE id=?", (full_name, user_id))
        await db.commit()


async def set_group(user_id: int, group_name: Optional[str]) -> None:
    async with connect_db() as db:
        await db.execute(
            "UPDATE users SET group_name=? WHERE id=?",
            (_clean_group(group_name), user_id),
        )
        await db.commit()


async def set_name_confirmed(tg_id: int) -> None:
    async with connect_db() as db:
        await db.execute(
            "UPDATE users SET name_confirmed=1 WHERE telegram_id=?", (tg_id,)
        )
        await db.commit()


async def apply_rating(user_id: int, kind: str) -> None:
    delta = {"on_time": 10, "late": 2, "no_show": -10}[kind]
    u = await get_user(user_id)
    if not u:
        return
    new_r = max(0, min(100, u["rating"] + delta))
    ot = u["on_time"] + (1 if kind == "on_time" else 0)
    la = u["late"] + (1 if kind == "late" else 0)
    ns = u["no_show"] + (1 if kind == "no_show" else 0)
    async with connect_db() as db:
        await db.execute(
            """
            UPDATE users
            SET rating=?, category=?, on_time=?, late=?, no_show=?
            WHERE id=?
            """,
            (new_r, category(new_r), ot, la, ns, user_id),
        )
        await db.commit()


async def all_subjects(group_name: Optional[str] = None) -> list[dict]:
    async with connect_db() as db:
        group_name = _clean_group(group_name)
        if group_name:
            cur = await db.execute(
                """
                SELECT * FROM subjects
                WHERE group_name IS NULL OR group_name='' OR group_name=?
                ORDER BY name
                """,
                (group_name,),
            )
        else:
            cur = await db.execute("SELECT * FROM subjects ORDER BY name")
        return [dict(r) for r in await cur.fetchall()]


async def get_subject(sid: int) -> Optional[dict]:
    async with connect_db() as db:
        cur = await db.execute("SELECT * FROM subjects WHERE id=?", (sid,))
        row = await cur.fetchone()
        return dict(row) if row else None


async def add_subject(name: str, group_name: Optional[str]) -> int:
    async with connect_db() as db:
        cur = await db.execute(
            "INSERT INTO subjects (name, group_name) VALUES (?, ?)",
            (name.strip(), _clean_group(group_name)),
        )
        await db.commit()
        return cur.lastrowid


async def delete_subject(sid: int) -> None:
    async with connect_db() as db:
        await db.execute("DELETE FROM subjects WHERE id=?", (sid,))
        await db.commit()


async def classes_for_subject(sid: int) -> list[dict]:
    async with connect_db() as db:
        cur = await db.execute(
            "SELECT * FROM classes WHERE subject_id=? ORDER BY dt", (sid,)
        )
        return [dict(r) for r in await cur.fetchall()]


async def classes_for_date(date_iso: str, group_name: Optional[str] = None) -> list[dict]:
    date_prefix = f"{date_iso}%"
    group_name = _clean_group(group_name)
    async with connect_db() as db:
        if group_name:
            cur = await db.execute(
                """
                SELECT c.*, s.name AS subject_name, s.group_name
                FROM classes c
                JOIN subjects s ON c.subject_id=s.id
                WHERE c.dt LIKE ?
                  AND (s.group_name IS NULL OR s.group_name='' OR s.group_name=?)
                ORDER BY c.dt
                """,
                (date_prefix, group_name),
            )
        else:
            cur = await db.execute(
                """
                SELECT c.*, s.name AS subject_name, s.group_name
                FROM classes c
                JOIN subjects s ON c.subject_id=s.id
                WHERE c.dt LIKE ?
                ORDER BY c.dt
                """,
                (date_prefix,),
            )
        return [dict(r) for r in await cur.fetchall()]


async def get_class(cid: int) -> Optional[dict]:
    async with connect_db() as db:
        cur = await db.execute(
            """
            SELECT c.*, s.name AS subject_name, s.group_name
            FROM classes c
            JOIN subjects s ON c.subject_id=s.id
            WHERE c.id=?
            """,
            (cid,),
        )
        row = await cur.fetchone()
        return dict(row) if row else None


async def add_class(
    subject_id: int,
    dt: str,
    room: Optional[str],
    teacher: Optional[str],
    duration_minutes: int = DEFAULT_DURATION_MINUTES,
    class_type: str = "ПР",
    opens_at: Optional[str] = None,
    closes_at: Optional[str] = None,
    create_queue: bool = True,
) -> int:
    duration_minutes = max(15, min(360, int(duration_minutes or DEFAULT_DURATION_MINUTES)))
    async with connect_db() as db:
        cur = await db.execute(
            """
            INSERT INTO classes
                (subject_id, dt, duration_minutes, room, teacher, class_type, opens_at, closes_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (subject_id, dt, duration_minutes, room, teacher, class_type, opens_at, closes_at),
        )
        cid = cur.lastrowid
        if create_queue:
            await db.execute("INSERT INTO queues (class_id) VALUES (?)", (cid,))
        await db.commit()
        return cid


async def delete_class(cid: int) -> None:
    async with connect_db() as db:
        await db.execute("DELETE FROM classes WHERE id=?", (cid,))
        await db.commit()


async def assignments_for_class(cid: int) -> list[dict]:
    cls = await get_class(cid)
    if not cls:
        return []
    async with connect_db() as db:
        cur = await db.execute(
            """
            SELECT * FROM assignments
            WHERE class_id=? OR (subject_id=? AND class_id IS NULL)
            ORDER BY COALESCE(deadline, '9999-12-31T23:59:59'), title
            """,
            (cid, cls["subject_id"]),
        )
        return [dict(r) for r in await cur.fetchall()]


async def add_assignment(
    class_id: Optional[int],
    subject_id: int,
    title: str,
    description: Optional[str],
    deadline: Optional[str],
    url: Optional[str],
) -> int:
    async with connect_db() as db:
        cur = await db.execute(
            """
            INSERT INTO assignments
                (class_id, subject_id, title, description, deadline, url)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (class_id, subject_id, title, description, deadline, url),
        )
        await db.commit()
        return cur.lastrowid


async def delete_assignment(aid: int) -> None:
    async with connect_db() as db:
        await db.execute("DELETE FROM assignments WHERE id=?", (aid,))
        await db.commit()


async def queue_for_class(cid: int) -> Optional[dict]:
    async with connect_db() as db:
        cur = await db.execute("SELECT * FROM queues WHERE class_id=?", (cid,))
        row = await cur.fetchone()
        return dict(row) if row else None


async def get_queue(qid: int) -> Optional[dict]:
    async with connect_db() as db:
        cur = await db.execute(
            """
            SELECT q.*, c.dt, c.duration_minutes, c.room, c.subject_id,
                   c.class_type, s.name AS subject_name, s.group_name
            FROM queues q
            JOIN classes c ON q.class_id=c.id
            JOIN subjects s ON c.subject_id=s.id
            WHERE q.id=?
            """,
            (qid,),
        )
        row = await cur.fetchone()
        return dict(row) if row else None


async def set_queue_status(qid: int, status: str) -> None:
    if status not in {"pending", "open", "closed", "completed"}:
        raise ValueError(f"Unknown queue status: {status}")
    async with connect_db() as db:
        await db.execute("UPDATE queues SET status=? WHERE id=?", (status, qid))
        await db.commit()


async def queue_entries(qid: int) -> list[dict]:
    async with connect_db() as db:
        cur = await db.execute(
            """
            SELECT e.*, u.full_name, u.username, u.telegram_id, u.rating,
                   u.category AS user_cat, u.group_name
            FROM entries e
            JOIN users u ON e.user_id=u.id
            WHERE e.queue_id=?
            ORDER BY COALESCE(e.position, 9999), e.id
            """,
            (qid,),
        )
        return [dict(r) for r in await cur.fetchall()]


async def is_in_queue(qid: int, user_id: int) -> bool:
    async with connect_db() as db:
        cur = await db.execute(
            "SELECT id FROM entries WHERE queue_id=? AND user_id=?", (qid, user_id)
        )
        return bool(await cur.fetchone())


async def join_queue(qid: int, user_id: int) -> None:
    async with connect_db() as db:
        await db.execute(
            "INSERT OR IGNORE INTO entries (queue_id, user_id) VALUES (?, ?)",
            (qid, user_id),
        )
        await db.commit()


async def leave_queue(qid: int, user_id: int) -> None:
    async with connect_db() as db:
        await db.execute(
            "DELETE FROM entries WHERE queue_id=? AND user_id=?", (qid, user_id)
        )
        await db.commit()


async def randomize_queue(qid: int) -> None:
    async with connect_db() as db:
        cur = await db.execute(
            """
            SELECT e.id, u.category
            FROM entries e
            JOIN users u ON e.user_id=u.id
            WHERE e.queue_id=?
            """,
            (qid,),
        )
        rows = await cur.fetchall()

    groups = {"good": [], "middle": [], "poor": []}
    for row in rows:
        groups[row["category"]].append(row["id"])
    for values in groups.values():
        random.shuffle(values)

    ordered = groups["good"] + groups["middle"] + groups["poor"]
    if not ordered:
        await set_queue_status(qid, "closed")
        return

    first_cut = max(1, (len(ordered) + 2) // 3)
    second_cut = max(first_cut, (2 * len(ordered) + 2) // 3)

    async with connect_db() as db:
        for pos, entry_id in enumerate(ordered, 1):
            qcat = "good" if pos <= first_cut else ("middle" if pos <= second_cut else "poor")
            await db.execute(
                "UPDATE entries SET position=?, q_category=? WHERE id=?",
                (pos, qcat, entry_id),
            )
        await db.execute("UPDATE queues SET status='closed' WHERE id=?", (qid,))
        await db.commit()


async def mark_submission(qid: int, user_id: int, kind: str) -> None:
    if kind not in {"on_time", "late", "no_show"}:
        raise ValueError(f"Unknown submission kind: {kind}")
    submitted = 1 if kind != "no_show" else 0
    on_time = 1 if kind == "on_time" else 0
    async with connect_db() as db:
        await db.execute(
            """
            UPDATE entries
            SET submitted=?, on_time=?
            WHERE queue_id=? AND user_id=?
            """,
            (submitted, on_time, qid, user_id),
        )
        await db.commit()
    await apply_rating(user_id, kind)


async def carry_queue(qid: int, next_class_id: int) -> int:
    async with connect_db() as db:
        nq_cur = await db.execute("SELECT id FROM queues WHERE class_id=?", (next_class_id,))
        nq = await nq_cur.fetchone()
        if not nq:
            cur = await db.execute("INSERT INTO queues (class_id) VALUES (?)", (next_class_id,))
            next_queue_id = cur.lastrowid
        else:
            next_queue_id = nq["id"]

        unsub = await db.execute(
            "SELECT user_id FROM entries WHERE queue_id=? AND submitted=0", (qid,)
        )
        for row in await unsub.fetchall():
            await db.execute(
                "INSERT OR IGNORE INTO entries (queue_id, user_id) VALUES (?, ?)",
                (next_queue_id, row["user_id"]),
            )
        await db.execute("UPDATE queues SET status='completed' WHERE id=?", (qid,))
        await db.commit()
        return next_queue_id


async def user_active_entries(user_id: int) -> list[dict]:
    async with connect_db() as db:
        cur = await db.execute(
            """
            SELECT e.*, q.status AS queue_status, q.class_id,
                   c.dt, c.duration_minutes, c.room,
                   s.name AS subject_name, s.group_name
            FROM entries e
            JOIN queues q ON e.queue_id = q.id
            JOIN classes c ON q.class_id = c.id
            JOIN subjects s ON c.subject_id = s.id
            WHERE e.user_id = ?
              AND q.status IN ('open', 'closed')
            ORDER BY c.dt
            """,
            (user_id,),
        )
        return [dict(r) for r in await cur.fetchall()]


async def class_counts_for_month(year: int, month: int, group_name: Optional[str] = None) -> dict[str, int]:
    prefix = f"{year:04d}-{month:02d}-%"
    group_name = _clean_group(group_name)
    async with connect_db() as db:
        if group_name:
            cur = await db.execute(
                """
                SELECT substr(c.dt, 1, 10) AS day, COUNT(*) AS cnt
                FROM classes c JOIN subjects s ON c.subject_id=s.id
                WHERE c.dt LIKE ?
                  AND (s.group_name IS NULL OR s.group_name='' OR s.group_name=?)
                GROUP BY day
                """,
                (prefix, group_name),
            )
        else:
            cur = await db.execute(
                """
                SELECT substr(c.dt, 1, 10) AS day, COUNT(*) AS cnt
                FROM classes c JOIN subjects s ON c.subject_id=s.id
                WHERE c.dt LIKE ?
                GROUP BY day
                """,
                (prefix,),
            )
        return {row["day"]: row["cnt"] for row in await cur.fetchall()}


async def get_classes_to_auto_open(now_iso: str) -> list[dict]:
    """Return classes with pending queues whose opens_at time has passed."""
    async with connect_db() as db:
        cur = await db.execute(
            """
            SELECT c.id FROM classes c
            JOIN queues q ON q.class_id = c.id
            WHERE q.status = 'pending'
              AND c.opens_at IS NOT NULL
              AND c.opens_at <= ?
            """,
            (now_iso,),
        )
        return [dict(r) for r in await cur.fetchall()]


async def get_classes_to_auto_close(now_iso: str) -> list[dict]:
    """Return classes with open queues whose closes_at time has passed."""
    async with connect_db() as db:
        cur = await db.execute(
            """
            SELECT c.id FROM classes c
            JOIN queues q ON q.class_id = c.id
            WHERE q.status = 'open'
              AND c.closes_at IS NOT NULL
              AND c.closes_at <= ?
            """,
            (now_iso,),
        )
        return [dict(r) for r in await cur.fetchall()]


async def user_entries_for_queues(user_id: int, queue_ids: list[int]) -> dict[int, dict]:
    if not queue_ids:
        return {}
    placeholders = ",".join("?" * len(queue_ids))
    async with connect_db() as db:
        cur = await db.execute(
            f"SELECT * FROM entries WHERE user_id=? AND queue_id IN ({placeholders})",
            (user_id, *queue_ids),
        )
        return {row["queue_id"]: dict(row) for row in await cur.fetchall()}


async def class_exists_by_subject_dt(subject_id: int, dt: str) -> bool:
    async with connect_db() as db:
        cur = await db.execute(
            "SELECT id FROM classes WHERE subject_id=? AND dt=?", (subject_id, dt)
        )
        return bool(await cur.fetchone())


# ─── ИКБО-42-24 schedule seed ────────────────────────────────────────────────

_SEED_GROUP = "ИКБО-42-24"

# class_type: ПР = practical (has queue), ЛК = lecture (no queue), ФК = physical ed (no queue)
# (subject_name, date YYYY-MM-DD, time_start HH:MM, time_end HH:MM, room, teacher, class_type)
_RAW_CLASSES: list[tuple] = [
    # ══════════════════════ Week 15 ══════════════════════
    # Wednesday 20.05.2026
    ("Иностранный язык",                              "2026-05-20", "12:40", "14:10", "И-313",   "Ослякова И. В.",      "ПР"),
    ("Теория принятия решений",                       "2026-05-20", "14:20", "15:50", "Г-110-а", "Железняк Л. М.",      "ПР"),
    ("Теория вероятностей и мат. статистика",         "2026-05-20", "16:20", "17:50", "А-10",    "Козлова О. Ю.",       "ЛК"),
    # Thursday 21.05.2026
    ("Анализ и концептуальное моделирование систем",  "2026-05-21", "09:00", "10:30", "И-212-г", "Трушин С. М.",        "ПР"),
    ("Многоагентное моделирование",                   "2026-05-21", "10:40", "12:10", "Г-110-б", "Гололобов А. А.",     "ПР"),
    ("Технология разработки программных приложений",  "2026-05-21", "12:40", "14:10", "И-203-б", "Золотухин С. А.",     "ПР"),
    ("Физическая культура и спорт",                   "2026-05-21", "14:20", "15:50", "ФОК-9",   None,                  "ФК"),
    ("Программирование на языке Питон",               "2026-05-21", "16:20", "17:50", "А-2",     "Горчаков А. В.",      "ЛК"),
    # Friday 22.05.2026
    ("Программирование на языке Питон",               "2026-05-22", "09:00", "10:30", "А-424-2", "Бурдин А. М.",        "ПР"),
    ("Программирование на языке Питон",               "2026-05-22", "10:40", "12:10", "А-424-2", "Бурдин А. М.",        "ПР"),
    ("Проектирование баз данных",                     "2026-05-22", "12:40", "14:10", "А-11",    "Семыкина Н. А.",      "ЛК"),
    # Saturday 23.05.2026
    ("Теория вероятностей и мат. статистика",         "2026-05-23", "09:00", "10:30", "А-403",   "Осадченко А. В.",     "ПР"),
    ("Теория вероятностей и мат. статистика",         "2026-05-23", "10:40", "12:10", "А-403",   "Осадченко А. В.",     "ПР"),
    # ══════════════════════ Week 16 ══════════════════════
    # Monday 25.05.2026
    ("Философия",                                     "2026-05-25", "16:20", "17:50", "А-63",    "Никитина Е. А.",      "ЛК"),
    ("Социальная психология и педагогика",            "2026-05-25", "18:00", "19:30", "А-63",    "Талалуева Т. А.",     "ЛК"),
    # Tuesday 26.05.2026
    ("Теория принятия решений",                       "2026-05-26", "09:00", "10:30", "Г-112",   "Сорокин А. Б.",       "ЛК"),
    ("Проектирование баз данных",                     "2026-05-26", "10:40", "12:10", "И-212-а", "Копылова Я. А.",      "ПР"),
    # Wednesday 27.05.2026
    ("Иностранный язык",                              "2026-05-27", "12:40", "14:10", "И-313",   "Ослякова И. В.",      "ПР"),
    ("Теория принятия решений",                       "2026-05-27", "14:20", "15:50", "Г-110-а", "Железняк Л. М.",      "ПР"),
    ("Анализ и концептуальное моделирование систем",  "2026-05-27", "16:20", "17:50", "А-10",    "Ахмедова Х. Г.",      "ЛК"),
    # Thursday 28.05.2026
    ("Анализ и концептуальное моделирование систем",  "2026-05-28", "09:00", "10:30", "И-212-г", "Трушин С. М.",        "ПР"),
    ("Многоагентное моделирование",                   "2026-05-28", "10:40", "12:10", "Г-110-б", "Гололобов А. А.",     "ПР"),
    ("Технология разработки программных приложений",  "2026-05-28", "12:40", "14:10", "И-203-б", "Золотухин С. А.",     "ПР"),
    ("Физическая культура и спорт",                   "2026-05-28", "14:20", "15:50", "ФОК-9",   None,                  "ФК"),
    # Friday 29.05.2026
    ("Программирование на языке Питон",               "2026-05-29", "09:00", "10:30", "А-424-2", "Бурдин А. М.",        "ПР"),
    ("Программирование на языке Питон",               "2026-05-29", "10:40", "12:10", "А-424-2", "Бурдин А. М.",        "ПР"),
    ("Технология разработки программных приложений",  "2026-05-29", "12:40", "14:10", "А-11",    "Жматов Д. В.",        "ЛК"),
    # Saturday 30.05.2026
    ("Теория вероятностей и мат. статистика",         "2026-05-30", "09:00", "10:30", "А-403",   "Осадченко А. В.",     "ПР"),
    ("Теория вероятностей и мат. статистика",         "2026-05-30", "10:40", "12:10", "А-403",   "Осадченко А. В.",     "ПР"),
    # ══════════════════════ Week 17 ══════════════════════
    # Monday 01.06.2026
    ("Социальная психология и педагогика",            "2026-06-01", "09:00", "10:30", "Б-403",   "Жемерикина Ю. И.",   "ПР"),
    ("Философия",                                     "2026-06-01", "10:40", "12:10", "Б-402",   "Девайкин И. А.",     "ПР"),
    # Tuesday 02.06.2026
    ("Социальная психология и педагогика",            "2026-06-02", "09:00", "10:30", "Б-403",   "Жемерикина Ю. И.",   "ПР"),
    ("Философия",                                     "2026-06-02", "10:40", "12:10", "Б-402",   "Девайкин И. А.",     "ПР"),
    # Wednesday 03.06.2026
    ("Программирование на языке Питон",               "2026-06-03", "09:00", "10:30", "А-424-2", "Бурдин А. М.",        "ПР"),
    ("Программирование на языке Питон",               "2026-06-03", "10:40", "12:10", "А-424-2", "Бурдин А. М.",        "ПР"),
    ("Технология разработки программных приложений",  "2026-06-03", "12:40", "14:10", "А-11",    "Жматов Д. В.",        "ЛК"),
    # Thursday 04.06.2026
    ("Теория вероятностей и мат. статистика",         "2026-06-04", "09:00", "10:30", "А-403",   "Осадченко А. В.",     "ПР"),
    ("Теория вероятностей и мат. статистика",         "2026-06-04", "10:40", "12:10", "А-403",   "Осадченко А. В.",     "ПР"),
]


async def seed_ikbo_42_24() -> dict:
    """Seed ИКБО-42-24 schedule from real timetable (weeks 15–17, May–June 2026)."""
    existing = await all_subjects(_SEED_GROUP)
    if existing:
        return {"status": "already_seeded", "count": len(existing)}

    # Group by date, sorted by start time
    day_groups: dict[str, list[tuple]] = defaultdict(list)
    for entry in _RAW_CLASSES:
        day_groups[entry[1]].append(entry)
    for d in day_groups:
        day_groups[d].sort(key=lambda e: e[2])

    subject_cache: dict[str, int] = {}
    imported = 0

    for date_str in sorted(day_groups):
        prev_t_start: Optional[str] = None
        for subj_name, date, t_start, t_end, room, teacher, class_type in day_groups[date_str]:
            dt = datetime.fromisoformat(f"{date}T{t_start}:00")
            is_queueable = class_type == "ПР"

            if is_queueable:
                if prev_t_start:
                    opens_dt = datetime.fromisoformat(f"{date}T{prev_t_start}:00")
                else:
                    opens_dt = dt - timedelta(minutes=90)
                closes_dt = dt - timedelta(minutes=10)
                opens_iso = opens_dt.isoformat()
                closes_iso = closes_dt.isoformat()
            else:
                opens_iso = None
                closes_iso = None

            if subj_name not in subject_cache:
                subject_cache[subj_name] = await add_subject(subj_name, _SEED_GROUP)
            sid = subject_cache[subj_name]

            t0 = datetime.strptime(t_start, "%H:%M")
            t1 = datetime.strptime(t_end, "%H:%M")
            duration = int((t1 - t0).total_seconds() / 60)

            dt_str = f"{date}T{t_start}:00"
            if not await class_exists_by_subject_dt(sid, dt_str):
                await add_class(
                    sid, dt_str, room, teacher, duration,
                    class_type=class_type,
                    opens_at=opens_iso,
                    closes_at=closes_iso,
                    create_queue=is_queueable,
                )
                imported += 1

            prev_t_start = t_start

    return {"status": "ok", "imported": imported}


async def reseed_ikbo_42_24() -> dict:
    """Delete existing ИКБО-42-24 schedule and reseed with correct data."""
    async with connect_db() as db:
        await db.execute("DELETE FROM subjects WHERE group_name=?", (_SEED_GROUP,))
        await db.commit()
    return await seed_ikbo_42_24()

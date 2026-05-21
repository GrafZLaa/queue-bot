import contextlib
import os
import random
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Optional

import aiosqlite

DB = os.getenv("DB_PATH", "queue.db")
DEFAULT_DURATION = 90

# Increment this whenever _RAW_CLASSES changes — triggers auto-reseed on next deploy
_SCHEDULE_VERSION = 4
_SEED_GROUP = "ИКБО-42-24"

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS users (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    telegram_id    INTEGER UNIQUE NOT NULL,
    username       TEXT,
    full_name      TEXT NOT NULL DEFAULT '',
    group_name     TEXT,
    name_confirmed INTEGER NOT NULL DEFAULT 0,
    rating         INTEGER NOT NULL DEFAULT 50,
    category       TEXT    NOT NULL DEFAULT 'middle',
    on_time        INTEGER NOT NULL DEFAULT 0,
    late           INTEGER NOT NULL DEFAULT 0,
    no_show        INTEGER NOT NULL DEFAULT 0
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
    duration_minutes INTEGER NOT NULL DEFAULT 90,
    room             TEXT,
    teacher          TEXT,
    class_type       TEXT NOT NULL DEFAULT 'ПР',
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
    FOREIGN KEY (class_id)    REFERENCES classes(id)   ON DELETE CASCADE,
    FOREIGN KEY (subject_id)  REFERENCES subjects(id)  ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS queues (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    class_id INTEGER UNIQUE NOT NULL,
    status   TEXT NOT NULL DEFAULT 'pending',
    FOREIGN KEY (class_id) REFERENCES classes(id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS entries (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    queue_id   INTEGER NOT NULL,
    user_id    INTEGER NOT NULL,
    position   INTEGER,
    q_category TEXT,
    submitted  INTEGER NOT NULL DEFAULT 0,
    on_time    INTEGER NOT NULL DEFAULT 0,
    UNIQUE(queue_id, user_id),
    FOREIGN KEY (queue_id) REFERENCES queues(id) ON DELETE CASCADE,
    FOREIGN KEY (user_id)  REFERENCES users(id)  ON DELETE CASCADE
);
"""


@contextlib.asynccontextmanager
async def connect_db():
    async with aiosqlite.connect(DB) as conn:
        conn.row_factory = aiosqlite.Row
        await conn.execute("PRAGMA foreign_keys = ON")
        yield conn


# ─── helpers ─────────────────────────────────────────────────────────────────

def _cat(rating: int) -> str:
    if rating >= 65:
        return "good"
    if rating <= 35:
        return "poor"
    return "middle"


def class_end_time(dt: str, duration_minutes: Optional[int] = None) -> datetime:
    return datetime.fromisoformat(dt) + timedelta(minutes=duration_minutes or DEFAULT_DURATION)


# ─── init & migrate ──────────────────────────────────────────────────────────

async def init() -> None:
    async with connect_db() as db:
        await db.executescript(SCHEMA)
        await db.commit()
    await _migrate()


async def _migrate() -> None:
    async with connect_db() as db:
        cur = await db.execute("PRAGMA table_info(users)")
        user_cols = {r["name"] for r in await cur.fetchall()}
        cur = await db.execute("PRAGMA table_info(classes)")
        class_cols = {r["name"] for r in await cur.fetchall()}

        for col, ddl in [
            ("group_name",     "ALTER TABLE users ADD COLUMN group_name TEXT"),
            ("name_confirmed", "ALTER TABLE users ADD COLUMN name_confirmed INTEGER NOT NULL DEFAULT 0"),
        ]:
            if col not in user_cols:
                await db.execute(ddl)

        for col, ddl in [
            ("class_type", "ALTER TABLE classes ADD COLUMN class_type TEXT NOT NULL DEFAULT 'ПР'"),
            ("opens_at",   "ALTER TABLE classes ADD COLUMN opens_at TEXT"),
            ("closes_at",  "ALTER TABLE classes ADD COLUMN closes_at TEXT"),
        ]:
            if col not in class_cols:
                await db.execute(ddl)

        await db.commit()


# ─── meta ─────────────────────────────────────────────────────────────────────

async def meta_get(key: str) -> Optional[str]:
    async with connect_db() as db:
        cur = await db.execute("SELECT value FROM meta WHERE key=?", (key,))
        row = await cur.fetchone()
        return row["value"] if row else None


async def meta_set(key: str, value: str) -> None:
    async with connect_db() as db:
        await db.execute("INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)", (key, value))
        await db.commit()


# ─── users ───────────────────────────────────────────────────────────────────

async def ensure_user(tg_id: int, username: Optional[str], full_name: str,
                      group_name: Optional[str] = None) -> dict:
    group = (group_name or "").strip() or None
    async with connect_db() as db:
        await db.execute(
            "INSERT OR IGNORE INTO users (telegram_id, username, full_name, group_name) VALUES (?,?,?,?)",
            (tg_id, username, full_name, group),
        )
        await db.execute(
            "UPDATE users SET username=?, full_name=?, group_name=? WHERE telegram_id=?",
            (username, full_name, group, tg_id),
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
                (group_name.strip(),),
            )
        else:
            cur = await db.execute("SELECT * FROM users ORDER BY rating DESC, full_name")
        return [dict(r) for r in await cur.fetchall()]


async def set_name_confirmed(tg_id: int) -> None:
    async with connect_db() as db:
        await db.execute("UPDATE users SET name_confirmed=1 WHERE telegram_id=?", (tg_id,))
        await db.commit()


async def set_full_name(user_id: int, name: str) -> None:
    async with connect_db() as db:
        await db.execute("UPDATE users SET full_name=? WHERE id=?", (name, user_id))
        await db.commit()


async def set_group(user_id: int, group_name: Optional[str]) -> None:
    group = (group_name or "").strip() or None
    async with connect_db() as db:
        await db.execute("UPDATE users SET group_name=? WHERE id=?", (group, user_id))
        await db.commit()


async def set_rating(user_id: int, rating: int) -> None:
    r = max(0, min(100, rating))
    async with connect_db() as db:
        await db.execute("UPDATE users SET rating=?, category=? WHERE id=?", (r, _cat(r), user_id))
        await db.commit()


async def apply_submission(user_id: int, kind: str) -> None:
    delta = {"on_time": 10, "late": 2, "no_show": -10}[kind]
    u = await get_user(user_id)
    if not u:
        return
    r = max(0, min(100, u["rating"] + delta))
    ot = u["on_time"] + (1 if kind == "on_time" else 0)
    la = u["late"]    + (1 if kind == "late"    else 0)
    ns = u["no_show"] + (1 if kind == "no_show" else 0)
    async with connect_db() as db:
        await db.execute(
            "UPDATE users SET rating=?, category=?, on_time=?, late=?, no_show=? WHERE id=?",
            (r, _cat(r), ot, la, ns, user_id),
        )
        await db.commit()


# ─── subjects ────────────────────────────────────────────────────────────────

async def all_subjects(group_name: Optional[str] = None) -> list[dict]:
    async with connect_db() as db:
        if group_name:
            cur = await db.execute(
                "SELECT * FROM subjects WHERE group_name=? ORDER BY name",
                ((group_name or "").strip(),),
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
    group = (group_name or "").strip() or None
    async with connect_db() as db:
        cur = await db.execute(
            "INSERT INTO subjects (name, group_name) VALUES (?,?)", (name.strip(), group)
        )
        await db.commit()
        return cur.lastrowid


async def delete_subject(sid: int) -> None:
    async with connect_db() as db:
        await db.execute("DELETE FROM subjects WHERE id=?", (sid,))
        await db.commit()


# ─── classes ─────────────────────────────────────────────────────────────────

async def classes_for_subject(sid: int) -> list[dict]:
    async with connect_db() as db:
        cur = await db.execute("SELECT * FROM classes WHERE subject_id=? ORDER BY dt", (sid,))
        return [dict(r) for r in await cur.fetchall()]


async def classes_for_date(date_iso: str, group_name: Optional[str] = None) -> list[dict]:
    group = (group_name or "").strip() or None
    async with connect_db() as db:
        if group:
            cur = await db.execute(
                """
                SELECT c.*, s.name AS subject_name, s.group_name
                FROM classes c JOIN subjects s ON c.subject_id=s.id
                WHERE c.dt LIKE ? AND s.group_name=?
                ORDER BY c.dt
                """,
                (f"{date_iso}%", group),
            )
        else:
            cur = await db.execute(
                """
                SELECT c.*, s.name AS subject_name, s.group_name
                FROM classes c JOIN subjects s ON c.subject_id=s.id
                WHERE c.dt LIKE ?
                ORDER BY c.dt
                """,
                (f"{date_iso}%",),
            )
        return [dict(r) for r in await cur.fetchall()]


async def get_class(cid: int) -> Optional[dict]:
    async with connect_db() as db:
        cur = await db.execute(
            """
            SELECT c.*, s.name AS subject_name, s.group_name
            FROM classes c JOIN subjects s ON c.subject_id=s.id
            WHERE c.id=?
            """,
            (cid,),
        )
        row = await cur.fetchone()
        return dict(row) if row else None


async def add_class(subject_id: int, dt: str, room: Optional[str], teacher: Optional[str],
                    duration_minutes: int = DEFAULT_DURATION, class_type: str = "ПР",
                    opens_at: Optional[str] = None, closes_at: Optional[str] = None,
                    create_queue: bool = True) -> int:
    dur = max(15, min(360, duration_minutes or DEFAULT_DURATION))
    async with connect_db() as db:
        cur = await db.execute(
            """
            INSERT INTO classes (subject_id, dt, duration_minutes, room, teacher,
                                 class_type, opens_at, closes_at)
            VALUES (?,?,?,?,?,?,?,?)
            """,
            (subject_id, dt, dur, room, teacher, class_type, opens_at, closes_at),
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


async def class_counts_for_month(year: int, month: int, group_name: Optional[str] = None) -> dict:
    group = (group_name or "").strip() or None
    prefix = f"{year:04d}-{month:02d}-%"
    async with connect_db() as db:
        if group:
            cur = await db.execute(
                """
                SELECT substr(c.dt,1,10) AS day, COUNT(*) AS cnt
                FROM classes c JOIN subjects s ON c.subject_id=s.id
                WHERE c.dt LIKE ? AND s.group_name=?
                GROUP BY day
                """,
                (prefix, group),
            )
        else:
            cur = await db.execute(
                "SELECT substr(c.dt,1,10) AS day, COUNT(*) AS cnt FROM classes WHERE dt LIKE ? GROUP BY day",
                (prefix,),
            )
        return {r["day"]: r["cnt"] for r in await cur.fetchall()}


# ─── assignments ─────────────────────────────────────────────────────────────

async def assignments_for_class(cid: int) -> list[dict]:
    cls = await get_class(cid)
    if not cls:
        return []
    async with connect_db() as db:
        cur = await db.execute(
            """
            SELECT * FROM assignments
            WHERE class_id=? OR (subject_id=? AND class_id IS NULL)
            ORDER BY COALESCE(deadline,'9999'), title
            """,
            (cid, cls["subject_id"]),
        )
        return [dict(r) for r in await cur.fetchall()]


async def add_assignment(class_id: Optional[int], subject_id: int, title: str,
                         description: Optional[str], deadline: Optional[str],
                         url: Optional[str]) -> int:
    async with connect_db() as db:
        cur = await db.execute(
            "INSERT INTO assignments (class_id, subject_id, title, description, deadline, url) VALUES (?,?,?,?,?,?)",
            (class_id, subject_id, title, description, deadline, url),
        )
        await db.commit()
        return cur.lastrowid


# ─── queues ──────────────────────────────────────────────────────────────────

async def queue_for_class(cid: int) -> Optional[dict]:
    async with connect_db() as db:
        cur = await db.execute("SELECT * FROM queues WHERE class_id=?", (cid,))
        row = await cur.fetchone()
        return dict(row) if row else None


async def get_queue(qid: int) -> Optional[dict]:
    async with connect_db() as db:
        cur = await db.execute(
            """
            SELECT q.*, c.dt, c.room, c.subject_id, c.class_type,
                   s.name AS subject_name, s.group_name
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
    assert status in ("pending", "open", "closed", "completed")
    async with connect_db() as db:
        await db.execute("UPDATE queues SET status=? WHERE id=?", (status, qid))
        await db.commit()


async def queue_entries(qid: int) -> list[dict]:
    async with connect_db() as db:
        cur = await db.execute(
            """
            SELECT e.*, u.full_name, u.username, u.telegram_id,
                   u.rating, u.category AS user_cat, u.group_name
            FROM entries e JOIN users u ON e.user_id=u.id
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
            "INSERT OR IGNORE INTO entries (queue_id, user_id) VALUES (?,?)", (qid, user_id)
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
            "SELECT e.id, u.category FROM entries e JOIN users u ON e.user_id=u.id WHERE e.queue_id=?",
            (qid,),
        )
        rows = list(await cur.fetchall())

    groups: dict[str, list[int]] = {"good": [], "middle": [], "poor": []}
    for r in rows:
        groups[r["category"]].append(r["id"])
    for lst in groups.values():
        random.shuffle(lst)

    ordered = groups["good"] + groups["middle"] + groups["poor"]
    n = len(ordered)
    if not n:
        await set_queue_status(qid, "closed")
        return

    cut1 = max(1, (n + 2) // 3)
    cut2 = max(cut1, (2 * n + 2) // 3)

    async with connect_db() as db:
        for pos, eid in enumerate(ordered, 1):
            qcat = "good" if pos <= cut1 else ("middle" if pos <= cut2 else "poor")
            await db.execute("UPDATE entries SET position=?, q_category=? WHERE id=?", (pos, qcat, eid))
        await db.execute("UPDATE queues SET status='closed' WHERE id=?", (qid,))
        await db.commit()


async def mark_submission(qid: int, user_id: int, kind: str) -> None:
    assert kind in ("on_time", "late", "no_show")
    async with connect_db() as db:
        await db.execute(
            "UPDATE entries SET submitted=?, on_time=? WHERE queue_id=? AND user_id=?",
            (1 if kind != "no_show" else 0, 1 if kind == "on_time" else 0, qid, user_id),
        )
        await db.commit()
    await apply_submission(user_id, kind)


async def carry_queue(qid: int, next_class_id: int) -> int:
    async with connect_db() as db:
        row = await (await db.execute("SELECT id FROM queues WHERE class_id=?", (next_class_id,))).fetchone()
        if row:
            next_qid = row["id"]
        else:
            next_qid = (await db.execute("INSERT INTO queues (class_id) VALUES (?)", (next_class_id,))).lastrowid
        unsub = await (await db.execute(
            "SELECT user_id FROM entries WHERE queue_id=? AND submitted=0", (qid,)
        )).fetchall()
        for r in unsub:
            await db.execute(
                "INSERT OR IGNORE INTO entries (queue_id, user_id) VALUES (?,?)", (next_qid, r["user_id"])
            )
        await db.execute("UPDATE queues SET status='completed' WHERE id=?", (qid,))
        await db.commit()
        return next_qid


async def user_active_entries(user_id: int) -> list[dict]:
    async with connect_db() as db:
        cur = await db.execute(
            """
            SELECT e.*, q.status AS queue_status, q.class_id,
                   c.dt, c.room, s.name AS subject_name
            FROM entries e
            JOIN queues q ON e.queue_id=q.id
            JOIN classes c ON q.class_id=c.id
            JOIN subjects s ON c.subject_id=s.id
            WHERE e.user_id=? AND q.status IN ('open','closed')
            ORDER BY c.dt
            """,
            (user_id,),
        )
        return [dict(r) for r in await cur.fetchall()]


async def user_entries_for_queues(user_id: int, queue_ids: list[int]) -> dict[int, dict]:
    if not queue_ids:
        return {}
    ph = ",".join("?" * len(queue_ids))
    async with connect_db() as db:
        cur = await db.execute(
            f"SELECT * FROM entries WHERE user_id=? AND queue_id IN ({ph})",
            (user_id, *queue_ids),
        )
        return {r["queue_id"]: dict(r) for r in await cur.fetchall()}


async def get_classes_to_auto_open(now_iso: str) -> list[dict]:
    async with connect_db() as db:
        cur = await db.execute(
            """
            SELECT c.id FROM classes c JOIN queues q ON q.class_id=c.id
            WHERE q.status='pending' AND c.opens_at IS NOT NULL
              AND c.opens_at <= ? AND c.closes_at > ?
            """,
            (now_iso, now_iso),
        )
        return [dict(r) for r in await cur.fetchall()]


async def get_past_pending_classes(now_iso: str) -> list[dict]:
    async with connect_db() as db:
        cur = await db.execute(
            """
            SELECT c.id FROM classes c JOIN queues q ON q.class_id=c.id
            WHERE q.status='pending' AND c.closes_at IS NOT NULL AND c.closes_at <= ?
            """,
            (now_iso,),
        )
        return [dict(r) for r in await cur.fetchall()]


async def get_classes_to_auto_close(now_iso: str) -> list[dict]:
    async with connect_db() as db:
        cur = await db.execute(
            """
            SELECT c.id FROM classes c JOIN queues q ON q.class_id=c.id
            WHERE q.status='open' AND c.closes_at IS NOT NULL AND c.closes_at <= ?
            """,
            (now_iso,),
        )
        return [dict(r) for r in await cur.fetchall()]


# ─── seed ─────────────────────────────────────────────────────────────────────

# Source: Google Calendar ИКБО-42-24
# (subject_name, YYYY-MM-DD, HH:MM start, HH:MM end, room, teacher, type)
# type: ПР = семинар/практика (queue), ЛК = лекция (no queue), ФК = физра (no queue)
_RAW_CLASSES: list[tuple] = [
    # ══ Неделя 15 (18–23 мая) ══

    # Пн 18.05
    ("Социальная психология и педагогика",           "2026-05-18", "09:00", "10:30", "Б-403",   "Жемерикина Ю. И.",  "ПР"),
    ("Философия",                                    "2026-05-18", "10:40", "12:10", "Б-402",   "Девайкин И. А.",    "ПР"),
    # Вт 19.05
    ("Многоагентное моделирование",                  "2026-05-19", "09:00", "10:30", "Г-112",   "Гололобов А. А.",   "ЛК"),
    ("Проектирование баз данных",                    "2026-05-19", "10:40", "12:10", "И-212-а", "Копылова Я. А.",    "ПР"),
    # Ср 20.05
    ("Иностранный язык",                             "2026-05-20", "12:40", "14:10", "И-342",   "Ослякова И. В.",    "ПР"),
    ("Теория принятия решений",                      "2026-05-20", "14:20", "15:50", "Г-110-а", "Железняк Л. М.",   "ПР"),
    ("Теория вероятностей и мат. статистика",        "2026-05-20", "16:20", "17:50", "А-10",    "Козлова О. Ю.",     "ЛК"),
    # Чт 21.05
    ("Анализ и концептуальное моделирование систем", "2026-05-21", "09:00", "10:30", "И-212-г", "Трушин С. М.",      "ПР"),
    ("Многоагентное моделирование",                  "2026-05-21", "10:40", "12:10", "Г-110-б", "Гололобов А. А.",   "ПР"),
    ("Технология разработки программных приложений", "2026-05-21", "12:40", "14:10", "И-203-б", "Золотухин С. А.",   "ПР"),
    ("Физическая культура и спорт",                  "2026-05-21", "14:35", "15:35", "ФОК-9",   None,                "ФК"),
    ("Программирование на языке Python",             "2026-05-21", "16:20", "17:50", "А-2",     "Горчаков А. В.",    "ЛК"),
    # Пт 22.05
    ("Программирование на языке Python",             "2026-05-22", "09:00", "10:30", "А-424-2", "Бурдин А. М.",      "ПР"),
    ("Программирование на языке Python",             "2026-05-22", "10:40", "12:10", "А-424-2", "Бурдин А. М.",      "ПР"),
    ("Проектирование баз данных",                    "2026-05-22", "12:40", "14:10", "А-11",    "Семыкина Н. А.",    "ЛК"),
    # Сб 23.05
    ("Теория вероятностей и мат. статистика",        "2026-05-23", "09:00", "10:30", "А-403",   "Осадченко А. В.",   "ПР"),
    ("Теория вероятностей и мат. статистика",        "2026-05-23", "10:40", "12:10", "А-403",   "Осадченко А. В.",   "ПР"),

    # ══ Неделя 16 (25–30 мая) ══

    # Пн 25.05
    ("Философия",                                    "2026-05-25", "16:20", "17:50", "А-63МП",  "Никитина Е. А.",    "ЛК"),
    ("Социальная психология и педагогика",           "2026-05-25", "18:00", "19:30", "А-63МП",  "Талалуева Т. А.",   "ЛК"),
    # Вт 26.05
    ("Теория принятия решений",                      "2026-05-26", "09:00", "10:30", "Г-112",   "Сорокин А. Б.",     "ЛК"),
    ("Проектирование баз данных",                    "2026-05-26", "10:40", "12:10", "И-212-а", "Копылова Я. А.",    "ПР"),
    # Ср 27.05
    ("Иностранный язык",                             "2026-05-27", "12:40", "14:10", "И-342",   "Ослякова И. В.",    "ПР"),
    ("Теория принятия решений",                      "2026-05-27", "14:20", "15:50", "Г-110-а", "Железняк Л. М.",   "ПР"),
    ("Анализ и концептуальное моделирование систем", "2026-05-27", "16:20", "17:50", "А-10",    "Ахмедова Х. Г.",    "ЛК"),
    # Чт 28.05
    ("Анализ и концептуальное моделирование систем", "2026-05-28", "09:00", "10:30", "И-212-г", "Трушин С. М.",      "ПР"),
    ("Многоагентное моделирование",                  "2026-05-28", "10:40", "12:10", "Г-110-б", "Гололобов А. А.",   "ПР"),
    ("Технология разработки программных приложений", "2026-05-28", "12:40", "14:10", "И-203-б", "Золотухин С. А.",   "ПР"),
    ("Физическая культура и спорт",                  "2026-05-28", "14:35", "15:35", "ФОК-9",   None,                "ФК"),
    # Пт 29.05
    ("Программирование на языке Python",             "2026-05-29", "09:00", "10:30", "А-424-2", "Бурдин А. М.",      "ПР"),
    ("Программирование на языке Python",             "2026-05-29", "10:40", "12:10", "А-424-2", "Бурдин А. М.",      "ПР"),
    ("Технология разработки программных приложений", "2026-05-29", "12:40", "14:10", "А-11",    "Жматов Д. В.",      "ЛК"),
    # Сб 30.05
    ("Теория вероятностей и мат. статистика",        "2026-05-30", "09:00", "10:30", "А-403",   "Осадченко А. В.",   "ПР"),
    ("Теория вероятностей и мат. статистика",        "2026-05-30", "10:40", "12:10", "А-403",   "Осадченко А. В.",   "ПР"),

    # ══ Неделя 17 (1–4 июня) ══

    # Пн 01.06
    ("Социальная психология и педагогика",           "2026-06-01", "09:00", "10:30", "Б-403",   "Жемерикина Ю. И.",  "ПР"),
    ("Философия",                                    "2026-06-01", "10:40", "12:10", "Б-402",   "Девайкин И. А.",    "ПР"),
    # Вт 02.06
    ("Социальная психология и педагогика",           "2026-06-02", "09:00", "10:30", "Б-403",   "Жемерикина Ю. И.",  "ПР"),
    ("Философия",                                    "2026-06-02", "10:40", "12:10", "Б-402",   "Девайкин И. А.",    "ПР"),
    # Ср 03.06
    ("Программирование на языке Python",             "2026-06-03", "09:00", "10:30", "А-424-2", "Бурдин А. М.",      "ПР"),
    ("Программирование на языке Python",             "2026-06-03", "10:40", "12:10", "А-424-2", "Бурдин А. М.",      "ПР"),
    ("Технология разработки программных приложений", "2026-06-03", "12:40", "14:10", "А-11",    "Жматов Д. В.",      "ЛК"),
    # Чт 04.06
    ("Теория вероятностей и мат. статистика",        "2026-06-04", "09:00", "10:30", "А-403",   "Осадченко А. В.",   "ПР"),
    ("Теория вероятностей и мат. статистика",        "2026-06-04", "10:40", "12:10", "А-403",   "Осадченко А. В.",   "ПР"),
]


async def _do_seed() -> dict:
    day_groups: dict[str, list] = defaultdict(list)
    for row in _RAW_CLASSES:
        day_groups[row[1]].append(row)
    for lst in day_groups.values():
        lst.sort(key=lambda r: r[2])

    subject_map: dict[str, int] = {}
    imported = 0

    for date in sorted(day_groups):
        prev_start: Optional[str] = None
        for subj, d, t_start, t_end, room, teacher, ctype in day_groups[date]:
            is_pr = ctype == "ПР"
            if is_pr:
                opens = (
                    datetime.fromisoformat(f"{d}T{prev_start}:00")
                    if prev_start
                    else datetime.fromisoformat(f"{d}T{t_start}:00") - timedelta(minutes=90)
                )
                closes = datetime.fromisoformat(f"{d}T{t_start}:00") - timedelta(minutes=10)
                opens_iso  = opens.isoformat()
                closes_iso = closes.isoformat()
            else:
                opens_iso = closes_iso = None

            if subj not in subject_map:
                subject_map[subj] = await add_subject(subj, _SEED_GROUP)
            sid = subject_map[subj]

            dur = int((datetime.strptime(t_end, "%H:%M") - datetime.strptime(t_start, "%H:%M")).total_seconds() / 60)
            dt_str = f"{d}T{t_start}:00"

            async with connect_db() as db:
                exists = bool((await (await db.execute(
                    "SELECT id FROM classes WHERE subject_id=? AND dt=?", (sid, dt_str)
                )).fetchone()))

            if not exists:
                await add_class(sid, dt_str, room, teacher, dur,
                                class_type=ctype, opens_at=opens_iso, closes_at=closes_iso,
                                create_queue=is_pr)
                imported += 1

            prev_start = t_start

    return {"status": "ok", "imported": imported}


async def reseed_ikbo_42_24() -> dict:
    """Wipe ИКБО-42-24 schedule and insert fresh data from _RAW_CLASSES."""
    async with connect_db() as db:
        # Explicit delete — don't rely on ON DELETE CASCADE (old SQLite schemas may lack it)
        cur = await db.execute("SELECT id FROM subjects WHERE group_name=?", (_SEED_GROUP,))
        ids = [r[0] for r in await cur.fetchall()]
        if ids:
            ph = ",".join("?" * len(ids))
            await db.execute(f"DELETE FROM queues WHERE class_id IN (SELECT id FROM classes WHERE subject_id IN ({ph}))", ids)
            await db.execute(f"DELETE FROM classes WHERE subject_id IN ({ph})", ids)
        await db.execute("DELETE FROM subjects WHERE group_name=?", (_SEED_GROUP,))
        await db.commit()
    result = await _do_seed()
    await meta_set("schedule_version", str(_SCHEDULE_VERSION))
    return result


async def ensure_seed_current() -> dict:
    """On every startup: reseed if _SCHEDULE_VERSION changed, seed if DB is empty."""
    stored = await meta_get("schedule_version")
    if stored != str(_SCHEDULE_VERSION):
        return await reseed_ikbo_42_24()
    subjects = await all_subjects(_SEED_GROUP)
    if not subjects:
        result = await _do_seed()
        await meta_set("schedule_version", str(_SCHEDULE_VERSION))
        return result
    return {"status": "current", "version": _SCHEDULE_VERSION}

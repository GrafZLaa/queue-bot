import contextlib
import os
import random
from datetime import datetime, timedelta
from typing import Optional

import aiosqlite

DB = os.getenv("DB_PATH", "queue.db")
DEFAULT_DURATION_MINUTES = 90

SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    telegram_id   INTEGER UNIQUE NOT NULL,
    username      TEXT,
    full_name     TEXT NOT NULL,
    group_name    TEXT,
    rating        INTEGER DEFAULT 50,
    category      TEXT DEFAULT 'middle',
    on_time       INTEGER DEFAULT 0,
    late          INTEGER DEFAULT 0,
    no_show       INTEGER DEFAULT 0
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
    """Async context manager yielding a SQLite connection with row dicts and FK support."""
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
    """Return a queue category by rating."""
    if rating >= 65:
        return "good"
    if rating <= 35:
        return "poor"
    return "middle"


def class_end_time(dt: str, duration_minutes: Optional[int] = None) -> datetime:
    """Return the calculated class end datetime."""
    start = datetime.fromisoformat(dt)
    minutes = duration_minutes or DEFAULT_DURATION_MINUTES
    return start + timedelta(minutes=minutes)


async def _columns(table: str) -> set[str]:
    async with connect_db() as db:
        cur = await db.execute(f"PRAGMA table_info({table})")
        return {row["name"] for row in await cur.fetchall()}


async def _migrate() -> None:
    """Apply lightweight migrations for databases created by older versions."""
    user_columns = await _columns("users")
    class_columns = await _columns("classes")
    async with connect_db() as db:
        if "group_name" not in user_columns:
            await db.execute("ALTER TABLE users ADD COLUMN group_name TEXT")
        if "duration_minutes" not in class_columns:
            await db.execute(
                "ALTER TABLE classes ADD COLUMN duration_minutes INTEGER DEFAULT 90"
            )
        await db.execute(
            "UPDATE classes SET duration_minutes=? WHERE duration_minutes IS NULL",
            (DEFAULT_DURATION_MINUTES,),
        )
        await db.commit()


async def init() -> None:
    """Initialize the SQLite database and apply schema migrations."""
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
    """Create or update a Telegram user profile."""
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
    """Return classes for a date, filtered by the student's group when provided."""
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
    room: str,
    teacher: str,
    duration_minutes: int = DEFAULT_DURATION_MINUTES,
) -> int:
    duration_minutes = max(15, min(360, int(duration_minutes or DEFAULT_DURATION_MINUTES)))
    async with connect_db() as db:
        cur = await db.execute(
            """
            INSERT INTO classes (subject_id, dt, duration_minutes, room, teacher)
            VALUES (?, ?, ?, ?, ?)
            """,
            (subject_id, dt, duration_minutes, room, teacher),
        )
        cid = cur.lastrowid
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
    """Close a queue and assign positions by rating group, randomized inside groups."""
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
    """Move users without successful submission to the next class queue."""
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
    """Return queue entries for a user in open or closed queues, ordered by class datetime."""
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


async def user_entries_for_queues(user_id: int, queue_ids: list[int]) -> dict[int, dict]:
    """Return {queue_id: entry} for a user across the given queue IDs."""
    if not queue_ids:
        return {}
    placeholders = ",".join("?" * len(queue_ids))
    async with connect_db() as db:
        cur = await db.execute(
            f"SELECT * FROM entries WHERE user_id=? AND queue_id IN ({placeholders})",
            (user_id, *queue_ids),
        )
        return {row["queue_id"]: dict(row) for row in await cur.fetchall()}

import aiosqlite
import random
from datetime import datetime
from typing import Optional

DB = "queue.db"

SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    telegram_id   INTEGER UNIQUE NOT NULL,
    username      TEXT,
    full_name     TEXT NOT NULL,
    rating        INTEGER DEFAULT 50,
    category      TEXT DEFAULT 'middle',
    on_time       INTEGER DEFAULT 0,
    late          INTEGER DEFAULT 0,
    no_show       INTEGER DEFAULT 0
);
CREATE TABLE IF NOT EXISTS subjects (
    id    INTEGER PRIMARY KEY AUTOINCREMENT,
    name  TEXT NOT NULL,
    group_name TEXT
);
CREATE TABLE IF NOT EXISTS classes (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    subject_id  INTEGER NOT NULL,
    dt          TEXT NOT NULL,
    room        TEXT,
    teacher     TEXT,
    FOREIGN KEY (subject_id) REFERENCES subjects(id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS assignments (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    class_id    INTEGER,
    subject_id  INTEGER NOT NULL,
    title       TEXT NOT NULL,
    description TEXT,
    deadline    TEXT,
    url         TEXT
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
    FOREIGN KEY (user_id)  REFERENCES users(id)
);
"""


def category(rating: int) -> str:
    """
    Определяет категорию пользователя на основе его рейтинга.

    Args:
        rating: Целочисленное значение рейтинга (0-100).

    Returns:
        Строка-категория: "good" (>=65), "poor" (<=35) или "middle" (в остальных случаях).
    """
    if rating >= 65: return "good"
    if rating <= 35: return "poor"
    return "middle"


async def init():
    """
    Инициализирует базу данных: создает необходимые таблицы, если они еще не существуют.
    """
    async with aiosqlite.connect(DB) as db:
        db.row_factory = aiosqlite.Row
        await db.executescript(SCHEMA)
        await db.commit()


# ── Users ────────────────────────────────────────────────────────────────────

async def ensure_user(tg_id: int, username: Optional[str], full_name: str) -> dict:
    """
    Создает пользователя в базе или обновляет данные существующего, если он уже есть.

    Args:
        tg_id: Telegram ID пользователя.
        username: Юзернейм пользователя.
        full_name: Полное имя пользователя.

    Returns:
        Словарь с данными пользователя из базы.
    """
    async with aiosqlite.connect(DB) as db:
        db.row_factory = aiosqlite.Row
        await db.execute(
            "INSERT OR IGNORE INTO users (telegram_id, username, full_name) VALUES (?,?,?)",
            (tg_id, username, full_name)
        )
        await db.execute(
            "UPDATE users SET username=?, full_name=? WHERE telegram_id=?",
            (username, full_name, tg_id)
        )
        await db.commit()
        cur = await db.execute("SELECT * FROM users WHERE telegram_id=?", (tg_id,))
        return dict(await cur.fetchone())


async def get_user_by_tg(tg_id: int) -> Optional[dict]:
    """
    Получает данные пользователя по его Telegram ID.

    Args:
        tg_id: Telegram ID пользователя.

    Returns:
        Словарь с данными пользователя или None, если пользователь не найден.
    """
    async with aiosqlite.connect(DB) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM users WHERE telegram_id=?", (tg_id,))
        row = await cur.fetchone()
        return dict(row) if row else None


async def get_user(user_id: int) -> Optional[dict]:
    """
    Получает данные пользователя по внутреннему ID в базе.

    Args:
        user_id: ID пользователя в базе данных.

    Returns:
        Словарь с данными пользователя или None, если пользователь не найден.
    """
    async with aiosqlite.connect(DB) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM users WHERE id=?", (user_id,))
        row = await cur.fetchone()
        return dict(row) if row else None


async def all_users() -> list:
    """
    Получает список всех пользователей, отсортированных по рейтингу в порядке убывания.

    Returns:
        Список словарей с данными пользователей.
    """

    async with aiosqlite.connect(DB) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM users ORDER BY rating DESC")
        return [dict(r) for r in await cur.fetchall()]


async def set_rating(user_id: int, rating: int):
    """
    Принудительно устанавливает рейтинг пользователя.

    Args:
        user_id: ID пользователя в базе данных.
        rating: Значение рейтинга (будет приведено к диапазону 0-100).
    """

    r = max(0, min(100, rating))
    async with aiosqlite.connect(DB) as db:
        await db.execute(
            "UPDATE users SET rating=?, category=? WHERE id=?",
            (r, category(r), user_id)
        )
        await db.commit()


async def set_full_name(user_id: int, full_name: str):
    """
    Обновляет полное имя пользователя.

    Args:
        user_id: ID пользователя в базе данных.
        full_name: Новое полное имя.
    """
    async with aiosqlite.connect(DB) as db:
        await db.execute(
            "UPDATE users SET full_name=? WHERE id=?",
            (full_name, user_id)
        )
        await db.commit()


async def apply_rating(user_id: int, kind: str):
    """
    Корректирует рейтинг и статистику посещаемости пользователя по завершении занятия.

    Args:
        user_id: ID пользователя в базе данных.
        kind: Тип события ("on_time", "late", "no_show").
    """
    delta = {"on_time": 10, "late": 2, "no_show": -10}[kind]
    u = await get_user(user_id)
    if not u: return
    new_r = max(0, min(100, u["rating"] + delta))
    ot = u["on_time"] + (1 if kind == "on_time" else 0)
    la = u["late"]    + (1 if kind == "late"    else 0)
    ns = u["no_show"] + (1 if kind == "no_show" else 0)
    async with aiosqlite.connect(DB) as db:
        await db.execute(
            "UPDATE users SET rating=?,category=?,on_time=?,late=?,no_show=? WHERE id=?",
            (new_r, category(new_r), ot, la, ns, user_id)
        )
        await db.commit()


# ── Subjects ─────────────────────────────────────────────────────────────────

async def all_subjects() -> list:
    """
    Возвращает список всех предметов, отсортированных по названию.

    Returns:
        Список словарей предметов.
    """
    async with aiosqlite.connect(DB) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM subjects ORDER BY name")
        return [dict(r) for r in await cur.fetchall()]


async def get_subject(sid: int) -> Optional[dict]:
    """
    Получает данные о предмете по его ID.

    Args:
        sid: ID предмета.

    Returns:
        Словарь с данными предмета или None.
    """
    async with aiosqlite.connect(DB) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM subjects WHERE id=?", (sid,))
        row = await cur.fetchone()
        return dict(row) if row else None


async def add_subject(name: str, group_name: Optional[str]) -> int:
    """
    Добавляет новый предмет в базу данных.

    Args:
        name: Название предмета.
        group_name: Название группы или потока.

    Returns:
        ID созданного предмета.
    """
    async with aiosqlite.connect(DB) as db:
        cur = await db.execute(
            "INSERT INTO subjects (name, group_name) VALUES (?,?)", (name, group_name)
        )
        await db.commit()
        return cur.lastrowid


async def delete_subject(sid: int):
    """
    Удаляет предмет и все связанные с ним классы и задания (каскадно).

    Args:
        sid: ID предмета.
    """
    async with aiosqlite.connect(DB) as db:
        await db.execute("DELETE FROM subjects WHERE id=?", (sid,))
        await db.commit()


# ── Classes ───────────────────────────────────────────────────────────────────

async def classes_for_subject(sid: int) -> list:
    """
    Возвращает список занятий для указанного предмета, отсортированных по дате.

    Args:
        sid: ID предмета.

    Returns:
        Список занятий.
    """
    async with aiosqlite.connect(DB) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT * FROM classes WHERE subject_id=? ORDER BY dt", (sid,)
        )
        return [dict(r) for r in await cur.fetchall()]


async def get_class(cid: int) -> Optional[dict]:
    """
    Получает расширенную информацию о занятии (включая название предмета).

    Args:
        cid: ID занятия.

    Returns:
        Словарь с данными занятия или None.
    """
    async with aiosqlite.connect(DB) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT c.*, s.name AS subject_name, s.group_name "
            "FROM classes c JOIN subjects s ON c.subject_id=s.id WHERE c.id=?", (cid,)
        )
        row = await cur.fetchone()
        return dict(row) if row else None


async def add_class(subject_id: int, dt: str, room: str, teacher: str) -> int:
    """
    Добавляет занятие и создает для него пустую очередь.

    Args:
        subject_id: ID предмета.
        dt: Дата и время занятия в формате ISO.
        room: Номер аудитории.
        teacher: Имя преподавателя.

    Returns:
        ID созданного занятия.
    """
    async with aiosqlite.connect(DB) as db:
        cur = await db.execute(
            "INSERT INTO classes (subject_id,dt,room,teacher) VALUES (?,?,?,?)",
            (subject_id, dt, room, teacher)
        )
        cid = cur.lastrowid
        await db.execute("INSERT INTO queues (class_id) VALUES (?)", (cid,))
        await db.commit()
        return cid


async def delete_class(cid: int):
    """
    Удаляет занятие по его ID.

    Args:
        cid: ID занятия.
    """

    async with aiosqlite.connect(DB) as db:
        await db.execute("DELETE FROM classes WHERE id=?", (cid,))
        await db.commit()


# ── Assignments ───────────────────────────────────────────────────────────────

async def assignments_for_class(cid: int) -> list:
    """
    Возвращает список заданий для занятия: специфичные для этого занятия или общие для предмета.

    Args:
        cid: ID занятия.

    Returns:
        Список заданий, отсортированных по дедлайну.
    """
    cls = await get_class(cid)
    if not cls: return []
    async with aiosqlite.connect(DB) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT * FROM assignments WHERE class_id=? OR (subject_id=? AND class_id IS NULL) ORDER BY deadline",
            (cid, cls["subject_id"])
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
    """
    Создает новое учебное задание.

    Args:
        class_id: ID занятия (может быть None для общих заданий предмета).
        subject_id: ID предмета.
        title: Название задания.
        description: Описание.
        deadline: Дедлайн в формате ISO.
        url: Ссылка на материал.

    Returns:
        ID созданного задания.
    """

    async with aiosqlite.connect(DB) as db:
        cur = await db.execute(
            "INSERT INTO assignments (class_id,subject_id,title,description,deadline,url) VALUES (?,?,?,?,?,?)",
            (class_id, subject_id, title, description, deadline, url)
        )
        await db.commit()
        return cur.lastrowid


async def delete_assignment(aid: int):
    """
    Удаляет задание по его ID.

    Args:
        aid: ID задания.
    """
    async with aiosqlite.connect(DB) as db:
        await db.execute("DELETE FROM assignments WHERE id=?", (aid,))
        await db.commit()


# ── Queues ────────────────────────────────────────────────────────────────────

async def queue_for_class(cid: int) -> Optional[dict]:
    """
    Получает информацию об очереди конкретного занятия.

    Args:
        cid: ID занятия.

    Returns:
        Словарь очереди или None.
    """
    async with aiosqlite.connect(DB) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM queues WHERE class_id=?", (cid,))
        row = await cur.fetchone()
        return dict(row) if row else None


async def get_queue(qid: int) -> Optional[dict]:
    """
    Получает информацию об очереди по её ID с расширенными данными о занятии и предмете.

    Args:
        qid: ID очереди.

    Returns:
        Словарь данных очереди или None.
    """
    async with aiosqlite.connect(DB) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT q.*, c.dt, c.room, c.subject_id, s.name AS subject_name "
            "FROM queues q JOIN classes c ON q.class_id=c.id "
            "JOIN subjects s ON c.subject_id=s.id WHERE q.id=?", (qid,)
        )
        row = await cur.fetchone()
        return dict(row) if row else None


async def set_queue_status(qid: int, status: str):
    """
    Меняет статус очереди (например, "pending", "closed", "completed").

    Args:
        qid: ID очереди.
        status: Статус очереди.
    """
    async with aiosqlite.connect(DB) as db:
        await db.execute("UPDATE queues SET status=? WHERE id=?", (status, qid))
        await db.commit()


async def queue_entries(qid: int) -> list:
    """
    Получает список всех участников очереди с данными их профилей.

    Args:
        qid: ID очереди.

    Returns:
        Список участников, отсортированных по их позиции в очереди.
    """
    async with aiosqlite.connect(DB) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT e.*, u.full_name, u.username, u.telegram_id, u.rating, u.category AS user_cat "
            "FROM entries e JOIN users u ON e.user_id=u.id "
            "WHERE e.queue_id=? ORDER BY COALESCE(e.position,9999), e.id",
            (qid,)
        )
        return [dict(r) for r in await cur.fetchall()]


async def is_in_queue(qid: int, user_id: int) -> bool:
    """
    Проверяет, записан ли пользователь в очередь.

    Args:
        qid: ID очереди.
        user_id: ID пользователя.

    Returns:
        True, если пользователь в очереди, иначе False.
    """
    async with aiosqlite.connect(DB) as db:
        cur = await db.execute(
            "SELECT id FROM entries WHERE queue_id=? AND user_id=?", (qid, user_id)
        )
        return bool(await cur.fetchone())


async def join_queue(qid: int, user_id: int):
    """
    Добавляет пользователя в очередь.

    Args:
        qid: ID очереди.
        user_id: ID пользователя.
    """
    async with aiosqlite.connect(DB) as db:
        await db.execute(
            "INSERT OR IGNORE INTO entries (queue_id, user_id) VALUES (?,?)", (qid, user_id)
        )
        await db.commit()


async def leave_queue(qid: int, user_id: int):
    """
    Удаляет пользователя из очереди.

    Args:
        qid: ID очереди.
        user_id: ID пользователя.
    """
    async with aiosqlite.connect(DB) as db:
        await db.execute(
            "DELETE FROM entries WHERE queue_id=? AND user_id=?", (qid, user_id)
        )
        await db.commit()


async def randomize_queue(qid: int):
    """
    Перемешивает очередь, распределяя позиции на основе рейтинга пользователей,
    и закрывает её.

    Args:
        qid: ID очереди.
    """

    async with aiosqlite.connect(DB) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT e.id, u.category FROM entries e JOIN users u ON e.user_id=u.id WHERE e.queue_id=?",
            (qid,)
        )
        rows = await cur.fetchall()

    groups = {"good": [], "middle": [], "poor": []}
    for r in rows:
        groups[r["category"]].append(r["id"])
    for g in groups.values():
        random.shuffle(g)

    ordered = groups["good"] + groups["middle"] + groups["poor"]
    n = len(ordered)
    if n == 0:
        await set_queue_status(qid, "closed")
        return

    t1 = max(1, (n + 2) // 3)
    t2 = max(t1, (2 * n + 2) // 3)

    async with aiosqlite.connect(DB) as db:
        for pos, eid in enumerate(ordered, 1):
            qcat = "good" if pos <= t1 else ("middle" if pos <= t2 else "poor")
            await db.execute(
                "UPDATE entries SET position=?, q_category=? WHERE id=?", (pos, qcat, eid)
            )
        await db.execute("UPDATE queues SET status='closed' WHERE id=?", (qid,))
        await db.commit()


async def mark_submission(qid: int, user_id: int, kind: str):
    """
    Отмечает факт сдачи работы пользователем и обновляет его рейтинг.

    Args:
        qid: ID очереди.
        user_id: ID пользователя.
        kind: Результат сдачи ("on_time", "late", "no_show").
    """
    submitted = 1 if kind != "no_show" else 0
    on_time   = 1 if kind == "on_time" else 0
    async with aiosqlite.connect(DB) as db:
        await db.execute(
            "UPDATE entries SET submitted=?, on_time=? WHERE queue_id=? AND user_id=?",
            (submitted, on_time, qid, user_id)
        )
        await db.commit()
    await apply_rating(user_id, kind)


async def carry_queue(qid: int, next_class_id: int) -> int:
    """
    Переносит пользователей, не сдавших работу в текущей очереди, в очередь следующего занятия.

    Args:
        qid: ID текущей очереди.
        next_class_id: ID следующего занятия.

    Returns:
        ID очереди следующего занятия.
    """
    async with aiosqlite.connect(DB) as db:
        db.row_factory = aiosqlite.Row
        nq_cur = await db.execute("SELECT id FROM queues WHERE class_id=?", (next_class_id,))
        nq = await nq_cur.fetchone()
        if not nq:
            cur = await db.execute("INSERT INTO queues (class_id) VALUES (?)", (next_class_id,))
            nqid = cur.lastrowid
        else:
            nqid = nq["id"]

        unsub = await db.execute(
            "SELECT user_id FROM entries WHERE queue_id=? AND submitted=0", (qid,)
        )
        for row in await unsub.fetchall():
            await db.execute(
                "INSERT OR IGNORE INTO entries (queue_id,user_id) VALUES (?,?)",
                (nqid, row["user_id"])
            )
        await db.execute("UPDATE queues SET status='completed' WHERE id=?", (qid,))
        await db.commit()
        return nqid

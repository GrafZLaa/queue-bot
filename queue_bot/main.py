import asyncio
import json
import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from aiohttp import web
from aiogram import Bot, Dispatcher, F
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.filters import Command
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    BotCommand, CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup,
    KeyboardButton, MenuButtonWebApp, Message, ReplyKeyboardMarkup, WebAppInfo,
)
from dotenv import load_dotenv

from . import database as db
from .webapp_auth import WebAppAuthError, validate_init_data

load_dotenv()

TOKEN    = os.getenv("BOT_TOKEN", "")
PORT     = int(os.getenv("PORT", "3000"))
WEB_URL  = os.getenv("WEB_URL", f"http://localhost:{PORT}")
ALLOW_UNVERIFIED = os.getenv("ALLOW_UNVERIFIED_WEBAPP", "0") == "1"
ADMIN_IDS = [int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit()]
INDEX_HTML = Path(__file__).resolve().parent / "index.html"

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger(__name__)

dp = Dispatcher(storage=MemoryStorage())

_CAT_EMOJI  = {"good": "🟢", "middle": "🟡", "poor": "🔴"}
_CAT_LABEL  = {"good": "Добросовестный", "middle": "Средний", "poor": "Отстающий"}
_STAT_EMOJI = {"pending": "⏳", "open": "🟢", "closed": "🔴", "completed": "✅"}
_STAT_LABEL = {
    "pending":   "запись ещё не открыта",
    "open":      "запись открыта",
    "closed":    "очередь сформирована",
    "completed": "пара завершена",
}


# ─── keyboards ───────────────────────────────────────────────────────────────

def _reply_kb(tg_id: int) -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📅 Открыть Очереди", web_app=WebAppInfo(url=WEB_URL))],
            [KeyboardButton(text="📋 Моя позиция"), KeyboardButton(text="❓ Помощь")],
        ],
        resize_keyboard=True,
        is_persistent=True,
    )


def _ib(text: str, data: str) -> InlineKeyboardButton:
    return InlineKeyboardButton(text=text, callback_data=data)


def _kb(*rows: list[InlineKeyboardButton]) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=list(rows))


def _main_menu_kb(tg_id: int) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="📅 Открыть журнал занятий", web_app=WebAppInfo(url=WEB_URL))],
        [_ib("📊 Мой рейтинг", "my_rating"), _ib("🏆 Лидерборд", "leaderboard")],
    ]
    if tg_id in ADMIN_IDS:
        rows.append([_ib("⚙️ Админ-панель", "admin")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


# ─── formatting ──────────────────────────────────────────────────────────────

def _fmt_dt(value: str) -> str:
    try:
        return datetime.fromisoformat(value).strftime("%d.%m.%Y %H:%M")
    except ValueError:
        return value


def _class_times(cls: dict) -> tuple[str, str]:
    start = datetime.fromisoformat(cls["dt"])
    end   = db.class_end_time(cls["dt"], cls.get("duration_minutes"))
    return start.strftime("%H:%M"), end.strftime("%H:%M")


def _fmt_user(u: dict) -> str:
    cat = f"{_CAT_EMOJI[u['category']]} {_CAT_LABEL[u['category']]}"
    return (
        f"👤 *{u['full_name']}*\n"
        f"🎓 Группа: `{u.get('group_name') or '—'}`\n"
        f"⭐ Рейтинг: `{u['rating']}/100`  {cat}\n"
        f"✅ {u['on_time']}  ⏰ {u['late']}  ❌ {u['no_show']}"
    )


# ─── notifications ───────────────────────────────────────────────────────────

async def _notify_open(bot: Bot, class_id: int) -> None:
    cls = await db.get_class(class_id)
    if not cls:
        return
    users = await db.all_users(cls.get("group_name"))
    text = (
        f"🟢 *Открыта запись в очередь!*\n\n"
        f"📖 {cls['subject_name']}\n"
        f"📅 {_fmt_dt(cls['dt'])}\n"
        f"🚪 {cls.get('room') or '—'}\n\n"
        f"Нажмите «📅 Открыть Очереди» чтобы записаться."
    )
    for u in users:
        try:
            await bot.send_message(u["telegram_id"], text, parse_mode="Markdown")
        except Exception:
            pass


async def _notify_close(bot: Bot, queue_id: int, class_id: int) -> None:
    cls = await db.get_class(class_id)
    subj = cls["subject_name"] if cls else "Занятие"
    for entry in await db.queue_entries(queue_id):
        pos  = entry["position"] or "—"
        qcat = entry.get("q_category") or "middle"
        try:
            await bot.send_message(
                entry["telegram_id"],
                f"🔔 *Запись закрыта!*\n\n"
                f"📖 {subj}\n"
                f"Позиция: *{pos}*\n"
                f"Категория: {_CAT_EMOJI[qcat]} {_CAT_LABEL[qcat]}",
                parse_mode="Markdown",
            )
        except Exception:
            pass


# ─── background tasks ────────────────────────────────────────────────────────

async def _auto_open_task(bot: Bot) -> None:
    while True:
        await asyncio.sleep(60)
        try:
            now = datetime.now().isoformat()
            for row in await db.get_classes_to_auto_open(now):
                q = await db.queue_for_class(row["id"])
                if q and q["status"] == "pending":
                    await db.set_queue_status(q["id"], "open")
                    await _notify_open(bot, row["id"])
                    log.info("Auto-opened queue class_id=%s", row["id"])
        except Exception:
            log.exception("auto_open error")


async def _auto_close_task(bot: Bot) -> None:
    while True:
        await asyncio.sleep(60)
        try:
            now = datetime.now().isoformat()
            for row in await db.get_classes_to_auto_close(now):
                q = await db.queue_for_class(row["id"])
                if q and q["status"] == "open":
                    await db.randomize_queue(q["id"])
                    await _notify_close(bot, q["id"], row["id"])
                    log.info("Auto-closed queue class_id=%s", row["id"])
        except Exception:
            log.exception("auto_close error")


# ─── bot handlers ─────────────────────────────────────────────────────────────

@dp.message(Command("start"))
async def cmd_start(msg: Message) -> None:
    u = msg.from_user
    name = f"{u.first_name or ''} {u.last_name or ''}".strip() or u.username or "Студент"
    user = await db.ensure_user(u.id, u.username, name, "ИКБО-42-24")
    await msg.answer(
        f"👋 Привет, *{user['full_name']}*!\n\n"
        "Бот управляет очередями на сдачу практических работ.\n\n"
        "📅 Нажми «*Открыть Очереди*» чтобы увидеть расписание и записаться.",
        reply_markup=_reply_kb(u.id),
        parse_mode="Markdown",
    )


@dp.message(Command("myqueue"))
@dp.message(F.text == "📋 Моя позиция")
async def cmd_myqueue(msg: Message) -> None:
    user = await db.get_user_by_tg(msg.from_user.id)
    if not user:
        await msg.answer("Напишите /start.")
        return
    entries = await db.user_active_entries(user["id"])
    if not entries:
        await msg.answer("📭 Нет активных записей в очередях.")
        return
    lines = ["📋 *Ваши записи:*\n"]
    for e in entries:
        st = _STAT_EMOJI.get(e["queue_status"], "⏳")
        pos = f"позиция *{e['position']}*" if e.get("position") else "позиция не определена"
        lines.append(
            f"{st} *{e['subject_name']}*\n"
            f"📅 {_fmt_dt(e['dt'])} · 🚪 {e.get('room') or '—'}\n"
            f"📌 {pos}"
        )
    await msg.answer("\n\n".join(lines), parse_mode="Markdown")


@dp.message(Command("help"))
@dp.message(F.text == "❓ Помощь")
async def cmd_help(msg: Message) -> None:
    text = (
        "❓ *Помощь*\n\n"
        "*/start* — главное меню\n"
        "*/myqueue* — ваши записи\n"
        "*/help* — эта справка\n"
    )
    if msg.from_user.id in ADMIN_IDS:
        text += "\n_Администратор:_\n`/reseed` — принудительно обновить расписание\n"
    await msg.answer(text, parse_mode="Markdown")


@dp.message(Command("reseed"))
async def cmd_reseed(msg: Message) -> None:
    if msg.from_user.id not in ADMIN_IDS:
        return
    m = await msg.answer("⏳ Обновляю расписание...")
    result = await db.reseed_ikbo_42_24()
    await m.edit_text(
        f"✅ Расписание обновлено — {result['imported']} занятий.\n"
        f"Версия: {db._SCHEDULE_VERSION}",
    )


@dp.callback_query(F.data == "main_menu")
async def cb_main(cq: CallbackQuery) -> None:
    await cq.answer()
    await cq.message.edit_text(
        "📋 *Журнал очередей*",
        reply_markup=_main_menu_kb(cq.from_user.id),
        parse_mode="Markdown",
    )


@dp.callback_query(F.data == "my_rating")
async def cb_rating(cq: CallbackQuery) -> None:
    await cq.answer()
    user = await db.get_user_by_tg(cq.from_user.id)
    await cq.message.edit_text(
        _fmt_user(user) if user else "Напишите /start.",
        reply_markup=_kb([_ib("◀️ Назад", "main_menu")]),
        parse_mode="Markdown",
    )


@dp.callback_query(F.data == "leaderboard")
async def cb_leaderboard(cq: CallbackQuery) -> None:
    await cq.answer()
    users = (await db.all_users())[:20]
    medals = {1: "🥇", 2: "🥈", 3: "🥉"}
    lines = ["🏆 *Топ студентов*\n"]
    for i, u in enumerate(users, 1):
        g = f" `{u['group_name']}`" if u.get("group_name") else ""
        lines.append(
            f"{medals.get(i, f'`{i:>2}.`')} {_CAT_EMOJI[u['category']]} "
            f"*{u['full_name']}*{g} — {u['rating']}/100"
        )
    await cq.message.edit_text(
        "\n".join(lines),
        reply_markup=_kb([_ib("◀️ Назад", "main_menu")]),
        parse_mode="Markdown",
    )


@dp.callback_query(F.data == "admin")
async def cb_admin(cq: CallbackQuery) -> None:
    if cq.from_user.id not in ADMIN_IDS:
        await cq.answer("Нет доступа", show_alert=True)
        return
    await cq.answer()
    await cq.message.edit_text(
        "⚙️ *Админ-панель*",
        reply_markup=_kb(
            [_ib("👥 Студенты", "adm_users")],
            [_ib("◀️ Назад", "main_menu")],
        ),
        parse_mode="Markdown",
    )


@dp.callback_query(F.data == "adm_users")
async def cb_adm_users(cq: CallbackQuery) -> None:
    if cq.from_user.id not in ADMIN_IDS:
        await cq.answer("Нет доступа", show_alert=True)
        return
    await cq.answer()
    users = await db.all_users()
    rows = [
        [_ib(f"{_CAT_EMOJI[u['category']]} {u['full_name'][:22]} ({u['rating']})", f"adm_user_{u['id']}")]
        for u in users
    ]
    rows.append([_ib("◀️ Назад", "admin")])
    await cq.message.edit_text("👥 *Студенты*", reply_markup=_kb(*rows), parse_mode="Markdown")


@dp.callback_query(F.data.startswith("adm_user_"))
async def cb_adm_user(cq: CallbackQuery) -> None:
    if cq.from_user.id not in ADMIN_IDS:
        await cq.answer("Нет доступа", show_alert=True)
        return
    await cq.answer()
    uid = int(cq.data.split("_")[2])
    user = await db.get_user(uid)
    await cq.message.edit_text(
        _fmt_user(user) if user else "Не найден",
        reply_markup=_kb(
            [_ib("⭐ Рейтинг +10", f"adm_rate_{uid}_up"), _ib("⭐ Рейтинг -10", f"adm_rate_{uid}_dn")],
            [_ib("◀️ Назад", "adm_users")],
        ),
        parse_mode="Markdown",
    )


@dp.callback_query(F.data.startswith("adm_rate_"))
async def cb_adm_rate(cq: CallbackQuery) -> None:
    if cq.from_user.id not in ADMIN_IDS:
        await cq.answer("Нет доступа", show_alert=True)
        return
    parts = cq.data.split("_")
    uid, direction = int(parts[2]), parts[3]
    user = await db.get_user(uid)
    if user:
        new_r = max(0, min(100, user["rating"] + (10 if direction == "up" else -10)))
        await db.set_rating(uid, new_r)
        user = await db.get_user(uid)
    await cq.answer(f"Рейтинг: {user['rating']}/100" if user else "Не найден", show_alert=True)
    await cq.message.edit_text(
        _fmt_user(user) if user else "Не найден",
        reply_markup=_kb(
            [_ib("⭐ Рейтинг +10", f"adm_rate_{uid}_up"), _ib("⭐ Рейтинг -10", f"adm_rate_{uid}_dn")],
            [_ib("◀️ Назад", "adm_users")],
        ),
        parse_mode="Markdown",
    )


@dp.callback_query(F.data.startswith("adm_openq_"))
async def cb_openq(cq: CallbackQuery) -> None:
    if cq.from_user.id not in ADMIN_IDS:
        await cq.answer("Нет доступа", show_alert=True)
        return
    _, _, qid, cid = cq.data.split("_")
    await db.set_queue_status(int(qid), "open")
    await _notify_open(cq.bot, int(cid))
    await cq.answer("Запись открыта", show_alert=True)


@dp.callback_query(F.data.startswith("adm_closeq_"))
async def cb_closeq(cq: CallbackQuery) -> None:
    if cq.from_user.id not in ADMIN_IDS:
        await cq.answer("Нет доступа", show_alert=True)
        return
    _, _, qid, cid = cq.data.split("_")
    qid_int = int(qid)
    await db.randomize_queue(qid_int)
    await _notify_close(cq.bot, qid_int, int(cid))
    await cq.answer("Очередь сформирована", show_alert=True)


@dp.callback_query(F.data == "noop")
async def cb_noop(cq: CallbackQuery) -> None:
    await cq.answer()


# ─── web helpers ─────────────────────────────────────────────────────────────

def _cors(resp: web.StreamResponse) -> web.StreamResponse:
    resp.headers["Access-Control-Allow-Origin"]  = "*"
    resp.headers["Access-Control-Allow-Headers"] = "Content-Type, X-Telegram-Init-Data"
    resp.headers["Access-Control-Allow-Methods"] = "GET,POST,OPTIONS"
    return resp


def _json(data: dict, status: int = 200) -> web.Response:
    return _cors(web.json_response(data, status=status))


async def _get_json(req: web.Request) -> dict:
    try:
        return await req.json()
    except Exception:
        return {}


def _init_data(req: web.Request, body: Optional[dict] = None) -> str:
    if req.headers.get("X-Telegram-Init-Data"):
        return req.headers["X-Telegram-Init-Data"]
    if body and body.get("init_data"):
        return body["init_data"]
    return req.rel_url.query.get("init_data", "")


async def _tg_profile(req: web.Request, body: Optional[dict] = None,
                      required: bool = False) -> Optional[dict]:
    init = _init_data(req, body)
    if init:
        try:
            return validate_init_data(init, TOKEN)
        except WebAppAuthError as e:
            if required:
                raise web.HTTPUnauthorized(
                    text=json.dumps({"error": str(e)}, ensure_ascii=False),
                    content_type="application/json",
                )
            return None
    if ALLOW_UNVERIFIED:
        uid = (body and body.get("user_id")) or req.rel_url.query.get("user_id")
        if uid:
            return {"id": int(uid), "first_name": body.get("name", "Dev") if body else "Dev"}
        if ADMIN_IDS:
            return {"id": ADMIN_IDS[0], "first_name": "Dev"}
    if required:
        raise web.HTTPUnauthorized(
            text=json.dumps({"error": "Требуется Telegram WebApp"}, ensure_ascii=False),
            content_type="application/json",
        )
    return None


async def _db_user(req: web.Request, body: Optional[dict] = None,
                   required: bool = False) -> Optional[dict]:
    profile = await _tg_profile(req, body, required=required)
    if not profile:
        return None
    return await db.get_user_by_tg(int(profile["id"]))


async def _require_admin(req: web.Request, body: Optional[dict] = None) -> dict:
    profile = await _tg_profile(req, body, required=True)
    if int(profile["id"]) not in ADMIN_IDS:
        raise web.HTTPForbidden(
            text=json.dumps({"error": "Нет доступа"}, ensure_ascii=False),
            content_type="application/json",
        )
    return profile


def _class_payload(cls: dict, queue: Optional[dict], entries: list,
                   user_entry: Optional[dict] = None) -> dict:
    start, end = _class_times(cls)
    return {
        "id":             cls["id"],
        "subject_id":     cls["subject_id"],
        "subject_name":   cls["subject_name"],
        "group_name":     cls.get("group_name") or "",
        "teacher":        cls.get("teacher") or "",
        "room":           cls.get("room") or "",
        "type":           cls.get("class_type", "ПР"),
        "date":           datetime.fromisoformat(cls["dt"]).strftime("%Y-%m-%d"),
        "time_start":     start,
        "time_end":       end,
        "duration_minutes": cls.get("duration_minutes") or db.DEFAULT_DURATION,
        "queue_id":       queue["id"] if queue else None,
        "queue_status":   queue["status"] if queue else "no_queue",
        "queue_count":    len(entries),
        "opens_at":       cls.get("opens_at"),
        "closes_at":      cls.get("closes_at"),
        "user_in_queue":  user_entry is not None,
        "user_position":  user_entry["position"] if user_entry and user_entry.get("position") else None,
        "user_q_category": user_entry.get("q_category") if user_entry else None,
    }


# ─── API handlers ─────────────────────────────────────────────────────────────

async def api_index(req: web.Request) -> web.Response:
    try:
        return web.Response(text=INDEX_HTML.read_text(encoding="utf-8"), content_type="text/html")
    except FileNotFoundError:
        return web.Response(text="index.html not found", status=404)


async def api_me(req: web.Request) -> web.Response:
    profile = await _tg_profile(req)
    if not profile:
        return _json({"authenticated": False, "registered": False, "is_admin": False})
    user = await db.get_user_by_tg(int(profile["id"]))
    return _json({
        "authenticated": True,
        "registered":    bool(user),
        "is_admin":      int(profile["id"]) in ADMIN_IDS,
        "telegram_user": profile,
        "user":          user,
    })


async def api_register(req: web.Request) -> web.Response:
    body    = await _get_json(req)
    profile = await _tg_profile(req, body, required=True)
    tg_id   = int(profile["id"])
    name    = (body.get("name") or "").strip()
    if len(name.split()) < 2:
        first = profile.get("first_name", "")
        last  = profile.get("last_name", "")
        name  = f"{first} {last}".strip() or name
    user = await db.ensure_user(tg_id, profile.get("username"), name, "ИКБО-42-24")
    await db.set_name_confirmed(tg_id)
    user["name_confirmed"] = 1
    return _json({"status": "ok", "user": user})


async def api_schedule_month(req: web.Request) -> web.Response:
    try:
        year  = int(req.rel_url.query["year"])
        month = int(req.rel_url.query["month"])
    except (KeyError, ValueError):
        return _json({"counts": {}})
    user = await _db_user(req)
    counts = await db.class_counts_for_month(year, month, user.get("group_name") if user else None)
    return _json({"counts": counts})


async def api_schedule(req: web.Request) -> web.Response:
    date_str = req.rel_url.query.get("date", "")
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        return _json({"classes": []})

    user = await _db_user(req)
    group = user.get("group_name") if user else None
    classes = await db.classes_for_date(date_str, group)

    queues: dict[int, Optional[dict]] = {}
    entries_map: dict[int, list] = {}
    for cls in classes:
        q = await db.queue_for_class(cls["id"])
        queues[cls["id"]] = q
        if q:
            entries_map[q["id"]] = await db.queue_entries(q["id"])

    queue_ids = [q["id"] for q in queues.values() if q]
    user_entries = await db.user_entries_for_queues(user["id"], queue_ids) if user and queue_ids else {}

    result = []
    for cls in classes:
        q   = queues.get(cls["id"])
        ent = entries_map.get(q["id"], []) if q else []
        ue  = user_entries.get(q["id"]) if q else None
        result.append(_class_payload(cls, q, ent, ue))
    return _json({"classes": result})


async def api_queue_detail(req: web.Request) -> web.Response:
    cid  = int(req.match_info["class_id"])
    user = await _db_user(req)
    cls  = await db.get_class(cid)
    if not cls:
        return _json({"error": "not found"}, status=404)
    q    = await db.queue_for_class(cid)
    ents = await db.queue_entries(q["id"]) if q else []
    assigns = await db.assignments_for_class(cid)
    payload = _class_payload(cls, q, ents)
    payload.update({
        "class_id": cid,
        "queue": {
            "id":     q["id"] if q else None,
            "status": q["status"] if q else "pending",
            "entries": [
                {
                    "user_id":    e["user_id"],
                    "telegram_id": e["telegram_id"],
                    "full_name":  e["full_name"],
                    "group_name": e.get("group_name") or "",
                    "position":   e["position"],
                    "q_category": e["q_category"],
                    "user_cat":   e["user_cat"],
                    "submitted":  bool(e["submitted"]),
                    "on_time":    bool(e["on_time"]),
                }
                for e in ents
            ],
        },
        "assignments": [
            {"title": a["title"], "description": a.get("description"),
             "deadline": a.get("deadline"), "url": a.get("url")}
            for a in assigns
        ],
    })
    return _json(payload)


async def api_join(req: web.Request) -> web.Response:
    cid  = int(req.match_info["class_id"])
    body = await _get_json(req)
    user = await _db_user(req, body, required=True)
    if not user:
        return _json({"error": "Напишите /start боту"}, status=400)
    q = await db.queue_for_class(cid)
    if not q or q["status"] != "open":
        return _json({"error": "Запись закрыта"}, status=400)
    if await db.is_in_queue(q["id"], user["id"]):
        return _json({"status": "already_in"})
    await db.join_queue(q["id"], user["id"])
    return _json({"status": "ok"})


async def api_leave(req: web.Request) -> web.Response:
    cid  = int(req.match_info["class_id"])
    body = await _get_json(req)
    user = await _db_user(req, body, required=True)
    if not user:
        return _json({"error": "not found"}, status=400)
    q = await db.queue_for_class(cid)
    if q:
        await db.leave_queue(q["id"], user["id"])
    return _json({"status": "ok"})


async def api_reseed(req: web.Request) -> web.Response:
    body = await _get_json(req)
    await _require_admin(req, body)
    result = await db.reseed_ikbo_42_24()
    return _json(result)


async def api_admin_subjects(req: web.Request) -> web.Response:
    body = await _get_json(req) if req.method == "POST" else None
    await _require_admin(req, body)
    if req.method == "POST":
        sid = await db.add_subject((body or {}).get("name", "").strip(), (body or {}).get("group_name"))
        return _json({"status": "ok", "id": sid})
    return _json({"subjects": await db.all_subjects()})


async def api_admin_classes(req: web.Request) -> web.Response:
    body = await _get_json(req)
    await _require_admin(req, body)
    dt = body.get("dt") or f"{body.get('date','')!s}T{body.get('time','00:00')!s}:00"
    try:
        datetime.fromisoformat(dt)
    except ValueError:
        return _json({"error": "Некорректная дата"}, status=400)
    cid = await db.add_class(
        int(body["subject_id"]), dt,
        body.get("room"), body.get("teacher"),
        int(body.get("duration_minutes") or db.DEFAULT_DURATION),
    )
    return _json({"status": "ok", "id": cid})


async def api_admin_queue_action(req: web.Request) -> web.Response:
    body   = await _get_json(req)
    await _require_admin(req, body)
    qid    = int(req.match_info["queue_id"])
    action = req.match_info["action"]
    bot    = req.app.get("bot")
    if action == "open":
        await db.set_queue_status(qid, "open")
        if bot:
            q = await db.get_queue(qid)
            if q:
                await _notify_open(bot, q["class_id"])
    elif action == "close":
        await db.randomize_queue(qid)
        if bot:
            q = await db.get_queue(qid)
            if q:
                await _notify_close(bot, qid, q["class_id"])
    elif action in ("pending", "completed"):
        await db.set_queue_status(qid, action)
    else:
        return _json({"error": "unknown action"}, status=400)
    return _json({"status": "ok"})


async def api_admin_mark(req: web.Request) -> web.Response:
    body = await _get_json(req)
    await _require_admin(req, body)
    await db.mark_submission(int(body["queue_id"]), int(body["user_id"]), body["kind"])
    return _json({"status": "ok"})


async def api_options(req: web.Request) -> web.Response:
    return _cors(web.Response())


# ─── app startup ──────────────────────────────────────────────────────────────

def _create_bot() -> Bot:
    if not TOKEN:
        raise RuntimeError("BOT_TOKEN is not set")
    proxy = os.getenv("HTTPS_PROXY") or os.getenv("https_proxy")
    if proxy:
        from aiogram.client.session.aiohttp import AiohttpSession
        return Bot(token=TOKEN, session=AiohttpSession(proxy=proxy))
    return Bot(token=TOKEN)


async def main() -> None:
    bot = _create_bot()

    await db.init()
    result = await db.ensure_seed_current()
    log.info("Schedule seed: %s", result)

    try:
        await bot.set_chat_menu_button(
            menu_button=MenuButtonWebApp(text="📅 Очереди", web_app=WebAppInfo(url=WEB_URL))
        )
    except Exception:
        log.warning("Could not set menu button")

    try:
        await bot.set_my_commands([
            BotCommand(command="start",   description="Запустить бота"),
            BotCommand(command="myqueue", description="Мои записи в очередях"),
            BotCommand(command="help",    description="Помощь"),
        ])
    except Exception:
        log.warning("Could not set commands")

    app = web.Application()
    app["bot"] = bot
    app.router.add_get("/",                                    api_index)
    app.router.add_get("/api/me",                              api_me)
    app.router.add_post("/api/register",                       api_register)
    app.router.add_get("/api/schedule/month",                  api_schedule_month)
    app.router.add_get("/api/schedule",                        api_schedule)
    app.router.add_get("/api/queue/{class_id}",                api_queue_detail)
    app.router.add_post("/api/queue/{class_id}/join",          api_join)
    app.router.add_post("/api/queue/{class_id}/leave",         api_leave)
    app.router.add_post("/api/reseed",                         api_reseed)
    app.router.add_get("/api/admin/subjects",                  api_admin_subjects)
    app.router.add_post("/api/admin/subjects",                 api_admin_subjects)
    app.router.add_post("/api/admin/classes",                  api_admin_classes)
    app.router.add_post("/api/admin/queue/{queue_id}/{action}", api_admin_queue_action)
    app.router.add_post("/api/admin/mark",                     api_admin_mark)
    app.router.add_route("OPTIONS", "/{path:.*}",              api_options)

    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()
    log.info("Server on port %s | WEB_URL=%s | schedule v%s", PORT, WEB_URL, db._SCHEDULE_VERSION)

    asyncio.create_task(_auto_open_task(bot))
    asyncio.create_task(_auto_close_task(bot))

    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


def run() -> None:
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())


if __name__ == "__main__":
    run()

"""Application entry point for the Queue Bot package."""

import asyncio
import json
import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import aiohttp
from aiohttp import web
from aiogram import Bot, Dispatcher, F
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    BotCommand,
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    MenuButtonWebApp,
    Message,
    ReplyKeyboardMarkup,
    WebAppInfo,
)
from dotenv import load_dotenv

from . import database as db
from .webapp_auth import WebAppAuthError, validate_init_data

load_dotenv()

TOKEN = os.getenv("BOT_TOKEN", "")
ADMIN_IDS = [int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit()]
PORT = int(os.getenv("PORT", "3000"))
WEB_URL = os.getenv("WEB_URL", f"http://localhost:{PORT}")
ALLOW_UNVERIFIED_WEBAPP = os.getenv("ALLOW_UNVERIFIED_WEBAPP", "0") == "1"
PACKAGE_DIR = Path(__file__).resolve().parent
INDEX_HTML = PACKAGE_DIR / "index.html"

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger(__name__)

dp = Dispatcher(storage=MemoryStorage())

CAT_EMOJI = {"good": "🟢", "middle": "🟡", "poor": "🔴"}
CAT_LABEL = {"good": "Добросовестный", "middle": "Средний", "poor": "Отстающий"}
STAT_EMOJI = {"pending": "⏳", "open": "🟢", "closed": "🔴", "completed": "✅"}
STAT_LABEL = {
    "pending": "запись ещё не открыта",
    "open": "запись открыта",
    "closed": "очередь сформирована",
    "completed": "пара завершена",
}


class Form(StatesGroup):
    """FSM states for admin data entry."""

    subj_name = State()
    subj_group = State()
    cls_dt = State()
    cls_duration = State()
    cls_room = State()
    cls_teacher = State()
    asgn_title = State()
    asgn_desc = State()
    asgn_dl = State()
    asgn_url = State()
    edit_rating = State()
    edit_name = State()
    edit_group = State()


def kb(*rows: list[InlineKeyboardButton]) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=list(rows))


def btn(text: str, data: str) -> InlineKeyboardButton:
    return InlineKeyboardButton(text=text, callback_data=data)


def parse_dt(text: str) -> Optional[str]:
    """Parse common Russian class date formats into ISO datetime."""
    for fmt in ("%d.%m.%Y %H:%M", "%d.%m %H:%M", "%Y-%m-%d %H:%M"):
        try:
            parsed = datetime.strptime(text.strip(), fmt)
            if parsed.year == 1900:
                parsed = parsed.replace(year=datetime.now().year)
            return parsed.isoformat()
        except ValueError:
            continue
    return None


def parse_duration(text: str) -> Optional[int]:
    value = text.strip()
    if value == "-":
        return db.DEFAULT_DURATION_MINUTES
    try:
        minutes = int(value)
    except ValueError:
        return None
    if minutes < 15 or minutes > 360:
        return None
    return minutes


def fmt_dt(value: str) -> str:
    try:
        return datetime.fromisoformat(value).strftime("%d.%m.%Y %H:%M")
    except ValueError:
        return value


def class_times(cls: dict) -> tuple[str, str]:
    start = datetime.fromisoformat(cls["dt"])
    end = db.class_end_time(cls["dt"], cls.get("duration_minutes"))
    return start.strftime("%H:%M"), end.strftime("%H:%M")


def fmt_class_line(cls: dict) -> str:
    start, end = class_times(cls)
    return f"{fmt_dt(cls['dt'])} - {end} ({cls.get('duration_minutes') or 90} мин.)"


def fmt_user(user: dict) -> str:
    cat = f"{CAT_EMOJI[user['category']]} {CAT_LABEL[user['category']]}"
    group = user.get("group_name") or "не указана"
    return (
        f"👤 *{user['full_name']}*\n"
        f"🎓 Группа: `{group}`\n"
        f"⭐ Рейтинг: `{user['rating']}/100`\n"
        f"Категория: {cat}\n"
        f"✅ Вовремя: {user['on_time']}  ⏰ Поздно: {user['late']}  ❌ Не сдал: {user['no_show']}"
    )


async def notify_queue_open(bot: Bot, class_id: int) -> None:
    """Notify all students in a class group that the queue has opened."""
    cls = await db.get_class(class_id)
    if not cls:
        return
    users = await db.all_users(cls.get("group_name"))
    start_str = fmt_dt(cls["dt"])
    for u in users:
        try:
            await bot.send_message(
                u["telegram_id"],
                f"🟢 *Открыта запись в очередь!*\n\n"
                f"📖 {cls['subject_name']}\n"
                f"📅 {start_str}\n"
                f"🚪 {cls.get('room') or '—'}\n\n"
                f"Откройте журнал занятий, чтобы записаться.",
                parse_mode="Markdown",
            )
        except Exception:
            log.exception("Cannot notify student tg_id=%s", u["telegram_id"])


def main_menu_kb(tg_id: int) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="📅 Открыть журнал занятий", web_app=WebAppInfo(url=WEB_URL))],
        [btn("📊 Мой рейтинг", "my_rating"), btn("🏆 Лидерборд", "leaderboard")],
    ]
    if tg_id in ADMIN_IDS:
        rows.append([btn("⚙️ Админ-панель", "admin")])
    return kb(*rows)


def main_reply_kb(tg_id: int) -> ReplyKeyboardMarkup:
    buttons: list[list[KeyboardButton]] = [
        [KeyboardButton(text="📅 Открыть Очереди", web_app=WebAppInfo(url=WEB_URL))],
        [KeyboardButton(text="📋 Моя позиция"), KeyboardButton(text="❓ Помощь")],
    ]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True, is_persistent=True)


def is_admin_id(tg_id: int) -> bool:
    return tg_id in ADMIN_IDS


def is_admin(cq: CallbackQuery) -> bool:
    return is_admin_id(cq.from_user.id)


@dp.message(Command("start"))
async def cmd_start(msg: Message, state: FSMContext) -> None:
    tg_user = msg.from_user
    first = tg_user.first_name or ""
    last = tg_user.last_name or ""
    name = f"{first} {last}".strip() or tg_user.username or "Пользователь"
    user = await db.ensure_user(tg_user.id, tg_user.username, name, "ИКБО-42-24")
    await msg.answer(
        f"👋 Привет, *{user['full_name']}*!\n\n"
        "Я помогаю управлять очередями на сдачу практических работ в *РТУ МИРЭА*.\n\n"
        "📅 Показываю расписание занятий твоей группы\n"
        "🎫 Записываю в очередь на сдачу\n"
        "📊 Веду рейтинг — чем лучше сдаёшь, тем выше место в очереди\n"
        "🔔 Уведомляю, когда открывается запись\n\n"
        "Нажми *«📅 Открыть Очереди»* внизу, чтобы начать.",
        reply_markup=main_reply_kb(tg_user.id),
        parse_mode="Markdown",
    )


async def _send_myqueue(msg: Message) -> None:
    user = await db.get_user_by_tg(msg.from_user.id)
    if not user:
        await msg.answer("Напишите /start для регистрации.")
        return
    entries = await db.user_active_entries(user["id"])
    if not entries:
        await msg.answer("📭 У вас нет активных записей в очереди.\n\nОткройте журнал занятий, чтобы записаться.")
        return
    lines = ["📋 *Ваши активные записи:*\n"]
    for entry in entries:
        status_emoji = STAT_EMOJI.get(entry["queue_status"], "⏳")
        pos_text = f"позиция *{entry['position']}*" if entry.get("position") else "позиция не определена"
        cat = entry.get("q_category") or "middle"
        cat_text = f"{CAT_EMOJI[cat]} {CAT_LABEL[cat]}"
        start = fmt_dt(entry["dt"])
        room = entry.get("room") or "—"
        lines.append(
            f"{status_emoji} *{entry['subject_name']}*\n"
            f"📅 {start} · 🚪 {room}\n"
            f"📌 {pos_text} · {cat_text}"
        )
    await msg.answer("\n\n".join(lines), parse_mode="Markdown")


@dp.message(Command("myqueue"))
@dp.message(F.text == "📋 Моя позиция")
async def cmd_myqueue(msg: Message) -> None:
    await _send_myqueue(msg)


async def _help_text(msg: Message) -> None:
    text = (
        "❓ *Помощь*\n\n"
        "*/start* — главное меню\n"
        "*/myqueue* — ваши записи в очередях\n"
        "*/help* — эта справка\n"
    )
    if is_admin_id(msg.from_user.id):
        text += (
            "\n_Только для администратора:_\n"
            "`/reseed` — сбросить и перезагрузить расписание ИКБО-42-24\n"
        )
    await msg.answer(text, parse_mode="Markdown")


@dp.message(Command("help"))
@dp.message(F.text == "❓ Помощь")
async def cmd_help(msg: Message) -> None:
    await _help_text(msg)



@dp.message(Command("seed"))
async def cmd_seed(msg: Message) -> None:
    if not is_admin_id(msg.from_user.id):
        return
    progress = await msg.answer("⏳ Заполняю расписание ИКБО-42-24...")
    result = await db.seed_ikbo_42_24()
    if result["status"] == "already_seeded":
        await progress.edit_text(
            f"ℹ️ Расписание уже загружено ({result['count']} предметов).\n"
            f"Используйте /reseed для полного сброса.",
        )
    else:
        await progress.edit_text(
            f"✅ Расписание ИКБО-42-24 загружено!\n"
            f"Создано занятий: *{result['imported']}*\n\n"
            f"Охват: недели 15–18 (май–июнь 2026)",
            parse_mode="Markdown",
        )


@dp.message(Command("reseed"))
async def cmd_reseed(msg: Message) -> None:
    if not is_admin_id(msg.from_user.id):
        return
    progress = await msg.answer("⏳ Сброс и повторная загрузка расписания ИКБО-42-24...")
    result = await db.reseed_ikbo_42_24()
    await progress.edit_text(
        f"✅ Расписание ИКБО-42-24 обновлено!\n"
        f"Создано занятий: *{result['imported']}*\n\n"
        f"Охват: недели 15–18 (май–июнь 2026)",
        parse_mode="Markdown",
    )


async def auto_open_queues_task(bot: Bot) -> None:
    """Auto-open queues at their opens_at time (previous class start or 90 min before)."""
    while True:
        await asyncio.sleep(60)
        try:
            now_iso = datetime.now().isoformat()
            for row in await db.get_classes_to_auto_open(now_iso):
                queue = await db.queue_for_class(row["id"])
                if queue and queue["status"] == "pending":
                    await db.set_queue_status(queue["id"], "open")
                    await notify_queue_open(bot, row["id"])
                    log.info("Auto-opened queue for class_id=%s", row["id"])
        except Exception:
            log.exception("Error in auto_open_queues_task")


async def auto_close_queues_task(bot: Bot) -> None:
    """Auto-close and randomize queues 10 minutes before each class."""
    while True:
        await asyncio.sleep(60)
        try:
            now_iso = datetime.now().isoformat()
            for row in await db.get_classes_to_auto_close(now_iso):
                queue = await db.queue_for_class(row["id"])
                if queue and queue["status"] == "open":
                    await db.randomize_queue(queue["id"])
                    log.info("Auto-closed queue for class_id=%s", row["id"])
                    for entry in await db.queue_entries(queue["id"]):
                        try:
                            pos = entry["position"] or "—"
                            qcat = entry.get("q_category") or "middle"
                            cls = await db.get_class(row["id"])
                            subj = cls["subject_name"] if cls else "Занятие"
                            await bot.send_message(
                                entry["telegram_id"],
                                f"🔔 *Запись закрыта!*\n\n"
                                f"📖 {subj}\n"
                                f"Позиция: *{pos}*\n"
                                f"Категория: {CAT_EMOJI[qcat]} {CAT_LABEL[qcat]}",
                                parse_mode="Markdown",
                            )
                        except Exception:
                            pass
        except Exception:
            log.exception("Error in auto_close_queues_task")


@dp.callback_query(F.data == "main_menu")
async def cb_main(cq: CallbackQuery) -> None:
    await cq.answer()
    await cq.message.edit_text(
        "📋 *Журнал очередей*",
        reply_markup=main_menu_kb(cq.from_user.id),
        parse_mode="Markdown",
    )


@dp.callback_query(F.data == "my_rating")
async def cb_rating(cq: CallbackQuery) -> None:
    await cq.answer()
    user = await db.get_user_by_tg(cq.from_user.id)
    await cq.message.edit_text(
        fmt_user(user) if user else "Напишите /start для регистрации.",
        reply_markup=kb([btn("◀️ Назад", "main_menu")]),
        parse_mode="Markdown",
    )


@dp.callback_query(F.data == "leaderboard")
async def cb_leaderboard(cq: CallbackQuery) -> None:
    await cq.answer()
    users = (await db.all_users())[:20]
    medals = {1: "🥇", 2: "🥈", 3: "🥉"}
    lines = ["🏆 *Топ студентов*\n"]
    for i, user in enumerate(users, 1):
        group = f" `{user['group_name']}`" if user.get("group_name") else ""
        lines.append(
            f"{medals.get(i, f'`{i:>2}.`')} {CAT_EMOJI[user['category']]} "
            f"*{user['full_name']}*{group} - {user['rating']}/100"
        )
    await cq.message.edit_text(
        "\n".join(lines),
        reply_markup=kb([btn("◀️ Назад", "main_menu")]),
        parse_mode="Markdown",
    )


@dp.callback_query(F.data == "admin")
async def cb_admin(cq: CallbackQuery) -> None:
    if not is_admin(cq):
        await cq.answer("Нет доступа", show_alert=True)
        return
    await cq.answer()
    await cq.message.edit_text(
        "⚙️ *Админ-панель*",
        reply_markup=kb(
            [btn("📚 Предметы и пары", "adm_subjects")],
            [btn("👥 Студенты", "adm_users")],
            [btn("◀️ Назад", "main_menu")],
        ),
        parse_mode="Markdown",
    )


async def render_subjects(message: Message) -> None:
    subjects = await db.all_subjects()
    rows = []
    for subject in subjects:
        group = f" [{subject['group_name']}]" if subject.get("group_name") else ""
        rows.append(
            [
                btn(f"📖 {subject['name']}{group}", f"adm_subj_{subject['id']}"),
                btn("🗑", f"adm_delsubj_{subject['id']}"),
            ]
        )
    rows += [[btn("➕ Добавить предмет", "adm_newsubj")], [btn("◀️ Назад", "admin")]]
    await message.edit_text("📚 *Предметы*", reply_markup=kb(*rows), parse_mode="Markdown")


@dp.callback_query(F.data == "adm_subjects")
async def cb_adm_subjects(cq: CallbackQuery) -> None:
    if not is_admin(cq):
        await cq.answer("Нет доступа", show_alert=True)
        return
    await cq.answer()
    await render_subjects(cq.message)


@dp.callback_query(F.data == "adm_newsubj")
async def cb_adm_newsubj(cq: CallbackQuery, state: FSMContext) -> None:
    if not is_admin(cq):
        await cq.answer("Нет доступа", show_alert=True)
        return
    await cq.answer()
    await state.set_state(Form.subj_name)
    await cq.message.edit_text(
        "Название предмета:",
        reply_markup=kb([btn("❌ Отмена", "adm_subjects")]),
    )


@dp.message(Form.subj_name)
async def fsm_subj_name(msg: Message, state: FSMContext) -> None:
    await state.update_data(name=msg.text.strip())
    await state.set_state(Form.subj_group)
    await msg.answer("Группа предмета или `-`, если предмет общий:", parse_mode="Markdown")


@dp.message(Form.subj_group)
async def fsm_subj_group(msg: Message, state: FSMContext) -> None:
    data = await state.get_data()
    group = None if msg.text.strip() == "-" else msg.text.strip().upper()
    await db.add_subject(data["name"], group)
    await state.clear()
    await msg.answer("✅ Предмет добавлен.", reply_markup=kb([btn("📚 Предметы", "adm_subjects")]))


@dp.callback_query(F.data.startswith("adm_delsubj_"))
async def cb_delsubj(cq: CallbackQuery) -> None:
    if not is_admin(cq):
        await cq.answer("Нет доступа", show_alert=True)
        return
    await db.delete_subject(int(cq.data.split("_")[2]))
    await cq.answer("Удалено", show_alert=True)
    await render_subjects(cq.message)


async def render_subject_detail(message: Message, subject_id: int) -> None:
    subject = await db.get_subject(subject_id)
    if not subject:
        await message.edit_text("Предмет не найден.", reply_markup=kb([btn("◀️ Назад", "adm_subjects")]))
        return
    classes = await db.classes_for_subject(subject_id)
    rows = []
    for cls in classes:
        queue = await db.queue_for_class(cls["id"])
        status = STAT_EMOJI.get(queue["status"] if queue else "pending", "⏳")
        start, end = class_times(cls)
        rows.append(
            [
                btn(f"{status} {fmt_dt(cls['dt'])} - {end} | {cls['room'] or '?'}", f"adm_clsd_{cls['id']}"),
                btn("🗑", f"adm_delcls_{cls['id']}"),
            ]
        )
    rows += [[btn("➕ Добавить пару", f"adm_addcls_{subject_id}")], [btn("◀️ Назад", "adm_subjects")]]
    group = f"\nГруппа: `{subject['group_name']}`" if subject.get("group_name") else "\nПредмет общий"
    await message.edit_text(
        f"📖 *{subject['name']}*{group}",
        reply_markup=kb(*rows),
        parse_mode="Markdown",
    )


@dp.callback_query(F.data.startswith("adm_subj_"))
async def cb_adm_subj(cq: CallbackQuery) -> None:
    if not is_admin(cq):
        await cq.answer("Нет доступа", show_alert=True)
        return
    await cq.answer()
    await render_subject_detail(cq.message, int(cq.data.split("_")[2]))


@dp.callback_query(F.data.startswith("adm_addcls_"))
async def cb_addcls(cq: CallbackQuery, state: FSMContext) -> None:
    if not is_admin(cq):
        await cq.answer("Нет доступа", show_alert=True)
        return
    subject_id = int(cq.data.split("_")[2])
    await cq.answer()
    await state.update_data(subject_id=subject_id)
    await state.set_state(Form.cls_dt)
    await cq.message.edit_text(
        "Дата и время пары: `ДД.ММ.ГГГГ ЧЧ:ММ`",
        reply_markup=kb([btn("❌ Отмена", f"adm_subj_{subject_id}")]),
        parse_mode="Markdown",
    )


@dp.message(Form.cls_dt)
async def fsm_cls_dt(msg: Message, state: FSMContext) -> None:
    dt = parse_dt(msg.text)
    if not dt:
        await msg.answer("Формат: `15.03.2026 10:40`", parse_mode="Markdown")
        return
    await state.update_data(dt=dt)
    await state.set_state(Form.cls_duration)
    await msg.answer("Длительность в минутах или `-` для стандартных 90:", parse_mode="Markdown")


@dp.message(Form.cls_duration)
async def fsm_cls_duration(msg: Message, state: FSMContext) -> None:
    duration = parse_duration(msg.text)
    if duration is None:
        await msg.answer("Введите число от 15 до 360 или `-`.", parse_mode="Markdown")
        return
    await state.update_data(duration_minutes=duration)
    await state.set_state(Form.cls_room)
    await msg.answer("Аудитория или `-`:", parse_mode="Markdown")


@dp.message(Form.cls_room)
async def fsm_cls_room(msg: Message, state: FSMContext) -> None:
    room = None if msg.text.strip() == "-" else msg.text.strip()
    await state.update_data(room=room)
    await state.set_state(Form.cls_teacher)
    await msg.answer("Преподаватель или `-`:", parse_mode="Markdown")


@dp.message(Form.cls_teacher)
async def fsm_cls_teacher(msg: Message, state: FSMContext) -> None:
    data = await state.get_data()
    teacher = None if msg.text.strip() == "-" else msg.text.strip()
    class_id = await db.add_class(
        data["subject_id"],
        data["dt"],
        data.get("room"),
        teacher,
        data.get("duration_minutes", db.DEFAULT_DURATION_MINUTES),
    )
    await state.clear()
    await msg.answer(
        "✅ Пара добавлена.",
        reply_markup=kb([btn("К паре", f"adm_clsd_{class_id}")]),
    )


@dp.callback_query(F.data.startswith("adm_delcls_"))
async def cb_delcls(cq: CallbackQuery) -> None:
    if not is_admin(cq):
        await cq.answer("Нет доступа", show_alert=True)
        return
    class_id = int(cq.data.split("_")[2])
    cls = await db.get_class(class_id)
    await db.delete_class(class_id)
    await cq.answer("Пара удалена", show_alert=True)
    await render_subject_detail(cq.message, cls["subject_id"])


async def render_class_detail(message: Message, class_id: int) -> None:
    cls = await db.get_class(class_id)
    if not cls:
        await message.edit_text("Пара не найдена.", reply_markup=kb([btn("◀️ Назад", "adm_subjects")]))
        return
    queue = await db.queue_for_class(class_id)
    entries = await db.queue_entries(queue["id"]) if queue else []
    start, end = class_times(cls)
    lines = [
        f"📅 *{cls['subject_name']}*",
        f"🕘 {datetime.fromisoformat(cls['dt']).strftime('%d.%m.%Y')} {start} - {end}",
        f"🚪 {cls['room'] or '—'}  👨‍🏫 {cls['teacher'] or '—'}",
        f"👥 Группа: `{cls['group_name'] or 'общая'}`",
    ]
    if queue:
        status = f"{STAT_EMOJI.get(queue['status'], '')} {STAT_LABEL.get(queue['status'], queue['status'])}"
        lines.append(f"\n🎫 {status}; записано: {len(entries)}")
    rows = []
    if queue:
        if queue["status"] == "pending":
            rows.append([btn("🟢 Открыть запись", f"adm_openq_{queue['id']}_{class_id}")])
        if queue["status"] == "open":
            rows.append([btn("🔀 Закрыть и сформировать очередь", f"adm_closeq_{queue['id']}_{class_id}")])
        if queue["status"] == "closed":
            rows.append([btn("📋 Отметить сдачи", f"adm_mark_{queue['id']}_0")])
            all_classes = await db.classes_for_subject(cls["subject_id"])
            next_classes = [item for item in all_classes if item["dt"] > cls["dt"]]
            if next_classes:
                rows.append([btn("⏩ Перенести несдавших", f"adm_carry_{queue['id']}_{next_classes[0]['id']}")])
    rows += [
        [btn("📝 Добавить задание", f"adm_addasgn_{class_id}")],
        [btn("◀️ Назад", f"adm_subj_{cls['subject_id']}")],
    ]
    await message.edit_text("\n".join(lines), reply_markup=kb(*rows), parse_mode="Markdown")


@dp.callback_query(F.data.startswith("adm_clsd_"))
async def cb_adm_clsd(cq: CallbackQuery) -> None:
    if not is_admin(cq):
        await cq.answer("Нет доступа", show_alert=True)
        return
    await cq.answer()
    await render_class_detail(cq.message, int(cq.data.split("_")[2]))


@dp.callback_query(F.data.startswith("adm_addasgn_"))
async def cb_addasgn(cq: CallbackQuery, state: FSMContext) -> None:
    if not is_admin(cq):
        await cq.answer("Нет доступа", show_alert=True)
        return
    class_id = int(cq.data.split("_")[2])
    await cq.answer()
    await state.update_data(class_id=class_id)
    await state.set_state(Form.asgn_title)
    await cq.message.edit_text(
        "Название задания:",
        reply_markup=kb([btn("❌ Отмена", f"adm_clsd_{class_id}")]),
    )


@dp.message(Form.asgn_title)
async def fsm_asgn_title(msg: Message, state: FSMContext) -> None:
    await state.update_data(title=msg.text.strip())
    await state.set_state(Form.asgn_desc)
    await msg.answer("Описание или `-`:", parse_mode="Markdown")


@dp.message(Form.asgn_desc)
async def fsm_asgn_desc(msg: Message, state: FSMContext) -> None:
    desc = None if msg.text.strip() == "-" else msg.text.strip()
    await state.update_data(description=desc)
    await state.set_state(Form.asgn_dl)
    await msg.answer("Дедлайн `ДД.ММ.ГГГГ ЧЧ:ММ` или `-`:", parse_mode="Markdown")


@dp.message(Form.asgn_dl)
async def fsm_asgn_dl(msg: Message, state: FSMContext) -> None:
    text = msg.text.strip()
    if text == "-":
        deadline = None
    else:
        deadline = parse_dt(text)
        if deadline is None:
            await msg.answer("Формат дедлайна: `15.03.2026 23:59` или `-`.", parse_mode="Markdown")
            return
    await state.update_data(deadline=deadline)
    await state.set_state(Form.asgn_url)
    await msg.answer("Ссылка на материал или `-`:", parse_mode="Markdown")


@dp.message(Form.asgn_url)
async def fsm_asgn_url(msg: Message, state: FSMContext) -> None:
    data = await state.get_data()
    cls = await db.get_class(data["class_id"])
    url = None if msg.text.strip() == "-" else msg.text.strip()
    await db.add_assignment(
        data["class_id"],
        cls["subject_id"],
        data["title"],
        data.get("description"),
        data.get("deadline"),
        url,
    )
    await state.clear()
    await msg.answer(
        "✅ Задание добавлено.",
        reply_markup=kb([btn("К паре", f"adm_clsd_{data['class_id']}")]),
    )


@dp.callback_query(F.data.startswith("adm_openq_"))
async def cb_openq(cq: CallbackQuery) -> None:
    if not is_admin(cq):
        await cq.answer("Нет доступа", show_alert=True)
        return
    _, _, queue_id, class_id = cq.data.split("_")
    class_id_int = int(class_id)
    await db.set_queue_status(int(queue_id), "open")
    await notify_queue_open(cq.bot, class_id_int)
    await cq.answer("Запись открыта, студенты уведомлены", show_alert=True)
    await render_class_detail(cq.message, class_id_int)


@dp.callback_query(F.data.startswith("adm_closeq_"))
async def cb_closeq(cq: CallbackQuery) -> None:
    if not is_admin(cq):
        await cq.answer("Нет доступа", show_alert=True)
        return
    _, _, queue_id, class_id = cq.data.split("_")
    queue_id_int = int(queue_id)
    await db.randomize_queue(queue_id_int)
    for entry in await db.queue_entries(queue_id_int):
        try:
            pos = entry["position"] or "—"
            qcat = entry.get("q_category") or "middle"
            await cq.bot.send_message(
                entry["telegram_id"],
                "🎲 *Очередь сформирована!*\n\n"
                f"Позиция: *{pos}*\n"
                f"Категория очереди: {CAT_EMOJI[qcat]} {CAT_LABEL[qcat]}",
                parse_mode="Markdown",
            )
        except Exception:
            log.exception("Cannot notify queue participant")
    await cq.answer("Очередь сформирована", show_alert=True)
    await render_class_detail(cq.message, int(class_id))


@dp.callback_query(F.data.startswith("adm_carry_"))
async def cb_carry(cq: CallbackQuery) -> None:
    if not is_admin(cq):
        await cq.answer("Нет доступа", show_alert=True)
        return
    _, _, queue_id, next_class_id = cq.data.split("_")
    await db.carry_queue(int(queue_id), int(next_class_id))
    await cq.answer("Несдавшие перенесены", show_alert=True)
    await render_class_detail(cq.message, int(next_class_id))


async def render_mark_page(message: Message, queue_id: int, page: int = 0) -> None:
    per_page = 4
    queue = await db.get_queue(queue_id)
    entries = await db.queue_entries(queue_id)
    chunk = entries[page * per_page : (page + 1) * per_page]
    rows = []
    for entry in chunk:
        pos = entry["position"] or "—"
        name = entry["full_name"][:24]
        current = "✅" if entry["submitted"] and entry["on_time"] else ("⏰" if entry["submitted"] else "❌")
        rows.append([btn(f"{pos}. {name} {current}", "noop")])
        rows.append(
            [
                btn("✅", f"adm_sub_{queue_id}_{entry['user_id']}_on_time_{page}"),
                btn("⏰", f"adm_sub_{queue_id}_{entry['user_id']}_late_{page}"),
                btn("❌", f"adm_sub_{queue_id}_{entry['user_id']}_no_show_{page}"),
            ]
        )
    nav = []
    if page > 0:
        nav.append(btn("◀", f"adm_mark_{queue_id}_{page - 1}"))
    if (page + 1) * per_page < len(entries):
        nav.append(btn("▶", f"adm_mark_{queue_id}_{page + 1}"))
    if nav:
        rows.append(nav)
    rows.append([btn("◀️ Назад", f"adm_clsd_{queue['class_id']}")])
    pages = max(1, (len(entries) - 1) // per_page + 1)
    await message.edit_text(
        f"📋 *Сдачи* - стр. {page + 1}/{pages}",
        reply_markup=kb(*rows),
        parse_mode="Markdown",
    )


@dp.callback_query(F.data.startswith("adm_mark_"))
async def cb_mark(cq: CallbackQuery) -> None:
    if not is_admin(cq):
        await cq.answer("Нет доступа", show_alert=True)
        return
    await cq.answer()
    parts = cq.data.split("_")
    queue_id = int(parts[2])
    page = int(parts[3]) if len(parts) > 3 else 0
    await render_mark_page(cq.message, queue_id, page)


@dp.callback_query(F.data.startswith("adm_sub_"))
async def cb_sub(cq: CallbackQuery) -> None:
    if not is_admin(cq):
        await cq.answer("Нет доступа", show_alert=True)
        return
    parts = cq.data.split("_")
    queue_id = int(parts[2])
    user_id = int(parts[3])
    if parts[4] == "on":
        kind, page = "on_time", int(parts[6])
    elif parts[4] == "no":
        kind, page = "no_show", int(parts[6])
    else:
        kind, page = parts[4], int(parts[5])
    await db.mark_submission(queue_id, user_id, kind)
    user = await db.get_user(user_id)
    await cq.answer(f"Готово. Рейтинг: {user['rating']}/100", show_alert=True)
    await render_mark_page(cq.message, queue_id, page)


@dp.callback_query(F.data == "adm_users")
async def cb_adm_users(cq: CallbackQuery) -> None:
    if not is_admin(cq):
        await cq.answer("Нет доступа", show_alert=True)
        return
    await cq.answer()
    users = await db.all_users()
    rows = [
        [btn(f"{CAT_EMOJI[user['category']]} {user['full_name'][:22]} ({user['rating']})", f"adm_user_{user['id']}")]
        for user in users
    ]
    rows.append([btn("◀️ Назад", "admin")])
    await cq.message.edit_text("👥 *Студенты*", reply_markup=kb(*rows), parse_mode="Markdown")


@dp.callback_query(F.data.startswith("adm_user_"))
async def cb_adm_user(cq: CallbackQuery) -> None:
    if not is_admin(cq):
        await cq.answer("Нет доступа", show_alert=True)
        return
    await cq.answer()
    user_id = int(cq.data.split("_")[2])
    user = await db.get_user(user_id)
    await cq.message.edit_text(
        fmt_user(user) if user else "Не найден",
        reply_markup=kb(
            [btn("✏️ Изменить ФИО", f"adm_editname_{user_id}")],
            [btn("🎓 Изменить группу", f"adm_editgroup_{user_id}")],
            [btn("⭐ Изменить рейтинг", f"adm_editr_{user_id}")],
            [btn("◀️ Назад", "adm_users")],
        ),
        parse_mode="Markdown",
    )


@dp.callback_query(F.data.startswith("adm_editname_"))
async def cb_editname(cq: CallbackQuery, state: FSMContext) -> None:
    if not is_admin(cq):
        await cq.answer("Нет доступа", show_alert=True)
        return
    user_id = int(cq.data.split("_")[2])
    await cq.answer()
    await state.update_data(edit_uid=user_id)
    await state.set_state(Form.edit_name)
    await cq.message.edit_text("Введите новое ФИО:", reply_markup=kb([btn("❌ Отмена", f"adm_user_{user_id}")]))


@dp.message(Form.edit_name)
async def fsm_edit_name(msg: Message, state: FSMContext) -> None:
    name = msg.text.strip()
    if len(name.split()) < 2:
        await msg.answer("Минимум два слова.")
        return
    data = await state.get_data()
    await db.set_full_name(data["edit_uid"], name)
    await state.clear()
    user = await db.get_user(data["edit_uid"])
    await msg.answer(fmt_user(user), reply_markup=kb([btn("◀️", f"adm_user_{data['edit_uid']}")]), parse_mode="Markdown")


@dp.callback_query(F.data.startswith("adm_editgroup_"))
async def cb_editgroup(cq: CallbackQuery, state: FSMContext) -> None:
    if not is_admin(cq):
        await cq.answer("Нет доступа", show_alert=True)
        return
    user_id = int(cq.data.split("_")[2])
    await cq.answer()
    await state.update_data(edit_uid=user_id)
    await state.set_state(Form.edit_group)
    await cq.message.edit_text(
        "Введите новую группу или `-`, чтобы очистить:",
        reply_markup=kb([btn("❌ Отмена", f"adm_user_{user_id}")]),
        parse_mode="Markdown",
    )


@dp.message(Form.edit_group)
async def fsm_edit_group(msg: Message, state: FSMContext) -> None:
    group = None if msg.text.strip() == "-" else msg.text.strip().upper()
    data = await state.get_data()
    await db.set_group(data["edit_uid"], group)
    await state.clear()
    user = await db.get_user(data["edit_uid"])
    await msg.answer(fmt_user(user), reply_markup=kb([btn("◀️", f"adm_user_{data['edit_uid']}")]), parse_mode="Markdown")


@dp.callback_query(F.data.startswith("adm_editr_"))
async def cb_editr(cq: CallbackQuery, state: FSMContext) -> None:
    if not is_admin(cq):
        await cq.answer("Нет доступа", show_alert=True)
        return
    user_id = int(cq.data.split("_")[2])
    await cq.answer()
    await state.update_data(edit_uid=user_id)
    await state.set_state(Form.edit_rating)
    await cq.message.edit_text(
        "Новый рейтинг от 0 до 100:",
        reply_markup=kb([btn("❌ Отмена", f"adm_user_{user_id}")]),
    )


@dp.message(Form.edit_rating)
async def fsm_edit_rating(msg: Message, state: FSMContext) -> None:
    try:
        rating = int(msg.text.strip())
    except ValueError:
        await msg.answer("Введите число от 0 до 100.")
        return
    data = await state.get_data()
    await db.set_rating(data["edit_uid"], rating)
    await state.clear()
    user = await db.get_user(data["edit_uid"])
    await msg.answer(fmt_user(user), reply_markup=kb([btn("◀️", f"adm_user_{data['edit_uid']}")]), parse_mode="Markdown")


@dp.callback_query(F.data == "noop")
async def cb_noop(cq: CallbackQuery) -> None:
    await cq.answer()


def add_cors(resp: web.StreamResponse) -> web.StreamResponse:
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.headers["Access-Control-Allow-Headers"] = "Content-Type, X-Telegram-Init-Data"
    resp.headers["Access-Control-Allow-Methods"] = "GET,POST,OPTIONS"
    return resp


def json_response(data: dict, status: int = 200) -> web.Response:
    return add_cors(web.json_response(data, status=status))


async def serve_index(request: web.Request) -> web.Response:
    try:
        return web.Response(text=INDEX_HTML.read_text(encoding="utf-8"), content_type="text/html")
    except FileNotFoundError:
        return web.Response(text="index.html not found", status=404)


async def request_json(request: web.Request) -> dict:
    try:
        return await request.json()
    except json.JSONDecodeError:
        return {}


def init_data_from_request(request: web.Request, data: Optional[dict] = None) -> str:
    if request.headers.get("X-Telegram-Init-Data"):
        return request.headers["X-Telegram-Init-Data"]
    if data and data.get("init_data"):
        return data["init_data"]
    return request.rel_url.query.get("init_data", "")


async def telegram_profile_from_request(
    request: web.Request,
    data: Optional[dict] = None,
    required: bool = False,
) -> Optional[dict]:
    init_data = init_data_from_request(request, data)
    if init_data:
        try:
            return validate_init_data(init_data, TOKEN)
        except WebAppAuthError as exc:
            if required:
                raise web.HTTPUnauthorized(
                    text=json.dumps({"error": str(exc)}, ensure_ascii=False),
                    content_type="application/json",
                )
            return None
    if ALLOW_UNVERIFIED_WEBAPP and data and data.get("user_id"):
        return {"id": int(data["user_id"]), "first_name": data.get("name", "Dev")}
    if required:
        raise web.HTTPUnauthorized(
            text=json.dumps({"error": "Требуется запуск из Telegram WebApp"}, ensure_ascii=False),
            content_type="application/json",
        )
    return None


async def db_user_from_request(
    request: web.Request,
    data: Optional[dict] = None,
    required: bool = False,
) -> Optional[dict]:
    profile = await telegram_profile_from_request(request, data, required=required)
    if not profile:
        return None
    return await db.get_user_by_tg(int(profile["id"]))


async def require_admin_user(request: web.Request, data: Optional[dict] = None) -> dict:
    profile = await telegram_profile_from_request(request, data, required=True)
    if not is_admin_id(int(profile["id"])):
        raise web.HTTPForbidden(
            text=json.dumps({"error": "Нет доступа"}, ensure_ascii=False),
            content_type="application/json",
        )
    return profile


def can_access_class(user: Optional[dict], cls: dict) -> bool:
    class_group = cls.get("group_name")
    if not class_group:
        return True
    if not user or not user.get("group_name"):
        return False
    return user["group_name"].strip().upper() == class_group.strip().upper()


def class_payload(
    cls: dict,
    queue: Optional[dict],
    entries: list[dict],
    user_entry: Optional[dict] = None,
) -> dict:
    start, end = class_times(cls)
    return {
        "id": cls["id"],
        "subject_id": cls["subject_id"],
        "subject_name": cls["subject_name"],
        "group_name": cls.get("group_name") or "",
        "teacher": cls.get("teacher") or "",
        "room": cls.get("room") or "",
        "type": cls.get("class_type", "ПР"),
        "date": datetime.fromisoformat(cls["dt"]).strftime("%Y-%m-%d"),
        "time_start": start,
        "time_end": end,
        "duration_minutes": cls.get("duration_minutes") or db.DEFAULT_DURATION_MINUTES,
        "queue_id": queue["id"] if queue else None,
        "queue_status": queue["status"] if queue else "no_queue",
        "queue_count": len(entries),
        "user_in_queue": user_entry is not None,
        "user_position": user_entry["position"] if user_entry and user_entry.get("position") else None,
        "user_q_category": user_entry.get("q_category") if user_entry else None,
    }


async def api_me(request: web.Request) -> web.Response:
    profile = await telegram_profile_from_request(request)
    if not profile:
        return json_response({"authenticated": False, "registered": False, "is_admin": False})
    user = await db.get_user_by_tg(int(profile["id"]))
    return json_response(
        {
            "authenticated": True,
            "registered": bool(user),
            "is_admin": is_admin_id(int(profile["id"])),
            "telegram_user": profile,
            "user": user,
        }
    )


async def api_register(request: web.Request) -> web.Response:
    data = await request_json(request)
    profile = await telegram_profile_from_request(request, data, required=True)
    tg_id = int(profile["id"])
    name = (data.get("name") or "").strip()
    if len(name.split()) < 2:
        first = profile.get("first_name", "")
        last = profile.get("last_name", "")
        name = f"{first} {last}".strip() or name
    # Group is always ИКБО-42-24 — single-group bot
    user = await db.ensure_user(tg_id, profile.get("username"), name, "ИКБО-42-24")
    await db.set_name_confirmed(tg_id)
    user["name_confirmed"] = 1
    return json_response({"status": "ok", "user": user})


async def api_schedule_month(request: web.Request) -> web.Response:
    try:
        year = int(request.rel_url.query["year"])
        month = int(request.rel_url.query["month"])
    except (KeyError, ValueError):
        return json_response({"counts": {}})
    user = await db_user_from_request(request)
    group_name = user.get("group_name") if user else None
    counts = await db.class_counts_for_month(year, month, group_name)
    return json_response({"counts": counts})


async def api_schedule(request: web.Request) -> web.Response:
    date_str = request.rel_url.query.get("date", "")
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        return json_response({"classes": []})
    user = await db_user_from_request(request)
    group_name = user.get("group_name") if user else None
    classes = await db.classes_for_date(date_str, group_name)

    queues: dict[int, Optional[dict]] = {}
    entries_map: dict[int, list[dict]] = {}
    for cls in classes:
        queue = await db.queue_for_class(cls["id"])
        queues[cls["id"]] = queue
        if queue:
            entries_map[queue["id"]] = await db.queue_entries(queue["id"])

    queue_ids = [q["id"] for q in queues.values() if q]
    user_entries = await db.user_entries_for_queues(user["id"], queue_ids) if user and queue_ids else {}

    result = []
    for cls in classes:
        queue = queues.get(cls["id"])
        entries = entries_map.get(queue["id"], []) if queue else []
        user_entry = user_entries.get(queue["id"]) if queue else None
        result.append(class_payload(cls, queue, entries, user_entry))
    return json_response({"classes": result})


async def api_queue_detail(request: web.Request) -> web.Response:
    class_id = int(request.match_info["class_id"])
    user = await db_user_from_request(request)
    cls = await db.get_class(class_id)
    if not cls:
        return json_response({"error": "not found"}, status=404)
    if user and not can_access_class(user, cls):
        return json_response({"error": "Эта пара относится к другой группе"}, status=403)
    queue = await db.queue_for_class(class_id)
    entries = await db.queue_entries(queue["id"]) if queue else []
    assignments = await db.assignments_for_class(class_id)
    payload = class_payload(cls, queue, entries)
    payload.update(
        {
            "class_id": class_id,
            "queue": {
                "id": queue["id"] if queue else None,
                "status": queue["status"] if queue else "pending",
                "entries": [
                    {
                        "user_id": entry["user_id"],
                        "telegram_id": entry["telegram_id"],
                        "full_name": entry["full_name"],
                        "group_name": entry.get("group_name") or "",
                        "position": entry["position"],
                        "q_category": entry["q_category"],
                        "user_cat": entry["user_cat"],
                        "submitted": bool(entry["submitted"]),
                        "on_time": bool(entry["on_time"]),
                    }
                    for entry in entries
                ],
            },
            "assignments": [
                {
                    "title": item["title"],
                    "description": item.get("description"),
                    "deadline": item.get("deadline"),
                    "url": item.get("url"),
                }
                for item in assignments
            ],
        }
    )
    return json_response(payload)


async def api_join(request: web.Request) -> web.Response:
    class_id = int(request.match_info["class_id"])
    data = await request_json(request)
    user = await db_user_from_request(request, data, required=True)
    if not user:
        return json_response({"error": "Сначала напишите /start боту"}, status=400)
    cls = await db.get_class(class_id)
    if not cls or not can_access_class(user, cls):
        return json_response({"error": "Пара недоступна вашей группе"}, status=403)
    queue = await db.queue_for_class(class_id)
    if not queue or queue["status"] != "open":
        return json_response({"error": "Запись закрыта"}, status=400)
    if await db.is_in_queue(queue["id"], user["id"]):
        return json_response({"status": "already_in"})
    await db.join_queue(queue["id"], user["id"])
    return json_response({"status": "ok"})


async def api_leave(request: web.Request) -> web.Response:
    class_id = int(request.match_info["class_id"])
    data = await request_json(request)
    user = await db_user_from_request(request, data, required=True)
    if not user:
        return json_response({"error": "not found"}, status=400)
    queue = await db.queue_for_class(class_id)
    if queue:
        await db.leave_queue(queue["id"], user["id"])
    return json_response({"status": "ok"})


async def api_seed(request: web.Request) -> web.Response:
    data = await request_json(request)
    await require_admin_user(request, data)
    result = await db.seed_ikbo_42_24()
    return json_response(result)


async def api_reseed(request: web.Request) -> web.Response:
    data = await request_json(request)
    await require_admin_user(request, data)
    result = await db.reseed_ikbo_42_24()
    return json_response(result)


async def api_admin_subjects(request: web.Request) -> web.Response:
    data = await request_json(request) if request.method == "POST" else None
    await require_admin_user(request, data)
    if request.method == "POST":
        subject_id = await db.add_subject(data.get("name", "").strip(), data.get("group_name"))
        return json_response({"status": "ok", "id": subject_id})
    return json_response({"subjects": await db.all_subjects()})


async def api_admin_classes(request: web.Request) -> web.Response:
    data = await request_json(request)
    await require_admin_user(request, data)
    dt = data.get("dt") or parse_dt(f"{data.get('date', '')} {data.get('time', '')}")
    if not dt:
        return json_response({"error": "Некорректная дата"}, status=400)
    class_id = await db.add_class(
        int(data["subject_id"]),
        dt,
        data.get("room"),
        data.get("teacher"),
        int(data.get("duration_minutes") or db.DEFAULT_DURATION_MINUTES),
    )
    return json_response({"status": "ok", "id": class_id})


async def api_admin_queue_action(request: web.Request) -> web.Response:
    data = await request_json(request)
    await require_admin_user(request, data)
    queue_id = int(request.match_info["queue_id"])
    action = request.match_info["action"]
    if action == "open":
        await db.set_queue_status(queue_id, "open")
        bot = request.app.get("bot")
        if bot:
            queue = await db.get_queue(queue_id)
            if queue:
                await notify_queue_open(bot, queue["class_id"])
    elif action == "close":
        await db.randomize_queue(queue_id)
        bot = request.app.get("bot")
        if bot:
            for entry in await db.queue_entries(queue_id):
                try:
                    pos = entry["position"] or "—"
                    qcat = entry.get("q_category") or "middle"
                    await bot.send_message(
                        entry["telegram_id"],
                        "🎲 *Очередь сформирована!*\n\n"
                        f"Позиция: *{pos}*\n"
                        f"Категория очереди: {CAT_EMOJI[qcat]} {CAT_LABEL[qcat]}",
                        parse_mode="Markdown",
                    )
                except Exception:
                    log.exception("Cannot notify queue participant")
    elif action in {"pending", "completed"}:
        await db.set_queue_status(queue_id, action)
    else:
        return json_response({"error": "unknown action"}, status=400)
    return json_response({"status": "ok"})


async def api_admin_mark(request: web.Request) -> web.Response:
    data = await request_json(request)
    await require_admin_user(request, data)
    await db.mark_submission(int(data["queue_id"]), int(data["user_id"]), data["kind"])
    return json_response({"status": "ok"})


async def options_handler(request: web.Request) -> web.Response:
    return add_cors(web.Response())


def create_bot() -> Bot:
    if not TOKEN:
        raise RuntimeError("BOT_TOKEN is not set. Configure the environment before starting the app.")
    proxy = os.getenv("HTTPS_PROXY") or os.getenv("https_proxy")
    if proxy:
        log.info("Using proxy: %s", proxy)
        return Bot(token=TOKEN, session=AiohttpSession(proxy=proxy))
    return Bot(token=TOKEN)


async def main() -> None:
    bot = create_bot()
    await db.init()
    await db.ensure_seed_current()  # auto-seed on first launch; reseeds if old fake data detected

    # Set persistent WebApp button and command list for all users
    try:
        await bot.set_chat_menu_button(
            menu_button=MenuButtonWebApp(text="📅 Очереди", web_app=WebAppInfo(url=WEB_URL))
        )
        log.info("Chat menu button set to WebApp: %s", WEB_URL)
    except Exception:
        log.warning("Could not set chat menu button (requires HTTPS and valid bot token)")

    commands = [
        BotCommand(command="start", description="Запустить бота / главное меню"),
        BotCommand(command="myqueue", description="Мои записи в очередях"),
        BotCommand(command="help", description="Помощь"),
    ]
    try:
        await bot.set_my_commands(commands)
    except Exception:
        log.warning("Could not set bot commands")

    app = web.Application()
    app["bot"] = bot
    app.router.add_get("/", serve_index)
    app.router.add_get("/api/me", api_me)
    app.router.add_post("/api/register", api_register)
    app.router.add_get("/api/schedule/month", api_schedule_month)
    app.router.add_get("/api/schedule", api_schedule)
    app.router.add_get("/api/queue/{class_id}", api_queue_detail)
    app.router.add_post("/api/queue/{class_id}/join", api_join)
    app.router.add_post("/api/queue/{class_id}/leave", api_leave)
    app.router.add_post("/api/seed", api_seed)
    app.router.add_post("/api/reseed", api_reseed)
    app.router.add_get("/api/admin/subjects", api_admin_subjects)
    app.router.add_post("/api/admin/subjects", api_admin_subjects)
    app.router.add_post("/api/admin/classes", api_admin_classes)
    app.router.add_post("/api/admin/queue/{queue_id}/{action}", api_admin_queue_action)
    app.router.add_post("/api/admin/mark", api_admin_mark)
    app.router.add_route("OPTIONS", "/{path_info:.*}", options_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()
    log.info("Server running on port %s", PORT)
    asyncio.create_task(auto_open_queues_task(bot))
    asyncio.create_task(auto_close_queues_task(bot))
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


def run() -> None:
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())


if __name__ == "__main__":
    run()

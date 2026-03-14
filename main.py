import asyncio
import logging
import os
import sys
from datetime import datetime
from typing import Optional

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery, InlineKeyboardButton,
    InlineKeyboardMarkup, Message, WebAppInfo
)
from aiohttp import web
from dotenv import load_dotenv

import database as db

load_dotenv()

TOKEN     = os.getenv("BOT_TOKEN", "")
ADMIN_IDS = [int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit()]
PORT      = int(os.getenv("PORT", "3000"))
WEB_URL   = os.getenv("WEB_URL", f"http://localhost:{PORT}")

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger(__name__)

bot = Bot(token=TOKEN)
dp  = Dispatcher(storage=MemoryStorage())

# ── FSM ───────────────────────────────────────────────────────────────────────
class Form(StatesGroup):
    subj_name   = State()
    subj_group  = State()
    cls_dt      = State()
    cls_room    = State()
    cls_teacher = State()
    asgn_title  = State()
    asgn_desc   = State()
    asgn_dl     = State()
    asgn_url    = State()
    edit_rating = State()
    edit_name   = State()
    register    = State()

CAT_EMOJI  = {"good": "🟢", "middle": "🟡", "poor": "🔴"}
CAT_LABEL  = {"good": "Добросовестный", "middle": "Средний", "poor": "Отстающий"}
STAT_EMOJI = {"pending": "⏳", "open": "🟢", "closed": "🔴", "completed": "✅"}

def kb(*rows): return InlineKeyboardMarkup(inline_keyboard=list(rows))
def btn(text, data): return InlineKeyboardButton(text=text, callback_data=data)

def parse_dt(text: str) -> Optional[str]:
    fmts = ["%d.%m.%Y %H:%M", "%d.%m %H:%M", "%Y-%m-%d %H:%M"]
    for f in fmts:
        try:
            dt = datetime.strptime(text.strip(), f)
            if dt.year == 1900:
                dt = dt.replace(year=datetime.now().year)
            return dt.isoformat()
        except ValueError:
            continue
    return None

def fmt_dt(s: str) -> str:
    try: return datetime.fromisoformat(s).strftime("%d.%m.%Y %H:%M")
    except: return s

def fmt_user(u: dict) -> str:
    cat = CAT_EMOJI[u["category"]] + " " + CAT_LABEL[u["category"]]
    return (
        f"👤 *{u['full_name']}*\n"
        f"⭐ Рейтинг: `{u['rating']}/100`\n"
        f"Категория: {cat}\n"
        f"✅ Вовремя: {u['on_time']}  ⏰ Поздно: {u['late']}  ❌ Не сдал: {u['no_show']}"
    )

def main_menu_kb(tg_id: int):
    rows = [
        [InlineKeyboardButton(text="📅 Открыть расписание", web_app=WebAppInfo(url=WEB_URL))],
        [btn("📊 Мой рейтинг", "my_rating"), btn("🏆 Лидерборд", "leaderboard")],
    ]
    if tg_id in ADMIN_IDS:
        rows.append([btn("⚙️ Админ-панель", "admin")])
    return kb(*rows)

# ── /start ────────────────────────────────────────────────────────────────────
@dp.message(Command("start"))
async def cmd_start(msg: Message, state: FSMContext):
    u = msg.from_user
    existing = await db.get_user_by_tg(u.id)
    if not existing:
        # New user — ask for full name
        await state.set_state(Form.register)
        await msg.answer(
            "👋 *Добро пожаловать!*\n\n"
            "Для регистрации введите ваше *ФИО* (Фамилия Имя Отчество):\n"
            "_Например: Иванов Иван Иванович_",
            parse_mode="Markdown"
        )
    else:
        await msg.answer(
            f"👋 *{existing['full_name']}*, добро пожаловать!\n\nОткрой расписание:",
            reply_markup=main_menu_kb(u.id), parse_mode="Markdown"
        )

@dp.message(Form.register)
async def fsm_register(msg: Message, state: FSMContext):
    name = msg.text.strip()
    if len(name.split()) < 2:
        await msg.answer("❌ Введите Фамилию и Имя (минимум 2 слова):")
        return
    u = msg.from_user
    await db.ensure_user(u.id, u.username, name)
    await state.clear()
    await msg.answer(
        f"✅ Вы зарегистрированы как *{name}*!\n\nОткрой расписание:",
        reply_markup=main_menu_kb(u.id), parse_mode="Markdown"
    )

@dp.callback_query(F.data == "main_menu")
async def cb_main(cq: CallbackQuery):
    await cq.answer()
    await cq.message.edit_text("👋 *Журнал очереди*",
        reply_markup=main_menu_kb(cq.from_user.id), parse_mode="Markdown")

@dp.callback_query(F.data == "my_rating")
async def cb_rating(cq: CallbackQuery):
    await cq.answer()
    u = await db.get_user_by_tg(cq.from_user.id)
    await cq.message.edit_text(fmt_user(u) if u else "Напишите /start",
        reply_markup=kb([btn("◀️ Назад","main_menu")]), parse_mode="Markdown")

@dp.callback_query(F.data == "leaderboard")
async def cb_lb(cq: CallbackQuery):
    await cq.answer()
    users = (await db.all_users())[:20]
    medals = {1:"🥇",2:"🥈",3:"🥉"}
    lines = ["🏆 *Топ студентов*\n"]
    for i, u in enumerate(users, 1):
        lines.append(f"{medals.get(i,f'`{i:>2}.`')} {CAT_EMOJI[u['category']]} *{u['full_name']}* — {u['rating']}/100")
    await cq.message.edit_text("\n".join(lines),
        reply_markup=kb([btn("◀️ Назад","main_menu")]), parse_mode="Markdown")

# ── Admin ─────────────────────────────────────────────────────────────────────
def is_admin(cq): return cq.from_user.id in ADMIN_IDS

@dp.callback_query(F.data == "admin")
async def cb_admin(cq: CallbackQuery):
    if not is_admin(cq): await cq.answer("⛔", show_alert=True); return
    await cq.answer()
    await cq.message.edit_text("⚙️ *Админ-панель*", reply_markup=kb(
        [btn("📚 Предметы","adm_subjects")],
        [btn("👥 Студенты","adm_users")],
        [btn("◀️ Назад","main_menu")]
    ), parse_mode="Markdown")

@dp.callback_query(F.data == "adm_subjects")
async def cb_adm_subjects(cq: CallbackQuery):
    if not is_admin(cq): await cq.answer("⛔", show_alert=True); return
    await cq.answer()
    subjects = await db.all_subjects()
    rows = []
    for s in subjects:
        grp = f" [{s['group_name']}]" if s.get("group_name") else ""
        rows.append([btn(f"📖 {s['name']}{grp}",f"adm_subj_{s['id']}"),btn("🗑",f"adm_delsubj_{s['id']}")])
    rows += [[btn("➕ Добавить предмет","adm_newsubj")],[btn("◀️ Назад","admin")]]
    await cq.message.edit_text("📚 *Предметы*", reply_markup=kb(*rows), parse_mode="Markdown")

@dp.callback_query(F.data == "adm_newsubj")
async def cb_adm_newsubj(cq: CallbackQuery, state: FSMContext):
    if not is_admin(cq): await cq.answer("⛔", show_alert=True); return
    await cq.answer(); await state.set_state(Form.subj_name)
    await cq.message.edit_text("📖 Название предмета:", reply_markup=kb([btn("❌ Отмена","adm_subjects")]))

@dp.message(Form.subj_name)
async def fsm_subj_name(msg: Message, state: FSMContext):
    await state.update_data(name=msg.text.strip()); await state.set_state(Form.subj_group)
    await msg.answer("Группа (или `-`):")

@dp.message(Form.subj_group)
async def fsm_subj_group(msg: Message, state: FSMContext):
    d = await state.get_data()
    grp = msg.text.strip() if msg.text.strip()!="-" else None
    await db.add_subject(d["name"], grp); await state.clear()
    await msg.answer(f"✅ *{d['name']}* добавлен!",
        reply_markup=kb([btn("📚 Предметы","adm_subjects")]), parse_mode="Markdown")

@dp.callback_query(F.data.startswith("adm_delsubj_"))
async def cb_delsubj(cq: CallbackQuery):
    if not is_admin(cq): await cq.answer("⛔", show_alert=True); return
    await db.delete_subject(int(cq.data.split("_")[2]))
    await cq.answer("Удалено!", show_alert=True)
    cq.data="adm_subjects"; await cb_adm_subjects(cq)

@dp.callback_query(F.data.startswith("adm_subj_"))
async def cb_adm_subj(cq: CallbackQuery):
    if not is_admin(cq): await cq.answer("⛔", show_alert=True); return
    await cq.answer()
    sid = int(cq.data.split("_")[2])
    subj = await db.get_subject(sid)
    classes = await db.classes_for_subject(sid)
    rows = []
    for c in classes:
        q = await db.queue_for_class(c["id"])
        st = STAT_EMOJI.get(q["status"] if q else "pending","⏳")
        rows.append([btn(f"{st} {fmt_dt(c['dt'])} | {c['room'] or '?'}",f"adm_clsd_{c['id']}"),
                     btn("🗑",f"adm_delcls_{c['id']}")])
    rows += [[btn("➕ Добавить пару",f"adm_addcls_{sid}")],[btn("◀️ Назад","adm_subjects")]]
    await cq.message.edit_text(f"📖 *{subj['name']}*", reply_markup=kb(*rows), parse_mode="Markdown")

@dp.callback_query(F.data.startswith("adm_clsd_"))
async def cb_adm_clsd(cq: CallbackQuery):
    if not is_admin(cq): await cq.answer("⛔", show_alert=True); return
    await cq.answer()
    cid = int(cq.data.split("_")[2])
    cls = await db.get_class(cid)
    q   = await db.queue_for_class(cid)
    entries = await db.queue_entries(q["id"]) if q else []
    lines=[f"📅 *{cls['subject_name']}*",f"🕐 {fmt_dt(cls['dt'])}",
           f"🚪 {cls['room'] or '—'}  👨‍🏫 {cls['teacher'] or '—'}"]
    if q: lines.append(f"\n🎫 {STAT_EMOJI.get(q['status'],'')} {q['status']} · {len(entries)} чел.")
    rows=[]
    if q:
        if q["status"]=="pending": rows.append([btn("🟢 Открыть запись",f"adm_openq_{q['id']}_{cid}")])
        if q["status"]=="open":    rows.append([btn("🔀 Закрыть и рандомить",f"adm_closeq_{q['id']}_{cid}")])
        if q["status"]=="closed":
            rows.append([btn("📋 Отметить сдачи",f"adm_mark_{q['id']}_0")])
            all_cls = await db.classes_for_subject(cls["subject_id"])
            nxt=[c for c in all_cls if c["dt"]>cls["dt"]]
            if nxt: rows.append([btn("⏩ Перенести",f"adm_carry_{q['id']}_{nxt[0]['id']}")])
    rows+=[[btn("📝 Добавить задание",f"adm_addasgn_{cid}")],
           [btn("◀️ Назад",f"adm_subj_{cls['subject_id']}")]]
    await cq.message.edit_text("\n".join(lines),reply_markup=kb(*rows),parse_mode="Markdown")

@dp.callback_query(F.data.startswith("adm_addcls_"))
async def cb_addcls(cq: CallbackQuery, state: FSMContext):
    if not is_admin(cq): await cq.answer("⛔", show_alert=True); return
    await cq.answer(); sid=int(cq.data.split("_")[2])
    await state.update_data(subject_id=sid); await state.set_state(Form.cls_dt)
    await cq.message.edit_text("📅 Дата и время: `ДД.ММ.ГГГГ ЧЧ:ММ`",
        reply_markup=kb([btn("❌ Отмена",f"adm_subj_{sid}")]),parse_mode="Markdown")

@dp.message(Form.cls_dt)
async def fsm_cls_dt(msg: Message, state: FSMContext):
    dt=parse_dt(msg.text)
    if not dt: await msg.answer("❌ Формат: `15.03.2025 10:00`",parse_mode="Markdown"); return
    await state.update_data(dt=dt); await state.set_state(Form.cls_room)
    await msg.answer("Аудитория (или `-`):")

@dp.message(Form.cls_room)
async def fsm_cls_room(msg: Message, state: FSMContext):
    await state.update_data(room=msg.text.strip() if msg.text.strip()!="-" else "")
    await state.set_state(Form.cls_teacher); await msg.answer("Преподаватель (или `-`):")

@dp.message(Form.cls_teacher)
async def fsm_cls_teacher(msg: Message, state: FSMContext):
    d=await state.get_data()
    teacher=msg.text.strip() if msg.text.strip()!="-" else ""
    await db.add_class(d["subject_id"],d["dt"],d["room"],teacher); await state.clear()
    await msg.answer("✅ Пара добавлена!",
        reply_markup=kb([btn("📖 К предмету",f"adm_subj_{d['subject_id']}")]),parse_mode="Markdown")

@dp.callback_query(F.data.startswith("adm_delcls_"))
async def cb_delcls(cq: CallbackQuery):
    if not is_admin(cq): await cq.answer("⛔", show_alert=True); return
    cid=int(cq.data.split("_")[2]); cls=await db.get_class(cid); sid=cls["subject_id"] if cls else None
    await db.delete_class(cid); await cq.answer("Удалено!",show_alert=True)
    if sid: cq.data=f"adm_subj_{sid}"; await cb_adm_subj(cq)

@dp.callback_query(F.data.startswith("adm_addasgn_"))
async def cb_addasgn(cq: CallbackQuery, state: FSMContext):
    if not is_admin(cq): await cq.answer("⛔", show_alert=True); return
    await cq.answer(); cid=int(cq.data.split("_")[2])
    await state.update_data(class_id=cid); await state.set_state(Form.asgn_title)
    await cq.message.edit_text("📝 Название задания:", reply_markup=kb([btn("❌ Отмена",f"adm_clsd_{cid}")]))

@dp.message(Form.asgn_title)
async def fsm_asgn_title(msg, state):
    await state.update_data(title=msg.text.strip()); await state.set_state(Form.asgn_desc)
    await msg.answer("Описание (или `-`):")

@dp.message(Form.asgn_desc)
async def fsm_asgn_desc(msg, state):
    desc=msg.text.strip() if msg.text.strip()!="-" else None
    await state.update_data(description=desc); await state.set_state(Form.asgn_dl)
    await msg.answer("Дедлайн `ДД.ММ.ГГГГ ЧЧ:ММ` (или `-`):",parse_mode="Markdown")

@dp.message(Form.asgn_dl)
async def fsm_asgn_dl(msg, state):
    t=msg.text.strip()
    await state.update_data(deadline=parse_dt(t) if t!="-" else None)
    await state.set_state(Form.asgn_url); await msg.answer("Ссылка на задание (или `-`):")

@dp.message(Form.asgn_url)
async def fsm_asgn_url(msg, state):
    d=await state.get_data(); url=msg.text.strip() if msg.text.strip()!="-" else None
    cls=await db.get_class(d["class_id"])
    await db.add_assignment(d["class_id"],cls["subject_id"],d["title"],
                             d.get("description"),d.get("deadline"),url)
    await state.clear()
    await msg.answer(f"✅ *{d['title']}* добавлено!",
        reply_markup=kb([btn("К паре",f"adm_clsd_{d['class_id']}")]),parse_mode="Markdown")

@dp.callback_query(F.data.startswith("adm_openq_"))
async def cb_openq(cq: CallbackQuery):
    if not is_admin(cq): await cq.answer("⛔", show_alert=True); return
    p=cq.data.split("_"); qid,cid=int(p[2]),int(p[3])
    await db.set_queue_status(qid,"open"); await cq.answer("✅ Открыта!",show_alert=True)
    cq.data=f"adm_clsd_{cid}"; await cb_adm_clsd(cq)

@dp.callback_query(F.data.startswith("adm_closeq_"))
async def cb_closeq(cq: CallbackQuery):
    if not is_admin(cq): await cq.answer("⛔", show_alert=True); return
    p=cq.data.split("_"); qid,cid=int(p[2]),int(p[3])
    await db.randomize_queue(qid)
    for e in await db.queue_entries(qid):
        try:
            pos=e["position"] or "—"; qcat=e.get("q_category") or "middle"
            await bot.send_message(e["telegram_id"],
                f"🎲 *Очередь сформирована!*\n\nПозиция: *{pos}*\nКатегория: {CAT_EMOJI[qcat]} {CAT_LABEL[qcat]}\n\nУдачи! 💪",
                parse_mode="Markdown")
        except: pass
    await cq.answer("🔀 Готово!",show_alert=True)
    cq.data=f"adm_clsd_{cid}"; await cb_adm_clsd(cq)

@dp.callback_query(F.data.startswith("adm_carry_"))
async def cb_carry(cq: CallbackQuery):
    if not is_admin(cq): await cq.answer("⛔", show_alert=True); return
    p=cq.data.split("_"); qid,ncid=int(p[2]),int(p[3])
    await db.carry_queue(qid,ncid); await cq.answer("⏩ Перенесено!",show_alert=True)
    cq.data=f"adm_clsd_{ncid}"; await cb_adm_clsd(cq)

@dp.callback_query(F.data.startswith("adm_mark_"))
async def cb_mark(cq: CallbackQuery):
    if not is_admin(cq): await cq.answer("⛔", show_alert=True); return
    await cq.answer()
    p=cq.data.split("_"); qid=int(p[2]); page=int(p[3]) if len(p)>3 else 0
    PAGE=4; q=await db.get_queue(qid); entries=await db.queue_entries(qid)
    chunk=entries[page*PAGE:(page+1)*PAGE]; rows=[]
    for e in chunk:
        pos=e["position"] or "—"; name=e["full_name"][:18]
        cur="✅" if e["submitted"] and e["on_time"] else ("⏰" if e["submitted"] else "❌")
        rows.append([btn(f"{pos}. {name} {cur}","noop")])
        rows.append([btn("✅",f"adm_sub_{qid}_{e['user_id']}_on_time_{page}"),
                     btn("⏰",f"adm_sub_{qid}_{e['user_id']}_late_{page}"),
                     btn("❌",f"adm_sub_{qid}_{e['user_id']}_no_show_{page}")])
    nav=[]
    if page>0: nav.append(btn("◀",f"adm_mark_{qid}_{page-1}"))
    if (page+1)*PAGE<len(entries): nav.append(btn("▶",f"adm_mark_{qid}_{page+1}"))
    if nav: rows.append(nav)
    rows.append([btn("◀️ Назад",f"adm_clsd_{q['class_id']}")])
    await cq.message.edit_text(
        f"📋 *Сдачи* — стр. {page+1}/{max(1,(len(entries)-1)//PAGE+1)}",
        reply_markup=kb(*rows),parse_mode="Markdown")

@dp.callback_query(F.data.startswith("adm_sub_"))
async def cb_sub(cq: CallbackQuery):
    if not is_admin(cq): await cq.answer("⛔", show_alert=True); return
    p=cq.data.split("_"); qid,uid,kind,page=int(p[2]),int(p[3]),p[4],int(p[5])
    await db.mark_submission(qid,uid,kind)
    u=await db.get_user(uid)
    label={"on_time":"✅ Вовремя","late":"⏰ Поздно","no_show":"❌ Не сдал"}.get(kind,"")
    await cq.answer(f"{label}. Рейтинг: {u['rating']}/100",show_alert=True)
    cq.data=f"adm_mark_{qid}_{page}"; await cb_mark(cq)

@dp.callback_query(F.data == "adm_users")
async def cb_adm_users(cq: CallbackQuery):
    if not is_admin(cq): await cq.answer("⛔", show_alert=True); return
    await cq.answer()
    users=await db.all_users()
    rows=[[btn(f"{CAT_EMOJI[u['category']]} {u['full_name'][:22]} ({u['rating']})",f"adm_user_{u['id']}")] for u in users]
    rows.append([btn("◀️ Назад","admin")])
    await cq.message.edit_text("👥 *Студенты*",reply_markup=kb(*rows),parse_mode="Markdown")

@dp.callback_query(F.data.startswith("adm_user_"))
async def cb_adm_user(cq: CallbackQuery):
    if not is_admin(cq): await cq.answer("⛔", show_alert=True); return
    await cq.answer(); uid=int(cq.data.split("_")[2]); u=await db.get_user(uid)
    await cq.message.edit_text(fmt_user(u) if u else "?",
        reply_markup=kb(
            [btn("✏️ Изменить ФИО", f"adm_editname_{uid}")],
            [btn("✏️ Изменить рейтинг", f"adm_editr_{uid}")],
            [btn("◀️ Назад","adm_users")]
        ),
        parse_mode="Markdown")

@dp.callback_query(F.data.startswith("adm_editname_"))
async def cb_editname(cq: CallbackQuery, state: FSMContext):
    if not is_admin(cq): await cq.answer("⛔", show_alert=True); return
    await cq.answer(); uid=int(cq.data.split("_")[2]); u=await db.get_user(uid)
    await state.update_data(edit_uid=uid); await state.set_state(Form.edit_name)
    await cq.message.edit_text(
        f"✏️ Текущее ФИО: *{u['full_name']}*\n\nВведите новое ФИО:",
        reply_markup=kb([btn("❌ Отмена",f"adm_user_{uid}")]),parse_mode="Markdown")

@dp.message(Form.edit_name)
async def fsm_edit_name(msg, state):
    name = msg.text.strip()
    if len(name.split()) < 2:
        await msg.answer("❌ Минимум 2 слова (Фамилия Имя):"); return
    d=await state.get_data(); uid=d["edit_uid"]
    await db.set_full_name(uid, name); await state.clear(); u=await db.get_user(uid)
    await msg.answer(f"✅ ФИО изменено!\n\n{fmt_user(u)}",
        reply_markup=kb([btn("◀️",f"adm_user_{uid}")]),parse_mode="Markdown")

@dp.callback_query(F.data.startswith("adm_editr_"))
async def cb_editr(cq: CallbackQuery, state: FSMContext):
    if not is_admin(cq): await cq.answer("⛔", show_alert=True); return
    await cq.answer(); uid=int(cq.data.split("_")[2]); u=await db.get_user(uid)
    await state.update_data(edit_uid=uid); await state.set_state(Form.edit_rating)
    await cq.message.edit_text(f"Рейтинг *{u['full_name']}*: `{u['rating']}/100`\n\nНовый (0–100):",
        reply_markup=kb([btn("❌ Отмена",f"adm_user_{uid}")]),parse_mode="Markdown")

@dp.message(Form.edit_rating)
async def fsm_edit_rating(msg, state):
    try:
        r=int(msg.text.strip()); d=await state.get_data(); uid=d["edit_uid"]
        await db.set_rating(uid,r); await state.clear(); u=await db.get_user(uid)
        await msg.answer(f"✅ Готово!\n\n{fmt_user(u)}",
            reply_markup=kb([btn("◀️",f"adm_user_{uid}")]),parse_mode="Markdown")
    except ValueError:
        await msg.answer("❌ Число от 0 до 100.")

@dp.callback_query(F.data == "noop")
async def cb_noop(cq): await cq.answer()

# ── Web API ───────────────────────────────────────────────────────────────────
def add_cors(resp):
    resp.headers["Access-Control-Allow-Origin"]  = "*"
    resp.headers["Access-Control-Allow-Headers"] = "Content-Type"
    return resp

async def serve_index(request):
    try:
        with open("index.html","r",encoding="utf-8") as f:
            return web.Response(text=f.read(),content_type="text/html")
    except FileNotFoundError:
        return web.Response(text="index.html not found",status=404)

async def api_schedule(request):
    date_str=request.rel_url.query.get("date","")
    if not date_str: return web.json_response({"classes":[]})
    try: date_obj=datetime.strptime(date_str,"%Y-%m-%d").date()
    except ValueError: return web.json_response({"classes":[]})
    subjects=await db.all_subjects(); result=[]
    for subj in subjects:
        for cls in await db.classes_for_subject(subj["id"]):
            try: cls_dt=datetime.fromisoformat(cls["dt"])
            except: continue
            if cls_dt.date()!=date_obj: continue
            queue=await db.queue_for_class(cls["id"])
            entries=await db.queue_entries(queue["id"]) if queue else []
            end_h=min(cls_dt.hour+1,23); end_m=30
            result.append({
                "id":cls["id"],"subject_name":subj["name"],
                "teacher":cls["teacher"] or "","room":cls["room"] or "","type":"ПР",
                "time_start":cls_dt.strftime("%H:%M"),
                "time_end":f"{end_h:02d}:{end_m:02d}",
                "queue_status":queue["status"] if queue else "pending",
                "queue_count":len(entries),
            })
    result.sort(key=lambda x:x["time_start"])
    return add_cors(web.json_response({"classes":result}))

async def api_queue_detail(request):
    cid=int(request.match_info["class_id"])
    cls=await db.get_class(cid)
    if not cls: return web.json_response({"error":"not found"},status=404)
    queue=await db.queue_for_class(cid)
    entries=await db.queue_entries(queue["id"]) if queue else []
    asgns=await db.assignments_for_class(cid)
    try:
        cls_dt=datetime.fromisoformat(cls["dt"])
        t_start=cls_dt.strftime("%H:%M")
        t_end=f"{min(cls_dt.hour+1,23):02d}:30"
    except: t_start=t_end=""
    return add_cors(web.json_response({
        "class_id":cid,"subject_name":cls["subject_name"],
        "teacher":cls["teacher"] or "","room":cls["room"] or "",
        "time_start":t_start,"time_end":t_end,
        "queue":{
            "id":queue["id"] if queue else None,
            "status":queue["status"] if queue else "pending",
            "entries":[{
                "telegram_id":e["telegram_id"],"full_name":e["full_name"],
                "position":e["position"],"q_category":e["q_category"],
                "user_cat":e["user_cat"],"submitted":bool(e["submitted"]),"on_time":bool(e["on_time"]),
            } for e in entries],
        },
        "assignments":[{"title":a["title"],"description":a.get("description"),
                        "deadline":a.get("deadline"),"url":a.get("url")} for a in asgns],
    }))

async def api_join(request):
    cid=int(request.match_info["class_id"]); data=await request.json()
    user=await db.get_user_by_tg(data.get("user_id",0))
    if not user: return web.json_response({"error":"Сначала напишите /start боту"},status=400)
    queue=await db.queue_for_class(cid)
    if not queue or queue["status"]!="open":
        return web.json_response({"error":"Запись закрыта"},status=400)
    if await db.is_in_queue(queue["id"],user["id"]):
        return add_cors(web.json_response({"status":"already_in"}))
    await db.join_queue(queue["id"],user["id"])
    return add_cors(web.json_response({"status":"ok"}))

async def api_leave(request):
    cid=int(request.match_info["class_id"]); data=await request.json()
    user=await db.get_user_by_tg(data.get("user_id",0))
    if not user: return web.json_response({"error":"not found"},status=400)
    queue=await db.queue_for_class(cid)
    if queue: await db.leave_queue(queue["id"],user["id"])
    return add_cors(web.json_response({"status":"ok"}))

async def options_handler(request):
    resp=web.Response()
    resp.headers.update({"Access-Control-Allow-Origin":"*",
                          "Access-Control-Allow-Methods":"GET,POST,OPTIONS",
                          "Access-Control-Allow-Headers":"Content-Type"})
    return resp

# ── Entry point ───────────────────────────────────────────────────────────────
async def main():
    await db.init()
    app=web.Application()
    app.router.add_get("/",serve_index)
    app.router.add_get("/api/schedule",api_schedule)
    app.router.add_get("/api/queue/{class_id}",api_queue_detail)
    app.router.add_post("/api/queue/{class_id}/join",api_join)
    app.router.add_post("/api/queue/{class_id}/leave",api_leave)
    app.router.add_route("OPTIONS","/{path_info:.*}",options_handler)
    runner=web.AppRunner(app); await runner.setup()
    await web.TCPSite(runner,"0.0.0.0",PORT).start()
    log.info(f"Server running on port {PORT}")
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__=="__main__":
    if sys.platform=="win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())

#!/usr/bin/env python3
"""
Телеграм-бот для Морского боя.
Токен и URL читаются из переменных окружения — не хранятся в коде.

Переменные окружения:
  BOT_TOKEN        — токен бота от @BotFather
  WEBAPP_URL       — URL фронтенда (https://morskoy-boy.ru)
  GAME_SERVER_URL  — URL Node.js сервера (https://morskoy-boy.ru или http://localhost:3000)
  SHOP_SECRET      — секрет для /api/admin/analytics и /api/reward (совпадает с server.js)
  ADMIN_IDS        — Telegram ID администраторов через запятую (для /stats, /notice, /post)
  STATS_CHAT_ID    — ID чата/пользователя куда слать авто-отчёт (обычно твой личный ID)
  WEBHOOK_URL      — публичный URL куда TG будет слать апдейты (https://morskoy-boy.ru/bot/webhook)
  WEBHOOK_PATH     — путь вебхука (по умолчанию /bot/webhook)
  WEBHOOK_PORT     — порт на котором слушает бот (по умолчанию 8443)
  WEBHOOK_SECRET   — секрет для X-Telegram-Bot-Api-Secret-Token
  USERS_DB_PATH    — путь к SQLite с пользователями бота (по умолчанию ./data/bot_users.db)
"""

import os
import logging
import asyncio
import sqlite3
import aiohttp
from datetime import datetime, timezone, timedelta

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    WebAppInfo,
    LabeledPrice,
)
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ConversationHandler,
    PreCheckoutQueryHandler,
    filters,
    ContextTypes,
)

# ── КОНФИГ ─────────────────────────────────────────────────────────────────────

BOT_TOKEN        = os.environ.get("BOT_TOKEN")
WEBAPP_URL       = os.environ.get("WEBAPP_URL",      "https://morskoy-boy.ru")
GAME_SERVER_URL  = os.environ.get("GAME_SERVER_URL", "http://localhost:3000")
SHOP_SECRET      = os.environ.get("SHOP_SECRET",     "shop_secret_change_me")
ADMIN_IDS        = set(x.strip() for x in os.environ.get("ADMIN_IDS", "").split(",") if x.strip())
STATS_CHAT_ID    = os.environ.get("STATS_CHAT_ID")
WEBHOOK_URL      = os.environ.get("WEBHOOK_URL")
WEBHOOK_PATH     = os.environ.get("WEBHOOK_PATH",    "/bot/webhook")
WEBHOOK_PORT     = int(os.environ.get("WEBHOOK_PORT", 8443))
WEBHOOK_SECRET   = os.environ.get("WEBHOOK_SECRET",  "")
USERS_DB_PATH    = os.environ.get("USERS_DB_PATH",   "./data/bot_users.db")

GAME_SHARE_TEXT  = "Приглашаю тебя поиграть в Морской бой прямо в Telegram:"
GAME_SHARE_URL   = "https://t.me/bteship_bot/bteship"
COMMUNITY_URL    = "https://t.me/+N35PmNXIvxFiYzMy"
BROWSER_URL      = "https://morskoy-boy.ru"

if not BOT_TOKEN:
    raise RuntimeError("Не задана переменная окружения BOT_TOKEN")

# ── ЛОГИРОВАНИЕ ────────────────────────────────────────────────────────────────

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)


# ── БАЗА ПОЛЬЗОВАТЕЛЕЙ БОТА ────────────────────────────────────────────────────

def _get_users_db() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(USERS_DB_PATH) or ".", exist_ok=True)
    con = sqlite3.connect(USERS_DB_PATH)
    con.execute("""
        CREATE TABLE IF NOT EXISTS bot_users (
            user_id    INTEGER PRIMARY KEY,
            username   TEXT,
            first_name TEXT,
            last_at    INTEGER DEFAULT (strftime('%s','now'))
        )
    """)
    con.commit()
    return con

def upsert_bot_user(user) -> None:
    """Запоминаем/обновляем пользователя, который написал боту."""
    try:
        con = _get_users_db()
        con.execute("""
            INSERT INTO bot_users (user_id, username, first_name, last_at)
            VALUES (?, ?, ?, strftime('%s','now'))
            ON CONFLICT(user_id) DO UPDATE SET
                username   = excluded.username,
                first_name = excluded.first_name,
                last_at    = excluded.last_at
        """, (user.id, user.username or "", user.first_name or ""))
        con.commit()
        con.close()
    except Exception as e:
        logger.error(f"[Users] upsert failed: {e}")

async def async_upsert_bot_user(user) -> None:
    """Неблокирующая обёртка для upsert_bot_user."""
    await asyncio.to_thread(upsert_bot_user, user)

def get_all_user_ids() -> list[int]:
    """Возвращает список всех user_id."""
    try:
        con = _get_users_db()
        rows = con.execute("SELECT user_id FROM bot_users").fetchall()
        con.close()
        return [r[0] for r in rows]
    except Exception as e:
        logger.error(f"[Users] get_all failed: {e}")
        return []

async def async_get_all_user_ids() -> list[int]:
    return await asyncio.to_thread(get_all_user_ids)

def get_users_count() -> int:
    try:
        con = _get_users_db()
        n = con.execute("SELECT COUNT(*) FROM bot_users").fetchone()[0]
        con.close()
        return n
    except:
        return 0

async def async_get_users_count() -> int:
    return await asyncio.to_thread(get_users_count)


# ── ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ────────────────────────────────────────────────────

def is_admin(user_id) -> bool:
    return str(user_id) in ADMIN_IDS

def admin_only(func):
    """Декоратор: отклоняет не-админов тихо (без ответа боту)."""
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not is_admin(update.effective_user.id):
            await update.message.reply_text("⛔ Нет доступа.")
            return ConversationHandler.END
        return await func(update, context)
    wrapper.__name__ = func.__name__
    return wrapper

def play_markup():
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("Играть", web_app=WebAppInfo(url=WEBAPP_URL))
    ]])

def main_markup():
    share_url = f"https://t.me/share/url?text={GAME_SHARE_TEXT}&url={GAME_SHARE_URL}"
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Играть",    web_app=WebAppInfo(url=WEBAPP_URL)),
            InlineKeyboardButton("Поделиться", url=share_url),
        ],
        [
            InlineKeyboardButton("Чат игроков", url=COMMUNITY_URL),
        ],
    ])

def normalize_user_id(user_id) -> str:
    return str(int(float(str(user_id))))

async def call_server(method: str, path: str, **kwargs) -> dict:
    url = f"{GAME_SERVER_URL}{path}"
    try:
        async with aiohttp.ClientSession() as session:
            async with getattr(session, method)(url, **kwargs) as resp:
                return await resp.json()
    except Exception as e:
        logger.error(f"[API] {method.upper()} {path} failed: {e}")
        return {"ok": False, "error": str(e)}

async def fetch_analytics() -> dict | None:
    result = await call_server(
        "get",
        "/api/admin/analytics",
        headers={"X-Admin-Secret": SHOP_SECRET},
    )
    if result.get("ok"):
        return result.get("data")
    logger.error(f"[Analytics] fetch failed: {result}")
    return None


# ── ФОРМАТИРОВАНИЕ ОТЧЁТА ──────────────────────────────────────────────────────

def fmt_analytics(data: dict, title: str = "Аналитика") -> str:
    p   = data.get("players",        {})
    b   = data.get("battles",        {})
    fb  = data.get("friend_battles", {})
    acc = data.get("accuracy",       {})
    wr  = data.get("winrate",        {})
    pu  = data.get("purchases",      {})
    tg  = data.get("tg",             {})
    br  = data.get("browser",        {})
    now_str = datetime.now(tz=timezone(timedelta(hours=3))).strftime("%d.%m.%Y %H:%M МСК")

    by_day_lines = ""
    for row in (b.get("by_day") or []):
        online_cnt = row.get('online', row.get('battles', 0))
        friend_cnt = row.get('friend', 0)
        friend_str = f" | 👥 {friend_cnt}" if friend_cnt else ""
        by_day_lines += f"  {row['day']}: 🎮 {online_cnt}{friend_str}\n"

    top_items_lines = ""
    for i, item in enumerate((pu.get("top_items") or []), 1):
        top_items_lines += f"  {i}. {item.get('name', item['item_id'])} — {item['cnt']} шт.\n"

    avg_acc = acc.get("avg_pct") or 0
    online  = data.get("online_now", 0)

    lines = [
        f"<b>{title}</b>",
        f"<i>{now_str}</i>",
        "",
        f"<b>Онлайн прямо сейчас:</b> {online}",
        "",
        "<b>Игроки (всего)</b>",
        f"  Зарегистрировано: {p.get('total', 0)}",
        f"  Новых за 24 ч: {p.get('new_24h', 0)}",
        f"  Активных за 7 дней: {p.get('active_7d', 0)}",
        f"  Активных за 30 дней: {p.get('active_30d', 0)}",
        "",
        "📱 <b>Telegram</b>",
        f"  Игроков: {tg.get('players', 0)}",
        f"  Новых за 24 ч: {tg.get('new_24h', 0)}",
        f"  Активных за 7 дней: {tg.get('active_7d', 0)}",
        f"  Активных за 30 дней: {tg.get('active_30d', 0)}",
        f"  Боёв сегодня: {tg.get('battles_today', 0)}",
        "",
        "🌐 <b>Браузер (morskoy-boy.ru)</b>",
        f"  Зарегистрированных: {br.get('players', 0)}",
        f"  Новых за 24 ч: {br.get('new_24h', 0)}",
        f"  Активных за 7 дней: {br.get('active_7d', 0)}",
        f"  Активных за 30 дней: {br.get('active_30d', 0)}",
        f"  Боёв сегодня (рег.): {br.get('battles_today', 0)}",
        f"  Боёв сегодня (гости): {br.get('guest_battles_today', 0)}",
        "",
        "<b>Бои онлайн (случайный)</b>",
        f"  За сегодня: {b.get('today', 0)}",
        f"  За 7 дней: {b.get('week', 0)}",
        f"  За 30 дней: {b.get('month', 0)}",
        f"  Всего: {b.get('total', 0)}",
        "",
        "<b>Бои с другом (по ссылке)</b>",
        f"  За сегодня: {fb.get('today', 0)}",
        f"  За 7 дней: {fb.get('week', 0)}",
        f"  За 30 дней: {fb.get('month', 0)}",
        f"  Всего: {fb.get('total', 0)}",
    ]

    if by_day_lines:
        lines += ["", "<b>По дням (7д) 🎮=онлайн 👥=друг:</b>", by_day_lines.rstrip()]

    lines += [
        "",
        "<b>Точность игроков (средняя)</b>",
        f"  {avg_acc}%  ({acc.get('total_hits', 0)} попаданий из {acc.get('total_shots', 0)} выстрелов)",
        "",
        "<b>Общий винрейт</b>",
        f"  {wr.get('pct', 0)}%  ({wr.get('wins', 0)} побед / {wr.get('losses', 0)} поражений)",
        "",
        "<b>Покупки (Telegram Stars)</b>",
        f"  За сегодня: {pu.get('today', 0)}",
        f"  За 7 дней: {pu.get('week', 0)}",
        f"  За 30 дней: {pu.get('month', 0)}",
        f"  Всего куплено: {pu.get('total', 0)}",
        f"  Возвратов: {pu.get('refunds', 0)}",
    ]

    if top_items_lines:
        lines += ["", "<b>Топ-5 товаров:</b>", top_items_lines.rstrip()]

    lines += [
        "",
        f"<b>Пользователей бота:</b> {get_users_count()}",
    ]

    return "\n".join(lines)


# ── КОМАНДЫ (публичные) ────────────────────────────────────────────────────────

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await async_upsert_bot_user(update.effective_user)
    name = update.effective_user.first_name or "Игрок"
    await update.message.reply_text(
        f"Привет, {name}!\n\n"
        f"<b>Морской бой</b> — классическая стратегия прямо в Telegram.\n\n"
        f"Играй против живых соперников, прокачивай уровень и соревнуйся в рейтинге.",
        parse_mode="HTML",
        reply_markup=play_markup(),
    )

async def play(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await async_upsert_bot_user(update.effective_user)
    await update.message.reply_text("Открываю игру:", reply_markup=play_markup())

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await async_upsert_bot_user(update.effective_user)
    admin_hint = "\n\n<b>Команды администратора:</b>\n/stats — аналитика\n/notice — создать уведомление в игре\n/post — рассылка всем пользователям\n/cancel — отменить текущую команду" if is_admin(update.effective_user.id) else ""
    await update.message.reply_text(
        "<b>Морской бой — помощь</b>\n\n"
        "/play — открыть игру\n"
        "/start — главное меню"
        f"{admin_hint}",
        parse_mode="HTML",
        reply_markup=play_markup(),
    )

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await async_upsert_bot_user(update.effective_user)
    await update.message.reply_text("Выбери действие:", reply_markup=main_markup())


# ── АНАЛИТИКА (/stats) — только админ ─────────────────────────────────────────

@admin_only
async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg  = await update.message.reply_text("Загружаю данные...")
    data = await fetch_analytics()
    if not data:
        await msg.edit_text("Не удалось получить данные от сервера.")
        return
    text = fmt_analytics(data, title="Аналитика Морского боя")
    await msg.edit_text(text, parse_mode="HTML")


async def send_daily_report(context: ContextTypes.DEFAULT_TYPE) -> None:
    if not STATS_CHAT_ID:
        return
    data = await fetch_analytics()
    if not data:
        logger.error("[DailyReport] Failed to fetch analytics")
        return
    text = fmt_analytics(data, title="Ежедневный отчёт — Морской бой")
    try:
        await context.bot.send_message(chat_id=STATS_CHAT_ID, text=text, parse_mode="HTML")
        logger.info(f"[DailyReport] Sent to {STATS_CHAT_ID}")
    except Exception as e:
        logger.error(f"[DailyReport] Send failed: {e}")


# ── УВЕДОМЛЕНИЯ (/notice) — только админ ──────────────────────────────────────

NOTICE_TITLE, NOTICE_BODY = range(2)

@admin_only
async def notice_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text(
        "📣 <b>Новое уведомление в игре</b>\n\nВведи <b>заголовок</b> (до 128 символов):\n\n/cancel — отмена",
        parse_mode="HTML",
    )
    return NOTICE_TITLE

async def notice_got_title(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data["notice_title"] = update.message.text.strip()[:128]
    await update.message.reply_text(
        "Теперь введи <b>текст</b> уведомления (до 1024 символов):\n\n/cancel — отмена",
        parse_mode="HTML",
    )
    return NOTICE_BODY

async def notice_got_body(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    title = context.user_data.pop("notice_title", "")
    body  = update.message.text.strip()[:1024]
    if not title or not body:
        await update.message.reply_text("Заголовок или текст пустой. Попробуй снова: /notice")
        return ConversationHandler.END
    result = await call_server(
        "post", "/api/notification",
        json={"title": title, "body": body},
        headers={"X-Admin-Secret": SHOP_SECRET},
    )
    if result.get("ok"):
        await update.message.reply_text(
            f"✅ Уведомление опубликовано!\n\n<b>{title}</b>\n{body}",
            parse_mode="HTML",
        )
    else:
        await update.message.reply_text(f"❌ Ошибка сервера: {result.get('error', '?')}")
    return ConversationHandler.END

async def notice_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data.pop("notice_title", None)
    context.user_data.pop("post_ready", None)
    await update.message.reply_text("Отменено.")
    return ConversationHandler.END


# ── РАССЫЛКА (/post) — только админ ───────────────────────────────────────────

# Три состояния: контент → дата/время → подтверждение
POST_CONTENT, POST_SCHEDULE, POST_CONFIRM_STATE = range(3)

MSK = timezone(timedelta(hours=3))  # UTC+3 Москва


def _parse_msk_datetime(text: str) -> datetime | None:
    """Парсит 'ДД.ММ.ГГГГ ЧЧ:ММ' и возвращает aware datetime в UTC. None если не удалось."""
    text = text.strip()
    for fmt in ("%d.%m.%Y %H:%M", "%d.%m.%y %H:%M"):
        try:
            dt_msk = datetime.strptime(text, fmt).replace(tzinfo=MSK)
            return dt_msk.astimezone(timezone.utc)
        except ValueError:
            continue
    return None


async def _do_broadcast(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Джоб: выполняет рассылку. Данные берёт из context.job.data."""
    data        = context.job.data
    src_msg_id  = data["msg_id"]
    src_chat_id = data["chat_id"]
    admin_id    = data["admin_id"]

    user_ids = await async_get_all_user_ids()
    sent = 0; failed = 0; blocked = 0

    for uid in user_ids:
        try:
            await context.bot.forward_message(
                chat_id=uid,
                from_chat_id=src_chat_id,
                message_id=src_msg_id,
            )
            sent += 1
        except Exception as e:
            err = str(e).lower()
            if any(w in err for w in ("blocked", "deactivated", "not found", "forbidden", "chat not found")):
                blocked += 1
            else:
                failed += 1
                logger.warning(f"[Post] uid={uid}: {e}")
        await asyncio.sleep(0.05)

    logger.info(f"[Post] done: sent={sent} blocked={blocked} failed={failed}")

    # Уведомляем админа об итогах
    try:
        await context.bot.send_message(
            chat_id=admin_id,
            text=(
                f"✅ <b>Рассылка завершена</b>\n\n"
                f"Отправлено: {sent}\n"
                f"Заблокировали бота: {blocked}\n"
                f"Ошибок: {failed}"
            ),
            parse_mode="HTML",
        )
    except Exception as e:
        logger.error(f"[Post] admin notify failed: {e}")


@admin_only
async def post_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    count = await async_get_users_count()
    await update.message.reply_text(
        f"📨 <b>Рассылка</b>\n\n"
        f"Пользователей в базе: <b>{count}</b>\n\n"
        f"Отправь сообщение для рассылки — любого формата:\n"
        f"текст, фото с подписью, видео, стикер, GIF.\n\n"
        f"/cancel — отмена",
        parse_mode="HTML",
    )
    return POST_CONTENT


async def post_got_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Получили контент — сохраняем, спрашиваем дату/время или «сейчас»."""
    msg = update.message
    context.user_data["post_msg_id"]  = msg.message_id
    context.user_data["post_chat_id"] = msg.chat_id

    now_msk = datetime.now(MSK).strftime("%d.%m.%Y %H:%M")
    await msg.reply_text(
        f"Когда разослать?\n\n"
        f"Введи дату и время по Москве в формате:\n"
        f"<code>ДД.ММ.ГГГГ ЧЧ:ММ</code>\n"
        f"Например: <code>{now_msk}</code>\n\n"
        f"Или напиши <b>сейчас</b> — разошлю немедленно.\n\n"
        f"/cancel — отмена",
        parse_mode="HTML",
    )
    return POST_SCHEDULE


async def post_got_schedule(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Получили дату/время — показываем итоговое подтверждение."""
    text = update.message.text.strip().lower()
    count = await async_get_users_count()

    if text in ("сейчас", "now", "0", "немедленно"):
        context.user_data["post_send_at"] = None  # None = немедленно
        when_str = "немедленно"
    else:
        dt_utc = _parse_msk_datetime(update.message.text.strip())
        if not dt_utc:
            await update.message.reply_text(
                "Не смог распознать дату. Нужен формат:\n"
                "<code>ДД.ММ.ГГГГ ЧЧ:ММ</code>  — например: "
                f"<code>{datetime.now(MSK).strftime('%d.%m.%Y %H:%M')}</code>\n\n"
                "Попробуй ещё раз или напиши <b>сейчас</b>.",
                parse_mode="HTML",
            )
            return POST_SCHEDULE

        now_utc = datetime.now(timezone.utc)
        if dt_utc <= now_utc:
            await update.message.reply_text(
                "⚠️ Это время уже прошло. Введи будущую дату/время или напиши <b>сейчас</b>.",
                parse_mode="HTML",
            )
            return POST_SCHEDULE

        context.user_data["post_send_at"] = dt_utc
        dt_msk   = dt_utc.astimezone(MSK)
        delay    = (dt_utc - now_utc).total_seconds()
        hours, rem = divmod(int(delay), 3600)
        mins       = rem // 60
        when_str = (
            f"{dt_msk.strftime('%d.%m.%Y %H:%M')} МСК "
            f"(через {hours}ч {mins}мин)"
        )

    await update.message.reply_text(
        f"Разослать пост <b>{count}</b> пользователям {when_str}?\n\n"
        f"Ответь <b>да</b> для подтверждения или /cancel для отмены.",
        parse_mode="HTML",
    )
    return POST_CONFIRM_STATE


async def post_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Ждём «да» — ставим в очередь или запускаем немедленно."""
    text = update.message.text.strip().lower()

    if text not in ("да", "yes", "+", "ок", "ok", "д"):
        await update.message.reply_text(
            "Жду «да» для подтверждения или /cancel для отмены."
        )
        return POST_CONFIRM_STATE

    src_msg_id  = context.user_data.pop("post_msg_id",  None)
    src_chat_id = context.user_data.pop("post_chat_id", None)
    send_at     = context.user_data.pop("post_send_at", None)

    if not src_msg_id or not src_chat_id:
        await update.message.reply_text("Что-то пошло не так. Попробуй /post снова.")
        return ConversationHandler.END

    job_data = {
        "msg_id":   src_msg_id,
        "chat_id":  src_chat_id,
        "admin_id": update.effective_user.id,
    }

    if send_at is None:
        # Немедленная рассылка
        user_ids   = await async_get_all_user_ids()
        status_msg = await update.message.reply_text(
            f"⏳ Рассылаю на {len(user_ids)} пользователей..."
        )
        sent = 0; failed = 0; blocked = 0
        for uid in user_ids:
            try:
                await context.bot.forward_message(
                    chat_id=uid,
                    from_chat_id=src_chat_id,
                    message_id=src_msg_id,
                )
                sent += 1
            except Exception as e:
                err = str(e).lower()
                if any(w in err for w in ("blocked", "deactivated", "not found", "forbidden", "chat not found")):
                    blocked += 1
                else:
                    failed += 1
                    logger.warning(f"[Post] uid={uid}: {e}")
            await asyncio.sleep(0.05)
        await status_msg.edit_text(
            f"✅ <b>Рассылка завершена</b>\n\n"
            f"Отправлено: {sent}\n"
            f"Заблокировали бота: {blocked}\n"
            f"Ошибок: {failed}",
            parse_mode="HTML",
        )
    else:
        # Отложенная рассылка через job_queue
        delay_sec = (send_at - datetime.now(timezone.utc)).total_seconds()
        context.application.job_queue.run_once(
            _do_broadcast,
            when=delay_sec,
            data=job_data,
            name=f"post_{update.effective_user.id}_{int(send_at.timestamp())}",
        )
        dt_msk = send_at.astimezone(MSK)
        await update.message.reply_text(
            f"⏰ <b>Запланировано!</b>\n\n"
            f"Рассылка запустится {dt_msk.strftime('%d.%m.%Y в %H:%M')} МСК.\n"
            f"Я пришлю отчёт после завершения.",
            parse_mode="HTML",
        )

    return ConversationHandler.END


# ── ПЛАТЕЖИ (TELEGRAM STARS) ───────────────────────────────────────────────────

async def pre_checkout(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query   = update.pre_checkout_query
    payload = query.invoice_payload
    logger.info(f"[Payment] pre_checkout: payload={payload} user={query.from_user.id}")
    try:
        parts = payload.split(":")
        if len(parts) != 3:
            await query.answer(ok=False, error_message="Неверный формат заказа")
            return
        if normalize_user_id(parts[0]) != normalize_user_id(query.from_user.id):
            logger.warning(f"[Payment] user mismatch")
            await query.answer(ok=False, error_message="Ошибка авторизации")
            return
        await query.answer(ok=True)
    except Exception as e:
        logger.error(f"[Payment] pre_checkout error: {e}")
        await query.answer(ok=True)


async def successful_payment(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await async_upsert_bot_user(update.effective_user)
    sp        = update.message.successful_payment
    payload   = sp.invoice_payload
    charge_id = sp.telegram_payment_charge_id
    user_id   = normalize_user_id(update.effective_user.id)
    logger.info(f"[Payment] successful: user={user_id} payload={payload} charge={charge_id}")
    try:
        parts   = payload.split(":")
        item_id = parts[1] if len(parts) >= 2 else None
        if not item_id:
            logger.error(f"[Payment] bad payload: {payload}")
            return
        result = await call_server(
            "post", "/api/webhook/telegram",
            json={"message": {
                "successful_payment": {
                    "invoice_payload":            sp.invoice_payload,
                    "telegram_payment_charge_id": charge_id,
                    "total_amount":               sp.total_amount,
                    "currency":                   sp.currency,
                },
                "_user_id": user_id,
            }},
            headers={"X-Shop-Secret": SHOP_SECRET},
        )
        if result.get("ok"):
            logger.info(f"[Payment] granted: user={user_id} item={item_id}")
        else:
            logger.error(f"[Payment] server error: {result}")
    except Exception as e:
        logger.error(f"[Payment] handler error: {e}")

    await update.message.reply_text(
        "Оплата прошла успешно!\n\n"
        "Ваш предмет уже доступен в инвентаре игры.\n"
        "Откройте игру и перейдите в Профиль → Инвентарь.",
        reply_markup=play_markup(),
    )


async def refunded_payment(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    rp        = update.message.refunded_payment
    charge_id = rp.telegram_payment_charge_id
    user_id   = normalize_user_id(update.effective_user.id)
    logger.info(f"[Refund] user={user_id} charge={charge_id}")
    result = await call_server(
        "post", "/api/webhook/telegram",
        json={"message": {
            "refunded_payment": {"telegram_payment_charge_id": charge_id},
            "_user_id": user_id,
        }},
        headers={"X-Shop-Secret": SHOP_SECRET},
    )
    if result.get("ok"):
        logger.info(f"[Refund] item revoked for user={user_id}")
    else:
        logger.error(f"[Refund] server error: {result}")
    await update.message.reply_text(
        "Возврат звёзд обработан. Предмет был удалён из вашего инвентаря.\n\n"
        "Если у вас есть вопросы — напишите нам.",
    )


# ── НАСТРОЙКА И ЗАПУСК ─────────────────────────────────────────────────────────

async def refunded_payment_filter(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message and update.message.refunded_payment:
        await refunded_payment(update, context)


def build_app() -> Application:
    app = Application.builder().token(BOT_TOKEN).build()

    # ── Публичные команды
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("play",  play))
    app.add_handler(CommandHandler("help",  help_command))

    # ── Только для админов: /stats
    app.add_handler(CommandHandler("stats", stats_command))

    # ── Только для админов: /notice (диалог)
    notice_handler = ConversationHandler(
        entry_points=[CommandHandler("notice", notice_start)],
        states={
            NOTICE_TITLE: [MessageHandler(filters.TEXT & ~filters.COMMAND, notice_got_title)],
            NOTICE_BODY:  [MessageHandler(filters.TEXT & ~filters.COMMAND, notice_got_body)],
        },
        fallbacks=[CommandHandler("cancel", notice_cancel)],
        conversation_timeout=180,
    )
    app.add_handler(notice_handler)

    # ── Только для админов: /post (диалог)
    post_handler = ConversationHandler(
        entry_points=[CommandHandler("post", post_start)],
        states={
            # 1: ждём любой контент
            POST_CONTENT: [
                MessageHandler(
                    (filters.TEXT | filters.PHOTO | filters.VIDEO |
                     filters.Document.ALL | filters.Sticker.ALL | filters.ANIMATION)
                    & ~filters.COMMAND,
                    post_got_message,
                ),
            ],
            # 2: ждём дату/время или «сейчас»
            POST_SCHEDULE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, post_got_schedule),
            ],
            # 3: ждём «да»
            POST_CONFIRM_STATE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, post_confirm),
            ],
        },
        fallbacks=[CommandHandler("cancel", notice_cancel)],
        conversation_timeout=600,  # 10 минут на весь диалог
    )
    app.add_handler(post_handler)

    # ── Платежи
    app.add_handler(PreCheckoutQueryHandler(pre_checkout))
    app.add_handler(MessageHandler(filters.SUCCESSFUL_PAYMENT, successful_payment))
    app.add_handler(MessageHandler(filters.StatusUpdate.ALL, refunded_payment_filter))

    # ── Обычные сообщения
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    # ── Ежедневный отчёт в 09:00 МСК (UTC+3 = UTC 06:00)
    if STATS_CHAT_ID:
        app.job_queue.run_daily(
            send_daily_report,
            time=datetime.strptime("06:00", "%H:%M").replace(tzinfo=timezone.utc).timetz(),
            name="daily_report",
        )
        logger.info(f"[DailyReport] Запланирован в 09:00 МСК → {STATS_CHAT_ID}")

    return app


def main() -> None:
    app = build_app()
    if WEBHOOK_URL:
        logger.info(f"[Bot] Запуск в режиме webhook: {WEBHOOK_URL}")
        app.run_webhook(
            listen="0.0.0.0",
            port=WEBHOOK_PORT,
            url_path=WEBHOOK_PATH,
            webhook_url=WEBHOOK_URL,
            secret_token=WEBHOOK_SECRET if WEBHOOK_SECRET else None,
            allowed_updates=["message", "pre_checkout_query"],
        )
    else:
        logger.info("[Bot] Запуск в режиме polling")
        app.run_polling(allowed_updates=["message", "pre_checkout_query"])


if __name__ == "__main__":
    main()


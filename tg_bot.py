#!/usr/bin/env python3
"""
Телеграм-бот для Морского боя.
Токен и URL читаются из переменных окружения — не хранятся в коде.

Переменные окружения:
  BOT_TOKEN        — токен бота от @BotFather
  WEBAPP_URL       — URL фронтенда (https://morskoy-boy.ru)
  GAME_SERVER_URL  — URL Node.js сервера (https://morskoy-boy.ru или http://localhost:3000)
  SHOP_SECRET      — секрет для /api/admin/analytics и /api/reward (совпадает с server.js)
  ADMIN_IDS        — Telegram ID администраторов через запятую (для /stats)
  STATS_CHAT_ID    — ID чата/пользователя куда слать авто-отчёт (обычно твой личный ID)
  WEBHOOK_URL      — публичный URL куда TG будет слать апдейты (https://morskoy-boy.ru/bot/webhook)
  WEBHOOK_PATH     — путь вебхука (по умолчанию /bot/webhook)
  WEBHOOK_PORT     — порт на котором слушает бот (по умолчанию 8443)
  WEBHOOK_SECRET   — секрет для X-Telegram-Bot-Api-Secret-Token
"""

import os
import logging
import asyncio
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
STATS_CHAT_ID    = os.environ.get("STATS_CHAT_ID")   # куда слать автоотчёт (Telegram ID)
WEBHOOK_URL      = os.environ.get("WEBHOOK_URL")
WEBHOOK_PATH     = os.environ.get("WEBHOOK_PATH",    "/bot/webhook")
WEBHOOK_PORT     = int(os.environ.get("WEBHOOK_PORT", 8443))
WEBHOOK_SECRET   = os.environ.get("WEBHOOK_SECRET",  "")

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


# ── ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ────────────────────────────────────────────────────

def is_admin(user_id) -> bool:
    return str(user_id) in ADMIN_IDS

def play_markup():
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("Играть", web_app=WebAppInfo(url=WEBAPP_URL))
    ]])

def main_markup():
    """Кнопки главного меню: Играть + Поделиться в одну строку, Чат отдельно."""
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
    """Форматирует данные аналитики в читаемый текст для Telegram."""
    p   = data.get("players",        {})
    b   = data.get("battles",        {})
    fb  = data.get("friend_battles", {})
    acc = data.get("accuracy",       {})
    wr  = data.get("winrate",        {})
    pu  = data.get("purchases",      {})
    tg  = data.get("tg",             {})
    br  = data.get("browser",        {})
    now_str = datetime.now(tz=timezone(timedelta(hours=3))).strftime("%d.%m.%Y %H:%M МСК")

    # Бои по дням
    by_day_lines = ""
    for row in (b.get("by_day") or []):
        online_cnt = row.get('online', row.get('battles', 0))
        friend_cnt = row.get('friend', 0)
        friend_str = f" | 👥 {friend_cnt}" if friend_cnt else ""
        by_day_lines += f"  {row['day']}: 🎮 {online_cnt}{friend_str}\n"

    # Топ товаров
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

    return "\n".join(lines)


# ── КОМАНДЫ ────────────────────────────────────────────────────────────────────

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    name = update.effective_user.first_name or "Игрок"
    await update.message.reply_text(
        f"Привет, {name}!\n\n"
        f"<b>Морской бой</b> — классическая стратегия прямо в Telegram.\n\n"
        f"Играй против живых игроков или бота, прокачивай уровень, "
        f"соревнуйся в рейтинге и открывай уникальные предметы.\n\n"
        f"Также можно сыграть в гостевом режиме через браузер: "
        f'<a href="{BROWSER_URL}">morskoy-boy.ru</a>',
        parse_mode="HTML",
        reply_markup=main_markup(),
        disable_web_page_preview=True,
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    admin_hint = "\n/stats — аналитика (только для админов)" if is_admin(update.effective_user.id) else ""
    await update.message.reply_text(
        "<b>Команды бота:</b>\n\n"
        "/start — приветствие\n"
        "/play  — открыть игру\n"
        "/help  — эта справка"
        f"{admin_hint}",
        parse_mode="HTML",
        reply_markup=play_markup(),
    )

async def play(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("Открываю игру:", reply_markup=play_markup())

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "Выбери действие:",
        reply_markup=main_markup(),
    )


# ── АНАЛИТИКА (/stats) ─────────────────────────────────────────────────────────

async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Команда /stats — только для администраторов."""
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text("Нет доступа.")
        return

    msg = await update.message.reply_text("Загружаю данные...")
    data = await fetch_analytics()

    if not data:
        await msg.edit_text("Не удалось получить данные от сервера.")
        return

    text = fmt_analytics(data, title="Аналитика Морского боя")
    await msg.edit_text(text, parse_mode="HTML")


async def send_daily_report(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Автоматический ежедневный отчёт."""
    if not STATS_CHAT_ID:
        return
    data = await fetch_analytics()
    if not data:
        logger.error("[DailyReport] Failed to fetch analytics")
        return
    text = fmt_analytics(data, title="Ежедневный отчёт — Морской бой")
    try:
        await context.bot.send_message(
            chat_id=STATS_CHAT_ID,
            text=text,
            parse_mode="HTML",
        )
        logger.info(f"[DailyReport] Sent to {STATS_CHAT_ID}")
    except Exception as e:
        logger.error(f"[DailyReport] Send failed: {e}")


# ── ПЛАТЕЖИ (TELEGRAM STARS) ───────────────────────────────────────────────────

async def pre_checkout(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.pre_checkout_query
    payload = query.invoice_payload

    logger.info(f"[Payment] pre_checkout: payload={payload} user={query.from_user.id}")

    try:
        parts = payload.split(":")
        if len(parts) != 3:
            await query.answer(ok=False, error_message="Неверный формат заказа")
            return

        user_id_in_payload = normalize_user_id(parts[0])
        user_id_actual      = normalize_user_id(query.from_user.id)

        if user_id_in_payload != user_id_actual:
            logger.warning(f"[Payment] user mismatch: payload={user_id_in_payload} actual={user_id_actual}")
            await query.answer(ok=False, error_message="Ошибка авторизации")
            return

        await query.answer(ok=True)

    except Exception as e:
        logger.error(f"[Payment] pre_checkout error: {e}")
        await query.answer(ok=True)


async def successful_payment(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
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
            "post",
            "/api/webhook/telegram",
            json={
                "message": {
                    "successful_payment": {
                        "invoice_payload":            sp.invoice_payload,
                        "telegram_payment_charge_id": charge_id,
                        "total_amount":               sp.total_amount,
                        "currency":                   sp.currency,
                    },
                    "_user_id": user_id,
                }
            },
            headers={"X-Shop-Secret": SHOP_SECRET},
        )

        if result.get("ok"):
            logger.info(f"[Payment] granted: user={user_id} item={item_id}")
        else:
            logger.error(f"[Payment] server error: {result}")

    except Exception as e:
        logger.error(f"[Payment] successful_payment handler error: {e}")

    item_name = parts[1].replace("_", " ").title() if len(parts) >= 2 else "товар"
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
        "post",
        "/api/webhook/telegram",
        json={
            "message": {
                "refunded_payment": {
                    "telegram_payment_charge_id": charge_id,
                },
                "_user_id": user_id,
            }
        },
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

    # Команды
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("play",  play))
    app.add_handler(CommandHandler("help",  help_command))
    app.add_handler(CommandHandler("stats", stats_command))

    # Платежи
    app.add_handler(PreCheckoutQueryHandler(pre_checkout))
    app.add_handler(MessageHandler(filters.SUCCESSFUL_PAYMENT, successful_payment))
    app.add_handler(MessageHandler(filters.StatusUpdate.ALL, refunded_payment_filter))

    # Обычные сообщения
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    # Ежедневный отчёт в 09:00 МСК (UTC+3 = UTC 06:00)
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

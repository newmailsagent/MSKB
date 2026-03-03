#!/usr/bin/env python3
"""
Телеграм-бот для Морского боя.
Токен и URL читаются из переменных окружения — не хранятся в коде.
"""

import os
import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes

# ── КОНФИГ из переменных окружения ─────────────────
BOT_TOKEN  = os.environ.get("BOT_TOKEN")
WEBAPP_URL = os.environ.get("WEBAPP_URL", "https://pobesedka.ru")

if not BOT_TOKEN:
    raise RuntimeError("Не задана переменная окружения BOT_TOKEN")

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Подавляем httpx чтобы токен не светился в логах URL запросов
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

# ── КНОПКА ИГРЫ ────────────────────────────────────
def play_button():
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("⚓ Играть", web_app=WebAppInfo(url=WEBAPP_URL))
    ]])

# ── КОМАНДЫ ────────────────────────────────────────
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    name = update.effective_user.first_name or "Игрок"
    await update.message.reply_text(
        f"Привет, {name}! ⚓\n\n"
        f"<b>Морской бой</b> — классическая игра прямо в Telegram.\n\n"
        f"🔹 Играй против бота или с друзьями онлайн\n"
        f"🔹 Отслеживай свою статистику\n"
        f"🔹 Соревнуйся в общем рейтинге\n\n"
        f"Нажми кнопку ниже чтобы начать:",
        parse_mode="HTML",
        reply_markup=play_button()
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "<b>Команды бота:</b>\n\n"
        "/start — приветствие\n"
        "/play  — открыть игру\n"
        "/help  — эта справка",
        parse_mode="HTML",
        reply_markup=play_button()
    )

async def play(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("Открываю игру:", reply_markup=play_button())

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "Нажми кнопку чтобы открыть игру 👇",
        reply_markup=play_button()
    )

# ── ЗАПУСК ──────────────────────────────────────────
def main() -> None:
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("play",  play))
    app.add_handler(CommandHandler("help",  help_command))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    logger.info(f"Бот запущен. WEBAPP_URL={WEBAPP_URL}")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()

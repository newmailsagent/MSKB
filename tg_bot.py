#!/usr/bin/env python3
"""
Телеграм-бот для Морского боя (bteship_bot)
Простой бот с кнопкой "Играть" и приветственным сообщением.

Установка: pip3 install python-telegram-bot
Запуск:    python3 tg_bot.py
PM2:       pm2 start tg_bot.py --interpreter python3 --name bteship-bot
"""

import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes

# ── НАСТРОЙКИ ──────────────────────────────────────
BOT_TOKEN  = "ВСТАВЬ_СЮДА_ТОКЕН_ОТ_BOTFATHER"  # токен от @BotFather
WEBAPP_URL = "https://pobesedka.ru"              # URL твоей игры

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ── КОМАНДЫ ────────────────────────────────────────

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Приветствие при /start или при нажатии кнопки Играть."""
    user = update.effective_user
    name = user.first_name or "Игрок"

    # Кнопка открывающая Mini App
    keyboard = [[
        InlineKeyboardButton(
            text="⚓ Играть",
            web_app=WebAppInfo(url=WEBAPP_URL)
        )
    ]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        f"Привет, {name}! ⚓\n\n"
        f"<b>Морской бой</b> — классическая игра в твоём Telegram.\n\n"
        f"🔹 Играй против бота или с друзьями онлайн\n"
        f"🔹 Отслеживай свою статистику\n"
        f"🔹 Соревнуйся в общем рейтинге\n\n"
        f"Нажми кнопку ниже чтобы начать игру:",
        parse_mode="HTML",
        reply_markup=reply_markup
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    keyboard = [[InlineKeyboardButton("⚓ Играть", web_app=WebAppInfo(url=WEBAPP_URL))]]
    await update.message.reply_text(
        "<b>Морской бой — команды:</b>\n\n"
        "/start — начало работы\n"
        "/play — открыть игру\n"
        "/help — эта справка",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def play(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    keyboard = [[InlineKeyboardButton("⚓ Играть", web_app=WebAppInfo(url=WEBAPP_URL))]]
    await update.message.reply_text(
        "Открываю игру...",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Ответ на любое другое сообщение."""
    keyboard = [[InlineKeyboardButton("⚓ Играть", web_app=WebAppInfo(url=WEBAPP_URL))]]
    await update.message.reply_text(
        "Нажми кнопку ниже чтобы открыть игру 👇",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

# ── ЗАПУСК ──────────────────────────────────────────

def main() -> None:
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help",  help_command))
    app.add_handler(CommandHandler("play",  play))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    logger.info("Бот запущен...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()

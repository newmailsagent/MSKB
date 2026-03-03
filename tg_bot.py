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

GAME_SHARE_TEXT = "Приглашаю тебя поиграть в Морской бой со мной прямо в Telegram:"
GAME_SHARE_URL  = "https://pobesedka.ru"  # или другая ссылка, если нужно

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


# ── КНОПКИ ─────────────────────────────────────────

def play_button():
    """Кнопка «Играть» открывает WebApp/игру."""
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("⚓ Играть", web_app=WebAppInfo(url=WEBAPP_URL))
    ]])


def share_button():
    """Кнопка «Поделиться игрой» — открывает окно выбора чата."""
    share_url = GAME_SHARE_URL
    share_text = GAME_SHARE_TEXT
    return InlineKeyboardMarkup([[
        InlineKeyboardButton(
            "📤 Поделиться игрой",
            url=f"https://t.me/share/url?url={share_url}&text={share_text}"
        )
    ]])


# ── КОМАНДЫ ────────────────────────────────────────
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    name = update.effective_user.first_name or "Игрок"

    # Две кнопки: играть и поделиться
    keyboard = [
        [InlineKeyboardButton("⚓ Играть", web_app=WebAppInfo(url=WEBAPP_URL))],
        [InlineKeyboardButton(
            "📤 Поделиться игрой",
            url=f"https://t.me/share/url?url={GAME_SHARE_URL}&text={GAME_SHARE_TEXT}"
        )],
    ]
    markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        f"Привет, {name}! ⚓\n\n"
        f"<b>Морской бой</b> — классическая игра прямо в Telegram.\n\n"
        f"🔹 Играй против бота или с друзьями онлайн\n"
        f"🔹 Отслеживай свою статистику\n"
        f"🔹 Соревнуйся в общем рейтинге\n\n"
        f"Нажми кнопку ниже чтобы начать:",
        parse_mode="HTML",
        reply_markup=markup
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
        "Выбери действие 👇",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("⚓ Играть", web_app=WebAppInfo(url=WEBAPP_URL))],
            [InlineKeyboardButton(
                "📤 Поделиться игрой",
                url=f"https://t.me/share/url?url={GAME_SHARE_URL}&text={GAME_SHARE_TEXT}"
            )]
        ])
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

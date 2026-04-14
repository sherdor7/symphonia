from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo
from telegram.ext import Application, CommandHandler, ContextTypes
import os

BOT_TOKEN = os.getenv("8524810543:AAHCihTyuTHCm5QmPiKelN6awOEhuvRxSLA")
WEB_APP_URL = os.getenv("https://cuculiform-unstirrable-marjory.ngrok-free.dev")  # later point this to your deployed web app

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton(
                text="Open Food Delivery App",
                web_app=WebAppInfo(url=WEB_APP_URL)
            )
        ]
    ])

    await update.message.reply_text(
        "Welcome to Food Delivery Bot.\nTap the button below to open the app.",
        reply_markup=keyboard
    )

def main():
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    print("Bot is running...")
    app.run_polling()

if __name__ == "__main__":
    main()
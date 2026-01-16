import os
import telebot
from flask import Flask, request
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

app = Flask(__name__)

# Получаем токен из переменных окружения
TOKEN = os.environ.get('TELEGRAM_TOKEN')
if not TOKEN:
    logging.error("TELEGRAM_TOKEN не найден в переменных окружения!")
    raise ValueError("TELEGRAM_TOKEN не найден")

bot = telebot.TeleBot(TOKEN)

# Простой обработчик команд
@bot.message_handler(commands=['start'])
def send_welcome(message):
    logging.info(f"Получена команда /start от пользователя {message.from_user.id}")
    bot.reply_to(message, "✅ Бот на Render работает! Миграция успешна!")

@bot.message_handler(func=lambda message: True)
def echo_all(message):
    logging.info(f"Получено сообщение: {message.text}")
    bot.reply_to(message, f"Вы написали: {message.text}")

# Вебхук для Telegram
@app.route('/webhook', methods=['POST'])
def webhook():
    logging.info("Получен запрос на вебхук")
    try:
        json_str = request.get_data().decode('utf-8')
        update = telebot.types.Update.de_json(json_str)
        bot.process_new_updates([update])
        logging.info("Сообщение обработано")
    except Exception as e:
        logging.error(f"Ошибка обработки вебхука: {e}")
    return 'OK', 200

# Health check для Render
@app.route('/health')
def health():
    return 'OK', 200

@app.route('/')
def home():
    return 'Бот для клуба "Увлекательные чтения" работает!'

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)

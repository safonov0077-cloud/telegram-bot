import os
import telebot
from flask import Flask, request
import logging
import json

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

app = Flask(__name__)

# Получаем токен из переменных окружения
TOKEN = os.environ.get('TELEGRAM_TOKEN')
if not TOKEN:
    logging.error("TELEGRAM_TOKEN не найден в переменных окружения!")
    raise ValueError("TELEGRAM_TOKEN не найден")

bot = telebot.TeleBot(TOKEN)
logging.info(f"Бот инициализирован с токеном: {TOKEN[:15]}...")

# ✅ РАБОЧИЕ ОБРАБОТЧИКИ КОМАНД

@bot.message_handler(commands=['start', 'help'])
def handle_start(message):
    user_id = message.from_user.id
    username = message.from_user.username or "Без имени"
    logging.info(f"⚡ ВЫЗВАН ОБРАБОТЧИК /start от @{username} (ID: {user_id})")
    
    # Проверяем, что бот может отвечать
    try:
        response = f"✅ Бот на Render работает!\nID чата: {message.chat.id}\nВаш ID: {user_id}"
        bot.send_message(message.chat.id, response)
        logging.info(f"✅ Ответ отправлен пользователю {user_id}")
    except Exception as e:
        logging.error(f"❌ Ошибка отправки: {e}")

@bot.message_handler(func=lambda message: True)
def handle_all_messages(message):
    user_id = message.from_user.id
    text = message.text or "без текста"
    logging.info(f"📨 Сообщение от {user_id}: '{text}'")
    
    try:
        if text.startswith('/'):
            bot.send_message(message.chat.id, f"🤖 Команда '{text}' получена, но не обрабатывается")
        else:
            bot.send_message(message.chat.id, f"📝 Вы написали: {text}")
        logging.info(f"✅ Ответ на сообщение отправлен")
    except Exception as e:
        logging.error(f"❌ Ошибка эхо-ответа: {e}")

# ✅ ИСПРАВЛЕННЫЙ ВЕБХУК С ДЕТАЛЬНОЙ ОБРАБОТКОЙ

@app.route('/webhook', methods=['POST'])
def webhook():
    logging.info("=" * 60)
    logging.info("🌐 ПОЛУЧЕН ВЕБХУК ОТ TELEGRAM")
    
    try:
        # Получаем сырые данные
        raw_data = request.get_data()
        logging.info(f"📦 Размер данных: {len(raw_data)} байт")
        
        # Декодируем и парсим JSON
        json_str = raw_data.decode('utf-8')
        update_data = json.loads(json_str)
        
        # Логируем структуру обновления
        logging.info(f"📊 Ключи в обновлении: {list(update_data.keys())}")
        
        # Проверяем тип обновления
        if 'message' in update_data:
            msg = update_data['message']
            user_id = msg.get('from', {}).get('id', 'unknown')
            text = msg.get('text', 'без текста')
            chat_id = msg.get('chat', {}).get('id', 'unknown')
            logging.info(f"💬 Сообщение: user_id={user_id}, chat_id={chat_id}, text='{text}'")
        elif 'callback_query' in update_data:
            logging.info("🔘 Callback query получен")
        else:
            logging.warning(f"⚠️ Неизвестный тип обновления: {update_data}")
        
        # Преобразуем JSON в объект Update
        update = telebot.types.Update.de_json(json_str)
        
        # ✅ КРИТИЧЕСКИЙ МОМЕНТ: Передаем обновление боту
        if update:
            bot.process_new_updates([update])
            logging.info("🔄 Обновление обработано ботом")
        else:
            logging.error("❌ Не удалось создать объект Update")
        
    except json.JSONDecodeError as e:
        logging.error(f"❌ Ошибка декодирования JSON: {e}")
        return 'Invalid JSON', 400
    except Exception as e:
        logging.error(f"❌ Критическая ошибка в вебхуке: {e}", exc_info=True)
        return 'Server Error', 500
    
    logging.info("=" * 60)
    return 'OK', 200

# ✅ ДОПОЛНИТЕЛЬНЫЕ МАРШРУТЫ ДЛЯ ТЕСТИРОВАНИЯ

@app.route('/health')
def health():
    return 'OK', 200

@app.route('/')
def home():
    return '''
    <h1>🤖 Бот для клуба "Увлекательные чтения"</h1>
    <p>Статус: <strong>Работает</strong> ✅</p>
    <p>Python: 3.13.4</p>
    <p>Вебхук: /webhook</p>
    <p>Health check: /health</p>
    <p>Отправьте /start боту в Telegram</p>
    '''

@app.route('/test')
def test():
    return 'Тестовая страница работает!', 200

@app.route('/debug')
def debug():
    token_exists = bool(TOKEN)
    return f'''
    <h1>🔍 Отладка бота</h1>
    <p>Токен установлен: {token_exists}</p>
    <p>Токен (первые 15 символов): {TOKEN[:15] if TOKEN else "Нет токена"}...</p>
    <p>URL бота: https://telegram-bot-club.onrender.com</p>
    <p>Вебхук: https://telegram-bot-club.onrender.com/webhook</p>
    <p><a href="/">Главная</a> | <a href="/health">Health</a></p>
    '''

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)

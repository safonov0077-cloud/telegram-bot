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

# ✅ ОБРАБОТЧИКИ КОМАНД

@bot.message_handler(commands=['start', 'help'])
def handle_start(message):
    user_id = message.from_user.id
    chat_id = message.chat.id
    username = message.from_user.username or "Без имени"
    
    logging.info(f"⚡ ВЫЗВАН ОБРАБОТЧИК /start от @{username} (ID: {user_id})")
    
    try:
        response = f"✅ Бот на Render работает!\nВаш ID: {user_id}\nID чата: {chat_id}"
        bot.send_message(chat_id, response)
        logging.info(f"✅ Ответ отправлен в chat_id={chat_id}")
    except Exception as e:
        logging.error(f"❌ Ошибка отправки: {e}")

@bot.message_handler(func=lambda message: True)
def handle_all_messages(message):
    user_id = message.from_user.id
    text = message.text or "без текста"
    
    logging.info(f"📨 Сообщение от {user_id}: '{text}'")
    
    try:
        if text.startswith('/'):
            bot.send_message(message.chat.id, f"🤖 Команда '{text}' получена")
        else:
            bot.send_message(message.chat.id, f"📝 Вы написали: {text}")
        logging.info(f"✅ Ответ на сообщение отправлен")
    except Exception as e:
        logging.error(f"❌ Ошибка эхо-ответа: {e}")

# ✅ ИСПРАВЛЕННЫЙ ВЕБХУК - РУЧНАЯ ОБРАБОТКА

@app.route('/webhook', methods=['POST'])
def webhook():
    logging.info("=" * 60)
    logging.info("🌐 ПОЛУЧЕН ВЕБХУК ОТ TELEGRAM")
    
    try:
        # Получаем сырые данные
        raw_data = request.get_data()
        logging.info(f"📦 Размер данных: {len(raw_data)} байт")
        
        if len(raw_data) == 0:
            logging.error("❌ Получены пустые данные")
            return 'Empty data', 400
            
        # Декодируем и парсим JSON
        json_str = raw_data.decode('utf-8')
        logging.info(f"📄 JSON строка (первые 200 символов): {json_str[:200]}")
        
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
            
            # ✅ РУЧНАЯ ОБРАБОТКА, если библиотека не работает
            if text == '/start' or text == '/help':
                try:
                    response = f"✅ Ручная обработка!\nВаш ID: {user_id}\nТекст: {text}"
                    bot.send_message(chat_id, response)
                    logging.info(f"✅ Ручной ответ отправлен в chat_id={chat_id}")
                except Exception as e:
                    logging.error(f"❌ Ошибка ручной отправки: {e}")
        
        # Пытаемся использовать библиотеку
        try:
            update = telebot.types.Update.de_json(json_str)
            if update and update.message:
                bot.process_new_updates([update])
                logging.info("🔄 Библиотека обработала обновление")
            else:
                logging.warning("⚠️ Библиотека не смогла обработать обновление")
        except Exception as lib_error:
            logging.error(f"❌ Ошибка библиотеки: {lib_error}")
        
    except json.JSONDecodeError as e:
        logging.error(f"❌ Ошибка декодирования JSON: {e}")
        return 'Invalid JSON', 400
    except Exception as e:
        logging.error(f"❌ Критическая ошибка в вебхуке: {e}", exc_info=True)
        return 'Server Error', 500
    
    logging.info("=" * 60)
    return 'OK', 200

# ✅ ТЕСТОВЫЕ МАРШРУТЫ

@app.route('/health')
def health():
    return 'OK', 200

@app.route('/')
def home():
    return '''
    <h1>🤖 Бот для клуба "Увлекательные чтения"</h1>
    <p>Статус: <strong>Работает</strong> ✅</p>
    <p>Версия: <strong>Ручная обработка</strong></p>
    <p><a href="/test_message">Тест отправки сообщения</a></p>
    '''

@app.route('/test_message')
def test_message():
    """Тестовая страница для проверки отправки сообщения"""
    try:
        # Пытаемся отправить сообщение в ваш личный чат
        bot.send_message(1039651708, "✅ Тестовое сообщение с сайта")
        return "Сообщение отправлено!"
    except Exception as e:
        return f"Ошибка отправки: {e}"

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)

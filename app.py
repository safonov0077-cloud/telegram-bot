import os
import logging
from flask import Flask, request
import json
import requests

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

app = Flask(__name__)

# Получаем токен
TOKEN = os.environ.get('TELEGRAM_TOKEN')
if not TOKEN:
    logging.error("❌ TELEGRAM_TOKEN не найден!")
    raise ValueError("TELEGRAM_TOKEN не найден")

logging.info(f"✅ Токен получен: {TOKEN[:15]}...")

@app.route('/webhook', methods=['POST'])
def webhook():
    logging.info("=" * 60)
    logging.info("🌐 НОВЫЙ ВЕБХУК ПОЛУЧЕН")
    
    try:
        # Получаем сырые данные
        raw_data = request.get_data()
        logging.info(f"📦 Размер данных: {len(raw_data)} байт")
        
        if len(raw_data) == 0:
            logging.error("❌ Пустые данные!")
            return 'Empty', 400
        
        # Показываем первые 300 символов
        json_str = raw_data.decode('utf-8')
        logging.info(f"📄 Данные (первые 300 символов): {json_str[:300]}")
        
        # Парсим JSON
        data = json.loads(json_str)
        logging.info(f"📊 Ключи в JSON: {list(data.keys())}")
        
        # Проверяем, есть ли сообщение
        if 'message' in data:
            msg = data['message']
            chat_id = msg['chat']['id']
            user_id = msg['from']['id']
            text = msg.get('text', '')
            
            logging.info(f"💬 Сообщение: chat_id={chat_id}, user_id={user_id}, text='{text}'")
            
            # Отправляем ответ через Telegram API
            if text == '/start' or text == '/start@UvlekatelnyeChteniyaClubBot':
                response_text = f"✅ Бот работает!\nВаш ID: {user_id}\nChat ID: {chat_id}"
            else:
                response_text = f"📝 Вы написали: {text}"
            
            # Отправка через прямой API запрос
            api_url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
            payload = {
                'chat_id': chat_id,
                'text': response_text
            }
            
            try:
                resp = requests.post(api_url, json=payload, timeout=10)
                logging.info(f"📤 Ответ отправлен. Статус: {resp.status_code}")
                if resp.status_code != 200:
                    logging.error(f"❌ Ошибка API: {resp.text}")
            except Exception as e:
                logging.error(f"❌ Ошибка отправки: {e}")
        else:
            logging.warning(f"⚠️ Нет поля 'message'. Весь JSON: {json.dumps(data, indent=2)}")
        
    except json.JSONDecodeError as e:
        logging.error(f"❌ Ошибка JSON: {e}")
        return 'Bad JSON', 400
    except Exception as e:
        logging.error(f"❌ Общая ошибка: {e}", exc_info=True)
        return 'Error', 500
    
    logging.info("=" * 60)
    return 'OK', 200

@app.route('/health')
def health():
    return 'OK', 200

@app.route('/')
def home():
    return '''
    <h1>🤖 Бот для клуба "Увлекательные чтения"</h1>
    <p><strong>Статус: Работает (прямой API)</strong></p>
    <p>Отправьте /start боту в Telegram</p>
    <p>Логи показывают детали каждого вебхука</p>
    '''

@app.route('/test')
def test():
    """Тестовая отправка сообщения"""
    try:
        test_url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
        test_data = {
            'chat_id': 1039651708,  # Ваш ID
            'text': '✅ Тест с сайта render.com'
        }
        resp = requests.post(test_url, json=test_data)
        return f"Тест отправлен: {resp.status_code}"
    except Exception as e:
        return f"Ошибка теста: {e}"

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)

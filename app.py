import os
import logging
from flask import Flask, request, jsonify
import requests

# Настройка логирования
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

app = Flask(__name__)

# Получение токена
TOKEN = os.environ.get('TELEGRAM_TOKEN')
if not TOKEN:
    logging.error("TELEGRAM_TOKEN не найден!")
    raise ValueError("TELEGRAM_TOKEN не найден!")

TELEGRAM_API_URL = f'https://api.telegram.org/bot{TOKEN}'

@app.route('/')
def index():
    return "Telegram Bot is running!"

@app.route('/webhook', methods=['POST'])
def webhook():
    """Обработка входящих сообщений от Telegram"""
    try:
        data = request.get_json()
        logging.info(f"Получен webhook: {data}")
        
        if 'message' in data:
            chat_id = data['message']['chat']['id']
            text = data['message'].get('text', '')
            
            if text:
                # Ответ на сообщение
                response_text = f"Вы написали: {text}"
                send_message(chat_id, response_text)
                logging.info(f"Отправлен ответ: {response_text}")
        
        return jsonify({'ok': True})
    
    except Exception as e:
        logging.error(f"Ошибка в webhook: {e}")
        return jsonify({'ok': False, 'error': str(e)})

def send_message(chat_id, text):
    """Отправка сообщения в Telegram"""
    url = f'{TELEGRAM_API_URL}/sendMessage'
    data = {
        'chat_id': chat_id,
        'text': text
    }
    response = requests.post(url, json=data)
    return response.json()

@app.route('/set_webhook', methods=['GET'])
def set_webhook():
    """Установка webhook для Telegram"""
    try:
        # Получаем URL из переменных окружения (для Render)
        webhook_url = os.environ.get('WEBHOOK_URL', 
                                     request.host_url.replace('http://', 'https://') + 'webhook')
        
        # Если не https, попробуем сделать через ngrok для локальной разработки
        if not webhook_url.startswith('https'):
            webhook_url = webhook_url.replace('http://', 'https://')
            logging.warning(f"Используем https вместо http: {webhook_url}")
        
        url = f'{TELEGRAM_API_URL}/setWebhook?url={webhook_url}'
        response = requests.get(url)
        result = response.json()
        
        logging.info(f"Webhook установлен: {result}")
        return jsonify(result)
    
    except Exception as e:
        logging.error(f"Ошибка установки webhook: {e}")
        return jsonify({'error': str(e)})

@app.route('/remove_webhook', methods=['GET'])
def remove_webhook():
    """Удаление webhook"""
    try:
        url = f'{TELEGRAM_API_URL}/deleteWebhook'
        response = requests.get(url)
        return jsonify(response.json())
    except Exception as e:
        return jsonify({'error': str(e)})

if __name__ == '__main__':
    # Для локальной разработки
    app.run(host='0.0.0.0', port=5000, debug=True)

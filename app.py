import os
import logging
from flask import Flask, request, jsonify
import json
import requests
from datetime import datetime

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

app = Flask(__name__)

# Токен из переменных окружения
TOKEN = os.environ.get('TELEGRAM_TOKEN')
if not TOKEN:
    logging.error("❌ TELEGRAM_TOKEN не найден!")
    raise ValueError("Установите TELEGRAM_TOKEN в переменных окружения")

logging.info(f"✅ Токен загружен: {TOKEN[:10]}...")

@app.route('/')
def index():
    return '''
    <h1>🤖 Бот для клуба "Увлекательные чтения"</h1>
    <p><strong>Статус: Работает</strong></p>
    <p>Вебхук: /webhook</p>
    <p>Проверка здоровья: <a href="/health">/health</a></p>
    '''

@app.route('/webhook', methods=['POST'])
def webhook():
    try:
        data = request.get_json()
        logging.info(f"Получен вебхук: {data}")
        
        if 'message' in data:
            chat_id = data['message']['chat']['id']
            text = data['message'].get('text', '')
            
            # Отправляем ответ
            url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
            payload = {
                'chat_id': chat_id,
                'text': f'Вы написали: {text}' if text else 'Получено сообщение!'
            }
            requests.post(url, json=payload)
            
        return jsonify({'status': 'ok'}), 200
    except Exception as e:
        logging.error(f"Ошибка: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/health')
def health():
    """Проверка здоровья - ОБЯЗАТЕЛЬНЫЙ эндпоинт для Render"""
    return jsonify({
        'status': 'healthy',
        'service': 'telegram-bot-club',
        'timestamp': datetime.now().isoformat()
    }), 200

@app.route('/test')
def test():
    """Тестовый эндпоинт"""
    return 'Тест пройден!', 200

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)

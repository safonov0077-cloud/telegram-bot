import os
import logging
from flask import Flask, request, jsonify
import json
import requests
from datetime import datetime

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Токен из переменных окружения
TOKEN = os.environ.get('TELEGRAM_TOKEN')

if not TOKEN:
    logger.error("❌ TELEGRAM_TOKEN не найден в переменных окружения!")
    # Не падаем, но логируем ошибку
else:
    logger.info(f"✅ Токен получен (первые 10 символов): {TOKEN[:10]}...")

@app.route('/')
def home():
    return '''
    <!DOCTYPE html>
    <html>
    <head>
        <title>Бот для клуба чтения</title>
        <style>
            body { font-family: Arial, sans-serif; padding: 20px; }
            .status { color: green; font-weight: bold; }
        </style>
    </head>
    <body>
        <h1>🤖 Бот для клуба "Увлекательные чтения"</h1>
        <p class="status">✅ Сервис работает</p>
        <p>Токен загружен: ''' + ("Да" if TOKEN else "Нет") + '''</p>
        <p>Проверка здоровья: <a href="/health">/health</a></p>
        <p>Вебхук: <code>/webhook</code></p>
    </body>
    </html>
    '''

@app.route('/health')
def health():
    """ОБЯЗАТЕЛЬНЫЙ эндпоинт для Render"""
    return jsonify({
        'status': 'healthy',
        'service': 'telegram-bot-club',
        'timestamp': datetime.now().isoformat(),
        'token_configured': bool(TOKEN)
    }), 200

@app.route('/webhook', methods=['POST'])
def webhook():
    """Обработчик вебхуков от Telegram"""
    try:
        data = request.get_json()
        logger.info(f"📨 Получен вебхук")
        
        if not TOKEN:
            return jsonify({'error': 'Token not configured'}), 500
        
        if 'message' in data:
            chat_id = data['message']['chat']['id']
            text = data['message'].get('text', '')
            user_name = data['message']['from'].get('first_name', 'Пользователь')
            
            logger.info(f"👤 {user_name}: {text}")
            
            # Ответ на команду /start
            response_text = f"Привет, {user_name}! Вы написали: {text}"
            if text == '/start':
                response_text = f"👋 Добро пожаловать, {user_name}!\n\nЯ бот клуба 'Увлекательные чтения'.\nИспользуйте /help для списка команд."
            elif text == '/help':
                response_text = "📚 Доступные команды:\n/start - Начать\n/help - Помощь\n/about - О клубе"
            
            # Отправляем ответ
            url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
            payload = {
                'chat_id': chat_id,
                'text': response_text,
                'parse_mode': 'HTML'
            }
            
            requests.post(url, json=payload, timeout=5)
            logger.info(f"📤 Ответ отправлен в чат {chat_id}")
            
        return jsonify({'status': 'ok'}), 200
        
    except Exception as e:
        logger.error(f"❌ Ошибка в вебхуке: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/test')
def test():
    """Тестовый эндпоинт"""
    return jsonify({'message': 'Test passed!'}), 200

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    logger.info(f"🚀 Запуск сервера на порту {port}")
    app.run(host='0.0.0.0', port=port)

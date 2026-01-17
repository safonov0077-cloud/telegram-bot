import os
import logging
import json
import requests
import random
import string
from datetime import datetime, timedelta
from flask import Flask, request, jsonify, render_template_string
from collections import defaultdict, deque
import threading
import time
import re

# ============ НАСТРОЙКА ============

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bot.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Создаем Flask приложение
app = Flask(__name__)

# Конфигурация
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN', '8585583418:...')
GROUP_ID = '@uvlekatelnyechteniya'  # ID группы
ADMIN_IDS = [1039651708]  # ID администраторов
GROUP_TOPICS = {
    'announcements': 1,    # Объявления
    'rules': 2,            # Правила
    'queue': 3,            # Очередь публикаций
    'reading_list': 4,     # Лист чтения дня
    'feedback': 5,         # Фидбек
    'duels': 6,            # Дуэли
    'games': 7,            # Игры дня
    'shop': 8,             # Магазин
    'offtop': 9,           # Оффтоп
}

# ============ ХРАНЕНИЕ ДАННЫХ ============

# В памяти (временное решение, потом заменим на БД)
users = {}  # user_id -> user_data
articles_queue = deque(maxlen=10)  # Очередь статей
published_articles = []  # Опубликованные сегодня статьи
user_articles = defaultdict(list)  # user_id -> список статей
user_balances = defaultdict(int)  # user_id -> баланс кавычек
user_last_submit = {}  # user_id -> время последней подачи
user_daily_reward = {}  # user_id -> дата последней награды
games_history = []  # История игр
duels = []  # Активные дуэли

# ============ ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ============

def send_telegram_message(chat_id, text, reply_to_message_id=None, topic_id=None, parse_mode='HTML'):
    """Отправка сообщения в Telegram"""
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    
    payload = {
        'chat_id': chat_id,
        'text': text,
        'parse_mode': parse_mode
    }
    
    if reply_to_message_id:
        payload['reply_to_message_id'] = reply_to_message_id
    
    if topic_id and (chat_id == GROUP_ID or str(chat_id).startswith('@') or (isinstance(chat_id, int) and chat_id < 0)):
        payload['message_thread_id'] = topic_id
    
    try:
        response = requests.post(url, json=payload, timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"Ошибка отправки сообщения: {e}")
        return None

def is_user_registered(user_id):
    """Проверка регистрации пользователя"""
    return user_id in users

def register_user(user_data):
    """Регистрация нового пользователя"""
    user_id = user_data['id']
    users[user_id] = {
        'id': user_id,
        'username': user_data.get('username'),
        'first_name': user_data.get('first_name', ''),
        'last_name': user_data.get('last_name', ''),
        'registered_at': datetime.now().isoformat(),
        'articles_count': 0,
        'feedback_given': 0,
        'feedback_received': 0,
        'games_played': 0,
        'duels_won': 0,
        'total_quotes': 0,
        'badges': ['новичок'],
        'subscription': 'free',
        'last_active': datetime.now().isoformat()
    }
    user_balances[user_id] = 50  # Стартовый бонус
    
    # Отправляем приветственное сообщение
    welcome_text = f"""
🎉 <b>Добро пожаловать в клуб "Увлекательные чтения"!</b>

<b>👤 Ваш профиль:</b>
├ Имя: {user_data.get('first_name', '')} {user_data.get('last_name', '')}
├ Юзернейм: @{user_data.get('username', 'нет')}
└ ID: {user_id}

<b>💰 Стартовый бонус:</b> 50 кавычек!

<b>📚 Что делать дальше:</b>
1. 📖 Прочитать правила: /rules
2. 📋 Посмотреть очередь: /queue  
3. 📝 Подать статью: /submit
4. 🎮 Поиграть: /game
5. 👤 Посмотреть профиль: /profile

<b>🎯 Наша философия:</b>
"Не обмен лайками, а реальное чтение и поддержка"

<b>📈 Ваш прогресс:</b>
├ Статьи: 0
├ Фидбеков получено: 0  
├ Фидбеков дано: 0
└ Кавычек: 50

Удачи в творчестве! ✍️
    """
    
    send_telegram_message(user_id, welcome_text)
    logger.info(f"✅ Зарегистрирован новый пользователь: {user_id}")
    return True

def can_submit_article(user_id):
    """Может ли пользователь подать статью"""
    if user_id not in user_last_submit:
        return True, "Можно подавать"
    
    last_submit = user_last_submit[user_id]
    time_diff = datetime.now() - last_submit
    
    # Проверяем временное ограничение (48-72 часа)
    min_hours = 48
    max_hours = 72
    
    if time_diff.total_seconds() < min_hours * 3600:
        hours_left = int((min_hours * 3600 - time_diff.total_seconds()) / 3600)
        return False, f"⏳ Вы сможете подать следующую статью через {hours_left} часов"
    
    # Проверяем, нет ли уже активной статьи в очереди
    user_active_articles = [a for a in articles_queue if a['user_id'] == user_id]
    if user_active_articles:
        return False, "⚠️ У вас уже есть статья в очереди"
    
    # Проверяем лимит очереди (макс 10 статей)
    if len(articles_queue) >= 10:
        return False, "📊 Очередь переполнена (макс 10 статей)"
    
    return True, "Можно подавать"

def add_article_to_queue(user_id, title, description, content):
    """Добавление статьи в очередь"""
    article_id = f"art_{len(articles_queue)}_{user_id}"
    
    article = {
        'id': article_id,
        'user_id': user_id,
        'title': title,
        'description': description,
        'content': content,
        'submitted_at': datetime.now().isoformat(),
        'status': 'pending',
        'feedback_count': 0,
        'reads': 0,
        'likes': 0
    }
    
    articles_queue.append(article)
    user_articles[user_id].append(article)
    user_last_submit[user_id] = datetime.now()
    
    # Начисляем кавычки за подачу статьи
    add_quotes(user_id, 10, "Подача статьи")
    
    # Обновляем статистику пользователя
    users[user_id]['articles_count'] += 1
    
    logger.info(f"📝 Статья добавлена в очередь: {article_id}")
    return article_id

def publish_daily_reading_list():
    """Публикация ежедневного листа чтения"""
    if not articles_queue:
        return "📭 Очередь статей пуста"
    
    # Берем до 5 статей из очереди
    today_articles = list(articles_queue)[:5]
    
    reading_list_text = f"""
📚 <b>ЛИСТ ЧТЕНИЯ НА {datetime.now().strftime('%d.%m.%Y')}</b>
────────────────────

<i>Сегодня для чтения {len(today_articles)} статей:</i>
    """
    
    for i, article in enumerate(today_articles, 1):
        user = users.get(article['user_id'], {})
        username = f"@{user.get('username', 'пользователь')}" if user.get('username') else "пользователь"
        
        reading_list_text += f"""
<b>{i}. {article['title']}</b>
├ Автор: {username}
├ Описание: {article['description'][:100]}...
└ 🔗 Ссылка: [читать статью]({article['content']})
        """
    
    reading_list_text += """
────────────────────
<b>🎯 ЗАДАНИЕ НА СЕГОДНЯ:</b>
1. Прочитайте минимум 1 статью из списка
2. Оставьте конструктивный фидбек
3. Получите 5 кавычек за каждый фидбек

<b>💬 КАК ДАВАТЬ ФИДБЕК:</b>
• Что понравилось?
• Что можно улучшить?
• Самый яркий момент
• Рекомендации автору

<b>⏰ НАПОМИНАНИЕ:</b>
Фидбек можно оставлять до 23:59 МСК
"""
    
    # Публикуем в теме "Лист чтения дня"
    send_telegram_message(GROUP_ID, reading_list_text, topic_id=GROUP_TOPICS['reading_list'])
    
    # Помечаем статьи как опубликованные
    for article in today_articles:
        article['status'] = 'published'
        article['published_at'] = datetime.now().isoformat()
        published_articles.append(article)
    
    # Удаляем опубликованные статьи из очереди
    for _ in range(len(today_articles)):
        if articles_queue:
            articles_queue.popleft()
    
    logger.info(f"📚 Опубликован лист чтения: {len(today_articles)} статей")
    return f"Опубликовано {len(today_articles)} статей"

def add_quotes(user_id, amount, reason):
    """Добавление кавычек пользователю"""
    user_balances[user_id] += amount
    users[user_id]['total_quotes'] += amount
    
    # Проверяем достижения
    check_achievements(user_id)
    
    logger.info(f"💰 Пользователю {user_id} добавлено {amount} кавычек: {reason}")
    return user_balances[user_id]

def check_achievements(user_id):
    """Проверка достижений пользователя"""
    user = users[user_id]
    new_badges = []
    
    # Бейджи за кавычки
    if user['total_quotes'] >= 1000 and 'магнат' not in user['badges']:
        new_badges.append('магнат')
    elif user['total_quotes'] >= 500 and 'богач' not in user['badges']:
        new_badges.append('богач')
    elif user['total_quotes'] >= 100 and 'состоятельный' not in user['badges']:
        new_badges.append('состоятельный')
    
    # Бейджи за статьи
    if user['articles_count'] >= 50 and 'прозаик' not in user['badges']:
        new_badges.append('прозаик')
    elif user['articles_count'] >= 20 and 'писатель' not in user['badges']:
        new_badges.append('писатель')
    elif user['articles_count'] >= 10 and 'автор' not in user['badges']:
        new_badges.append('автор')
    
    # Бейджи за фидбек
    if user['feedback_given'] >= 100 and 'наставник' not in user['badges']:
        new_badges.append('наставник')
    elif user['feedback_given'] >= 50 and 'критик' not in user['badges']:
        new_badges.append('критик')
    elif user['feedback_given'] >= 20 and 'читатель' not in user['badges']:
        new_badges.append('читатель')
    
    # Добавляем новые бейджи
    for badge in new_badges:
        if badge not in user['badges']:
            user['badges'].append(badge)
            # Уведомляем пользователя
            badge_text = f"""
🎖 <b>НОВЫЙ БЕЙДЖ!</b>

Поздравляем! Вы получили бейдж:
<b>"{badge.upper()}"</b>

Продолжайте в том же духе! 💪
            """
            send_telegram_message(user_id, badge_text)

def get_user_top(limit=10):
    """Получение топа пользователей"""
    user_list = []
    for user_id, user_data in users.items():
        user_list.append({
            'id': user_id,
            'name': user_data['first_name'],
            'username': user_data['username'],
            'articles': user_data['articles_count'],
            'quotes': user_balances.get(user_id, 0),
            'feedback_given': user_data['feedback_given']
        })
    
    # Сортируем по кавычкам
    user_list.sort(key=lambda x: x['quotes'], reverse=True)
    return user_list[:limit]

# ============ ИГРЫ И АКТИВНОСТИ ============

def start_paragraph_duel(initiator_id, topic=None):
    """Начало дуэли абзацев"""
    if not topic:
        topics = [
            "Утро после конца света",
            "Разговор с зеркалом",
            "Письмо из прошлого",
            "Тайна старой библиотеки",
            "Последний день лета"
        ]
        topic = random.choice(topics)
    
    duel_id = f"duel_{len(duels)}_{int(time.time())}"
    
    duel = {
        'id': duel_id,
        'topic': topic,
        'initiator': initiator_id,
        'participants': [initiator_id],
        'paragraphs': {},
        'status': 'waiting',
        'created_at': datetime.now().isoformat(),
        'votes': {},
        'winner': None,
        'prize': 25
    }
    
    duels.append(duel)
    
    # Публикуем в теме "Дуэли"
    duel_text = f"""
⚔️ <b>НОВАЯ ДУЭЛЬ АБЗАЦЕВ!</b>
────────────────────
<b>Тема:</b> {topic}
<b>Инициатор:</b> @{users[initiator_id].get('username', 'пользователь')}
<b>Приз:</b> 25 кавычек 🪙

<b>📝 ПРАВИЛА:</b>
1. Напишите абзац (3-5 предложений) на заданную тему
2. Время на написание: 15 минут
3. После истечения времени - голосование
4. Победитель получает приз

<b>🎯 КАК УЧАСТВОВАТЬ:</b>
Ответьте на это сообщение своим абзацем
────────────────────
    """
    
    result = send_telegram_message(GROUP_ID, duel_text, topic_id=GROUP_TOPICS['duels'])
    if result and 'result' in result:
        duel['message_id'] = result['result']['message_id']
    
    # Запускаем таймер дуэли
    threading.Timer(900, finish_duel, args=[duel_id]).start()  # 15 минут
    
    logger.info(f"⚔️ Начата дуэль: {duel_id}")
    return duel_id

def finish_duel(duel_id):
    """Завершение дуэли и определение победителя"""
    duel = next((d for d in duels if d['id'] == duel_id), None)
    if not duel or duel['status'] != 'waiting':
        return
    
    duel['status'] = 'voting'
    
    if len(duel['paragraphs']) < 2:
        # Недостаточно участников
        result_text = f"""
⚔️ <b>ДУЭЛЬ ЗАВЕРШЕНА</b>
────────────────────
<b>Тема:</b> {duel['topic']}

⚠️ Недостаточно участников для голосования.
Дуэль отменена.

Попробуйте начать новую дуэль! /duel
────────────────────
        """
        duel['status'] = 'cancelled'
    else:
        # Формируем сообщение для голосования
        result_text = f"""
⚔️ <b>ГОЛОСОВАНИЕ В ДУЭЛИ!</b>
────────────────────
<b>Тема:</b> {duel['topic']}
<b>Участников:</b> {len(duel['paragraphs'])}

<b>📝 РАБОТЫ УЧАСТНИКОВ:</b>
        """
        
        for i, (user_id, paragraph) in enumerate(duel['paragraphs'].items(), 1):
            username = f"@{users[user_id].get('username', 'пользователь')}" if users.get(user_id) else "пользователь"
            result_text += f"""
<b>Абзац #{i} (автор: {username}):</b>
{paragraph[:200]}...
────────────────────
            """
        
        result_text += """
<b>🎯 КАК ГОЛОСОВАТЬ:</b>
Ответьте номером понравившегося абзаца (1, 2, 3...)
Время голосования: 10 минут
        """
    
    # Публикуем результат
    send_telegram_message(GROUP_ID, result_text, 
                         reply_to_message_id=duel.get('message_id'),
                         topic_id=GROUP_TOPICS['duels'])
    
    # Запускаем таймер голосования
    threading.Timer(600, count_duel_votes, args=[duel_id]).start()  # 10 минут

def count_duel_votes(duel_id):
    """Подсчет голосов в дуэли"""
    duel = next((d for d in duels if d['id'] == duel_id), None)
    if not duel or duel['status'] != 'voting':
        return
    
    # Подсчитываем голоса
    votes_count = {}
    for voter_id, vote in duel['votes'].items():
        if vote in votes_count:
            votes_count[vote] += 1
        else:
            votes_count[vote] = 1
    
    if votes_count:
        winner_vote = max(votes_count.items(), key=lambda x: x[1])
        winner_index = winner_vote[0]
        
        # Находим user_id победителя
        participants = list(duel['paragraphs'].keys())
        if 0 < winner_index <= len(participants):
            winner_id = participants[winner_index - 1]
            duel['winner'] = winner_id
            
            # Начисляем приз
            add_quotes(winner_id, duel['prize'], "Победа в дуэли")
            users[winner_id]['duels_won'] += 1
            
            # Формируем текст результата
            winner_name = f"@{users[winner_id].get('username', 'пользователь')}" if users.get(winner_id) else "пользователь"
            result_text = f"""
🏆 <b>ДУЭЛЬ ЗАВЕРШЕНА!</b>
────────────────────
<b>ПОБЕДИТЕЛЬ:</b> {winner_name}
<b>Голосов:</b> {winner_vote[1]}
<b>Приз:</b> {duel['prize']} кавычек 🪙

Поздравляем победителя! 🎉

<b>📊 СТАТИСТИКА ГОЛОСОВАНИЯ:</b>
            """
            
            for vote, count in votes_count.items():
                result_text += f"Абзац #{vote}: {count} голосов\n"
            
            result_text += """
────────────────────
Спасибо всем участникам! ✨
            """
        else:
            result_text = "Ошибка определения победителя"
    else:
        result_text = "Голосов не было"
    
    duel['status'] = 'finished'
    
    # Публикуем результат
    send_telegram_message(GROUP_ID, result_text, 
                         reply_to_message_id=duel.get('message_id'),
                         topic_id=GROUP_TOPICS['duels'])

def truth_or_lie_game():
    """Игра 'Правда или выдумка'"""
    facts = [
        {"fact": "Первый роман, написанный на пишущей машинке, - 'Приключения Тома Сойера'", "truth": True},
        {"fact": "Стивен Кинг написал 'Сияние' под псевдонимом", "truth": False},
        {"fact": "Джейн Остин издавала свои романы анонимно", "truth": True},
        {"fact": "Рукопись 'Войны и мира' умещается в одной тетради", "truth": False},
        {"fact": "Агата Кристи работала фармацевтом во время войны", "truth": True},
        {"fact": "Эрнест Хемингуэй написал 'Старик и море' за одну ночь", "truth": False},
        {"fact": "Шекспир придумал более 1700 английских слов", "truth": True},
        {"fact": "Достоевский написал 'Преступление и наказание' за две недели", "truth": False},
    ]
    
    game_fact = random.choice(facts)
    
    game_text = f"""
🎮 <b>ИГРА ДНЯ: ПРАВДА ИЛИ ВЫДУМКА?</b>
────────────────────
<b>ФАКТ:</b>
{game_fact['fact']}

<b>🎯 ВАША ЗАДАЧА:</b>
Определите, правда это или выдумка!

<b>📝 КАК ИГРАТЬ:</b>
Ответьте на это сообщение:
• <b>Правда</b> - если думаете, что это правда
• <b>Выдумка</b> - если думаете, что это выдумка

<b>⏰ ВРЕМЯ:</b> 10 минут
<b>🎁 ПРИЗ:</b> 10 кавычек за правильный ответ
────────────────────
    """
    
    result = send_telegram_message(GROUP_ID, game_text, topic_id=GROUP_TOPICS['games'])
    
    # Сохраняем игру
    game_id = f"game_{len(games_history)}_{int(time.time())}"
    games_history.append({
        'id': game_id,
        'type': 'truth_or_lie',
        'fact': game_fact['fact'],
        'truth': game_fact['truth'],
        'message_id': result['result']['message_id'] if result and 'result' in result else None,
        'created_at': datetime.now().isoformat(),
        'participants': {},
        'prize': 10
    })
    
    # Запускаем таймер игры
    threading.Timer(600, finish_truth_game, args=[game_id]).start()
    
    logger.info(f"🎮 Запущена игра: {game_id}")
    return game_id

def finish_truth_game(game_id):
    """Завершение игры 'Правда или выдумка'"""
    game = next((g for g in games_history if g['id'] == game_id), None)
    if not game:
        return
    
    # Определяем правильный ответ
    correct_answer = "Правда" if game['truth'] else "Выдумка"
    
    # Находим победителей
    winners = []
    for user_id, answer in game['participants'].items():
        if answer.lower() == correct_answer.lower():
            winners.append(user_id)
            add_quotes(user_id, game['prize'], "Победа в игре")
    
    # Формируем результат
    result_text = f"""
🏆 <b>ИГРА ЗАВЕРШЕНА!</b>
────────────────────
<b>ФАКТ:</b> {game['fact']}
<b>ПРАВИЛЬНЫЙ ОТВЕТ:</b> {correct_answer}

<b>🎉 ПОБЕДИТЕЛИ:</b> {len(winners)} участников
    """
    
    if winners:
        result_text += "\n"
        for i, winner_id in enumerate(winners[:5], 1):  # Показываем первые 5
            username = f"@{users[winner_id].get('username', 'пользователь')}" if users.get(winner_id) else "пользователь"
            result_text += f"{i}. {username}\n"
        
        if len(winners) > 5:
            result_text += f"... и еще {len(winners) - 5} участников\n"
        
        result_text += f"\n<b>🎁 Каждый получает:</b> {game['prize']} кавычек"
    else:
        result_text += "\n😢 Победителей нет"
    
    result_text += """
────────────────────
Спасибо за участие! ✨
Следующая игра через 6 часов.
    """
    
    # Публикуем результат
    send_telegram_message(GROUP_ID, result_text, 
                         reply_to_message_id=game.get('message_id'),
                         topic_id=GROUP_TOPICS['games'])

def wheel_of_themes_game():
    """Колесо тем для мини-текстов"""
    themes = [
        "Неожиданная находка",
        "Разговор с незнакомцем",
        "Старая фотография",
        "Закрытая дверь",
        "Последний шанс",
        "Утраченное письмо",
        "Тайный знак",
        "Несбывшееся предсказание",
        "Ночное путешествие",
        "Забытый талант"
    ]
    
    selected_themes = random.sample(themes, 3)
    
    game_text = f"""
🎡 <b>КОЛЕСО ТЕМ</b>
────────────────────
<b>🎯 ЗАДАНИЕ:</b>
Напишите мини-текст (3-5 предложений) на одну из тем ниже.

<b>🎨 ТЕМЫ НА СЕГОДНЯ:</b>
1. {selected_themes[0]}
2. {selected_themes[1]}
3. {selected_themes[2]}

<b>📝 КАК УЧАСТВОВАТЬ:</b>
Ответьте на это сообщение своим текстом, указав номер темы.

<b>⏰ ВРЕМЯ:</b> 30 минут
<b>🎁 ПРИЗ:</b> 15 кавычек за лучший текст
<b>📊 КРИТЕРИИ:</b> оригинальность, выразительность, завершенность
────────────────────
    """
    
    result = send_telegram_message(GROUP_ID, game_text, topic_id=GROUP_TOPICS['games'])
    
    # Сохраняем игру
    game_id = f"wheel_{len(games_history)}_{int(time.time())}"
    games_history.append({
        'id': game_id,
        'type': 'wheel_of_themes',
        'themes': selected_themes,
        'message_id': result['result']['message_id'] if result and 'result' in result else None,
        'created_at': datetime.now().isoformat(),
        'participants': {},
        'prize': 15
    })
    
    # Запускаем таймер игры
    threading.Timer(1800, finish_wheel_game, args=[game_id]).start()  # 30 минут
    
    return game_id

def finish_wheel_game(game_id):
    """Завершение игры 'Колесо тем'"""
    game = next((g for g in games_history if g['id'] == game_id), None)
    if not game:
        return
    
    # Выбираем победителя (случайно из участников)
    if game['participants']:
        winner_id = random.choice(list(game['participants'].keys()))
        winner_text = game['participants'][winner_id]['text']
        theme_num = game['participants'][winner_id]['theme']
        
        # Начисляем приз
        add_quotes(winner_id, game['prize'], "Победа в Колесе тем")
        
        username = f"@{users[winner_id].get('username', 'пользователь')}" if users.get(winner_id) else "пользователь"
        theme = game['themes'][theme_num - 1]
        
        result_text = f"""
🏆 <b>КОЛЕСО ТЕМ ЗАВЕРШЕНО!</b>
────────────────────
<b>ПОБЕДИТЕЛЬ:</b> {username}
<b>Тема:</b> {theme}

<b>📖 ТЕКСТ-ПОБЕДИТЕЛЬ:</b>
{winner_text[:300]}...

<b>🎁 ПРИЗ:</b> {game['prize']} кавычек
────────────────────
<b>🎯 ЗАМЕЧАНИЯ:</b>
• Отличная образность!
• Завершенный сюжет
• Яркие детали

Спасибо всем участникам! ✍️
        """
    else:
        result_text = """
😢 <b>КОЛЕСО ТЕМ ЗАВЕРШЕНО</b>
────────────────────
Участников не было.

Попробуйте в следующий раз! 🎡
        """
    
    # Публикуем результат
    send_telegram_message(GROUP_ID, result_text, 
                         reply_to_message_id=game.get('message_id'),
                         topic_id=GROUP_TOPICS['games'])

# ============ ОБРАБОТЧИКИ КОМАНД ============

@app.route('/webhook', methods=['POST'])
def webhook():
    """Обработчик вебхуков от Telegram"""
    try:
        data = request.get_json()
        logger.info(f"📨 Получен вебхук: {data.keys()}")
        
        # Обработка сообщений
        if 'message' in data:
            process_message(data['message'])
        
        # Обработка callback запросов (нажатия на кнопки)
        elif 'callback_query' in data:
            process_callback(data['callback_query'])
        
        return jsonify({'status': 'ok'}), 200
        
    except Exception as e:
        logger.error(f"❌ Ошибка обработки вебхука: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500

def process_message(message):
    """Обработка входящих сообщений"""
    chat_id = message['chat']['id']
    user_id = message['from']['id']
    text = message.get('text', '')
    
    # Обновляем время последней активности
    if user_id in users:
        users[user_id]['last_active'] = datetime.now().isoformat()
    
    # Проверяем, является ли сообщение ответом в дуэли
    if 'reply_to_message' in message:
        process_reply(message)
        return
    
    # Обработка команд
    if text.startswith('/'):
        process_command(chat_id, user_id, text, message)
    else:
        # Обработка обычных сообщений
        if chat_id == user_id:  # Личное сообщение
            send_telegram_message(user_id, "Напишите /help для списка команд")
        # В группе обычные сообщения не обрабатываем

def process_command(chat_id, user_id, text, message):
    """Обработка команд"""
    command = text.split()[0].lower()
    
    # Регистрируем пользователя при первой команде
    if not is_user_registered(user_id) and command not in ['/start', '/help']:
        send_telegram_message(chat_id, "Сначала зарегистрируйтесь командой /start")
        return
    
    # Обработка команд
    if command == '/start':
        if is_user_registered(user_id):
            send_telegram_message(chat_id, "Вы уже зарегистрированы! Используйте /help для списка команд")
        else:
            user_data = {
                'id': user_id,
                'username': message['from'].get('username'),
                'first_name': message['from'].get('first_name', ''),
                'last_name': message['from'].get('last_name', '')
            }
            register_user(user_data)
    
    elif command == '/help':
        show_help(chat_id, user_id)
    
    elif command == '/rules':
        show_rules(chat_id)
    
    elif command == '/queue':
        show_queue(chat_id)
    
    elif command == '/submit':
        if chat_id != user_id:
            send_telegram_message(chat_id, "Эта команда работает только в личных сообщениях с ботом")
        else:
            start_article_submission(user_id)
    
    elif command == '/my_posts':
        show_my_posts(user_id)
    
    elif command == '/when_can_submit':
        check_submit_time(user_id)
    
    elif command == '/profile':
        show_profile(user_id)
    
    elif command == '/daily':
        give_daily_reward(user_id)
    
    elif command == '/balance':
        show_balance(user_id)
    
    elif command == '/top':
        show_top(chat_id)
    
    elif command == '/game':
        show_games_menu(chat_id, user_id)
    
    elif command == '/duel':
        if str(chat_id) == GROUP_ID or chat_id < 0:  # Групповой чат
            start_paragraph_duel(user_id)
        else:
            send_telegram_message(chat_id, "Дуэли работают только в группе клуба")
    
    elif command == '/admin_stats' and user_id in ADMIN_IDS:
        show_admin_stats(user_id)
    
    elif command == '/publish_reading_list' and user_id in ADMIN_IDS:
        result = publish_daily_reading_list()
        send_telegram_message(user_id, result)
    
    elif command == '/announce' and user_id in ADMIN_IDS:
        if len(text.split()) > 1:
            announcement = text.split(' ', 1)[1]
            make_announcement(announcement)
        else:
            send_telegram_message(user_id, "Использование: /announce [текст]")
    
    else:
        send_telegram_message(chat_id, "Неизвестная команда. Используйте /help")

def process_reply(message):
    """Обработка ответов на сообщения"""
    chat_id = message['chat']['id']
    user_id = message['from']['id']
    text = message.get('text', '')
    reply_to = message['reply_to_message']
    
    # Проверяем, является ли ответ дуэли
    for duel in duels:
        if duel.get('message_id') == reply_to['message_id'] and duel['status'] == 'waiting':
            # Участие в дуэли
            if user_id not in duel['participants']:
                duel['participants'].append(user_id)
            
            duel['paragraphs'][user_id] = text
            
            # Подтверждение участия
            send_telegram_message(user_id, "✅ Ваш абзац принят! Ждите результатов голосования.")
            return
    
    # Проверяем, является ли ответ игрой "Правда или выдумка"
    for game in games_history:
        if (game.get('message_id') == reply_to['message_id'] and 
            game['type'] == 'truth_or_lie' and
            'participants' in game):
            
            answer = text.lower().strip()
            if answer in ['правда', 'выдумка']:
                game['participants'][user_id] = answer
                send_telegram_message(user_id, "✅ Ваш ответ принят! Ждите результатов.")
            return
    
    # Проверяем, является ли ответ игрой "Колесо тем"
    for game in games_history:
        if (game.get('message_id') == reply_to['message_id'] and 
            game['type'] == 'wheel_of_themes' and
            'participants' in game):
            
            # Пытаемся определить номер темы
            match = re.search(r'^(\d+)[\s\.\)]*', text)
            if match:
                theme_num = int(match.group(1))
                if 1 <= theme_num <= 3:
                    game_text = text[match.end():].strip()
                    if game_text:
                        game['participants'][user_id] = {
                            'theme': theme_num,
                            'text': game_text
                        }
                        send_telegram_message(user_id, "✅ Ваш текст принят! Ждите результатов.")
                        return
            
            send_telegram_message(user_id, "Укажите номер темы в начале сообщения (1, 2 или 3)")
            return

def process_callback(callback):
    """Обработка callback запросов"""
    callback_id = callback['id']
    user_id = callback['from']['id']
    data = callback['data']
    
    # Здесь можно добавить обработку нажатий на inline-кнопки
    # Пока просто отвечаем
    send_telegram_message(user_id, f"Callback получен: {data}")
    
    # Подтверждаем получение callback
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/answerCallbackQuery"
    requests.post(url, json={'callback_query_id': callback_id})

# ============ КОМАНДЫ БОТА ============

def show_help(chat_id, user_id):
    """Показать помощь"""
    help_text = """
<b>📚 КЛУБ "УВЛЕКАТЕЛЬНЫЕ ЧТЕНИЯ"</b>
────────────────────
<b>👋 ДЛЯ НОВИЧКОВ:</b>
/start - Регистрация и приветствие
/rules - Правила клуба
/queue - Очередь публикаций
/help - Эта справка

<b>✍️ ДЛЯ АВТОРОВ:</b>
/submit - Подать статью в очередь (только в ЛС)
/my_posts - Мои статьи
/when_can_submit - Когда можно подать следующую статью
/profile - Мой профиль

<b>📖 ДЛЯ ЧИТАТЕЛЕЙ:</b>
/daily - Ежедневная награда (5 кавычек)
/balance - Баланс кавычек
/top - Топ участников

<b>🎮 ДЛЯ ИГРОКОВ:</b>
/game - Игры и активности
/duel - Начать дуэль (только в группе)

<b>🛠️ ДЛЯ АДМИНОВ:</b>
/admin_stats - Статистика
/publish_reading_list - Опубликовать лист чтения
/announce - Сделать объявление

<b>📞 ПОДДЕРЖКА:</b>
@SafonovAN74 - создатель клуба
────────────────────
<b>🎯 НАША ФИЛОСОФИЯ:</b>
"Не обмен лайками, а реальное чтение и поддержка"
    """
    
    send_telegram_message(chat_id, help_text)

def show_rules(chat_id):
    """Показать правила"""
    rules_text = """
<b>📜 ПРАВИЛА КЛУБА "УВЛЕКАТЕЛЬНЫЕ ЧТЕНИЯ"</b>
────────────────────
<b>🎯 ЦЕЛЬ:</b> Создать сообщество авторов без спама ссылок, с акцентом на качественный фидбек.

<b>📋 ОСНОВНЫЕ ПРАВИЛА:</b>

<b>1. 📝 Публикация статей:</b>
├ 1 статья раз в 48-72 часа
├ Максимум 1 активная статья в очереди
├ Всего 5-10 статей в день во всем клубе
└ Статьи проходят модерацию

<b>2. 💬 Фидбек:</b>
├ Обязательно для всех участников
├ Минимум 1 фидбек в день
├ Конструктивная критика приветствуется
└ Бесполезные комментарии удаляются

<b>3. ⚔️ Дуэли и игры:</b>
├ Участие по желанию
├ Уважительное отношение к соперникам
├ Честное голосование
└ Призы за победу

<b>4. 🏆 Рейтинг и кавычки:</b>
├ Кавычки - внутренняя валюта клуба
├ Начисляются за активность
├ Можно тратить в магазине
└ Рейтинг обновляется ежедневно

<b>5. 🚫 Запрещено:</b>
├ Спам и флуд
├ Оскорбления
├ Плагиат
├ Нарушение временных ограничений
└ Любая нечестная игра

<b>6. ✅ Разрешено:</b>
├ Обсуждение тем в соответствующих ветках
├ Предложения по улучшению клуба
├ Создание своих игр (с одобрения админов)
└ Дружеская атмосфера

<b>⚠️ НАРУШЕНИЕ ПРАВИЛ:</b>
1 предупреждение → 2 предупреждение → бан на неделю → перманентный бан.

<b>🙏 УВАЖАЙТЕ ДРУГ ДРУГА!</b>
────────────────────
    """
    
    send_telegram_message(chat_id, rules_text)

def show_queue(chat_id):
    """Показать очередь статей"""
    if not articles_queue:
        send_telegram_message(chat_id, "📭 Очередь статей пуста")
        return
    
    queue_text = "<b>📋 ОЧЕРЕДЬ ПУБЛИКАЦИЙ</b>\n────────────────────\n"
    
    for i, article in enumerate(list(articles_queue)[:5], 1):
        user = users.get(article['user_id'], {})
        username = f"@{user.get('username', 'пользователь')}" if user.get('username') else "пользователь"
        time_ago = (datetime.now() - datetime.fromisoformat(article['submitted_at'])).seconds // 3600
        
        queue_text += f"""
<b>{i}. {article['title']}</b>
├ Автор: {username}
├ В очереди: {time_ago} часов
├ Описание: {article['description'][:50]}...
└ Статус: {article['status']}
────────────────────
        """
    
    if len(articles_queue) > 5:
        queue_text += f"\n... и еще {len(articles_queue) - 5} статей в очереди"
    
    queue_text += """
    
<b>📊 СТАТИСТИКА:</b>
├ Всего в очереди: {} статей
├ Опубликовано сегодня: {} статей
└ Свободных мест: {} из 10
    """.format(len(articles_queue), len(published_articles), 10 - len(articles_queue))
    
    send_telegram_message(chat_id, queue_text)

def start_article_submission(user_id):
    """Начать процесс подачи статьи"""
    can_submit, message = can_submit_article(user_id)
    
    if not can_submit:
        send_telegram_message(user_id, message)
        return
    
    # Здесь должен быть диалог подачи статьи
    # Пока заглушка
    submit_text = """
<b>📝 ПОДАЧА СТАТЬИ</b>
────────────────────
<b>ТРЕБОВАНИЯ:</b>
1. Оригинальный текст (не плагиат)
2. Минимум 1000 символов
3. Завершенная мысль/история
4. Корректное оформление

<b>📊 ЛИМИТЫ:</b>
├ 1 статья раз в 48-72 часа
├ Максимум 1 статья в очереди
└ Очередь: 10 статей максимум

<b>🎁 НАГРАДА:</b>
+10 кавычек за подачу статьи
+5 кавычек за каждый полученный фидбек

<b>📋 КАК ПОДАТЬ:</b>
Отправьте мне сообщение в формате:

<b>ЗАГОЛОВОК</b>
Тема статьи

<b>ОПИСАНИЕ</b>
Краткое описание (2-3 предложения)

<b>ССЫЛКА</b>
Ссылка на статью (Telegram, Telegra.ph, Гугл Док и т.д.)

────────────────────
<b>Пример:</b>
<b>ЗАГОЛОВОК</b>
Как я написал свой первый роман

<b>ОПИСАНИЕ</b>
История о том, как я за 30 дней написал роман из 50 тысяч слов. Рассказываю о методике, сложностях и результатах.

<b>ССЫЛКА</b>
https://telegra.ph/kak-ya-napisal-svoj-pervyj-roman-01-01
    """
    
    send_telegram_message(user_id, submit_text)

def show_my_posts(user_id):
    """Показать статьи пользователя"""
    user_posts = user_articles.get(user_id, [])
    
    if not user_posts:
        send_telegram_message(user_id, "📭 У вас еще нет статей")
        return
    
    posts_text = f"<b>📚 МОИ СТАТЬИ ({len(user_posts)})</b>\n────────────────────\n"
    
    for i, post in enumerate(user_posts[-5:], 1):  # Последние 5 статей
        status_emoji = "✅" if post['status'] == 'published' else "⏳" if post['status'] == 'pending' else "❌"
        time_ago = (datetime.now() - datetime.fromisoformat(post['submitted_at'])).days
        
        posts_text += f"""
<b>{i}. {post['title']}</b>
├ Статус: {status_emoji} {post['status']}
├ Подана: {time_ago} дней назад
├ Фидбеков: {post['feedback_count']}
├ Прочтений: {post['reads']}
└ Лайков: {post['likes']}
────────────────────
        """
    
    if len(user_posts) > 5:
        posts_text += f"\n... и еще {len(user_posts) - 5} статей"
    
    posts_text += f"""
    
<b>📊 СТАТИСТИКА:</b>
├ Всего статей: {len(user_posts)}
├ Опубликовано: {len([p for p in user_posts if p['status'] == 'published'])}
├ В очереди: {len([p for p in user_posts if p['status'] == 'pending'])}
└ Получено фидбеков: {sum(p['feedback_count'] for p in user_posts)}
    """
    
    send_telegram_message(user_id, posts_text)

def check_submit_time(user_id):
    """Проверить, когда можно подать следующую статью"""
    if user_id not in user_last_submit:
        send_telegram_message(user_id, "✅ Вы можете подать статью прямо сейчас!")
        return
    
    last_submit = user_last_submit[user_id]
    time_diff = datetime.now() - last_submit
    hours_passed = time_diff.total_seconds() / 3600
    
    if hours_passed >= 48:
        send_telegram_message(user_id, "✅ Вы можете подать статью прямо сейчас!")
    else:
        hours_left = 48 - hours_passed
        send_telegram_message(user_id, f"⏳ Вы сможете подать следующую статью через {int(hours_left)} часов")

def show_profile(user_id):
    """Показать профиль пользователя"""
    if user_id not in users:
        send_telegram_message(user_id, "Сначала зарегистрируйтесь: /start")
        return
    
    user = users[user_id]
    
    # Вычисляем рейтинг
    total_users = len(users)
    user_list = get_user_top(total_users)
    user_rank = next((i+1 for i, u in enumerate(user_list) if u['id'] == user_id), total_users)
    
    profile_text = f"""
<b>👤 ПРОФИЛЬ</b>
────────────────────
<b>Имя:</b> {user['first_name']} {user['last_name']}
<b>Юзернейм:</b> @{user['username'] if user['username'] else 'не установлен'}
<b>В клубе с:</b> {datetime.fromisoformat(user['registered_at']).strftime('%d.%m.%Y')}

<b>📊 СТАТИСТИКА:</b>
├ Рейтинг: #{user_rank} из {total_users}
├ Статей: {user['articles_count']}
├ Фидбеков получено: {user['feedback_received']}
├ Фидбеков дано: {user['feedback_given']}
├ Игр сыграно: {user['games_played']}
├ Дуэлей выиграно: {user['duels_won']}
└ Всего кавычек: {user_balances.get(user_id, 0)}

<b>🎖 БЕЙДЖИ:</b>
{', '.join(user['badges']) if user['badges'] else 'пока нет бейджей'}

<b>💰 БАЛАНС:</b> {user_balances.get(user_id, 0)} кавычек

<b>📅 АКТИВНОСТЬ:</b>
Последняя активность: {datetime.fromisoformat(user['last_active']).strftime('%d.%m.%Y %H:%M')}
────────────────────
<b>🎯 ДОСТИЖЕНИЯ:</b>
    """
    
    # Проверяем близость к достижениям
    if user['articles_count'] < 10:
        profile_text += f"\n📝 До бейджа 'Автор': {10 - user['articles_count']} статей"
    if user['feedback_given'] < 20:
        profile_text += f"\n💬 До бейджа 'Читатель': {20 - user['feedback_given']} фидбеков"
    if user_balances.get(user_id, 0) < 100:
        profile_text += f"\n💰 До бейджа 'Состоятельный': {100 - user_balances.get(user_id, 0)} кавычек"
    
    send_telegram_message(user_id, profile_text)

def give_daily_reward(user_id):
    """Выдать ежедневную награду"""
    today = datetime.now().date().isoformat()
    
    if user_id in user_daily_reward and user_daily_reward[user_id] == today:
        send_telegram_message(user_id, "⏳ Вы уже получали награду сегодня. Приходите завтра!")
        return
    
    # Начисляем награду
    reward = 5
    add_quotes(user_id, reward, "Ежедневная награда")
    user_daily_reward[user_id] = today
    
    reward_text = f"""
🎁 <b>ЕЖЕДНЕВНАЯ НАГРАДА</b>
────────────────────
Вы получили: <b>{reward} кавычек</b>

<b>💰 Ваш баланс:</b> {user_balances.get(user_id, 0)} кавычек

<b>🎯 ЧТО ДЕЛАТЬ ДАЛЬШЕ:</b>
├ Прочитать лист чтения дня
├ Дать фидбек по одной статье
├ Участвовать в играх
└ Поддержать других авторов

Возвращайтесь завтра за новой наградой! ⏰
────────────────────
    """
    
    send_telegram_message(user_id, reward_text)

def show_balance(user_id):
    """Показать баланс кавычек"""
    balance = user_balances.get(user_id, 0)
    
    balance_text = f"""
💰 <b>ВАШ БАЛАНС</b>
────────────────────
<b>Кавычек:</b> {balance}

<b>🏆 ВАШЕ МЕСТО В ТОПЕ:</b>
    """
    
    # Получаем позицию в топе
    user_list = get_user_top(len(users))
    user_rank = next((i+1 for i, u in enumerate(user_list) if u['id'] == user_id), len(users))
    
    if user_rank <= 10:
        balance_text += f"#{user_rank} 🏅"
    elif user_rank <= 50:
        balance_text += f"#{user_rank} 🥈"
    else:
        balance_text += f"#{user_rank} 🥉"
    
    balance_text += f" из {len(users)} участников\n"
    
    balance_text += """
<b>🎁 ЧТО МОЖНО КУПИТЬ:</b>
(магазин скоро откроется)
├ Особые бейджи
├ Подарки для других
├ Участие в эксклюзивных играх
└ Приоритет в очереди

<b>💸 КАК ЗАРАБОТАТЬ:</b>
├ +10 за подачу статьи
├ +5 за качественный фидбек
├ +3 за участие в игре
├ +2 за прочтение статьи дня
└ +5 ежедневная награда
────────────────────
    """
    
    send_telegram_message(user_id, balance_text)

def show_top(chat_id):
    """Показать топ участников"""
    top_users = get_user_top(10)
    
    if not top_users:
        send_telegram_message(chat_id, "📭 Пока нет участников в топе")
        return
    
    top_text = "<b>🏆 ТОП УЧАСТНИКОВ</b>\n────────────────────\n"
    
    medals = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣", "🔟"]
    
    for i, user in enumerate(top_users[:10]):
        medal = medals[i] if i < len(medals) else f"{i+1}."
        username = f"@{user['username']}" if user['username'] else user['name']
        
        top_text += f"""
{medal} <b>{username}</b>
├ Кавычек: {user['quotes']}
├ Статей: {user['articles']}
└ Фидбеков: {user['feedback_given']}
────────────────────
        """
    
    top_text += f"""
<b>📊 СТАТИСТИКА КЛУБА:</b>
├ Участников: {len(users)}
├ Статей в очереди: {len(articles_queue)}
├ Опубликовано сегодня: {len(published_articles)}
└ Всего кавычек в системе: {sum(user_balances.values())}
────────────────────
    """
    
    send_telegram_message(chat_id, top_text)

def show_games_menu(chat_id, user_id):
    """Показать меню игр"""
    games_text = """
🎮 <b>ИГРЫ И АКТИВНОСТИ</b>
────────────────────
<b>🎯 ДОСТУПНЫЕ ИГРЫ:</b>

<b>1. ⚔️ Дуэль абзацев</b>
Напишите мини-текст на заданную тему
Команда: /duel (только в группе)

<b>2. 🎲 Правда или выдумка?</b>
Угадайте, правдив ли факт
Запускается автоматически 3 раза в день

<b>3. 🎡 Колесо тем</b>
Напишите текст на случайную тему
Запускается 2 раза в день

<b>4. 😄 Анекдот дня</b>
Развлекательный контент в оффтопе
Ежедневно в 20:00 МСК

<b>🎁 НАГРАДЫ:</b>
├ Победа в дуэли: 25 кавычек
├ Правильный ответ в игре: 10 кавычек
├ Участие в игре: 3 кавычки
└ Лучший текст в Колесе тем: 15 кавычек

<b>📅 РАСПИСАНИЕ:</b>
├ 10:00 - Дуэль абзацев
├ 14:00 - Правда или выдумка?
├ 18:00 - Колесо тем
├ 20:00 - Анекдот дня
└ 21:00 - Итоги дня

<b>🎯 КАК ИГРАТЬ:</b>
Подписывайтесь на группу: @uvlekatelnyechteniya
Игры запускаются автоматически!
────────────────────
    """
    
    send_telegram_message(chat_id, games_text)

def show_admin_stats(user_id):
    """Показать статистику для админа"""
    stats_text = f"""
<b>📊 АДМИН СТАТИСТИКА</b>
────────────────────
<b>👥 ПОЛЬЗОВАТЕЛИ:</b>
├ Всего: {len(users)}
├ Активных (последние 7 дней): {len([u for u in users.values() if (datetime.now() - datetime.fromisoformat(u['last_active'])).days < 7])}
├ Новых сегодня: {len([u for u in users.values() if (datetime.now() - datetime.fromisoformat(u['registered_at'])).days == 0])}
└ Премиум: {len([u for u in users.values() if u.get('subscription') == 'premium'])}

<b>📝 СТАТЬИ:</b>
├ В очереди: {len(articles_queue)}
├ Опубликовано сегодня: {len(published_articles)}
├ Всего за неделю: {sum(1 for art in published_articles if (datetime.now() - datetime.fromisoformat(art.get('published_at', datetime.now().isoformat()))).days < 7)}
└ Среднее в день: {len(published_articles) / max(1, (datetime.now() - min((datetime.fromisoformat(art.get('published_at', datetime.now().isoformat())) for art in published_articles), default=datetime.now())).days)}

<b>💬 ФИДБЕК:</b>
├ Всего фидбеков: {sum(u['feedback_given'] for u in users.values())}
├ Среднее на пользователя: {sum(u['feedback_given'] for u in users.values()) / max(1, len(users))}
└ Среднее на статью: {sum(u['feedback_given'] for u in users.values()) / max(1, sum(u['articles_count'] for u in users.values()))}

<b>🎮 ИГРЫ:</b>
├ Всего игр: {len(games_history)}
├ Активных дуэлей: {len([d for d in duels if d['status'] in ['waiting', 'voting']])}
└ Среднее участие: {sum(len(g.get('participants', {})) for g in games_history) / max(1, len(games_history))}

<b>💰 ЭКОНОМИКА:</b>
├ Всего кавычек: {sum(user_balances.values())}
├ Средний баланс: {sum(user_balances.values()) / max(1, len(user_balances))}
└ Общий оборот: {sum(u['total_quotes'] for u in users.values())}

<b>📈 РОСТ:</b>
├ Новых пользователей/день: {len(users) / max(1, (datetime.now() - min((datetime.fromisoformat(u['registered_at']) for u in users.values()), default=datetime.now())).days)}
├ Статей/день: {sum(u['articles_count'] for u in users.values()) / max(1, (datetime.now() - min((datetime.fromisoformat(u['registered_at']) for u in users.values()), default=datetime.now())).days)}
└ Удержание (30 дней): {len([u for u in users.values() if (datetime.now() - datetime.fromisoformat(u['registered_at'])).days >= 30]) / max(1, len([u for u in users.values() if (datetime.now() - datetime.fromisoformat(u['registered_at'])).days >= 30])) * 100:.1f}%

<b>⚠️ ПРОБЛЕМЫ:</b>
├ Пользователи без фидбека: {len([u for u in users.values() if u['feedback_given'] == 0])}
├ Статьи без фидбека: {sum(1 for art in published_articles if art['feedback_count'] == 0)}
└ Неактивные (>30 дней): {len([u for u in users.values() if (datetime.now() - datetime.fromisoformat(u['last_active'])).days > 30])}
────────────────────
    """
    
    send_telegram_message(user_id, stats_text)

def make_announcement(text):
    """Сделать объявление всем участникам"""
    announcement = f"""
📢 <b>ВАЖНОЕ ОБЪЯВЛЕНИЕ</b>
────────────────────
{text}
────────────────────
    """
    
    # Отправляем в тему "Объявления"
    send_telegram_message(GROUP_ID, announcement, topic_id=GROUP_TOPICS['announcements'])
    
    # Отправляем всем пользователям
    for user_id in users:
        try:
            send_telegram_message(user_id, announcement)
        except:
            pass  # Пропускаем если пользователь заблокировал бота
    
    return f"Объявление отправлено {len(users)} пользователям"

# ============ АВТОМАТИЧЕСКИЕ ЗАДАЧИ ============

def schedule_daily_tasks():
    """Планирование ежедневных задач"""
    def run_tasks():
        while True:
            now = datetime.now()
            
            # 10:00 - Дуэль абзацев
            if now.hour == 10 and now.minute == 0:
                if duels and all(d['status'] != 'waiting' for d in duels):
                    user_ids = list(users.keys())
                    if user_ids:
                        start_paragraph_duel(random.choice(user_ids))
            
            # 14:00 - Правда или выдумка
            elif now.hour == 14 and now.minute == 0:
                truth_or_lie_game()
            
            # 18:00 - Колесо тем
            elif now.hour == 18 and now.minute == 0:
                wheel_of_themes_game()
            
            # 19:00 - Лист чтения (если не опубликован)
            elif now.hour == 19 and now.minute == 0:
                if not published_articles or (datetime.now() - datetime.fromisoformat(published_articles[0].get('published_at', datetime.now().isoformat()))).days > 0:
                    publish_daily_reading_list()
            
            # 20:00 - Анекдот дня
            elif now.hour == 20 and now.minute == 0:
                jokes = [
                    "Писатель пришел к врачу. Тот ему: 'У вас переутомление. Вам нужно сменить род деятельности, например, заняться чем-нибудь простым... Ведением блога, например.'",
                    "— Почему писатели такие бедные? — Потому что они всегда работают на свой страх и риск.",
                    "Писатель — это человек, который годами учится писать, а потом всю жизнь жалеет, что научился.",
                    "— Как отличить начинающего писателя от опытного? — Начинающий думает, как бы написать получше. Опытный думает, как бы продать то, что написал.",
                    "Писатель заходит в бар и говорит: 'Налейте мне стакан вдохновения'. Бармен: 'Извините, вдохновение закончилось. Осталось только отчаяние и дедлайн'."
                ]
                joke_text = f"""
😄 <b>АНЕКДОТ ДНЯ</b>
────────────────────
{random.choice(jokes)}
────────────────────
#оффтоп #юмор
                """
                send_telegram_message(GROUP_ID, joke_text, topic_id=GROUP_TOPICS['offtop'])
            
            # 21:00 - Итоги дня
            elif now.hour == 21 and now.minute == 0:
                daily_summary = f"""
📊 <b>ИТОГИ ДНЯ {now.strftime('%d.%m.%Y')}</b>
────────────────────
<b>📝 СТАТЬИ:</b>
├ Опубликовано: {len([a for a in published_articles if (datetime.now() - datetime.fromisoformat(a.get('published_at', datetime.now().isoformat()))).days == 0])}
├ Новых в очереди: {len(articles_queue)}
└ Всего прочтений: {sum(a.get('reads', 0) for a in published_articles if (datetime.now() - datetime.fromisoformat(a.get('published_at', datetime.now().isoformat()))).days == 0)}

<b>🎮 ИГРЫ:</b>
├ Проведено дуэлей: {len([d for d in duels if (datetime.now() - datetime.fromisoformat(d.get('created_at', datetime.now().isoformat()))).days == 0])}
├ Участников игр: {sum(len(g.get('participants', {})) for g in games_history if (datetime.now() - datetime.fromisoformat(g.get('created_at', datetime.now().isoformat()))).days == 0)}
└ Раздано кавычек: {sum(g.get('prize', 0) for g in games_history if (datetime.now() - datetime.fromisoformat(g.get('created_at', datetime.now().isoformat()))).days == 0)}

<b>👥 АКТИВНОСТЬ:</b>
├ Новых участников: {len([u for u in users.values() if (datetime.now() - datetime.fromisoformat(u['registered_at'])).days == 0])}
├ Активных сегодня: {len([u for u in users.values() if (datetime.now() - datetime.fromisoformat(u['last_active'])).days == 0])}
└ Всего участников: {len(users)}

<b>🎉 ПОБЕДИТЕЛИ ДНЯ:</b>
(проверьте результаты игр в соответствующих темах)

<b>🎯 ЗАДАЧА НА ЗАВТРА:</b>
Не забудьте прочитать лист чтения и оставить фидбек!
────────────────────
                """
                send_telegram_message(GROUP_ID, daily_summary, topic_id=GROUP_TOPICS['announcements'])
            
            # Спим 60 секунд перед следующей проверкой
            time.sleep(60)
    
    # Запускаем планировщик в отдельном потоке
    scheduler = threading.Thread(target=run_tasks, daemon=True)
    scheduler.start()
    logger.info("✅ Планировщик ежедневных задач запущен")

# ============ ВЕБ-ИНТЕРФЕЙС ============

@app.route('/')
def home():
    """Главная страница"""
    return '''
    <!DOCTYPE html>
    <html>
    <head>
        <title>Клуб "Увлекательные чтения"</title>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <style>
            body {
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                max-width: 1200px;
                margin: 0 auto;
                padding: 20px;
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                min-height: 100vh;
                color: #333;
            }
            .container {
                background: rgba(255, 255, 255, 0.95);
                padding: 40px;
                border-radius: 20px;
                box-shadow: 0 20px 60px rgba(0, 0, 0, 0.3);
            }
            .header {
                text-align: center;
                margin-bottom: 40px;
            }
            .stats-grid {
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
                gap: 20px;
                margin-bottom: 40px;
            }
            .stat-card {
                background: #f8f9fa;
                padding: 20px;
                border-radius: 10px;
                border-left: 5px solid #667eea;
            }
            .stat-number {
                font-size: 2.5em;
                font-weight: bold;
                color: #667eea;
            }
            .stat-label {
                color: #666;
                margin-top: 5px;
            }
            .section {
                margin-bottom: 40px;
            }
            .btn {
                display: inline-block;
                padding: 12px 24px;
                background: #667eea;
                color: white;
                text-decoration: none;
                border-radius: 8px;
                margin: 10px 5px;
                transition: transform 0.2s;
            }
            .btn:hover {
                transform: translateY(-2px);
                box-shadow: 0 5px 15px rgba(0, 0, 0, 0.2);
            }
            .queue-item {
                background: #f8f9fa;
                padding: 15px;
                margin: 10px 0;
                border-radius: 8px;
                border-left: 4px solid #4CAF50;
            }
            .user-top {
                display: flex;
                align-items: center;
                padding: 10px;
                background: #f8f9fa;
                margin: 5px 0;
                border-radius: 8px;
            }
            .user-rank {
                width: 40px;
                text-align: center;
                font-weight: bold;
                color: #667eea;
            }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>📚 Клуб "Увлекательные чтения"</h1>
                <p>Сообщество авторов без спама, с акцентом на качественный фидбек</p>
            </div>
            
            <div class="stats-grid">
                <div class="stat-card">
                    <div class="stat-number">''' + str(len(users)) + '''</div>
                    <div class="stat-label">Участников</div>
                </div>
                <div class="stat-card">
                    <div class="stat-number">''' + str(len(articles_queue)) + '''</div>
                    <div class="stat-label">Статей в очереди</div>
                </div>
                <div class="stat-card">
                    <div class="stat-number">''' + str(len(published_articles)) + '''</div>
                    <div class="stat-label">Опубликовано сегодня</div>
                </div>
                <div class="stat-card">
                    <div class="stat-number">''' + str(sum(user_balances.values())) + '''</div>
                    <div class="stat-label">Кавычек в системе</div>
                </div>
            </div>
            
            <div class="section">
                <h2>🚀 Быстрый старт</h2>
                <a href="https://t.me/UvlekatelnyeChteniyaClubBot" class="btn" target="_blank">💬 Открыть бота</a>
                <a href="https://t.me/uvlekatelnyechteniya" class="btn" target="_blank">👥 Перейти в группу</a>
                <a href="/health" class="btn">📊 Проверка здоровья</a>
            </div>
            
            <div class="section">
                <h2>📋 Очередь статей (первые 3)</h2>
                ''' + get_queue_html() + '''
            </div>
            
            <div class="section">
                <h2>🏆 Топ участников</h2>
                ''' + get_top_html() + '''
            </div>
            
            <div class="section">
                <h2>🛠 Администрирование</h2>
                <a href="/admin" class="btn">👨‍💼 Панель администратора</a>
                <a href="/stats" class="btn">📈 Детальная статистика</a>
            </div>
            
            <div class="section">
                <h2>📱 Присоединяйтесь!</h2>
                <p>Телеграм-бот: <a href="https://t.me/UvlekatelnyeChteniyaClubBot" target="_blank">@UvlekatelnyeChteniyaClubBot</a></p>
                <p>Группа клуба: <a href="https://t.me/uvlekatelnyechteniya" target="_blank">@uvlekatelnyechteniya</a></p>
                <p>Создатель: <a href="https://t.me/SafonovAN74" target="_blank">@SafonovAN74</a></p>
            </div>
        </div>
    </body>
    </html>
    '''

def get_queue_html():
    """HTML для очереди статей"""
    if not articles_queue:
        return "<p>Очередь пуста</p>"
    
    html = ""
    for i, article in enumerate(list(articles_queue)[:3], 1):
        user = users.get(article['user_id'], {})
        username = user.get('username', 'пользователь')
        html += f'''
        <div class="queue-item">
            <h3>{i}. {article['title']}</h3>
            <p><strong>Автор:</strong> @{username}</p>
            <p><strong>Описание:</strong> {article['description'][:100]}...</p>
            <p><strong>В очереди:</strong> {(datetime.now() - datetime.fromisoformat(article['submitted_at'])).seconds // 3600} часов</p>
        </div>
        '''
    return html

def get_top_html():
    """HTML для топа пользователей"""
    top_users = get_user_top(5)
    if not top_users:
        return "<p>Пока нет участников</p>"
    
    html = ""
    medals = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣"]
    
    for i, user in enumerate(top_users):
        medal = medals[i] if i < len(medals) else f"{i+1}."
        html += f'''
        <div class="user-top">
            <div class="user-rank">{medal}</div>
            <div>
                <strong>@{user['username']}</strong><br>
                <small>{user['quotes']} кавычек • {user['articles']} статей</small>
            </div>
        </div>
        '''
    return html

@app.route('/health')
def health():
    """Проверка здоровья системы"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'stats': {
            'users': len(users),
            'articles_queue': len(articles_queue),
            'published_today': len(published_articles),
            'active_games': len([g for g in games_history if (datetime.now() - datetime.fromisoformat(g['created_at'])).seconds < 3600]),
            'total_quotes': sum(user_balances.values())
        }
    }), 200

@app.route('/admin')
def admin_panel():
    """Панель администратора"""
    return '''
    <!DOCTYPE html>
    <html>
    <head>
        <title>Админ-панель</title>
        <style>
            body { font-family: Arial, sans-serif; padding: 20px; }
            .admin-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; }
            .admin-card { background: #f0f0f0; padding: 20px; border-radius: 10px; }
            .btn { display: inline-block; padding: 10px 20px; background: #4CAF50; color: white; text-decoration: none; margin: 5px; }
        </style>
    </head>
    <body>
        <h1>👨‍💼 Админ-панель</h1>
        <div class="admin-grid">
            <div class="admin-card">
                <h3>📊 Статистика</h3>
                <p>Пользователей: ''' + str(len(users)) + '''</p>
                <p>Статей в очереди: ''' + str(len(articles_queue)) + '''</p>
                <a href="/stats" class="btn">Подробная статистика</a>
            </div>
            <div class="admin-card">
                <h3>📝 Управление</h3>
                <a href="/publish_reading_list" class="btn">Опубликовать лист чтения</a>
                <a href="/clear_queue" class="btn">Очистить очередь</a>
            </div>
            <div class="admin-card">
                <h3>⚙️ Настройки</h3>
                <a href="/settings" class="btn">Настройки бота</a>
                <a href="/backup" class="btn">Создать бэкап</a>
            </div>
        </div>
    </body>
    </html>
    '''

# ============ ЗАПУСК ПРИЛОЖЕНИЯ ============

@app.route('/set_webhook', methods=['GET'])
def set_webhook():
    """Установка вебхука"""
    webhook_url = request.args.get('url')
    
    if not webhook_url:
        return '''
        <h3>Установка вебхука</h3>
        <form method="GET">
            <input type="url" name="url" placeholder="https://ваш-сайт.ру/webhook" style="width: 300px; padding: 10px;">
            <input type="submit" value="Установить">
        </form>
        <p>Текущий вебхук: ''' + f"https://telegram-bot-club.onrender.com/webhook" + '''</p>
        '''
    
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/setWebhook"
    payload = {'url': webhook_url}
    
    try:
        response = requests.post(url, json=payload)
        result = response.json()
        
        if result.get('ok'):
            return f"✅ Вебхук установлен на {webhook_url}"
        else:
            return f"❌ Ошибка: {result.get('description')}"
    except Exception as e:
        return f"❌ Ошибка подключения: {str(e)}"

if __name__ == '__main__':
    # Запускаем планировщик задач
    schedule_daily_tasks()
    
    # Запускаем Flask приложение
    port = int(os.environ.get('PORT', 5000))
    logger.info(f"🚀 Запуск сервера на порту {port}")
    logger.info(f"📊 Всего пользователей: {len(users)}")
    logger.info(f"📝 Статей в очереди: {len(articles_queue)}")
    logger.info(f"💰 Всего кавычек: {sum(user_balances.values())}")
    
    app.run(host='0.0.0.0', port=port)

import os
import logging
import json
import requests
import random
from datetime import datetime
from flask import Flask, request, jsonify
from collections import defaultdict, deque
import threading
import time
import re
import atexit

# ============ НАСТРОЙКА ============

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bot.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Конфигурация
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN', 'PUT_YOUR_TOKEN_HERE')
GROUP_ID = os.environ.get('TG_GROUP_ID', '@uvlekatelnyechteniya')  # можно @username или -100...
ADMIN_IDS = [1039651708]

GROUP_TOPICS = {
    'announcements': 1,
    'rules': 2,
    'queue': 3,
    'reading_list': 4,
    'feedback': 5,
    'duels': 6,
    'games': 7,
    'shop': 8,
    'offtop': 9,
}

# ============ ХРАНЕНИЕ ДАННЫХ ============

users = {}  # user_id(int) -> user_data(dict)
articles_queue = deque(maxlen=10)
published_articles = []
user_articles = defaultdict(list)
user_balances = defaultdict(int)
user_last_submit = {}  # user_id(int) -> datetime
user_daily_reward = {}  # user_id(int) -> 'YYYY-MM-DD'
games_history = []
duels = []
games_results = []
games_pin_message_id = None

DATA_FILE = os.environ.get('BOT_DATA_FILE', 'data.json')
DATA_LOCK = threading.Lock()


# ============ ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ============

def send_telegram_message(
    chat_id,
    text,
    reply_to_message_id=None,
    topic_id=None,
    parse_mode='HTML',
    reply_markup=None
):
    """Отправка сообщения в Telegram"""
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"

    payload = {
        'chat_id': chat_id,
        'text': text,
        'parse_mode': parse_mode
    }

    if reply_to_message_id:
        payload['reply_to_message_id'] = reply_to_message_id

    # message_thread_id работает только в супергруппе с темами
    if topic_id and (
        chat_id == GROUP_ID
        or str(chat_id).startswith('@')
        or (isinstance(chat_id, int) and chat_id < 0)
        or (isinstance(chat_id, str) and chat_id.startswith('-'))
    ):
        payload['message_thread_id'] = topic_id

    if reply_markup:
        payload['reply_markup'] = reply_markup

    try:
        response = requests.post(url, json=payload, timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"Ошибка отправки сообщения: {e}")
        return None


def delete_telegram_message(chat_id, message_id):
    """Удаление сообщения в Telegram"""
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/deleteMessage"
    payload = {'chat_id': chat_id, 'message_id': message_id}
    try:
        response = requests.post(url, json=payload, timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"Ошибка удаления сообщения: {e}")
        return None


def schedule_message_deletion(chat_id, message_id, delay_seconds):
    """Планирование удаления сообщения"""
    threading.Timer(delay_seconds, delete_telegram_message, args=[chat_id, message_id]).start()


def send_temporary_message(
    chat_id,
    text,
    delete_after_seconds,
    reply_to_message_id=None,
    topic_id=None,
    reply_markup=None
):
    """Отправка временного сообщения, удаляемого через заданное время"""
    result = send_telegram_message(
        chat_id,
        text,
        reply_to_message_id=reply_to_message_id,
        topic_id=topic_id,
        reply_markup=reply_markup
    )
    if result and 'result' in result:
        schedule_message_deletion(chat_id, result['result']['message_id'], delete_after_seconds)
    return result


def save_data():
    """Сохранение данных в JSON файл"""
    with DATA_LOCK:
        payload = {
            'users': {str(k): v for k, v in users.items()},
            'articles_queue': list(articles_queue),
            'published_articles': published_articles,
            'user_articles': {str(k): v for k, v in dict(user_articles).items()},
            'user_balances': {str(k): v for k, v in dict(user_balances).items()},
            'user_last_submit': {str(k): v.isoformat() for k, v in user_last_submit.items()},
            'user_daily_reward': {str(k): v for k, v in user_daily_reward.items()},
            'games_history': games_history,
            'duels': duels,
            'games_results': games_results,
            'games_pin_message_id': games_pin_message_id
        }
        try:
            with open(DATA_FILE, 'w', encoding='utf-8') as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"Ошибка сохранения данных: {e}")


def load_data():
    """Загрузка данных из JSON файла"""
    global users, articles_queue, published_articles, user_articles
    global user_balances, user_last_submit, user_daily_reward
    global games_history, duels, games_results, games_pin_message_id

    if not os.path.exists(DATA_FILE):
        return

    try:
        with open(DATA_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)

        raw_users = data.get('users', {})
        users = {int(k): v for k, v in raw_users.items()}

        articles_queue = deque(data.get('articles_queue', []), maxlen=10)
        published_articles = data.get('published_articles', [])

        raw_user_articles = data.get('user_articles', {})
        user_articles = defaultdict(list, {int(k): v for k, v in raw_user_articles.items()})

        raw_user_balances = data.get('user_balances', {})
        user_balances = defaultdict(int, {int(k): int(v) for k, v in raw_user_balances.items()})

        user_last_submit = {
            int(k): datetime.fromisoformat(v)
            for k, v in data.get('user_last_submit', {}).items()
        }

        raw_daily = data.get('user_daily_reward', {})
        user_daily_reward = {int(k): v for k, v in raw_daily.items()}

        games_history = data.get('games_history', [])
        duels = data.get('duels', [])
        games_results = data.get('games_results', [])
        games_pin_message_id = data.get('games_pin_message_id')

        logger.info("Данные загружены из файла")
    except Exception as e:
        logger.error(f"Ошибка загрузки данных: {e}")


def schedule_data_saves(interval_seconds=60):
    """Периодическое сохранение данных"""
    def save_loop():
        while True:
            time.sleep(interval_seconds)
            save_data()

    thread = threading.Thread(target=save_loop, daemon=True)
    thread.start()


def is_user_registered(user_id):
    return user_id in users


def register_user(user_data):
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
    user_balances[user_id] = 50

    welcome_text = f"""
🎉 <b>Добро пожаловать в клуб "Увлекательные чтения"!</b>

<b>Ваш профиль:</b>
- Имя: {user_data.get('first_name', '')} {user_data.get('last_name', '')}
- Юзернейм: @{user_data.get('username', 'нет')}
- ID: {user_id}

<b>Стартовый бонус:</b> 50 кавычек

<b>Что делать дальше:</b>
1) Прочитать правила: /rules
2) Посмотреть очередь: /queue
3) Подать статью: /submit
4) Поиграть: /game
5) Посмотреть профиль: /profile

<b>Наша философия:</b>
"Не обмен лайками, а реальное чтение и поддержка"
    """

    send_telegram_message(user_id, welcome_text)
    logger.info(f"Зарегистрирован новый пользователь: {user_id}")
    return True


def can_submit_article(user_id):
    if user_id not in user_last_submit:
        return True, "Можно подавать"

    last_submit = user_last_submit[user_id]
    time_diff = datetime.now() - last_submit

    min_hours = 48
    if time_diff.total_seconds() < min_hours * 3600:
        hours_left = int((min_hours * 3600 - time_diff.total_seconds()) / 3600)
        return False, f"Вы сможете подать следующую статью через {hours_left} часов"

    user_active_articles = [a for a in articles_queue if a.get('user_id') == user_id]
    if user_active_articles:
        return False, "У вас уже есть статья в очереди"

    if len(articles_queue) >= 10:
        return False, "Очередь переполнена (макс 10 статей)"

    return True, "Можно подавать"


def add_quotes(user_id, amount, reason):
    user_balances[user_id] += amount
    users[user_id]['total_quotes'] += amount
    check_achievements(user_id)
    logger.info(f"Пользователю {user_id} добавлено {amount} кавычек: {reason}")
    return user_balances[user_id]


def check_achievements(user_id):
    user = users[user_id]
    new_badges = []

    if user['total_quotes'] >= 1000 and 'магнат' not in user['badges']:
        new_badges.append('магнат')
    elif user['total_quotes'] >= 500 and 'богач' not in user['badges']:
        new_badges.append('богач')
    elif user['total_quotes'] >= 100 and 'состоятельный' not in user['badges']:
        new_badges.append('состоятельный')

    if user['articles_count'] >= 50 and 'прозаик' not in user['badges']:
        new_badges.append('прозаик')
    elif user['articles_count'] >= 20 and 'писатель' not in user['badges']:
        new_badges.append('писатель')
    elif user['articles_count'] >= 10 and 'автор' not in user['badges']:
        new_badges.append('автор')

    if user['feedback_given'] >= 100 and 'наставник' not in user['badges']:
        new_badges.append('наставник')
    elif user['feedback_given'] >= 50 and 'критик' not in user['badges']:
        new_badges.append('критик')
    elif user['feedback_given'] >= 20 and 'читатель' not in user['badges']:
        new_badges.append('читатель')

    for badge in new_badges:
        if badge not in user['badges']:
            user['badges'].append(badge)
            badge_text = f"""
🎖 <b>НОВЫЙ БЕЙДЖ</b>

Вы получили бейдж: <b>{badge.upper()}</b>
            """
            send_telegram_message(user_id, badge_text)


def add_article_to_queue(user_id, title, description, content):
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

    add_quotes(user_id, 10, "Подача статьи")
    users[user_id]['articles_count'] += 1

    logger.info(f"Статья добавлена в очередь: {article_id}")
    return article_id


def publish_daily_reading_list():
    if not articles_queue:
        return "Очередь статей пуста"

    today_articles = list(articles_queue)[:5]

    reading_list_text = f"""
📚 <b>ЛИСТ ЧТЕНИЯ НА {datetime.now().strftime('%d.%m.%Y')}</b>

<i>Сегодня для чтения {len(today_articles)} статей:</i>
    """

    for i, article in enumerate(today_articles, 1):
        user = users.get(article['user_id'], {})
        username = f"@{user.get('username')}" if user.get('username') else "пользователь"
        safe_url = article.get('content', '').strip()

        reading_list_text += f"""
<b>{i}. {article['title']}</b>
- Автор: {username}
- Описание: {article['description'][:100]}...
- Ссылка: <a href="{safe_url}">читать статью</a>
        """

    reading_list_text += """
<b>ЗАДАНИЕ НА СЕГОДНЯ:</b>
1) Прочитайте минимум 1 статью из списка
2) Оставьте конструктивный фидбек
3) Получите 5 кавычек за каждый фидбек

<b>Напоминание:</b>
Фидбек можно оставлять до 23:59 МСК
"""

    send_telegram_message(GROUP_ID, reading_list_text, topic_id=GROUP_TOPICS['reading_list'])

    for article in today_articles:
        article['status'] = 'published'
        article['published_at'] = datetime.now().isoformat()
        published_articles.append(article)

    for _ in range(len(today_articles)):
        if articles_queue:
            articles_queue.popleft()

    logger.info(f"Опубликован лист чтения: {len(today_articles)} статей")
    return f"Опубликовано {len(today_articles)} статей"


def get_user_top(limit=10):
    user_list = []
    for user_id, user_data in users.items():
        user_list.append({
            'id': user_id,
            'name': user_data.get('first_name', ''),
            'username': user_data.get('username'),
            'articles': user_data.get('articles_count', 0),
            'quotes': user_balances.get(user_id, 0),
            'feedback_given': user_data.get('feedback_given', 0)
        })

    user_list.sort(key=lambda x: x['quotes'], reverse=True)
    return user_list[:limit]


def update_games_pin():
    global games_pin_message_id

    if not games_results:
        pin_text = "🏆 <b>РЕЗУЛЬТАТЫ ИГР</b>\n\nПока нет завершенных игр."
    else:
        lines = ["🏆 <b>РЕЗУЛЬТАТЫ ИГР</b>", ""]
        for result in games_results[-10:]:
            winners_text = ", ".join(result.get('winners', [])) if result.get('winners') else "Нет победителей"
            lines.append(
                f"• <b>{result.get('title','')}</b> - {result.get('date','')}\n"
                f"  Победители: {winners_text}"
            )
        pin_text = "\n".join(lines)

    if games_pin_message_id:
        edit_url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/editMessageText"
        payload = {
            'chat_id': GROUP_ID,
            'message_id': games_pin_message_id,
            'text': pin_text,
            'parse_mode': 'HTML'
        }
        try:
            requests.post(edit_url, json=payload, timeout=10)
        except Exception as e:
            logger.error(f"Ошибка обновления закрепа: {e}")
        return

    result = send_telegram_message(GROUP_ID, pin_text, topic_id=GROUP_TOPICS['games'])
    if result and 'result' in result:
        games_pin_message_id = result['result']['message_id']
        pin_url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/pinChatMessage"
        pin_payload = {
            'chat_id': GROUP_ID,
            'message_id': games_pin_message_id,
            'disable_notification': True
        }
        try:
            requests.post(pin_url, json=pin_payload, timeout=10)
        except Exception as e:
            logger.error(f"Ошибка закрепления сообщения: {e}")


def build_main_menu_keyboard():
    return {
        "inline_keyboard": [
            [
                {"text": "📜 Правила", "callback_data": "menu_rules"},
                {"text": "📋 Очередь", "callback_data": "menu_queue"}
            ],
            [
                {"text": "👤 Профиль", "callback_data": "menu_profile"},
                {"text": "🏆 Топ", "callback_data": "menu_top"}
            ],
            [
                {"text": "🎮 Игры", "callback_data": "menu_games"},
                {"text": "💰 Баланс", "callback_data": "menu_balance"}
            ],
            [
                {"text": "🎁 Ежедневная награда", "callback_data": "menu_daily"},
                {"text": "✍️ Подать статью", "callback_data": "menu_submit"}
            ]
        ]
    }


def show_main_menu(chat_id):
    menu_text = (
        "<b>МЕНЮ КЛУБА</b>\n"
        "Выберите действие кнопкой ниже."
    )
    send_telegram_message(
        chat_id,
        menu_text,
        reply_markup=build_main_menu_keyboard()
    )


# ============ ИГРЫ И АКТИВНОСТИ ============

def start_paragraph_duel(initiator_id, topic=None):
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

    initiator_username = users.get(initiator_id, {}).get('username') or "пользователь"
    duel_text = f"""
⚔️ <b>НОВАЯ ДУЭЛЬ АБЗАЦЕВ</b>

<b>Тема:</b> {topic}
<b>Инициатор:</b> @{initiator_username}
<b>Приз:</b> 25 кавычек

<b>Правила:</b>
1) Напишите абзац (3-5 предложений) на заданную тему
2) Время на написание: 15 минут
3) После истечения времени - голосование
4) Победитель получает приз

<b>Как участвовать:</b>
Ответьте на это сообщение своим абзацем
    """

    result = send_telegram_message(GROUP_ID, duel_text, topic_id=GROUP_TOPICS['duels'])
    if result and 'result' in result:
        duel['message_id'] = result['result']['message_id']

    threading.Timer(900, finish_duel, args=[duel_id]).start()
    logger.info(f"Начата дуэль: {duel_id}")
    return duel_id


def finish_duel(duel_id):
    duel = next((d for d in duels if d['id'] == duel_id), None)
    if not duel or duel['status'] != 'waiting':
        return

    duel['status'] = 'voting'

    if len(duel['paragraphs']) < 2:
        result_text = f"""
⚔️ <b>ДУЭЛЬ ЗАВЕРШЕНА</b>

<b>Тема:</b> {duel['topic']}

Недостаточно участников для голосования.
Дуэль отменена.

Попробуйте начать новую дуэль: /duel
        """
        duel['status'] = 'cancelled'
    else:
        result_text = f"""
⚔️ <b>ГОЛОСОВАНИЕ В ДУЭЛИ</b>

<b>Тема:</b> {duel['topic']}
<b>Участников:</b> {len(duel['paragraphs'])}

<b>Работы участников:</b>
        """

        for i, (user_id, paragraph) in enumerate(duel['paragraphs'].items(), 1):
            username = users.get(user_id, {}).get('username') or "пользователь"
            result_text += f"""
<b>Абзац #{i} (автор: @{username}):</b>
{paragraph[:200]}...
            """

        result_text += """
<b>Как голосовать:</b>
Ответьте номером понравившегося абзаца (1, 2, 3...)
Время голосования: 10 минут
        """

    send_telegram_message(
        GROUP_ID,
        result_text,
        reply_to_message_id=duel.get('message_id'),
        topic_id=GROUP_TOPICS['duels']
    )

    threading.Timer(600, count_duel_votes, args=[duel_id]).start()


def count_duel_votes(duel_id):
    duel = next((d for d in duels if d['id'] == duel_id), None)
    if not duel or duel['status'] != 'voting':
        return

    votes_count = {}
    for _, vote in duel['votes'].items():
        votes_count[vote] = votes_count.get(vote, 0) + 1

    if votes_count:
        winner_vote = max(votes_count.items(), key=lambda x: x[1])
        winner_index = winner_vote[0]

        participants = list(duel['paragraphs'].keys())
        if 0 < winner_index <= len(participants):
            winner_id = participants[winner_index - 1]
            duel['winner'] = winner_id

            add_quotes(winner_id, duel['prize'], "Победа в дуэли")
            users[winner_id]['duels_won'] += 1

            winner_name = users.get(winner_id, {}).get('username') or "пользователь"
            result_text = f"""
🏆 <b>ДУЭЛЬ ЗАВЕРШЕНА</b>

<b>Победитель:</b> @{winner_name}
<b>Голосов:</b> {winner_vote[1]}
<b>Приз:</b> {duel['prize']} кавычек

<b>Статистика голосования:</b>
            """
            for vote, count in votes_count.items():
                result_text += f"Абзац #{vote}: {count} голосов\n"
        else:
            result_text = "Ошибка определения победителя"
    else:
        result_text = "Голосов не было"

    duel['status'] = 'finished'

    send_telegram_message(
        GROUP_ID,
        result_text,
        reply_to_message_id=duel.get('message_id'),
        topic_id=GROUP_TOPICS['duels']
    )


def truth_or_lie_game():
    facts = [
        {"fact": "Джейн Остин издавала свои романы анонимно", "truth": True},
        {"fact": "Агата Кристи работала фармацевтом во время войны", "truth": True},
        {"fact": "Шекспир придумал более 1700 английских слов", "truth": True},
        {"fact": "Достоевский написал 'Преступление и наказание' за две недели", "truth": False},
        {"fact": "Эрнест Хемингуэй написал 'Старик и море' за одну ночь", "truth": False},
    ]

    game_fact = random.choice(facts)

    game_text = f"""
🎮 <b>ИГРА ДНЯ: ПРАВДА ИЛИ ВЫДУМКА</b>

<b>Факт:</b>
{game_fact['fact']}

<b>Задача:</b>
Определите, правда это или выдумка

<b>Как играть:</b>
Ответьте на это сообщение:
- <b>Правда</b>
- <b>Выдумка</b>

<b>Время:</b> 10 минут
<b>Приз:</b> 10 кавычек за правильный ответ
    """

    result = send_telegram_message(GROUP_ID, game_text, topic_id=GROUP_TOPICS['games'])

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

    threading.Timer(600, finish_truth_game, args=[game_id]).start()
    logger.info(f"Запущена игра: {game_id}")
    return game_id


def finish_truth_game(game_id):
    game = next((g for g in games_history if g['id'] == game_id), None)
    if not game:
        return

    correct_answer = "Правда" if game['truth'] else "Выдумка"

    winners = []
    for user_id, answer in game['participants'].items():
        if answer.lower() == correct_answer.lower():
            winners.append(user_id)
            add_quotes(user_id, game['prize'], "Победа в игре")

    winners_names = [
        f"@{users[winner_id].get('username', 'пользователь')}" if users.get(winner_id) else "пользователь"
        for winner_id in winners
    ]

    result_text = f"""
🏆 <b>ИГРА ЗАВЕРШЕНА</b>

<b>Факт:</b> {game['fact']}
<b>Правильный ответ:</b> {correct_answer}

<b>Победители:</b> {len(winners)} участников
    """

    if winners:
        result_text += "\n"
        for i, winner_id in enumerate(winners[:5], 1):
            username = users.get(winner_id, {}).get('username') or "пользователь"
            result_text += f"{i}. @{username}\n"
        if len(winners) > 5:
            result_text += f"... и еще {len(winners) - 5} участников\n"
        result_text += f"\n<b>Каждый получает:</b> {game['prize']} кавычек"
    else:
        result_text += "\nПобедителей нет"

    send_telegram_message(
        GROUP_ID,
        result_text,
        reply_to_message_id=game.get('message_id'),
        topic_id=GROUP_TOPICS['games']
    )

    games_results.append({
        'title': "Правда или выдумка",
        'date': datetime.now().strftime('%d.%m.%Y'),
        'winners': winners_names
    })
    update_games_pin()


def wheel_of_themes_game():
    themes = [
        "Неожиданная находка",
        "Разговор с незнакомцем",
        "Старая фотография",
        "Закрытая дверь",
        "Последний шанс",
        "Утраченное письмо",
        "Тайный знак",
        "Ночное путешествие",
        "Забытый талант"
    ]

    selected_themes = random.sample(themes, 3)

    game_text = f"""
🎡 <b>КОЛЕСО ТЕМ</b>

<b>Задание:</b>
Напишите мини-текст (3-5 предложений) на одну из тем ниже

<b>Темы:</b>
1) {selected_themes[0]}
2) {selected_themes[1]}
3) {selected_themes[2]}

<b>Как участвовать:</b>
Ответьте на это сообщение своим текстом, указав номер темы в начале

<b>Время:</b> 30 минут
<b>Приз:</b> 15 кавычек за лучший текст
    """

    result = send_telegram_message(GROUP_ID, game_text, topic_id=GROUP_TOPICS['games'])

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

    threading.Timer(1800, finish_wheel_game, args=[game_id]).start()
    return game_id


def finish_wheel_game(game_id):
    game = next((g for g in games_history if g['id'] == game_id), None)
    if not game:
        return

    if game['participants']:
        winner_id = random.choice(list(game['participants'].keys()))
        winner_text = game['participants'][winner_id]['text']
        theme_num = game['participants'][winner_id]['theme']

        add_quotes(winner_id, game['prize'], "Победа в Колесе тем")

        username = users.get(winner_id, {}).get('username') or "пользователь"
        theme = game['themes'][theme_num - 1]

        result_text = f"""
🏆 <b>КОЛЕСО ТЕМ ЗАВЕРШЕНО</b>

<b>Победитель:</b> @{username}
<b>Тема:</b> {theme}

<b>Текст-победитель:</b>
{winner_text[:300]}...

<b>Приз:</b> {game['prize']} кавычек
        """
        winners_names = [f"@{username}"]
    else:
        result_text = """
КОЛЕСО ТЕМ ЗАВЕРШЕНО

Участников не было
        """
        winners_names = []

    send_telegram_message(
        GROUP_ID,
        result_text,
        reply_to_message_id=game.get('message_id'),
        topic_id=GROUP_TOPICS['games']
    )

    games_results.append({
        'title': "Колесо тем",
        'date': datetime.now().strftime('%d.%m.%Y'),
        'winners': winners_names
    })
    update_games_pin()


# ============ ОБРАБОТЧИКИ ============

@app.route('/webhook', methods=['POST'])
def webhook():
    try:
        data = request.get_json(force=True, silent=True) or {}
        logger.info(f"Получен вебхук: {list(data.keys())}")

        if 'message' in data:
            process_message(data['message'])
        elif 'callback_query' in data:
            process_callback(data['callback_query'])

        return jsonify({'status': 'ok'}), 200
    except Exception as e:
        logger.error(f"Ошибка обработки вебхука: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500


def process_message(message):
    chat_id = message['chat']['id']
    user_id = message['from']['id']
    text = message.get('text', '')

    if user_id in users:
        users[user_id]['last_active'] = datetime.now().isoformat()

    if 'reply_to_message' in message:
        process_reply(message)
        return

    if text.startswith('/'):
        process_command(chat_id, user_id, text, message)
    else:
        if chat_id == user_id:
            send_telegram_message(user_id, "Напишите /help для списка команд")


def process_command(chat_id, user_id, text, message):
    command = text.split()[0].lower()

    if not is_user_registered(user_id) and command not in ['/start', '/help']:
        send_telegram_message(chat_id, "Сначала зарегистрируйтесь командой /start")
        return

    if command == '/start':
        if is_user_registered(user_id):
            send_telegram_message(chat_id, "Вы уже зарегистрированы. Используйте /help")
        else:
            user_data = {
                'id': user_id,
                'username': message['from'].get('username'),
                'first_name': message['from'].get('first_name', ''),
                'last_name': message['from'].get('last_name', '')
            }
            register_user(user_data)
        show_main_menu(chat_id)

    elif command == '/help':
        show_help(chat_id, user_id)
        show_main_menu(chat_id)

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
        if (isinstance(chat_id, int) and chat_id < 0) or str(chat_id) == str(GROUP_ID):
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
    chat_id = message['chat']['id']
    user_id = message['from']['id']
    text = message.get('text', '')
    message_thread_id = message.get('message_thread_id')
    reply_to = message['reply_to_message']

    if message_thread_id == GROUP_TOPICS.get('games'):
        schedule_message_deletion(chat_id, message['message_id'], 24 * 60 * 60)

    for duel in duels:
        if duel.get('message_id') == reply_to.get('message_id') and duel.get('status') == 'waiting':
            if user_id not in duel['participants']:
                duel['participants'].append(user_id)
            duel['paragraphs'][user_id] = text

            send_temporary_message(
                user_id,
                "Ваш абзац принят. Ждите результатов голосования.",
                60
            )
            return

    for game in games_history:
        if (
            game.get('message_id') == reply_to.get('message_id')
            and game.get('type') == 'truth_or_lie'
            and 'participants' in game
        ):
            answer = text.lower().strip()
            if answer in ['правда', 'выдумка']:
                game['participants'][user_id] = answer
                send_temporary_message(user_id, "Ваш ответ принят. Ждите результатов.", 60)
            return

    for game in games_history:
        if (
            game.get('message_id') == reply_to.get('message_id')
            and game.get('type') == 'wheel_of_themes'
            and 'participants' in game
        ):
            match = re.search(r'^(\d+)[\s\.\)]*', text)
            if match:
                theme_num = int(match.group(1))
                if 1 <= theme_num <= 3:
                    game_text = text[match.end():].strip()
                    if game_text:
                        game['participants'][user_id] = {'theme': theme_num, 'text': game_text}
                        send_temporary_message(user_id, "Ваш текст принят. Ждите результатов.", 60)
                        return

            send_temporary_message(user_id, "Укажите номер темы в начале сообщения (1, 2 или 3)", 60)
            return


def process_callback(callback):
    callback_id = callback['id']
    user_id = callback['from']['id']
    data = callback['data']
    chat_id = (callback.get('message', {}) or {}).get('chat', {}).get('id') or user_id

    if data == 'menu_rules':
        show_rules(chat_id)
    elif data == 'menu_queue':
        show_queue(chat_id)
    elif data == 'menu_profile':
        show_profile(user_id)
    elif data == 'menu_top':
        show_top(chat_id)
    elif data == 'menu_games':
        show_games_menu(chat_id, user_id)
    elif data == 'menu_balance':
        show_balance(user_id)
    elif data == 'menu_daily':
        give_daily_reward(user_id)
    elif data == 'menu_submit':
        if chat_id == user_id:
            start_article_submission(user_id)
        else:
            send_temporary_message(
                user_id,
                "Чтобы подать статью, напишите боту в личные сообщения и нажмите «Подать статью».",
                60
            )
    else:
        send_temporary_message(user_id, f"Callback получен: {data}", 60)

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/answerCallbackQuery"
    try:
        requests.post(url, json={'callback_query_id': callback_id}, timeout=10)
    except Exception as e:
        logger.error(f"Ошибка answerCallbackQuery: {e}")


# ============ КОМАНДЫ ============

def show_help(chat_id, user_id):
    help_text = """
<b>КЛУБ "УВЛЕКАТЕЛЬНЫЕ ЧТЕНИЯ"</b>

<b>Для новичков:</b>
/start - регистрация
/help - справка

<b>Для авторов:</b>
/submit - подать статью в очередь (только в ЛС)
/my_posts - мои статьи
/when_can_submit - когда можно подать следующую
/profile - профиль

<b>Для читателей:</b>
/daily - ежедневная награда (5 кавычек)
/balance - баланс
/top - топ участников

<b>Для игроков:</b>
/game - игры и активности
/duel - начать дуэль (только в группе)

<b>Для админов:</b>
/admin_stats - статистика
/publish_reading_list - опубликовать лист чтения
/announce - объявление

<b>Поддержка:</b>
@SafonovAN74
    """
    send_telegram_message(chat_id, help_text)


def show_rules(chat_id):
    rules_text = """
<b>ПРАВИЛА КЛУБА "УВЛЕКАТЕЛЬНЫЕ ЧТЕНИЯ"</b>

<b>Цель:</b> сообщество авторов без спама ссылок, с акцентом на качественный фидбек

<b>1) Публикация статей:</b>
- 1 статья раз в 48-72 часа
- максимум 1 активная статья в очереди
- очередь максимум 10 статей

<b>2) Фидбек:</b>
- конструктивно
- критикуем текст, не личность

<b>3) Дуэли и игры:</b>
- участие добровольное
- честное голосование
- призы за победы

<b>4) Кавычки:</b>
- внутренняя валюта
- начисляется за активность
- позже можно тратить в магазине

<b>Запрещено:</b>
- спам, флуд, оскорбления, плагиат

Нарушение: предупреждение -> бан
    """
    send_telegram_message(chat_id, rules_text)


def show_queue(chat_id):
    if not articles_queue:
        send_telegram_message(chat_id, "Очередь статей пуста")
        return

    queue_text = "<b>ОЧЕРЕДЬ ПУБЛИКАЦИЙ</b>\n"
    for i, article in enumerate(list(articles_queue)[:5], 1):
        user = users.get(article['user_id'], {})
        username = f"@{user.get('username')}" if user.get('username') else "пользователь"
        time_ago = (datetime.now() - datetime.fromisoformat(article['submitted_at'])).seconds // 3600

        queue_text += f"""
<b>{i}. {article['title']}</b>
- Автор: {username}
- В очереди: {time_ago} часов
- Описание: {article['description'][:50]}...
- Статус: {article['status']}
        """

    if len(articles_queue) > 5:
        queue_text += f"\n... и еще {len(articles_queue) - 5} статей в очереди"

    queue_text += f"""

<b>Статистика:</b>
- Всего в очереди: {len(articles_queue)}
- Опубликовано сегодня: {len(published_articles)}
- Свободных мест: {10 - len(articles_queue)} из 10
    """

    send_telegram_message(chat_id, queue_text)


def start_article_submission(user_id):
    can_submit, message = can_submit_article(user_id)
    if not can_submit:
        send_telegram_message(user_id, message)
        return

    submit_text = """
<b>ПОДАЧА СТАТЬИ</b>

<b>Требования:</b>
1) оригинальный текст
2) минимум 1000 символов
3) завершенная мысль
4) аккуратное оформление

<b>Лимиты:</b>
- 1 статья раз в 48-72 часа
- максимум 1 статья в очереди
- очередь максимум 10 статей

<b>Награда:</b>
+10 кавычек за подачу
+5 кавычек за каждый полученный фидбек

<b>Как подать:</b>
Отправьте сообщение в формате:

<b>ЗАГОЛОВОК</b>
Тема статьи

<b>ОПИСАНИЕ</b>
Кратко (2-3 предложения)

<b>ССЫЛКА</b>
https://...
    """
    send_telegram_message(user_id, submit_text)


def show_my_posts(user_id):
    posts = user_articles.get(user_id, [])
    if not posts:
        send_telegram_message(user_id, "У вас еще нет статей")
        return

    posts_text = f"<b>МОИ СТАТЬИ ({len(posts)})</b>\n"
    for i, post in enumerate(posts[-5:], 1):
        status = post.get('status', 'pending')
        time_ago = (datetime.now() - datetime.fromisoformat(post['submitted_at'])).days
        posts_text += f"""
<b>{i}. {post['title']}</b>
- Статус: {status}
- Подана: {time_ago} дней назад
- Фидбеков: {post.get('feedback_count', 0)}
- Прочтений: {post.get('reads', 0)}
- Лайков: {post.get('likes', 0)}
        """

    send_telegram_message(user_id, posts_text)


def check_submit_time(user_id):
    if user_id not in user_last_submit:
        send_telegram_message(user_id, "Вы можете подать статью прямо сейчас")
        return

    last_submit = user_last_submit[user_id]
    hours_passed = (datetime.now() - last_submit).total_seconds() / 3600
    if hours_passed >= 48:
        send_telegram_message(user_id, "Вы можете подать статью прямо сейчас")
    else:
        send_telegram_message(user_id, f"Вы сможете подать следующую статью через {int(48 - hours_passed)} часов")


def show_profile(user_id):
    if user_id not in users:
        send_telegram_message(user_id, "Сначала зарегистрируйтесь: /start")
        return

    user = users[user_id]
    total_users = len(users)
    ranked = get_user_top(total_users)
    user_rank = next((i + 1 for i, u in enumerate(ranked) if u['id'] == user_id), total_users)

    profile_text = f"""
<b>ПРОФИЛЬ</b>

<b>Имя:</b> {user.get('first_name','')} {user.get('last_name','')}
<b>Юзернейм:</b> @{user.get('username') or 'не установлен'}
<b>В клубе с:</b> {datetime.fromisoformat(user['registered_at']).strftime('%d.%m.%Y')}

<b>Статистика:</b>
- Рейтинг: #{user_rank} из {total_users}
- Статей: {user.get('articles_count', 0)}
- Фидбеков получено: {user.get('feedback_received', 0)}
- Фидбеков дано: {user.get('feedback_given', 0)}
- Игр сыграно: {user.get('games_played', 0)}
- Дуэлей выиграно: {user.get('duels_won', 0)}
- Баланс: {user_balances.get(user_id, 0)} кавычек

<b>Бейджи:</b>
{', '.join(user.get('badges', [])) if user.get('badges') else 'пока нет'}
    """
    send_telegram_message(user_id, profile_text)


def give_daily_reward(user_id):
    today = datetime.now().date().isoformat()
    if user_daily_reward.get(user_id) == today:
        send_telegram_message(user_id, "Вы уже получали награду сегодня. Приходите завтра")
        return

    reward = 5
    add_quotes(user_id, reward, "Ежедневная награда")
    user_daily_reward[user_id] = today

    send_telegram_message(
        user_id,
        f"🎁 <b>Ежедневная награда</b>\n\nВы получили {reward} кавычек\nБаланс: {user_balances.get(user_id, 0)}"
    )


def show_balance(user_id):
    balance = user_balances.get(user_id, 0)
    send_telegram_message(user_id, f"💰 <b>Ваш баланс</b>\n\nКавычек: {balance}")


def show_top(chat_id):
    top_users = get_user_top(10)
    if not top_users:
        send_telegram_message(chat_id, "Пока нет участников в топе")
        return

    medals = ["🥇", "🥈", "🥉", "4", "5", "6", "7", "8", "9", "10"]
    top_text = "<b>ТОП УЧАСТНИКОВ</b>\n"

    for i, u in enumerate(top_users[:10]):
        medal = medals[i]
        username = f"@{u['username']}" if u.get('username') else (u.get('name') or "пользователь")
        top_text += f"\n{medal} <b>{username}</b> - {u['quotes']} кавычек, {u['articles']} статей"

    send_telegram_message(chat_id, top_text)


def show_games_menu(chat_id, user_id):
    games_text = """
🎮 <b>Игры и активности</b>

1) Дуэль абзацев
Команда: /duel (только в группе)

2) Правда или выдумка
Запускается по расписанию

3) Колесо тем
Запускается по расписанию
    """
    send_telegram_message(chat_id, games_text)


def show_admin_stats(user_id):
    stats_text = f"""
<b>АДМИН СТАТИСТИКА</b>

- Пользователей: {len(users)}
- Статей в очереди: {len(articles_queue)}
- Опубликовано сегодня: {len(published_articles)}
- Всего кавычек: {sum(user_balances.values())}
    """
    send_telegram_message(user_id, stats_text)


def make_announcement(text):
    announcement = f"<b>ВАЖНОЕ ОБЪЯВЛЕНИЕ</b>\n\n{text}"
    send_telegram_message(GROUP_ID, announcement, topic_id=GROUP_TOPICS['announcements'])

    for uid in list(users.keys()):
        try:
            send_telegram_message(uid, announcement)
        except Exception:
            pass

    return f"Объявление отправлено {len(users)} пользователям"


# ============ АВТОЗАДАЧИ ============

def schedule_daily_tasks():
    def run_tasks():
        while True:
            now = datetime.now()

            if now.hour == 10 and now.minute == 0:
                if users:
                    start_paragraph_duel(random.choice(list(users.keys())))

            elif now.hour == 14 and now.minute == 0:
                truth_or_lie_game()

            elif now.hour == 18 and now.minute == 0:
                wheel_of_themes_game()

            elif now.hour == 19 and now.minute == 0:
                publish_daily_reading_list()

            time.sleep(60)

    scheduler = threading.Thread(target=run_tasks, daemon=True)
    scheduler.start()
    logger.info("Планировщик ежедневных задач запущен")


# ============ ВЕБ-ИНТЕРФЕЙС (минимум) ============

@app.route('/')
def home():
    return jsonify({
        "status": "ok",
        "users": len(users),
        "queue": len(articles_queue),
        "published_today": len(published_articles),
        "total_quotes": sum(user_balances.values())
    })


@app.route('/health')
def health():
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'stats': {
            'users': len(users),
            'articles_queue': len(articles_queue),
            'published_today': len(published_articles),
            'total_quotes': sum(user_balances.values())
        }
    }), 200


# ============ ЗАПУСК ============

if __name__ == '__main__':
    load_data()
    schedule_data_saves()
    atexit.register(save_data)

    schedule_daily_tasks()

    port = int(os.environ.get('PORT', 5000))
    logger.info(f"Запуск сервера на порту {port}")
    app.run(host='0.0.0.0', port=port)

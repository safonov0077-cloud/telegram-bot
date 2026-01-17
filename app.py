import os
import json
import logging
import threading
import time
import re
import atexit
import random
from datetime import datetime, timedelta
from collections import defaultdict, deque
from urllib.parse import urlparse

import requests
from flask import Flask, request, jsonify

# =========================
# НАСТРОЙКИ
# =========================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("clubbot")

app = Flask(__name__)

# ТОКЕН ИЗ ПЕРЕМЕННЫХ ОКРУЖЕНИЯ
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "").strip()
if not TELEGRAM_TOKEN:
    logger.error("❌ TELEGRAM_TOKEN пустой! Задайте переменную окружения TELEGRAM_TOKEN")
    TELEGRAM_TOKEN = ""

# ID ГРУППЫ (получен через @getidsbot)
GROUP_ID = "-1003646270051"  # Ваша группа @uvlekatelnyechteniya

# ID администратора
ADMIN_IDS = {1039651708}

# Временное хранение данных в памяти
users = {}
articles_queue = deque(maxlen=10)
published_articles = []
user_articles = defaultdict(list)
user_balances = defaultdict(int)
user_last_submit = {}
user_daily_reward = {}
user_submit_notified = {}
user_states = {}

# Игры
games_history = []
duels = []

# Тексты
ALLOWED_PLATFORMS_TEXT = "VK, Дзен, Telegram"
ALLOWED_DOMAINS = {
    "vk.com", "m.vk.com",
    "dzen.ru", "zen.yandex.ru",
    "t.me", "telegra.ph",
}

# =========================
# TELEGRAM API
# =========================

def tg(method: str, payload: dict, timeout: int = 12):
    """Базовый вызов Telegram API"""
    if not TELEGRAM_TOKEN:
        logger.error("Токен не установлен!")
        return None
        
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/{method}"
    try:
        response = requests.post(url, json=payload, timeout=timeout)
        response.raise_for_status()
        data = response.json()
        if not data.get("ok"):
            logger.error(f"Telegram API error {method}: {data}")
        return data
    except Exception as e:
        logger.error(f"Telegram request failed {method}: {e}")
        return None

def send_telegram_message(chat_id, text, parse_mode="HTML", reply_markup=None):
    """Отправка сообщения в Telegram"""
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": parse_mode,
        "disable_web_page_preview": True,
    }
    if reply_markup:
        payload["reply_markup"] = reply_markup
    
    logger.info(f"📤 Отправка сообщения в {chat_id}: {text[:100]}...")
    return tg("sendMessage", payload)

def answer_callback(callback_query_id, text, show_alert=False):
    """Ответ на callback query"""
    payload = {
        "callback_query_id": callback_query_id,
        "text": text,
        "show_alert": show_alert
    }
    return tg("answerCallbackQuery", payload)

# =========================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# =========================

def html_escape(s: str) -> str:
    """Экранирование HTML"""
    if s is None:
        return ""
    return (
        str(s)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )

def normalize_command(text: str) -> str:
    """Нормализация команды"""
    if not text:
        return ""
    cmd = text.split()[0].strip().lower()
    if "@" in cmd:
        cmd = cmd.split("@", 1)[0]
    return cmd

def safe_username(user_id: int) -> str:
    """Безопасное получение username"""
    user = users.get(user_id, {})
    username = user.get("username")
    if username:
        return "@" + username
    name = (user.get("first_name", "") + " " + user.get("last_name", "")).strip()
    return name if name else f"пользователь {user_id}"

def parse_domain(url: str) -> str:
    """Парсинг домена из URL"""
    try:
        parsed = urlparse(url.strip())
        return (parsed.netloc or "").lower()
    except Exception:
        return ""

def is_allowed_article_url(url: str) -> bool:
    """Проверка допустимости URL"""
    if not url or not isinstance(url, str):
        return False
    
    url = url.strip()
    if not (url.startswith("http://") or url.startswith("https://")):
        return False
    
    domain = parse_domain(url)
    if not domain:
        return False
    
    # Убираем www.
    if domain.startswith("www."):
        domain = domain[4:]
    
    # Проверяем домен и поддомены
    for allowed_domain in ALLOWED_DOMAINS:
        if domain == allowed_domain or domain.endswith("." + allowed_domain):
            return True
    
    return False

# =========================
# РЕГИСТРАЦИЯ ПОЛЬЗОВАТЕЛЕЙ
# =========================

def is_user_registered(user_id: int) -> bool:
    """Проверка регистрации пользователя"""
    return user_id in users

def register_user(user_data: dict) -> bool:
    """Регистрация нового пользователя"""
    user_id = int(user_data["id"])
    
    if user_id in users:
        return True
    
    users[user_id] = {
        "id": user_id,
        "username": user_data.get("username"),
        "first_name": user_data.get("first_name", ""),
        "last_name": user_data.get("last_name", ""),
        "registered_at": datetime.now().isoformat(),
        "articles_count": 0,
        "feedback_given": 0,
        "games_played": 0,
        "total_quotes": 50,
        "badges": ["новичок"],
        "last_active": datetime.now().isoformat()
    }
    
    user_balances[user_id] = 50  # Стартовый бонус
    
    welcome_text = f"""📚 <b>Увлекательные чтения</b>

Добро пожаловать в клуб, где ценят реальные отзывы, а не обмен лайками!

🎯 <b>Как начать:</b>
1. <b>/daily</b> - ежедневная награда (5 кавычек)
2. <b>/balance</b> - проверить баланс
3. <b>/queue</b> - посмотреть очередь статей (в группе)
4. <b>/submit</b> - подать свою статью

📜 <b>Правила:</b>
• 1 статья раз в 48-72 часа
• Ссылки только: {ALLOWED_PLATFORMS_TEXT}
• Реальные отзывы, а не "норм"

💰 <b>Баланс:</b> 50 кавычек (стартовый бонус)

Пиши <b>/help</b> для списка команд!"""
    
    send_telegram_message(user_id, welcome_text)
    logger.info(f"✅ Зарегистрирован новый пользователь: {user_id}")
    return True

# =========================
# СИСТЕМА КАВЫЧЕК
# =========================

def add_quotes(user_id: int, amount: int, reason: str) -> int:
    """Начисление кавычек"""
    user_id = int(user_id)
    user_balances[user_id] = user_balances.get(user_id, 0) + int(amount)
    
    if user_id in users:
        users[user_id]["total_quotes"] = users[user_id].get("total_quotes", 0) + int(amount)
    
    logger.info(f"💰 {user_id}: +{amount} кавычек ({reason})")
    return user_balances[user_id]

def spend_quotes(user_id: int, amount: int, reason: str) -> bool:
    """Списание кавычек"""
    user_id = int(user_id)
    amount = int(amount)
    
    if user_balances.get(user_id, 0) < amount:
        return False
    
    user_balances[user_id] -= amount
    logger.info(f"🪙 {user_id}: -{amount} кавычек ({reason})")
    return True

# =========================
# ОЧЕРЕДЬ СТАТЕЙ
# =========================

def can_submit_article(user_id: int):
    """Проверка возможности подачи статьи"""
    user_id = int(user_id)
    
    # Если пользователь никогда не подавал
    if user_id not in user_last_submit:
        return True, "✅ Можно подавать статью"
    
    last_submit = user_last_submit[user_id]
    time_diff = datetime.now() - last_submit
    
    # Минимум 48 часов между подачами
    min_hours = 48
    if time_diff.total_seconds() < min_hours * 3600:
        hours_left = int((min_hours * 3600 - time_diff.total_seconds()) / 3600)
        return False, f"⏳ Можно будет подать через {hours_left} часов"
    
    # Максимум 1 активная статья в очереди
    if any(article["user_id"] == user_id for article in articles_queue):
        return False, "⚠️ У тебя уже есть статья в очереди"
    
    # Максимум 10 статей в очереди
    if len(articles_queue) >= 10:
        return False, "📦 Очередь заполнена (максимум 10 статей)"
    
    return True, "✅ Можно подавать статью"

def add_article_to_queue(user_id: int, title: str, description: str, url: str) -> str:
    """Добавление статьи в очередь"""
    user_id = int(user_id)
    article_id = f"art_{int(time.time())}_{user_id}"
    
    article = {
        "id": article_id,
        "user_id": user_id,
        "title": title[:120],
        "description": description[:600],
        "url": url,
        "submitted_at": datetime.now().isoformat(),
        "status": "pending",
        "feedback_count": 0
    }
    
    articles_queue.append(article)
    user_articles[user_id].append(article)
    user_last_submit[user_id] = datetime.now()
    
    # Награда за подачу
    add_quotes(user_id, 10, "Подача статьи")
    if user_id in users:
        users[user_id]["articles_count"] = users[user_id].get("articles_count", 0) + 1
    
    logger.info(f"📝 Добавлено в очередь: {article_id}")
    return article_id

def parse_submission_text(text: str):
    """Парсинг текста подачи статьи"""
    text = (text or "").strip()
    
    # Ищем блоки по маркерам
    def get_block(marker):
        pattern = rf"{marker}\s*\n(.+?)(?=\n[A-ZА-ЯЁ]+\n|\Z)"
        match = re.search(pattern, text, flags=re.S | re.I)
        return match.group(1).strip() if match else ""
    
    title = get_block("ЗАГОЛОВОК")
    description = get_block("ОПИСАНИЕ")
    link = get_block("ССЫЛКА").split()[0].strip() if get_block("ССЫЛКА") else ""
    
    return title, description, link

# =========================
# КОМАНДЫ ДЛЯ ЛИЧНЫХ СООБЩЕНИЙ
# =========================

def show_help(chat_id):
    """Показать помощь"""
    text = f"""📚 <b>Помощь по командам</b>

<b>Личные команды:</b>
/start - регистрация
/help - помощь
/profile - профиль
/balance - баланс кавычек
/daily - ежедневная награда
/submit - подать статью
/my_posts - мои статьи

<b>Команды в группе:</b>
/queue - очередь статей
/top - топ участников
/game - игры дня
/rules - правила клуба

<b>Важно:</b>
• Ссылки принимаем только: {ALLOWED_PLATFORMS_TEXT}
• 1 статья раз в 48-72 часа
• Реальные отзывы приветствуются

Пиши команды в нужном месте! 🤖"""
    send_telegram_message(chat_id, text)

def show_profile(user_id: int, chat_id=None):
    """Показать профиль"""
    if not is_user_registered(user_id):
        send_telegram_message(user_id, "Сначала зарегистрируйся через /start")
        return
    
    user = users[user_id]
    balance = user_balances.get(user_id, 0)
    
    # Считаем рейтинг
    all_users = list(users.items())
    sorted_users = sorted(all_users, key=lambda x: user_balances.get(x[0], 0), reverse=True)
    rank = next((i+1 for i, (uid, _) in enumerate(sorted_users) if uid == user_id), len(sorted_users))
    
    text = f"""👤 <b>Профиль</b>

<b>Имя:</b> {html_escape(user.get('first_name', ''))} {html_escape(user.get('last_name', ''))}
<b>Юзернейм:</b> @{html_escape(user.get('username', 'нет'))}
<b>Рейтинг:</b> #{rank} из {len(sorted_users)}

<b>Статистика:</b>
• Статей подано: {user.get('articles_count', 0)}
• Фидбеков дано: {user.get('feedback_given', 0)}
• Игр сыграно: {user.get('games_played', 0)}
• Баланс: {balance} кавычек 🪙

<b>Бейджи:</b> {', '.join(user.get('badges', ['новичок']))}"""
    
    target_chat = chat_id or user_id
    send_telegram_message(target_chat, text)

def show_rules(chat_id):
    """Показать правила"""
    text = f"""📜 <b>Правила клуба</b>

<b>Основные принципы:</b>
1. Качество, а не количество
2. Взаимность: получил фидбек → дай фидбек
3. Уважение к авторам
4. Реальная поддержка

<b>Очередь статей:</b>
• 1 статья раз в 48-72 часа
• Максимум 1 активная статья в очереди
• Всего в очереди: до 10 статей
• Лист чтения публикуется в 19:00 МСК

<b>Ссылки принимаем только:</b>
{ALLOWED_PLATFORMS_TEXT}

<b>Фидбек:</b>
• Конструктивная критика
• Что понравилось/не понравилось
• "Норм" не считается фидбеком

<b>Игры и активность:</b>
• Участие в играх поощряется
• Кавычки начисляются за активность
• Топ участников обновляется ежедневно

Соблюдай правила, и клуб будет полезным для всех! 🤝"""
    send_telegram_message(chat_id, text)

def show_top(chat_id):
    """Показать топ участников"""
    if not users:
        send_telegram_message(chat_id, "🏆 <b>Топ участников</b>\n\nПока никто не зарегистрирован. Будь первым! 🚀")
        return
    
    # Сортируем по кавычкам
    top_users = []
    for user_id, user_data in users.items():
        top_users.append({
            "id": user_id,
            "name": user_data.get("first_name", ""),
            "username": user_data.get("username"),
            "quotes": user_balances.get(user_id, 0),
            "articles": user_data.get("articles_count", 0)
        })
    
    top_users.sort(key=lambda x: x["quotes"], reverse=True)
    
    medals = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣", "🔟"]
    
    lines = ["🏆 <b>Топ участников</b>\n"]
    for i, user in enumerate(top_users[:10]):
        medal = medals[i] if i < len(medals) else f"{i+1}."
        name = f"@{user['username']}" if user['username'] else user['name']
        lines.append(f"{medal} <b>{html_escape(name)}</b> - {user['quotes']} 🪙 (статей: {user['articles']})")
    
    send_telegram_message(chat_id, "\n".join(lines))

def show_queue(chat_id):
    """Показать очередь статей"""
    if not articles_queue:
        send_telegram_message(chat_id, "📭 <b>Очередь публикаций</b>\n\nОчередь пустая. Будь первым, кто подаст статью! ✍️")
        return
    
    lines = ["📋 <b>Очередь публикаций</b>\n"]
    
    for i, article in enumerate(list(articles_queue)[:10], 1):
        author = safe_username(article["user_id"])
        title = html_escape(article["title"])
        lines.append(f"{i}. <b>{title}</b>\n   👤 Автор: {html_escape(author)}")
    
    lines.append(f"\n<b>Всего в очереди:</b> {len(articles_queue)} из 10")
    send_telegram_message(chat_id, "\n".join(lines))

def give_daily_reward(user_id: int):
    """Выдать ежедневную награду"""
    user_id = int(user_id)
    today = datetime.now().date().isoformat()
    
    # Проверяем, получал ли сегодня
    if user_daily_reward.get(str(user_id)) == today or user_daily_reward.get(user_id) == today:
        send_telegram_message(user_id, "⏳ <b>Ежедневная награда</b>\n\nТы уже получал награду сегодня. Возвращайся завтра! 😊")
        return
    
    # Выдаем награду
    reward = 5
    new_balance = add_quotes(user_id, reward, "Ежедневная награда")
    user_daily_reward[user_id] = today
    
    send_telegram_message(
        user_id,
        f"🎁 <b>Ежедневная награда</b>\n\n+{reward} кавычек 🪙\n<b>Новый баланс:</b> {new_balance}\n\nВозвращайся завтра за новой наградой! 🚀"
    )

def start_article_submission(user_id: int):
    """Начать процесс подачи статьи"""
    can_submit, message = can_submit_article(user_id)
    if not can_submit:
        send_telegram_message(user_id, message)
        return
    
    user_states[user_id] = {
        "state": "awaiting_article",
        "started_at": datetime.now().isoformat()
    }
    
    text = f"""✍️ <b>Подача статьи</b>

<b>Формат сообщения:</b>

ЗАГОЛОВОК
Твой заголовок здесь

ОПИСАНИЕ
2-3 предложения о статье

ССЫЛКА
https://example.com

<b>Важно:</b>
• Ссылки принимаем только: {ALLOWED_PLATFORMS_TEXT}
• Заголовок: до 120 символов
• Описание: до 600 символов
• Проверь ссылку перед отправкой

Отправь сообщение в указанном формате."""
    
    send_telegram_message(user_id, text)

# =========================
# ИГРЫ
# =========================

def show_games_menu(chat_id):
    """Показать меню игр"""
    text = """🎮 <b>Игры дня</b>

Выбери игру для участия:

<b>1. ⚔️ Дуэль абзацев</b>
• Напиши текст на заданную тему
• 15 минут на написание
• 10 минут голосование
• Приз: 25 кавычек

<b>2. 🎲 Правда или выдумка</b>
• Угадай ложный факт
• 10 минут на обсуждение
• Приз: 10 кавычек

<b>3. 🎡 Колесо тем</b>
• Напиши текст на случайную тему
• 30 минут на написание
• Приз: 15 кавычек

<b>Команды:</b>
/game - это меню
/duel - начать дуэль (в группе)"""
    
    keyboard = {
        "inline_keyboard": [
            [{"text": "⚔️ Начать дуэль", "callback_data": "start_duel"}],
            [{"text": "🎲 Правда или выдумка", "callback_data": "truth_game"}],
            [{"text": "🎡 Колесо тем", "callback_data": "wheel_game"}]
        ]
    }
    
    send_telegram_message(chat_id, text, reply_markup=keyboard)

def start_paragraph_duel(initiator_id: int):
    """Начать дуэль абзацев"""
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
        "id": duel_id,
        "topic": topic,
        "initiator": initiator_id,
        "participants": [],
        "paragraphs": {},
        "status": "waiting",
        "created_at": datetime.now().isoformat(),
        "votes": {},
        "winner": None,
        "prize": 25
    }
    
    duels.append(duel)
    
    text = f"""⚔️ <b>Дуэль абзацев началась!</b>

<b>Тема:</b> {topic}
<b>Инициатор:</b> {safe_username(initiator_id)}
<b>Приз:</b> 25 кавычек 🪙

<b>Правила:</b>
1. Напиши 3-5 предложений на тему
2. Время: 15 минут
3. Отправь свой текст ответом на это сообщение
4. После будет голосование (10 минут)

Участвуй и побеждай! ✍️"""
    
    send_telegram_message(GROUP_ID, text)
    
    # Таймер для завершения приема текстов
    threading.Timer(900, finish_duel_submissions, args=[duel_id]).start()
    
    return duel_id

def finish_duel_submissions(duel_id: str):
    """Завершить прием текстов в дуэли"""
    duel = next((d for d in duels if d["id"] == duel_id and d["status"] == "waiting"), None)
    if not duel:
        return
    
    if len(duel["paragraphs"]) < 2:
        send_telegram_message(GROUP_ID, "⚔️ Дуэль отменена: недостаточно участников 😔")
        duel["status"] = "cancelled"
        return
    
    duel["status"] = "voting"
    
    # Показываем тексты для голосования
    text = f"""🗳 <b>Голосование в дуэли</b>

<b>Тема:</b> {duel['topic']}
<b>Участников:</b> {len(duel['paragraphs'])}

"""
    
    participants = list(duel["paragraphs"].items())
    for i, (user_id, paragraph) in enumerate(participants, 1):
        username = safe_username(user_id)
        text += f"\n<b>#{i} - {username}</b>\n{html_escape(paragraph[:150])}...\n"
    
    text += "\n<b>Голосование:</b> ответь числом (1, 2, 3...) на это сообщение\n<b>Время:</b> 10 минут"
    
    send_telegram_message(GROUP_ID, text)
    
    # Таймер для завершения голосования
    threading.Timer(600, finish_duel_voting, args=[duel_id]).start()

def finish_duel_voting(duel_id: str):
    """Завершить голосование в дуэли"""
    duel = next((d for d in duels if d["id"] == duel_id and d["status"] == "voting"), None)
    if not duel:
        return
    
    # Подсчет голосов
    votes_count = defaultdict(int)
    for vote in duel["votes"].values():
        votes_count[vote] += 1
    
    if not votes_count:
        send_telegram_message(GROUP_ID, "⚔️ Дуэль завершена: никто не проголосовал 😔")
        duel["status"] = "finished"
        return
    
    # Определяем победителя
    winner_index = max(votes_count.items(), key=lambda x: x[1])[0]
    participants = list(duel["paragraphs"].keys())
    
    if 1 <= winner_index <= len(participants):
        winner_id = participants[winner_index - 1]
        duel["winner"] = winner_id
        
        # Награждаем победителя
        add_quotes(winner_id, duel["prize"], "Победа в дуэли")
        if winner_id in users:
            users[winner_id]["games_played"] = users[winner_id].get("games_played", 0) + 1
        
        send_telegram_message(
            GROUP_ID,
            f"""🏆 <b>Дуэль завершена!</b>

<b>Победитель:</b> {safe_username(winner_id)}
<b>Тема:</b> {duel['topic']}
<b>Приз:</b> {duel['prize']} кавычек 🪙

Поздравляем победителя! 🎉"""
        )
    
    duel["status"] = "finished"
    games_history.append({
        "type": "duel",
        "topic": duel["topic"],
        "winner": duel["winner"],
        "date": datetime.now().isoformat()
    })

# =========================
# ОБРАБОТКА СООБЩЕНИЙ
# =========================

def process_message(message: dict):
    """Обработка входящего сообщения"""
    chat_id = message["chat"]["id"]
    user_id = int(message["from"]["id"])
    text = message.get("text", "") or ""
    
    # Обновляем активность
    if user_id in users:
        users[user_id]["last_active"] = datetime.now().isoformat()
    
    # Проверяем, является ли это ответом на сообщение в дуэли
    if "reply_to_message" in message and chat_id == int(GROUP_ID):
        reply_to = message["reply_to_message"]
        reply_text = reply_to.get("text", "")
        
        # Ответ в дуэли (текст)
        if "⚔️ Дуэль абзацев началась!" in reply_text or "🗳 Голосование в дуэли" in reply_text:
            handle_duel_response(user_id, text, reply_text)
            return
        
        # Голосование в дуэли (число)
        if "Голосование:" in reply_text:
            try:
                vote = int(text.strip())
                handle_duel_vote(user_id, vote)
            except ValueError:
                pass
            return
    
    # Обработка команд
    if text.startswith("/"):
        command = normalize_command(text)
        
        # Регистрация
        if command == "/start":
            user_data = {
                "id": user_id,
                "username": message["from"].get("username"),
                "first_name": message["from"].get("first_name", ""),
                "last_name": message["from"].get("last_name", "")
            }
            register_user(user_data)
            return
        
        # Проверка регистрации для остальных команд
        if not is_user_registered(user_id) and command not in ["/start", "/help"]:
            send_telegram_message(chat_id, "⚠️ Сначала зарегистрируйся через /start")
            return
        
        # Личные команды (работают везде)
        if command == "/help":
            show_help(chat_id)
            return
        
        if command == "/profile":
            show_profile(user_id, chat_id)
            return
        
        if command == "/balance":
            balance = user_balances.get(user_id, 0)
            send_telegram_message(chat_id, f"💰 <b>Твой баланс:</b> {balance} кавычек 🪙")
            return
        
        if command == "/daily":
            give_daily_reward(user_id)
            return
        
        if command == "/submit":
            if chat_id == user_id:  # Только в личных сообщениях
                start_article_submission(user_id)
            else:
                send_telegram_message(chat_id, "✍️ Подача статьи доступна только в личных сообщениях с ботом")
            return
        
        if command == "/rules":
            show_rules(chat_id)
            return
        
        # Групповые команды (работают в группе)
        if command == "/queue":
            if chat_id == int(GROUP_ID) or chat_id == user_id:
                show_queue(chat_id)
            else:
                send_telegram_message(chat_id, "📋 Очередь можно посмотреть в группе или в личных сообщениях")
            return
        
        if command == "/top":
            if chat_id == int(GROUP_ID) or chat_id == user_id:
                show_top(chat_id)
            else:
                send_telegram_message(chat_id, "🏆 Топ можно посмотреть в группе или в личных сообщениях")
            return
        
        if command == "/game":
            if chat_id == int(GROUP_ID) or chat_id == user_id:
                show_games_menu(chat_id)
            else:
                send_telegram_message(chat_id, "🎮 Игры доступны в группе")
            return
        
        if command == "/duel":
            if chat_id == int(GROUP_ID):
                start_paragraph_duel(user_id)
            else:
                send_telegram_message(chat_id, "⚔️ Дуэли доступны только в группе")
            return
        
        # Админ команды
        if command == "/publish_reading_list" and user_id in ADMIN_IDS:
            publish_reading_list()
            return
        
        if command == "/admin_stats" and user_id in ADMIN_IDS:
            show_admin_stats(user_id)
            return
        
        # Неизвестная команда
        send_telegram_message(chat_id, "🤔 Неизвестная команда. Напиши /help для списка команд")
        return
    
    # Обработка подачи статьи (только в личных сообщениях)
    if chat_id == user_id and user_id in user_states:
        state = user_states[user_id].get("state")
        
        if state == "awaiting_article":
            title, description, url = parse_submission_text(text)
            
            if not title or not description or not url:
                send_telegram_message(user_id, "❌ Не вижу все три блока: ЗАГОЛОВОК, ОПИСАНИЕ, ССЫЛКА\n\nПопробуй еще раз:")
                return
            
            if not is_allowed_article_url(url):
                send_telegram_message(
                    user_id, 
                    f"❌ Ссылка недопустима\n\nПринимаем только: {ALLOWED_PLATFORMS_TEXT}\n\nУбедись, что ссылка начинается с https:// и ведет на разрешенный сайт"
                )
                return
            
            can_submit, message = can_submit_article(user_id)
            if not can_submit:
                send_telegram_message(user_id, message)
                del user_states[user_id]
                return
            
            # Добавляем статью в очередь
            article_id = add_article_to_queue(user_id, title, description, url)
            
            # Уведомление в группу
            group_notification = f"""📝 <b>Новая статья в очереди!</b>

<b>Заголовок:</b> {html_escape(title)}
<b>Автор:</b> {safe_username(user_id)}

Очередь: /queue"""
            send_telegram_message(GROUP_ID, group_notification)
            
            # Подтверждение автору
            send_telegram_message(
                user_id,
                f"""✅ <b>Статья добавлена в очередь!</b>

<b>Заголовок:</b> {html_escape(title)}
<b>ID:</b> {article_id}
<b>Позиция в очереди:</b> {len(articles_queue)}

Жди публикации в листе чтения (19:00 МСК) 🕔"""
            )
            
            del user_states[user_id]
            return
    
    # Если не команда и не состояние, показываем помощь
    if chat_id == user_id:
        send_telegram_message(user_id, "Напиши /help для списка команд или /submit чтобы подать статью ✍️")

def handle_duel_response(user_id: int, text: str, reply_text: str):
    """Обработка ответа в дуэли"""
    # Находим активную дуэль
    active_duel = None
    for duel in duels:
        if duel["status"] == "waiting":
            active_duel = duel
            break
    
    if not active_duel:
        return
    
    # Проверяем, не участвовал ли уже
    if user_id in active_duel["paragraphs"]:
        send_telegram_message(user_id, "⚠️ Ты уже отправил текст для этой дуэли")
        return
    
    # Сохраняем текст
    active_duel["paragraphs"][user_id] = text
    active_duel["participants"].append(user_id)
    
    send_telegram_message(user_id, "✅ Твой текст принят! Жди начала голосования 🤞")
    
    # Уведомление в группу
    if len(active_duel["paragraphs"]) == 1:
        send_telegram_message(GROUP_ID, f"🎯 Первый участник дуэли: {safe_username(user_id)}! Еще есть время присоединиться ⏳")

def handle_duel_vote(user_id: int, vote: int):
    """Обработка голоса в дуэли"""
    # Находим дуэль в стадии голосования
    voting_duel = None
    for duel in duels:
        if duel["status"] == "voting":
            voting_duel = duel
            break
    
    if not voting_duel:
        return
    
    # Проверяем, не голосовал ли уже
    if user_id in voting_duel["votes"]:
        send_telegram_message(user_id, "⚠️ Ты уже проголосовал в этой дуэли")
        return
    
    # Проверяем корректность голоса
    participants_count = len(voting_duel["paragraphs"])
    if 1 <= vote <= participants_count:
        voting_duel["votes"][user_id] = vote
        send_telegram_message(user_id, "✅ Твой голос учтен! Спасибо за участие 🤝")

# =========================
# CALLBACK ОБРАБОТЧИК
# =========================

def handle_callback(callback: dict):
    """Обработка callback запросов"""
    callback_id = callback["id"]
    user_id = int(callback["from"]["id"])
    data = callback.get("data", "")
    
    # Обновляем активность
    if user_id in users:
        users[user_id]["last_active"] = datetime.now().isoformat()
    
    if data == "start_duel":
        if is_user_registered(user_id):
            start_paragraph_duel(user_id)
            answer_callback(callback_id, "Дуэль началась! Смотри в группе ⚔️")
        else:
            answer_callback(callback_id, "Сначала зарегистрируйся через /start", show_alert=True)
    
    elif data == "truth_game":
        answer_callback(callback_id, "Игра 'Правда или выдумка' скоро будет доступна! 🎲")
    
    elif data == "wheel_game":
        answer_callback(callback_id, "Игра 'Колесо тем' скоро будет доступна! 🎡")
    
    else:
        answer_callback(callback_id, "Кнопка пока не работает. Скоро добавим функционал! 🚧")

# =========================
# АДМИН ФУНКЦИИ
# =========================

def publish_reading_list():
    """Опубликовать лист чтения (админ)"""
    if not articles_queue:
        send_telegram_message(GROUP_ID, "📭 <b>Лист чтения</b>\n\nОчередь пустая. Нечего публиковать 😔")
        return
    
    # Берем до 5 статей из очереди
    articles_to_publish = []
    while len(articles_to_publish) < 5 and articles_queue:
        article = articles_queue.popleft()
        articles_to_publish.append(article)
        published_articles.append(article)
    
    # Формируем лист чтения
    lines = [f"📚 <b>Лист чтения на {datetime.now().strftime('%d.%m.%Y')}</b>\n"]
    
    for i, article in enumerate(articles_to_publish, 1):
        author = safe_username(article["user_id"])
        title = html_escape(article["title"])
        description = html_escape(article["description"][:200])
        url = article["url"]
        
        lines.append(f"""
<b>{i}. {title}</b>
👤 <i>Автор: {author}</i>
📝 {description}...
🔗 <a href="{url}">Читать статью</a>
""")
    
    lines.append("""
<b>🎯 Задание на сегодня:</b>
1. Прочитай минимум 1 статью
2. Оставь конструктивный фидбек
3. Получи кавычки за активность

<b>⏰ Фидбек принимаем до 23:59 МСК</b>""")
    
    text = "\n".join(lines)
    send_telegram_message(GROUP_ID, text)
    
    # Награждаем авторов
    for article in articles_to_publish:
        user_id = article["user_id"]
        add_quotes(user_id, 15, "Статья опубликована в листе чтения")
        send_telegram_message(user_id, f"🎉 Твоя статья '{html_escape(article['title'][:50])}...' опубликована в листе чтения! +15 кавычек 🪙")
    
    return f"Опубликовано {len(articles_to_publish)} статей"

def show_admin_stats(user_id: int):
    """Показать статистику админу"""
    stats = {
        "users": len(users),
        "articles_in_queue": len(articles_queue),
        "articles_published_today": len(published_articles),
        "total_quotes": sum(user_balances.values()),
        "active_duels": len([d for d in duels if d["status"] in ["waiting", "voting"]]),
        "last_hour_active": len([u for u in users.values() 
                                 if (datetime.now() - datetime.fromisoformat(u["last_active"])).seconds < 3600])
    }
    
    text = f"""📊 <b>Статистика системы</b>

<b>Пользователи:</b> {stats['users']}
<b>Активных за час:</b> {stats['last_hour_active']}
<b>Статей в очереди:</b> {stats['articles_in_queue']}/10
<b>Опубликовано сегодня:</b> {stats['articles_published_today']}
<b>Всего кавычек в системе:</b> {stats['total_quotes']} 🪙
<b>Активных дуэлей:</b> {stats['active_duels']}

<b>Админ команды:</b>
/publish_reading_list - опубликовать лист чтения"""
    
    send_telegram_message(user_id, text)

# =========================
# WEBHOOK И МАРШРУТЫ FLASK
# =========================

@app.route("/webhook", methods=["POST"])
def webhook():
    """Обработка вебхука от Telegram"""
    try:
        data = request.get_json(force=True, silent=True) or {}
        
        # Логируем тип обновления
        update_keys = list(data.keys())
        logger.info(f"📨 Получен вебхук: {update_keys}")
        
        # Обработка сообщения
        if "message" in data:
            process_message(data["message"])
        
        # Обработка callback запросов
        elif "callback_query" in data:
            handle_callback(data["callback_query"])
        
        return jsonify({"status": "ok"}), 200
    
    except Exception as e:
        logger.error(f"❌ Ошибка в webhook: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500

@app.route("/health", methods=["GET"])
def health():
    """Health check для Render"""
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "users": len(users),
        "queue": len(articles_queue),
        "version": "2.0"
    }), 200

@app.route("/", methods=["GET"])
def home():
    """Главная страница"""
    return """
    <h1>🤖 Бот для клуба "Увлекательные чтения"</h1>
    <p>Статус: <strong>Работает ✅</strong></p>
    <p>Пользователей: {}</p>
    <p>Статей в очереди: {}</p>
    <p><a href="/health">Проверка здоровья</a></p>
    """.format(len(users), len(articles_queue))

# =========================
# АВТОМАТИЧЕСКИЕ ЗАДАЧИ
# =========================

def schedule_daily_tasks():
    """Планировщик ежедневных задач"""
    def task_loop():
        while True:
            now = datetime.now()
            
            # Публикация листа чтения в 19:00 МСК (16:00 UTC)
            if now.hour == 16 and now.minute == 0:
                if articles_queue:
                    publish_reading_list()
            
            # Ежедневный сброс счетчиков в 00:00 МСК (21:00 UTC предыдущего дня)
            if now.hour == 21 and now.minute == 0:
                published_articles.clear()
                logger.info("📅 Ежедневный сброс: очищен список опубликованных статей")
            
            time.sleep(60)  # Проверка каждую минуту
    
    thread = threading.Thread(target=task_loop, daemon=True)
    thread.start()

def schedule_submit_reminders():
    """Напоминания о возможности подать статью"""
    def reminder_loop():
        while True:
            now = datetime.now()
            
            for user_id, last_submit in list(user_last_submit.items()):
                if not isinstance(last_submit, datetime):
                    continue
                
                # Проверяем, прошло ли 48 часов
                hours_passed = (now - last_submit).total_seconds() / 3600
                if hours_passed >= 48:
                    # Проверяем, не уведомляли ли уже
                    last_notified = user_submit_notified.get(user_id)
                    if not last_notified or (now - last_notified).total_seconds() > 3600:
                        if user_id in users:
                            send_telegram_message(
                                user_id,
                                "🔔 <b>Можно подать новую статью!</b>\n\nПрошло более 48 часов с последней подачи. Используй /submit ✍️"
                            )
                            user_submit_notified[user_id] = now
            
            time.sleep(300)  # Проверка каждые 5 минут
    
    thread = threading.Thread(target=reminder_loop, daemon=True)
    thread.start()

# =========================
# ИНИЦИАЛИЗАЦИЯ
# =========================

def init():
    """Инициализация приложения"""
    logger.info("🚀 Инициализация бота...")
    
    # Запуск фоновых задач
    schedule_daily_tasks()
    schedule_submit_reminders()
    
    logger.info(f"✅ Бот инициализирован. Пользователей: {len(users)}, Очередь: {len(articles_queue)}")

# Запуск инициализации при импорте
init()

# =========================
# ЗАПУСК СЕРВЕРА
# =========================

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)

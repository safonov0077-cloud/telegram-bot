import os
import json
import logging
import threading
import time
import re
import atexit
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

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "").strip()
if not TELEGRAM_TOKEN:
    logger.warning("TELEGRAM_TOKEN пустой. Бот не сможет отвечать, пока не задашь переменную окружения.")

# Лучше использовать числовой ID супергруппы вида -100xxxxxxxxxx.
# Если оставишь @username, обычно тоже работает, но с темами иногда бывают сюрпризы.
GROUP_ID = os.environ.get("GROUP_ID", "@uvlekatelnyechteniya").strip()

# Админы (ID пользователей)
ADMIN_IDS = set(map(int, os.environ.get("ADMIN_IDS", "1039651708").split(",")))

# Темы (message_thread_id)
GROUP_TOPICS = {
    "announcements": 1,   # Объявления
    "rules": 2,           # Правила и FAQ
    "queue": 3,           # Очередь публикаций
    "reading_list": 4,    # Лист чтения дня
    "feedback": 5,        # Фидбек
    "duels": 6,           # Дуэли
    "games": 7,           # Игры дня
    "shop": 8,            # Магазин
    "offtop": 9,          # Оффтоп
}

DATA_FILE = os.environ.get("BOT_DATA_FILE", "data.json")
DATA_LOCK = threading.Lock()

# =========================
# ДАННЫЕ (память -> JSON)
# =========================

users = {}  # user_id -> profile dict
articles_queue = deque(maxlen=10)
published_articles = []  # list of articles (today)
user_articles = defaultdict(list)  # user_id -> list of articles
user_balances = defaultdict(int)  # user_id -> quotes
user_last_submit = {}  # user_id -> datetime
user_daily_reward = {}  # user_id -> ISO date string
user_submit_notified = {}  # user_id -> ISO datetime string last notification

games_history = []  # truth/wheel etc
games_results = []  # for pinned results
games_pin_message_id = None

duels = []  # paragraph duels

# Новое: кости со ставками
dice_games = {}  # game_id -> dict

# Новое: закрепленные меню в темах
topic_menu_message_ids = {}  # topic_key -> message_id

# Новое: “чистый UI” для каждого пользователя (чтоб не мусорить)
# ключ: (user_id, chat_id, thread_id) -> last_message_id
user_last_ui_message = {}

# Новое: стейт для подачи статьи в личке
user_states = {}  # user_id -> dict(state=..., started_at=...)

# =========================
# ТЕКСТЫ (легче, позитивнее, 3 платформы)
# =========================

ALLOWED_PLATFORMS_TEXT = "VK, Дзен, Telegram"
ALLOWED_DOMAINS = {
    "vk.com", "m.vk.com",
    "dzen.ru", "zen.yandex.ru",
    "t.me", "telegra.ph",
}

WELCOME_PRIVATE = (
    "📚 <b>Увлекательные чтения</b>\n\n"
    "Тут не цирк взаимных лайков, а нормальный клуб: читаем, обсуждаем, растем.\n"
    "Есть очередь, лист чтения в 19:00 МСК и игры, чтобы мозг не превращался в пюре.\n\n"
    "Ссылки на статьи принимаем только: <b>{}</b>.\n"
    "Это не потому что мы вредные. Хотя и это тоже.\n\n"
    "Команды:\n"
    "/help - помощь\n"
    "/submit - подать статью (в личке)\n"
    "/profile - профиль\n"
    "/balance - баланс\n"
).format(ALLOWED_PLATFORMS_TEXT)

# =========================
# TELEGRAM API HELPERS
# =========================

def tg(method: str, payload: dict, timeout: int = 12):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/{method}"
    try:
        r = requests.post(url, json=payload, timeout=timeout)
        r.raise_for_status()
        data = r.json()
        if not data.get("ok"):
            logger.error("Telegram API error %s: %s", method, data)
        return data
    except Exception as e:
        logger.error("Telegram request failed %s: %s", method, e)
        return None

def send_telegram_message(chat_id, text, topic_id=None, reply_to_message_id=None, parse_mode="HTML", reply_markup=None, disable_web_page_preview=True):
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": parse_mode,
        "disable_web_page_preview": disable_web_page_preview,
    }
    if topic_id:
        payload["message_thread_id"] = topic_id
    if reply_to_message_id:
        payload["reply_to_message_id"] = reply_to_message_id
    if reply_markup:
        payload["reply_markup"] = reply_markup
    return tg("sendMessage", payload)

def edit_message_text(chat_id, message_id, text, reply_markup=None, parse_mode="HTML", disable_web_page_preview=True):
    payload = {
        "chat_id": chat_id,
        "message_id": message_id,
        "text": text,
        "parse_mode": parse_mode,
        "disable_web_page_preview": disable_web_page_preview,
    }
    if reply_markup is not None:
        payload["reply_markup"] = reply_markup
    return tg("editMessageText", payload)

def edit_message_reply_markup(chat_id, message_id, reply_markup=None):
    payload = {"chat_id": chat_id, "message_id": message_id, "reply_markup": reply_markup}
    return tg("editMessageReplyMarkup", payload)

def delete_telegram_message(chat_id, message_id):
    payload = {"chat_id": chat_id, "message_id": message_id}
    return tg("deleteMessage", payload)

def pin_message(chat_id, message_id, disable_notification=True):
    payload = {"chat_id": chat_id, "message_id": message_id, "disable_notification": disable_notification}
    return tg("pinChatMessage", payload)

def answer_callback(callback_query_id, text, show_alert=False):
    payload = {"callback_query_id": callback_query_id, "text": text, "show_alert": show_alert}
    return tg("answerCallbackQuery", payload)

def send_dice(chat_id, topic_id=None, emoji="🎲"):
    payload = {"chat_id": chat_id, "emoji": emoji, "disable_notification": True}
    if topic_id:
        payload["message_thread_id"] = topic_id
    return tg("sendDice", payload, timeout=15)

# =========================
# UTILS
# =========================

def html_escape(s: str) -> str:
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
    cmd = (text or "").split()[0].strip().lower()
    if "@" in cmd:
        cmd = cmd.split("@", 1)[0]
    return cmd

def is_group_chat(chat_id) -> bool:
    # В Telegram супергруппы обычно имеют отрицательный int id.
    if isinstance(chat_id, int) and chat_id < 0:
        return True
    if isinstance(chat_id, str) and (chat_id.startswith("@") or chat_id.startswith("-100")):
        return True
    return False

def safe_username(user_id: int) -> str:
    u = users.get(user_id, {})
    username = u.get("username")
    if username:
        return "@" + username
    name = (u.get("first_name", "") + " " + u.get("last_name", "")).strip()
    return name if name else f"пользователь {user_id}"

def parse_domain(url: str) -> str:
    try:
        p = urlparse(url.strip())
        return (p.netloc or "").lower()
    except Exception:
        return ""

def is_allowed_article_url(url: str) -> bool:
    if not url or not isinstance(url, str):
        return False
    url = url.strip()
    if not (url.startswith("http://") or url.startswith("https://")):
        return False
    domain = parse_domain(url)
    if not domain:
        return False
    # нормализуем www.
    if domain.startswith("www."):
        domain = domain[4:]
    if domain in ALLOWED_DOMAINS:
        return True
    # на всякий случай разрешим поддомены
    for d in ALLOWED_DOMAINS:
        if domain.endswith("." + d):
            return True
    return False

# =========================
# PERSISTENCE
# =========================

def save_data():
    with DATA_LOCK:
        payload = {
            "users": users,
            "articles_queue": list(articles_queue),
            "published_articles": published_articles,
            "user_articles": dict(user_articles),
            "user_balances": dict(user_balances),
            "user_last_submit": {str(k): v.isoformat() for k, v in user_last_submit.items()},
            "user_daily_reward": user_daily_reward,
            "user_submit_notified": user_submit_notified,
            "games_history": games_history,
            "games_results": games_results,
            "games_pin_message_id": games_pin_message_id,
            "duels": duels,
            "dice_games": dice_games,
            "topic_menu_message_ids": topic_menu_message_ids,
        }
        try:
            with open(DATA_FILE, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error("save_data error: %s", e)

def load_data():
    global users, articles_queue, published_articles, user_articles
    global user_balances, user_last_submit, user_daily_reward, user_submit_notified
    global games_history, games_results, games_pin_message_id, duels
    global dice_games, topic_menu_message_ids

    if not os.path.exists(DATA_FILE):
        return
    try:
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        users = data.get("users", {})
        articles_queue = deque(data.get("articles_queue", []), maxlen=10)
        published_articles = data.get("published_articles", [])
        user_articles = defaultdict(list, data.get("user_articles", {}))
        user_balances = defaultdict(int, data.get("user_balances", {}))
        user_last_submit = {
            int(k): datetime.fromisoformat(v) for k, v in data.get("user_last_submit", {}).items()
        }
        user_daily_reward = data.get("user_daily_reward", {})
        user_submit_notified = data.get("user_submit_notified", {})
        games_history = data.get("games_history", [])
        games_results = data.get("games_results", [])
        games_pin_message_id = data.get("games_pin_message_id")
        duels = data.get("duels", [])
        dice_games = data.get("dice_games", {})
        topic_menu_message_ids = data.get("topic_menu_message_ids", {})
        logger.info("✅ Данные загружены")
    except Exception as e:
        logger.error("load_data error: %s", e)

def schedule_data_saves(interval_seconds=60):
    def loop():
        while True:
            time.sleep(interval_seconds)
            save_data()
    t = threading.Thread(target=loop, daemon=True)
    t.start()

# =========================
# CLEAN UI (удаляем прошлое сообщение бота для конкретного пользователя)
# =========================

def send_clean_ui(chat_id, user_id, text, topic_id=None, reply_markup=None, ttl_seconds=None):
    key = (int(user_id), str(chat_id), int(topic_id or 0))
    old_id = user_last_ui_message.get(key)
    if old_id:
        delete_telegram_message(chat_id, old_id)

    result = send_telegram_message(chat_id, text, topic_id=topic_id, reply_markup=reply_markup)
    if result and result.get("ok") and result.get("result", {}).get("message_id"):
        mid = result["result"]["message_id"]
        user_last_ui_message[key] = mid
        if ttl_seconds:
            threading.Timer(ttl_seconds, delete_telegram_message, args=[chat_id, mid]).start()
    return result

# =========================
# KEYBOARDS
# =========================

def build_main_menu_inline():
    return {
        "inline_keyboard": [
            [
                {"text": "📜 Правила", "callback_data": "menu_rules"},
                {"text": "📋 Очередь", "callback_data": "menu_queue"},
            ],
            [
                {"text": "👤 Профиль", "callback_data": "menu_profile"},
                {"text": "💰 Баланс", "callback_data": "menu_balance"},
            ],
            [
                {"text": "🎮 Игры", "callback_data": "menu_games"},
                {"text": "🛒 Магазин", "callback_data": "menu_shop"},
            ],
            [
                {"text": "✍️ Подать статью", "callback_data": "menu_submit"},
                {"text": "🏆 Топ", "callback_data": "menu_top"},
            ],
        ]
    }

def build_private_reply_keyboard():
    # Это “обычная клавиатура” (ReplyKeyboardMarkup). В личке удобно, в группе хуже.
    return {
        "keyboard": [
            ["📜 Правила", "📋 Очередь"],
            ["👤 Профиль", "💰 Баланс"],
            ["🎮 Игры", "🛒 Магазин"],
            ["✍️ Подать статью", "🏆 Топ"],
        ],
        "resize_keyboard": True,
        "one_time_keyboard": False,
        "input_field_placeholder": "Выбери действие кнопкой или напиши /help",
    }

def topic_menu_keyboard(topic_key: str):
    common = [
        {"text": "💰 Баланс", "callback_data": "m:balance"},
        {"text": "👤 Профиль", "callback_data": "m:profile"},
    ]

    if topic_key == "rules":
        return {"inline_keyboard": [
            [{"text": "📜 Правила (кратко)", "callback_data": "m:rules_short"}],
            [{"text": "🧭 Как все устроено", "callback_data": "m:how_it_works"}],
            common
        ]}

    if topic_key == "queue":
        return {"inline_keyboard": [
            [{"text": "📋 Показать очередь", "callback_data": "m:queue"}],
            [{"text": "✍️ Подать статью (в личку)", "callback_data": "m:submit_hint"}],
            common
        ]}

    if topic_key == "reading_list":
        return {"inline_keyboard": [
            [{"text": "📚 Лист чтения (сегодня)", "callback_data": "m:reading_today"}],
            [{"text": "🔔 Напомнить про подачу", "callback_data": "m:submit_remind"}],
            common
        ]}

    if topic_key == "feedback":
        return {"inline_keyboard": [
            [{"text": "💬 Как дать фидбек", "callback_data": "m:feedback_how"}],
            [{"text": "🎁 Награда за фидбек", "callback_data": "m:feedback_reward"}],
            common
        ]}

    if topic_key == "duels":
        return {"inline_keyboard": [
            [{"text": "⚔️ Начать дуэль", "callback_data": "m:duel_start"}],
            [{"text": "📌 Как участвовать", "callback_data": "m:duel_how"}],
            common
        ]}

    if topic_key == "games":
        return {"inline_keyboard": [
            [{"text": "🎲 Кости (ставки)", "callback_data": "m:dice"}],
            [{"text": "🤥 Правда или выдумка", "callback_data": "m:truth"}],
            [{"text": "🎡 Колесо тем", "callback_data": "m:wheel"}],
            [{"text": "🏆 Результаты игр", "callback_data": "m:games_results"}],
            common
        ]}

    if topic_key == "shop":
        return {"inline_keyboard": [
            [{"text": "🛒 Витрина", "callback_data": "m:shop_show"}],
            [{"text": "🎁 Купить", "callback_data": "m:shop_buy"}],
            common
        ]}

    if topic_key == "offtop":
        return {"inline_keyboard": [
            [{"text": "😄 Шутка дня", "callback_data": "m:joke"}],
            common
        ]}

    return {"inline_keyboard": [common]}

def topic_menu_text(topic_key: str) -> str:
    if topic_key == "rules":
        return (
            "📜 <b>Правила и FAQ</b>\n\n"
            "Тут все по-взрослому, но без занудства.\n"
            "Очередь, лист чтения, фидбек, игры и кавычки 🪙.\n"
            f"Ссылки принимаем только: <b>{ALLOWED_PLATFORMS_TEXT}</b>.\n"
        )
    if topic_key == "queue":
        return (
            "📋 <b>Очередь публикаций</b>\n\n"
            "Порядок спасает нервы. И авторов тоже.\n"
            "Ограничение: 1 ссылка раз в 48-72 часа, 1 активная ссылка на участника.\n"
        )
    if topic_key == "reading_list":
        return (
            "📚 <b>Лист чтения</b>\n\n"
            "Каждый день в 19:00 МСК публикуем лист на 5-10 ссылок.\n"
            "Читаем его, а не превращаем чат в свалку ссылок.\n"
        )
    if topic_key == "feedback":
        return (
            "💬 <b>Фидбек</b>\n\n"
            "Можно быть строгим к тексту. Нельзя быть токсичным к человеку.\n"
            "Комментарий уровня “норм” не считается фидбеком. Да, жизнь жестока.\n"
        )
    if topic_key == "duels":
        return (
            "⚔️ <b>Дуэли</b>\n\n"
            "Дуэль абзацев: тема, таймер, голосование, приз.\n"
            "Пишем коротко, бодро, без взаимного поедания.\n"
        )
    if topic_key == "games":
        return (
            "🎮 <b>Игры дня</b>\n\n"
            "Игры нужны, чтобы клуб не превращался в обязаловку.\n"
            "Тут есть факты, темы и кости со ставками. Да, мы взрослые люди. Почти.\n"
        )
    if topic_key == "shop":
        return (
            "🛒 <b>Магазин</b>\n\n"
            "Тут тратятся “Кавычки” 🪙.\n"
            "Пока витрина небольшая, но будет веселее. Люди любят кнопки и блестяшки.\n"
        )
    if topic_key == "offtop":
        return (
            "😄 <b>Оффтоп</b>\n\n"
            "Иногда надо выдохнуть. Тут можно шутки, курьезы и просто поболтать.\n"
        )
    return "📌 <b>Меню</b>"

def ensure_topic_menu(topic_key: str):
    if topic_key not in GROUP_TOPICS:
        return
    topic_id = GROUP_TOPICS[topic_key]
    text = topic_menu_text(topic_key)
    kb = topic_menu_keyboard(topic_key)

    existing_id = topic_menu_message_ids.get(topic_key)

    # Пытаемся обновить существующее
    if existing_id:
        res = edit_message_text(GROUP_ID, existing_id, text, reply_markup=kb)
        if res and res.get("ok"):
            return

    # Создаем новое
    res = send_telegram_message(GROUP_ID, text, topic_id=topic_id, reply_markup=kb)
    if res and res.get("ok") and res.get("result", {}).get("message_id"):
        mid = res["result"]["message_id"]
        topic_menu_message_ids[topic_key] = mid
        pin_message(GROUP_ID, mid, disable_notification=True)

def ensure_all_topic_menus():
    for k in GROUP_TOPICS.keys():
        ensure_topic_menu(k)

# =========================
# USERS
# =========================

def is_user_registered(user_id: int) -> bool:
    return int(user_id) in users

def register_user(user_data: dict) -> bool:
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
        "feedback_received": 0,
        "games_played": 0,
        "duels_won": 0,
        "dice_wins": 0,
        "dice_losses": 0,
        "total_quotes": 0,
        "badges": ["новичок"],
        "subscription": "free",
        "last_active": datetime.now().isoformat()
    }
    user_balances[user_id] = 50

    # Привет в личку + reply-клавиатура
    send_telegram_message(
        user_id,
        WELCOME_PRIVATE,
        reply_markup=build_private_reply_keyboard()
    )

    logger.info("✅ Зарегистрирован новый пользователь: %s", user_id)
    return True

# =========================
# QUOTES / ACHIEVEMENTS
# =========================

def add_quotes(user_id: int, amount: int, reason: str):
    user_id = int(user_id)
    user_balances[user_id] += int(amount)
    if user_id in users:
        users[user_id]["total_quotes"] += int(amount)
        check_achievements(user_id)
    logger.info("💰 %s: %+d кавычек (%s)", user_id, amount, reason)
    return user_balances[user_id]

def spend_quotes(user_id: int, amount: int, reason: str):
    user_id = int(user_id)
    amount = int(amount)
    if user_balances[user_id] < amount:
        return False
    user_balances[user_id] -= amount
    logger.info("🪙 %s: -%d кавычек (%s)", user_id, amount, reason)
    return True

def check_achievements(user_id: int):
    user = users.get(user_id)
    if not user:
        return
    new_badges = []

    tq = user.get("total_quotes", 0)
    if tq >= 1000 and "магнат" not in user["badges"]:
        new_badges.append("магнат")
    elif tq >= 500 and "богач" not in user["badges"]:
        new_badges.append("богач")
    elif tq >= 100 and "состоятельный" not in user["badges"]:
        new_badges.append("состоятельный")

    ac = user.get("articles_count", 0)
    if ac >= 50 and "прозаик" not in user["badges"]:
        new_badges.append("прозаик")
    elif ac >= 20 and "писатель" not in user["badges"]:
        new_badges.append("писатель")
    elif ac >= 10 and "автор" not in user["badges"]:
        new_badges.append("автор")

    fg = user.get("feedback_given", 0)
    if fg >= 100 and "наставник" not in user["badges"]:
        new_badges.append("наставник")
    elif fg >= 50 and "критик" not in user["badges"]:
        new_badges.append("критик")
    elif fg >= 20 and "читатель" not in user["badges"]:
        new_badges.append("читатель")

    for b in new_badges:
        if b not in user["badges"]:
            user["badges"].append(b)
            send_telegram_message(
                user_id,
                f"🎖 <b>Новый бейдж!</b>\n\n<b>{html_escape(b.upper())}</b>\n\nПродолжай, человек. Это почти похвала 🙂"
            )

# =========================
# QUEUE / SUBMIT
# =========================

def can_submit_article(user_id: int):
    user_id = int(user_id)
    if user_id not in user_last_submit:
        return True, "Можно подавать"

    last_submit = user_last_submit[user_id]
    time_diff = datetime.now() - last_submit

    min_hours = 48
    if time_diff.total_seconds() < min_hours * 3600:
        hours_left = int((min_hours * 3600 - time_diff.total_seconds()) / 3600)
        return False, f"⏳ Можно будет подать через {hours_left} ч."

    if any(a["user_id"] == user_id for a in articles_queue):
        return False, "⚠️ У тебя уже есть ссылка в очереди"

    if len(articles_queue) >= 10:
        return False, "📦 Очередь забита (макс 10). Загляни позже."

    return True, "Можно подавать"

def add_article_to_queue(user_id: int, title: str, description: str, url: str):
    user_id = int(user_id)
    article_id = f"art_{int(time.time())}_{user_id}"

    article = {
        "id": article_id,
        "user_id": user_id,
        "title": title[:120],
        "description": description[:600],
        "content": url,
        "submitted_at": datetime.now().isoformat(),
        "status": "pending",
        "feedback_count": 0,
        "reads": 0,
        "likes": 0
    }

    articles_queue.append(article)
    user_articles[user_id].append(article)
    user_last_submit[user_id] = datetime.now()
    user_submit_notified[user_id] = ""  # сбросим уведомление, чтобы потом напомнить

    add_quotes(user_id, 10, "Подача статьи")
    if user_id in users:
        users[user_id]["articles_count"] += 1

    logger.info("📝 Добавлено в очередь: %s", article_id)
    return article_id

def start_article_submission(user_id: int):
    can_submit, msg = can_submit_article(user_id)
    if not can_submit:
        send_telegram_message(user_id, msg)
        return

    user_states[int(user_id)] = {"state": "await_article", "started_at": datetime.now().isoformat()}

    text = (
        "✍️ <b>Подача статьи</b>\n\n"
        "Ссылки принимаем только: <b>{}</b>\n"
        "Формат сообщения такой:\n\n"
        "<b>ЗАГОЛОВОК</b>\n"
        "Твой заголовок\n\n"
        "<b>ОПИСАНИЕ</b>\n"
        "2-3 предложения, по делу\n\n"
        "<b>ССЫЛКА</b>\n"
        "https://...\n\n"
        "Подсказка: “норм” не считается описанием 🙂"
    ).format(ALLOWED_PLATFORMS_TEXT)

    send_telegram_message(user_id, text)

def parse_submission_text(text: str):
    # Пытаемся вытащить блоки ЗАГОЛОВОК / ОПИСАНИЕ / ССЫЛКА
    t = (text or "").strip()
    # Упростим: ищем маркеры по строкам
    def block(name):
        pattern = rf"{name}\s*\n(.+?)(?=\n[A-ZА-ЯЁ ]+\n|\Z)"
        m = re.search(pattern, t, flags=re.S | re.I)
        return m.group(1).strip() if m else ""

    title = block("ЗАГОЛОВОК")
    desc = block("ОПИСАНИЕ")
    link = block("ССЫЛКА").split()[0].strip() if block("ССЫЛКА") else ""

    return title, desc, link

# =========================
# READING LIST
# =========================

def publish_daily_reading_list():
    if not articles_queue:
        return "📭 Очередь пустая"

    today_articles = list(articles_queue)[:10]  # 5-10

    header = f"📚 <b>Лист чтения на {datetime.now().strftime('%d.%m.%Y')}</b>\n\n"
    body = "Сегодня читаем вот это:\n"
    lines = []

    for i, a in enumerate(today_articles, 1):
        author = safe_username(a["user_id"])
        title = html_escape(a["title"])
        desc = html_escape(a["description"][:160])
        url = a["content"]
        lines.append(
            f"\n<b>{i}. {title}</b>\n"
            f"Автор: {html_escape(author)}\n"
            f"{desc}\n"
            f"<a href=\"{html_escape(url)}\">Открыть статью</a>"
        )

    footer = (
        "\n\n<b>Задание на сегодня</b>\n"
        "1) Прочитай минимум 1 статью\n"
        "2) Оставь нормальный фидбек\n"
        "3) Забери кавычки 🪙\n\n"
        "Фидбек можно оставлять до 23:59 МСК.\n"
        "И да, клуб не кусается. Максимум слегка стыдит 🙂"
    )

    text = header + body + "".join(lines) + footer

    send_telegram_message(GROUP_ID, text, topic_id=GROUP_TOPICS["reading_list"])

    # помечаем опубликованные
    for a in today_articles:
        a["status"] = "published"
        a["published_at"] = datetime.now().isoformat()
        published_articles.append(a)

    for _ in range(len(today_articles)):
        if articles_queue:
            articles_queue.popleft()

    return f"Опубликовано {len(today_articles)}"

# =========================
# TOP / PROFILE / BALANCE
# =========================

def get_user_top(limit=10):
    rows = []
    for uid, u in users.items():
        rows.append({
            "id": int(uid),
            "name": u.get("first_name", ""),
            "username": u.get("username"),
            "articles": u.get("articles_count", 0),
            "quotes": int(user_balances.get(int(uid), 0)),
            "feedback_given": u.get("feedback_given", 0),
        })
    rows.sort(key=lambda x: x["quotes"], reverse=True)
    return rows[:limit]

def show_profile(user_id: int, chat_id=None, topic_id=None, as_clean_ui=False):
    user_id = int(user_id)
    if user_id not in users:
        send_telegram_message(user_id, "Сначала /start в личке. Telegram не умеет читать мысли.")
        return

    u = users[user_id]
    total_users = max(1, len(users))
    ranked = get_user_top(total_users)
    rank = next((i+1 for i, r in enumerate(ranked) if r["id"] == user_id), total_users)

    text = (
        "👤 <b>Профиль</b>\n\n"
        f"Имя: {html_escape((u.get('first_name','')+' '+u.get('last_name','')).strip())}\n"
        f"Юзернейм: @{html_escape(u.get('username') or 'не установлен')}\n"
        f"Рейтинг: #{rank} из {total_users}\n\n"
        "Статистика:\n"
        f"- Статей: {u.get('articles_count',0)}\n"
        f"- Фидбеков дано: {u.get('feedback_given',0)}\n"
        f"- Дуэлей выиграно: {u.get('duels_won',0)}\n"
        f"- Кости: побед {u.get('dice_wins',0)}, поражений {u.get('dice_losses',0)}\n"
        f"- Баланс: {user_balances.get(user_id,0)} кавычек 🪙\n\n"
        f"Бейджи: {', '.join(u.get('badges', []) )}"
    )

    if chat_id is None:
        chat_id = user_id

    if as_clean_ui:
        send_clean_ui(chat_id, user_id, text, topic_id=topic_id, ttl_seconds=90)
    else:
        send_telegram_message(chat_id, text, topic_id=topic_id)

def show_top(chat_id, topic_id=None):
    top = get_user_top(10)
    if not top:
        send_telegram_message(chat_id, "Пока топ пустой. Это редкий шанс стать легендой.")
        return

    medals = ["🥇","🥈","🥉","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣","🔟"]
    lines = ["🏆 <b>Топ участников</b>\n"]
    for i, u in enumerate(top):
        medal = medals[i] if i < len(medals) else f"{i+1}."
        name = ("@" + u["username"]) if u.get("username") else u.get("name") or str(u["id"])
        lines.append(f"{medal} <b>{html_escape(name)}</b> - {u['quotes']} 🪙, статей {u['articles']}, фидбеков {u['feedback_given']}")
    send_telegram_message(chat_id, "\n".join(lines), topic_id=topic_id)

def show_queue(chat_id, topic_id=None):
    if not articles_queue:
        send_telegram_message(chat_id, "📭 Очередь пустая. Редкое состояние гармонии.", topic_id=topic_id)
        return

    lines = ["📋 <b>Очередь публикаций</b>\n"]
    for i, a in enumerate(list(articles_queue)[:10], 1):
        author = safe_username(a["user_id"])
        title = html_escape(a["title"])
        lines.append(f"{i}. <b>{title}</b> (автор {html_escape(author)})")

    lines.append(f"\nВсего в очереди: {len(articles_queue)} из 10")
    send_telegram_message(chat_id, "\n".join(lines), topic_id=topic_id)

# =========================
# DAILY REWARD
# =========================

def give_daily_reward(user_id: int):
    user_id = int(user_id)
    today = datetime.now().date().isoformat()

    if user_daily_reward.get(str(user_id)) == today or user_daily_reward.get(user_id) == today:
        send_telegram_message(user_id, "⏳ Награда уже была сегодня. Завтра снова можно.")
        return

    reward = 5
    add_quotes(user_id, reward, "Ежедневная награда")
    user_daily_reward[user_id] = today

    send_telegram_message(
        user_id,
        f"🎁 <b>Ежедневная награда</b>\n\n+{reward} кавычек 🪙\nБаланс: {user_balances.get(user_id,0)}\n\nВозвращайся завтра. Ритуалы это основа цивилизации 🙂"
    )

# =========================
# GAMES PIN (результаты)
# =========================

def update_games_pin():
    global games_pin_message_id

    if not games_results:
        pin_text = "🏆 <b>Результаты игр</b>\n\nПока нет завершенных игр. Стыдно, но переживем."
    else:
        lines = ["🏆 <b>Результаты игр</b>\n"]
        for r in games_results[-10:]:
            winners = ", ".join(r.get("winners", [])) if r.get("winners") else "нет победителей"
            lines.append(f"• <b>{html_escape(r.get('title','Игра'))}</b> ({html_escape(r.get('date',''))})\n  Победители: {html_escape(winners)}")
        pin_text = "\n".join(lines)

    topic_id = GROUP_TOPICS["games"]
    if games_pin_message_id:
        edit_message_text(GROUP_ID, games_pin_message_id, pin_text)
        return

    res = send_telegram_message(GROUP_ID, pin_text, topic_id=topic_id)
    if res and res.get("ok") and res.get("result", {}).get("message_id"):
        games_pin_message_id = res["result"]["message_id"]
        pin_message(GROUP_ID, games_pin_message_id, disable_notification=True)

# =========================
# DICE GAME (ставки)
# =========================

def dice_stake_picker_keyboard():
    return {
        "inline_keyboard": [[
            {"text": "5 🪙", "callback_data": "dice:new:5"},
            {"text": "10 🪙", "callback_data": "dice:new:10"},
            {"text": "20 🪙", "callback_data": "dice:new:20"},
            {"text": "50 🪙", "callback_data": "dice:new:50"},
        ]]
    }

def dice_challenge_keyboard(game_id: str):
    return {
        "inline_keyboard": [[
            {"text": "✅ Принять", "callback_data": f"dice:join:{game_id}"},
            {"text": "🚫 Отмена", "callback_data": f"dice:cancel:{game_id}"},
        ]]
    }

def start_dice_challenge(creator_id: int, stake: int):
    creator_id = int(creator_id)
    stake = int(stake)

    if user_balances.get(creator_id, 0) < stake:
        return None, "Не хватает кавычек на ставку. Это не бедность, это сюжет."

    game_id = f"dice_{int(time.time())}_{creator_id}"
    dice_games[game_id] = {
        "id": game_id,
        "creator_id": creator_id,
        "stake": stake,
        "status": "open",
        "created_at": datetime.now().isoformat(),
        "message_id": None,
        "acceptor_id": None,
    }

    text = (
        "🎲 <b>Дуэль костей</b>\n\n"
        f"Игрок: {html_escape(safe_username(creator_id))}\n"
        f"Ставка: <b>{stake} кавычек</b> 🪙\n\n"
        "Кто смелый, жми “Принять”. Победитель забирает банк.\n"
        "Если ничья, переброс (один раз)."
    )

    res = send_telegram_message(GROUP_ID, text, topic_id=GROUP_TOPICS["games"], reply_markup=dice_challenge_keyboard(game_id))
    if res and res.get("ok") and res.get("result", {}).get("message_id"):
        dice_games[game_id]["message_id"] = res["result"]["message_id"]
        return game_id, "Создано"
    return None, "Не удалось создать игру (Telegram сегодня в настроении)."

def finish_dice_game(game_id: str, winner_id: int, loser_id: int, stake: int, v1: int, v2: int):
    winner_id = int(winner_id)
    loser_id = int(loser_id)
    stake = int(stake)

    add_quotes(winner_id, stake * 2, "Победа в костях")
    if winner_id in users:
        users[winner_id]["dice_wins"] = users[winner_id].get("dice_wins", 0) + 1
    if loser_id in users:
        users[loser_id]["dice_losses"] = users[loser_id].get("dice_losses", 0) + 1

    winners_names = [safe_username(winner_id)]
    games_results.append({
        "title": "Кости",
        "date": datetime.now().strftime("%d.%m.%Y"),
        "winners": winners_names,
    })
    update_games_pin()

def accept_dice_challenge(game_id: str, acceptor_id: int):
    acceptor_id = int(acceptor_id)
    g = dice_games.get(game_id)
    if not g or g.get("status") != "open":
        return False, "Игра уже не доступна."

    creator_id = int(g["creator_id"])
    if acceptor_id == creator_id:
        return False, "Играть с самим собой можно, но это уже психология, не игры."

    stake = int(g["stake"])
    if user_balances.get(creator_id, 0) < stake:
        return False, "У создателя ставки уже нет кавычек. Мистика."
    if user_balances.get(acceptor_id, 0) < stake:
        return False, "Тебе не хватает кавычек на ставку."

    # Блокируем ставки: списываем у обоих в банк
    spend_quotes(creator_id, stake, "Ставка в костях")
    spend_quotes(acceptor_id, stake, "Ставка в костях")

    g["status"] = "playing"
    g["acceptor_id"] = acceptor_id

    # Роллы через sendDice
    d1 = send_dice(GROUP_ID, topic_id=GROUP_TOPICS["games"], emoji="🎲")
    v1 = None
    if d1 and d1.get("ok"):
        v1 = d1["result"]["dice"]["value"]

    d2 = send_dice(GROUP_ID, topic_id=GROUP_TOPICS["games"], emoji="🎲")
    v2 = None
    if d2 and d2.get("ok"):
        v2 = d2["result"]["dice"]["value"]

    if not v1 or not v2:
        # если Telegram не отдал значения, честно вернем деньги
        add_quotes(creator_id, stake, "Возврат ставки (ошибка dice)")
        add_quotes(acceptor_id, stake, "Возврат ставки (ошибка dice)")
        g["status"] = "cancelled"
        return False, "Dice не сработал. Ставки возвращены."

    # Ничья: один переброс
    if v1 == v2:
        d1b = send_dice(GROUP_ID, topic_id=GROUP_TOPICS["games"], emoji="🎲")
        d2b = send_dice(GROUP_ID, topic_id=GROUP_TOPICS["games"], emoji="🎲")
        if d1b and d1b.get("ok"):
            v1 = d1b["result"]["dice"]["value"]
        if d2b and d2b.get("ok"):
            v2 = d2b["result"]["dice"]["value"]

    if v1 > v2:
        winner, loser = creator_id, acceptor_id
    elif v2 > v1:
        winner, loser = acceptor_id, creator_id
    else:
        # снова ничья: возвращаем
        add_quotes(creator_id, stake, "Возврат ставки (ничья)")
        add_quotes(acceptor_id, stake, "Возврат ставки (ничья)")
        g["status"] = "finished"
        return True, "Ничья. Ставки возвращены."

    finish_dice_game(game_id, winner, loser, stake, v1, v2)
    g["status"] = "finished"

    # Обновим исходное сообщение игры
    mid = g.get("message_id")
    if mid:
        text = (
            "🎲 <b>Дуэль костей завершена</b>\n\n"
            f"{html_escape(safe_username(creator_id))}: {v1}\n"
            f"{html_escape(safe_username(acceptor_id))}: {v2}\n\n"
            f"Победитель: <b>{html_escape(safe_username(winner))}</b>\n"
            f"Приз: <b>{stake*2} кавычек</b> 🪙\n"
        )
        edit_message_text(GROUP_ID, mid, text, reply_markup={"inline_keyboard": []})

    return True, "Сыграно"

def cancel_dice_game(game_id: str, requester_id: int):
    requester_id = int(requester_id)
    g = dice_games.get(game_id)
    if not g:
        return False, "Не нашел игру."

    if requester_id != int(g["creator_id"]) and requester_id not in ADMIN_IDS:
        return False, "Отменять может создатель или админ."

    if g.get("status") != "open":
        return False, "Эта игра уже началась или закончилась."

    g["status"] = "cancelled"
    mid = g.get("message_id")
    if mid:
        edit_message_text(GROUP_ID, mid, "🎲 Игра отменена. Никто не пострадал. Почти.", reply_markup={"inline_keyboard": []})
    return True, "Отменено"

# =========================
# SHOP (минимальный MVP)
# =========================

SHOP_ITEMS = [
    {"id": "badge_bookworm", "title": "Бейдж: Книжный маньяк", "price": 120, "type": "badge", "value": "книжный маньяк"},
    {"id": "badge_kind", "title": "Бейдж: Добрая критика", "price": 80, "type": "badge", "value": "добрая критика"},
]

def shop_list_text():
    lines = ["🛒 <b>Витрина</b>\n", "Траты делают жизнь ярче. Иногда.\n"]
    for it in SHOP_ITEMS:
        lines.append(f"• <b>{html_escape(it['title'])}</b> - {it['price']} 🪙")
    lines.append("\nПокупки пока простые: бейджи. Дальше будет веселее.")
    return "\n".join(lines)

def shop_list_keyboard():
    rows = []
    for it in SHOP_ITEMS:
        rows.append([{"text": f"Купить: {it['price']} 🪙", "callback_data": f"shop:buy:{it['id']}"}])
    return {"inline_keyboard": rows}

def shop_buy(user_id: int, item_id: str):
    user_id = int(user_id)
    it = next((x for x in SHOP_ITEMS if x["id"] == item_id), None)
    if not it:
        return False, "Товара нет. Как и смысла в этом мире."

    price = int(it["price"])
    if user_balances.get(user_id, 0) < price:
        return False, "Не хватает кавычек. Сначала заработай, потом шикуй 🙂"

    ok = spend_quotes(user_id, price, f"Покупка {item_id}")
    if not ok:
        return False, "Не вышло списать кавычки."

    if it["type"] == "badge" and user_id in users:
        badge = it["value"]
        if badge not in users[user_id]["badges"]:
            users[user_id]["badges"].append(badge)

    return True, f"Куплено: {it['title']}"

# =========================
# COMMANDS (личка и группа)
# =========================

def show_help(chat_id):
    text = (
        "📚 <b>Помощь</b>\n\n"
        "Главное:\n"
        "/start - регистрация\n"
        "/help - помощь\n\n"
        "Для автора (в личке):\n"
        "/submit - подать статью\n"
        "/profile - профиль\n"
        "/balance - баланс\n"
        "/daily - ежедневная награда\n\n"
        "Для группы:\n"
        "/queue - очередь\n"
        "/top - топ\n\n"
        f"Ссылки на статьи принимаем только: <b>{ALLOWED_PLATFORMS_TEXT}</b>.\n"
    )
    send_telegram_message(chat_id, text)

def show_rules(chat_id):
    text = (
        "📜 <b>Правила клуба</b>\n\n"
        "Цель: реальные чтения и фидбек, а не спам ссылками.\n\n"
        "Очередь:\n"
        "- 1 ссылка раз в 48-72 часа\n"
        "- 1 активная ссылка на участника\n"
        "- В день читаем лист, а не 200 ссылок подряд\n\n"
        f"Ссылки только: <b>{ALLOWED_PLATFORMS_TEXT}</b>\n\n"
        "Фидбек:\n"
        "- Можно жестко по тексту\n"
        "- Нельзя токсично по человеку\n"
        "- “норм” не фидбек 🙂\n\n"
        "Игры:\n"
        "- По желанию, но приветствуются\n"
        "- Кавычки это валюта для движухи, а не пропуск в рай\n"
    )
    send_telegram_message(chat_id, text)

# =========================
# CALLBACK HANDLER
# =========================

def handle_callback(callback):
    cb_id = callback["id"]
    user_id = int(callback["from"]["id"])
    data = callback.get("data", "")
    msg = callback.get("message", {})
    chat_id = msg.get("chat", {}).get("id", user_id)
    topic_id = msg.get("message_thread_id")

    # Обновим активность
    if user_id in users:
        users[user_id]["last_active"] = datetime.now().isoformat()

    # Баланс - лучше всплывашка
    if data == "menu_balance" or data == "m:balance":
        bal = user_balances.get(user_id, 0)
        answer_callback(cb_id, f"Баланс: {bal} 🪙", show_alert=True)
        return

    # Профиль - чистый UI в теме, чтобы не засорять
    if data in ("menu_profile", "m:profile"):
        show_profile(user_id, chat_id=chat_id, topic_id=topic_id, as_clean_ui=True)
        answer_callback(cb_id, "Профиль показан", show_alert=False)
        return

    # Основное меню
    if data == "menu_rules":
        show_rules(chat_id)
        answer_callback(cb_id, "Ок", show_alert=False)
        return

    if data == "menu_queue" or data == "m:queue":
        show_queue(chat_id, topic_id=topic_id)
        answer_callback(cb_id, "Очередь показана", show_alert=False)
        return

    if data == "menu_top":
        show_top(chat_id, topic_id=topic_id)
        answer_callback(cb_id, "Ок", show_alert=False)
        return

    if data == "menu_games":
        send_clean_ui(chat_id, user_id, "🎮 Игры находятся в теме “Игры дня”. Там же есть меню сверху.", topic_id=topic_id, ttl_seconds=40)
        answer_callback(cb_id, "Ок", show_alert=False)
        return

    if data == "menu_shop" or data == "m:shop_show":
        # покажем витрину как clean ui
        send_clean_ui(chat_id, user_id, shop_list_text(), topic_id=topic_id, reply_markup=shop_list_keyboard(), ttl_seconds=120)
        answer_callback(cb_id, "Витрина показана", show_alert=False)
        return

    if data == "menu_submit" or data == "m:submit_hint":
        # подсказка, что submit только в личке
        send_clean_ui(chat_id, user_id, "✍️ Подача статьи работает только в личке с ботом. Напиши мне /start и потом /submit.", topic_id=topic_id, ttl_seconds=60)
        answer_callback(cb_id, "Ок", show_alert=False)
        return

    # Меню тем
    if data == "m:rules_short":
        send_clean_ui(chat_id, user_id, "📜 Кратко: очередь, лист чтения, фидбек по делу, ссылки только VK/Дзен/Telegram.", topic_id=topic_id, ttl_seconds=60)
        answer_callback(cb_id, "Ок", show_alert=False)
        return

    if data == "m:how_it_works":
        text = (
            "🧭 <b>Как тут все устроено</b>\n\n"
            "1) Очередь: подаешь 1 ссылку раз в 48-72 часа.\n"
            "2) В 19:00 МСК выходит лист чтения на 5-10 ссылок.\n"
            "3) Читаем лист, пишем фидбек, получаем кавычки 🪙.\n"
            "4) Игры и дуэли для разрядки.\n"
        )
        send_clean_ui(chat_id, user_id, text, topic_id=topic_id, ttl_seconds=120)
        answer_callback(cb_id, "Ок", show_alert=False)
        return

    if data == "m:reading_today":
        send_clean_ui(chat_id, user_id, "Лист чтения публикуется в 19:00 МСК в теме “Лист чтения дня”.", topic_id=topic_id, ttl_seconds=60)
        answer_callback(cb_id, "Ок", show_alert=False)
        return

    if data == "m:submit_remind":
        send_clean_ui(chat_id, user_id, "Напоминания о возможности подать ссылку приходят в личку. Для этого нужен /start в личке.", topic_id=topic_id, ttl_seconds=80)
        answer_callback(cb_id, "Ок", show_alert=False)
        return

    if data == "m:feedback_how":
        send_clean_ui(
            chat_id, user_id,
            "💬 Фидбек по-человечески:\n"
            "- Что понравилось\n"
            "- Что можно улучшить\n"
            "- Самая сильная деталь\n"
            "- Один совет автору\n\n"
            "“норм” не фидбек 🙂",
            topic_id=topic_id, ttl_seconds=120
        )
        answer_callback(cb_id, "Ок", show_alert=False)
        return

    if data == "m:feedback_reward":
        send_clean_ui(chat_id, user_id, "🎁 За качественный фидбек можно давать кавычки. Это клуб, а не суд, но поощрения будут.", topic_id=topic_id, ttl_seconds=90)
        answer_callback(cb_id, "Ок", show_alert=False)
        return

    if data == "m:duel_start":
        # старт дуэли текстом-командой: пусть будет доступно любому
        start_paragraph_duel(user_id)
        answer_callback(cb_id, "Дуэль создана", show_alert=False)
        return

    if data == "m:duel_how":
        send_clean_ui(chat_id, user_id, "⚔️ Участие: отвечаешь на сообщение дуэли своим абзацем. Потом голосование.", topic_id=topic_id, ttl_seconds=80)
        answer_callback(cb_id, "Ок", show_alert=False)
        return

    # Dice stake picker
    if data == "m:dice":
        send_clean_ui(chat_id, user_id, "🎲 Выбери ставку:", topic_id=topic_id, reply_markup=dice_stake_picker_keyboard(), ttl_seconds=60)
        answer_callback(cb_id, "Ок", show_alert=False)
        return

    if data.startswith("dice:new:"):
        if not is_user_registered(user_id):
            answer_callback(cb_id, "Сначала /start в личке с ботом.", show_alert=True)
            return
        try:
            stake = int(data.split(":")[-1])
        except Exception:
            answer_callback(cb_id, "Ставка не распознана.", show_alert=True)
            return

        gid, msg2 = start_dice_challenge(user_id, stake)
        if gid:
            answer_callback(cb_id, "Игра создана в теме “Игры дня”.", show_alert=False)
        else:
            answer_callback(cb_id, msg2, show_alert=True)
        return

    if data.startswith("dice:join:"):
        gid = data.split(":", 2)[2]
        ok, msg2 = accept_dice_challenge(gid, user_id)
        answer_callback(cb_id, msg2, show_alert=not ok)
        return

    if data.startswith("dice:cancel:"):
        gid = data.split(":", 2)[2]
        ok, msg2 = cancel_dice_game(gid, user_id)
        answer_callback(cb_id, msg2, show_alert=not ok)
        return

    # Shop buy
    if data.startswith("shop:buy:"):
        if not is_user_registered(user_id):
            answer_callback(cb_id, "Сначала /start в личке.", show_alert=True)
            return
        item_id = data.split(":", 2)[2]
        ok, msg2 = shop_buy(user_id, item_id)
        answer_callback(cb_id, msg2, show_alert=not ok)
        return

    if data == "m:shop_buy":
        send_clean_ui(chat_id, user_id, "Выбери товар в витрине и нажми “Купить”.", topic_id=topic_id, ttl_seconds=60)
        answer_callback(cb_id, "Ок", show_alert=False)
        return

    if data == "m:joke":
        jokes = [
            "Писатель хотел вдохновения. Нашел дедлайн.",
            "Очередь спасает нервы. Особенно чужие.",
            "Фидбек уровня “норм” это как чай без чая. Вроде что-то, но нет.",
        ]
        send_clean_ui(chat_id, user_id, "😄 " + jokes[int(time.time()) % len(jokes)], topic_id=topic_id, ttl_seconds=60)
        answer_callback(cb_id, "Ок", show_alert=False)
        return

    if data == "m:truth":
        send_clean_ui(chat_id, user_id, "🤥 “Правда или выдумка” запускается по расписанию. Скоро будет отдельная кнопка “старт по запросу”.", topic_id=topic_id, ttl_seconds=80)
        answer_callback(cb_id, "Ок", show_alert=False)
        return

    if data == "m:wheel":
        send_clean_ui(chat_id, user_id, "🎡 “Колесо тем” запускается по расписанию. Будет и ручной запуск.", topic_id=topic_id, ttl_seconds=80)
        answer_callback(cb_id, "Ок", show_alert=False)
        return

    if data == "m:games_results":
        update_games_pin()
        answer_callback(cb_id, "Обновил закреп с результатами", show_alert=False)
        return

    answer_callback(cb_id, "Кнопка нажата. Толку пока мало, но это временно 🙂", show_alert=False)

# =========================
# DUELS (абзацы) - минимально, как у тебя было
# =========================

def start_paragraph_duel(initiator_id: int, topic=None):
    initiator_id = int(initiator_id)
    if not topic:
        topics = [
            "Утро после конца света",
            "Разговор с зеркалом",
            "Письмо из прошлого",
            "Тайна старой библиотеки",
            "Последний день лета"
        ]
        topic = topics[int(time.time()) % len(topics)]

    duel_id = f"duel_{len(duels)}_{int(time.time())}"
    duel = {
        "id": duel_id,
        "topic": topic,
        "initiator": initiator_id,
        "participants": [initiator_id],
        "paragraphs": {},
        "status": "waiting",
        "created_at": datetime.now().isoformat(),
        "votes": {},
        "winner": None,
        "prize": 25,
        "message_id": None,
    }
    duels.append(duel)

    text = (
        "⚔️ <b>Дуэль абзацев</b>\n\n"
        f"Тема: <b>{html_escape(topic)}</b>\n"
        f"Инициатор: {html_escape(safe_username(initiator_id))}\n"
        "Правила:\n"
        "- 3-5 предложений\n"
        "- 15 минут на сдачу\n"
        "- потом голосование\n\n"
        "Чтобы участвовать, ответь на это сообщение своим абзацем."
    )

    res = send_telegram_message(GROUP_ID, text, topic_id=GROUP_TOPICS["duels"])
    if res and res.get("ok") and res.get("result", {}).get("message_id"):
        duel["message_id"] = res["result"]["message_id"]

    # таймер завершения приема
    threading.Timer(900, finish_duel, args=[duel_id]).start()
    return duel_id

def finish_duel(duel_id: str):
    duel = next((d for d in duels if d["id"] == duel_id), None)
    if not duel or duel["status"] != "waiting":
        return
    duel["status"] = "voting"

    if len(duel["paragraphs"]) < 2:
        duel["status"] = "cancelled"
        send_telegram_message(GROUP_ID, "⚔️ Дуэль отменена: мало участников. Это не позор, это статистика.", topic_id=GROUP_TOPICS["duels"])
        return

    # простое голосование: ответ числом
    text = (
        "🗳 <b>Голосование в дуэли</b>\n\n"
        f"Тема: {html_escape(duel['topic'])}\n\n"
    )
    participants = list(duel["paragraphs"].items())
    for i, (uid, para) in enumerate(participants, 1):
        text += f"\n<b>#{i}</b> ({html_escape(safe_username(uid))}):\n{html_escape(para[:220])}...\n"

    text += "\nОтветь числом (1, 2, 3...). Время: 10 минут."
    send_telegram_message(GROUP_ID, text, topic_id=GROUP_TOPICS["duels"])
    threading.Timer(600, count_duel_votes, args=[duel_id]).start()

def count_duel_votes(duel_id: str):
    duel = next((d for d in duels if d["id"] == duel_id), None)
    if not duel or duel["status"] != "voting":
        return

    votes_count = defaultdict(int)
    for _, vote in duel.get("votes", {}).items():
        votes_count[int(vote)] += 1

    if not votes_count:
        duel["status"] = "finished"
        send_telegram_message(GROUP_ID, "🗳 Голосов нет. Дуэль ушла в небытие.", topic_id=GROUP_TOPICS["duels"])
        return

    winner_index = max(votes_count.items(), key=lambda x: x[1])[0]
    participants = list(duel["paragraphs"].keys())
    if not (1 <= winner_index <= len(participants)):
        duel["status"] = "finished"
        return

    winner_id = participants[winner_index - 1]
    duel["winner"] = winner_id
    duel["status"] = "finished"
    add_quotes(winner_id, duel["prize"], "Победа в дуэли")
    if winner_id in users:
        users[winner_id]["duels_won"] = users[winner_id].get("duels_won", 0) + 1

    send_telegram_message(
        GROUP_ID,
        f"🏆 <b>Дуэль завершена</b>\n\nПобедитель: {html_escape(safe_username(winner_id))}\nПриз: {duel['prize']} 🪙",
        topic_id=GROUP_TOPICS["duels"]
    )

# =========================
# MESSAGE HANDLER
# =========================

def handle_text_button(chat_id, user_id, text, thread_id=None):
    # Обработка reply-клавиатуры в личке
    t = (text or "").strip()
    if t == "📜 Правила":
        show_rules(user_id)
        return True
    if t == "📋 Очередь":
        show_queue(user_id)
        return True
    if t == "👤 Профиль":
        show_profile(user_id)
        return True
    if t == "💰 Баланс":
        send_telegram_message(user_id, f"Баланс: {user_balances.get(int(user_id),0)} 🪙")
        return True
    if t == "🎮 Игры":
        send_telegram_message(user_id, "Игры проходят в группе в теме “Игры дня”.")
        return True
    if t == "🛒 Магазин":
        send_telegram_message(user_id, shop_list_text(), reply_markup=shop_list_keyboard())
        return True
    if t == "✍️ Подать статью":
        start_article_submission(user_id)
        return True
    if t == "🏆 Топ":
        show_top(user_id)
        return True
    return False

def process_message(message: dict):
    chat_id = message["chat"]["id"]
    user_id = int(message["from"]["id"])
    text = message.get("text", "") or ""
    thread_id = message.get("message_thread_id")

    # activity
    if user_id in users:
        users[user_id]["last_active"] = datetime.now().isoformat()

    # Ответ на сообщение (для дуэлей/игр) - тут минимально: дуэль абзацев
    if "reply_to_message" in message:
        reply_to = message["reply_to_message"]
        # дуэль: ответ на стартовое сообщение
        for duel in duels:
            if duel.get("message_id") == reply_to.get("message_id") and duel.get("status") == "waiting":
                duel["participants"] = list(set(duel.get("participants", []) + [user_id]))
                duel["paragraphs"][user_id] = text
                send_telegram_message(user_id, "✅ Абзац принят. Жди голосование.", disable_web_page_preview=True)
                # можно чистить в теме лишнее: оставим, потому что это контент дуэли
                return

        # голосование в дуэли: если человек ответил числом в теме дуэлей
        if thread_id == GROUP_TOPICS["duels"]:
            m = re.match(r"^\s*(\d{1,2})\s*$", text)
            if m:
                vote = int(m.group(1))
                # найдём последнюю дуэль в статусе voting
                active = next((d for d in reversed(duels) if d.get("status") == "voting"), None)
                if active:
                    active.setdefault("votes", {})[user_id] = vote
                    # не шумим в чате: подтвердим в личке
                    send_telegram_message(user_id, "🗳 Голос принят. Спасибо за честность. Или хотя бы за попытку.")
                return

    # Если пользователь в личке и в состоянии подачи статьи
    if chat_id == user_id:
        st = user_states.get(user_id, {})
        if st.get("state") == "await_article" and not text.startswith("/"):
            title, desc, link = parse_submission_text(text)
            if not title or not desc or not link:
                send_telegram_message(user_id, "Не вижу все три блока: ЗАГОЛОВОК, ОПИСАНИЕ, ССЫЛКА. Попробуй еще раз.")
                return
            if not is_allowed_article_url(link):
                send_telegram_message(user_id, f"Ссылка должна быть на {ALLOWED_PLATFORMS_TEXT} и начинаться с https://")
                return
            ok, msg2 = can_submit_article(user_id)
            if not ok:
                send_telegram_message(user_id, msg2)
                return
            aid = add_article_to_queue(user_id, title, desc, link)
            user_states.pop(user_id, None)
            send_telegram_message(user_id, f"✅ Принято в очередь: <b>{html_escape(title)}</b>\nID: {aid}\n\nЖди лист чтения в 19:00 МСК 🙂")
            return

        # Reply keyboard buttons
        if handle_text_button(chat_id, user_id, text):
            return

    # Команды
    if text.startswith("/"):
        command = normalize_command(text)

        # авто-регистрация: /start в любом месте
        if command == "/start":
            user_data = {
                "id": user_id,
                "username": message["from"].get("username"),
                "first_name": message["from"].get("first_name", ""),
                "last_name": message["from"].get("last_name", "")
            }
            register_user(user_data)

            # в группе скажем коротко, а подробности уйдут в личку
            if is_group_chat(chat_id):
                send_telegram_message(chat_id, "✅ Ок. Я написал тебе в личку. Проверь сообщения с ботом.", topic_id=thread_id)
            else:
                send_telegram_message(chat_id, "✅ Ты зарегистрирован. Пользуйся кнопками снизу или /help.")
            return

        if command == "/help":
            show_help(chat_id)
            return

        # остальные команды требуют регистрации
        if not is_user_registered(user_id) and command not in ("/start", "/help"):
            send_telegram_message(chat_id, "Сначала зарегистрируйся: /start (лучше в личке с ботом).", topic_id=thread_id)
            return

        if command == "/rules":
            show_rules(chat_id)
            return

        if command == "/queue":
            show_queue(chat_id, topic_id=thread_id)
            return

        if command == "/top":
            show_top(chat_id, topic_id=thread_id)
            return

        if command == "/profile":
            # в группе лучше чистым UI
            show_profile(user_id, chat_id=chat_id, topic_id=thread_id, as_clean_ui=is_group_chat(chat_id))
            return

        if command == "/balance":
            # в личке сообщением, в группе пусть будет в личку
            if chat_id == user_id:
                send_telegram_message(user_id, f"Баланс: {user_balances.get(user_id,0)} 🪙")
            else:
                send_telegram_message(user_id, f"Баланс: {user_balances.get(user_id,0)} 🪙")
                send_telegram_message(chat_id, "💰 Баланс отправил в личку.", topic_id=thread_id)
            return

        if command == "/daily":
            give_daily_reward(user_id)
            return

        if command == "/submit":
            if chat_id != user_id:
                send_telegram_message(chat_id, "✍️ Подача статьи только в личке с ботом.", topic_id=thread_id)
            else:
                start_article_submission(user_id)
            return

        if command == "/publish_reading_list" and user_id in ADMIN_IDS:
            res = publish_daily_reading_list()
            send_telegram_message(user_id, res)
            return

        if command == "/pin_menus" and user_id in ADMIN_IDS:
            ensure_all_topic_menus()
            send_telegram_message(user_id, "✅ Меню в темах обновлены и закреплены.")
            return

        send_telegram_message(chat_id, "Неизвестная команда. /help", topic_id=thread_id)
        return

    # Не команды
    if chat_id == user_id:
        send_telegram_message(user_id, "Напиши /help или жми кнопки снизу.")
        return

# =========================
# WEBHOOK
# =========================

@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        data = request.get_json(force=True, silent=True) or {}
        logger.info("📨 Webhook keys: %s", list(data.keys()))

        if "message" in data:
            process_message(data["message"])
        elif "callback_query" in data:
            handle_callback(data["callback_query"])

        return jsonify({"status": "ok"}), 200
    except Exception as e:
        logger.error("Webhook error: %s", e, exc_info=True)
        return jsonify({"error": str(e)}), 500

@app.route("/health")
def health():
    return jsonify({
        "status": "healthy",
        "ts": datetime.now().isoformat(),
        "users": len(users),
        "queue": len(articles_queue),
        "published_today": len(published_articles),
        "total_quotes": sum(int(v) for v in user_balances.values()),
    }), 200

@app.route("/")
def home():
    return "OK", 200

# =========================
# АВТОЗАДАЧИ
# =========================

def schedule_submit_notifications(interval_seconds=60):
    def loop():
        while True:
            time.sleep(interval_seconds)
            now = datetime.now()
            for uid, last in list(user_last_submit.items()):
                if not isinstance(last, datetime):
                    continue
                ready_at = last + timedelta(hours=48)
                if now >= ready_at:
                    last_not = user_submit_notified.get(uid, "") or ""
                    # уведомляем один раз на каждую подачу
                    if not last_not:
                        send_telegram_message(uid, "🔔 Можно снова подать статью. /submit в личке 🙂")
                        user_submit_notified[uid] = datetime.now().isoformat()
    t = threading.Thread(target=loop, daemon=True)
    t.start()

def schedule_daily_tasks():
    def loop():
        # Раз в час обновим закрепленные меню, чтобы не терялись после переездов/удалений
        last_menu_refresh = 0
        while True:
            now = datetime.now()

            if time.time() - last_menu_refresh > 3600:
                ensure_all_topic_menus()
                last_menu_refresh = time.time()

            # 19:00 МСК лист чтения: тут без таймзоны, используй время сервера.
            # На Render обычно UTC. Если хочешь строго МСК, лучше перевести через pytz.
            # Пока оставим как есть, потому что стабильность важнее мечты.
            if now.hour == 19 and now.minute == 0:
                publish_daily_reading_list()

            time.sleep(60)
    t = threading.Thread(target=loop, daemon=True)
    t.start()

# =========================
# INIT (важно для gunicorn)
# =========================

def init():
    load_data()
    schedule_data_saves()
    schedule_submit_notifications()
    schedule_daily_tasks()
    ensure_all_topic_menus()
    atexit.register(save_data)
    logger.info("🚀 Init done. Users=%d Queue=%d", len(users), len(articles_queue))

init()

import os
import json
import logging
import random
import threading
import time
import re
import atexit
from datetime import datetime
from collections import defaultdict, deque

import requests
from flask import Flask, request, jsonify

# =========================
# НАСТРОЙКИ
# =========================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("bot.log")]
)
logger = logging.getLogger("app")

app = Flask(__name__)

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "").strip()
if not TELEGRAM_TOKEN:
    raise RuntimeError("TELEGRAM_TOKEN is required")

# ВАЖНО:
# Лучше использовать числовой chat_id группы (отрицательный), чем @username.
# Но можно и @username, если группа публичная.
GROUP_ID = os.environ.get("GROUP_ID", "@uvlekatelnyechteniya").strip()

ADMIN_IDS = set(int(x) for x in os.environ.get("ADMIN_IDS", "1039651708").split(",") if x.strip().isdigit())

DATA_FILE = os.environ.get("BOT_DATA_FILE", "data.json")
DATA_LOCK = threading.Lock()

# =========================
# ДАННЫЕ (ПАМЯТЬ + JSON)
# =========================

users = {}  # user_id -> dict
articles_queue = deque(maxlen=10)
published_articles = []
user_articles = defaultdict(list)
user_balances = defaultdict(int)
user_last_submit = {}  # user_id -> datetime
user_daily_reward = {}  # user_id -> "YYYY-MM-DD"
games_history = []
duels = []
games_results = []
games_pin_message_id = None

# =========================
# TELEGRAM API HELPERS
# =========================

def tg_request(method: str, payload: dict, timeout: int = 10):
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

def send_telegram_message(
    chat_id,
    text,
    topic_id=None,
    reply_to_message_id=None,
    parse_mode="HTML",
    reply_markup=None,
    disable_web_page_preview=True
):
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": parse_mode,
        "disable_web_page_preview": disable_web_page_preview
    }
    if topic_id:
        payload["message_thread_id"] = topic_id
    if reply_to_message_id:
        payload["reply_to_message_id"] = reply_to_message_id
    if reply_markup:
        payload["reply_markup"] = reply_markup

    return tg_request("sendMessage", payload)

def delete_telegram_message(chat_id, message_id):
    return tg_request("deleteMessage", {"chat_id": chat_id, "message_id": message_id})

def schedule_message_deletion(chat_id, message_id, delay_seconds):
    t = threading.Timer(delay_seconds, delete_telegram_message, args=[chat_id, message_id])
    t.daemon = True
    t.start()

def send_temporary_message(chat_id, text, delete_after_seconds, topic_id=None, reply_markup=None):
    result = send_telegram_message(chat_id, text, topic_id=topic_id, reply_markup=reply_markup)
    if result and "result" in result:
        schedule_message_deletion(chat_id, result["result"]["message_id"], delete_after_seconds)
    return result

# =========================
# KEYBOARDS (ReplyKeyboardMarkup)
# =========================

def kb_private_persistent():
    # Постоянная клавиатура для ЛС
    return {
        "keyboard": [
            [{"text": "📜 Правила"}, {"text": "📋 Очередь"}],
            [{"text": "👤 Профиль"}, {"text": "💰 Баланс"}],
            [{"text": "🎁 Награда"}, {"text": "🎮 Игры"}],
            [{"text": "✍️ Подать статью"}, {"text": "ℹ️ Помощь"}],
        ],
        "resize_keyboard": True,
        "one_time_keyboard": False,
        "is_persistent": True,
        "input_field_placeholder": "Выберите действие кнопкой снизу"
    }

def kb_group_root():
    # В группе постоянно одна кнопка
    return {
        "keyboard": [
            [{"text": "🧭 Меню"}]
        ],
        "resize_keyboard": True,
        "one_time_keyboard": False,
        "is_persistent": True,
        "input_field_placeholder": "Нажмите Меню"
    }

def kb_group_popup():
    # Всплывающее меню на один раз
    return {
        "keyboard": [
            [{"text": "📜 Правила"}, {"text": "📋 Очередь"}],
            [{"text": "🏆 Топ"}, {"text": "🎮 Игры"}],
            [{"text": "ℹ️ Помощь"}, {"text": "🧭 Закрыть меню"}],
        ],
        "resize_keyboard": True,
        "one_time_keyboard": True,
        "is_persistent": False
    }

# =========================
# НОРМАЛИЗАЦИЯ КОМАНД
# =========================

TEXT_TO_COMMAND = {
    "📜 Правила": "/rules",
    "📋 Очередь": "/queue",
    "👤 Профиль": "/profile",
    "💰 Баланс": "/balance",
    "🎁 Награда": "/daily",
    "🎮 Игры": "/game",
    "✍️ Подать статью": "/submit",
    "ℹ️ Помощь": "/help",
    "🧭 Меню": "/menu",
    "🧭 Закрыть меню": "/close_menu",
}

def normalize_command(text: str) -> str:
    cmd = (text or "").split()[0].strip().lower()
    if "@" in cmd:
        cmd = cmd.split("@", 1)[0]
    return cmd

# =========================
# ПЕРСИСТЕНТНОСТЬ
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
            "user_daily_reward": dict(user_daily_reward),
            "games_history": games_history,
            "duels": duels,
            "games_results": games_results,
            "games_pin_message_id": games_pin_message_id
        }
        try:
            with open(DATA_FILE, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error("save_data error: %s", e)

def load_data():
    global users, articles_queue, published_articles, user_articles
    global user_balances, user_last_submit, user_daily_reward
    global games_history, duels, games_results, games_pin_message_id

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
            int(k): datetime.fromisoformat(v)
            for k, v in data.get("user_last_submit", {}).items()
        }
        user_daily_reward = defaultdict(str, data.get("user_daily_reward", {}))
        games_history = data.get("games_history", [])
        duels = data.get("duels", [])
        games_results = data.get("games_results", [])
        games_pin_message_id = data.get("games_pin_message_id")

        logger.info("Data loaded from %s", DATA_FILE)
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
# ЛОГИКА ПОЛЬЗОВАТЕЛЕЙ
# =========================

def is_user_registered(user_id: int) -> bool:
    return user_id in users

def register_user(user_data: dict):
    user_id = int(user_data["id"])
    if user_id in users:
        return

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
        "total_quotes": 0,
        "badges": ["новичок"],
        "subscription": "free",
        "last_active": datetime.now().isoformat()
    }
    user_balances[user_id] = max(user_balances.get(user_id, 0), 50)

    welcome_text = (
        '<b>Добро пожаловать в клуб "Увлекательные чтения"</b>\n\n'
        f"<b>Профиль:</b>\n"
        f"Имя: {users[user_id]['first_name']} {users[user_id]['last_name']}\n"
        f"Юзернейм: @{users[user_id]['username'] if users[user_id]['username'] else 'нет'}\n"
        f"ID: {user_id}\n\n"
        "<b>Стартовый бонус:</b> 50 кавычек\n\n"
        "Кнопки снизу это главное меню."
    )

    # В ЛС можно не иметь права писать, если человек не нажимал Start в боте.
    # Поэтому просто пытаемся, а если не выйдет, не валим весь процесс.
    try:
        send_telegram_message(user_id, welcome_text, reply_markup=kb_private_persistent())
    except Exception:
        pass

    logger.info("Registered user %s", user_id)

# =========================
# МЕНЮ И КОМАНДЫ
# =========================

def show_help(chat_id, topic_id=None, is_private=False):
    text = (
        "<b>Помощь</b>\n\n"
        "/start - регистрация\n"
        "/help - помощь\n"
        "/rules - правила\n"
        "/queue - очередь\n"
        "/profile - профиль\n"
        "/balance - баланс\n"
        "/daily - ежедневная награда\n"
        "/game - игры\n"
        "/submit - подать статью (только ЛС)\n"
        "/menu - меню клавиатуры\n"
    )
    reply_markup = kb_private_persistent() if is_private else kb_group_root()
    send_telegram_message(chat_id, text, topic_id=topic_id, reply_markup=reply_markup)

def show_rules(chat_id, topic_id=None, is_private=False):
    text = (
        '<b>Правила клуба "Увлекательные чтения"</b>\n\n'
        "1) Подача статьи: 1 раз в 48-72 часа, максимум 1 активная в очереди\n"
        "2) Фидбек: минимум 1 нормальный фидбек в день, пустые комментарии удаляются\n"
        "3) Уважение: без оскорблений и токсика\n"
        "4) Кавычки: начисляются за активность, тратятся в магазине\n"
        "5) Спам ссылок и накрутки не нужны\n"
    )
    reply_markup = kb_private_persistent() if is_private else kb_group_root()
    send_telegram_message(chat_id, text, topic_id=topic_id, reply_markup=reply_markup)

def show_queue(chat_id, topic_id=None, is_private=False):
    if not articles_queue:
        reply_markup = kb_private_persistent() if is_private else kb_group_root()
        send_telegram_message(chat_id, "Очередь пуста", topic_id=topic_id, reply_markup=reply_markup)
        return

    lines = ["<b>Очередь публикаций</b>\n"]
    for i, art in enumerate(list(articles_queue)[:10], 1):
        u = users.get(int(art.get("user_id", 0)), {})
        uname = f"@{u.get('username')}" if u.get("username") else "пользователь"
        lines.append(f"{i}. <b>{art.get('title','без названия')}</b> (автор: {uname})")

    reply_markup = kb_private_persistent() if is_private else kb_group_root()
    send_telegram_message(chat_id, "\n".join(lines), topic_id=topic_id, reply_markup=reply_markup)

def show_profile(user_id, topic_id=None, is_private=True):
    if user_id not in users:
        send_telegram_message(user_id, "Сначала зарегистрируйтесь: /start", reply_markup=kb_private_persistent())
        return
    u = users[user_id]
    bal = user_balances.get(user_id, 0)
    text = (
        "<b>Профиль</b>\n\n"
        f"Имя: {u['first_name']} {u['last_name']}\n"
        f"Юзернейм: @{u['username'] if u['username'] else 'нет'}\n"
        f"Статей: {u['articles_count']}\n"
        f"Фидбеков дано: {u['feedback_given']}\n"
        f"Фидбеков получено: {u['feedback_received']}\n"
        f"Баланс: {bal} кавычек\n"
    )
    send_telegram_message(user_id, text, topic_id=topic_id, reply_markup=kb_private_persistent())

def show_balance(user_id, topic_id=None, is_private=True):
    bal = user_balances.get(user_id, 0)
    text = f"<b>Баланс</b>\n\n{bal} кавычек"
    reply_markup = kb_private_persistent() if is_private else kb_group_root()
    send_telegram_message(user_id if is_private else GROUP_ID, text, topic_id=topic_id, reply_markup=reply_markup)

def give_daily_reward(user_id, topic_id=None, is_private=True):
    today = datetime.now().date().isoformat()
    if user_daily_reward.get(user_id) == today:
        reply_markup = kb_private_persistent() if is_private else kb_group_root()
        send_telegram_message(user_id if is_private else GROUP_ID, "Награда уже получена сегодня", topic_id=topic_id, reply_markup=reply_markup)
        return

    reward = 5
    user_balances[user_id] += reward
    users[user_id]["total_quotes"] = users[user_id].get("total_quotes", 0) + reward
    user_daily_reward[user_id] = today

    reply_markup = kb_private_persistent() if is_private else kb_group_root()
    send_telegram_message(user_id if is_private else GROUP_ID, f"Начислено {reward} кавычек", topic_id=topic_id, reply_markup=reply_markup)

def show_games_menu(chat_id, topic_id=None, is_private=False):
    text = (
        "<b>Игры и активности</b>\n\n"
        "Дуэль абзацев: /duel (в группе)\n"
        "Остальные игры можно добавить позже, когда люди начнут реально писать, а не просто жать кнопки.\n"
    )
    reply_markup = kb_private_persistent() if is_private else kb_group_root()
    send_telegram_message(chat_id, text, topic_id=topic_id, reply_markup=reply_markup)

def start_article_submission(user_id):
    text = (
        "<b>Подача статьи</b>\n\n"
        "Отправьте одним сообщением:\n"
        "ЗАГОЛОВОК: ...\n"
        "ОПИСАНИЕ: ...\n"
        "ССЫЛКА: ...\n"
    )
    send_telegram_message(user_id, text, reply_markup=kb_private_persistent())

# =========================
# WEBHOOK
# =========================

@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        data = request.get_json(force=True)
        logger.info("Webhook keys: %s", list(data.keys()))

        msg = data.get("message") or data.get("edited_message")
        if msg:
            process_message(msg)
            return jsonify({"status": "ok"}), 200

        cb = data.get("callback_query")
        if cb:
            # можно допилить inline callbacks, если надо
            return jsonify({"status": "ok"}), 200

        return jsonify({"status": "ignored"}), 200
    except Exception as e:
        logger.error("Webhook error: %s", e, exc_info=True)
        return jsonify({"error": str(e)}), 500

def process_message(message: dict):
    chat = message.get("chat", {})
    from_user = message.get("from", {})
    chat_id = chat.get("id")
    user_id = from_user.get("id")
    text = message.get("text", "") or ""
    topic_id = message.get("message_thread_id")

    if not chat_id or not user_id:
        return

    is_private = (chat.get("type") == "private")

    # обновляем активность
    if user_id in users:
        users[user_id]["last_active"] = datetime.now().isoformat()

    # Маппинг кнопок reply keyboard в команды
    if not text.startswith("/") and text in TEXT_TO_COMMAND:
        text = TEXT_TO_COMMAND[text]

    if text.startswith("/"):
        process_command(chat_id, user_id, text, topic_id, is_private, message)
        return

    # В группе обычные сообщения можно игнорить (если privacy mode выключен, иначе будет поток текста)
    if is_private:
        send_telegram_message(chat_id, "Напишите /help или используйте кнопки снизу", reply_markup=kb_private_persistent())
    else:
        # в группе не спамим
        pass

def process_command(chat_id, user_id, text, topic_id, is_private, message):
    command = normalize_command(text)

    # /start и /help разрешены без регистрации
    if not is_user_registered(user_id) and command not in ["/start", "/help", "/menu"]:
        if is_private:
            send_telegram_message(chat_id, "Сначала зарегистрируйтесь: /start", reply_markup=kb_private_persistent())
        else:
            send_telegram_message(chat_id, "Сначала зарегистрируйтесь командой /start (лучше в ЛС)", topic_id=topic_id, reply_markup=kb_group_root())
        return

    # /start
    if command == "/start":
        # регистрируем
        if not is_user_registered(user_id):
            ud = {
                "id": user_id,
                "username": message.get("from", {}).get("username"),
                "first_name": message.get("from", {}).get("first_name", ""),
                "last_name": message.get("from", {}).get("last_name", "")
            }
            register_user(ud)

        if is_private:
            send_telegram_message(chat_id, "Готово. Используйте кнопки снизу.", reply_markup=kb_private_persistent())
        else:
            send_telegram_message(chat_id, "Готово. Для полного меню лучше писать боту в ЛС.", topic_id=topic_id, reply_markup=kb_group_root())
        return

    # меню для reply keyboard
    if command == "/menu":
        if is_private:
            send_telegram_message(chat_id, "Меню включено", reply_markup=kb_private_persistent())
        else:
            send_telegram_message(chat_id, "Откройте меню кнопкой снизу", topic_id=topic_id, reply_markup=kb_group_root())
        return

    if command == "/close_menu":
        # возвращаем одну кнопку в группе
        if is_private:
            send_telegram_message(chat_id, "Меню", reply_markup=kb_private_persistent())
        else:
            send_telegram_message(chat_id, "Меню закрыто", topic_id=topic_id, reply_markup=kb_group_root())
        return

    # В группе: по нажатию "Меню" показываем всплывающее меню
    if not is_private and command == "/menu_open":
        send_telegram_message(chat_id, "Выберите действие", topic_id=topic_id, reply_markup=kb_group_popup())
        return

    # /help
    if command == "/help":
        show_help(chat_id, topic_id=topic_id, is_private=is_private)
        return

    if command == "/rules":
        show_rules(chat_id, topic_id=topic_id, is_private=is_private)
        return

    if command == "/queue":
        show_queue(chat_id, topic_id=topic_id, is_private=is_private)
        return

    if command == "/profile":
        if is_private:
            show_profile(user_id, is_private=True)
        else:
            send_telegram_message(chat_id, "Профиль показываю в ЛС. Напишите боту.", topic_id=topic_id, reply_markup=kb_group_root())
        return

    if command == "/balance":
        if is_private:
            show_balance(user_id, is_private=True)
        else:
            send_telegram_message(chat_id, "Баланс показываю в ЛС. Напишите боту.", topic_id=topic_id, reply_markup=kb_group_root())
        return

    if command == "/daily":
        if is_private:
            give_daily_reward(user_id, is_private=True)
        else:
            send_telegram_message(chat_id, "Награда выдаётся в ЛС. Напишите боту.", topic_id=topic_id, reply_markup=kb_group_root())
        return

    if command == "/game":
        show_games_menu(chat_id if not is_private else user_id, topic_id=topic_id, is_private=is_private)
        return

    if command == "/submit":
        if is_private:
            start_article_submission(user_id)
        else:
            send_telegram_message(chat_id, "Подача статьи только в ЛС с ботом", topic_id=topic_id, reply_markup=kb_group_root())
        return

    # спец логика: кнопка "🧭 Меню" в группе
    if not is_private and TEXT_TO_COMMAND.get("🧭 Меню") == "/menu":
        # если команда пришла как /menu уже обработана выше
        pass

    # неизвестно
    if is_private:
        send_telegram_message(chat_id, "Неизвестная команда. Используйте /help", reply_markup=kb_private_persistent())
    else:
        send_telegram_message(chat_id, "Неизвестная команда. Используйте /help", topic_id=topic_id, reply_markup=kb_group_root())

# Хитрость: кнопка "🧭 Меню" у нас отправляет "/menu" через TEXT_TO_COMMAND
# Но чтобы она именно показывала popup меню, мы ловим её отдельной веткой в process_message:
# если в группе пришёл текст "🧭 Меню", покажем popup, иначе в ЛС просто покажем обычное.
def _patch_menu_open():
    original = process_message

    def wrapped(message: dict):
        chat = message.get("chat", {})
        chat_type = chat.get("type")
        text = (message.get("text", "") or "").strip()
        topic_id = message.get("message_thread_id")
        chat_id = chat.get("id")

        if chat_type != "private" and text == "🧭 Меню":
            send_telegram_message(chat_id, "Выберите действие", topic_id=topic_id, reply_markup=kb_group_popup())
            return

        if chat_type != "private" and text == "🧭 Закрыть меню":
            send_telegram_message(chat_id, "Меню закрыто", topic_id=topic_id, reply_markup=kb_group_root())
            return

        return original(message)

    return wrapped

process_message = _patch_menu_open()

# =========================
# HEALTH + WEBHOOK SETTER
# =========================

@app.route("/health")
def health():
    return jsonify({
        "status": "healthy",
        "ts": datetime.now().isoformat(),
        "users": len(users),
        "queue": len(articles_queue),
        "published_today": len(published_articles),
    }), 200

@app.route("/set_webhook", methods=["GET"])
def set_webhook():
    url = request.args.get("url", "").strip()
    if not url:
        return (
            "<h3>Set webhook</h3>"
            "<p>Use: /set_webhook?url=https://YOURDOMAIN/webhook</p>"
        ), 200

    resp = tg_request("setWebhook", {"url": url})
    return jsonify(resp or {"ok": False}), 200

# =========================
# BOOTSTRAP
# =========================

_BOOTSTRAPPED = False

def bootstrap_once():
    global _BOOTSTRAPPED
    if _BOOTSTRAPPED:
        return
    _BOOTSTRAPPED = True
    load_data()
    schedule_data_saves()
    atexit.register(save_data)
    logger.info("Bootstrapped. users=%s queue=%s", len(users), len(articles_queue))

bootstrap_once()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5000"))
    app.run(host="0.0.0.0", port=port)

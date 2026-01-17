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

# --------------------
# ЛОГИ
# --------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("club-bot")

app = Flask(__name__)

# --------------------
# КОНФИГ
# --------------------
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "").strip()
if not TELEGRAM_TOKEN:
    raise RuntimeError("TELEGRAM_TOKEN не задан в переменных окружения")

# Лучше хранить числовой id супергруппы: -100xxxxxxxxxx
# Можно оставить @username, но тогда часть проверок по чату будет менее надежной.
GROUP_ID_RAW = os.environ.get("GROUP_ID", "@uvlekatelnyechteniya").strip()
try:
    GROUP_ID = int(GROUP_ID_RAW)
except ValueError:
    GROUP_ID = GROUP_ID_RAW  # @username

ADMIN_IDS_RAW = os.environ.get("ADMIN_IDS", "1039651708").strip()
ADMIN_IDS = []
for part in ADMIN_IDS_RAW.split(","):
    part = part.strip()
    if part:
        try:
            ADMIN_IDS.append(int(part))
        except ValueError:
            pass

DATA_FILE = os.environ.get("BOT_DATA_FILE", "data.json")

# ТЕМЫ ФОРУМА (message_thread_id)
# Заменяй id на свои реальные, если отличаются.
GROUP_TOPICS = {
    "announcements": 1,   # Объявления
    "rules": 2,           # Правила
    "queue": 3,           # Очередь
    "reading_list": 4,    # Лист чтения
    "feedback": 5,        # Фидбек
    "duels": 6,           # Дуэли
    "games": 7,           # Игры дня
    "shop": 8,            # Магазин
    "offtop": 9,          # Оффтоп
}

# ПРАВИЛА ПЛАТФОРМ ДЛЯ ССЫЛОК
ALLOWED_HOSTS = {
    "vk.com", "m.vk.com",
    "dzen.ru", "m.dzen.ru", "zen.yandex.ru",
    "t.me", "telegra.ph", "telegram.me"
}

# --------------------
# ДАННЫЕ (память + файл)
# --------------------
DATA_LOCK = threading.Lock()

users = {}  # user_id -> dict
articles_queue = deque(maxlen=10)  # очередь статей
published_articles = []  # опубликованные сегодня
user_articles = defaultdict(list)  # user_id -> список статей
user_balances = defaultdict(int)  # user_id -> кавычки
user_last_submit = {}  # user_id -> datetime
user_daily_reward = {}  # user_id -> "YYYY-MM-DD"
games_history = []
duels = []
games_results = []

# Меню-пины по темам: topic_key -> message_id
topic_menu_message_ids = {}

# Чтобы не засорять чат: последнее сообщение бота для (user_id, topic_id)
last_bot_reply = {}  # (user_id, topic_id) -> message_id

# Стартовые настройки
START_BONUS_QUOTES = 50
SUBMIT_REWARD_QUOTES = 10
DAILY_REWARD_QUOTES = 5

MIN_SUBMIT_HOURS = 48

# --------------------
# TELEGRAM API
# --------------------
def tg(method: str, payload: dict, timeout: int = 15):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/{method}"
    r = requests.post(url, json=payload, timeout=timeout)
    r.raise_for_status()
    data = r.json()
    if not data.get("ok"):
        raise RuntimeError(f"Telegram API error: {data}")
    return data

def send_message(chat_id, text, topic_id=None, reply_to_message_id=None, reply_markup=None, parse_mode="HTML"):
    payload = {"chat_id": chat_id, "text": text, "parse_mode": parse_mode}
    if reply_to_message_id:
        payload["reply_to_message_id"] = reply_to_message_id
    if topic_id is not None:
        payload["message_thread_id"] = topic_id  # forum topic
    if reply_markup:
        payload["reply_markup"] = reply_markup
    try:
        return tg("sendMessage", payload)
    except Exception as e:
        logger.error(f"sendMessage failed: {e}")
        return None

def edit_message(chat_id, message_id, text=None, reply_markup=None, parse_mode="HTML"):
    payload = {"chat_id": chat_id, "message_id": message_id}
    if text is not None:
        payload["text"] = text
        payload["parse_mode"] = parse_mode
    if reply_markup is not None:
        payload["reply_markup"] = reply_markup
    try:
        return tg("editMessageText" if text is not None else "editMessageReplyMarkup", payload)
    except Exception as e:
        logger.warning(f"editMessage failed: {e}")
        return None

def delete_message(chat_id, message_id):
    payload = {"chat_id": chat_id, "message_id": message_id}
    try:
        return tg("deleteMessage", payload)
    except Exception as e:
        logger.warning(f"deleteMessage failed: {e}")
        return None

def pin_message(chat_id, message_id, disable_notification=True):
    payload = {"chat_id": chat_id, "message_id": message_id, "disable_notification": disable_notification}
    try:
        return tg("pinChatMessage", payload)
    except Exception as e:
        logger.warning(f"pinChatMessage failed: {e}")
        return None

def answer_callback(callback_query_id, text=None, show_alert=False):
    payload = {"callback_query_id": callback_query_id}
    if text:
        payload["text"] = text[:190]  # не раздуваем
    payload["show_alert"] = bool(show_alert)
    try:
        return tg("answerCallbackQuery", payload)
    except Exception as e:
        logger.warning(f"answerCallbackQuery failed: {e}")
        return None

def get_bot_username():
    try:
        me = tg("getMe", {})
        return me["result"].get("username")
    except Exception:
        return None

BOT_USERNAME = os.environ.get("BOT_USERNAME", "").strip() or get_bot_username() or "YourBot"

def bot_deeplink():
    return f"https://t.me/{BOT_USERNAME}"

# --------------------
# ПОЛЕЗНОЕ
# --------------------
def now_iso():
    return datetime.now().isoformat()

def sep():
    return "--------------------"

def normalize_command(text: str) -> str:
    """
    /start@MyBot -> /start
    """
    cmd = (text or "").split()[0].strip().lower()
    if "@" in cmd:
        cmd = cmd.split("@", 1)[0]
    return cmd

def is_group_chat(chat_id) -> bool:
    if isinstance(chat_id, int):
        return chat_id < 0
    if isinstance(chat_id, str):
        return chat_id.startswith("@")
    return False

def is_allowed_article_url(url: str) -> bool:
    try:
        u = urlparse(url.strip())
        host = (u.netloc or "").lower()
        if not host:
            return False
        # отрезаем порт
        host = host.split(":")[0]
        return host in ALLOWED_HOSTS
    except Exception:
        return False

def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

# --------------------
# СОХРАНЕНИЕ/ЗАГРУЗКА
# --------------------
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
            "games_history": games_history,
            "duels": duels,
            "games_results": games_results,
            "topic_menu_message_ids": topic_menu_message_ids,
        }
        try:
            with open(DATA_FILE, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"save_data failed: {e}")

def load_data():
    global users, articles_queue, published_articles, user_articles
    global user_balances, user_last_submit, user_daily_reward
    global games_history, duels, games_results, topic_menu_message_ids

    if not os.path.exists(DATA_FILE):
        logger.info("data.json не найден, стартуем с нуля")
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
        user_daily_reward = data.get("user_daily_reward", {})
        games_history = data.get("games_history", [])
        duels = data.get("duels", [])
        games_results = data.get("games_results", [])
        topic_menu_message_ids = data.get("topic_menu_message_ids", {})

        logger.info("Данные загружены из файла")
    except Exception as e:
        logger.error(f"load_data failed: {e}")

def schedule_data_saves(interval_seconds=60):
    def loop():
        while True:
            time.sleep(interval_seconds)
            save_data()
    t = threading.Thread(target=loop, daemon=True)
    t.start()

# --------------------
# РЕГИСТРАЦИЯ/ПРОФИЛЬ
# --------------------
def is_user_registered(user_id: int) -> bool:
    return str(user_id) in users or user_id in users

def get_user(user_id: int) -> dict:
    # совместимость: вдруг ключи строкой
    if user_id in users:
        return users[user_id]
    if str(user_id) in users:
        return users[str(user_id)]
    return None

def set_user(user_id: int, obj: dict):
    # храним строковым ключом, чтобы json был проще
    users[str(user_id)] = obj

def register_user(from_obj: dict) -> bool:
    user_id = int(from_obj["id"])
    if is_user_registered(user_id):
        return False

    profile = {
        "id": user_id,
        "username": from_obj.get("username"),
        "first_name": from_obj.get("first_name", ""),
        "last_name": from_obj.get("last_name", ""),
        "registered_at": now_iso(),
        "last_active": now_iso(),
        "articles_count": 0,
        "feedback_given": 0,
        "feedback_received": 0,
        "games_played": 0,
        "duels_won": 0,
        "total_quotes": 0,
        "badges": ["новичок"],
        "subscription": "free",
    }
    set_user(user_id, profile)
    user_balances[user_id] = max(user_balances.get(user_id, 0), START_BONUS_QUOTES)
    profile["total_quotes"] += START_BONUS_QUOTES
    save_data()

    text = (
        f"🎉 <b>Добро пожаловать в «Увлекательные чтения»</b>\n"
        f"{sep()}\n"
        f"Профиль создан. Да, теперь ты официально в клубе.\n\n"
        f"👤 <b>Профиль</b>\n"
        f"Имя: {profile['first_name']} {profile['last_name']}\n"
        f"Юзернейм: @{profile['username'] if profile['username'] else 'не задан'}\n\n"
        f"🪙 <b>Стартовый бонус</b>: {START_BONUS_QUOTES} кавычек\n\n"
        f"📌 <b>Как жить дальше</b>\n"
        f"• Правила: /rules\n"
        f"• Очередь: /queue\n"
        f"• Баланс: /balance\n"
        f"• Профиль: /profile\n\n"
        f"Ссылки на статьи принимаем только из ВК, Дзен и Телеграм.\n"
        f"Не потому что мы вредные. Просто потому что хаоса и так достаточно."
    )
    send_message(user_id, text, reply_markup=build_private_reply_keyboard())
    logger.info(f"Registered user {user_id}")
    return True

# --------------------
# ВАЛЮТА
# --------------------
def add_quotes(user_id: int, amount: int, reason: str):
    bal = user_balances.get(user_id, 0) + amount
    user_balances[user_id] = bal
    u = get_user(user_id)
    if u:
        u["total_quotes"] = int(u.get("total_quotes", 0)) + amount
        set_user(user_id, u)
    logger.info(f"Quotes +{amount} for {user_id}: {reason}")
    return bal

# --------------------
# ПОДАЧА СТАТЕЙ
# --------------------
def can_submit_article(user_id: int):
    last = user_last_submit.get(user_id)
    if not last:
        return True, "Можно подавать"
    diff = datetime.now() - last
    if diff.total_seconds() < MIN_SUBMIT_HOURS * 3600:
        left = int((MIN_SUBMIT_HOURS * 3600 - diff.total_seconds()) / 3600)
        return False, f"⏳ Рано. Подать можно примерно через {left} ч."
    # только 1 активная статья в очереди
    for a in articles_queue:
        if int(a.get("user_id")) == user_id:
            return False, "📌 У тебя уже есть статья в очереди."
    if len(articles_queue) >= 10:
        return False, "📦 Очередь забита (10/10). Подожди, пока разгребем."
    return True, "Можно подавать"

def add_article_to_queue(user_id: int, title: str, description: str, url: str):
    article_id = f"art_{int(time.time())}_{user_id}"
    art = {
        "id": article_id,
        "user_id": user_id,
        "title": title.strip()[:120],
        "description": description.strip()[:400],
        "url": url.strip(),
        "submitted_at": now_iso(),
        "status": "pending",
        "feedback_count": 0,
        "reads": 0,
        "likes": 0,
    }
    articles_queue.append(art)
    user_articles[user_id].append(art)
    user_last_submit[user_id] = datetime.now()

    u = get_user(user_id)
    if u:
        u["articles_count"] = int(u.get("articles_count", 0)) + 1
        set_user(user_id, u)

    add_quotes(user_id, SUBMIT_REWARD_QUOTES, "Подача статьи")
    save_data()
    return article_id

# --------------------
# ПИН-МЕНЮ ПО ТЕМАМ
# --------------------
def build_topic_menu(topic_key: str):
    """
    Возвращает (text, reply_markup) для закрепа в конкретной теме.
    """
    if topic_key == "queue":
        text = (
            f"📋 <b>Очередь публикаций</b>\n"
            f"{sep()}\n"
            f"Тут нет спама. Тут порядок. Иногда даже справедливость.\n\n"
            f"Ссылки принимаем только из ВК, Дзен и Телеграм.\n"
        )
        kb = {
            "inline_keyboard": [
                [{"text": "Показать очередь", "callback_data": "m:queue"}],
                [{"text": "Подать статью", "callback_data": "m:submit"}],
                [{"text": "Когда можно подать", "callback_data": "m:when"}],
            ]
        }
        return text, kb

    if topic_key == "reading_list":
        text = (
            f"📚 <b>Лист чтения</b>\n"
            f"{sep()}\n"
            f"Здесь появляются подборки на день. Читаем, пишем фидбек, остаемся людьми.\n"
        )
        kb = {
            "inline_keyboard": [
                [{"text": "Показать текущий лист", "callback_data": "m:reading"}],
                [{"text": "Как писать фидбек", "callback_data": "m:feedback_how"}],
            ]
        }
        return text, kb

    if topic_key == "duels":
        text = (
            f"⚔️ <b>Дуэли абзацев</b>\n"
            f"{sep()}\n"
            f"Коротко, честно, без лишнего пафоса.\n"
            f"Чтобы участвовать, надо отвечать текстом на дуэльное сообщение.\n"
        )
        kb = {
            "inline_keyboard": [
                [{"text": "Начать дуэль", "callback_data": "m:duel_start"}],
                [{"text": "Как участвовать", "callback_data": "m:duel_how"}],
            ]
        }
        return text, kb

    if topic_key == "games":
        text = (
            f"🎮 <b>Игры дня</b>\n"
            f"{sep()}\n"
            f"Игры запускаются по расписанию. Победы приносят кавычки, поражения приносят опыт.\n"
        )
        kb = {
            "inline_keyboard": [
                [{"text": "Расписание", "callback_data": "m:games_schedule"}],
                [{"text": "Результаты (закреп)", "callback_data": "m:games_results"}],
            ]
        }
        return text, kb

    if topic_key == "shop":
        text = (
            f"🛒 <b>Магазин кавычек</b>\n"
            f"{sep()}\n"
            f"Трать внутреннюю валюту на приятные штуки. Мир редко дает скидки, мы пытаемся.\n"
        )
        kb = {
            "inline_keyboard": [
                [{"text": "Прайс", "callback_data": "m:shop_price"}],
                [{"text": "Потратить", "callback_data": "m:shop_spend"}],
                [{"text": "Баланс", "callback_data": "m:balance"}],
            ]
        }
        return text, kb

    if topic_key == "rules":
        text = (
            f"📜 <b>Правила</b>\n"
            f"{sep()}\n"
            f"Никакой магии. Просто договоренности, чтобы клуб не превратился в мусорку.\n"
        )
        kb = {
            "inline_keyboard": [
                [{"text": "Открыть правила", "callback_data": "m:rules"}],
                [{"text": "Профиль", "callback_data": "m:profile"}],
            ]
        }
        return text, kb

    if topic_key == "feedback":
        text = (
            f"💬 <b>Фидбек</b>\n"
            f"{sep()}\n"
            f"Фидбек тут ценится больше, чем самообман. Пиши по делу и по-человечески.\n"
        )
        kb = {
            "inline_keyboard": [
                [{"text": "Как писать фидбек", "callback_data": "m:feedback_how"}],
                [{"text": "Баланс", "callback_data": "m:balance"}],
            ]
        }
        return text, kb

    if topic_key == "announcements":
        text = (
            f"📌 <b>Объявления</b>\n"
            f"{sep()}\n"
            f"Тут важное. Не обещаю, что будет весело, но будет полезно.\n"
        )
        kb = {
            "inline_keyboard": [
                [{"text": "Правила", "callback_data": "m:rules"}, {"text": "Очередь", "callback_data": "m:queue"}],
                [{"text": "Профиль", "callback_data": "m:profile"}, {"text": "Баланс", "callback_data": "m:balance"}],
            ]
        }
        return text, kb

    # offtop
    text = (
        f"😄 <b>Оффтоп</b>\n"
        f"{sep()}\n"
        f"Тут можно выдохнуть. Только не превращай это в работу модераторов.\n"
    )
    kb = {
        "inline_keyboard": [
            [{"text": "Команды", "callback_data": "m:help"}],
            [{"text": "Баланс", "callback_data": "m:balance"}],
        ]
    }
    return text, kb

def ensure_topic_menus():
    """
    В каждой теме создаем/обновляем закреп-меню.
    Это и есть твои 'постоянно закрепленные кнопки в темах'.
    """
    for topic_key, thread_id in GROUP_TOPICS.items():
        text, kb = build_topic_menu(topic_key)
        stored = topic_menu_message_ids.get(topic_key)

        if stored:
            # пробуем обновить
            ok = edit_message(GROUP_ID, stored, text=text, reply_markup=kb)
            if ok:
                pin_message(GROUP_ID, stored, disable_notification=True)
                continue

        # если не получилось, создаем заново
        res = send_message(GROUP_ID, text, topic_id=thread_id, reply_markup=kb)
        if res and "result" in res:
            mid = res["result"]["message_id"]
            topic_menu_message_ids[topic_key] = mid
            pin_message(GROUP_ID, mid, disable_notification=True)

    save_data()
    logger.info("Topic menus ensured")

# --------------------
# КЛАВИАТУРА В ЛС (ReplyKeyboardMarkup)
# --------------------
def build_private_reply_keyboard():
    # “обычная клавиатура” внизу, удобна в личке
    return {
        "keyboard": [
            [{"text": "📜 Правила"}, {"text": "📋 Очередь"}],
            [{"text": "👤 Профиль"}, {"text": "🪙 Баланс"}],
            [{"text": "✍️ Подать статью"}, {"text": "🎁 Награда"}],
            [{"text": "🎮 Игры"}, {"text": "⚔️ Дуэль"}],
        ],
        "resize_keyboard": True,
        "one_time_keyboard": False
    }

def map_private_button_to_command(text: str):
    t = (text or "").strip()
    mapping = {
        "📜 Правила": "/rules",
        "📋 Очередь": "/queue",
        "👤 Профиль": "/profile",
        "🪙 Баланс": "/balance",
        "✍️ Подать статью": "/submit",
        "🎁 Награда": "/daily",
        "🎮 Игры": "/game",
        "⚔️ Дуэль": "/duel",
    }
    return mapping.get(t)

# --------------------
# ОТВЕТЫ БЕЗ ЗАСОРЕНИЯ ЧАТА
# --------------------
def send_topic_reply_clean(user_id: int, chat_id, topic_id: int, text: str, ttl_seconds: int = 120):
    """
    В теме хранит для каждого пользователя только последнее сообщение бота.
    Предыдущее удаляет. Текущее удаляет по таймеру.
    """
    key = (user_id, topic_id)
    old_mid = last_bot_reply.get(key)
    if old_mid:
        delete_message(chat_id, old_mid)

    res = send_message(chat_id, text, topic_id=topic_id)
    if res and "result" in res:
        mid = res["result"]["message_id"]
        last_bot_reply[key] = mid

        def later_delete():
            time.sleep(ttl_seconds)
            delete_message(chat_id, mid)

        threading.Thread(target=later_delete, daemon=True).start()

# --------------------
# ТЕКСТЫ
# --------------------
def text_rules():
    return (
        f"📜 <b>Правила клуба «Увлекательные чтения»</b>\n"
        f"{sep()}\n"
        f"Цель простая: меньше спама, больше чтения и нормального фидбека.\n\n"
        f"🧾 <b>Ссылки на статьи</b>\n"
        f"Принимаем только: ВК, Дзен, Телеграм.\n"
        f"Да, три платформы. Нет, это не заговор.\n\n"
        f"📋 <b>Очередь</b>\n"
        f"• 1 подача раз в {MIN_SUBMIT_HOURS} часов\n"
        f"• 1 активная статья в очереди\n"
        f"• максимум 10 статей в очереди\n\n"
        f"💬 <b>Фидбек</b>\n"
        f"• по делу, без токсичности\n"
        f"• минимум один нормальный фидбек в день (если ты живой участник, а не призрак)\n\n"
        f"🪙 <b>Кавычки</b>\n"
        f"Это внутренняя валюта. Дают за активность, тратятся в магазине.\n\n"
        f"Если очень хочется нарушать, делай это в художественных текстах, а не в правилах."
    )

def text_help():
    return (
        f"🧭 <b>Команды</b>\n"
        f"{sep()}\n"
        f"/start регистрация\n"
        f"/rules правила\n"
        f"/queue очередь\n"
        f"/submit подать статью (лучше в личке)\n"
        f"/profile профиль\n"
        f"/balance баланс\n"
        f"/daily ежедневная награда\n"
        f"/game игры\n"
        f"/duel дуэль (в группе)\n\n"
        f"Админам:\n"
        f"/refresh_menus обновить закрепы-меню\n"
        f"/publish_reading_list опубликовать лист чтения\n"
    )

def text_submit_instructions():
    return (
        f"✍️ <b>Подача статьи</b>\n"
        f"{sep()}\n"
        f"Ссылки принимаем только из ВК, Дзен и Телеграм.\n\n"
        f"Формат одним сообщением:\n\n"
        f"<b>ЗАГОЛОВОК</b>\n"
        f"Тема статьи\n\n"
        f"<b>ОПИСАНИЕ</b>\n"
        f"2-3 предложения\n\n"
        f"<b>ССЫЛКА</b>\n"
        f"https://...\n\n"
        f"Подсказка: чем понятнее описание, тем меньше вопросов и тем быстрее очередь двигается."
    )

# --------------------
# ОЧЕРЕДЬ/ЛИСТ ЧТЕНИЯ
# --------------------
def show_queue_text():
    if not articles_queue:
        return f"📭 Очередь пуста. Редкий момент гармонии.\n{sep()}\nПодай статью: /submit"
    lines = [f"📋 <b>Очередь</b>", sep()]
    for i, a in enumerate(list(articles_queue)[:10], 1):
        uid = int(a["user_id"])
        u = get_user(uid) or {}
        name = f"@{u.get('username')}" if u.get("username") else (u.get("first_name") or "автор")
        lines.append(f"{i}. <b>{a['title']}</b>\n   Автор: {name}\n   Ссылка: {a['url']}")
    lines.append(sep())
    lines.append(f"Всего в очереди: {len(articles_queue)}/10")
    return "\n".join(lines)

def publish_daily_reading_list():
    if not articles_queue:
        return "Очередь пуста, публиковать нечего."

    today = datetime.now().strftime("%d.%m.%Y")
    batch = list(articles_queue)[:5]

    lines = [f"📚 <b>Лист чтения на {today}</b>", sep()]
    for i, a in enumerate(batch, 1):
        uid = int(a["user_id"])
        u = get_user(uid) or {}
        name = f"@{u.get('username')}" if u.get("username") else (u.get("first_name") or "автор")
        desc = (a.get("description") or "").strip()
        if len(desc) > 160:
            desc = desc[:160] + "..."
        lines.append(
            f"{i}. <b>{a['title']}</b>\n"
            f"   Автор: {name}\n"
            f"   Описание: {desc}\n"
            f"   Читать: {a['url']}"
        )

    lines.append(sep())
    lines.append("Задача: прочитать хотя бы 1 статью и оставить нормальный фидбек.")
    text = "\n\n".join(lines)

    send_message(GROUP_ID, text, topic_id=GROUP_TOPICS["reading_list"])

    # помечаем и удаляем из очереди
    for a in batch:
        a["status"] = "published"
        a["published_at"] = now_iso()
        published_articles.append(a)

    for _ in range(len(batch)):
        if articles_queue:
            articles_queue.popleft()

    save_data()
    return f"Опубликовано {len(batch)} статей."

# --------------------
# ИГРЫ/ДУЭЛИ (минимально)
# --------------------
def start_duel(initiator_id: int, topic_text: str = None):
    if not topic_text:
        topics = [
            "Утро после странной новости",
            "Письмо, которое нельзя отправить",
            "Один разговор на кухне",
            "Старая фотография и один шанс",
            "Человек, который устал молчать",
        ]
        topic_text = topics[int(time.time()) % len(topics)]

    duel_id = f"duel_{int(time.time())}"
    duel = {
        "id": duel_id,
        "topic": topic_text,
        "initiator": initiator_id,
        "status": "waiting",
        "created_at": now_iso(),
        "participants": [initiator_id],
        "paragraphs": {},
        "votes": {},
        "prize": 25
    }
    duels.append(duel)
    u = get_user(initiator_id) or {}
    name = f"@{u.get('username')}" if u.get("username") else "инициатор"

    text = (
        f"⚔️ <b>Дуэль абзацев</b>\n"
        f"{sep()}\n"
        f"Тема: <b>{topic_text}</b>\n"
        f"Инициатор: {name}\n"
        f"Приз: {duel['prize']} кавычек\n\n"
        f"Как участвовать:\n"
        f"Ответь на это сообщение своим абзацем (3-5 предложений).\n"
        f"Время: 15 минут."
    )
    res = send_message(GROUP_ID, text, topic_id=GROUP_TOPICS["duels"])
    if res and "result" in res:
        duel["message_id"] = res["result"]["message_id"]

    # таймер окончания
    threading.Timer(900, finish_duel, args=[duel_id]).start()
    save_data()
    return duel_id

def finish_duel(duel_id: str):
    duel = next((d for d in duels if d["id"] == duel_id), None)
    if not duel or duel["status"] != "waiting":
        return

    duel["status"] = "voting"
    if len(duel["paragraphs"]) < 2:
        text = (
            f"⚔️ <b>Дуэль завершена</b>\n{sep()}\n"
            f"Тема: {duel['topic']}\n\n"
            f"Участников мало. Дуэль отменена.\n"
            f"Жизнь сурова, но справедлива."
        )
        duel["status"] = "cancelled"
        send_message(GROUP_ID, text, topic_id=GROUP_TOPICS["duels"], reply_to_message_id=duel.get("message_id"))
        save_data()
        return

    lines = [f"🗳 <b>Голосование</b>", sep(), f"Тема: {duel['topic']}", ""]
    participants = list(duel["paragraphs"].items())
    for idx, (uid, para) in enumerate(participants, 1):
        u = get_user(uid) or {}
        name = f"@{u.get('username')}" if u.get("username") else "участник"
        snippet = para.strip()
        if len(snippet) > 260:
            snippet = snippet[:260] + "..."
        lines.append(f"<b>{idx}. {name}</b>\n{snippet}\n")

    lines.append(sep())
    lines.append("Ответь числом (1, 2, 3...). Время: 10 минут.")
    text = "\n".join(lines)
    send_message(GROUP_ID, text, topic_id=GROUP_TOPICS["duels"], reply_to_message_id=duel.get("message_id"))

    threading.Timer(600, count_duel_votes, args=[duel_id]).start()
    save_data()

def count_duel_votes(duel_id: str):
    duel = next((d for d in duels if d["id"] == duel_id), None)
    if not duel or duel["status"] != "voting":
        return

    votes_count = defaultdict(int)
    for voter_id, vote_num in duel["votes"].items():
        votes_count[int(vote_num)] += 1

    if not votes_count:
        text = f"⚔️ Дуэль: голосов нет. Такое тоже бывает.\n{sep()}\nТема: {duel['topic']}"
        duel["status"] = "finished"
        send_message(GROUP_ID, text, topic_id=GROUP_TOPICS["duels"], reply_to_message_id=duel.get("message_id"))
        save_data()
        return

    winner_index = max(votes_count.items(), key=lambda x: x[1])[0]
    participants = list(duel["paragraphs"].keys())
    if 1 <= winner_index <= len(participants):
        winner_id = participants[winner_index - 1]
        duel["winner"] = winner_id
        duel["status"] = "finished"

        add_quotes(winner_id, duel["prize"], "Победа в дуэли")
        u = get_user(winner_id) or {}
        u["duels_won"] = int(u.get("duels_won", 0)) + 1
        set_user(winner_id, u)

        name = f"@{u.get('username')}" if u.get("username") else "победитель"
        text = (
            f"🏆 <b>Дуэль завершена</b>\n{sep()}\n"
            f"Победитель: {name}\n"
            f"Тема: {duel['topic']}\n"
            f"Приз: {duel['prize']} кавычек\n\n"
            f"Спасибо всем, кто писал и не исчез в туман."
        )
        send_message(GROUP_ID, text, topic_id=GROUP_TOPICS["duels"], reply_to_message_id=duel.get("message_id"))
        save_data()

# --------------------
# ЕЖЕДНЕВНАЯ НАГРАДА
# --------------------
def give_daily_reward(user_id: int):
    today = datetime.now().date().isoformat()
    if user_daily_reward.get(str(user_id)) == today or user_daily_reward.get(user_id) == today:
        return False, "⏳ Сегодня ты уже забирал награду."
    add_quotes(user_id, DAILY_REWARD_QUOTES, "Ежедневная награда")
    user_daily_reward[str(user_id)] = today
    save_data()
    return True, f"🎁 +{DAILY_REWARD_QUOTES} кавычек. Баланс: {user_balances.get(user_id, 0)}"

# --------------------
# ПЛАНИРОВЩИК (простая версия)
# --------------------
def schedule_daily_tasks():
    def loop():
        while True:
            now = datetime.now()
            # 19:00 лист чтения (по локальному времени сервера)
            if now.hour == 19 and now.minute == 0:
                try:
                    publish_daily_reading_list()
                except Exception as e:
                    logger.warning(f"publish_daily_reading_list failed: {e}")
                time.sleep(60)
            time.sleep(20)

    t = threading.Thread(target=loop, daemon=True)
    t.start()

# --------------------
# ОБРАБОТКА ВХОДЯЩЕГО
# --------------------
def process_message(message: dict):
    chat_id = message["chat"]["id"]
    from_obj = message.get("from", {})
    user_id = int(from_obj.get("id"))
    text = message.get("text", "") or ""

    # Кнопки ReplyKeyboard в личке мапим в команды
    if chat_id == user_id:
        mapped = map_private_button_to_command(text)
        if mapped:
            text = mapped

    u = get_user(user_id)
    if u:
        u["last_active"] = now_iso()
        set_user(user_id, u)

    # Ответы на сообщения (дуэльные, голосование и т.п.)
    if "reply_to_message" in message:
        return process_reply(message)

    if text.startswith("/"):
        return process_command(chat_id, user_id, text, message)

    # В личке можно подсказать
    if chat_id == user_id:
        send_message(user_id, "Напиши /help или нажми кнопки снизу.", reply_markup=build_private_reply_keyboard())

def process_command(chat_id: int, user_id: int, text: str, message: dict):
    cmd = normalize_command(text)

    # в группе команды работают только для зарегистрированных (кроме /start /help /rules)
    if is_group_chat(chat_id) and not is_user_registered(user_id) and cmd not in ["/start", "/help", "/rules"]:
        # в группе отвечаем аккуратно, в той же теме
        topic_id = message.get("message_thread_id")
        msg = f"📌 Сначала регистрация: открой бота в личке и нажми /start.\n{bot_deeplink()}"
        if topic_id:
            send_topic_reply_clean(user_id, chat_id, topic_id, msg, ttl_seconds=120)
        else:
            send_message(chat_id, msg)
        return

    if cmd == "/start":
        if chat_id == user_id:
            created = register_user(message.get("from", {}))
            if not created:
                send_message(user_id, "Ты уже зарегистрирован. Жми кнопки снизу.", reply_markup=build_private_reply_keyboard())
        else:
            # в группе /start не регистрируем, только отправляем в личку
            topic_id = message.get("message_thread_id")
            msg = f"Регистрация делается в личке: {bot_deeplink()}\nНажми /start там."
            if topic_id:
                send_topic_reply_clean(user_id, chat_id, topic_id, msg, ttl_seconds=120)
            else:
                send_message(chat_id, msg)
        return

    if cmd == "/help":
        if chat_id == user_id:
            send_message(user_id, text_help(), reply_markup=build_private_reply_keyboard())
        else:
            topic_id = message.get("message_thread_id")
            if topic_id:
                send_topic_reply_clean(user_id, chat_id, topic_id, text_help(), ttl_seconds=180)
            else:
                send_message(chat_id, text_help())
        return

    if cmd == "/rules":
        if chat_id == user_id:
            send_message(user_id, text_rules(), reply_markup=build_private_reply_keyboard())
        else:
            topic_id = message.get("message_thread_id") or GROUP_TOPICS.get("rules")
            send_message(GROUP_ID, text_rules(), topic_id=topic_id)
        return

    if cmd == "/queue":
        txt = show_queue_text()
        if chat_id == user_id:
            send_message(user_id, txt, reply_markup=build_private_reply_keyboard())
        else:
            topic_id = message.get("message_thread_id") or GROUP_TOPICS.get("queue")
            send_topic_reply_clean(user_id, chat_id, topic_id, txt, ttl_seconds=180)
        return

    if cmd == "/profile":
        u = get_user(user_id)
        if not u:
            send_message(user_id, f"Регистрация в личке: {bot_deeplink()}")
            return
        txt = (
            f"👤 <b>Профиль</b>\n{sep()}\n"
            f"Имя: {u.get('first_name','')} {u.get('last_name','')}\n"
            f"Юзернейм: @{u.get('username') if u.get('username') else 'не задан'}\n"
            f"Кавычки: {user_balances.get(user_id,0)}\n"
            f"Статей: {u.get('articles_count',0)}\n"
            f"Дуэлей выиграно: {u.get('duels_won',0)}\n"
        )
        if chat_id == user_id:
            send_message(user_id, txt, reply_markup=build_private_reply_keyboard())
        else:
            topic_id = message.get("message_thread_id")
            if topic_id:
                send_topic_reply_clean(user_id, chat_id, topic_id, txt, ttl_seconds=180)
            else:
                send_message(chat_id, txt)
        return

    if cmd == "/balance":
        bal = user_balances.get(user_id, 0)
        txt = f"🪙 Баланс: <b>{bal}</b> кавычек"
        if chat_id == user_id:
            send_message(user_id, txt, reply_markup=build_private_reply_keyboard())
        else:
            topic_id = message.get("message_thread_id")
            if topic_id:
                send_topic_reply_clean(user_id, chat_id, topic_id, txt, ttl_seconds=120)
            else:
                send_message(chat_id, txt)
        return

    if cmd == "/daily":
        ok, txt = give_daily_reward(user_id)
        if chat_id == user_id:
            send_message(user_id, txt, reply_markup=build_private_reply_keyboard())
        else:
            topic_id = message.get("message_thread_id")
            if topic_id:
                send_topic_reply_clean(user_id, chat_id, topic_id, txt, ttl_seconds=120)
            else:
                send_message(chat_id, txt)
        return

    if cmd == "/submit":
        # подавать лучше в личке, но можно и из группы, если дать все поля
        if chat_id != user_id and is_group_chat(chat_id):
            topic_id = message.get("message_thread_id") or GROUP_TOPICS.get("queue")
            send_topic_reply_clean(user_id, chat_id, topic_id, f"Подавать статью удобнее в личке: {bot_deeplink()}", ttl_seconds=180)
            return

        can, msg = can_submit_article(user_id)
        if not can:
            send_message(user_id, msg, reply_markup=build_private_reply_keyboard())
            return

        # если командой без текста, показываем инструкцию
        if text.strip() == "/submit":
            send_message(user_id, text_submit_instructions(), reply_markup=build_private_reply_keyboard())
            return

        # иначе пытаемся распарсить формат
        parsed = parse_submission_message(text)
        if not parsed:
            send_message(user_id, "Не понял формат. Вот инструкция:\n\n" + text_submit_instructions(), reply_markup=build_private_reply_keyboard())
            return

        title, desc, url = parsed
        if not is_allowed_article_url(url):
            send_message(
                user_id,
                "Ссылка не подходит. Принимаем только ВК, Дзен и Телеграм.\n"
                "Примеры: vk.com, dzen.ru, t.me, telegra.ph",
                reply_markup=build_private_reply_keyboard()
            )
            return

        art_id = add_article_to_queue(user_id, title, desc, url)
        send_message(
            user_id,
            f"✅ Принято. Статья в очереди.\n{sep()}\nID: {art_id}\nБаланс: {user_balances.get(user_id,0)}",
            reply_markup=build_private_reply_keyboard()
        )
        return

    if cmd == "/duel":
        if not is_group_chat(chat_id):
            send_message(chat_id, "Дуэли проходят в группе клуба.")
            return
        start_duel(user_id)
        return

    if cmd == "/publish_reading_list":
        if not is_admin(user_id):
            send_message(chat_id, "Это команда для админов.")
            return
        res = publish_daily_reading_list()
        send_message(user_id, res)
        return

    if cmd == "/refresh_menus":
        if not is_admin(user_id):
            send_message(chat_id, "Это команда для админов.")
            return
        ensure_topic_menus()
        send_message(user_id, "Меню в темах обновлены и закреплены.")
        return

    # неизвестное
    if chat_id == user_id:
        send_message(user_id, "Неизвестная команда. /help", reply_markup=build_private_reply_keyboard())
    else:
        topic_id = message.get("message_thread_id")
        msg = "Неизвестная команда. /help"
        if topic_id:
            send_topic_reply_clean(user_id, chat_id, topic_id, msg, ttl_seconds=120)
        else:
            send_message(chat_id, msg)

def parse_submission_message(text: str):
    """
    Ожидаем блоки:
    ЗАГОЛОВОК
    ...
    ОПИСАНИЕ
    ...
    ССЫЛКА
    ...
    """
    t = text.strip()
    # Уберем команду /submit из начала
    t = re.sub(r"^/submit(\s+)?", "", t, flags=re.IGNORECASE).strip()

    def find_block(name):
        m = re.search(rf"\b{name}\b", t, flags=re.IGNORECASE)
        return m.start() if m else None

    p1 = find_block("ЗАГОЛОВОК")
    p2 = find_block("ОПИСАНИЕ")
    p3 = find_block("ССЫЛКА")
    if p1 is None or p2 is None or p3 is None:
        return None
    if not (p1 < p2 < p3):
        return None

    title = t[p1 + len("ЗАГОЛОВОК"):p2].strip()
    desc = t[p2 + len("ОПИСАНИЕ"):p3].strip()
    url = t[p3 + len("ССЫЛКА"):].strip().split()[0]

    if not title or not url:
        return None
    return title, desc, url

def process_reply(message: dict):
    chat_id = message["chat"]["id"]
    user_id = int(message.get("from", {}).get("id"))
    text = (message.get("text") or "").strip()
    reply_to = message.get("reply_to_message", {})
    topic_id = message.get("message_thread_id")

    # ответы в дуэли: отвечают на сообщение дуэли
    for duel in duels:
        if duel.get("message_id") and reply_to.get("message_id") == duel.get("message_id") and duel.get("status") == "waiting":
            duel["participants"] = list(set(duel.get("participants", []) + [user_id]))
            duel["paragraphs"][user_id] = text
            save_data()
            # подтверждение в личку
            send_message(user_id, "✅ Абзац принят. Жди голосования.", reply_markup=build_private_reply_keyboard())
            return

    # голосование: если дуэль в статусе voting, принимаем число как голос, если ответ в теме дуэлей
    if topic_id == GROUP_TOPICS.get("duels"):
        m = re.match(r"^\s*(\d+)\s*$", text)
        if m:
            vote_num = int(m.group(1))
            # находим последнюю дуэль в voting
            duel = next((d for d in reversed(duels) if d.get("status") == "voting"), None)
            if duel:
                duel["votes"][user_id] = vote_num
                save_data()
                send_message(user_id, "🗳 Голос учтен.", reply_markup=build_private_reply_keyboard())
                return

# --------------------
# CALLBACK (кнопки)
# --------------------
def process_callback(cb: dict):
    callback_id = cb["id"]
    user_id = int(cb["from"]["id"])
    data = cb.get("data", "")
    msg = cb.get("message", {})
    chat_id = msg.get("chat", {}).get("id")
    topic_id = msg.get("message_thread_id")

    # Подтверждаем сразу, чтобы Telegram не крутил "часики"
    answer_callback(callback_id)

    def reply_in_topic(text: str, ttl=150):
        if chat_id and topic_id:
            send_topic_reply_clean(user_id, chat_id, topic_id, text, ttl_seconds=ttl)
        else:
            # fallback в личку
            send_message(user_id, text, reply_markup=build_private_reply_keyboard())

    if data == "m:rules":
        reply_in_topic(text_rules(), ttl=220)
        return

    if data == "m:help":
        reply_in_topic(text_help(), ttl=220)
        return

    if data == "m:queue":
        reply_in_topic(show_queue_text(), ttl=220)
        return

    if data == "m:submit":
        # в тему пишем коротко, в личку даем инструкцию
        reply_in_topic(f"Подать статью лучше в личке: {bot_deeplink()}\nТам напиши /submit", ttl=180)
        send_message(user_id, text_submit_instructions(), reply_markup=build_private_reply_keyboard())
        return

    if data == "m:when":
        can, msg_text = can_submit_article(user_id)
        if can:
            reply_in_topic("✅ Подать можно уже сейчас. В личке: /submit", ttl=160)
        else:
            reply_in_topic(msg_text, ttl=160)
        return

    if data == "m:profile":
        u = get_user(user_id)
        if not u:
            send_message(user_id, f"Регистрация в личке: {bot_deeplink()}", reply_markup=build_private_reply_keyboard())
            return
        reply_in_topic(
            f"👤 Профиль: @{u.get('username') if u.get('username') else 'не задан'}\n"
            f"Кавычки: {user_balances.get(user_id,0)}\n"
            f"Статей: {u.get('articles_count',0)}",
            ttl=180
        )
        return

    if data == "m:balance":
        bal = user_balances.get(user_id, 0)
        # для баланса удобно показывать алертом: только человеку, без мусора в чате
        answer_callback(callback_id, text=f"Баланс: {bal} кавычек", show_alert=True)
        return

    if data == "m:reading":
        # покажем последнее опубликованное (если есть)
        if not published_articles:
            reply_in_topic("Пока листов чтения нет. Очередь можно посмотреть в теме Очередь.", ttl=180)
            return
        last = published_articles[-5:]
        lines = ["📚 Последние статьи из листа:", sep()]
        for a in last:
            lines.append(f"• <b>{a.get('title')}</b>\n  {a.get('url')}")
        reply_in_topic("\n".join(lines), ttl=220)
        return

    if data == "m:feedback_how":
        reply_in_topic(
            "💬 Как писать фидбек:\n"
            "• что понравилось конкретно\n"
            "• что можно улучшить (одна мысль, без лекций)\n"
            "• самый яркий момент\n"
            "• пожелание автору\n\n"
            "Идея простая: помоги тексту стать лучше, а не самоутвердись.",
            ttl=240
        )
        return

    if data == "m:duel_start":
        # запуск дуэли в теме дуэлей
        if is_group_chat(chat_id):
            start_duel(user_id)
        else:
            send_message(user_id, "Дуэли запускаются в группе.", reply_markup=build_private_reply_keyboard())
        return

    if data == "m:duel_how":
        reply_in_topic(
            "⚔️ Как участвовать в дуэли:\n"
            "1) дождись дуэльного сообщения\n"
            "2) нажми 'Ответить'\n"
            "3) напиши абзац 3-5 предложений\n\n"
            "Кнопка 'участвовать' не вставит текст за тебя. Пока что.",
            ttl=240
        )
        return

    if data == "m:games_schedule":
        reply_in_topic(
            "🎮 Расписание (пример):\n"
            "• 10:00 дуэль\n"
            "• 14:00 игра дня\n"
            "• 18:00 колесо тем\n"
            "• 19:00 лист чтения\n\n"
            "Время сервера может чуть гулять. Как и вдохновение.",
            ttl=240
        )
        return

    if data == "m:games_results":
        if not games_results:
            reply_in_topic("Пока результатов нет.", ttl=160)
        else:
            last = games_results[-5:]
            lines = ["🏆 Результаты:", sep()]
            for r in last:
                winners = ", ".join(r.get("winners") or []) or "без победителей"
                lines.append(f"• {r.get('title')} ({r.get('date')}): {winners}")
            reply_in_topic("\n".join(lines), ttl=220)
        return

    if data == "m:shop_price":
        reply_in_topic(
            "🛒 Прайс (заглушка, но честная):\n"
            "• Подарок участнику: 20\n"
            "• Особый бейдж: 100\n"
            "• Приоритет в очереди: 150\n\n"
            "Пока это черновик. Но черновики тоже люди.",
            ttl=260
        )
        return

    if data == "m:shop_spend":
        reply_in_topic("Тратить будем через личку, чтобы не превращать тему в кассу.\nНапиши боту в личку: /balance", ttl=180)
        return

    # неизвестное
    reply_in_topic(f"Кнопка нажата: {data}", ttl=120)

# --------------------
# WEBHOOK
# --------------------
@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        data = request.get_json(force=True, silent=False) or {}
        if "message" in data:
            process_message(data["message"])
        elif "callback_query" in data:
            process_callback(data["callback_query"])
        return jsonify({"ok": True}), 200
    except Exception as e:
        logger.error(f"webhook error: {e}", exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 200

@app.route("/health", methods=["GET"])
def health():
    return jsonify({
        "status": "healthy",
        "time": now_iso(),
        "users": len(users),
        "queue": len(articles_queue),
        "published": len(published_articles),
        "total_quotes": sum(user_balances.values()) if user_balances else 0
    }), 200

# --------------------
# СТАРТ ФОНА (1 раз)
# --------------------
_started = False

def start_background_once():
    global _started
    if _started:
        return
    _started = True
    load_data()
    schedule_data_saves()
    schedule_daily_tasks()
    ensure_topic_menus()
    atexit.register(save_data)
    logger.info("Background started")

@app.before_request
def _warmup():
    start_background_once()

if __name__ == "__main__":
    start_background_once()
    port = int(os.environ.get("PORT", "5000"))
    logger.info(f"Starting Flask on {port}")
    app.run(host="0.0.0.0", port=port)

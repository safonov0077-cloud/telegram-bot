import os
import json
import logging
import threading
import time
import re
import sqlite3
from datetime import datetime, timedelta
from collections import defaultdict
from urllib.parse import urlparse
from typing import Optional, Dict, Any, List, Tuple

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
BOT_USERNAME = os.environ.get("BOT_USERNAME", "").strip()
GROUP_ID = int(os.environ.get("GROUP_ID", "-1003646270051"))
TOPIC_QUEUE_ID = int(os.environ.get("TOPIC_QUEUE_ID", "0"))
DB_PATH = os.environ.get("DB_PATH", "clubbot.sqlite3").strip()

ADMIN_IDS = {
    int(x) for x in os.environ.get("ADMIN_IDS", "1039651708").split(",")
    if x.strip().isdigit()
}

ALLOWED_PLATFORMS_TEXT = "VK, Дзен, Telegram"
ALLOWED_DOMAINS = {
    "vk.com", "m.vk.com",
    "dzen.ru", "zen.yandex.ru",
    "t.me", "telegra.ph",
}

URL_RE = re.compile(r"(https?://\S+)", re.I)

if not TELEGRAM_TOKEN:
    logger.error("TELEGRAM_TOKEN пустой. Бот не сможет работать.")

# =========================
# STORAGE (SQLite)
# =========================

class Storage:
    def __init__(self, path: str):
        self.path = path
        self.lock = threading.RLock()
        self.local = threading.local()
        self._init_db()

    def _get_conn(self) -> sqlite3.Connection:
        conn = getattr(self.local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(self.path, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            conn.execute("PRAGMA foreign_keys=ON;")
            self.local.conn = conn
        return conn

    def _exec(self, sql: str, params: Tuple = ()) -> None:
        with self.lock:
            conn = self._get_conn()
            conn.execute(sql, params)
            conn.commit()

    def _exec_many(self, sql: str, seq_of_params: List[Tuple]) -> None:
        with self.lock:
            conn = self._get_conn()
            conn.executemany(sql, seq_of_params)
            conn.commit()

    def _query_one(self, sql: str, params: Tuple = ()) -> Optional[sqlite3.Row]:
        conn = self._get_conn()
        cur = conn.execute(sql, params)
        return cur.fetchone()

    def _query_all(self, sql: str, params: Tuple = ()) -> List[sqlite3.Row]:
        conn = self._get_conn()
        cur = conn.execute(sql, params)
        return cur.fetchall()

    def _init_db(self) -> None:
        with self.lock:
            conn = sqlite3.connect(self.path, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            conn.execute("PRAGMA foreign_keys=ON;")

            conn.executescript("""
            CREATE TABLE IF NOT EXISTS meta (
                k TEXT PRIMARY KEY,
                v TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                registered_at TEXT,
                last_active TEXT,
                articles_count INTEGER DEFAULT 0,
                feedback_given INTEGER DEFAULT 0,
                games_played INTEGER DEFAULT 0,
                total_quotes INTEGER DEFAULT 0,
                badges_json TEXT DEFAULT '["новичок"]'
            );

            CREATE TABLE IF NOT EXISTS balances (
                user_id INTEGER PRIMARY KEY,
                balance INTEGER NOT NULL DEFAULT 0,
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS user_state (
                user_id INTEGER PRIMARY KEY,
                last_submit_at TEXT,
                daily_reward_date TEXT,
                submit_notified_at TEXT,
                state TEXT,
                state_started_at TEXT,
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS submissions (
                article_id TEXT PRIMARY KEY,
                user_id INTEGER NOT NULL,
                url TEXT NOT NULL,
                submitted_at TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS queue (
                position INTEGER PRIMARY KEY AUTOINCREMENT,
                article_id TEXT UNIQUE NOT NULL,
                user_id INTEGER NOT NULL,
                queued_at TEXT NOT NULL,
                FOREIGN KEY(article_id) REFERENCES submissions(article_id) ON DELETE CASCADE,
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS published (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                article_id TEXT NOT NULL,
                user_id INTEGER NOT NULL,
                url TEXT NOT NULL,
                published_at TEXT NOT NULL,
                list_date TEXT NOT NULL,
                FOREIGN KEY(article_id) REFERENCES submissions(article_id) ON DELETE CASCADE,
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS games_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                game_type TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS duels (
                duel_id TEXT PRIMARY KEY,
                topic TEXT NOT NULL,
                initiator INTEGER NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                prize INTEGER NOT NULL,
                thread_id INTEGER,
                announce_message_id INTEGER,
                vote_message_id INTEGER,
                submissions_deadline TEXT,
                vote_deadline TEXT,
                participants_json TEXT DEFAULT '[]',
                paragraphs_json TEXT DEFAULT '{}',
                votes_json TEXT DEFAULT '{}',
                winner INTEGER,
                FOREIGN KEY(initiator) REFERENCES users(id) ON DELETE CASCADE
            );
            """)

            conn.commit()
            conn.close()

    # ---- meta ----
    def get_meta(self, k: str) -> Optional[str]:
        row = self._query_one("SELECT v FROM meta WHERE k = ?", (k,))
        return row["v"] if row else None

    def set_meta(self, k: str, v: str) -> None:
        self._exec("INSERT INTO meta(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v", (k, v))

    # ---- users ----
    def is_registered(self, user_id: int) -> bool:
        row = self._query_one("SELECT 1 FROM users WHERE id=?", (int(user_id),))
        return bool(row)

    def upsert_user(self, user_data: Dict[str, Any]) -> None:
        uid = int(user_data["id"])
        now = datetime.now().isoformat()
        username = user_data.get("username")
        first_name = user_data.get("first_name", "")
        last_name = user_data.get("last_name", "")

        with self.lock:
            conn = self._get_conn()
            existing = conn.execute("SELECT id FROM users WHERE id=?", (uid,)).fetchone()
            if not existing:
                conn.execute(
                    """INSERT INTO users(id, username, first_name, last_name, registered_at, last_active, total_quotes)
                       VALUES(?,?,?,?,?,?,?)""",
                    (uid, username, first_name, last_name, now, now, 50)
                )
                conn.execute("INSERT INTO balances(user_id, balance) VALUES(?,?)", (uid, 50))
                conn.execute("INSERT INTO user_state(user_id) VALUES(?)", (uid,))
            else:
                conn.execute(
                    """UPDATE users SET username=?, first_name=?, last_name=?, last_active=?
                       WHERE id=?""",
                    (username, first_name, last_name, now, uid)
                )
            conn.commit()

    def set_last_active(self, user_id: int) -> None:
        self._exec("UPDATE users SET last_active=? WHERE id=?", (datetime.now().isoformat(), int(user_id)))

    def get_user(self, user_id: int) -> Optional[Dict[str, Any]]:
        row = self._query_one("SELECT * FROM users WHERE id=?", (int(user_id),))
        if not row:
            return None
        d = dict(row)
        try:
            d["badges"] = json.loads(d.get("badges_json") or "[]")
        except Exception:
            d["badges"] = ["новичок"]
        return d

    def get_balance(self, user_id: int) -> int:
        row = self._query_one("SELECT balance FROM balances WHERE user_id=?", (int(user_id),))
        return int(row["balance"]) if row else 0

    def add_quotes(self, user_id: int, amount: int, reason: str) -> int:
        uid = int(user_id)
        amt = int(amount)
        with self.lock:
            conn = self._get_conn()
            conn.execute("UPDATE balances SET balance = balance + ? WHERE user_id=?", (amt, uid))
            conn.execute("UPDATE users SET total_quotes = total_quotes + ? WHERE id=?", (amt, uid))
            conn.commit()
        logger.info(f"quotes +{amt} to {uid} ({reason})")
        return self.get_balance(uid)

    def spend_quotes(self, user_id: int, amount: int, reason: str) -> bool:
        uid = int(user_id)
        amt = int(amount)
        with self.lock:
            conn = self._get_conn()
            row = conn.execute("SELECT balance FROM balances WHERE user_id=?", (uid,)).fetchone()
            bal = int(row["balance"]) if row else 0
            if bal < amt:
                return False
            conn.execute("UPDATE balances SET balance = balance - ? WHERE user_id=?", (amt, uid))
            conn.commit()
        logger.info(f"quotes -{amt} from {uid} ({reason})")
        return True

    # ---- user_state ----
    def get_last_submit_at(self, user_id: int) -> Optional[datetime]:
        row = self._query_one("SELECT last_submit_at FROM user_state WHERE user_id=?", (int(user_id),))
        if row and row["last_submit_at"]:
            try:
                return datetime.fromisoformat(row["last_submit_at"])
            except Exception:
                return None
        return None

    def set_last_submit_at(self, user_id: int, dt: datetime) -> None:
        self._exec("UPDATE user_state SET last_submit_at=? WHERE user_id=?", (dt.isoformat(), int(user_id)))

    def get_daily_reward_date(self, user_id: int) -> Optional[str]:
        row = self._query_one("SELECT daily_reward_date FROM user_state WHERE user_id=?", (int(user_id),))
        return row["daily_reward_date"] if row else None

    def set_daily_reward_date(self, user_id: int, date_iso: str) -> None:
        self._exec("UPDATE user_state SET daily_reward_date=? WHERE user_id=?", (date_iso, int(user_id)))

    def get_submit_notified_at(self, user_id: int) -> Optional[datetime]:
        row = self._query_one("SELECT submit_notified_at FROM user_state WHERE user_id=?", (int(user_id),))
        if row and row["submit_notified_at"]:
            try:
                return datetime.fromisoformat(row["submit_notified_at"])
            except Exception:
                return None
        return None

    def set_submit_notified_at(self, user_id: int, dt: datetime) -> None:
        self._exec("UPDATE user_state SET submit_notified_at=? WHERE user_id=?", (dt.isoformat(), int(user_id)))

    def set_state(self, user_id: int, state: str) -> None:
        now = datetime.now().isoformat()
        self._exec(
            "UPDATE user_state SET state=?, state_started_at=? WHERE user_id=?",
            (state, now, int(user_id))
        )

    def clear_state(self, user_id: int) -> None:
        self._exec("UPDATE user_state SET state=NULL, state_started_at=NULL WHERE user_id=?", (int(user_id),))

    def get_state(self, user_id: int) -> Optional[str]:
        row = self._query_one("SELECT state FROM user_state WHERE user_id=?", (int(user_id),))
        return row["state"] if row else None

    # ---- submissions + queue ----
    def queue_count(self) -> int:
        row = self._query_one("SELECT COUNT(*) AS c FROM queue")
        return int(row["c"]) if row else 0

    def queue_has_user(self, user_id: int) -> bool:
        row = self._query_one("SELECT 1 FROM queue WHERE user_id=? LIMIT 1", (int(user_id),))
        return bool(row)

    def add_submission_and_queue(self, user_id: int, url: str) -> str:
        uid = int(user_id)
        article_id = f"art_{int(time.time())}_{uid}"
        now = datetime.now().isoformat()
        with self.lock:
            conn = self._get_conn()
            conn.execute(
                "INSERT INTO submissions(article_id,user_id,url,submitted_at,status) VALUES(?,?,?,?,?)",
                (article_id, uid, url, now, "pending")
            )
            conn.execute(
                "INSERT INTO queue(article_id,user_id,queued_at) VALUES(?,?,?)",
                (article_id, uid, now)
            )
            conn.execute("UPDATE user_state SET last_submit_at=? WHERE user_id=?", (now, uid))
            conn.execute("UPDATE users SET articles_count = articles_count + 1 WHERE id=?", (uid,))
            conn.commit()
        return article_id

    def list_queue(self, limit: int = 10) -> List[Dict[str, Any]]:
        rows = self._query_all(
            """SELECT q.position, s.article_id, s.user_id, s.url, s.submitted_at
               FROM queue q
               JOIN submissions s ON s.article_id = q.article_id
               ORDER BY q.position ASC
               LIMIT ?""",
            (int(limit),)
        )
        return [dict(r) for r in rows]

    def pop_from_queue(self, n: int) -> List[Dict[str, Any]]:
        with self.lock:
            conn = self._get_conn()
            rows = conn.execute(
                """SELECT q.position, s.article_id, s.user_id, s.url, s.submitted_at
                   FROM queue q
                   JOIN submissions s ON s.article_id = q.article_id
                   ORDER BY q.position ASC
                   LIMIT ?""",
                (int(n),)
            ).fetchall()

            if not rows:
                return []

            positions = [int(r["position"]) for r in rows]
            conn.executemany("DELETE FROM queue WHERE position=?", [(p,) for p in positions])
            conn.commit()

        return [dict(r) for r in rows]

    def add_published(self, article: Dict[str, Any], list_date: str) -> None:
        with self.lock:
            conn = self._get_conn()
            conn.execute(
                "INSERT INTO published(article_id,user_id,url,published_at,list_date) VALUES(?,?,?,?,?)",
                (article["article_id"], int(article["user_id"]), article["url"], datetime.now().isoformat(), list_date)
            )
            conn.execute("UPDATE submissions SET status='published' WHERE article_id=?", (article["article_id"],))
            conn.commit()

    def list_user_submissions(self, user_id: int, limit: int = 10) -> List[Dict[str, Any]]:
        rows = self._query_all(
            """SELECT article_id, url, submitted_at, status
               FROM submissions
               WHERE user_id=?
               ORDER BY submitted_at DESC
               LIMIT ?""",
            (int(user_id), int(limit))
        )
        return [dict(r) for r in rows]

    # ---- top ----
    def top_users(self, limit: int = 10) -> List[Dict[str, Any]]:
        rows = self._query_all(
            """SELECT u.id, u.username, u.first_name, u.last_name, u.articles_count, b.balance
               FROM users u
               JOIN balances b ON b.user_id = u.id
               ORDER BY b.balance DESC
               LIMIT ?""",
            (int(limit),)
        )
        return [dict(r) for r in rows]

    def rank_of_user(self, user_id: int) -> Tuple[int, int]:
        uid = int(user_id)
        row_total = self._query_one("SELECT COUNT(*) AS c FROM users")
        total = int(row_total["c"]) if row_total else 0
        rows = self._query_all(
            """SELECT u.id
               FROM users u
               JOIN balances b ON b.user_id = u.id
               ORDER BY b.balance DESC"""
        )
        rank = total
        for i, r in enumerate(rows, 1):
            if int(r["id"]) == uid:
                rank = i
                break
        return rank, total

    # ---- games ----
    def add_game_history(self, game_type: str, payload: Dict[str, Any]) -> None:
        self._exec(
            "INSERT INTO games_history(game_type,payload_json,created_at) VALUES(?,?,?)",
            (game_type, json.dumps(payload, ensure_ascii=False), datetime.now().isoformat())
        )

    # ---- duels ----
    def create_duel(self, duel_id: str, topic: str, initiator: int, prize: int, thread_id: Optional[int],
                    announce_message_id: Optional[int], submissions_deadline: datetime) -> None:
        self._exec(
            """INSERT INTO duels(duel_id,topic,initiator,status,created_at,prize,thread_id,announce_message_id,submissions_deadline)
               VALUES(?,?,?,?,?,?,?,?,?)""",
            (
                duel_id, topic, int(initiator), "waiting", datetime.now().isoformat(), int(prize),
                int(thread_id) if thread_id else None,
                int(announce_message_id) if announce_message_id else None,
                submissions_deadline.isoformat()
            )
        )

    def get_active_duel_waiting(self) -> Optional[Dict[str, Any]]:
        row = self._query_one("SELECT * FROM duels WHERE status='waiting' ORDER BY created_at DESC LIMIT 1")
        return dict(row) if row else None

    def get_active_duel_voting(self) -> Optional[Dict[str, Any]]:
        row = self._query_one("SELECT * FROM duels WHERE status='voting' ORDER BY created_at DESC LIMIT 1")
        return dict(row) if row else None

    def get_duel_by_id(self, duel_id: str) -> Optional[Dict[str, Any]]:
        row = self._query_one("SELECT * FROM duels WHERE duel_id=?", (duel_id,))
        return dict(row) if row else None

    def update_duel_json_fields(self, duel_id: str, participants: List[int], paragraphs: Dict[str, str], votes: Dict[str, int]) -> None:
        self._exec(
            """UPDATE duels SET participants_json=?, paragraphs_json=?, votes_json=? WHERE duel_id=?""",
            (
                json.dumps(participants, ensure_ascii=False),
                json.dumps(paragraphs, ensure_ascii=False),
                json.dumps(votes, ensure_ascii=False),
                duel_id
            )
        )

    def set_duel_status(self, duel_id: str, status: str) -> None:
        self._exec("UPDATE duels SET status=? WHERE duel_id=?", (status, duel_id))

    def set_duel_voting(self, duel_id: str, vote_message_id: int, vote_deadline: datetime) -> None:
        self._exec(
            "UPDATE duels SET status='voting', vote_message_id=?, vote_deadline=? WHERE duel_id=?",
            (int(vote_message_id), vote_deadline.isoformat(), duel_id)
        )

    def set_duel_winner(self, duel_id: str, winner_id: Optional[int]) -> None:
        self._exec("UPDATE duels SET winner=? WHERE duel_id=?", (int(winner_id) if winner_id else None, duel_id))

    def list_duels_due(self, now: datetime) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        waiting = self._query_all(
            "SELECT * FROM duels WHERE status='waiting' AND submissions_deadline IS NOT NULL AND submissions_deadline <= ?",
            (now.isoformat(),)
        )
        voting = self._query_all(
            "SELECT * FROM duels WHERE status='voting' AND vote_deadline IS NOT NULL AND vote_deadline <= ?",
            (now.isoformat(),)
        )
        return ([dict(r) for r in waiting], [dict(r) for r in voting])

store = Storage(DB_PATH)

# =========================
# TELEGRAM API
# =========================

def tg(method: str, payload: dict, timeout: int = 12):
    if not TELEGRAM_TOKEN:
        logger.error("TELEGRAM_TOKEN не установлен")
        return None

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/{method}"
    try:
        resp = requests.post(url, json=payload, timeout=timeout)
        data = resp.json()
        if not data.get("ok"):
            logger.error(f"Telegram API error {method}: {data.get('description')} | keys={list(payload.keys())}")
        return data
    except Exception as e:
        logger.error(f"Telegram request failed {method}: {e}")
        return None

def send_telegram_message(chat_id, text, parse_mode="HTML", reply_markup=None, message_thread_id=None, reply_to_message_id=None):
    payload = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": True,
    }
    if parse_mode:
        payload["parse_mode"] = parse_mode
    if reply_markup:
        payload["reply_markup"] = reply_markup
    if message_thread_id:
        payload["message_thread_id"] = int(message_thread_id)
    if reply_to_message_id:
        payload["reply_to_message_id"] = int(reply_to_message_id)

    logger.info(f"sendMessage -> chat_id={chat_id} thread={message_thread_id} text={str(text)[:120]}...")
    return tg("sendMessage", payload)

def answer_callback(callback_query_id, text, show_alert=False):
    return tg("answerCallbackQuery", {
        "callback_query_id": callback_query_id,
        "text": text,
        "show_alert": show_alert
    })

# =========================
# ВСПОМОГАТЕЛЬНЫЕ
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
    if not text:
        return ""
    cmd = text.split()[0].strip().lower()
    if "@" in cmd:
        cmd = cmd.split("@", 1)[0]
    return cmd

def safe_username(user_id: int) -> str:
    u = store.get_user(user_id) or {}
    username = u.get("username")
    if username:
        return "@" + username
    name = (u.get("first_name", "") + " " + u.get("last_name", "")).strip()
    return name if name else f"пользователь {user_id}"

def parse_domain(url: str) -> str:
    try:
        parsed = urlparse(url.strip())
        return (parsed.netloc or "").lower()
    except Exception:
        return ""

def is_allowed_article_url(url: str) -> bool:
    if not url or not isinstance(url, str):
        return False
    url = url.strip()
    if not (url.startswith("http://") or url.startswith("https://")):
        return False
    domain = parse_domain(url)
    if domain.startswith("www."):
        domain = domain[4:]
    for allowed in ALLOWED_DOMAINS:
        if domain == allowed or domain.endswith("." + allowed):
            return True
    return False

def extract_first_url(text: str) -> str:
    if not text:
        return ""
    m = URL_RE.search(text.strip())
    return m.group(1).strip() if m else ""

def choose_thread_id(incoming_thread_id: Optional[int], forced_topic_id: int = 0) -> Optional[int]:
    if forced_topic_id and forced_topic_id > 0:
        return forced_topic_id
    return incoming_thread_id if incoming_thread_id else None

# =========================
# ЛОГИКА КЛУБА
# =========================

def can_submit_article(user_id: int) -> Tuple[bool, str]:
    uid = int(user_id)

    last_submit = store.get_last_submit_at(uid)
    if last_submit:
        diff = datetime.now() - last_submit
        if diff.total_seconds() < 48 * 3600:
            hours_left = int((48 * 3600 - diff.total_seconds()) / 3600)
            if hours_left < 1:
                return False, "Можно будет подать менее чем через час"
            return False, f"Можно будет подать через {hours_left} часов"

    if store.queue_has_user(uid):
        return False, "У тебя уже есть ссылка в очереди"

    if store.queue_count() >= 10:
        return False, "Очередь заполнена (максимум 10 ссылок)"

    return True, "Можно подавать"

def register_user(user_data: dict) -> None:
    store.upsert_user(user_data)

    welcome_text = f"""📚 <b>Увлекательные чтения</b>

Добро пожаловать в клуб, где ценят реальные отзывы, а не обмен лайками.

🎯 <b>Как начать:</b>
1. <b>/daily</b> - ежедневная награда (5 кавычек)
2. <b>/balance</b> - баланс
3. <b>/queue</b> - очередь (в группе или в личке)
4. <b>/submit</b> - подать статью (ссылкой)

📜 <b>Правила:</b>
• 1 ссылка раз в 48-72 часа
• Ссылки только: {ALLOWED_PLATFORMS_TEXT}
• Реальные отзывы, а не "норм"

💰 <b>Старт:</b> 50 кавычек 🪙

Пиши <b>/help</b> для списка команд."""
    send_telegram_message(int(user_data["id"]), welcome_text)

def show_help(chat_id: int, thread_id: Optional[int] = None) -> None:
    text = f"""📚 <b>Команды</b>

<b>Личные:</b>
/start - регистрация
/help - помощь
/profile - профиль
/balance - баланс
/daily - ежедневка
/submit - подать ссылку
/my_posts - мои ссылки

<b>Группа:</b>
/queue - очередь
/top - топ
/game - игры дня
/rules - правила

<b>Важно:</b>
• Ссылки: {ALLOWED_PLATFORMS_TEXT}
• Лимит: 1 ссылка раз в 48-72 часа
"""
    send_telegram_message(chat_id, text, message_thread_id=thread_id)

def show_profile(user_id: int, chat_id: int, thread_id: Optional[int] = None) -> None:
    if not store.is_registered(user_id):
        send_telegram_message(chat_id, "Сначала зарегистрируйся через /start в личке с ботом.", message_thread_id=thread_id)
        return

    u = store.get_user(user_id) or {}
    balance = store.get_balance(user_id)
    rank, total = store.rank_of_user(user_id)

    text = f"""👤 <b>Профиль</b>

<b>Имя:</b> {html_escape(u.get("first_name",""))} {html_escape(u.get("last_name",""))}
<b>Юзернейм:</b> @{html_escape(u.get("username","нет"))}
<b>Рейтинг:</b> #{rank} из {total}

<b>Статистика:</b>
• Ссылок: {u.get("articles_count",0)}
• Фидбеков: {u.get("feedback_given",0)}
• Игр: {u.get("games_played",0)}
• Баланс: {balance} 🪙
"""
    send_telegram_message(chat_id, text, message_thread_id=thread_id)

def show_rules(chat_id: int, thread_id: Optional[int] = None) -> None:
    text = f"""📜 <b>Правила клуба</b>

<b>Принципы:</b>
1) Качество важнее количества
2) Взаимность: получил поддержку - поддержи
3) Уважение к авторам
4) Реальные отзывы

<b>Очередь:</b>
• 1 ссылка раз в 48-72 часа
• 1 активная ссылка на человека
• Очередь до 10 ссылок
• Лист чтения в 19:00 МСК

<b>Ссылки принимаем только:</b>
{ALLOWED_PLATFORMS_TEXT}

<b>Фидбек:</b>
• Конструктивно
• "Норм" не считается фидбеком
"""
    send_telegram_message(chat_id, text, message_thread_id=thread_id)

def show_top(chat_id: int, thread_id: Optional[int] = None) -> None:
    top = store.top_users(10)
    if not top:
        send_telegram_message(chat_id, "Пока никого нет в топе. Стань первым.", message_thread_id=thread_id)
        return

    medals = ["🥇","🥈","🥉","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣","🔟"]
    lines = ["🏆 <b>Топ участников</b>\n"]
    for i, row in enumerate(top):
        name = f"@{row['username']}" if row.get("username") else (row.get("first_name","") or f"пользователь {row['id']}")
        lines.append(f"{medals[i]} <b>{html_escape(name)}</b> - {row['balance']} 🪙 (ссылок: {row['articles_count']})")
    send_telegram_message(chat_id, "\n".join(lines), message_thread_id=thread_id)

def show_queue(chat_id: int, thread_id: Optional[int] = None) -> None:
    q = store.list_queue(10)
    if not q:
        send_telegram_message(chat_id, "📭 <b>Очередь</b>\n\nПусто.", message_thread_id=thread_id)
        return

    lines = ["📋 <b>Очередь публикаций</b>\n"]
    for i, a in enumerate(q, 1):
        author = safe_username(int(a["user_id"]))
        url = a["url"]
        lines.append(f"{i}. 👤 <b>{html_escape(author)}</b>\n   🔗 <a href=\"{url}\">Открыть</a>")
    lines.append(f"\n<b>Всего:</b> {store.queue_count()} из 10")
    send_telegram_message(chat_id, "\n".join(lines), message_thread_id=thread_id)

def give_daily_reward(user_id: int) -> None:
    today = datetime.now().date().isoformat()
    if store.get_daily_reward_date(user_id) == today:
        send_telegram_message(user_id, "⏳ Ты уже получал ежедневку сегодня.")
        return
    reward = 5
    bal = store.add_quotes(user_id, reward, "Ежедневная награда")
    store.set_daily_reward_date(user_id, today)
    send_telegram_message(user_id, f"🎁 +{reward} 🪙\nНовый баланс: {bal}")

def start_article_submission(user_id: int) -> None:
    ok, msg = can_submit_article(user_id)
    if not ok:
        send_telegram_message(user_id, f"⏳ {html_escape(msg)}")
        return
    store.set_state(user_id, "awaiting_link")
    send_telegram_message(
        user_id,
        f"""✍️ <b>Подача статьи</b>

Просто отправь ссылку одним сообщением.

<b>Важно:</b>
• Только: {ALLOWED_PLATFORMS_TEXT}
• Ссылка начинается с https://
• Лимит: 1 ссылка раз в 48-72 часа
"""
    )

def show_my_posts(user_id: int) -> None:
    posts = store.list_user_submissions(user_id, 10)
    if not posts:
        send_telegram_message(user_id, "У тебя пока нет поданных ссылок.")
        return
    lines = ["🗂 <b>Твои ссылки</b>\n"]
    for i, p in enumerate(posts, 1):
        ts = (p.get("submitted_at","") or "")[:19].replace("T", " ")
        url = p.get("url","")
        st = p.get("status","")
        lines.append(f"{i}. {ts} ({html_escape(st)})\n🔗 <a href=\"{url}\">Открыть</a>")
    send_telegram_message(user_id, "\n".join(lines))

# =========================
# ИГРЫ: Дуэль абзацев (с сохранением)
# =========================

DUEL_TOPICS = [
    "Утро после конца света",
    "Разговор с зеркалом",
    "Письмо из прошлого",
    "Тайна старой библиотеки",
    "Последний день лета"
]

def show_games_menu(chat_id: int, thread_id: Optional[int] = None) -> None:
    text = """🎮 <b>Игры дня</b>

<b>⚔️ Дуэль абзацев</b>
• 3-5 предложений на тему
• 15 минут на текст
• 10 минут голосование
• Приз: 25 🪙

Команды:
/game - меню
/duel - старт дуэли (в группе)
"""
    kb = {"inline_keyboard": [[{"text": "⚔️ Начать дуэль", "callback_data": "start_duel"}]]}
    send_telegram_message(chat_id, text, reply_markup=kb, message_thread_id=thread_id)

def start_duel_in_group(initiator_id: int, thread_id: Optional[int]) -> None:
    topic = DUEL_TOPICS[int(time.time()) % len(DUEL_TOPICS)]
    duel_id = f"duel_{int(time.time())}_{initiator_id}"
    prize = 25
    deadline = datetime.utcnow() + timedelta(minutes=15)

    text = f"""⚔️ <b>Дуэль абзацев началась!</b>

<b>Тема:</b> {html_escape(topic)}
<b>Инициатор:</b> {html_escape(safe_username(initiator_id))}
<b>Приз:</b> {prize} 🪙

<b>Правила:</b>
1) 3-5 предложений на тему
2) Время: 15 минут
3) Отправь текст ответом на это сообщение
"""
    resp = send_telegram_message(GROUP_ID, text, message_thread_id=thread_id)
    msg_id = None
    if resp and resp.get("ok"):
        msg_id = resp["result"]["message_id"]

    store.create_duel(
        duel_id=duel_id,
        topic=topic,
        initiator=initiator_id,
        prize=prize,
        thread_id=thread_id,
        announce_message_id=msg_id,
        submissions_deadline=deadline
    )

def duel_load_json(duel: Dict[str, Any]) -> Tuple[List[int], Dict[str, str], Dict[str, int]]:
    try:
        participants = json.loads(duel.get("participants_json") or "[]")
        paragraphs = json.loads(duel.get("paragraphs_json") or "{}")
        votes = json.loads(duel.get("votes_json") or "{}")
        if not isinstance(participants, list):
            participants = []
        if not isinstance(paragraphs, dict):
            paragraphs = {}
        if not isinstance(votes, dict):
            votes = {}
        return participants, paragraphs, votes
    except Exception:
        return [], {}, {}

def duel_accept_paragraph(user_id: int, text: str) -> None:
    duel = store.get_active_duel_waiting()
    if not duel:
        return

    participants, paragraphs, votes = duel_load_json(duel)

    uid = int(user_id)
    if str(uid) in paragraphs:
        return

    paragraphs[str(uid)] = text.strip()
    participants.append(uid)
    store.update_duel_json_fields(duel["duel_id"], participants, paragraphs, votes)

def duel_accept_vote(voter_id: int, vote_index: int) -> None:
    duel = store.get_active_duel_voting()
    if not duel:
        return

    participants, paragraphs, votes = duel_load_json(duel)
    vid = int(voter_id)

    if str(vid) in votes:
        return

    if 1 <= vote_index <= len(participants):
        votes[str(vid)] = int(vote_index)
        store.update_duel_json_fields(duel["duel_id"], participants, paragraphs, votes)

def duel_finish_submissions(duel: Dict[str, Any]) -> None:
    participants, paragraphs, votes = duel_load_json(duel)
    thread_id = duel.get("thread_id")

    if len(paragraphs) < 2:
        store.set_duel_status(duel["duel_id"], "cancelled")
        send_telegram_message(GROUP_ID, "⚔️ Дуэль отменена: недостаточно участников.", message_thread_id=thread_id)
        return

    lines = [f"🗳 <b>Голосование в дуэли</b>\n\n<b>Тема:</b> {html_escape(duel['topic'])}\n<b>Участников:</b> {len(participants)}\n"]
    for i, uid in enumerate(participants, 1):
        username = html_escape(safe_username(uid))
        snippet = html_escape((paragraphs.get(str(uid), "")[:240]).strip())
        lines.append(f"\n<b>#{i} - {username}</b>\n{snippet}\n")

    lines.append("\nОтветь числом (1, 2, 3...) на это сообщение. Время: 10 минут.")
    resp = send_telegram_message(GROUP_ID, "\n".join(lines), message_thread_id=thread_id)
    vote_msg_id = None
    if resp and resp.get("ok"):
        vote_msg_id = resp["result"]["message_id"]

    vote_deadline = datetime.utcnow() + timedelta(minutes=10)
    store.set_duel_voting(duel["duel_id"], vote_msg_id or 0, vote_deadline)

def duel_finish_voting(duel: Dict[str, Any]) -> None:
    participants, paragraphs, votes = duel_load_json(duel)
    thread_id = duel.get("thread_id")

    if not votes:
        store.set_duel_status(duel["duel_id"], "finished")
        send_telegram_message(GROUP_ID, "⚔️ Дуэль завершена: никто не проголосовал.", message_thread_id=thread_id)
        return

    counts = defaultdict(int)
    for v in votes.values():
        try:
            counts[int(v)] += 1
        except Exception:
            pass

    winner_index = max(counts.items(), key=lambda x: x[1])[0]
    winner_id = None
    if 1 <= winner_index <= len(participants):
        winner_id = participants[winner_index - 1]

    store.set_duel_winner(duel["duel_id"], winner_id)
    store.set_duel_status(duel["duel_id"], "finished")

    if winner_id:
        store.add_quotes(winner_id, int(duel["prize"]), "Победа в дуэли")
        store.add_game_history("duel", {
            "topic": duel["topic"],
            "winner": winner_id,
            "votes": votes,
            "participants": participants
        })
        send_telegram_message(
            GROUP_ID,
            f"🏆 <b>Дуэль завершена!</b>\n\n<b>Победитель:</b> {html_escape(safe_username(winner_id))}\n<b>Тема:</b> {html_escape(duel['topic'])}\n<b>Приз:</b> {duel['prize']} 🪙",
            message_thread_id=thread_id
        )

# =========================
# ЛИСТ ЧТЕНИЯ
# =========================

def publish_reading_list(thread_id: Optional[int]) -> None:
    items = store.pop_from_queue(5)
    if not items:
        send_telegram_message(GROUP_ID, "📭 <b>Лист чтения</b>\n\nОчередь пустая.", message_thread_id=thread_id)
        return

    list_date = datetime.now().strftime("%d.%m.%Y")
    lines = [f"📚 <b>Лист чтения на {list_date}</b>\n"]

    for i, a in enumerate(items, 1):
        author = html_escape(safe_username(int(a["user_id"])))
        url = a["url"]
        lines.append(f"<b>{i})</b> 👤 <i>{author}</i>\n🔗 <a href=\"{url}\">Открыть</a>\n")
        store.add_published(a, list_date)
        store.add_quotes(int(a["user_id"]), 15, "Ссылка попала в лист чтения")

    lines.append(
        "<b>🎯 Задание:</b>\n"
        "1) Прочитай минимум 1 ссылку\n"
        "2) Оставь конструктивный фидбек\n"
        "3) Получи кавычки за активность\n\n"
        "<b>⏰ Фидбек до 23:59 МСК</b>"
    )
    send_telegram_message(GROUP_ID, "\n".join(lines), message_thread_id=thread_id)

# =========================
# ОБРАБОТКА UPDATES
# =========================

def process_message(message: dict) -> None:
    chat_id = int(message["chat"]["id"])
    user_id = int(message["from"]["id"])
    text = message.get("text", "") or ""
    thread_id = message.get("message_thread_id")
    message_id = message.get("message_id")

    if store.is_registered(user_id):
        store.set_last_active(user_id)

    # reply-handling for duels in group
    if chat_id == GROUP_ID and "reply_to_message" in message:
        reply_to = message["reply_to_message"]
        reply_mid = reply_to.get("message_id")
        waiting = store.get_active_duel_waiting()
        voting = store.get_active_duel_voting()

        if waiting and reply_mid and int(reply_mid) == int(waiting.get("announce_message_id") or 0):
            if store.is_registered(user_id) and text.strip():
                duel_accept_paragraph(user_id, text)
            return

        if voting and reply_mid and int(reply_mid) == int(voting.get("vote_message_id") or 0):
            try:
                vote = int(text.strip())
            except Exception:
                return
            if store.is_registered(user_id):
                duel_accept_vote(user_id, vote)
            return

    # commands
    if text.startswith("/"):
        cmd = normalize_command(text)

        if cmd == "/start":
            user_data = {
                "id": user_id,
                "username": message["from"].get("username"),
                "first_name": message["from"].get("first_name", ""),
                "last_name": message["from"].get("last_name", "")
            }

            # /start в группе: объясняем, что нужна личка
            if chat_id != user_id:
                link = f"https://t.me/{BOT_USERNAME}" if BOT_USERNAME else "(BOT_USERNAME не задан)"
                msg = (
                    "Регистрация делается в личке с ботом.\n"
                    "Открой чат с ботом и нажми Start.\n"
                    f"Ссылка: {link}"
                )
                send_telegram_message(chat_id, msg, parse_mode=None, message_thread_id=thread_id, reply_to_message_id=message_id)
                return

            register_user(user_data)
            return

        if cmd == "/help":
            show_help(chat_id, thread_id=thread_id if chat_id == GROUP_ID else None)
            return

        # дальше нужна регистрация
        if not store.is_registered(user_id):
            send_telegram_message(chat_id, "Сначала зарегистрируйся через /start в личке с ботом.", message_thread_id=thread_id)
            return

        if cmd == "/profile":
            show_profile(user_id, chat_id, thread_id=thread_id if chat_id == GROUP_ID else None)
            return

        if cmd == "/balance":
            bal = store.get_balance(user_id)
            send_telegram_message(chat_id, f"💰 <b>Твой баланс:</b> {bal} 🪙", message_thread_id=thread_id if chat_id == GROUP_ID else None)
            return

        if cmd == "/daily":
            give_daily_reward(user_id)
            return

        if cmd == "/submit":
            if chat_id == user_id:
                start_article_submission(user_id)
            else:
                send_telegram_message(chat_id, "Подача ссылки доступна только в личных сообщениях с ботом.", message_thread_id=thread_id)
            return

        if cmd == "/my_posts":
            if chat_id == user_id:
                show_my_posts(user_id)
            else:
                send_telegram_message(chat_id, "Список твоих ссылок смотри в личке: /my_posts", message_thread_id=thread_id)
            return

        if cmd == "/rules":
            show_rules(chat_id, thread_id=thread_id if chat_id == GROUP_ID else None)
            return

        if cmd == "/queue":
            if chat_id == GROUP_ID or chat_id == user_id:
                out_thread = choose_thread_id(thread_id if chat_id == GROUP_ID else None, TOPIC_QUEUE_ID if chat_id == GROUP_ID else 0)
                show_queue(chat_id, thread_id=out_thread)
            else:
                send_telegram_message(chat_id, "Очередь смотри в группе или в личке с ботом.", message_thread_id=thread_id)
            return

        if cmd == "/top":
            if chat_id == GROUP_ID or chat_id == user_id:
                show_top(chat_id, thread_id=thread_id if chat_id == GROUP_ID else None)
            else:
                send_telegram_message(chat_id, "Топ смотри в группе или в личке.", message_thread_id=thread_id)
            return

        if cmd == "/game":
            if chat_id == GROUP_ID or chat_id == user_id:
                show_games_menu(chat_id, thread_id=thread_id if chat_id == GROUP_ID else None)
            else:
                send_telegram_message(chat_id, "Игры доступны в группе.", message_thread_id=thread_id)
            return

        if cmd == "/duel":
            if chat_id == GROUP_ID:
                start_duel_in_group(user_id, thread_id=thread_id)
            else:
                send_telegram_message(chat_id, "Дуэли доступны только в группе.", message_thread_id=thread_id)
            return

        # admin
        if cmd == "/publish_reading_list" and user_id in ADMIN_IDS:
            out_thread = choose_thread_id(thread_id if chat_id == GROUP_ID else None, TOPIC_QUEUE_ID if chat_id == GROUP_ID else 0)
            publish_reading_list(out_thread)
            return

        send_telegram_message(chat_id, "Неизвестная команда. Напиши /help.", message_thread_id=thread_id if chat_id == GROUP_ID else None)
        return

    # state handling (private chat)
    if chat_id == user_id and store.is_registered(user_id):
        state = store.get_state(user_id)
        if state == "awaiting_link":
            url = extract_first_url(text)
            if not url:
                send_telegram_message(user_id, "Не вижу ссылку. Просто отправь https://... одним сообщением.")
                return

            if not is_allowed_article_url(url):
                send_telegram_message(
                    user_id,
                    f"Ссылка недопустима.\nПринимаем только: {ALLOWED_PLATFORMS_TEXT}\nПроверь, что это https:// и разрешенный сайт.",
                    parse_mode=None
                )
                return

            ok, msg = can_submit_article(user_id)
            if not ok:
                send_telegram_message(user_id, msg, parse_mode=None)
                store.clear_state(user_id)
                return

            article_id = store.add_submission_and_queue(user_id, url)
            store.add_quotes(user_id, 10, "Подача ссылки")

            notify_thread = choose_thread_id(None, TOPIC_QUEUE_ID)
            send_telegram_message(
                GROUP_ID,
                f"📝 <b>Новая ссылка в очереди!</b>\n\n<b>Автор:</b> {html_escape(safe_username(user_id))}\n🔗 <a href=\"{url}\">Открыть</a>\n\nОчередь: /queue",
                message_thread_id=notify_thread
            )

            send_telegram_message(
                user_id,
                f"✅ <b>Ссылка добавлена в очередь!</b>\n\n<b>ID:</b> {html_escape(article_id)}\n<b>Позиция:</b> {store.queue_count()}",
            )

            store.clear_state(user_id)
            return

        # not state
        if text.strip():
            send_telegram_message(user_id, "Напиши /help или /submit, чтобы подать ссылку.")

def handle_callback(callback: dict) -> None:
    callback_id = callback["id"]
    user_id = int(callback["from"]["id"])
    data = callback.get("data", "")

    cb_msg = callback.get("message", {}) or {}
    cb_chat = int((cb_msg.get("chat", {}) or {}).get("id", 0))
    cb_thread = cb_msg.get("message_thread_id")

    if data == "start_duel":
        if not store.is_registered(user_id):
            answer_callback(callback_id, "Сначала зарегистрируйся через /start в личке.", show_alert=True)
            return
        if cb_chat == GROUP_ID:
            start_duel_in_group(user_id, thread_id=cb_thread)
            answer_callback(callback_id, "Дуэль запущена.")
        else:
            answer_callback(callback_id, "Дуэль запускается в группе.")
        return

    answer_callback(callback_id, "Пока не работает.")

# =========================
# ФОН: задачи и дедлайны дуэлей
# =========================

def background_loop():
    while True:
        try:
            now_utc = datetime.utcnow()

            # Лист чтения: 19:00 МСК = 16:00 UTC
            key_publish = "last_publish_date_utc"
            today_utc = now_utc.date().isoformat()
            if now_utc.hour == 16 and now_utc.minute == 0:
                last = store.get_meta(key_publish)
                if last != today_utc:
                    if store.queue_count() > 0:
                        publish_reading_list(choose_thread_id(None, TOPIC_QUEUE_ID))
                    store.set_meta(key_publish, today_utc)

            # Сброс published: 00:00 МСК = 21:00 UTC (условно)
            key_reset = "last_reset_date_utc"
            if now_utc.hour == 21 and now_utc.minute == 0:
                last = store.get_meta(key_reset)
                if last != today_utc:
                    store.set_meta(key_reset, today_utc)

            # Напоминания о возможности подать
            # простой проход по всем пользователям с last_submit_at
            users_rows = store._query_all("SELECT user_id, last_submit_at, submit_notified_at FROM user_state WHERE last_submit_at IS NOT NULL")
            now_local = datetime.now()
            for r in users_rows:
                uid = int(r["user_id"])
                try:
                    last_submit = datetime.fromisoformat(r["last_submit_at"])
                except Exception:
                    continue
                hours = (now_local - last_submit).total_seconds() / 3600
                if hours >= 48 and not store.queue_has_user(uid):
                    last_notified = None
                    if r["submit_notified_at"]:
                        try:
                            last_notified = datetime.fromisoformat(r["submit_notified_at"])
                        except Exception:
                            last_notified = None
                    if (not last_notified) or ((now_local - last_notified).total_seconds() > 3600):
                        resp = send_telegram_message(uid, "🔔 Можно подать новую ссылку. Используй /submit")
                        if resp and resp.get("ok"):
                            store.set_submit_notified_at(uid, now_local)

            # Дуэли: закрыть прием/голосование по дедлайнам
            waiting_due, voting_due = store.list_duels_due(now_utc)

            for d in waiting_due:
                duel_finish_submissions(d)

            for d in voting_due:
                duel_finish_voting(d)

        except Exception as e:
            logger.error(f"background_loop error: {e}", exc_info=True)

        time.sleep(20)

threading.Thread(target=background_loop, daemon=True).start()

# =========================
# FLASK ROUTES
# =========================

@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        data = request.get_json(force=True, silent=True) or {}
        logger.info(f"webhook keys: {list(data.keys())}")

        if "message" in data:
            process_message(data["message"])
        elif "callback_query" in data:
            handle_callback(data["callback_query"])

        return jsonify({"status": "ok"}), 200
    except Exception as e:
        logger.error(f"webhook error: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500

@app.route("/health", methods=["GET"])
def health():
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "db_path": DB_PATH,
        "users": store._query_one("SELECT COUNT(*) AS c FROM users")["c"],
        "queue": store.queue_count(),
        "version": "3.0-sqlite"
    }), 200

@app.route("/", methods=["GET"])
def home():
    return (
        "<h1>ClubBot</h1>"
        "<p>Status: OK</p>"
        f"<p>Users: {store._query_one('SELECT COUNT(*) AS c FROM users')['c']}</p>"
        f"<p>Queue: {store.queue_count()}</p>"
        "<p><a href='/health'>Health</a></p>"
    )

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)

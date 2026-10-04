import os
import re
import io
import time
import shutil
import asyncio
import sqlite3
import logging
import qrcode
import threading
import functools
from datetime import datetime

try:
    asyncio.get_event_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

from pyrogram import Client, filters, enums
from pyrogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton,
    Message, CallbackQuery
)
from pyrogram.raw import functions, types as raw_types
from pyrogram.errors import (
    FloodWait, SessionPasswordNeeded,
    PhoneCodeInvalid, PhoneCodeExpired,
    PasswordHashInvalid, PhoneNumberInvalid,
    UserNotParticipant, AuthKeyUnregistered,
    UserDeactivated, UserDeactivatedBan,
    MessageNotModified
)

from fampay_gmail import verify_gmail_payment

API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
LOG_CHANNEL = os.getenv("LOG_CHANNEL", "@felixlogs")
UPI_ID = os.getenv("UPI_ID", "")
DEFAULT_2FA = os.getenv("DEFAULT_2FA", "Felix@4545")

if not (API_ID and API_HASH and BOT_TOKEN and ADMIN_ID and UPI_ID):
    raise SystemExit(
        "Missing required environment variables. Set API_ID, API_HASH, BOT_TOKEN, ADMIN_ID and UPI_ID "
        "in Railway's Variables tab (see .env.example)."
    )
DEFAULT_NAME = os.getenv("DEFAULT_NAME", "Felix Store")
SUPPORT_USER = os.getenv("SUPPORT_USER", "@rxexa")
BOT_NAME = os.getenv("BOT_NAME", "Felix Store")
WELCOME_IMG = os.getenv("WELCOME_IMG", "https://www.image2url.com/r2/default/images/1790616463704-669a0260-6dd5-478b-807e-9b0a61da9d3a.jpg")

FAMPAY_GMAIL = os.getenv("FAMPAY_GMAIL", "")
FAMPAY_GMAIL_APP_PASSWORD = os.getenv("FAMPAY_GMAIL_APP_PASSWORD", "")
FAMPAY_AUTO_VERIFY = bool(FAMPAY_GMAIL and FAMPAY_GMAIL_APP_PASSWORD)

SESSIONS_DIR = os.getenv("SESSIONS_DIR", "session")
SOLD_DIR = os.getenv("SOLD_DIR", "sold_session")
PENDING_DIR = os.getenv("PENDING_DIR", "pending_session")
DB_PATH = os.getenv("DB_PATH", "felixstore.db")

os.makedirs(SESSIONS_DIR, exist_ok=True)
os.makedirs(SOLD_DIR, exist_ok=True)
os.makedirs(PENDING_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("FelixStore")

user_state = {}
_otp_cooldowns = {}
_otp_locks = {}


def get_otp_lock(key) -> asyncio.Lock:
    lock = _otp_locks.get(key)
    if lock is None:
        lock = asyncio.Lock()
        _otp_locks[key] = lock
    return lock

def now_iso() -> str:
    return datetime.now().isoformat()


def row_value(row, key, default=None):
    if row is None:
        return default
    try:
        return row[key]
    except Exception:
        return default


def normalize_phone(text: str) -> str:
    phone = (text or "").strip().replace(" ", "").replace("-", "")
    if phone and not phone.startswith("+"):
        phone = "+" + phone
    return phone


def is_valid_phone(phone: str) -> bool:
    return bool(re.match(r"^\+\d{7,15}$", phone or ""))


def is_valid_upi(upi_id: str) -> bool:
    return bool(re.match(r"^[a-zA-Z0-9.\-_]{2,}@[a-zA-Z0-9.\-_]{2,}$", upi_id or ""))


def build_session_client(base_path: str, no_updates: bool = True) -> Client:
    abs_base = os.path.abspath(base_path)
    workdir = os.path.dirname(abs_base)
    session_name = os.path.basename(abs_base)
    return Client(
        session_name,
        api_id=API_ID,
        api_hash=API_HASH,
        no_updates=no_updates,
        workdir=workdir
    )


def unique_session_dest(country: str, phone: str) -> str:
    country_dir = os.path.join(SESSIONS_DIR, country)
    os.makedirs(country_dir, exist_ok=True)
    clean_phone = phone.lstrip("+")
    base = os.path.join(country_dir, clean_phone)
    if not os.path.exists(base + ".session"):
        return base
    idx = 1
    while os.path.exists(f"{base}_{idx}.session"):
        idx += 1
    return f"{base}_{idx}"


def move_session_bundle(src_base: str, dest_base: str) -> str:
    src_session = src_base + ".session"
    dest_session = dest_base + ".session"
    if not os.path.exists(src_session):
        raise FileNotFoundError(f"Missing session file: {src_session}")
    shutil.move(src_session, dest_session)
    for ext in (".session-journal", ".session-wal", ".session-shm"):
        src_side = src_base + ext
        dest_side = dest_base + ext
        if os.path.exists(src_side):
            try:
                shutil.move(src_side, dest_side)
            except Exception:
                try:
                    os.remove(src_side)
                except Exception:
                    pass
    return dest_session


def move_session(src_base: str, country: str, phone: str) -> str:
    dest_base = unique_session_dest(country, phone)
    return move_session_bundle(src_base, dest_base)


def move_existing_session_file(src_session_path: str, country: str, phone: str) -> str:
    src_base = src_session_path.replace(".session", "")
    return move_session(src_base, country, phone)


def clean_session_files(base_path: str):
    for ext in (".session", ".session-journal", ".session-wal", ".session-shm"):
        target = base_path + ext
        if os.path.exists(target):
            try:
                os.remove(target)
            except Exception as e:
                logger.warning(f"Could not remove {target}: {e}")


_db_lock = threading.Lock()


_db_conn = None
def get_db() -> sqlite3.Connection:
    global _db_conn
    if _db_conn is None:
        _db_conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30)
        _db_conn.execute("PRAGMA journal_mode=WAL")
        _db_conn.execute("PRAGMA busy_timeout=30000")
        _db_conn.execute("PRAGMA synchronous=NORMAL")
        _db_conn.execute("PRAGMA cache_size=5000")
        _db_conn.execute("PRAGMA temp_store=MEMORY")
        _db_conn.row_factory = sqlite3.Row
    return _db_conn


def db_execute(query: str, params: tuple = (), fetch: str = "none"):
    with _db_lock:
        conn = get_db()
        try:
            cur = conn.cursor()
            cur.execute(query, params)
            conn.commit()
            if fetch == "one":
                return cur.fetchone()
            if fetch == "all":
                return cur.fetchall()
            if fetch == "lastid":
                return cur.lastrowid
            if fetch == "rowcount":
                return cur.rowcount
            return None
        except Exception as e:
            logger.error(f"DB Error: {e} | Query: {query[:200]}")
            try:
                conn.rollback()
            except Exception:
                pass
            return None


def db_execute_many(queries):
    with _db_lock:
        conn = get_db()
        try:
            cur = conn.cursor()
            for query, params in queries:
                cur.execute(query, params)
            conn.commit()
        except Exception as e:
            logger.error(f"DB Multi Error: {e}")
            try:
                conn.rollback()
            except Exception:
                pass


def init_db():
    with _db_lock:
        conn = get_db()
        cur = conn.cursor()
        cur.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            id                INTEGER PRIMARY KEY,
            name              TEXT    DEFAULT '',
            username          TEXT    DEFAULT '',
            balance           REAL    DEFAULT 0,
            total_spent       REAL    DEFAULT 0,
            total_deposited   REAL    DEFAULT 0,
            total_sold        INTEGER DEFAULT 0,
            joined_at         TEXT    DEFAULT '',
            is_banned         INTEGER DEFAULT 0,
            ban_reason        TEXT    DEFAULT '',
            last_active       TEXT    DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS orders (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id      INTEGER,
            session_name TEXT,
            status       TEXT    DEFAULT 'active',
            country      TEXT,
            price        REAL,
            timestamp    TEXT,
            password     TEXT    DEFAULT 'Felix@2025',
            last_otp     TEXT    DEFAULT '',
            phone        TEXT    DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS sell_orders (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id          INTEGER,
            phone_number     TEXT,
            country          TEXT    DEFAULT '',
            status           TEXT    DEFAULT 'pending',
            sell_price       REAL    DEFAULT 0,
            timestamp        TEXT,
            session_path     TEXT    DEFAULT '',
            spam_result      TEXT    DEFAULT '',
            rejection_reason TEXT    DEFAULT '',
            upi_id           TEXT    DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS deposit_requests (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id    INTEGER,
            message_id INTEGER DEFAULT 0,
            status     TEXT    DEFAULT 'pending',
            amount     REAL    DEFAULT 0,
            timestamp  TEXT
        );

        CREATE TABLE IF NOT EXISTS settings (
            key   TEXT PRIMARY KEY,
            value TEXT
        );

        CREATE TABLE IF NOT EXISTS country_buy_prices (
            country TEXT PRIMARY KEY,
            price   REAL
        );

        CREATE TABLE IF NOT EXISTS country_sell_prices (
            country    TEXT PRIMARY KEY,
            sell_price REAL
        );

        CREATE TABLE IF NOT EXISTS business_stats (
            key   TEXT PRIMARY KEY,
            value REAL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS force_join (
            id       INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE,
            added_at TEXT
        );

        CREATE TABLE IF NOT EXISTS admin_logs (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            action    TEXT,
            details   TEXT    DEFAULT '',
            timestamp TEXT
        );

        CREATE TABLE IF NOT EXISTS referrals (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            referrer_id INTEGER,
            referred_id INTEGER,
            rewarded    INTEGER DEFAULT 0,
            timestamp   TEXT
        );

        CREATE TABLE IF NOT EXISTS custom_prices (
            session_file TEXT PRIMARY KEY,
            country      TEXT,
            price        REAL,
            set_by       INTEGER,
            timestamp    TEXT
        );

        CREATE TABLE IF NOT EXISTS account_notes (
            session_file TEXT PRIMARY KEY,
            note         TEXT DEFAULT '',
            added_by     INTEGER,
            timestamp    TEXT
        );

        CREATE TABLE IF NOT EXISTS promo_codes (
            code      TEXT PRIMARY KEY,
            discount  REAL    DEFAULT 0,
            max_uses  INTEGER DEFAULT 1,
            used      INTEGER DEFAULT 0,
            active    INTEGER DEFAULT 1,
            created   TEXT
        );

        CREATE TABLE IF NOT EXISTS promo_usage (
            id      INTEGER PRIMARY KEY AUTOINCREMENT,
            code    TEXT,
            user_id INTEGER,
            used_at TEXT
        );
        """)
        try:
            cur.execute("ALTER TABLE sell_orders ADD COLUMN upi_id TEXT DEFAULT ''")
        except Exception:
            pass
        try:
            cur.execute("ALTER TABLE sell_orders ADD COLUMN old_2fa TEXT DEFAULT ''")
        except Exception:
            pass
        try:
            cur.execute("ALTER TABLE sell_orders ADD COLUMN new_2fa TEXT DEFAULT ''")
        except Exception:
            pass

        defaults = [
            ("default_buy_price", "50"),
            ("default_sell_price", "30"),
            ("sell_feature", "on"),
            ("buy_feature", "on"),
            ("maintenance_mode", "off"),
            ("accept_spam_accounts", "on"),
            ("min_deposit", "10"),
            ("max_deposit", "50000"),
            ("referral_bonus", "10"),
            ("welcome_message", "Buy & sell premium Telegram accounts.\nFast • Safe • Reliable"),
            ("otp_cooldown", "10"),
            ("max_active_orders", "1"),
            ("auto_cleanup", "off"),
        ]
        for key, value in defaults:
            cur.execute("INSERT OR IGNORE INTO settings(key,value) VALUES(?,?)", (key, value))

        for key in (
            "total_sold", "total_revenue", "total_deposited",
            "total_bought_from_users", "total_users", "total_rejected",
            "total_deposits_approved", "total_promo_used",
        ):
            cur.execute("INSERT OR IGNORE INTO business_stats(key,value) VALUES(?,0)", (key,))
        conn.commit()


init_db()

def register_user(user):
    existing = db_execute("SELECT id FROM users WHERE id=?", (user.id,), fetch="one")
    if existing:
        db_execute(
            "UPDATE users SET name=?,username=?,last_active=? WHERE id=?",
            (user.first_name or "", user.username or "", now_iso(), user.id)
        )
        return
    db_execute_many([
        ("INSERT INTO users(id,name,username,joined_at,last_active) VALUES(?,?,?,?,?)",
         (user.id, user.first_name or "", user.username or "", now_iso(), now_iso())),
        ("UPDATE business_stats SET value=value+1 WHERE key='total_users'", ()),
    ])


def get_user(uid: int):
    return db_execute("SELECT * FROM users WHERE id=?", (uid,), fetch="one")


def get_setting(key: str, default=None):
    row = db_execute("SELECT value FROM settings WHERE key=?", (key,), fetch="one")
    return row["value"] if row else default


def set_setting(key: str, value):
    db_execute("INSERT OR REPLACE INTO settings(key,value) VALUES(?,?)", (key, str(value)))


def get_buy_price(country: str) -> float:
    row = db_execute("SELECT price FROM country_buy_prices WHERE country=?", (country,), fetch="one")
    return float(row["price"]) if row else float(get_setting("default_buy_price", "50"))


def get_buy_price_for_session(session_file: str, country: str) -> float:
    row = db_execute("SELECT price FROM custom_prices WHERE session_file=?", (session_file,), fetch="one")
    if row:
        return float(row["price"])
    return get_buy_price(country)


def get_sell_price(country: str = "default") -> float:
    row = db_execute("SELECT sell_price FROM country_sell_prices WHERE country=?", (country,), fetch="one")
    return float(row["sell_price"]) if row else float(get_setting("default_sell_price", "30"))


def add_balance(uid: int, amount: float):
    if amount <= 0:
        logger.warning(f"add_balance called with invalid amount={amount} for uid={uid}")
        return
    db_execute_many([
        ("UPDATE users SET balance=balance+?, total_deposited=total_deposited+? WHERE id=?",
         (amount, amount, uid)),
        ("UPDATE business_stats SET value=value+? WHERE key='total_deposited'", (amount,)),
    ])


def generate_unique_deposit_amount(base_amount: float) -> float:
    import random
    base = round(float(base_amount), 0)
    for _ in range(50):
        candidate = round(base + random.randint(1, 98) / 100, 2)
        exists = db_execute(
            "SELECT id FROM deposit_requests WHERE status='pending' AND amount=?",
            (candidate,), fetch="one"
        )
        if not exists:
            return candidate
    return round(base + random.randint(1, 98) / 100, 2)


def credit_wallet(uid: int, amount: float):
    if amount <= 0:
        logger.warning(f"credit_wallet called with invalid amount={amount} for uid={uid}")
        return
    db_execute("UPDATE users SET balance=balance+? WHERE id=?", (amount, uid))


def deduct_balance(uid: int, amount: float):
    if amount <= 0:
        logger.warning(f"deduct_balance called with invalid amount={amount} for uid={uid}")
        return
    db_execute(
        "UPDATE users SET balance=CASE WHEN balance>=? THEN balance-? ELSE balance END, total_spent=total_spent+? WHERE id=?",
        (amount, amount, amount, uid)
    )


def get_active_order(uid: int):
    return db_execute(
        "SELECT * FROM orders WHERE user_id=? AND status='active' ORDER BY id DESC LIMIT 1",
        (uid,), fetch="one"
    )


def get_pending_sell(uid: int):
    return db_execute(
        "SELECT * FROM sell_orders WHERE user_id=? AND status='pending' ORDER BY id DESC LIMIT 1",
        (uid,), fetch="one"
    )


def create_buy_order(uid, session_name, country, price, phone, password=None) -> int:
    pwd = password or DEFAULT_2FA
    order_id = db_execute(
        "INSERT INTO orders(user_id,session_name,status,country,price,timestamp,password,phone) "
        "VALUES(?,?,'active',?,?,?,?,?)",
        (uid, session_name, country, price, now_iso(), pwd, phone),
        fetch="lastid"
    )
    db_execute_many([
        ("UPDATE business_stats SET value=value+1 WHERE key='total_sold'", ()),
        ("UPDATE business_stats SET value=value+? WHERE key='total_revenue'", (price,)),
    ])
    return order_id


def create_sell_order(uid, phone, session_path, spam_result, country="", old_2fa="", new_2fa="") -> int:
    return db_execute(
        "INSERT INTO sell_orders(user_id,phone_number,country,status,timestamp,session_path,spam_result,old_2fa,new_2fa) "
        "VALUES(?,?,?,'pending',?,?,?,?,?)",
        (uid, phone, country, now_iso(), session_path, spam_result, old_2fa, new_2fa),
        fetch="lastid"
    )


def get_all_users():
    return db_execute("SELECT * FROM users ORDER BY id DESC", fetch="all") or []


def get_user_orders(uid: int):
    return db_execute("SELECT * FROM orders WHERE user_id=? ORDER BY id DESC", (uid,), fetch="all") or []


def get_user_sell_orders(uid: int):
    return db_execute("SELECT * FROM sell_orders WHERE user_id=? ORDER BY id DESC", (uid,), fetch="all") or []


def get_pending_sell_orders():
    return db_execute("SELECT * FROM sell_orders WHERE status='pending' ORDER BY id ASC", fetch="all") or []


def get_stat(key: str) -> float:
    row = db_execute("SELECT value FROM business_stats WHERE key=?", (key,), fetch="one")
    return float(row["value"]) if row else 0.0


def is_banned(uid: int) -> bool:
    row = db_execute("SELECT is_banned FROM users WHERE id=?", (uid,), fetch="one")
    return bool(row["is_banned"]) if row else False


def ban_user(uid: int, reason: str = ""):
    db_execute("UPDATE users SET is_banned=1, ban_reason=? WHERE id=?", (reason, uid))


def unban_user(uid: int):
    db_execute("UPDATE users SET is_banned=0, ban_reason='' WHERE id=?", (uid,))


def log_action(action: str, details: str = ""):
    db_execute(
        "INSERT INTO admin_logs(action,details,timestamp) VALUES(?,?,?)",
        (action, details, now_iso())
    )


def get_force_join_channels():
    return db_execute("SELECT * FROM force_join", fetch="all") or []


def add_force_join(username: str):
    db_execute(
        "INSERT OR IGNORE INTO force_join(username,added_at) VALUES(?,?)",
        (username.lstrip("@").lower(), now_iso())
    )


def remove_force_join(username: str):
    db_execute("DELETE FROM force_join WHERE username=?", (username.lstrip("@").lower(),))


def set_custom_price(session_file: str, country: str, price: float, admin_id: int):
    db_execute(
        "INSERT OR REPLACE INTO custom_prices(session_file,country,price,set_by,timestamp) VALUES(?,?,?,?,?)",
        (session_file, country, price, admin_id, now_iso())
    )


def get_custom_price(session_file: str):
    return db_execute("SELECT * FROM custom_prices WHERE session_file=?", (session_file,), fetch="one")


def set_account_note(session_file: str, note: str, admin_id: int):
    db_execute(
        "INSERT OR REPLACE INTO account_notes(session_file,note,added_by,timestamp) VALUES(?,?,?,?)",
        (session_file, note, admin_id, now_iso())
    )


def get_account_note(session_file: str) -> str:
    row = db_execute("SELECT note FROM account_notes WHERE session_file=?", (session_file,), fetch="one")
    return row["note"] if row else ""


def create_promo(code: str, discount: float, max_uses: int):
    db_execute(
        "INSERT OR REPLACE INTO promo_codes(code,discount,max_uses,used,active,created) VALUES(?,?,?,0,1,?)",
        (code.upper(), discount, max_uses, now_iso())
    )


def get_promo(code: str):
    return db_execute(
        "SELECT * FROM promo_codes WHERE code=? AND active=1",
        (code.upper(),), fetch="one"
    )


def use_promo(code: str, uid: int):
    db_execute_many([
        ("UPDATE promo_codes SET used=used+1 WHERE code=?", (code.upper(),)),
        ("INSERT INTO promo_usage(code,user_id,used_at) VALUES(?,?,?)", (code.upper(), uid, now_iso())),
        ("UPDATE business_stats SET value=value+1 WHERE key='total_promo_used'", ()),
    ])


def has_used_promo(code: str, uid: int) -> bool:
    row = db_execute("SELECT id FROM promo_usage WHERE code=? AND user_id=?", (code.upper(), uid), fetch="one")
    return row is not None


def get_available_countries():
    result = {}
    if not os.path.exists(SESSIONS_DIR):
        return result
    for folder in sorted(os.listdir(SESSIONS_DIR)):
        path = os.path.join(SESSIONS_DIR, folder)
        if not os.path.isdir(path):
            continue
        sessions = [name for name in os.listdir(path) if name.endswith(".session")]
        if sessions:
            result[folder] = len(sessions)
    return result


def get_country_sessions(country: str):
    path = os.path.join(SESSIONS_DIR, country)
    if not os.path.exists(path):
        return []
    return sorted(name for name in os.listdir(path) if name.endswith(".session"))


def get_first_session(country: str):
    sessions = get_country_sessions(country)
    return sessions[0] if sessions else None


def make_qr(amount: float) -> io.BytesIO:
    upi = f"upi://pay?pa={UPI_ID}&pn=FelixStore&am={amount:.2f}&cu=INR&tn=FelixStore+Deposit"
    qr = qrcode.QRCode(
        version=1,
        error_correction=qrcode.constants.ERROR_CORRECT_H,
        box_size=10,
        border=4,
    )
    qr.add_data(upi)
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white")
    buf = io.BytesIO()
    buf.name = "payment_qr.png"
    img.save(buf, "PNG")
    buf.seek(0)
    return buf


def make_payout_qr(upi_id: str, amount: float) -> io.BytesIO:
    upi = f"upi://pay?pa={upi_id}&pn=FelixStorePayout&am={amount:.2f}&cu=INR&tn=FelixStore+Sell+Payment"
    qr = qrcode.QRCode(
        version=1,
        error_correction=qrcode.constants.ERROR_CORRECT_H,
        box_size=10,
        border=4,
    )
    qr.add_data(upi)
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white")
    buf = io.BytesIO()
    buf.name = "payout_qr.png"
    img.save(buf, "PNG")
    buf.seek(0)
    return buf


async def check_force_join(client, uid: int):
    missing = []
    left_statuses = set()
    for attr in ("BANNED", "LEFT", "KICKED"):
        value = getattr(enums.ChatMemberStatus, attr, None)
        if value is not None:
            left_statuses.add(value)
    for channel in get_force_join_channels():
        try:
            member = await client.get_chat_member(f"@{channel['username']}", uid)
            if member.status in left_statuses:
                missing.append(channel["username"])
        except UserNotParticipant:
            missing.append(channel["username"])
        except Exception:
            pass
    return missing


async def send_force_join_msg(client, message: Message, missing):
    buttons = [[InlineKeyboardButton(f"📢 Join @{ch}", url=f"https://t.me/{ch}")] for ch in missing]
    buttons.append([InlineKeyboardButton("✅ ɪ ᴊᴏɪɴᴇᴅ — ᴄʜᴇᴄᴋ ᴀɢᴀɪɴ", callback_data="fj_check")])
    await message.reply(
        f"⚠️ **ᴊᴏɪɴ ʀᴇǫᴜɪʀᴇᴅ!**\n\nTo use **{BOT_NAME}** you must join our channels first.\nJoin all channels below and click ✅",
        reply_markup=InlineKeyboardMarkup(buttons)
    )


async def guard(client, message: Message) -> bool:
    if not message.from_user:
        return False
    uid = message.from_user.id
    if uid == ADMIN_ID:
        return True
    if is_banned(uid):
        await message.reply(f"🚫 **ʏᴏᴜ ᴀʀᴇ ʙᴀɴɴᴇᴅ!**\nContact {SUPPORT_USER} to appeal.")
        return False
    if get_setting("maintenance_mode") == "on":
        await message.reply("🔧 **ʙᴏᴛ ɪꜱ ᴜɴᴅᴇʀ ᴍᴀɪɴᴛᴇɴᴀɴᴄᴇ.** Try later.")
        return False
    missing = await check_force_join(client, uid)
    if missing:
        await send_force_join_msg(client, message, missing)
        return False
    return True


_FLAGS = {
    "india": "🇮🇳", "usa": "🇺🇸", "uk": "🇬🇧", "russia": "🇷🇺", "germany": "🇩🇪",
    "france": "🇫🇷", "canada": "🇨🇦", "australia": "🇦🇺", "pakistan": "🇵🇰",
    "bangladesh": "🇧🇩", "nepal": "🇳🇵", "brazil": "🇧🇷", "china": "🇨🇳",
    "japan": "🇯🇵", "korea": "🇰🇷", "turkey": "🇹🇷", "uae": "🇦🇪",
    "indonesia": "🇮🇩", "spain": "🇪🇸", "italy": "🇮🇹", "malaysia": "🇲🇾",
    "thailand": "🇹🇭", "philippines": "🇵🇭", "vietnam": "🇻🇳", "egypt": "🇪🇬",
    "iran": "🇮🇷", "iraq": "🇮🇶", "saudi": "🇸🇦", "nigeria": "🇳🇬",
    "south africa": "🇿🇦", "mexico": "🇲🇽", "argentina": "🇦🇷",
}


def flag(country: str) -> str:
    return _FLAGS.get((country or "").lower(), "🌍")


async def safe_edit_text(message_obj, text, reply_markup=None):
    try:
        await message_obj.edit_text(text, reply_markup=reply_markup)
    except MessageNotModified:
        pass
    except Exception as e:
        logger.error(f"edit_text failed: {type(e).__name__}: {e}")


async def safe_stop(client_obj):
    if client_obj is None:
        return
    try:
        await client_obj.stop()
    except Exception:
        pass


async def safe_disconnect(client_obj):
    if client_obj is None:
        return
    try:
        await client_obj.disconnect()
    except Exception:
        pass
    try:
        if hasattr(client_obj, "storage") and client_obj.storage:
            await client_obj.storage.close()
    except Exception:
        pass


def main_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🐱 ʙᴜʏ ᴀᴄᴄᴏᴜɴᴛ", callback_data="menu_buy"),
         InlineKeyboardButton("💸 ꜱᴇʟʟ ᴀᴄᴄᴏᴜɴᴛ", callback_data="menu_sell")],
        [InlineKeyboardButton("👤 ᴍʏ ᴘʀᴏꜰɪʟᴇ", callback_data="menu_profile"),
         InlineKeyboardButton("💳 ᴀᴅᴅ ꜰᴜɴᴅꜱ", callback_data="menu_addfunds")],
        [InlineKeyboardButton("🆘 ꜱᴜᴘᴘᴏʀᴛ", callback_data="menu_support")]
    ])

OTP_SENT_TEXT = """🐱 **ᴏᴛᴘ ꜱᴇɴᴛ!** ✅

-------------------------------------
📩 Check your Telegram app.

⚠️ **ᴅᴏ ɴᴏᴛ ᴛʏᴘᴇ ᴛʜᴇ ᴄᴏᴅᴇ ɪɴ ᴄʜᴀᴛ**
(Telegram's security will expire it!)

👇 **ᴜꜱᴇ ᴛʜɪꜱ ᴅɪᴀʟᴘᴀᴅ ᴛᴏ ᴇɴᴛᴇʀ ɪᴛ:**
-------------------------------------"""

def otp_numpad_kb(entered_digits=""):
    kb = []
    display_text = f"📟 ᴇɴᴛᴇʀ ᴄᴏᴅᴇ: {entered_digits}" if entered_digits else "📟 Enter Code"
    kb.append([InlineKeyboardButton(display_text, callback_data="numpad_ignore")])
    kb.append([InlineKeyboardButton("1", callback_data="numpad_1"),
               InlineKeyboardButton("2", callback_data="numpad_2"),
               InlineKeyboardButton("3", callback_data="numpad_3")])
    kb.append([InlineKeyboardButton("4", callback_data="numpad_4"),
               InlineKeyboardButton("5", callback_data="numpad_5"),
               InlineKeyboardButton("6", callback_data="numpad_6")])
    kb.append([InlineKeyboardButton("7", callback_data="numpad_7"),
               InlineKeyboardButton("8", callback_data="numpad_8"),
               InlineKeyboardButton("9", callback_data="numpad_9")])
    kb.append([InlineKeyboardButton("❌ ᴄʟᴇᴀʀ", callback_data="numpad_clear"),
               InlineKeyboardButton("0", callback_data="numpad_0"),
               InlineKeyboardButton("✅ ꜱᴜʙᴍɪᴛ", callback_data="numpad_submit")])
    return InlineKeyboardMarkup(kb)


def admin_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("💰 ʙᴜʏ ᴘʀɪᴄᴇ", callback_data="adm_buy_price"),
         InlineKeyboardButton("💵 ꜱᴇʟʟ ᴘʀɪᴄᴇ", callback_data="adm_sell_price")],
        [InlineKeyboardButton("➕ ᴀᴅᴅ ʙᴀʟᴀɴᴄᴇ", callback_data="adm_add_bal"),
         InlineKeyboardButton("➖ ᴅᴇᴅᴜᴄᴛ ʙᴀʟ", callback_data="adm_deduct_bal")],
        [InlineKeyboardButton("📱 ᴀᴅᴅ ᴀᴄᴄᴏᴜɴᴛ", callback_data="adm_add_acc"),
         InlineKeyboardButton("📋 ᴍᴀɴᴀɢᴇ ᴀᴄᴄꜱ", callback_data="adm_manage")],
        [InlineKeyboardButton("🏷 ᴄᴏᴜɴᴛʀʏ ʙᴜʏ", callback_data="adm_c_buy"),
         InlineKeyboardButton("🏷 ᴄᴏᴜɴᴛʀʏ ꜱᴇʟʟ", callback_data="adm_c_sell")],
        [InlineKeyboardButton("💲 ᴄᴜꜱᴛᴏᴍ ᴘʀɪᴄᴇ", callback_data="adm_custom_price"),
         InlineKeyboardButton("📝 ᴀᴄᴄᴏᴜɴᴛ ɴᴏᴛᴇ", callback_data="adm_acc_note")],
        [InlineKeyboardButton("📢 ʙʀᴏᴀᴅᴄᴀꜱᴛ", callback_data="adm_broadcast"),
         InlineKeyboardButton("📊 ꜱᴛᴀᴛꜱ", callback_data="adm_stats")],
        [InlineKeyboardButton("👥 ᴍᴀɴᴀɢᴇ ᴜꜱᴇʀꜱ", callback_data="adm_users"),
         InlineKeyboardButton("📦 ᴘᴇɴᴅɪɴɢ ꜱᴇʟʟꜱ", callback_data="adm_pending_sells")],
        [InlineKeyboardButton("💳 ᴘᴇɴᴅɪɴɢ ᴅᴇᴘꜱ", callback_data="adm_pending_deps"),
         InlineKeyboardButton("📢 ꜰᴏʀᴄᴇ ᴊᴏɪɴ", callback_data="adm_fj")],
        [InlineKeyboardButton("🛡 ꜱᴘᴀᴍ ᴄᴏɴᴛʀᴏʟ", callback_data="adm_spam"),
         InlineKeyboardButton("📝 ʟᴏɢꜱ", callback_data="adm_logs")],
        [InlineKeyboardButton("🛒 ʙᴜʏ ᴛᴏɢɢʟᴇ", callback_data="adm_buy_tog"),
         InlineKeyboardButton("💸 ꜱᴇʟʟ ᴛᴏɢɢʟᴇ", callback_data="adm_sell_tog")],
        [InlineKeyboardButton("🔧 ᴍᴀɪɴᴛᴇɴᴀɴᴄᴇ", callback_data="adm_maint"),
         InlineKeyboardButton("⚙️ ʙᴏᴛ ꜱᴇᴛᴛɪɴɢꜱ", callback_data="adm_settings")],
        [InlineKeyboardButton("🎟 ᴘʀᴏᴍᴏ ᴄᴏᴅᴇꜱ", callback_data="adm_promo"),
         InlineKeyboardButton("📤 ᴇxᴘᴏʀᴛ ᴜꜱᴇʀꜱ", callback_data="adm_export")],
    ])


app = Client("felix_store", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)


@app.on_callback_query(filters.regex(r"^menu_(buy|sell|profile|addfunds|support)$"))
async def cb_main_menu(client, cq: CallbackQuery):
    action = cq.data.split("_")[1]
    msg = cq.message
    msg.from_user = cq.from_user
    if action == "buy":
        await cmd_buy(client, msg)
    elif action == "sell":
        await cmd_sell(client, msg)
    elif action == "profile":
        await cmd_profile(client, msg)
    elif action == "addfunds":
        await cmd_add_funds(client, msg)
    elif action == "support":
        await cmd_support(client, msg)
    await cq.answer()


@app.on_message(filters.command("start") & filters.private)
async def cmd_start(client, message: Message):
    user = message.from_user
    if not user:
        return
    try:
        await _cmd_start_inner(client, message, user)
    except Exception as e:
        # Last-resort safety net: whatever broke above (markdown parse error
        # from an unescaped name/setting, a transient DB/network hiccup,
        # anything), the user must see *something* instead of total silence.
        # Plain text, no parse_mode, no keyboard — nothing here can fail.
        logger.exception(f"cmd_start failed for uid={user.id}: {e}")
        try:
            await client.send_message(
                user.id,
                "⚠️ Something went wrong starting the bot. Please try /start again, "
                f"or contact {SUPPORT_USER} if this keeps happening.",
                parse_mode=enums.ParseMode.DISABLED,
            )
        except Exception as e2:
            logger.error(f"cmd_start fallback send also failed uid={user.id}: {e2}")


async def _cmd_start_inner(client, message: Message, user):
    register_user(user)
    user_state.pop(user.id, None)

    if is_banned(user.id) and user.id != ADMIN_ID:
        await message.reply(f"🚫 You are banned. Contact {SUPPORT_USER}.")
        return

    if get_setting("maintenance_mode") == "on" and user.id != ADMIN_ID:
        await message.reply("🔧 Bot is under maintenance. Try later.")
        return

    args = message.text.split()
    if len(args) > 1 and args[1].isdigit():
        ref_id = int(args[1])
        if ref_id != user.id:
            existing = db_execute("SELECT id FROM referrals WHERE referred_id=?", (user.id,), fetch="one")
            if not existing:
                bonus = float(get_setting("referral_bonus", "10"))
                db_execute_many([
                    ("INSERT INTO referrals(referrer_id,referred_id,rewarded,timestamp) VALUES(?,?,1,?)",
                     (ref_id, user.id, now_iso())),
                    ("UPDATE users SET balance=balance+? WHERE id=?", (bonus, ref_id)),
                ])
                try:
                    await client.send_message(ref_id, f"🎁 **ʀᴇꜰᴇʀʀᴀʟ ʙᴏɴᴜꜱ!**\n\n₹{bonus:.0f} credited for referring {user.first_name}!")
                except Exception:
                    pass

    missing = await check_force_join(client, user.id)
    if missing and user.id != ADMIN_ID:
        await send_force_join_msg(client, message, missing)
        return

    welcome = get_setting("welcome_message", "Welcome!")
    text = (
        f"**{BOT_NAME}**\n\n"
        f"{welcome}\n\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"👋 Hello, **{user.first_name}**!\n"
        f"🆔 ID: `{user.id}`\n"
        f"━━━━━━━━━━━━━━━━\n\n"
        f"Use the menu below to get started 👇"
    )
    try:
        await message.reply_photo(WELCOME_IMG, caption=text, reply_markup=main_kb())
    except Exception:
        await message.reply(text, reply_markup=main_kb())


@app.on_callback_query(filters.regex("^fj_check$"))
async def cb_fj_check(client, cq: CallbackQuery):
    missing = await check_force_join(client, cq.from_user.id)
    if missing:
        await cq.answer("❌ You haven't joined all channels!", show_alert=True)
        return
    await cq.message.delete()
    await client.send_message(cq.from_user.id, f"✅ **ᴀᴄᴄᴇꜱꜱ ɢʀᴀɴᴛᴇᴅ!** Welcome to {BOT_NAME}!", reply_markup=main_kb())


@app.on_message(filters.command("cancel") & filters.private)
async def cmd_cancel(client, message: Message):
    uid = message.from_user.id
    data = user_state.pop(uid, {})
    session_client = data.get("client")
    if session_client:
        await safe_disconnect(session_client)
    await message.reply("❌ **ᴄᴀɴᴄᴇʟʟᴇᴅ.**", reply_markup=main_kb())


@app.on_message(filters.command("admin") & filters.private)
async def cmd_admin(client, message: Message):
    if message.from_user.id != ADMIN_ID:
        await message.reply("❌ Unauthorized.")
        return
    user_state.pop(ADMIN_ID, None)
    countries = get_available_countries()
    total_sessions = sum(countries.values())
    users_count = len(get_all_users())
    await message.reply(
        f"🔧 **{BOT_NAME} — Admin Panel**\n\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"👥 ᴜꜱᴇʀꜱ: **{users_count}**\n"
        f"📦 ꜱᴇꜱꜱɪᴏɴꜱ: **{total_sessions}**\n"
        f"🌍 ᴄᴏᴜɴᴛʀɪᴇꜱ: **{len(countries)}**\n"
        f"━━━━━━━━━━━━━━━━\n\n"
        f"Select an option:",
        reply_markup=admin_kb()
    )


@app.on_callback_query(filters.regex("^adm_back$"))
async def cb_adm_back(client, cq: CallbackQuery):
    if cq.from_user.id != ADMIN_ID:
        return
    user_state.pop(ADMIN_ID, None)
    await cq.message.edit_text(f"🔧 **{BOT_NAME} — Admin Panel**", reply_markup=admin_kb())


@app.on_message(filters.regex("^👤 My Profile$") & filters.private)
async def cmd_profile(client, message: Message):
    if not await guard(client, message):
        return
    uid = message.from_user.id
    user = get_user(uid)
    if not user:
        register_user(message.from_user)
        user = get_user(uid)
    orders = get_user_orders(uid)
    sells = get_user_sell_orders(uid)
    sold = sum(1 for item in sells if item["status"] == "approved")
    bot_me = await client.get_me()
    ref_link = f"https://t.me/{bot_me.username}?start={uid}"
    ref_count = db_execute("SELECT COUNT(*) as c FROM referrals WHERE referrer_id=?", (uid,), fetch="one")
    await message.reply(
        f"👤 **ᴍʏ ᴘʀᴏꜰɪʟᴇ**\n\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"📛 ɴᴀᴍᴇ: **{user['name']}**\n"
        f"🆔 ID: `{user['id']}`\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"💰 ʙᴀʟᴀɴᴄᴇ: ₹**{user['balance']:.2f}**\n"
        f"💳 ᴅᴇᴘᴏꜱɪᴛᴇᴅ: ₹{user['total_deposited']:.2f}\n"
        f"💸 ꜱᴘᴇɴᴛ: ₹{user['total_spent']:.2f}\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"🛒 ʙᴏᴜɢʜᴛ: {len(orders)}\n"
        f"💹 ꜱᴏʟᴅ: {sold}\n"
        f"👥 ʀᴇꜰᴇʀʀᴀʟꜱ: {ref_count['c'] if ref_count else 0}\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"🔗 ʀᴇꜰ: `{ref_link}`",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("📊 ꜱᴛᴀᴛɪꜱᴛɪᴄꜱ", callback_data="prof_stats"),
             InlineKeyboardButton("📋 ᴍʏ ᴏʀᴅᴇʀꜱ", callback_data="prof_orders")],
            [InlineKeyboardButton("🎁 ʀᴇꜰᴇʀ & ᴇᴀʀɴ", callback_data="prof_refer"),
             InlineKeyboardButton("🎟 ᴘʀᴏᴍᴏ ᴄᴏᴅᴇ", callback_data="prof_promo")],
            [InlineKeyboardButton("📜 ʙᴜʏ ʜɪꜱᴛᴏʀʏ", callback_data="hist_buy"),
             InlineKeyboardButton("📤 ꜱᴇʟʟ ʜɪꜱᴛᴏʀʏ", callback_data="hist_sell")]
        ])
    )

@app.on_callback_query(filters.regex(r"^prof_(stats|orders|refer|promo)$"))
async def cb_prof_menu(client, cq: CallbackQuery):
    action = cq.data.split("_")[1]
    msg = cq.message
    msg.from_user = cq.from_user
    if action == "stats":
        await cmd_stats(client, msg)
    elif action == "orders":
        await cmd_orders(client, msg)
    elif action == "refer":
        await cmd_refer(client, msg)
    elif action == "promo":
        await cmd_promo(client, msg)
    await cq.answer()


@app.on_callback_query(filters.regex("^hist_buy$"))
async def cb_hist_buy(client, cq: CallbackQuery):
    orders = get_user_orders(cq.from_user.id)
    if not orders:
        await cq.answer("No history!", show_alert=True)
        return
    lines = ["📜 **ᴘᴜʀᴄʜᴀꜱᴇ ʜɪꜱᴛᴏʀʏ** (last 20)\n"]
    for idx, order in enumerate(orders[:20], 1):
        emoji = "✅" if order["status"] == "completed" else "🔄"
        lines.append(f"{idx}. {emoji} `{order['phone']}` | {order['country']} | ₹{order['price']:.0f}")
    await cq.message.edit_text(
        "\n".join(lines),
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙", callback_data="hist_close")]])
    )


@app.on_callback_query(filters.regex("^hist_sell$"))
async def cb_hist_sell(client, cq: CallbackQuery):
    orders = get_user_sell_orders(cq.from_user.id)
    if not orders:
        await cq.answer("No history!", show_alert=True)
        return
    emoji_map = {"pending": "⏳", "approved": "✅", "rejected": "❌"}
    lines = ["📤 **ꜱᴇʟʟ ʜɪꜱᴛᴏʀʏ** (last 20)\n"]
    for idx, order in enumerate(orders[:20], 1):
        lines.append(f"{idx}. {emoji_map.get(order['status'], '❓')} `{order['phone_number']}` | ₹{order['sell_price']:.0f} | {order['status'].upper()}")
    await cq.message.edit_text(
        "\n".join(lines),
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙", callback_data="hist_close")]])
    )


@app.on_callback_query(filters.regex("^hist_close$"))
async def cb_hist_close(client, cq: CallbackQuery):
    await cq.message.delete()


@app.on_message(filters.regex("^📋 My Orders$") & filters.private)
async def cmd_orders(client, message: Message):
    if not await guard(client, message):
        return
    active = get_active_order(message.from_user.id)
    if not active:
        await message.reply(
            "📋 **ɴᴏ ᴀᴄᴛɪᴠᴇ ᴏʀᴅᴇʀꜱ**",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🐱 ʙᴜʏ ᴀᴄᴄᴏᴜɴᴛ", callback_data="go_buy")]])
        )
        return
    await message.reply(
        f"📋 **ᴀᴄᴛɪᴠᴇ ᴏʀᴅᴇʀ**\n\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"📱 ᴘʜᴏɴᴇ: `{active['phone']}`\n"
        f"🌍 ᴄᴏᴜɴᴛʀʏ: {active['country']}\n"
        f"💰 ᴘʀɪᴄᴇ: ₹{active['price']:.0f}\n"
        f"🔑 2FA: `{active['password']}`\n"
        f"━━━━━━━━━━━━━━━━",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔑 ɢᴇᴛ ᴏᴛᴘ", callback_data=f"otp_{active['id']}")],
            [InlineKeyboardButton("🚪 ʟᴏɢᴏᴜᴛ", callback_data=f"lo_ask_{active['id']}")],
        ])
    )


@app.on_message(filters.regex("^📊 Statistics$") & filters.private)
async def cmd_stats(client, message: Message):
    if not await guard(client, message):
        return
    uid = message.from_user.id
    user = get_user(uid)
    if not user:
        register_user(message.from_user)
        user = get_user(uid)
    orders = get_user_orders(uid)
    sells = get_user_sell_orders(uid)
    ref_count = db_execute("SELECT COUNT(*) as c FROM referrals WHERE referrer_id=?", (uid,), fetch="one")
    await message.reply(
        f"📊 **ʏᴏᴜʀ ꜱᴛᴀᴛɪꜱᴛɪᴄꜱ**\n\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"🛒 ʙᴏᴜɢʜᴛ: {len(orders)}\n"
        f"💸 ꜱᴘᴇɴᴛ: ₹{user['total_spent']:.2f}\n"
        f"💹 ꜱᴏʟᴅ: {sum(1 for item in sells if item['status'] == 'approved')}\n"
        f"⏳ Pending: {sum(1 for item in sells if item['status'] == 'pending')}\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"💳 ᴅᴇᴘᴏꜱɪᴛᴇᴅ: ₹{user['total_deposited']:.2f}\n"
        f"💰 ʙᴀʟᴀɴᴄᴇ: ₹{user['balance']:.2f}\n"
        f"👥 ʀᴇꜰᴇʀʀᴀʟꜱ: {ref_count['c'] if ref_count else 0}\n"
        f"━━━━━━━━━━━━━━━━"
    )


@app.on_message(filters.regex("^🆘 Support$") & filters.private)
async def cmd_support(client, message: Message):
    await message.reply(
        f"🆘 **ꜱᴜᴘᴘᴏʀᴛ**\n\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"👤 ᴄᴏɴᴛᴀᴄᴛ: {SUPPORT_USER}\n"
        f"━━━━━━━━━━━━━━━━\n\n"
        f"For issues with:\n"
        f"• Account not working\n"
        f"• Payment problems\n"
        f"• Sell order disputes\n\n"
        f"Message our support directly.",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("💬 ᴄᴏɴᴛᴀᴄᴛ ꜱᴜᴘᴘᴏʀᴛ", url=f"https://t.me/{SUPPORT_USER.lstrip('@')}")]
        ])
    )


@app.on_message(filters.regex("^🎁 Refer & Earn$") & filters.private)
async def cmd_refer(client, message: Message):
    if not await guard(client, message):
        return
    uid = message.from_user.id
    bot_me = await client.get_me()
    ref_link = f"https://t.me/{bot_me.username}?start={uid}"
    bonus = get_setting("referral_bonus", "10")
    ref_count = db_execute("SELECT COUNT(*) as c FROM referrals WHERE referrer_id=?", (uid,), fetch="one")
    total_refs = ref_count["c"] if ref_count else 0
    await message.reply(
        f"🎁 **ʀᴇꜰᴇʀ & ᴇᴀʀɴ**\n\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"💰 ʙᴏɴᴜꜱ ᴘᴇʀ ʀᴇꜰᴇʀʀᴀʟ: ₹{bonus}\n"
        f"👥 ʏᴏᴜʀ ʀᴇꜰᴇʀʀᴀʟꜱ: {total_refs}\n"
        f"━━━━━━━━━━━━━━━━\n\n"
        f"Share your link:\n`{ref_link}`\n\n"
        f"Each new user earns you ₹{bonus}!"
    )


@app.on_message(filters.regex("^🎟 Promo Code$") & filters.private)
async def cmd_promo(client, message: Message):
    if not await guard(client, message):
        return
    user_state[message.from_user.id] = {"step": "user_promo"}
    await message.reply("🎟 **ᴇɴᴛᴇʀ ᴘʀᴏᴍᴏ ᴄᴏᴅᴇ:**\n\n_(or /cancel to abort)_")


@app.on_message(filters.regex("^💳 Add Funds$") & filters.private)
async def cmd_add_funds(client, message: Message):
    if not await guard(client, message):
        return
    user_state[message.from_user.id] = {"step": "dep_amount"}
    await message.reply(
        f"💳 **ᴀᴅᴅ ꜰᴜɴᴅꜱ**\n\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"📱 ᴜᴘɪ: `{UPI_ID}`\n"
        f"━━━━━━━━━━━━━━━━\n\n"
        f"Min: ₹{get_setting('min_deposit', '10')} | Max: ₹{get_setting('max_deposit', '50000')}\n\n"
        f"Enter amount (₹):"
    )


@app.on_message(filters.regex("^🐱 Buy Account$") & filters.private)
async def cmd_buy(client, message: Message):
    if not await guard(client, message):
        return
    uid = message.from_user.id
    if get_setting("buy_feature") == "off":
        await message.reply(f"❌ **ʙᴜʏɪɴɢ ᴅɪꜱᴀʙʟᴇᴅ.**\nContact {SUPPORT_USER}.", reply_markup=main_kb())
        return
    if get_active_order(uid):
        await message.reply("⚠️ You already have an active order. Check 📋 My Orders.", reply_markup=main_kb())
        return
    countries = get_available_countries()
    if not countries:
        await message.reply("😔 **ɴᴏ ᴀᴄᴄᴏᴜɴᴛꜱ ᴀᴠᴀɪʟᴀʙʟᴇ ʀɪɢʜᴛ ɴᴏᴡ.**\nCheck back later!", reply_markup=main_kb())
        return
    buttons = []
    for country, count in countries.items():
        price = get_buy_price(country)
        buttons.append([InlineKeyboardButton(f"{flag(country)} {country} — ₹{price:.0f} | {count} acc", callback_data=f"bc_{country}")])
    buttons.append([InlineKeyboardButton("❌ ᴄᴀɴᴄᴇʟ", callback_data="buy_cancel")])
    await message.reply("🐱 **ʙᴜʏ ᴀᴄᴄᴏᴜɴᴛ** — Select Country:", reply_markup=InlineKeyboardMarkup(buttons))


@app.on_callback_query(filters.regex("^go_buy$"))
async def cb_go_buy(client, cq: CallbackQuery):
    countries = get_available_countries()
    if not countries:
        await cq.answer("No accounts available!", show_alert=True)
        return
    buttons = []
    for country, count in countries.items():
        price = get_buy_price(country)
        buttons.append([InlineKeyboardButton(f"{flag(country)} {country} — ₹{price:.0f} | {count} acc", callback_data=f"bc_{country}")])
    buttons.append([InlineKeyboardButton("❌ ᴄᴀɴᴄᴇʟ", callback_data="buy_cancel")])
    await cq.message.edit_text("🐱 **ʙᴜʏ ᴀᴄᴄᴏᴜɴᴛ** — Select Country:", reply_markup=InlineKeyboardMarkup(buttons))


@app.on_callback_query(filters.regex("^buy_cancel$"))
async def cb_buy_cancel(client, cq: CallbackQuery):
    await cq.message.delete()


@app.on_callback_query(filters.regex(r"^bc_(.+)$"))
async def cb_buy_country(client, cq: CallbackQuery):
    country = cq.data[3:]
    sessions = get_country_sessions(country)
    if not sessions:
        await cq.answer("No accounts available!", show_alert=True)
        return
    user = get_user(cq.from_user.id)
    balance = user["balance"] if user else 0
    first_session = sessions[0]
    price = get_buy_price_for_session(first_session, country)
    note = get_account_note(first_session)
    afford = "✅" if balance >= price else "❌"
    text = (
        f"🐱 **ᴘᴜʀᴄʜᴀꜱᴇ ᴅᴇᴛᴀɪʟꜱ**\n\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"{flag(country)} Country: **{country}**\n"
        f"💰 ᴘʀɪᴄᴇ: ₹**{price:.0f}**\n"
        f"📦 ᴀᴠᴀɪʟᴀʙʟᴇ: {len(sessions)}\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"💳 ʙᴀʟᴀɴᴄᴇ: ₹{balance:.2f} {afford}\n"
    )
    if note:
        text += f"📝 ɴᴏᴛᴇ: {note}\n"
    text += "━━━━━━━━━━━━━━━━\n\nConfirm purchase?"
    await cq.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ ᴄᴏɴꜰɪʀᴍ", callback_data=f"bcnf_{country}")],
            [InlineKeyboardButton("💳 ᴀᴅᴅ ꜰᴜɴᴅꜱ", callback_data="dep_inline"),
             InlineKeyboardButton("🔙 ʙᴀᴄᴋ", callback_data="bc_back")],
        ])
    )


@app.on_callback_query(filters.regex("^bc_back$"))
async def cb_bc_back(client, cq: CallbackQuery):
    countries = get_available_countries()
    if not countries:
        await cq.message.edit_text("❌ No accounts available.")
        return
    buttons = []
    for country, count in countries.items():
        price = get_buy_price(country)
        buttons.append([InlineKeyboardButton(f"{flag(country)} {country} — ₹{price:.0f} | {count} acc", callback_data=f"bc_{country}")])
    buttons.append([InlineKeyboardButton("❌ ᴄᴀɴᴄᴇʟ", callback_data="buy_cancel")])
    await cq.message.edit_text("🐱 **ʙᴜʏ ᴀᴄᴄᴏᴜɴᴛ** — Select Country:", reply_markup=InlineKeyboardMarkup(buttons))


@app.on_callback_query(filters.regex("^dep_inline$"))
async def cb_dep_inline(client, cq: CallbackQuery):
    user_state[cq.from_user.id] = {"step": "dep_amount"}
    await cq.message.edit_text("💳 **ᴀᴅᴅ ꜰᴜɴᴅꜱ**\n\nEnter amount (₹):")


@app.on_callback_query(filters.regex(r"^bcnf_(.+)$"))
async def cb_buy_confirm(client, cq: CallbackQuery):
    country = cq.data[5:]
    uid = cq.from_user.id
    if get_setting("buy_feature") == "off":
        await cq.answer("Buying is disabled!", show_alert=True)
        return
    if get_active_order(uid):
        await cq.answer("Active order exists!", show_alert=True)
        return
    session_file = get_first_session(country)
    if not session_file:
        await cq.answer("No accounts available!", show_alert=True)
        return
    price = get_buy_price_for_session(session_file, country)
    user = get_user(uid)
    balance = user["balance"] if user else 0
    if balance < price:
        await cq.message.edit_text(
            f"❌ **ɪɴꜱᴜꜰꜰɪᴄɪᴇɴᴛ ʙᴀʟᴀɴᴄᴇ!**\n\n"
            f"💳 ʙᴀʟᴀɴᴄᴇ: ₹{balance:.2f}\n"
            f"💸 ʀᴇǫᴜɪʀᴇᴅ: ₹{price:.0f}\n"
            f"📊 ɴᴇᴇᴅ: ₹{price - balance:.2f}",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("💳 ᴀᴅᴅ ꜰᴜɴᴅꜱ", callback_data="dep_inline")],
                [InlineKeyboardButton("🔙 ʙᴀᴄᴋ", callback_data="bc_back")],
            ])
        )
        return
    phone = session_file.replace(".session", "")
    
    src_base = os.path.join(SESSIONS_DIR, country, phone)
    dest_dir = os.path.join(SOLD_DIR, country)
    os.makedirs(dest_dir, exist_ok=True)
    dest_base = os.path.join(dest_dir, phone)
    
    idx = 1
    while os.path.exists(dest_base + ".session"):
        dest_base = os.path.join(dest_dir, f"{phone}_{idx}")
        idx += 1

    try:
        move_session_bundle(src_base, dest_base)
    except Exception as e:
        logger.error(f"Failed to move sold session {phone}: {e}")
        await cq.message.edit_text("❌ **ꜰᴀɪʟᴇᴅ ᴛᴏ ᴘʀᴏᴄᴇꜱꜱ ᴛʜᴇ ꜱᴇꜱꜱɪᴏɴ.** Please try again.")
        return

    new_session_name = os.path.basename(dest_base) + ".session"
    deduct_balance(uid, price)
    order_id = create_buy_order(uid, new_session_name, country, price, phone, DEFAULT_2FA)
    log_action("account_sold", f"uid={uid} phone={phone} country={country} price={price}")
    await cq.message.edit_text(
        f"✅ **ᴘᴜʀᴄʜᴀꜱᴇ ꜱᴜᴄᴄᴇꜱꜱꜰᴜʟ!**\n\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"📱 ᴘʜᴏɴᴇ: `{phone}`\n"
        f"{flag(country)} Country: {country}\n"
        f"💰 ᴘʀɪᴄᴇ: ₹{price:.0f}\n"
        f"🔑 2FA: `{DEFAULT_2FA}`\n"
        f"━━━━━━━━━━━━━━━━\n\n"
        f"**ʜᴏᴡ ᴛᴏ ʟᴏɢɪɴ:**\n"
        f"1️⃣ Telegram → Add Account\n"
        f"2️⃣ Enter: `{phone}`\n"
        f"3️⃣ Click **ɢᴇᴛ ᴏᴛᴘ** → Enter code\n"
        f"4️⃣ 2FA: `{DEFAULT_2FA}`",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔑 ɢᴇᴛ ᴏᴛᴘ", callback_data=f"otp_{order_id}")],
            [InlineKeyboardButton("🚪 ʟᴏɢᴏᴜᴛ", callback_data=f"lo_ask_{order_id}")],
        ])
    )
    try:
        await client.send_message(
            ADMIN_ID,
            f"🛒 **ꜱᴀʟᴇ ᴀʟᴇʀᴛ**\n\nUser: {cq.from_user.first_name} (`{uid}`)\n📱 `{phone}` | {flag(country)} {country}\n💰 ₹{price:.0f} | Order #{order_id}"
        )
    except Exception:
        pass


@app.on_callback_query(filters.regex(r"^otp_(\d+)$"))
async def cb_get_otp(client, cq: CallbackQuery):
    order_id = int(cq.data[4:])
    uid = cq.from_user.id
    order = db_execute("SELECT * FROM orders WHERE id=? AND user_id=?", (order_id, uid), fetch="one")
    if not order:
        await cq.answer("Order not found!", show_alert=True)
        return
    if order["status"] != "active":
        await cq.answer("Order not active!", show_alert=True)
        return

    cooldown_secs = int(get_setting("otp_cooldown", "10"))
    last_req = _otp_cooldowns.get(uid, 0)
    now_ts = time.time()
    if now_ts - last_req < cooldown_secs:
        wait = int(cooldown_secs - (now_ts - last_req)) + 1
        await safe_edit_text(
            cq.message,
            f"⏳ **Please wait {wait}s before requesting OTP again.**",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔄 ʀᴇᴛʀʏ", callback_data=f"otp_{order_id}")]])
        )
        return

    lock = get_otp_lock(order_id)
    if lock.locked():
        await cq.answer("⏳ Already fetching OTP, please wait…", show_alert=True)
        return

    _otp_cooldowns[uid] = now_ts
    await cq.answer("🔄 Fetching OTP...")

    base_path = os.path.abspath(os.path.join(SOLD_DIR, order["country"], order["session_name"].replace(".session", "")))
    if not os.path.exists(base_path + ".session"):
        await safe_edit_text(
            cq.message,
            f"❌ **ꜱᴇꜱꜱɪᴏɴ ɴᴏᴛ ꜰᴏᴜɴᴅ!**\nContact {SUPPORT_USER}.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🆘 ꜱᴜᴘᴘᴏʀᴛ", url=f"https://t.me/{SUPPORT_USER.lstrip('@')}")]])
        )
        return

    async with lock:
        otp = None
        session_client = None
        try:
            session_client = build_session_client(base_path)

            async def _fetch():
                await session_client.start()
                found = None
                async for msg in session_client.get_chat_history(777000, limit=10):
                    if not msg.text:
                        continue
                    cleaned = msg.text.replace("-", "").replace(" ", "")
                    match = re.search(r"(?<!\d)(\d{5,6})(?!\d)", cleaned)
                    if match:
                        found = match.group(1)
                        break
                return found

            # Pyrogram's start()/get_chat_history have no built-in timeout —
            # a stuck DC connection here would hold this order's lock forever,
            # permanently showing "Already fetching OTP". Bound it explicitly.
            otp = await asyncio.wait_for(_fetch(), timeout=25)
            await safe_stop(session_client)
            session_client = None
        except asyncio.TimeoutError:
            # start()/get_chat_history hung past our bound. Force-disconnect
            # rather than safe_stop() — a graceful stop() can itself hang on
            # a connection that's already stuck mid-handshake.
            if session_client:
                await safe_disconnect(session_client)
            logger.warning(f"OTP fetch timed out oid={order_id}")
            await safe_edit_text(
                cq.message,
                "⏳ **ᴛᴇʟᴇɢʀᴀᴍ ᴄᴏɴɴᴇᴄᴛɪᴏɴ ᴛɪᴍᴇᴅ ᴏᴜᴛ**\n\nTry again in a few seconds.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔄 ʀᴇᴛʀʏ", callback_data=f"otp_{order_id}")]])
            )
            return
        except AuthKeyUnregistered:
            if session_client:
                await safe_stop(session_client)
            await safe_edit_text(
                cq.message,
                f"❌ **ꜱᴇꜱꜱɪᴏɴ ᴇxᴘɪʀᴇᴅ!**\nContact {SUPPORT_USER}.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🆘 ꜱᴜᴘᴘᴏʀᴛ", url=f"https://t.me/{SUPPORT_USER.lstrip('@')}")]])
            )
            return
        except FloodWait as e:
            if session_client:
                await safe_stop(session_client)
            await safe_edit_text(
                cq.message,
                f"⏳ **ᴛᴇʟᴇɢʀᴀᴍ ʀᴀᴛᴇ ʟɪᴍɪᴛ**\n\nPlease wait **{e.value + 2} seconds** and try again.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔄 ʀᴇᴛʀʏ", callback_data=f"otp_{order_id}")]])
            )
            return
        except sqlite3.OperationalError as e:
            if session_client:
                await safe_stop(session_client)
            logger.error(f"OTP db locked oid={order_id}: {e}")
            await safe_edit_text(
                cq.message,
                "⚠️ **ꜱᴇꜱꜱɪᴏɴ ʙᴜꜱʏ**\n\nAnother request is using this session. Please retry in a few seconds.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔄 ʀᴇᴛʀʏ", callback_data=f"otp_{order_id}")]])
            )
            return
        except Exception as e:
            if session_client:
                await safe_stop(session_client)
            logger.error(f"OTP error oid={order_id}: {type(e).__name__}: {e}")
            await safe_edit_text(
                cq.message,
                f"❌ **ᴇʀʀᴏʀ ꜰᴇᴛᴄʜɪɴɢ ᴏᴛᴘ**\n\nError: `{type(e).__name__}`\n\nMake sure you already requested a login code in Telegram app first,\nthen tap Retry.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔄 ʀᴇᴛʀʏ", callback_data=f"otp_{order_id}")],
                    [InlineKeyboardButton("🆘 ꜱᴜᴘᴘᴏʀᴛ", url=f"https://t.me/{SUPPORT_USER.lstrip('@')}")],
                ])
            )
            return

    buttons = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔄 ʀᴇꜰʀᴇꜱʜ ᴏᴛᴘ", callback_data=f"otp_{order_id}")],
        [InlineKeyboardButton("🚪 ʟᴏɢᴏᴜᴛ ʙᴏᴛ", callback_data=f"lo_ask_{order_id}")],
    ])
    if otp:
        if otp == (order["last_otp"] or ""):
            await safe_edit_text(
                cq.message,
                f"⚠️ **ɴᴏ ɴᴇᴡ ᴏᴛᴘ ʏᴇᴛ!**\n\n━━━━━━━━━━━━━━━━\nLast OTP: `{otp}` _(already shown)_\n━━━━━━━━━━━━━━━━\n\nRequest a **ɴᴇᴡ ᴄᴏᴅᴇ** in Telegram first,\nthen click Refresh OTP.",
                reply_markup=buttons
            )
            return
        db_execute("UPDATE orders SET last_otp=? WHERE id=?", (otp, order_id))
        await safe_edit_text(
            cq.message,
            f"✅ **ᴏᴛᴘ ʀᴇᴀᴅʏ!**\n\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"🔑 ᴏᴛᴘ: `{otp}`\n"
            f"🔐 2FA: `{order['password']}`\n"
            f"━━━━━━━━━━━━━━━━\n\n"
            f"1️⃣ Enter OTP in Telegram app\n"
            f"2️⃣ 2FA: `{order['password']}`\n"
            f"⏰ Expires in ~5 min",
            reply_markup=buttons
        )
        return
    await safe_edit_text(
        cq.message,
        f"⚠️ **ɴᴏ ᴏᴛᴘ ꜰᴏᴜɴᴅ**\n\n📱 ᴘʜᴏɴᴇ: `{order['phone']}`\n\n1️⃣ Enter phone in Telegram\n2️⃣ Request login code\n3️⃣ Wait 10 sec, then refresh",
        reply_markup=buttons
    )


@app.on_callback_query(filters.regex(r"^lo_ask_(\d+)$"))
async def cb_lo_ask(client, cq: CallbackQuery):
    oid = cq.data.split("lo_ask_")[1]
    await cq.message.edit_text(
        "⚠️ **ᴄᴏɴꜰɪʀᴍ ʟᴏɢᴏᴜᴛ**\n\nThis will end the bot's session.\nYou can still use the account normally.\n\nAre you sure?",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ ʏᴇꜱ", callback_data=f"lo_yes_{oid}")],
            [InlineKeyboardButton("❌ ɴᴏ", callback_data=f"lo_no_{oid}")],
        ])
    )


@app.on_callback_query(filters.regex(r"^lo_no_(\d+)$"))
async def cb_lo_no(client, cq: CallbackQuery):
    oid = cq.data.split("lo_no_")[1]
    await cq.message.edit_text(
        "👍 **ᴄᴀɴᴄᴇʟʟᴇᴅ.** Account still active.",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔑 ɢᴇᴛ ᴏᴛᴘ", callback_data=f"otp_{oid}")],
            [InlineKeyboardButton("🚪 ʟᴏɢᴏᴜᴛ", callback_data=f"lo_ask_{oid}")],
        ])
    )


@app.on_callback_query(filters.regex(r"^lo_yes_(\d+)$"))
async def cb_lo_yes(client, cq: CallbackQuery):
    order_id = int(cq.data.split("lo_yes_")[1])
    order = db_execute("SELECT * FROM orders WHERE id=? AND user_id=?", (order_id, cq.from_user.id), fetch="one")
    if not order:
        await cq.answer("Order not found!", show_alert=True)
        return
    base_path = os.path.join(SOLD_DIR, order["country"], order["session_name"].replace(".session", ""))
    await cq.answer("🔄 Logging out...")
    session_client = None
    async with get_otp_lock(order_id):
        try:
            if os.path.exists(base_path + ".session"):
                session_client = build_session_client(base_path)
                await session_client.start()
                await session_client.log_out()
                session_client = None
        except (UserDeactivated, UserDeactivatedBan, AuthKeyUnregistered):
            if session_client:
                await safe_stop(session_client)
                session_client = None
        except Exception as e:
            logger.warning(f"Logout: {e}")
            if session_client:
                await safe_stop(session_client)
                session_client = None
    clean_session_files(base_path)
    db_execute("UPDATE orders SET status='completed' WHERE id=?", (order_id,))
    await safe_edit_text(
        cq.message,
        f"✅ **ʟᴏɢᴏᴜᴛ ꜱᴜᴄᴄᴇꜱꜱꜰᴜʟ!**\n\n📱 ᴘʜᴏɴᴇ: `{order['phone']}`\n🔐 2FA: `{order['password']}`\n\nEnjoy your account! 🎉"
    )


async def check_other_devices(session_client: Client) -> int:
    try:
        auths = await session_client.invoke(functions.account.GetAuthorizations())
        other_devices = [a for a in auths.authorizations if not a.current]
        return len(other_devices)
    except Exception as e:
        logger.error(f"Error checking authorizations: {e}")
        raise e


def sanitize_tfa_status_for_user(status_str: str) -> str:
    if not status_str:
        return "Secured ✅"
    status_lower = status_str.lower()
    if "wrong old password" in status_lower:
        return "Unchanged (Wrong 2FA password provided) ⚠️"
    if "old password not provided" in status_lower:
        return "Unchanged (Old 2FA password not provided) ⚠️"
    if "error" in status_lower:
        return "Unchanged (Error securing 2FA) ⚠️"
    if status_str.startswith("✅"):
        return "Secured ✅"
    return "Active 🔑"


async def check_spambot(session_client: Client) -> str:
    try:
        await session_client.send_message("SpamBot", "/start")
        await asyncio.sleep(6)
        async for msg in session_client.get_chat_history("SpamBot", limit=1):
            return msg.text or msg.caption or ""
        return ""
    except Exception as e:
        logger.error(f"SpamBot error: {e}")
        return f"Error: {e}"


def is_spam_free(text: str) -> bool:
    lowered = (text or "").lower()
    indicators = [
        "no limits", "good news", "your account is free", "has no limitations",
        "free of limitations", "free from limitations", "isn't limited",
    ]
    return any(item in lowered for item in indicators)


async def do_full_cleanup(session_client: Client, bot_username: str = ""):
    errors = []
    try:
        await session_client.update_profile(
            first_name=DEFAULT_NAME,
            last_name="",
            bio=f"@{bot_username}" if bot_username else DEFAULT_NAME,
        )
    except FloodWait as e:
        await asyncio.sleep(e.value + 2)
        try:
            await session_client.update_profile(
                first_name=DEFAULT_NAME,
                last_name="",
                bio=f"@{bot_username}" if bot_username else DEFAULT_NAME,
            )
        except Exception as ex:
            errors.append(f"Profile: {ex}")
    except Exception as e:
        errors.append(f"Profile: {e}")

    try:
        await session_client.update_username("")
    except Exception as e:
        errors.append(f"Username: {e}")

    try:
        await session_client.invoke(functions.account.SetPrivacy(
            key=raw_types.InputPrivacyKeyPhoneNumber(),
            rules=[raw_types.InputPrivacyValueDisallowAll()]
        ))
    except Exception as e:
        errors.append(f"Phone privacy: {e}")

    try:
        await session_client.invoke(functions.account.SetPrivacy(
            key=raw_types.InputPrivacyKeyAddedByPhone(),
            rules=[raw_types.InputPrivacyValueAllowContacts()]
        ))
    except Exception as e:
        errors.append(f"AddByPhone: {e}")

    try:
        await session_client.invoke(functions.account.SetPrivacy(
            key=raw_types.InputPrivacyKeyStatusTimestamp(),
            rules=[raw_types.InputPrivacyValueDisallowAll()]
        ))
    except Exception as e:
        errors.append(f"LastSeen: {e}")

    try:
        photo_ids = []
        async for photo in session_client.get_chat_photos("me"):
            photo_ids.append(photo.file_id)
        for file_id in photo_ids:
            try:
                await session_client.delete_profile_photos(file_id)
            except FloodWait as e:
                await asyncio.sleep(e.value + 1)
                try:
                    await session_client.delete_profile_photos(file_id)
                except Exception:
                    pass
            except Exception:
                pass
    except Exception as e:
        errors.append(f"Photos: {e}")

    try:
        contacts = await session_client.get_contacts()
        for contact in contacts:
            try:
                peer = await session_client.resolve_peer(contact.id)
                await session_client.invoke(functions.contacts.DeleteContacts(id=[peer]))
            except FloodWait as e:
                await asyncio.sleep(e.value + 1)
            except Exception:
                pass
    except Exception as e:
        errors.append(f"Contacts: {e}")

    try:
        await session_client.invoke(functions.contacts.ResetSaved())
    except Exception as e:
        errors.append(f"ResetSaved: {e}")

    try:
        async for dialog in session_client.get_dialogs():
            try:
                chat = dialog.chat
                chat_id = chat.id
                if chat_id == 777000:
                    continue
                if getattr(chat, "username", None) and chat.username.lower() == "spambot":
                    continue
                if chat.type in (enums.ChatType.GROUP, enums.ChatType.SUPERGROUP, enums.ChatType.CHANNEL):
                    try:
                        try:
                            await session_client.leave_chat(chat_id, delete=True)
                        except TypeError:
                            await session_client.leave_chat(chat_id)
                    except FloodWait as e:
                        await asyncio.sleep(e.value + 1)
                        try:
                            await session_client.leave_chat(chat_id)
                        except Exception:
                            pass
                elif chat.type in (enums.ChatType.BOT, enums.ChatType.PRIVATE):
                    try:
                        peer = await session_client.resolve_peer(chat_id)
                        await session_client.invoke(functions.messages.DeleteHistory(
                            peer=peer, max_id=0, just_clear=True, revoke=True
                        ))
                    except FloodWait as e:
                        await asyncio.sleep(e.value + 1)
                    except Exception:
                        pass
                    if chat.type == enums.ChatType.BOT:
                        try:
                            await session_client.block_user(chat_id)
                        except Exception:
                            pass
            except FloodWait as e:
                await asyncio.sleep(e.value + 1)
            except Exception:
                continue
    except Exception as e:
        errors.append(f"Chats: {e}")
    return errors


async def do_set_2fa(session_client: Client, old_password: str = "") -> str:
    try:
        pwd_info = await session_client.invoke(functions.account.GetPassword())
        if pwd_info.has_password:
            if not old_password:
                return "⚠️ Account has 2FA — old password not provided, left unchanged"
            try:
                await session_client.change_cloud_password(old_password, DEFAULT_2FA)
                return f"✅ 2FA changed to `{DEFAULT_2FA}`"
            except PasswordHashInvalid:
                return "⚠️ Wrong old password — 2FA left unchanged"
            except Exception as e:
                return f"⚠️ 2FA change error: {e}"
        try:
            await session_client.enable_cloud_password(DEFAULT_2FA)
            return f"✅ 2FA enabled: `{DEFAULT_2FA}`"
        except Exception as e:
            return f"⚠️ 2FA enable error: {e}"
    except Exception as e:
        return f"⚠️ 2FA check error: {e}"


async def run_admin_add_pipeline(bot: Client, uid: int, status_msg=None):
    data = user_state.get(uid, {})
    session_client = data.get("client")
    phone = data.get("phone", "")
    country = data.get("country", "Unknown")
    if not session_client:
        if status_msg:
            await status_msg.edit_text("❌ ɪɴᴛᴇʀɴᴀʟ ᴇʀʀᴏʀ: session lost. Please start over.")
        else:
            await bot.send_message(uid, "❌ ɪɴᴛᴇʀɴᴀʟ ᴇʀʀᴏʀ: session lost.")
        user_state.pop(uid, None)
        return
    try:
        if status_msg:
            await status_msg.edit_text("🔍 **ᴄʜᴇᴄᴋɪɴɢ ꜱᴘᴀᴍʙᴏᴛ…**")
        spam_result = await check_spambot(session_client)
        clean = is_spam_free(spam_result)
        accept_spam = get_setting("accept_spam_accounts") == "on"
        if clean:
            spam_line = "✅ **ꜱᴘᴀᴍ:** Account is clean!"
        elif accept_spam:
            spam_line = f"⚠️ **ꜱᴘᴀᴍ:** Flagged (accepted by admin setting)\n`{spam_result[:200]}`"
        else:
            spam_line = f"⚠️ **ꜱᴘᴀᴍ:** Flagged!\n`{spam_result[:200]}`"
        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("✅ ᴄʟᴇᴀɴ ᴜᴘ & ᴀᴅᴅ", callback_data=f"acc_yes_{uid}"),
                InlineKeyboardButton("➕ ᴀᴅᴅ ᴀꜱ-ɪꜱ", callback_data=f"acc_no_{uid}"),
            ],
            [InlineKeyboardButton("🗑 ᴅɪꜱᴄᴀʀᴅ", callback_data=f"acc_discard_{uid}")],
        ])
        text = (
            f"📱 **ᴘʜᴏɴᴇ:** `{phone}`\n"
            f"{flag(country)} **ᴄᴏᴜɴᴛʀʏ:** {country}\n\n"
            f"{spam_line}\n\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"**ᴡʜᴀᴛ ᴅᴏ ʏᴏᴜ ᴡᴀɴᴛ ᴛᴏ ᴅᴏ?**\n\n"
            f"✅ **ᴄʟᴇᴀɴ ᴜᴘ & ᴀᴅᴅ** → Name=`{DEFAULT_NAME}`, 2FA=`{DEFAULT_2FA}`, delete chats\n\n"
            f"➕ **ᴀᴅᴅ ᴀꜱ-ɪꜱ** → No changes\n\n"
            f"🗑 **ᴅɪꜱᴄᴀʀᴅ** → Delete\n"
            f"━━━━━━━━━━━━━━━━"
        )
        if status_msg:
            await status_msg.edit_text(text, reply_markup=keyboard)
        else:
            await bot.send_message(uid, text, reply_markup=keyboard)
        user_state[uid]["step"] = "adm_cleanup_choice"
        user_state[uid]["spam_result"] = spam_result
    except Exception as e:
        logger.error(f"Admin pipeline error: {e}")
        await bot.send_message(uid, f"❌ ᴇʀʀᴏʀ: `{str(e)[:200]}`")
        await safe_stop(session_client)
        if data.get("path"):
            clean_session_files(data["path"])
        user_state.pop(uid, None)


async def run_user_sell_pipeline(bot: Client, status_msg: Message, session_client: Client, uid: int):
    data = user_state.get(uid, {})
    phone = data.get("phone", "")
    base_path = data.get("path", "")
    country = data.get("country", "Unknown")
    try:
        status = await status_msg.reply("🔍 **ᴄʜᴇᴄᴋɪɴɢ ꜱᴘᴀᴍʙᴏᴛ…**")
        spam_result = await check_spambot(session_client)
        clean = is_spam_free(spam_result)
        accept_spam = get_setting("accept_spam_accounts") == "on"
        if not clean and not accept_spam:
            await safe_stop(session_client)
            user_state[uid]["client"] = None
            clean_session_files(base_path)
            db_execute("UPDATE business_stats SET value=value+1 WHERE key='total_rejected'", ())
            user_state.pop(uid, None)
            await status.edit_text(
                f"❌ **ᴀᴄᴄᴏᴜɴᴛ ʀᴇᴊᴇᴄᴛᴇᴅ!**\n\nSpam restrictions found:\n`{spam_result[:300]}`\n\nWe only accept clean accounts.\nContact {SUPPORT_USER} if you think this is wrong.",
                reply_markup=main_kb()
            )
            return

        user_state[uid]["spam_result"] = spam_result
        await status.edit_text("🔍 **ᴄʜᴇᴄᴋɪɴɢ ᴀᴄᴛɪᴠᴇ ꜱᴇꜱꜱɪᴏɴꜱ ᴏɴ ᴏᴛʜᴇʀ ᴅᴇᴠɪᴄᴇꜱ…**")
        other_devices = await check_other_devices(session_client)
        
        if other_devices > 0:
            user_state[uid]["step"] = "sell_wait_logout"
            user_state[uid]["status_msg_id"] = status.id
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ ᴄʜᴇᴄᴋᴇᴅ & ʟᴏɢɢᴇᴅ ᴏᴜᴛ", callback_data="sell_verify_logout")],
                [InlineKeyboardButton("❌ ᴄᴀɴᴄᴇʟ", callback_data="sell_cancel_logout")]
            ])
            await status.edit_text(
                f"⚠️ **ᴀᴄᴛɪᴏɴ ʀᴇǫᴜɪʀᴇᴅ: ʟᴏɢᴏᴜᴛ ᴏᴛʜᴇʀ ᴅᴇᴠɪᴄᴇꜱ!**\n\n"
                f"━━━━━━━━━━━━━━━━\n"
                f"📱 ᴘʜᴏɴᴇ: `{phone}`\n"
                f"🖥 Active sessions on other devices: **{other_devices}**\n"
                f"━━━━━━━━━━━━━━━━\n\n"
                f"👉 Please open your Telegram App → Settings → Devices, and **ᴛᴇʀᴍɪɴᴀᴛᴇ ᴀʟʟ ᴏᴛʜᴇʀ ꜱᴇꜱꜱɪᴏɴꜱ** (log out from all other apps/devices).\n\n"
                f"⚠️ **ɪᴍᴘᴏʀᴛᴀɴᴛ:** DO NOT terminate the bot's session. If you click 'Terminate all other sessions', it might log the bot out too. To avoid this, log out from each of your other devices manually.\n\n"
                f"Once you have logged out from your other devices, click the button below to verify:",
                reply_markup=kb
            )
            return
        
        await proceed_with_sell_pipeline(bot, status, session_client, uid)

    except Exception as e:
        logger.error(f"User sell pipeline: {type(e).__name__}: {e}")
        current = user_state.get(uid, {}).get("client")
        if current:
            await safe_stop(current)
        user_state.pop(uid, None)
        try:
            await status_msg.reply(f"❌ ᴇʀʀᴏʀ: `{type(e).__name__}: {str(e)[:200]}`\n\nContact {SUPPORT_USER}.", reply_markup=main_kb())
        except Exception:
            pass


async def proceed_with_sell_pipeline(bot: Client, status: Message, session_client: Client, uid: int):
    data = user_state.get(uid, {})
    phone = data.get("phone", "")
    base_path = data.get("path", "")
    country = data.get("country", "Unknown")
    spam_result = data.get("spam_result", "")
    try:
        spam_note = "✅ Clean" if is_spam_free(spam_result) else "⚠️ Flagged (admin accepted)"
        await status.edit_text(
            f"🔄 **ᴘʀᴏᴄᴇꜱꜱɪɴɢ…**\n\n🛡 ꜱᴘᴀᴍ: {spam_note}\n\nCleaning and securing your account…"
        )
        bot_me = await bot.get_me()
        try:
            await bot.send_chat_action(uid, enums.ChatAction.TYPING)
        except Exception:
            pass
        tfa_status = await do_set_2fa(session_client, data.get("old_2fa", ""))
        try:
            await bot.send_chat_action(uid, enums.ChatAction.TYPING)
        except Exception:
            pass
        if get_setting("auto_cleanup") == "on":
            await do_full_cleanup(session_client, bot_me.username or "")
        else:
            logger.info(f"Skipping auto-cleanup for {phone} (disabled in settings).")
        await safe_stop(session_client)
        user_state[uid]["client"] = None

        old_pwd = data.get("old_2fa", "") or "None"
        new_pwd = DEFAULT_2FA if tfa_status.startswith("✅") else "Unchanged"
        order_id = create_sell_order(uid, phone, base_path + ".session", spam_result, country, old_pwd, new_pwd)
        user_state[uid]["step"] = "sell_upi_id"
        user_state[uid]["sell_oid"] = order_id
        user_state[uid]["tfa_stat"] = tfa_status
        
        sanitized_tfa = sanitize_tfa_status_for_user(tfa_status)
        await status.edit_text(
            f"✅ **ᴀᴄᴄᴏᴜɴᴛ ᴠᴇʀɪꜰɪᴇᴅ & ꜱᴇᴄᴜʀᴇᴅ!**\n\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"📱 ᴘʜᴏɴᴇ: `{phone}`\n"
            f"🔑 2FA Status: {sanitized_tfa}\n"
            f"━━━━━━━━━━━━━━━━\n\n"
            f"💳 **ᴇɴᴛᴇʀ ʏᴏᴜʀ ᴜᴘɪ ɪᴅ** to receive payment:\n"
            f"_(e.g., `yourname@upi` or `9876543210@paytm`)_\n\n"
            f"Or type `wallet` to add to bot wallet instead.\n"
            f"_(or /cancel)_"
        )
    except Exception as e:
        logger.error(f"Proceed sell pipeline error: {e}")
        raise e


def admin_only(fn):
    @functools.wraps(fn)
    async def wrapper(client, cq: CallbackQuery):
        if cq.from_user.id != ADMIN_ID:
            await cq.answer("❌ Unauthorized!", show_alert=True)
            return
        await fn(client, cq)
    return wrapper


@app.on_message(filters.regex("^💸 Sell Account$") & filters.private)
async def cmd_sell(client, message: Message):
    if not await guard(client, message):
        return
    uid = message.from_user.id
    if get_setting("sell_feature") == "off":
        await message.reply(f"❌ **ꜱᴇʟʟ ᴅɪꜱᴀʙʟᴇᴅ.**\nContact {SUPPORT_USER}.", reply_markup=main_kb())
        return
    pending = get_pending_sell(uid)
    if pending:
        await message.reply(
            f"⚠️ **ᴘᴇɴᴅɪɴɢ ꜱᴇʟʟ ᴏʀᴅᴇʀ**\n\n📱 `{pending['phone_number']}` | ⏳ PENDING\n\nWait for admin review.",
            reply_markup=main_kb()
        )
        return
    user_state[uid] = {"step": "sell_phone"}
    await message.reply(
        f"💸 **ꜱᴇʟʟ ʏᴏᴜʀ ᴀᴄᴄᴏᴜɴᴛ**\n\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"💰 ᴘᴀʏᴏᴜᴛ: ~₹{get_sell_price():.0f} (to wallet OR UPI)\n"
        f"✅ Must be spam-free\n"
        f"━━━━━━━━━━━━━━━━\n\n"
        f"Enter phone (e.g., `+919876543210`):\n"
        f"Or /cancel to abort."
    )


@app.on_callback_query(filters.regex("^sell_cancel$"))
async def cb_sell_cancel(client, cq: CallbackQuery):
    data = user_state.pop(cq.from_user.id, {})
    if data.get("client"):
        await safe_disconnect(data["client"])
    await cq.message.edit_text("❌ Sell cancelled.")


@app.on_callback_query(filters.regex("^sell_verify_logout$"))
async def cb_sell_verify_logout(client, cq: CallbackQuery):
    uid = cq.from_user.id
    data = user_state.get(uid)
    if not data or data.get("step") != "sell_wait_logout":
        await cq.answer("Session expired or invalid state.", show_alert=True)
        return
    session_client = data.get("client")
    if not session_client:
        await cq.answer("Session lost. Start again.", show_alert=True)
        user_state.pop(uid, None)
        await cq.message.edit_text("❌ Session lost. Start again using 💸 Sell Account.", reply_markup=main_kb())
        return

    await cq.answer("🔄 Verifying logout...")
    await cq.message.edit_text("🔍 **ʀᴇᴄʜᴇᴄᴋɪɴɢ ᴀᴄᴛɪᴠᴇ ꜱᴇꜱꜱɪᴏɴꜱ ᴏɴ ᴏᴛʜᴇʀ ᴅᴇᴠɪᴄᴇꜱ…**")
    try:
        other_devices = await check_other_devices(session_client)
    except (AuthKeyUnregistered, UserDeactivated, UserDeactivatedBan):
        await cq.message.edit_text("❌ **ꜱᴇꜱꜱɪᴏɴ ᴇxᴘɪʀᴇᴅ ᴏʀ ʟᴏɢɢᴇᴅ ᴏᴜᴛ.** Please start again using 💸 Sell Account.", reply_markup=main_kb())
        user_state.pop(uid, None)
        return
    except Exception as e:
        logger.error(f"Verify logout error: {e}")
        await cq.message.edit_text(f"❌ ᴇʀʀᴏʀ ᴄʜᴇᴄᴋɪɴɢ ꜱᴇꜱꜱɪᴏɴꜱ: `{str(e)[:200]}`", reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔄 ʀᴇᴛʀʏ", callback_data="sell_verify_logout")],
            [InlineKeyboardButton("❌ ᴄᴀɴᴄᴇʟ", callback_data="sell_cancel_logout")]
        ]))
        return

    if other_devices > 0:
        await cq.answer(f"⚠️ {other_devices} other device(s) still active!", show_alert=True)
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ ᴄʜᴇᴄᴋᴇᴅ & ʟᴏɢɢᴇᴅ ᴏᴜᴛ", callback_data="sell_verify_logout")],
            [InlineKeyboardButton("❌ ᴄᴀɴᴄᴇʟ", callback_data="sell_cancel_logout")]
        ])
        await cq.message.edit_text(
            f"⚠️ **ᴀᴄᴛɪᴏɴ ʀᴇǫᴜɪʀᴇᴅ: ʟᴏɢᴏᴜᴛ ᴏᴛʜᴇʀ ᴅᴇᴠɪᴄᴇꜱ!**\n\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"📱 ᴘʜᴏɴᴇ: `{data.get('phone', '')}`\n"
            f"🖥 Active sessions on other devices: **{other_devices}**\n"
            f"━━━━━━━━━━━━━━━━\n\n"
            f"👉 Please open your Telegram App → Settings → Devices, and **ᴛᴇʀᴍɪɴᴀᴛᴇ ᴀʟʟ ᴏᴛʜᴇʀ ꜱᴇꜱꜱɪᴏɴꜱ** (log out from all other apps/devices).\n\n"
            f"⚠️ **ɪᴍᴘᴏʀᴛᴀɴᴛ:** DO NOT terminate the bot's session. If you click 'Terminate all other sessions', it might log the bot out too. To avoid this, log out from each of your other devices manually.\n\n"
            f"Once you have logged out from your other devices, click the button below to verify:",
            reply_markup=kb
        )
        return

    await proceed_with_sell_pipeline(client, cq.message, session_client, uid)


@app.on_callback_query(filters.regex("^sell_cancel_logout$"))
async def cb_sell_cancel_logout(client, cq: CallbackQuery):
    uid = cq.from_user.id
    data = user_state.pop(uid, {})
    session_client = data.get("client")
    if session_client:
        await safe_disconnect(session_client)
    if data.get("path"):
        clean_session_files(data["path"])
    await cq.message.edit_text("❌ **ꜱᴇʟʟ ᴄᴀɴᴄᴇʟʟᴇᴅ.**", reply_markup=main_kb())


@app.on_callback_query(filters.regex(r"^acc_yes_(\d+)$"))
@admin_only
async def cb_acc_yes(client, cq: CallbackQuery):
    uid = int(cq.data.split("acc_yes_")[1])
    data = user_state.get(uid, {})
    session_client = data.get("client")
    if not session_client:
        await cq.answer("Session expired.", show_alert=True)
        return
    await cq.message.edit_text("🔄 **ᴄʟᴇᴀɴɪɴɢ ᴀᴄᴄᴏᴜɴᴛ… ᴘʟᴇᴀꜱᴇ ᴡᴀɪᴛ.**")
    try:
        bot_me = await client.get_me()
        tfa_status = await do_set_2fa(session_client, data.get("old_2fa", ""))
        errors = await do_full_cleanup(session_client, bot_me.username or "")
        await safe_stop(session_client)
        user_state[uid]["client"] = None
        saved_path = move_session(data["path"], data.get("country", "Unknown"), data.get("phone", ""))
        log_action("add_account", f"phone={data.get('phone')} country={data.get('country')} cleaned=yes")
        user_state.pop(uid, None)
        err_text = "\n".join(f"• {item}" for item in errors) if errors else "None ✅"
        await cq.message.edit_text(
            f"✅ **ᴀᴄᴄᴏᴜɴᴛ ᴀᴅᴅᴇᴅ & ᴄʟᴇᴀɴᴇᴅ!**\n\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"📱 ᴘʜᴏɴᴇ: `{data.get('phone', '')}`\n"
            f"{flag(data.get('country', 'Unknown'))} Country: {data.get('country', 'Unknown')}\n"
            f"🔑 2FA: {tfa_status}\n"
            f"📂 ꜱᴀᴠᴇᴅ: `{saved_path}`\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"⚠️ Cleanup Errors:\n{err_text}"
        )
    except Exception as e:
        logger.error(f"acc_yes error: {e}")
        current = user_state.get(uid, {}).get("client")
        if current:
            await safe_stop(current)
        user_state.pop(uid, None)
        await cq.message.edit_text(f"❌ ᴇʀʀᴏʀ: `{str(e)[:300]}`")


@app.on_callback_query(filters.regex(r"^acc_no_(\d+)$"))
@admin_only
async def cb_acc_no(client, cq: CallbackQuery):
    uid = int(cq.data.split("acc_no_")[1])
    data = user_state.get(uid, {})
    session_client = data.get("client")
    if not session_client:
        await cq.answer("Session expired.", show_alert=True)
        return
    await cq.message.edit_text("🔄 **ꜱᴀᴠɪɴɢ ᴀᴄᴄᴏᴜɴᴛ ᴀꜱ-ɪꜱ…**")
    try:
        await safe_stop(session_client)
        user_state[uid]["client"] = None
        saved_path = move_session(data["path"], data.get("country", "Unknown"), data.get("phone", ""))
        log_action("add_account", f"phone={data.get('phone')} country={data.get('country')} cleaned=no")
        user_state.pop(uid, None)
        await cq.message.edit_text(
            f"✅ **ᴀᴄᴄᴏᴜɴᴛ ᴀᴅᴅᴇᴅ (ɴᴏ ᴄʟᴇᴀɴᴜᴘ)**\n\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"📱 ᴘʜᴏɴᴇ: `{data.get('phone', '')}`\n"
            f"{flag(data.get('country', 'Unknown'))} Country: {data.get('country', 'Unknown')}\n"
            f"📂 ꜱᴀᴠᴇᴅ: `{saved_path}`\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"⚠️ Name/2FA/Chats were NOT changed."
        )
    except Exception as e:
        logger.error(f"acc_no error: {e}")
        user_state.pop(uid, None)
        await cq.message.edit_text(f"❌ ᴇʀʀᴏʀ: `{str(e)[:300]}`")


@app.on_callback_query(filters.regex(r"^acc_discard_(\d+)$"))
@admin_only
async def cb_acc_discard(client, cq: CallbackQuery):
    uid = int(cq.data.split("acc_discard_")[1])
    data = user_state.get(uid, {})
    if data.get("client"):
        await safe_stop(data["client"])
    if data.get("path"):
        clean_session_files(data["path"])
    log_action("discard_account", data.get("phone", ""))
    user_state.pop(uid, None)
    await cq.message.edit_text("🗑 **ᴀᴄᴄᴏᴜɴᴛ ᴅɪꜱᴄᴀʀᴅᴇᴅ.** No changes made.")


@app.on_callback_query(filters.regex("^adm_stats$"))
@admin_only
async def cb_adm_stats(client, cq: CallbackQuery):
    users = get_all_users()
    countries = get_available_countries()
    pending_deps = db_execute("SELECT COUNT(*) as c FROM deposit_requests WHERE status='pending'", fetch="one")
    promo_used = db_execute("SELECT COUNT(*) as c FROM promo_usage", fetch="one")
    await cq.message.edit_text(
        f"📊 **ʙᴜꜱɪɴᴇꜱꜱ ꜱᴛᴀᴛɪꜱᴛɪᴄꜱ**\n\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"👥 ᴜꜱᴇʀꜱ: **{len(users)}**\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"🛒 ꜱᴏʟᴅ: **{int(get_stat('total_sold'))}**\n"
        f"💰 ʀᴇᴠᴇɴᴜᴇ: ₹**{get_stat('total_revenue'):.2f}**\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"💳 ᴛᴏᴛᴀʟ ᴅᴇᴘᴏꜱɪᴛꜱ: ₹**{get_stat('total_deposited'):.2f}**\n"
        f"⏳ Pending Deps: **{pending_deps['c'] if pending_deps else 0}**\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"📦 ʙᴏᴜɢʜᴛ ꜰʀᴏᴍ ᴜꜱᴇʀꜱ: **{int(get_stat('total_bought_from_users'))}**\n"
        f"❌ ʀᴇᴊᴇᴄᴛᴇᴅ ꜱᴇʟʟꜱ: **{int(get_stat('total_rejected'))}**\n"
        f"⏳ Pending Sells: **{len(get_pending_sell_orders())}**\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"📁 ꜱᴇꜱꜱɪᴏɴꜱ: **{sum(countries.values())}**\n"
        f"🌍 ᴄᴏᴜɴᴛʀɪᴇꜱ: **{len(countries)}**\n"
        f"🎟 ᴘʀᴏᴍᴏ ᴜꜱᴇᴅ: **{promo_used['c'] if promo_used else 0}**",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 ʙᴀᴄᴋ", callback_data="adm_back")]])
    )


@app.on_callback_query(filters.regex("^adm_buy_price$"))
@admin_only
async def cb_adm_buy_price(client, cq: CallbackQuery):
    user_state[ADMIN_ID] = {"step": "adm_set_buy_price"}
    await cq.message.edit_text(f"💰 **ᴅᴇꜰᴀᴜʟᴛ ʙᴜʏ ᴘʀɪᴄᴇ**\n\nCurrent: ₹{get_setting('default_buy_price')}\n\nEnter new price (₹):")


@app.on_callback_query(filters.regex("^adm_sell_price$"))
@admin_only
async def cb_adm_sell_price(client, cq: CallbackQuery):
    user_state[ADMIN_ID] = {"step": "adm_set_sell_price"}
    await cq.message.edit_text(f"💵 **ᴅᴇꜰᴀᴜʟᴛ ꜱᴇʟʟ ᴘʀɪᴄᴇ**\n\nCurrent: ₹{get_setting('default_sell_price')}\n\nEnter new price (₹):")


@app.on_callback_query(filters.regex("^adm_c_buy$"))
@admin_only
async def cb_adm_c_buy(client, cq: CallbackQuery):
    user_state[ADMIN_ID] = {"step": "adm_c_buy_name"}
    await cq.message.edit_text("🏷 **ᴄᴏᴜɴᴛʀʏ ʙᴜʏ ᴘʀɪᴄᴇ**\n\nEnter country name:")


@app.on_callback_query(filters.regex("^adm_c_sell$"))
@admin_only
async def cb_adm_c_sell(client, cq: CallbackQuery):
    user_state[ADMIN_ID] = {"step": "adm_c_sell_name"}
    await cq.message.edit_text("🏷 **ᴄᴏᴜɴᴛʀʏ ꜱᴇʟʟ ᴘʀɪᴄᴇ**\n\nEnter country name:")


@app.on_callback_query(filters.regex("^adm_custom_price$"))
@admin_only
async def cb_adm_custom_price(client, cq: CallbackQuery):
    countries = get_available_countries()
    if not countries:
        await cq.message.edit_text("❌ No accounts available.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 ʙᴀᴄᴋ", callback_data="adm_back")]]))
        return
    buttons = [[InlineKeyboardButton(f"{flag(country)} {country} ({count})", callback_data=f"cp_country_{country}")] for country, count in countries.items()]
    buttons.append([InlineKeyboardButton("🔙 ʙᴀᴄᴋ", callback_data="adm_back")])
    await cq.message.edit_text("💲 **ᴄᴜꜱᴛᴏᴍ ᴘʀɪᴄᴇ — ꜱᴇʟᴇᴄᴛ ᴄᴏᴜɴᴛʀʏ:**", reply_markup=InlineKeyboardMarkup(buttons))


@app.on_callback_query(filters.regex(r"^cp_country_(.+)$"))
@admin_only
async def cb_cp_country(client, cq: CallbackQuery):
    country = cq.data[11:]
    sessions = get_country_sessions(country)
    if not sessions:
        await cq.answer("No sessions!", show_alert=True)
        return
    buttons = []
    for session_file in sessions[:25]:
        phone = session_file.replace(".session", "")
        custom = get_custom_price(session_file)
        label = f"📱 {phone}" + (f" [₹{custom['price']:.0f}]" if custom else "")
        buttons.append([InlineKeyboardButton(label, callback_data=f"cp_set_{country}__{session_file}")])
    buttons.append([InlineKeyboardButton("🔙 ʙᴀᴄᴋ", callback_data="adm_custom_price")])
    await cq.message.edit_text(f"💲 **{flag(country)} {country}** — Select Account:", reply_markup=InlineKeyboardMarkup(buttons))


@app.on_callback_query(filters.regex(r"^cp_set_(.+)__(.+\.session)$"))
@admin_only
async def cb_cp_set(client, cq: CallbackQuery):
    raw = cq.data[7:]
    sep = raw.index("__")
    country = raw[:sep]
    session_file = raw[sep + 2:]
    phone = session_file.replace(".session", "")
    custom = get_custom_price(session_file)
    current_price = f"₹{custom['price']:.0f}" if custom else f"Default (₹{get_buy_price(country):.0f})"
    user_state[ADMIN_ID] = {"step": "adm_cp_price", "country": country, "sf": session_file, "phone": phone}
    await cq.message.edit_text(
        f"💲 **ᴄᴜꜱᴛᴏᴍ ᴘʀɪᴄᴇ**\n\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"📱 ᴘʜᴏɴᴇ: `{phone}`\n"
        f"{flag(country)} Country: {country}\n"
        f"💰 ᴄᴜʀʀᴇɴᴛ ᴘʀɪᴄᴇ: {current_price}\n"
        f"━━━━━━━━━━━━━━━━\n\n"
        f"Enter new custom price (₹):\n"
        f"Or type `remove` to remove custom price:"
    )


@app.on_callback_query(filters.regex("^adm_acc_note$"))
@admin_only
async def cb_adm_acc_note(client, cq: CallbackQuery):
    countries = get_available_countries()
    if not countries:
        await cq.message.edit_text("❌ No accounts.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 ʙᴀᴄᴋ", callback_data="adm_back")]]))
        return
    buttons = [[InlineKeyboardButton(f"{flag(country)} {country} ({count})", callback_data=f"note_country_{country}")] for country, count in countries.items()]
    buttons.append([InlineKeyboardButton("🔙 ʙᴀᴄᴋ", callback_data="adm_back")])
    await cq.message.edit_text("📝 **ᴀᴄᴄᴏᴜɴᴛ ɴᴏᴛᴇ — ꜱᴇʟᴇᴄᴛ ᴄᴏᴜɴᴛʀʏ:**", reply_markup=InlineKeyboardMarkup(buttons))


@app.on_callback_query(filters.regex(r"^note_country_(.+)$"))
@admin_only
async def cb_note_country(client, cq: CallbackQuery):
    country = cq.data[13:]
    sessions = get_country_sessions(country)
    if not sessions:
        await cq.answer("No sessions!", show_alert=True)
        return
    buttons = []
    for session_file in sessions[:25]:
        phone = session_file.replace(".session", "")
        note = get_account_note(session_file)
        label = f"📱 {phone}" + (" 📝" if note else "")
        buttons.append([InlineKeyboardButton(label, callback_data=f"note_set_{country}__{session_file}")])
    buttons.append([InlineKeyboardButton("🔙 ʙᴀᴄᴋ", callback_data="adm_acc_note")])
    await cq.message.edit_text(f"📝 **{flag(country)} {country}** — Select Account:", reply_markup=InlineKeyboardMarkup(buttons))


@app.on_callback_query(filters.regex(r"^note_set_(.+)__(.+\.session)$"))
@admin_only
async def cb_note_set(client, cq: CallbackQuery):
    raw = cq.data[9:]
    sep = raw.index("__")
    country = raw[:sep]
    session_file = raw[sep + 2:]
    phone = session_file.replace(".session", "")
    note = get_account_note(session_file)
    user_state[ADMIN_ID] = {"step": "adm_note_text", "country": country, "sf": session_file, "phone": phone}
    await cq.message.edit_text(
        f"📝 **ᴀᴄᴄᴏᴜɴᴛ ɴᴏᴛᴇ**\n\n"
        f"📱 `{phone}` | {flag(country)} {country}\n"
        f"Current Note: {note or 'None'}\n\n"
        f"Enter new note (or `remove` to delete):"
    )


@app.on_callback_query(filters.regex("^adm_add_bal$"))
@admin_only
async def cb_adm_add_bal(client, cq: CallbackQuery):
    user_state[ADMIN_ID] = {"step": "adm_add_bal_uid"}
    await cq.message.edit_text("➕ **ᴀᴅᴅ ʙᴀʟᴀɴᴄᴇ**\n\nEnter User ID:")


@app.on_callback_query(filters.regex("^adm_deduct_bal$"))
@admin_only
async def cb_adm_deduct_bal(client, cq: CallbackQuery):
    user_state[ADMIN_ID] = {"step": "adm_deduct_bal_uid"}
    await cq.message.edit_text("➖ **ᴅᴇᴅᴜᴄᴛ ʙᴀʟᴀɴᴄᴇ**\n\nEnter User ID:")


@app.on_callback_query(filters.regex("^adm_add_acc$"))
@admin_only
async def cb_adm_add_acc(client, cq: CallbackQuery):
    user_state[ADMIN_ID] = {"step": "adm_acc_country"}
    await cq.message.edit_text(
        f"📱 **ᴀᴅᴅ ᴀᴄᴄᴏᴜɴᴛ**\n\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"Step 1: Enter **ᴄᴏᴜɴᴛʀʏ ɴᴀᴍᴇ**\n"
        f"(e.g., India, USA, UK, Russia)\n"
        f"━━━━━━━━━━━━━━━━"
    )


@app.on_callback_query(filters.regex("^adm_manage$"))
@admin_only
async def cb_adm_manage(client, cq: CallbackQuery):
    countries = get_available_countries()
    if not countries:
        await cq.message.edit_text("📋 No accounts.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 ʙᴀᴄᴋ", callback_data="adm_back")]]))
        return
    buttons = [[InlineKeyboardButton(f"{flag(country)} {country} ({count})", callback_data=f"mng_c_{country}")] for country, count in countries.items()]
    buttons.append([InlineKeyboardButton("🔙 ʙᴀᴄᴋ", callback_data="adm_back")])
    await cq.message.edit_text("📋 **ᴍᴀɴᴀɢᴇ ᴀᴄᴄᴏᴜɴᴛꜱ** — Select Country:", reply_markup=InlineKeyboardMarkup(buttons))


@app.on_callback_query(filters.regex(r"^mng_c_(.+)$"))
@admin_only
async def cb_mng_country(client, cq: CallbackQuery):
    country = cq.data[6:]
    sessions = get_country_sessions(country)
    if not sessions:
        await cq.answer("No sessions!", show_alert=True)
        return
    buttons = []
    for session_file in sessions[:25]:
        phone = session_file.replace(".session", "")
        custom = get_custom_price(session_file)
        note = get_account_note(session_file)
        extras = ""
        if custom:
            extras += f" 💲₹{custom['price']:.0f}"
        if note:
            extras += " 📝"
        buttons.append([InlineKeyboardButton(f"📱 {phone}{extras}", callback_data=f"mng_n_{country}__{phone}")])
    buttons.append([InlineKeyboardButton("🔙 ʙᴀᴄᴋ", callback_data="adm_manage")])
    await cq.message.edit_text(f"📋 **{flag(country)} {country}** — {len(sessions)} sessions", reply_markup=InlineKeyboardMarkup(buttons))


@app.on_callback_query(filters.regex(r"^mng_n_(.+)__(.+)$"))
@admin_only
async def cb_mng_num(client, cq: CallbackQuery):
    raw = cq.data[6:]
    sep = raw.index("__")
    country = raw[:sep]
    phone = raw[sep + 2:]
    session_file = f"{phone}.session"
    custom = get_custom_price(session_file)
    note = get_account_note(session_file)
    price = f"₹{custom['price']:.0f}" if custom else f"Default ₹{get_buy_price(country):.0f}"
    await cq.message.edit_text(
        f"📱 **{phone}** | {flag(country)} {country}\n\n"
        f"💰 ᴘʀɪᴄᴇ: {price}\n"
        f"📝 ɴᴏᴛᴇ: {note or 'None'}\n\n"
        f"Choose action:",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔑 ᴄʜᴇᴄᴋ ᴏᴛᴘ", callback_data=f"adm_otp_{country}__{phone}")],
            [InlineKeyboardButton("🔍 ꜱᴘᴀᴍʙᴏᴛ ᴄʜᴇᴄᴋ", callback_data=f"adm_spam_chk_{country}__{phone}")],
            [InlineKeyboardButton("💲 ꜱᴇᴛ ᴄᴜꜱᴛᴏᴍ ᴘʀɪᴄᴇ", callback_data=f"cp_set_{country}__{session_file}")],
            [InlineKeyboardButton("📝 ꜱᴇᴛ ɴᴏᴛᴇ", callback_data=f"note_set_{country}__{session_file}")],
            [InlineKeyboardButton("🚪 ʟᴏɢᴏᴜᴛ & ᴅᴇʟᴇᴛᴇ", callback_data=f"adm_lo_{country}__{phone}")],
            [InlineKeyboardButton("🗑 ᴅᴇʟᴇᴛᴇ ᴏɴʟʏ", callback_data=f"adm_del_{country}__{phone}")],
            [InlineKeyboardButton("🔙 ʙᴀᴄᴋ", callback_data=f"mng_c_{country}")],
        ])
    )


@app.on_callback_query(filters.regex(r"^adm_otp_(.+)__(.+)$"))
@admin_only
async def cb_adm_otp(client, cq: CallbackQuery):
    raw = cq.data[8:]
    sep = raw.index("__")
    country = raw[:sep]
    phone = raw[sep + 2:]
    base_path = os.path.join(SESSIONS_DIR, country, phone)
    if not os.path.exists(base_path + ".session"):
        await cq.answer("Session file not found!", show_alert=True)
        return
    lock = get_otp_lock(base_path)
    if lock.locked():
        await cq.answer("⏳ Already checking, please wait…", show_alert=True)
        return
    await cq.answer("🔄 Checking OTP...")
    otp = None
    session_client = None
    async with lock:
        try:
            session_client = build_session_client(base_path)
            await session_client.start()
            async for msg in session_client.get_chat_history(777000, limit=10):
                if not msg.text:
                    continue
                cleaned = msg.text.replace("-", "").replace(" ", "")
                match = re.search(r"(?<!\d)(\d{5,6})(?!\d)", cleaned)
                if match:
                    otp = match.group(1)
                    break
            await safe_stop(session_client)
        except FloodWait as e:
            if session_client:
                await safe_stop(session_client)
            await safe_edit_text(
                cq.message,
                f"⏳ Rate limited. Wait {e.value + 2}s and retry.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 ʙᴀᴄᴋ", callback_data=f"mng_n_{country}__{phone}")]])
            )
            return
        except Exception as e:
            if session_client:
                await safe_stop(session_client)
            await safe_edit_text(
                cq.message,
                f"❌ ᴇʀʀᴏʀ: `{type(e).__name__}: {str(e)[:150]}`",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 ʙᴀᴄᴋ", callback_data=f"mng_n_{country}__{phone}")]])
            )
            return
    result_text = f"🔑 **ᴏᴛᴘ ᴄʜᴇᴄᴋ**\n\n📱 `{phone}`\n"
    result_text += f"🔑 ᴏᴛᴘ: `{otp}`" if otp else "⚠️ No OTP found in last 10 messages from Telegram."
    await safe_edit_text(
        cq.message,
        result_text,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 ʙᴀᴄᴋ", callback_data=f"mng_n_{country}__{phone}")]])
    )


@app.on_callback_query(filters.regex(r"^adm_spam_chk_(.+)__(.+)$"))
@admin_only
async def cb_adm_spam_chk(client, cq: CallbackQuery):
    raw = cq.data[13:]
    sep = raw.index("__")
    country = raw[:sep]
    phone = raw[sep + 2:]
    base_path = os.path.join(SESSIONS_DIR, country, phone)
    if not os.path.exists(base_path + ".session"):
        await cq.answer("Session not found!", show_alert=True)
        return
    lock = get_otp_lock(base_path)
    if lock.locked():
        await cq.answer("⏳ Already checking, please wait…", show_alert=True)
        return
    await cq.answer("🔄 Checking SpamBot...")
    session_client = None
    async with lock:
        try:
            session_client = build_session_client(base_path)
            await session_client.start()
            result = await check_spambot(session_client)
            await safe_stop(session_client)
        except Exception as e:
            if session_client:
                await safe_stop(session_client)
            await safe_edit_text(cq.message, f"❌ ᴇʀʀᴏʀ: `{str(e)[:200]}`")
            return
    clean = is_spam_free(result)
    await safe_edit_text(
        cq.message,
        f"🔍 **ꜱᴘᴀᴍʙᴏᴛ ᴄʜᴇᴄᴋ**\n\n📱 `{phone}` — {'✅ Clean' if clean else '⚠️ Flagged'}\n\n`{result[:500]}`",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 ʙᴀᴄᴋ", callback_data=f"mng_n_{country}__{phone}")]])
    )


@app.on_callback_query(filters.regex(r"^adm_lo_(.+)__(.+)$"))
@admin_only
async def cb_adm_lo(client, cq: CallbackQuery):
    raw = cq.data[7:]
    sep = raw.index("__")
    country = raw[:sep]
    phone = raw[sep + 2:]
    base_path = os.path.join(SESSIONS_DIR, country, phone)
    await safe_edit_text(cq.message, "🔄 Logging out...")
    session_client = None
    async with get_otp_lock(base_path):
        try:
            if os.path.exists(base_path + ".session"):
                session_client = build_session_client(base_path)
                await session_client.start()
                await session_client.log_out()
        except (UserDeactivated, UserDeactivatedBan, AuthKeyUnregistered):
            pass
        except Exception as e:
            logger.warning(f"Admin logout: {e}")
        finally:
            if session_client:
                await safe_stop(session_client)
    clean_session_files(base_path)
    log_action("admin_logout", f"{phone} from {country}")
    await safe_edit_text(
        cq.message,
        f"✅ Logged out & deleted `{phone}`",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 ʙᴀᴄᴋ", callback_data=f"mng_c_{country}")]])
    )


@app.on_callback_query(filters.regex(r"^adm_del_(.+)__(.+)$"))
@admin_only
async def cb_adm_del(client, cq: CallbackQuery):
    raw = cq.data[8:]
    sep = raw.index("__")
    country = raw[:sep]
    phone = raw[sep + 2:]
    clean_session_files(os.path.join(SESSIONS_DIR, country, phone))
    log_action("admin_delete", f"{phone} from {country}")
    await cq.message.edit_text(
        f"✅ Deleted `{phone}`",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 ʙᴀᴄᴋ", callback_data=f"mng_c_{country}")]])
    )


@app.on_callback_query(filters.regex("^adm_buy_tog$"))
@admin_only
async def cb_adm_buy_tog(client, cq: CallbackQuery):
    new_value = "off" if get_setting("buy_feature") == "on" else "on"
    set_setting("buy_feature", new_value)
    log_action("toggle_buy", new_value)
    await cq.message.edit_text(
        f"🛒 **ʙᴜʏ ꜰᴇᴀᴛᴜʀᴇ** — {'✅' if new_value == 'on' else '❌'} {new_value.upper()}",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔄 ᴛᴏɢɢʟᴇ", callback_data="adm_buy_tog")],
            [InlineKeyboardButton("🔙 ʙᴀᴄᴋ", callback_data="adm_back")],
        ])
    )


@app.on_callback_query(filters.regex("^adm_sell_tog$"))
@admin_only
async def cb_adm_sell_tog(client, cq: CallbackQuery):
    new_value = "off" if get_setting("sell_feature") == "on" else "on"
    set_setting("sell_feature", new_value)
    log_action("toggle_sell", new_value)
    await cq.message.edit_text(
        f"💸 **ꜱᴇʟʟ ꜰᴇᴀᴛᴜʀᴇ** — {'✅' if new_value == 'on' else '❌'} {new_value.upper()}",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔄 ᴛᴏɢɢʟᴇ", callback_data="adm_sell_tog")],
            [InlineKeyboardButton("🔙 ʙᴀᴄᴋ", callback_data="adm_back")],
        ])
    )


@app.on_callback_query(filters.regex("^adm_maint$"))
@admin_only
async def cb_adm_maint(client, cq: CallbackQuery):
    new_value = "off" if get_setting("maintenance_mode") == "on" else "on"
    set_setting("maintenance_mode", new_value)
    log_action("toggle_maintenance", new_value)
    await cq.message.edit_text(
        f"🔧 **ᴍᴀɪɴᴛᴇɴᴀɴᴄᴇ** — {'🔧' if new_value == 'on' else '✅'} {new_value.upper()}",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔄 ᴛᴏɢɢʟᴇ", callback_data="adm_maint")],
            [InlineKeyboardButton("🔙 ʙᴀᴄᴋ", callback_data="adm_back")],
        ])
    )


@app.on_callback_query(filters.regex("^adm_spam$"))
@admin_only
async def cb_adm_spam(client, cq: CallbackQuery):
    current = get_setting("accept_spam_accounts")
    await cq.message.edit_text(
        f"🛡 **ꜱᴘᴀᴍ ᴄᴏɴᴛʀᴏʟ**\n\n"
        f"Accept Spam Accounts: {'✅' if current == 'on' else '❌'} **{current.upper()}**\n\n"
        f"ON  → Accept flagged accounts from sellers\n"
        f"OFF → Auto-reject flagged accounts",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔄 ᴛᴏɢɢʟᴇ", callback_data="adm_spam_tog")],
            [InlineKeyboardButton("🔙 ʙᴀᴄᴋ", callback_data="adm_back")],
        ])
    )


@app.on_callback_query(filters.regex("^adm_spam_tog$"))
@admin_only
async def cb_adm_spam_tog(client, cq: CallbackQuery):
    new_value = "off" if get_setting("accept_spam_accounts") == "on" else "on"
    set_setting("accept_spam_accounts", new_value)
    log_action("toggle_spam", new_value)
    await cq.message.edit_text(
        f"🛡 **ꜱᴘᴀᴍ ᴀᴄᴄᴇᴘᴛ** — {'✅' if new_value == 'on' else '❌'} {new_value.upper()}",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔄 ᴛᴏɢɢʟᴇ", callback_data="adm_spam_tog")],
            [InlineKeyboardButton("🔙 ʙᴀᴄᴋ", callback_data="adm_back")],
        ])
    )


@app.on_callback_query(filters.regex("^adm_promo$"))
@admin_only
async def cb_adm_promo(client, cq: CallbackQuery):
    promos = db_execute("SELECT * FROM promo_codes ORDER BY created DESC LIMIT 20", fetch="all") or []
    lines = ["🎟 **ᴘʀᴏᴍᴏ ᴄᴏᴅᴇꜱ**\n"]
    for promo in promos:
        lines.append(f"{'✅' if promo['active'] else '❌'} `{promo['code']}` — ₹{promo['discount']:.0f} off | {promo['used']}/{promo['max_uses']} used")
    if not promos:
        lines.append("No promo codes yet.")
    await cq.message.edit_text(
        "\n".join(lines),
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("➕ ᴄʀᴇᴀᴛᴇ ᴘʀᴏᴍᴏ", callback_data="adm_promo_create"),
             InlineKeyboardButton("❌ ᴅɪꜱᴀʙʟᴇ ᴘʀᴏᴍᴏ", callback_data="adm_promo_disable")],
            [InlineKeyboardButton("🔙 ʙᴀᴄᴋ", callback_data="adm_back")],
        ])
    )


@app.on_callback_query(filters.regex("^adm_promo_create$"))
@admin_only
async def cb_adm_promo_create(client, cq: CallbackQuery):
    user_state[ADMIN_ID] = {"step": "adm_promo_code"}
    await cq.message.edit_text("🎟 **ᴄʀᴇᴀᴛᴇ ᴘʀᴏᴍᴏ ᴄᴏᴅᴇ**\n\nStep 1: Enter promo code name:\nExample: `FELIX50`")


@app.on_callback_query(filters.regex("^adm_promo_disable$"))
@admin_only
async def cb_adm_promo_disable(client, cq: CallbackQuery):
    user_state[ADMIN_ID] = {"step": "adm_promo_dis_code"}
    await cq.message.edit_text("❌ **ᴅɪꜱᴀʙʟᴇ ᴘʀᴏᴍᴏ**\n\nEnter code to disable:")


@app.on_callback_query(filters.regex("^adm_fj$"))
@admin_only
async def cb_adm_fj(client, cq: CallbackQuery):
    channels = get_force_join_channels()
    text = "📢 **ꜰᴏʀᴄᴇ ᴊᴏɪɴ ᴄʜᴀɴɴᴇʟꜱ**\n\n━━━━━━━━━━━━━━━━\n"
    text += "\n".join(f"• @{channel['username']}" for channel in channels) if channels else "None set."
    text += "\n━━━━━━━━━━━━━━━━"
    buttons = [[InlineKeyboardButton(f"🗑 Remove @{channel['username']}", callback_data=f"fj_rm_{channel['username']}")] for channel in channels]
    buttons += [
        [InlineKeyboardButton("➕ ᴀᴅᴅ ᴄʜᴀɴɴᴇʟ", callback_data="fj_add")],
        [InlineKeyboardButton("🔙 ʙᴀᴄᴋ", callback_data="adm_back")],
    ]
    await cq.message.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons))


@app.on_callback_query(filters.regex("^fj_add$"))
@admin_only
async def cb_fj_add(client, cq: CallbackQuery):
    user_state[ADMIN_ID] = {"step": "adm_fj_add"}
    await cq.message.edit_text("📢 Enter channel username (without @):")


@app.on_callback_query(filters.regex(r"^fj_rm_(.+)$"))
@admin_only
async def cb_fj_rm(client, cq: CallbackQuery):
    username = cq.data[6:]
    remove_force_join(username)
    log_action("fj_remove", username)
    await cq.answer(f"✅ Removed @{username}", show_alert=True)
    await cb_adm_fj(client, cq)


@app.on_callback_query(filters.regex("^adm_users$"))
@admin_only
async def cb_adm_users(client, cq: CallbackQuery):
    await cq.message.edit_text(
        "👥 **ᴍᴀɴᴀɢᴇ ᴜꜱᴇʀꜱ**",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔍 ꜰɪɴᴅ ᴜꜱᴇʀ", callback_data="adm_find_user"),
             InlineKeyboardButton("🚫 ʙᴀɴ ᴜꜱᴇʀ", callback_data="adm_ban_u")],
            [InlineKeyboardButton("✅ ᴜɴʙᴀɴ ᴜꜱᴇʀ", callback_data="adm_unban_u"),
             InlineKeyboardButton("📋 ʙᴀɴɴᴇᴅ ʟɪꜱᴛ", callback_data="adm_banned_list")],
            [InlineKeyboardButton("👥 ᴀʟʟ ᴜꜱᴇʀꜱ", callback_data="adm_all_users"),
             InlineKeyboardButton("📤 ᴍᴇꜱꜱᴀɢᴇ ᴜꜱᴇʀ", callback_data="adm_msg_user")],
            [InlineKeyboardButton("🔙 ʙᴀᴄᴋ", callback_data="adm_back")],
        ])
    )


@app.on_callback_query(filters.regex("^adm_find_user$"))
@admin_only
async def cb_adm_find_user(client, cq: CallbackQuery):
    user_state[ADMIN_ID] = {"step": "adm_find_user"}
    await cq.message.edit_text("🔍 **ꜰɪɴᴅ ᴜꜱᴇʀ**\n\nEnter User ID or username:")


@app.on_callback_query(filters.regex("^adm_ban_u$"))
@admin_only
async def cb_adm_ban(client, cq: CallbackQuery):
    user_state[ADMIN_ID] = {"step": "adm_ban_uid"}
    await cq.message.edit_text("🚫 **ʙᴀɴ ᴜꜱᴇʀ**\n\nEnter User ID:")


@app.on_callback_query(filters.regex("^adm_unban_u$"))
@admin_only
async def cb_adm_unban(client, cq: CallbackQuery):
    user_state[ADMIN_ID] = {"step": "adm_unban_uid"}
    await cq.message.edit_text("✅ **ᴜɴʙᴀɴ ᴜꜱᴇʀ**\n\nEnter User ID:")


@app.on_callback_query(filters.regex("^adm_msg_user$"))
@admin_only
async def cb_adm_msg_user(client, cq: CallbackQuery):
    user_state[ADMIN_ID] = {"step": "adm_msg_user_uid"}
    await cq.message.edit_text("📤 **ᴍᴇꜱꜱᴀɢᴇ ᴜꜱᴇʀ**\n\nEnter User ID:")


@app.on_callback_query(filters.regex("^adm_banned_list$"))
@admin_only
async def cb_adm_banned_list(client, cq: CallbackQuery):
    users = db_execute("SELECT * FROM users WHERE is_banned=1", fetch="all") or []
    lines = ["🚫 **ʙᴀɴɴᴇᴅ ᴜꜱᴇʀꜱ**\n"]
    for user in users:
        lines.append(f"• `{user['id']}` — {user['name']} | {user['ban_reason']}")
    if not users:
        lines.append("No banned users.")
    await cq.message.edit_text("\n".join(lines), reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 ʙᴀᴄᴋ", callback_data="adm_users")]]))


@app.on_callback_query(filters.regex("^adm_all_users$"))
@admin_only
async def cb_adm_all_users(client, cq: CallbackQuery):
    users = get_all_users()
    lines = [f"👥 **ᴀʟʟ ᴜꜱᴇʀꜱ** ({len(users)})\n"]
    for user in users[:30]:
        lines.append(f"{'🚫' if user['is_banned'] else '✅'} `{user['id']}` — {user['name']} | ₹{user['balance']:.0f}")
    if len(users) > 30:
        lines.append(f"\n_(Showing 30 of {len(users)})_")
    await cq.message.edit_text("\n".join(lines), reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 ʙᴀᴄᴋ", callback_data="adm_users")]]))


@app.on_callback_query(filters.regex("^adm_pending_sells$"))
@admin_only
async def cb_adm_pending_sells(client, cq: CallbackQuery):
    pending = get_pending_sell_orders()
    if not pending:
        await cq.message.edit_text("📦 No pending sell orders.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 ʙᴀᴄᴋ", callback_data="adm_back")]]))
        return
    await cq.answer(f"{len(pending)} pending orders")
    for order in pending:
        user = get_user(order["user_id"])
        spam = row_value(order, "spam_result", "")[:300] or "Not checked"
        clean = is_spam_free(spam)
        upi_id = row_value(order, "upi_id", "")
        upi_line = f"\n💳 ᴜᴘɪ: `{upi_id}`" if upi_id else "\n💳 ᴘᴀʏᴏᴜᴛ: Wallet"
        old_pwd_val = row_value(order, "old_2fa", "") or "None"
        new_pwd_val = row_value(order, "new_2fa", "") or "None"
        try:
            await cq.message.reply(
                f"📦 **Sell Order #{order['id']}**\n\n"
                f"━━━━━━━━━━━━━━━━\n"
                f"👤 {row_value(user, 'name', 'Unknown')} (@{row_value(user, 'username', '')})\n"
                f"🆔 `{order['user_id']}`\n"
                f"📱 `{order['phone_number']}`\n"
                f"🌍 ᴄᴏᴜɴᴛʀʏ: {row_value(order, 'country', 'Unknown')}\n"
                f"📅 {str(order['timestamp'])[:16]}\n"
                f"🔑 Old 2FA: `{old_pwd_val}`\n"
                f"🔑 New 2FA: `{new_pwd_val}`\n"
                f"🛡 {'✅ Clean' if clean else '⚠️ Flagged'}{upi_line}\n"
                f"━━━━━━━━━━━━━━━━\n"
                f"`{spam}`",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("✅ ᴀᴘᴘʀᴏᴠᴇ & ᴘᴀʏ", callback_data=f"sell_apr_{order['id']}")],
                    [InlineKeyboardButton("❌ ʀᴇᴊᴇᴄᴛ", callback_data=f"sell_rej_{order['id']}")],
                    [InlineKeyboardButton("🔍 ʀᴇᴄʜᴇᴄᴋ", callback_data=f"sell_rchk_{order['id']}")],
                ])
            )
        except Exception as e:
            logger.error(f"Pending sell display: {e}")


@app.on_callback_query(filters.regex(r"^sell_apr_(\d+)$"))
@admin_only
async def cb_sell_apr(client, cq: CallbackQuery):
    order_id = int(cq.data[9:])
    order = db_execute("SELECT * FROM sell_orders WHERE id=?", (order_id,), fetch="one")
    if not order:
        await cq.answer("Not found!", show_alert=True)
        return
    suggested = get_sell_price(row_value(order, "country", "") or "default")
    user_state[ADMIN_ID] = {"step": "sell_apr_amount", "sell_oid": order_id}
    await cq.message.edit_text(
        f"✅ **Approve Order #{order_id}**\n\n"
        f"📱 `{order['phone_number']}`\n"
        f"💳 ᴜꜱᴇʀ ᴜᴘɪ: `{row_value(order, 'upi_id', '') or 'Not provided (wallet)'}`\n"
        f"💰 ꜱᴜɢɢᴇꜱᴛᴇᴅ: ₹{suggested:.0f}\n\n"
        f"Enter amount to pay user (₹):"
    )


@app.on_callback_query(filters.regex(r"^sell_rej_(\d+)$"))
@admin_only
async def cb_sell_rej(client, cq: CallbackQuery):
    user_state[ADMIN_ID] = {"step": "sell_rej_reason", "sell_oid": int(cq.data[9:])}
    await cq.message.edit_text(f"❌ **Reject Order #{int(cq.data[9:])}**\n\nEnter reason (or `skip`):")


@app.on_callback_query(filters.regex(r"^sell_rchk_(\d+)$"))
@admin_only
async def cb_sell_rchk(client, cq: CallbackQuery):
    order_id = int(cq.data[10:])
    order = db_execute("SELECT * FROM sell_orders WHERE id=?", (order_id,), fetch="one")
    session_path = row_value(order, "session_path", "")
    if not order or not session_path:
        await cq.answer("Session not found!", show_alert=True)
        return
    if not os.path.exists(session_path):
        await cq.answer("Session file missing!", show_alert=True)
        return
    lock = get_otp_lock(session_path)
    if lock.locked():
        await cq.answer("⏳ Already checking, please wait…", show_alert=True)
        return
    await cq.answer("🔄 Checking...")
    session_client = None
    async with lock:
        try:
            session_client = build_session_client(session_path.replace(".session", ""))

            async def _check():
                await session_client.start()
                return await check_spambot(session_client)

            result = await asyncio.wait_for(_check(), timeout=25)
            await safe_stop(session_client)
            db_execute("UPDATE sell_orders SET spam_result=? WHERE id=?", (result, order_id))
            await safe_edit_text(
                cq.message,
                f"🔍 **Recheck #{order_id}**\n\n📱 `{order['phone_number']}` — {'✅ Clean' if is_spam_free(result) else '⚠️ Flagged'}\n\n`{result[:400]}`",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("✅ ᴀᴘᴘʀᴏᴠᴇ", callback_data=f"sell_apr_{order_id}")],
                    [InlineKeyboardButton("❌ ʀᴇᴊᴇᴄᴛ", callback_data=f"sell_rej_{order_id}")],
                ])
            )
        except FloodWait as e:
            if session_client:
                await safe_stop(session_client)
            await safe_edit_text(
                cq.message,
                f"⏳ Rate limited. Wait {e.value + 2}s and retry.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 ʙᴀᴄᴋ", callback_data="adm_pending_sells")]])
            )
        except Exception as e:
            if session_client:
                await safe_stop(session_client)
            await safe_edit_text(
                cq.message,
                f"❌ ᴇʀʀᴏʀ: `{type(e).__name__}: {str(e)[:200]}`",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 ʙᴀᴄᴋ", callback_data="adm_pending_sells")]])
            )


@app.on_callback_query(filters.regex("^adm_pending_deps$"))
@admin_only
async def cb_adm_pending_deps(client, cq: CallbackQuery):
    deps = db_execute("SELECT * FROM deposit_requests WHERE status='pending' ORDER BY id DESC LIMIT 20", fetch="all") or []
    if not deps:
        await cq.message.edit_text("💳 No pending deposits.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 ʙᴀᴄᴋ", callback_data="adm_back")]]))
        return
    for dep in deps:
        user = get_user(dep["user_id"])
        try:
            await cq.message.reply(
                f"💳 **Deposit #{dep['id']}**\n\n👤 {row_value(user, 'name', 'Unknown')} (`{dep['user_id']}`)\n💰 ₹{dep['amount']:.2f}\n📅 {str(dep['timestamp'])[:16]}",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("✅ ᴀᴘᴘʀᴏᴠᴇ", callback_data=f"dep_apr_{dep['id']}_{dep['user_id']}")],
                    [InlineKeyboardButton("❌ ʀᴇᴊᴇᴄᴛ", callback_data=f"dep_rej_{dep['id']}_{dep['user_id']}")],
                ])
            )
        except Exception:
            pass
    await cq.answer(f"{len(deps)} pending deposits")


@app.on_callback_query(filters.regex(r"^dep_verify_(\d+)$"))
async def cb_dep_verify(client, cq: CallbackQuery):
    dep_id = int(cq.data.split("_")[2])
    uid = cq.from_user.id
    dep = db_execute("SELECT * FROM deposit_requests WHERE id=? AND user_id=?", (dep_id, uid), fetch="one")
    if not dep or dep["status"] != "pending":
        await cq.answer("Request not found or already processed.", show_alert=True)
        return
    if not FAMPAY_AUTO_VERIFY:
        await cq.answer("Auto-verify not set up. Please send a screenshot instead.", show_alert=True)
        return

    lock = get_otp_lock(f"dep_verify_{dep_id}")
    if lock.locked():
        await cq.answer("⏳ Already verifying, please wait…", show_alert=True)
        return

    await cq.answer()
    try:
        await cq.message.edit_caption("🔄 **ᴄʜᴇᴄᴋɪɴɢ ᴘᴀʏᴍᴇɴᴛ…**\n\nPlease wait a few seconds.")
    except Exception:
        pass

    async with lock:
        # Re-check status after acquiring lock — a concurrent click or admin
        # manual approval may have already settled this deposit.
        fresh = db_execute("SELECT status FROM deposit_requests WHERE id=?", (dep_id,), fetch="one")
        if not fresh or fresh["status"] != "pending":
            await cq.answer("Already processed.", show_alert=True)
            return

        result = await asyncio.to_thread(verify_gmail_payment, FAMPAY_GMAIL, FAMPAY_GMAIL_APP_PASSWORD, dep["amount"])
        if result.verified:
            # Atomic claim: only the call that actually flips pending->approved
            # is allowed to credit the wallet. Prevents double-credit if two
            # requests raced past the pre-lock checks.
            claimed = db_execute(
                "UPDATE deposit_requests SET status='approved' WHERE id=? AND status='pending'",
                (dep_id,), fetch="rowcount"
            )
            if not claimed:
                await cq.answer("Already processed.", show_alert=True)
                return
            add_balance(uid, dep["amount"])
            log_action("deposit_auto_verified", f"dep={dep_id} uid={uid} amount={dep['amount']}")
            user_state.pop(uid, None)
            text = (
                f"✅ **ᴅᴇᴘᴏꜱɪᴛ ᴀᴘᴘʀᴏᴠᴇᴅ!**\n\n"
                f"━━━━━━━━━━━━━━━━\n"
                f"💰 ₹{dep['amount']:.2f} added to your wallet\n"
                f"👤 ꜱᴇɴᴅᴇʀ: {result.sender_name or 'UPI User'}\n"
                f"🔖 ᴜᴛʀ: `{result.utr or 'N/A'}`\n"
                f"━━━━━━━━━━━━━━━━"
            )
            try:
                await cq.message.edit_caption(text)
            except Exception:
                await cq.message.reply(text)
            try:
                await client.send_message(
                    ADMIN_ID,
                    f"✅ **Auto Deposit #{dep_id}**\n👤 `{uid}`\n💰 ₹{dep['amount']:.2f}\n🔖 ᴜᴛʀ: `{result.utr or 'N/A'}`"
                )
            except Exception:
                pass
        else:
            retry_markup = InlineKeyboardMarkup([
                [InlineKeyboardButton("🔄 ʀᴇᴛʀʏ ᴠᴇʀɪꜰʏ", callback_data=f"dep_verify_{dep_id}")],
                [InlineKeyboardButton("❌ ᴄᴀɴᴄᴇʟ", callback_data="dep_cancel")],
            ])
            fail_text = (
                f"⚠️ **ᴘᴀʏᴍᴇɴᴛ ɴᴏᴛ ꜰᴏᴜɴᴅ ʏᴇᴛ**\n\n"
                f"💰 ᴀᴍᴏᴜɴᴛ: ₹{dep['amount']:.2f}\n"
                f"{result.message or 'No matching payment found yet.'}\n\n"
                f"Wait a few seconds after paying, then retry.\nOr send a screenshot for manual review."
            )
            try:
                await cq.message.edit_caption(fail_text, reply_markup=retry_markup)
            except Exception:
                try:
                    await cq.message.edit_text(fail_text, reply_markup=retry_markup)
                except Exception:
                    pass


@app.on_callback_query(filters.regex("^dep_cancel$"))
async def cb_dep_cancel(client, cq: CallbackQuery):
    dep_id = user_state.get(cq.from_user.id, {}).get("dep_id")
    if dep_id:
        db_execute("UPDATE deposit_requests SET status='cancelled' WHERE id=?", (dep_id,))
    user_state.pop(cq.from_user.id, None)
    await cq.answer()
    try:
        await cq.message.edit_caption("❌ Deposit cancelled.")
    except Exception:
        try:
            await cq.message.edit_text("❌ Deposit cancelled.")
        except Exception:
            pass


@app.on_callback_query(filters.regex(r"^dep_apr_(\d+)_(\d+)$"))
@admin_only
async def cb_dep_apr(client, cq: CallbackQuery):
    parts = cq.data.split("_")
    dep_id, dep_uid = int(parts[2]), int(parts[3])
    dep = db_execute("SELECT amount FROM deposit_requests WHERE id=?", (dep_id,), fetch="one")
    user_state[ADMIN_ID] = {"step": "dep_apr_amount", "dep_id": dep_id, "dep_uid": dep_uid, "dep_amount": dep["amount"] if dep else 0}
    await cq.message.edit_text(
        f"✅ **Approve Deposit #{dep_id}**\n\nUser: `{dep_uid}`\nScreenshot amount: ₹{dep['amount'] if dep else 0:.2f}\n\nEnter confirmed amount (₹):"
    )


@app.on_callback_query(filters.regex(r"^dep_rej_(\d+)_(\d+)$"))
@admin_only
async def cb_dep_rej(client, cq: CallbackQuery):
    parts = cq.data.split("_")
    dep_id, dep_uid = int(parts[2]), int(parts[3])
    db_execute("UPDATE deposit_requests SET status='rejected' WHERE id=?", (dep_id,))
    await cq.message.edit_text(f"❌ **Deposit #{dep_id} Rejected**")
    try:
        await client.send_message(dep_uid, f"❌ **ᴅᴇᴘᴏꜱɪᴛ ʀᴇᴊᴇᴄᴛᴇᴅ**\nContact {SUPPORT_USER} if wrong.")
    except Exception:
        pass


@app.on_callback_query(filters.regex("^adm_settings$"))
@admin_only
async def cb_adm_settings(client, cq: CallbackQuery):
    ac_stat = "🟢 ON" if get_setting("auto_cleanup") == "on" else "🔴 OFF"
    await cq.message.edit_text(
        "⚙️ **ʙᴏᴛ ꜱᴇᴛᴛɪɴɢꜱ**",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("📝 ᴡᴇʟᴄᴏᴍᴇ ᴍꜱɢ", callback_data="adm_set_welcome"),
             InlineKeyboardButton(f"🧹 ᴀᴜᴛᴏ ᴄʟᴇᴀɴ: {ac_stat}", callback_data="adm_autoclean_tog")],
            [InlineKeyboardButton("💰 ᴍɪɴ ᴅᴇᴘᴏꜱɪᴛ", callback_data="adm_set_mindep"),
             InlineKeyboardButton("💰 ᴍᴀx ᴅᴇᴘᴏꜱɪᴛ", callback_data="adm_set_maxdep")],
            [InlineKeyboardButton("🎁 ʀᴇꜰᴇʀʀᴀʟ ʙᴏɴᴜꜱ", callback_data="adm_set_refbonus"),
             InlineKeyboardButton("🔙 ʙᴀᴄᴋ", callback_data="adm_back")],
        ])
    )

@app.on_callback_query(filters.regex("^adm_autoclean_tog$"))
@admin_only
async def cb_adm_autoclean_tog(client, cq: CallbackQuery):
    current = get_setting("auto_cleanup")
    new_val = "off" if current == "on" else "on"
    set_setting("auto_cleanup", new_val)
    await cq.answer(f"Auto Cleanup is now {new_val.upper()}")
    await cb_adm_settings(client, cq)


@app.on_callback_query(filters.regex("^adm_set_welcome$"))
@admin_only
async def cb_set_welcome(client, cq: CallbackQuery):
    user_state[ADMIN_ID] = {"step": "adm_set_welcome"}
    await cq.message.edit_text("📝 Enter new welcome message:")


@app.on_callback_query(filters.regex("^adm_set_mindep$"))
@admin_only
async def cb_set_mindep(client, cq: CallbackQuery):
    user_state[ADMIN_ID] = {"step": "adm_set_mindep"}
    await cq.message.edit_text(f"💰 **ᴍɪɴ ᴅᴇᴘᴏꜱɪᴛ**\nCurrent: ₹{get_setting('min_deposit')}\nEnter new value:")


@app.on_callback_query(filters.regex("^adm_set_maxdep$"))
@admin_only
async def cb_set_maxdep(client, cq: CallbackQuery):
    user_state[ADMIN_ID] = {"step": "adm_set_maxdep"}
    await cq.message.edit_text(f"💰 **ᴍᴀx ᴅᴇᴘᴏꜱɪᴛ**\nCurrent: ₹{get_setting('max_deposit')}\nEnter new value:")


@app.on_callback_query(filters.regex("^adm_set_refbonus$"))
@admin_only
async def cb_set_refbonus(client, cq: CallbackQuery):
    user_state[ADMIN_ID] = {"step": "adm_set_refbonus"}
    await cq.message.edit_text(f"🎁 **ʀᴇꜰᴇʀʀᴀʟ ʙᴏɴᴜꜱ**\nCurrent: ₹{get_setting('referral_bonus')}\nEnter new value:")


@app.on_callback_query(filters.regex("^adm_logs$"))
@admin_only
async def cb_adm_logs(client, cq: CallbackQuery):
    logs = db_execute("SELECT * FROM admin_logs ORDER BY id DESC LIMIT 20", fetch="all") or []
    lines = ["📝 **ᴀᴅᴍɪɴ ʟᴏɢꜱ** (last 20)\n"]
    for item in logs:
        lines.append(f"• `{item['action']}` — {item['details'][:40]}\n  _{str(item['timestamp'])[:16]}_")
    if not logs:
        lines.append("No logs.")
    await cq.message.edit_text("\n".join(lines), reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 ʙᴀᴄᴋ", callback_data="adm_back")]]))


@app.on_callback_query(filters.regex("^adm_export$"))
@admin_only
async def cb_adm_export(client, cq: CallbackQuery):
    users = get_all_users()
    lines = ["ID | NAME | USERNAME | BALANCE | SPENT | JOINED | STATUS", "=" * 70]
    for user in users:
        status = "BANNED" if user["is_banned"] else "active"
        lines.append(
            f"{user['id']} | {user['name']} | @{user['username']} | ₹{user['balance']:.2f} | ₹{user['total_spent']:.2f} | {str(user['joined_at'])[:10]} | {status}"
        )
    bio = io.BytesIO("\n".join(lines).encode())
    bio.name = "users_export.txt"
    await cq.message.reply_document(bio, caption=f"📤 **ᴜꜱᴇʀꜱ ᴇxᴘᴏʀᴛ**\n{len(users)} users | {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    await cq.answer("✅ Exported!")


@app.on_callback_query(filters.regex("^adm_broadcast$"))
@admin_only
async def cb_adm_broadcast(client, cq: CallbackQuery):
    user_state[ADMIN_ID] = {"step": "adm_broadcast"}
    await cq.message.edit_text("📢 **ʙʀᴏᴀᴅᴄᴀꜱᴛ**\n\nSend message (text/photo/video/document):")


@app.on_callback_query(filters.regex(r"^numpad_(.*)$"))
async def cb_otp_numpad(client, cq: CallbackQuery):
    action = cq.data.split("_")[1]
    uid = cq.from_user.id
    if uid not in user_state or user_state[uid].get("step") not in ["sell_otp", "adm_acc_otp"]:
        await cq.answer("Session expired or invalid state.", show_alert=True)
        return
    
    state = user_state[uid]
    current_otp = state.get("otp_digits", "")

    if action == "ignore":
        await cq.answer()
        return

    if action == "cancel":
        await cq.message.edit_text("❌ Process cancelled.")
        user_state.pop(uid, None)
        return
    
    if action == "clear":
        current_otp = ""
    elif action == "submit":
        if len(current_otp) < 5:
            await cq.answer("OTP must be at least 5 digits!", show_alert=True)
            return
        
        await cq.message.edit_text(f"✅ **ᴏᴛᴘ ꜱᴜʙᴍɪᴛᴛᴇᴅ:** `{current_otp}`\n\n🔄 Verifying with Telegram API...")
        cq.message.text = current_otp
        cq.message.from_user = cq.from_user
        await master_handler(client, cq.message)
        return
    elif action.isdigit():
        if len(current_otp) < 6:
            current_otp += action
    
    state["otp_digits"] = current_otp
    display_otp = current_otp + ("-" * (5 - len(current_otp)) if len(current_otp) < 5 else "")
    display_otp = " ".join(list(display_otp))
    
    try:
        await cq.message.edit_text(
            OTP_SENT_TEXT,
            reply_markup=otp_numpad_kb(display_otp)
        )
    except Exception:
        pass
    await cq.answer()

@app.on_message(filters.private & ~filters.command(["start", "admin", "cancel"]))
async def master_handler(client, message: Message):
    if not message.from_user:
        return
    uid = message.from_user.id
    data = user_state.get(uid)
    if not data:
        return
    step = data.get("step", "")

    if step == "dep_amount":
        if not message.text:
            await message.reply("❌ Enter a number.")
            return
        try:
            amount = float(message.text.strip())
        except ValueError:
            await message.reply("❌ Invalid. Enter a number like `500`:")
            return
        min_dep = float(get_setting("min_deposit", "10"))
        max_dep = float(get_setting("max_deposit", "50000"))
        if amount < min_dep or amount > max_dep:
            await message.reply(f"❌ Amount must be ₹{min_dep:.0f}–₹{max_dep:.0f}.")
            return
        unique_amount = generate_unique_deposit_amount(amount)
        dep_id = db_execute(
            "INSERT INTO deposit_requests(user_id,message_id,status,amount,timestamp) VALUES(?,?,'pending',?,?)",
            (uid, 0, unique_amount, now_iso()),
            fetch="lastid"
        )
        user_state[uid] = {"step": "dep_screenshot", "amount": unique_amount, "dep_id": dep_id}
        caption = (
            f"💳 **ᴘᴀʏᴍᴇɴᴛ ǫʀ**\n\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"💰 ᴘᴀʏ ᴇxᴀᴄᴛʟʏ: ₹**{unique_amount:.2f}**\n"
            f"📱 ᴜᴘɪ: `{UPI_ID}`\n"
            f"━━━━━━━━━━━━━━━━\n\n"
            f"1️⃣ Scan QR or pay to UPI ID\n"
            f"2️⃣ Pay the EXACT amount ₹**{unique_amount:.2f}**\n"
            f"3️⃣ Tap ✅ below after paying, or send a screenshot\n\n"
            f"⚠️ Amount must match exactly for instant auto-verify!"
        )
        buttons = []
        if FAMPAY_AUTO_VERIFY:
            buttons.append([InlineKeyboardButton("✅ ɪ ʜᴀᴠᴇ ᴘᴀɪᴅ — ᴠᴇʀɪꜰʏ ɴᴏᴡ", callback_data=f"dep_verify_{dep_id}")])
        buttons.append([InlineKeyboardButton("❌ ᴄᴀɴᴄᴇʟ", callback_data="dep_cancel")])
        markup = InlineKeyboardMarkup(buttons)
        try:
            await message.reply_photo(make_qr(unique_amount), caption=caption, reply_markup=markup)
        except Exception as e:
            logger.error(f"QR Error: {e}")
            await message.reply_text(caption, reply_markup=markup)
        return

    if step == "dep_screenshot":
        if not (message.photo or message.document):
            await message.reply("⚠️ Please send the **ᴘᴀʏᴍᴇɴᴛ ꜱᴄʀᴇᴇɴꜱʜᴏᴛ** (photo), or tap ✅ I Have Paid above.")
            return
        amount = data.get("amount", 0)
        dep_id = data.get("dep_id")
        if dep_id:
            db_execute("UPDATE deposit_requests SET message_id=? WHERE id=?", (message.id, dep_id))
        else:
            dep_id = db_execute(
                "INSERT INTO deposit_requests(user_id,message_id,status,amount,timestamp) VALUES(?,?,'pending',?,?)",
                (uid, message.id, amount, now_iso()),
                fetch="lastid"
            )
        user = get_user(uid)
        try:
            await message.copy(
                ADMIN_ID,
                caption=f"💳 **Deposit #{dep_id}**\n\n👤 {row_value(user, 'name', 'Unknown')} (@{message.from_user.username or 'none'})\n🆔 `{uid}`\n💰 ₹{amount:.2f}"
            )
            await client.send_message(
                ADMIN_ID,
                f"Action for Deposit **#{dep_id}**:",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("✅ ᴀᴘᴘʀᴏᴠᴇ", callback_data=f"dep_apr_{dep_id}_{uid}")],
                    [InlineKeyboardButton("❌ ʀᴇᴊᴇᴄᴛ", callback_data=f"dep_rej_{dep_id}_{uid}")],
                ])
            )
        except Exception as e:
            logger.error(f"Admin dep notify: {e}")
        user_state.pop(uid, None)
        await message.reply("✅ **ꜱᴄʀᴇᴇɴꜱʜᴏᴛ ʀᴇᴄᴇɪᴠᴇᴅ!**\n\nAdmin will verify and credit your wallet shortly.", reply_markup=main_kb())
        return

    if step == "user_promo":
        if not message.text:
            await message.reply("❌ Enter a promo code.")
            return
        code = message.text.strip().upper()
        promo = get_promo(code)
        if not promo:
            user_state.pop(uid, None)
            await message.reply("❌ **ɪɴᴠᴀʟɪᴅ ᴏʀ ᴇxᴘɪʀᴇᴅ ᴘʀᴏᴍᴏ ᴄᴏᴅᴇ!**", reply_markup=main_kb())
            return
        if promo["used"] >= promo["max_uses"]:
            user_state.pop(uid, None)
            await message.reply("❌ **ᴘʀᴏᴍᴏ ᴄᴏᴅᴇ ɪꜱ ꜰᴜʟʟʏ ᴜꜱᴇᴅ ᴜᴘ!**", reply_markup=main_kb())
            return
        if has_used_promo(code, uid):
            user_state.pop(uid, None)
            await message.reply("❌ **ʏᴏᴜ ᴀʟʀᴇᴀᴅʏ ᴜꜱᴇᴅ ᴛʜɪꜱ ᴘʀᴏᴍᴏ ᴄᴏᴅᴇ!**", reply_markup=main_kb())
            return
        discount = float(promo["discount"])
        credit_wallet(uid, discount)
        use_promo(code, uid)
        user_state.pop(uid, None)
        await message.reply(
            f"🎟 **ᴘʀᴏᴍᴏ ᴀᴘᴘʟɪᴇᴅ!**\n\nCode: `{code}`\n💰 ₹{discount:.0f} added to your wallet!",
            reply_markup=main_kb()
        )
        return

    if step == "sell_phone":
        if not message.text:
            await message.reply("❌ Enter your phone number.")
            return
        phone = normalize_phone(message.text)
        if not is_valid_phone(phone):
            await message.reply("❌ Invalid format. Use `+919876543210`:")
            return
        await message.reply(f"📱 `{phone}`\n\n🔄 Sending OTP…")
        session_client = None
        try:
            session_name = f"sell_{uid}_{phone.lstrip('+')}"
            base_path = os.path.join(PENDING_DIR, session_name)
            if os.path.exists(base_path + ".session"):
                os.remove(base_path + ".session")
            if os.path.exists(base_path + ".session-journal"):
                os.remove(base_path + ".session-journal")
            session_client = Client(session_name, api_id=API_ID, api_hash=API_HASH, workdir=PENDING_DIR)
            async def _send():
                await session_client.connect()
                return await session_client.send_code(phone)

            sent = await asyncio.wait_for(_send(), timeout=25)
            user_state[uid].update({
                "step": "sell_otp",
                "phone": phone,
                "client": session_client,
                "hash": sent.phone_code_hash,
                "path": base_path,
                "country": "Unknown",
                "otp_digits": ""
            })
            await message.reply(OTP_SENT_TEXT, reply_markup=otp_numpad_kb(""))
        except FloodWait as e:
            if session_client:
                await safe_disconnect(session_client)
            await message.reply(f"⏳ **ᴛᴏᴏ ᴍᴀɴʏ ʀᴇǫᴜᴇꜱᴛꜱ!**\n\nPlease wait **{e.value} seconds** then try again.")
            user_state.pop(uid, None)
        except PhoneNumberInvalid:
            if session_client:
                await safe_disconnect(session_client)
            await message.reply("❌ Invalid phone number. Use format `+919876543210`:")
            user_state[uid]["step"] = "sell_phone"
        except Exception as e:
            logger.error(f"Sell phone: {type(e).__name__}: {e}")
            if session_client:
                await safe_disconnect(session_client)
            await message.reply(f"❌ ꜰᴀɪʟᴇᴅ ᴛᴏ ꜱᴇɴᴅ ᴏᴛᴘ: `{type(e).__name__}`\n\nTry again or /cancel.")
            user_state.pop(uid, None)
        return

    if step == "sell_otp":
        if not message.text:
            await message.reply("❌ Enter the OTP.")
            return
        otp = message.text.strip().replace(" ", "").replace("-", "")
        if not re.match(r"^\d{5,6}$", otp):
            await message.reply("❌ OTP must be 5-6 digits. Try again:")
            return
        session_client = data.get("client")
        if not session_client:
            await message.reply("❌ Session expired. Please start over with 💸 Sell Account.")
            user_state.pop(uid, None)
            return
        user_state[uid]["step"] = "sell_processing"
        try:
            await session_client.sign_in(data["phone"], data["hash"], otp)
            status_message = await message.reply("✅ **ᴏᴛᴘ ᴠᴇʀɪꜰɪᴇᴅ!** Processing your account…")
            await run_user_sell_pipeline(client, status_message, session_client, uid)
        except SessionPasswordNeeded:
            user_state[uid]["step"] = "sell_2fa"
            await message.reply("🔐 **2ꜰᴀ ʀᴇǫᴜɪʀᴇᴅ!** Enter your 2FA password:")
        except PhoneCodeInvalid:
            user_state[uid]["step"] = "sell_otp"
            await message.reply("❌ Wrong OTP. Enter the correct code:")
        except PhoneCodeExpired:
            await message.reply("❌ OTP expired. Please start over with 💸 Sell Account.")
            await safe_disconnect(session_client)
            user_state.pop(uid, None)
        except Exception as e:
            logger.error(f"Sell OTP: {type(e).__name__}: {e}")
            await message.reply(f"❌ ᴇʀʀᴏʀ: `{type(e).__name__}: {str(e)[:200]}`")
            await safe_disconnect(session_client)
            user_state.pop(uid, None)
        return

    if step == "sell_2fa":
        if not message.text:
            await message.reply("❌ Enter 2FA password.")
            return
        session_client = data.get("client")
        if not session_client:
            await message.reply("❌ Session expired. Start over with 💸 Sell Account.")
            user_state.pop(uid, None)
            return
        user_state[uid]["step"] = "sell_processing"
        try:
            await session_client.check_password(message.text.strip())
            user_state[uid]["old_2fa"] = message.text.strip()
            status_message = await message.reply("✅ **2ꜰᴀ ᴀᴄᴄᴇᴘᴛᴇᴅ!** Processing your account…")
            await run_user_sell_pipeline(client, status_message, session_client, uid)
        except PasswordHashInvalid:
            user_state[uid]["step"] = "sell_2fa"
            await message.reply("❌ Wrong 2FA password. Try again:")
        except Exception as e:
            logger.error(f"Sell 2FA: {type(e).__name__}: {e}")
            await message.reply(f"❌ ᴇʀʀᴏʀ: `{type(e).__name__}: {str(e)[:200]}`")
            await safe_disconnect(session_client)
            user_state.pop(uid, None)
        return

    if step == "sell_upi_id":
        if not message.text:
            await message.reply("❌ Enter your UPI ID or type `wallet`.")
            return
        upi_input = message.text.strip()
        order_id = data.get("sell_oid")
        phone = data.get("phone", "")
        country = data.get("country", "Unknown")
        tfa_status = data.get("tfa_stat", "")
        if upi_input.lower() == "wallet":
            upi_stored = ""
            pay_method = "💰 Bot Wallet"
        else:
            if not is_valid_upi(upi_input):
                await message.reply(
                    "❌ **ɪɴᴠᴀʟɪᴅ ᴜᴘɪ ɪᴅ ꜰᴏʀᴍᴀᴛ.**\n\n"
                    "✅ Valid examples:\n"
                    "• `name@upi`\n"
                    "• `9876543210@paytm`\n"
                    "• `firstname.lastname@okaxis`\n\n"
                    "Or type `wallet` to receive in bot wallet:"
                )
                return
            upi_stored = upi_input
            pay_method = f"💳 ᴜᴘɪ: `{upi_input}`"
        db_execute("UPDATE sell_orders SET upi_id=? WHERE id=?", (upi_stored, order_id))
        user_state.pop(uid, None)
        sanitized_tfa = sanitize_tfa_status_for_user(tfa_status)
        await message.reply(
            f"✅ **ᴀᴄᴄᴏᴜɴᴛ ꜱᴜʙᴍɪᴛᴛᴇᴅ!**\n\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"📱 ᴘʜᴏɴᴇ: `{phone}`\n"
            f"📋 ᴏʀᴅᴇʀ: **#{order_id}**\n"
            f"🔑 2FA Status: {sanitized_tfa}\n"
            f"💳 ᴘᴀʏᴍᴇɴᴛ: {pay_method}\n"
            f"━━━━━━━━━━━━━━━━\n\n"
            f"Admin will review within 24 hours.\n"
            f"You'll be notified once approved! 💰",
            reply_markup=main_kb()
        )
        order = db_execute("SELECT * FROM sell_orders WHERE id=?", (order_id,), fetch="one")
        spam_result = row_value(order, "spam_result", "")[:300] or "N/A"
        old_pwd_val = row_value(order, "old_2fa", "") or "None"
        new_pwd_val = row_value(order, "new_2fa", "") or "None"
        try:
            user = get_user(uid)
            await client.send_message(
                ADMIN_ID,
                f"📦 **New Sell Order #{order_id}**\n\n"
                f"━━━━━━━━━━━━━━━━\n"
                f"👤 {row_value(user, 'name', 'Unknown')} (@{row_value(user, 'username', '')})\n"
                f"🆔 `{uid}`\n"
                f"📱 `{phone}`\n"
                f"🌍 ᴄᴏᴜɴᴛʀʏ: {country}\n"
                f"🔑 Old 2FA: `{old_pwd_val}`\n"
                f"🔑 New 2FA: `{new_pwd_val}`\n"
                f"🛡 ꜱᴘᴀᴍ: {'✅ Clean' if is_spam_free(spam_result) else '⚠️ Flagged'}\n"
                f"💳 ᴘᴀʏ ᴛᴏ: {pay_method}\n"
                f"━━━━━━━━━━━━━━━━\n"
                f"`{spam_result}`",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("✅ ᴀᴘᴘʀᴏᴠᴇ & ᴘᴀʏ", callback_data=f"sell_apr_{order_id}")],
                    [InlineKeyboardButton("❌ ʀᴇᴊᴇᴄᴛ", callback_data=f"sell_rej_{order_id}")],
                    [InlineKeyboardButton("🔍 ʀᴇᴄʜᴇᴄᴋ ꜱᴘᴀᴍ", callback_data=f"sell_rchk_{order_id}")],
                ])
            )
        except Exception as e:
            logger.error(f"Admin notify sell: {e}")
        return

    if step == "adm_acc_country" and uid == ADMIN_ID:
        if not message.text:
            await message.reply("❌ Enter country name.")
            return
        user_state[uid]["country"] = message.text.strip()
        user_state[uid]["step"] = "adm_acc_phone"
        await message.reply(f"🌍 ᴄᴏᴜɴᴛʀʏ: **{message.text.strip()}**\n\nEnter phone number (`+919876543210`):")
        return

    if step == "adm_acc_phone" and uid == ADMIN_ID:
        if not message.text:
            return
        phone = normalize_phone(message.text)
        if not is_valid_phone(phone):
            await message.reply("❌ Invalid phone format. Try again:")
            return
        status_message = await message.reply(f"📱 `{phone}`\n\n🔄 Sending OTP…")
        session_client = None
        try:
            session_name = f"admadd_{phone.lstrip('+')}"
            base_path = os.path.join(PENDING_DIR, session_name)
            if os.path.exists(base_path + ".session"):
                os.remove(base_path + ".session")
            if os.path.exists(base_path + ".session-journal"):
                os.remove(base_path + ".session-journal")
            session_client = Client(session_name, api_id=API_ID, api_hash=API_HASH, workdir=PENDING_DIR)
            await session_client.connect()
            sent = await session_client.send_code(phone)
            user_state[uid].update({
                "step": "adm_acc_otp",
                "phone": phone,
                "client": session_client,
                "hash": sent.phone_code_hash,
                "path": base_path,
                "otp_digits": ""
            })
            await status_message.edit_text(OTP_SENT_TEXT, reply_markup=otp_numpad_kb(""))
        except FloodWait as e:
            if session_client:
                await safe_disconnect(session_client)
            await status_message.edit_text(f"⏳ **ꜰʟᴏᴏᴅᴡᴀɪᴛ:** Telegram says wait {e.value}s before retrying.")
            user_state.pop(uid, None)
        except PhoneNumberInvalid:
            if session_client:
                await safe_disconnect(session_client)
            user_state[uid]["step"] = "adm_acc_phone"
            await status_message.edit_text("❌ Invalid phone number. Enter again:")
        except Exception as e:
            logger.error(f"Admin add phone: {type(e).__name__}: {e}")
            if session_client:
                await safe_disconnect(session_client)
            await status_message.edit_text(f"❌ ᴇʀʀᴏʀ: `{type(e).__name__}: {str(e)[:200]}`")
            user_state.pop(uid, None)
        return

    if step == "adm_acc_otp" and uid == ADMIN_ID:
        if not message.text:
            return
        session_client = data.get("client")
        if not session_client:
            await message.reply("❌ Session expired. Start over.")
            user_state.pop(uid, None)
            return
        otp = message.text.strip().replace(" ", "").replace("-", "")
        status_message = await message.reply("🔄 Verifying OTP…")
        try:
            await session_client.sign_in(data["phone"], data["hash"], otp)
            user_state[uid]["step"] = "adm_cleanup_choice"
            await status_message.edit_text("✅ **ʟᴏɢɢᴇᴅ ɪɴ!** Analyzing account…")
            await run_admin_add_pipeline(client, uid, status_message)
        except SessionPasswordNeeded:
            user_state[uid]["step"] = "adm_acc_2fa"
            await status_message.edit_text("🔐 **2ꜰᴀ ʀᴇǫᴜɪʀᴇᴅ!** Enter 2FA password:")
        except PhoneCodeInvalid:
            await status_message.edit_text("❌ Wrong OTP. Try again:")
        except PhoneCodeExpired:
            await status_message.edit_text("❌ OTP expired. Use /admin → Add Account to start over.")
            await safe_disconnect(session_client)
            user_state.pop(uid, None)
        except FloodWait as e:
            await status_message.edit_text(f"⏳ FloodWait: wait {e.value}s, then try again.")
            await safe_disconnect(session_client)
            user_state.pop(uid, None)
        except Exception as e:
            logger.error(f"Admin OTP: {type(e).__name__}: {e}")
            await status_message.edit_text(f"❌ ᴇʀʀᴏʀ: `{type(e).__name__}: {str(e)[:200]}`")
            await safe_disconnect(session_client)
            user_state.pop(uid, None)
        return

    if step == "adm_acc_2fa" and uid == ADMIN_ID:
        if not message.text:
            return
        session_client = data.get("client")
        if not session_client:
            await message.reply("❌ Session expired.")
            user_state.pop(uid, None)
            return
        status_message = await message.reply("🔄 Checking 2FA…")
        try:
            await session_client.check_password(message.text.strip())
            user_state[uid]["old_2fa"] = message.text.strip()
            user_state[uid]["step"] = "adm_cleanup_choice"
            await status_message.edit_text("✅ **2ꜰᴀ ᴀᴄᴄᴇᴘᴛᴇᴅ!** Analyzing account…")
            await run_admin_add_pipeline(client, uid, status_message)
        except PasswordHashInvalid:
            await status_message.edit_text("❌ Wrong 2FA password. Try again:")
        except FloodWait as e:
            await status_message.edit_text(f"⏳ FloodWait: wait {e.value}s then try again.")
            await safe_disconnect(session_client)
            user_state.pop(uid, None)
        except Exception as e:
            logger.error(f"Admin 2FA: {type(e).__name__}: {e}")
            await status_message.edit_text(f"❌ ᴇʀʀᴏʀ: `{type(e).__name__}: {str(e)[:200]}`")
            await safe_disconnect(session_client)
            user_state.pop(uid, None)
        return

    if step == "sell_apr_amount" and uid == ADMIN_ID:
        if not message.text:
            return
        try:
            amount = float(message.text.strip())
        except ValueError:
            await message.reply("❌ Invalid amount:")
            return
        if amount <= 0:
            await message.reply("❌ Amount must be positive:")
            return
        order_id = data.get("sell_oid")
        order = db_execute("SELECT * FROM sell_orders WHERE id=?", (order_id,), fetch="one")
        if not order:
            user_state.pop(uid, None)
            await message.reply("❌ Order not found.")
            return
        if order["status"] != "pending":
            user_state.pop(uid, None)
            await message.reply(f"❌ Order #{order_id} already processed ({order['status']}).")
            return
        user_id = order["user_id"]
        upi_id = row_value(order, "upi_id", "") or ""
        country = row_value(order, "country", "") or "Unknown"
        session_path = row_value(order, "session_path", "")
        try:
            claimed = db_execute(
                "UPDATE sell_orders SET status='approved',sell_price=? WHERE id=? AND status='pending'",
                (amount, order_id), fetch="rowcount"
            )
            if not claimed:
                user_state.pop(uid, None)
                await message.reply(f"❌ Order #{order_id} already processed, nothing paid out.")
                return
            moved_to = None
            if session_path and os.path.exists(session_path):
                moved_to = move_existing_session_file(session_path, country, order["phone_number"])
            if moved_to:
                db_execute("UPDATE sell_orders SET session_path=? WHERE id=?", (moved_to, order_id))
            db_execute_many([
                ("UPDATE users SET total_sold=total_sold+1 WHERE id=?", (user_id,)),
                ("UPDATE business_stats SET value=value+1 WHERE key='total_bought_from_users'", ()),
            ])
            if not upi_id:
                credit_wallet(user_id, amount)
                payout_note = f"₹{amount:.2f} added to your bot wallet! 🎉"
                admin_note = f"✅ **Order #{order_id} Approved!** ₹{amount:.2f} added to wallet."
                await message.reply(admin_note)
            else:
                payout_note = f"₹{amount:.2f} is being sent to your UPI: `{upi_id}`\n\nPayment will arrive within a few minutes!"
                try:
                    await message.reply_photo(
                        make_payout_qr(upi_id, amount),
                        caption=(
                            f"✅ **Order #{order_id} Approved!**\n\n"
                            f"━━━━━━━━━━━━━━━━\n"
                            f"📱 ᴘʜᴏɴᴇ: `{order['phone_number']}`\n"
                            f"💰 ᴀᴍᴏᴜɴᴛ: ₹{amount:.2f}\n"
                            f"💳 ᴜᴘɪ: `{upi_id}`\n"
                            f"━━━━━━━━━━━━━━━━\n\n"
                            f"⬆️ Scan this QR or pay manually to UPI above."
                        )
                    )
                except Exception:
                    await message.reply(f"✅ **Order #{order_id} Approved!**\n\n📱 `{order['phone_number']}`\n💰 Pay ₹{amount:.2f} to UPI: `{upi_id}`")
            log_action("sell_approved", f"order={order_id} amount={amount} payout={'upi' if upi_id else 'wallet'}")
            user_state.pop(uid, None)
            try:
                await client.send_message(
                    user_id,
                    f"✅ **ꜱᴇʟʟ ᴀᴘᴘʀᴏᴠᴇᴅ!**\n\n📱 `{order['phone_number']}`\n💰 {payout_note}"
                )
            except Exception:
                pass
        except Exception as e:
            logger.error(f"sell_apr_amount failed for order={order_id}: {type(e).__name__}: {e}")
            await message.reply(f"❌ ᴀᴘᴘʀᴏᴠᴀʟ ꜰᴀɪʟᴇᴅ: `{type(e).__name__}: {str(e)[:200]}`")
        return

    if step == "sell_rej_reason" and uid == ADMIN_ID:
        reason = message.text.strip() if message.text else ""
        if reason.lower() == "skip":
            reason = "No reason provided."
        order_id = data.get("sell_oid")
        order = db_execute("SELECT * FROM sell_orders WHERE id=?", (order_id,), fetch="one")
        if not order:
            user_state.pop(uid, None)
            await message.reply("❌ Order not found.")
            return
        db_execute_many([
            ("UPDATE sell_orders SET status='rejected',rejection_reason=? WHERE id=?", (reason, order_id)),
            ("UPDATE business_stats SET value=value+1 WHERE key='total_rejected'", ()),
        ])
        session_path = row_value(order, "session_path", "")
        if session_path:
            clean_session_files(session_path.replace(".session", ""))
        log_action("sell_rejected", f"order={order_id} reason={reason}")
        user_state.pop(uid, None)
        await message.reply(f"❌ **Order #{order_id} Rejected.**")
        try:
            await client.send_message(
                order["user_id"],
                f"❌ **ꜱᴇʟʟ ʀᴇᴊᴇᴄᴛᴇᴅ**\n\n📱 `{order['phone_number']}`\nReason: {reason}\n\nContact {SUPPORT_USER} for more info."
            )
        except Exception:
            pass
        return

    if step == "dep_apr_amount" and uid == ADMIN_ID:
        if not message.text:
            return
        try:
            amount = float(message.text.strip())
        except ValueError:
            await message.reply("❌ Invalid number:")
            return
        if amount <= 0:
            await message.reply("❌ Amount must be positive:")
            return
        dep_id = data.get("dep_id")
        dep_uid = data.get("dep_uid")
        claimed = db_execute(
            "UPDATE deposit_requests SET status='approved',amount=? WHERE id=? AND status='pending'",
            (amount, dep_id), fetch="rowcount"
        )
        if not claimed:
            user_state.pop(uid, None)
            await message.reply(f"❌ Deposit #{dep_id} already processed, nothing credited.")
            return
        add_balance(dep_uid, amount)
        db_execute("UPDATE business_stats SET value=value+1 WHERE key='total_deposits_approved'", ())
        log_action("deposit_approved", f"dep={dep_id} uid={dep_uid} amount={amount}")
        user_state.pop(uid, None)
        await message.reply(f"✅ **Deposit #{dep_id} Approved!** ₹{amount:.2f} added.")
        try:
            await client.send_message(dep_uid, f"✅ **ᴅᴇᴘᴏꜱɪᴛ ᴀᴘᴘʀᴏᴠᴇᴅ!**\n\n₹{amount:.2f} added to your wallet!\nCheck with 👤 My Profile.")
        except Exception:
            pass
        return

    if step == "adm_add_bal_uid" and uid == ADMIN_ID:
        if not message.text or not message.text.strip().isdigit():
            await message.reply("❌ Enter valid User ID:")
            return
        target_id = int(message.text.strip())
        target = get_user(target_id)
        if not target:
            await message.reply("❌ User not found:")
            return
        user_state[uid].update({"step": "adm_add_bal_amount", "target_id": target_id})
        await message.reply(f"👤 **{target['name']}** (`{target_id}`)\n💰 ʙᴀʟᴀɴᴄᴇ: ₹{target['balance']:.2f}\n\nEnter amount to ADD (₹):")
        return

    if step == "adm_add_bal_amount" and uid == ADMIN_ID:
        if not message.text:
            return
        try:
            amount = float(message.text.strip())
        except ValueError:
            await message.reply("❌ Invalid number:")
            return
        target_id = data.get("target_id")
        credit_wallet(target_id, amount)
        log_action("add_balance", f"uid={target_id} amount={amount}")
        user_state.pop(uid, None)
        await message.reply(f"✅ ₹{amount:.2f} added to `{target_id}`.")
        try:
            await client.send_message(target_id, f"💰 **ʙᴀʟᴀɴᴄᴇ ᴀᴅᴅᴇᴅ!**\n\n₹{amount:.2f} credited by admin.")
        except Exception:
            pass
        return

    if step == "adm_deduct_bal_uid" and uid == ADMIN_ID:
        if not message.text or not message.text.strip().isdigit():
            await message.reply("❌ Enter valid User ID:")
            return
        target_id = int(message.text.strip())
        target = get_user(target_id)
        if not target:
            await message.reply("❌ User not found:")
            return
        user_state[uid].update({"step": "adm_deduct_bal_amount", "target_id": target_id})
        await message.reply(f"👤 **{target['name']}** (`{target_id}`)\n💰 ʙᴀʟᴀɴᴄᴇ: ₹{target['balance']:.2f}\n\nEnter amount to DEDUCT (₹):")
        return

    if step == "adm_deduct_bal_amount" and uid == ADMIN_ID:
        if not message.text:
            return
        try:
            amount = float(message.text.strip())
        except ValueError:
            await message.reply("❌ Invalid number:")
            return
        target_id = data.get("target_id")
        db_execute("UPDATE users SET balance=CASE WHEN balance>=? THEN balance-? ELSE 0 END WHERE id=?", (amount, amount, target_id))
        log_action("deduct_balance", f"uid={target_id} amount={amount}")
        user_state.pop(uid, None)
        await message.reply(f"✅ ₹{amount:.2f} deducted from `{target_id}`.")
        return

    if step == "adm_set_buy_price" and uid == ADMIN_ID:
        try:
            price = float(message.text.strip())
            set_setting("default_buy_price", price)
            log_action("set_buy_price", str(price))
            user_state.pop(uid, None)
            await message.reply(f"✅ Buy price → ₹{price:.0f}")
        except Exception:
            await message.reply("❌ Enter a number:")
        return

    if step == "adm_set_sell_price" and uid == ADMIN_ID:
        try:
            price = float(message.text.strip())
            set_setting("default_sell_price", price)
            log_action("set_sell_price", str(price))
            user_state.pop(uid, None)
            await message.reply(f"✅ Sell price → ₹{price:.0f}")
        except Exception:
            await message.reply("❌ Enter a number:")
        return

    if step == "adm_c_buy_name" and uid == ADMIN_ID:
        country = message.text.strip()
        user_state[uid].update({"step": "adm_c_buy_price", "country": country})
        await message.reply(f"🌍 {country} | Current: ₹{get_buy_price(country):.0f}\n\nEnter new buy price:")
        return

    if step == "adm_c_buy_price" and uid == ADMIN_ID:
        try:
            price = float(message.text.strip())
            country = data.get("country", "")
            db_execute("INSERT OR REPLACE INTO country_buy_prices(country,price) VALUES(?,?)", (country, price))
            log_action("set_country_buy", f"{country}={price}")
            user_state.pop(uid, None)
            await message.reply(f"✅ {country} buy price → ₹{price:.0f}")
        except Exception:
            await message.reply("❌ Enter a number:")
        return

    if step == "adm_c_sell_name" and uid == ADMIN_ID:
        country = message.text.strip()
        user_state[uid].update({"step": "adm_c_sell_price", "country": country})
        await message.reply(f"🌍 {country} | Current: ₹{get_sell_price(country):.0f}\n\nEnter new sell price:")
        return

    if step == "adm_c_sell_price" and uid == ADMIN_ID:
        try:
            price = float(message.text.strip())
            country = data.get("country", "")
            db_execute("INSERT OR REPLACE INTO country_sell_prices(country,sell_price) VALUES(?,?)", (country, price))
            log_action("set_country_sell", f"{country}={price}")
            user_state.pop(uid, None)
            await message.reply(f"✅ {country} sell price → ₹{price:.0f}")
        except Exception:
            await message.reply("❌ Enter a number:")
        return

    if step == "adm_cp_price" and uid == ADMIN_ID:
        if not message.text:
            return
        text = message.text.strip()
        if text.lower() == "remove":
            db_execute("DELETE FROM custom_prices WHERE session_file=?", (data.get("sf", ""),))
            user_state.pop(uid, None)
            await message.reply(f"✅ Custom price removed for `{data.get('phone', '')}`.")
            return
        try:
            price = float(text)
            set_custom_price(data.get("sf", ""), data.get("country", ""), price, uid)
            log_action("custom_price", f"{data.get('phone', '')}={price}")
            user_state.pop(uid, None)
            await message.reply(f"✅ Custom price for `{data.get('phone', '')}` → ₹{price:.0f}")
        except Exception:
            await message.reply("❌ Enter a number or `remove`:")
        return

    if step == "adm_note_text" and uid == ADMIN_ID:
        if not message.text:
            return
        text = message.text.strip()
        if text.lower() == "remove":
            db_execute("DELETE FROM account_notes WHERE session_file=?", (data.get("sf", ""),))
            user_state.pop(uid, None)
            await message.reply(f"✅ Note removed for `{data.get('phone', '')}`.")
            return
        set_account_note(data.get("sf", ""), text, uid)
        user_state.pop(uid, None)
        await message.reply(f"✅ Note set for `{data.get('phone', '')}`:\n_{text}_")
        return

    if step == "adm_ban_uid" and uid == ADMIN_ID:
        if not message.text or not message.text.strip().isdigit():
            await message.reply("❌ Enter valid User ID:")
            return
        user_state[uid].update({"step": "adm_ban_reason", "target_id": int(message.text.strip())})
        await message.reply(f"🚫 Banning `{int(message.text.strip())}`\nEnter reason (or `skip`):")
        return

    if step == "adm_ban_reason" and uid == ADMIN_ID:
        reason = message.text.strip() if message.text else ""
        if reason.lower() == "skip":
            reason = ""
        target_id = data.get("target_id")
        ban_user(target_id, reason)
        log_action("ban_user", f"uid={target_id}")
        user_state.pop(uid, None)
        await message.reply(f"🚫 User `{target_id}` banned.")
        try:
            await client.send_message(target_id, f"🚫 **ʏᴏᴜ ʜᴀᴠᴇ ʙᴇᴇɴ ʙᴀɴɴᴇᴅ.**\nContact {SUPPORT_USER} to appeal.")
        except Exception:
            pass
        return

    if step == "adm_unban_uid" and uid == ADMIN_ID:
        if not message.text or not message.text.strip().isdigit():
            await message.reply("❌ Enter valid User ID:")
            return
        target_id = int(message.text.strip())
        unban_user(target_id)
        log_action("unban_user", f"uid={target_id}")
        user_state.pop(uid, None)
        await message.reply(f"✅ User `{target_id}` unbanned.")
        try:
            await client.send_message(target_id, f"✅ **ʏᴏᴜ ʜᴀᴠᴇ ʙᴇᴇɴ ᴜɴʙᴀɴɴᴇᴅ!**\nUse /start to continue.")
        except Exception:
            pass
        return

    if step == "adm_find_user" and uid == ADMIN_ID:
        if not message.text:
            return
        query = message.text.strip()
        row = db_execute("SELECT * FROM users WHERE id=?", (int(query),), fetch="one") if query.isdigit() else db_execute(
            "SELECT * FROM users WHERE username LIKE ? OR name LIKE ?",
            (f"%{query}%", f"%{query}%"), fetch="one"
        )
        user_state.pop(uid, None)
        if not row:
            await message.reply("❌ User not found.")
            return
        await message.reply(
            f"🔍 **ᴜꜱᴇʀ ꜰᴏᴜɴᴅ**\n\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"🆔 `{row['id']}`\n"
            f"📛 {row['name']}\n"
            f"👤 @{row['username']}\n"
            f"💰 ₹{row['balance']:.2f}\n"
            f"💸 ꜱᴘᴇɴᴛ: ₹{row['total_spent']:.2f}\n"
            f"📅 {str(row['joined_at'])[:10]}\n"
            f"📊 {'🚫 Banned' if row['is_banned'] else '✅ Active'}\n"
            f"━━━━━━━━━━━━━━━━"
        )
        return

    if step == "adm_msg_user_uid" and uid == ADMIN_ID:
        if not message.text or not message.text.strip().isdigit():
            await message.reply("❌ Enter valid User ID:")
            return
        user_state[uid].update({"step": "adm_msg_user_text", "target_id": int(message.text.strip())})
        await message.reply(f"📤 Enter message to send to `{int(message.text.strip())}`:")
        return

    if step == "adm_msg_user_text" and uid == ADMIN_ID:
        target_id = data.get("target_id")
        user_state.pop(uid, None)
        try:
            if message.text:
                await client.send_message(target_id, f"📢 **ᴍᴇꜱꜱᴀɢᴇ ꜰʀᴏᴍ ᴀᴅᴍɪɴ:**\n\n{message.text}")
            else:
                await message.copy(target_id)
            await message.reply(f"✅ Message sent to `{target_id}`.")
        except Exception as e:
            await message.reply(f"❌ ꜰᴀɪʟᴇᴅ: {e}")
        return

    if step == "adm_fj_add" and uid == ADMIN_ID:
        if not message.text:
            return
        channel = message.text.strip().lstrip("@")
        add_force_join(channel)
        log_action("fj_add", channel)
        user_state.pop(uid, None)
        await message.reply(f"📢 Added **@{channel}** to Force Join list.")
        return

    if step == "adm_broadcast" and uid == ADMIN_ID:
        user_state.pop(uid, None)
        users = get_all_users()
        sent = 0
        failed = 0
        status = await message.reply(f"📢 Broadcasting to {len(users)} users…")
        for user in users:
            if user["id"] == ADMIN_ID:
                continue
            try:
                await message.copy(user["id"])
                sent += 1
            except FloodWait as e:
                await asyncio.sleep(e.value + 1)
                try:
                    await message.copy(user["id"])
                    sent += 1
                except Exception:
                    failed += 1
            except Exception:
                failed += 1
            if (sent + failed) % 30 == 0:
                try:
                    await status.edit_text(f"📢 Broadcasting…\n✅ {sent}\n❌ {failed}")
                except Exception:
                    pass
        log_action("broadcast", f"sent={sent} failed={failed}")
        await status.edit_text(f"📢 **ʙʀᴏᴀᴅᴄᴀꜱᴛ ᴄᴏᴍᴘʟᴇᴛᴇ!**\n\n✅ ꜱᴇɴᴛ: {sent}\n❌ ꜰᴀɪʟᴇᴅ: {failed}\n👥 ᴛᴏᴛᴀʟ: {len(users)}")
        return

    if step == "adm_set_welcome" and uid == ADMIN_ID:
        if not message.text:
            return
        set_setting("welcome_message", message.text.strip())
        user_state.pop(uid, None)
        await message.reply("✅ Welcome message updated.")
        return

    if step == "adm_set_mindep" and uid == ADMIN_ID:
        try:
            value = float(message.text.strip())
            set_setting("min_deposit", value)
            user_state.pop(uid, None)
            await message.reply(f"✅ Min deposit → ₹{value:.0f}")
        except Exception:
            await message.reply("❌ Enter a number:")
        return

    if step == "adm_set_maxdep" and uid == ADMIN_ID:
        try:
            value = float(message.text.strip())
            set_setting("max_deposit", value)
            user_state.pop(uid, None)
            await message.reply(f"✅ Max deposit → ₹{value:.0f}")
        except Exception:
            await message.reply("❌ Enter a number:")
        return

    if step == "adm_set_refbonus" and uid == ADMIN_ID:
        try:
            value = float(message.text.strip())
            set_setting("referral_bonus", value)
            user_state.pop(uid, None)
            await message.reply(f"✅ Referral bonus → ₹{value:.0f}")
        except Exception:
            await message.reply("❌ Enter a number:")
        return

    if step == "adm_promo_code" and uid == ADMIN_ID:
        if not message.text:
            return
        user_state[uid].update({"step": "adm_promo_discount", "promo_code": message.text.strip().upper()})
        await message.reply(f"🎟 ᴄᴏᴅᴇ: `{message.text.strip().upper()}`\n\nEnter discount amount (₹):")
        return

    if step == "adm_promo_discount" and uid == ADMIN_ID:
        try:
            discount = float(message.text.strip())
            user_state[uid].update({"step": "adm_promo_maxuses", "promo_disc": discount})
            await message.reply(f"💰 ᴅɪꜱᴄᴏᴜɴᴛ: ₹{discount:.0f}\n\nEnter max uses (number):")
        except Exception:
            await message.reply("❌ Enter a number:")
        return

    if step == "adm_promo_maxuses" and uid == ADMIN_ID:
        try:
            max_uses = int(message.text.strip())
            create_promo(data.get("promo_code", ""), data.get("promo_disc", 0), max_uses)
            log_action("create_promo", f"code={data.get('promo_code','')} disc={data.get('promo_disc',0)} max={max_uses}")
            user_state.pop(uid, None)
            await message.reply(
                f"✅ **ᴘʀᴏᴍᴏ ᴄʀᴇᴀᴛᴇᴅ!**\n\n"
                f"🎟 ᴄᴏᴅᴇ: `{data.get('promo_code', '')}`\n"
                f"💰 ᴅɪꜱᴄᴏᴜɴᴛ: ₹{data.get('promo_disc', 0):.0f}\n"
                f"👥 ᴍᴀx ᴜꜱᴇꜱ: {max_uses}"
            )
        except Exception:
            await message.reply("❌ Enter a number:")
        return

    if step == "adm_promo_dis_code" and uid == ADMIN_ID:
        if not message.text:
            return
        code = message.text.strip().upper()
        db_execute("UPDATE promo_codes SET active=0 WHERE code=?", (code,))
        user_state.pop(uid, None)
        await message.reply(f"❌ Promo `{code}` disabled.")
        return


async def cleanup_stale_states():
    while True:
        try:
            await asyncio.sleep(600)
            now = time.time()
            to_remove = []
            for uid, state in list(user_state.items()):
                if "ts" not in state:
                    state["ts"] = now
                elif now - state["ts"] > 3600:
                    to_remove.append(uid)
            for uid in to_remove:
                state = user_state.pop(uid, None)
                if state and state.get("client"):
                    try:
                        await safe_stop(state["client"])
                    except Exception:
                        pass
        except Exception as e:
            logger.error(f"Cleanup error: {e}")

async def main():
    asyncio.create_task(cleanup_stale_states())
    logger.info("🐱 Starting Felix Store…")
    await app.start()
    bot_me = await app.get_me()
    logger.info(f"🐱 ᴏɴʟɪɴᴇ: @{bot_me.username}")
    try:
        await app.send_message(
            ADMIN_ID,
            f"🐱 **{BOT_NAME} Online!**\n\n"
            f"🤖 @{bot_me.username}\n"
            f"🕐 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"✅ All systems go."
        )
    except Exception:
        pass
    await asyncio.Event().wait()


def _quiet_exception_handler(loop, context):
    exc = context.get("exception")
    if isinstance(exc, (ValueError, KeyError)):
        msg = str(exc)
        if "Peer id invalid" in msg or "ID not found" in msg:
            return
    loop.default_exception_handler(context)


if __name__ == "__main__":
    # IMPORTANT: do NOT create a new event loop here. Client() and its
    # Dispatcher already captured asyncio.get_event_loop() at import time
    # (pyrogram/dispatcher.py: self.loop = asyncio.get_event_loop()), and
    # Dispatcher.start() schedules all 32 handler_worker tasks on THAT loop
    # via self.loop.create_task(...). Swapping in a fresh loop here before
    # app.run() made app.run() drive a *different* loop than the one those
    # worker tasks were scheduled on - the workers were created (hence the
    # "Started 32 HandlerTasks" log) but never actually ran, so no incoming
    # update (any message/command, not just /start) was ever dispatched.
    asyncio.get_event_loop().set_exception_handler(_quiet_exception_handler)
    app.run(main())

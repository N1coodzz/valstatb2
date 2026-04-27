import asyncio
import csv
import hashlib
import logging
import os
import re
import sqlite3
import time
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import FSInputFile, KeyboardButton, Message, ReplyKeyboardMarkup
from openpyxl import Workbook

# =========================
# CONFIG
# =========================
BOT_TOKEN = os.getenv("BOT_TOKEN")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
TIMEZONE = ZoneInfo("Europe/Moscow")
DB_PATH = "bot.db"
LOG_PATH = "bot.log"
REMINDER_MINUTES = 10
DOUBLE_TAP_SECONDS = 1.2
SHIFT_PAGE_SIZE = 5
BET_PAGE_SIZE = 15

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN not found in environment")
if not OWNER_ID:
    raise RuntimeError("OWNER_ID not found in environment")

# =========================
# LOGGING
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.FileHandler(LOG_PATH, encoding="utf-8"), logging.StreamHandler()],
)
logger = logging.getLogger("value-bot")

bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())
scheduler = AsyncIOScheduler(timezone="Europe/Moscow")
ACTION_GUARD = {}


# =========================
# STATES
# =========================
class ShiftState(StatesGroup):
    waiting_budget = State()
    waiting_risk_decision = State()
    waiting_bet_amount = State()
    waiting_end_shift_confirm = State()
    waiting_delete_last_confirm = State()
    waiting_result_bet_number = State()
    waiting_result_status = State()
    waiting_edit_stake_bet_number = State()
    waiting_edit_stake_value = State()
    waiting_shift_number = State()
    waiting_shift_bet_number = State()
    waiting_shift_bet_result_status = State()


# =========================
# CONSTANTS / LABELS
# =========================
RESULT_MAP = {
    "🕒 В ожидании": "pending",
    "✅ Выигрыш": "win",
    "❌ Проигрыш": "lose",
    "🟡 Половина выигрыша": "half_win",
    "🟠 Половина проигрыша": "half_lose",
    "↩️ Возврат": "refund",
}
RESULT_LABELS = {
    "pending": "🕒 В ожидании",
    "win": "✅ Выигрыш",
    "lose": "❌ Проигрыш",
    "half_win": "🟡 Половина выигрыша",
    "half_lose": "🟠 Половина проигрыша",
    "refund": "↩️ Возврат",
}
BOOKMAKER_EMOJI = {
    "fonbet": "🔴",
    "фонбет": "🔴",
    "betcity": "🔵",
    "бетсити": "🔵",
    "betboom": "🟣",
    "бетбум": "🟣",
    "marathon": "🟠",
    "марафон": "🟠",
    "ligastavok": "🟢",
    "лига ставок": "🟢",
    "liga stavok": "🟢",
}


# =========================
# HELPERS
# =========================
def now_dt() -> datetime:
    return datetime.now(TIMEZONE)


def now_str() -> str:
    return now_dt().strftime("%Y-%m-%d %H:%M:%S")


def as_float(text: str) -> float:
    return float(text.replace(" ", "").replace(",", "."))


def connect():
    return sqlite3.connect(DB_PATH, check_same_thread=False)


def has_recent_action(user_id: int, action: str, seconds: float = DOUBLE_TAP_SECONDS) -> bool:
    key = f"{user_id}:{action}"
    now_ts = time.time()
    last_ts = ACTION_GUARD.get(key)
    ACTION_GUARD[key] = now_ts
    return last_ts is not None and (now_ts - last_ts) < seconds


def is_forward_message(message: Message) -> bool:
    return bool(
        getattr(message, "forward_origin", None)
        or getattr(message, "forward_from_chat", None)
        or getattr(message, "forward_from", None)
        or getattr(message, "forward_sender_name", None)
    )


def normalize_text(text: str) -> str:
    text = (text or "").lower().replace("ё", "е").replace("−", "-").replace("–", "-")
    text = re.sub(r"[^a-zа-я0-9+\-. ]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def hash_text(text: str) -> str:
    return hashlib.md5(text.strip().encode("utf-8")).hexdigest()


def bookmaker_label(bookmaker: str) -> str:
    norm = normalize_text(bookmaker)
    for key, emoji in BOOKMAKER_EMOJI.items():
        if key in norm:
            return f"{emoji} {bookmaker}"
    return f"⚪ {bookmaker or 'Other'}"


def split_sport_tournament(header: str):
    parts = [x.strip() for x in re.split(r"\s+-\s+", header.strip()) if x.strip()]
    sport = parts[0] if parts else ""
    tournament = " - ".join(parts[1:]) if len(parts) > 1 else ""
    return sport, tournament


def split_teams(match_name: str):
    parts = re.split(r"\s+[–-]\s+", match_name)
    if len(parts) >= 2:
        return parts[0].strip(), parts[1].strip()
    return "", ""


def parse_match_start(match_date: str, match_time: str) -> datetime:
    day, month = match_date.split("/")
    hour, minute = match_time.split(":")
    now = now_dt()
    dt = datetime(now.year, int(month), int(day), int(hour), int(minute), tzinfo=TIMEZONE)
    if dt < now - timedelta(days=30):
        dt = dt.replace(year=now.year + 1)
    return dt


def event_key_from(match_name: str, match_start_at: str) -> str:
    return f"{normalize_text(match_name)}__{match_start_at[:16]}"


def token_overlap_score(phrase: str, team: str) -> int:
    p_tokens = {t for t in normalize_text(phrase).split() if len(t) >= 3}
    t_tokens = {t for t in normalize_text(team).split() if len(t) >= 3}
    return len(p_tokens & t_tokens)


def infer_selection_side(selection: str, team_a: str, team_b: str) -> str:
    s = normalize_text(selection)
    if re.search(r"\bп1\b|\b1\b", s):
        return "home"
    if re.search(r"\bп2\b|\b2\b", s):
        return "away"
    score_a = token_overlap_score(selection, team_a)
    score_b = token_overlap_score(selection, team_b)
    if score_a > score_b and score_a > 0:
        return "home"
    if score_b > score_a and score_b > 0:
        return "away"
    return f"selection:{normalize_text(selection)[:60]}"


def extract_selection_phrase(market: str) -> str:
    m = re.search(r"(.+?)\s+(?:победит\s+с\s+форой|с\s+форой|фора|гандикап)", market, re.I)
    if m:
        return m.group(1).strip(" .,-")
    m = re.search(r"победа\s+(.+?)(?:\s+с\s+учетом|\s+-|\.|$)", market, re.I)
    if m:
        return m.group(1).strip(" .,-")
    return market[:80]


def parse_line_value(text: str):
    text = text.replace("−", "-").replace(",", ".")
    m = re.search(r"(?:фор[ао]й?|гандикап)[^+\-0-9]{0,20}([+\-]?\d+(?:\.\d+)?)", text, re.I)
    if m:
        return float(m.group(1))
    m = re.search(r"тотал\s+(?:больше|меньше)\s+(\d+(?:\.\d+)?)", text, re.I)
    if m:
        return float(m.group(1))
    return None


def normalize_market(parsed: dict) -> dict:
    market = parsed["market"]
    market_norm = normalize_text(market)
    team_a, team_b = split_teams(parsed["match_name"])
    line_value = parse_line_value(market)
    market_type = "unknown"
    market_side = "unknown"
    market_group = "Другое"
    selection_name = ""
    selection_side = "unknown"
    period_type = "full_match"

    if "1-й сет" in market_norm or "1 сет" in market_norm:
        period_type = "first_set"
    elif "1-й период" in market_norm or "1 период" in market_norm:
        period_type = "first_period"
    elif "с учетом овертайма" in market_norm or "овертаим" in market_norm or "овертайм" in market_norm:
        period_type = "full_match_overtime"

    if "фор" in market_norm or "гандикап" in market_norm:
        market_type = "handicap"
        market_side = "plus" if (line_value is not None and line_value > 0) else "minus" if (line_value is not None and line_value < 0) else "zero"
        market_group = "Фора плюсовая" if market_side == "plus" else "Фора минусовая" if market_side == "minus" else "Фора 0"
        selection_name = extract_selection_phrase(market)
        selection_side = infer_selection_side(selection_name, team_a, team_b)
    elif "тотал больше" in market_norm or re.search(r"\bтб\b", market_norm):
        market_type = "total"
        market_side = "over"
        market_group = "Тотал больше"
        selection_name = "total"
    elif "тотал меньше" in market_norm or re.search(r"\bтм\b", market_norm):
        market_type = "total"
        market_side = "under"
        market_group = "Тотал меньше"
        selection_name = "total"
    elif "победа" in market_norm or "победит" in market_norm:
        market_type = "moneyline"
        market_side = "win"
        market_group = "Победа"
        selection_name = extract_selection_phrase(market)
        selection_side = infer_selection_side(selection_name, team_a, team_b)

    semantic_key = "__".join([
        parsed.get("event_key", ""),
        market_type,
        selection_side,
        market_side,
        period_type,
    ])

    parsed.update({
        "team_a": team_a,
        "team_b": team_b,
        "selection_name": selection_name,
        "selection_side": selection_side,
        "market_type": market_type,
        "market_group": market_group,
        "market_side": market_side,
        "line_value": line_value,
        "period_type": period_type,
        "semantic_key": semantic_key,
    })
    return parsed


def calc_settlement(stake: float, odds: float, result_status: str):
    if result_status == "pending":
        return None, None
    if result_status == "win":
        payout = round(stake * odds, 2)
        return payout, round(payout - stake, 2)
    if result_status == "lose":
        return 0.0, round(-stake, 2)
    if result_status == "half_win":
        payout = round((stake / 2) * odds + (stake / 2), 2)
        return payout, round(payout - stake, 2)
    if result_status == "half_lose":
        payout = round(stake / 2, 2)
        return payout, round(payout - stake, 2)
    if result_status == "refund":
        return round(stake, 2), 0.0
    return None, None


def calc_roi(total_profit: float, total_stake: float) -> float:
    if not total_stake:
        return 0.0
    return round((total_profit / total_stake) * 100, 2)


def hedge_amount(stake1: float, odds1: float, odds2: float) -> float:
    if not odds2:
        return 0.0
    return round((stake1 * odds1) / odds2, 2)


def arbitrage_metrics(stake1: float, odds1: float, odds2: float):
    stake2 = hedge_amount(stake1, odds1, odds2)
    payout = round(stake1 * odds1, 2)
    total_stake = round(stake1 + stake2, 2)
    profit = round(payout - total_stake, 2)
    roi = calc_roi(profit, total_stake)
    implied_sum = round(1 / odds1 + 1 / odds2, 4) if odds1 and odds2 else 0
    return stake2, payout, total_stake, profit, roi, implied_sum


def handicap_bound(side: str, line: float):
    # Возвращает условие по марже Team A: margin > lower или margin < upper
    if side == "home":
        return "gt", -line
    if side == "away":
        return "lt", line
    return None, None


def save_log(level: str, message: str):
    db = connect()
    db.execute("CREATE TABLE IF NOT EXISTS logs(id INTEGER PRIMARY KEY AUTOINCREMENT, level TEXT NOT NULL, message TEXT NOT NULL, created_at TEXT NOT NULL)")
    db.execute("INSERT INTO logs(level, message, created_at) VALUES (?, ?, ?)", (level, message, now_str()))
    db.commit()
    db.close()


def log_info(text: str):
    logger.info(text)
    save_log("INFO", text)


def log_warning(text: str):
    logger.warning(text)
    save_log("WARNING", text)


def log_error(text: str):
    logger.error(text)
    save_log("ERROR", text)


# =========================
# DB INIT / MIGRATIONS
# =========================
def add_column_if_not_exists(table_name: str, column_name: str, ddl: str):
    db = connect()
    cols = db.execute(f"PRAGMA table_info({table_name})").fetchall()
    if column_name not in [c[1] for c in cols]:
        db.execute(ddl)
        db.commit()
    db.close()


def init_db():
    db = connect()
    db.execute("""
    CREATE TABLE IF NOT EXISTS shifts(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        started_at TEXT NOT NULL,
        ended_at TEXT,
        budget REAL NOT NULL,
        spent REAL NOT NULL DEFAULT 0,
        status TEXT NOT NULL
    )
    """)
    db.execute("""
    CREATE TABLE IF NOT EXISTS bets(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        shift_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        created_at TEXT NOT NULL,
        sport TEXT,
        tournament TEXT,
        match_name TEXT,
        match_date TEXT,
        match_time TEXT,
        match_start_at TEXT,
        market TEXT,
        odds REAL,
        ev REAL,
        bookmaker TEXT,
        stake REAL,
        source_text TEXT,
        match_hash TEXT UNIQUE,
        reminder_sent INTEGER DEFAULT 0,
        result_status TEXT DEFAULT 'pending',
        payout REAL,
        profit REAL,
        event_key TEXT,
        team_a TEXT,
        team_b TEXT,
        selection_name TEXT,
        selection_side TEXT,
        market_type TEXT,
        market_group TEXT,
        market_side TEXT,
        line_value REAL,
        period_type TEXT,
        semantic_key TEXT,
        risk_status TEXT DEFAULT 'new',
        risk_notes TEXT
    )
    """)
    db.execute("""
    CREATE TABLE IF NOT EXISTS rejected_bets(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        created_at TEXT NOT NULL,
        user_id INTEGER NOT NULL,
        sport TEXT,
        tournament TEXT,
        match_name TEXT,
        market TEXT,
        odds REAL,
        ev REAL,
        bookmaker TEXT,
        source_text TEXT,
        event_key TEXT,
        market_type TEXT,
        market_group TEXT,
        risk_status TEXT,
        risk_notes TEXT,
        reason TEXT
    )
    """)
    db.execute("CREATE TABLE IF NOT EXISTS logs(id INTEGER PRIMARY KEY AUTOINCREMENT, level TEXT NOT NULL, message TEXT NOT NULL, created_at TEXT NOT NULL)")
    db.commit()
    db.close()

    migrations = [
        ("bets", "match_start_at", "ALTER TABLE bets ADD COLUMN match_start_at TEXT"),
        ("bets", "reminder_sent", "ALTER TABLE bets ADD COLUMN reminder_sent INTEGER DEFAULT 0"),
        ("bets", "result_status", "ALTER TABLE bets ADD COLUMN result_status TEXT DEFAULT 'pending'"),
        ("bets", "payout", "ALTER TABLE bets ADD COLUMN payout REAL"),
        ("bets", "profit", "ALTER TABLE bets ADD COLUMN profit REAL"),
        ("bets", "event_key", "ALTER TABLE bets ADD COLUMN event_key TEXT"),
        ("bets", "team_a", "ALTER TABLE bets ADD COLUMN team_a TEXT"),
        ("bets", "team_b", "ALTER TABLE bets ADD COLUMN team_b TEXT"),
        ("bets", "selection_name", "ALTER TABLE bets ADD COLUMN selection_name TEXT"),
        ("bets", "selection_side", "ALTER TABLE bets ADD COLUMN selection_side TEXT"),
        ("bets", "market_type", "ALTER TABLE bets ADD COLUMN market_type TEXT"),
        ("bets", "market_group", "ALTER TABLE bets ADD COLUMN market_group TEXT"),
        ("bets", "market_side", "ALTER TABLE bets ADD COLUMN market_side TEXT"),
        ("bets", "line_value", "ALTER TABLE bets ADD COLUMN line_value REAL"),
        ("bets", "period_type", "ALTER TABLE bets ADD COLUMN period_type TEXT"),
        ("bets", "semantic_key", "ALTER TABLE bets ADD COLUMN semantic_key TEXT"),
        ("bets", "risk_status", "ALTER TABLE bets ADD COLUMN risk_status TEXT DEFAULT 'new'"),
        ("bets", "risk_notes", "ALTER TABLE bets ADD COLUMN risk_notes TEXT"),
    ]
    for t, c, ddl in migrations:
        add_column_if_not_exists(t, c, ddl)
    log_info("Database initialized")


# =========================
# DB OPERATIONS
# =========================
def get_active_shift(user_id: int):
    db = connect()
    row = db.execute("SELECT id, budget, spent, started_at FROM shifts WHERE user_id=? AND status='active' ORDER BY id DESC LIMIT 1", (user_id,)).fetchone()
    db.close()
    return row


def start_shift_db(user_id: int, started_at: str, budget: float):
    db = connect()
    db.execute("INSERT INTO shifts(user_id, started_at, budget, spent, status) VALUES (?, ?, ?, 0, 'active')", (user_id, started_at, budget))
    db.commit()
    db.close()
    log_info(f"Shift started | user={user_id} | budget={budget}")


def end_shift_db(shift_id: int, ended_at: str):
    db = connect()
    db.execute("UPDATE shifts SET ended_at=?, status='ended' WHERE id=?", (ended_at, shift_id))
    db.commit()
    db.close()
    log_info(f"Shift ended | shift_id={shift_id}")


def add_bet_db(shift_id: int, user_id: int, created_at: str, parsed: dict, stake: float):
    db = connect()
    db.execute("""
        INSERT INTO bets(
            shift_id,user_id,created_at,sport,tournament,match_name,match_date,match_time,match_start_at,
            market,odds,ev,bookmaker,stake,source_text,match_hash,reminder_sent,result_status,payout,profit,
            event_key,team_a,team_b,selection_name,selection_side,market_type,market_group,market_side,line_value,period_type,semantic_key,risk_status,risk_notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 'pending', NULL, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        shift_id, user_id, created_at, parsed["sport"], parsed["tournament"], parsed["match_name"], parsed["match_date"], parsed["match_time"], parsed["match_start_at"],
        parsed["market"], parsed["odds"], parsed["ev"], parsed["bookmaker"], stake, parsed["source_text"], parsed["hash"],
        parsed.get("event_key"), parsed.get("team_a"), parsed.get("team_b"), parsed.get("selection_name"), parsed.get("selection_side"), parsed.get("market_type"), parsed.get("market_group"), parsed.get("market_side"), parsed.get("line_value"), parsed.get("period_type"), parsed.get("semantic_key"), parsed.get("risk_status", "new"), parsed.get("risk_notes", "")
    ))
    db.execute("UPDATE shifts SET spent = spent + ? WHERE id = ?", (stake, shift_id))
    db.commit()
    db.close()
    log_info(f"Bet added | shift_id={shift_id} | user={user_id} | stake={stake} | risk={parsed.get('risk_status')}")


def save_rejected_bet(user_id: int, parsed: dict, reason: str):
    db = connect()
    db.execute("""
        INSERT INTO rejected_bets(created_at,user_id,sport,tournament,match_name,market,odds,ev,bookmaker,source_text,event_key,market_type,market_group,risk_status,risk_notes,reason)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        now_str(), user_id, parsed.get("sport"), parsed.get("tournament"), parsed.get("match_name"), parsed.get("market"), parsed.get("odds"), parsed.get("ev"), parsed.get("bookmaker"), parsed.get("source_text"), parsed.get("event_key"), parsed.get("market_type"), parsed.get("market_group"), parsed.get("risk_status"), parsed.get("risk_notes"), reason
    ))
    db.commit()
    db.close()
    log_warning(f"Rejected bet saved | reason={reason} | risk={parsed.get('risk_status')}")


def get_pending_similar_bets(parsed: dict):
    db = connect()
    rows = db.execute("""
        SELECT id, sport, match_name, market, odds, stake, bookmaker, result_status, selection_side, market_type, market_side, line_value, event_key, semantic_key, market_group
        FROM bets
        WHERE event_key = ? AND result_status = 'pending'
        ORDER BY id DESC
    """, (parsed.get("event_key"),)).fetchall()
    db.close()
    return rows


def get_last_bets(user_id: int, limit: int = 20):
    db = connect()
    rows = db.execute("SELECT id, sport, match_name, market, odds, stake, bookmaker, created_at, result_status FROM bets WHERE user_id=? ORDER BY id DESC LIMIT ?", (user_id, limit)).fetchall()
    db.close()
    return rows


def get_pending_bets(user_id: int, limit: int = 20):
    db = connect()
    rows = db.execute("SELECT id, sport, match_name, market, odds, stake, bookmaker, created_at, result_status FROM bets WHERE user_id=? AND result_status='pending' ORDER BY id DESC LIMIT ?", (user_id, limit)).fetchall()
    db.close()
    return rows


def get_last_bet(user_id: int):
    rows = get_last_bets(user_id, 1)
    return rows[0] if rows else None


def get_bet_by_id(bet_id: int):
    db = connect()
    row = db.execute("""
        SELECT id,sport,tournament,match_name,match_date,match_time,market,odds,ev,bookmaker,stake,created_at,result_status,payout,profit,shift_id
        FROM bets WHERE id=?
    """, (bet_id,)).fetchone()
    db.close()
    return row


def get_shift_by_id(shift_id: int, user_id: int):
    db = connect()
    row = db.execute("SELECT id,user_id,started_at,ended_at,budget,spent,status FROM shifts WHERE id=? AND user_id=?", (shift_id, user_id)).fetchone()
    db.close()
    return row


def list_shifts(user_id: int, offset: int = 0, limit: int = SHIFT_PAGE_SIZE):
    db = connect()
    rows = db.execute("SELECT id,started_at,ended_at,budget,spent,status FROM shifts WHERE user_id=? ORDER BY id DESC LIMIT ? OFFSET ?", (user_id, limit, offset)).fetchall()
    total = db.execute("SELECT COUNT(*) FROM shifts WHERE user_id=?", (user_id,)).fetchone()[0]
    db.close()
    return rows, total


def get_bets_by_shift(shift_id: int, offset: int = 0, limit: int = BET_PAGE_SIZE):
    db = connect()
    rows = db.execute("SELECT id,sport,match_name,market,odds,stake,bookmaker,created_at,result_status FROM bets WHERE shift_id=? ORDER BY id DESC LIMIT ? OFFSET ?", (shift_id, limit, offset)).fetchall()
    total = db.execute("SELECT COUNT(*) FROM bets WHERE shift_id=?", (shift_id,)).fetchone()[0]
    db.close()
    return rows, total


def count_bets_in_shift(shift_id: int) -> int:
    db = connect()
    row = db.execute("SELECT COUNT(*) FROM bets WHERE shift_id=?", (shift_id,)).fetchone()
    db.close()
    return row[0] if row else 0


def get_shift_stats(shift_id: int):
    db = connect()
    row = db.execute("""
        SELECT COUNT(*),COALESCE(SUM(stake),0),COALESCE(AVG(odds),0),COALESCE(AVG(ev),0),
               COALESCE(SUM(CASE WHEN result_status='win' THEN 1 ELSE 0 END),0),
               COALESCE(SUM(CASE WHEN result_status='lose' THEN 1 ELSE 0 END),0),
               COALESCE(SUM(CASE WHEN result_status='half_win' THEN 1 ELSE 0 END),0),
               COALESCE(SUM(CASE WHEN result_status='half_lose' THEN 1 ELSE 0 END),0),
               COALESCE(SUM(CASE WHEN result_status='refund' THEN 1 ELSE 0 END),0),
               COALESCE(SUM(CASE WHEN result_status='pending' THEN 1 ELSE 0 END),0),
               COALESCE(SUM(profit),0)
        FROM bets WHERE shift_id=?
    """, (shift_id,)).fetchone()
    db.close()
    return row


def get_market_stats_by_shift(shift_id: int):
    db = connect()
    rows = db.execute("""
        SELECT COALESCE(market_group,'Другое'), COUNT(*), COALESCE(SUM(stake),0), COALESCE(AVG(stake),0), COALESCE(AVG(odds),0), COALESCE(AVG(ev),0), COALESCE(SUM(profit),0),
               COALESCE(SUM(CASE WHEN result_status='pending' THEN 1 ELSE 0 END),0)
        FROM bets WHERE shift_id=? GROUP BY COALESCE(market_group,'Другое') ORDER BY COUNT(*) DESC
    """, (shift_id,)).fetchall()
    total = db.execute("SELECT COUNT(*) FROM bets WHERE shift_id=?", (shift_id,)).fetchone()[0]
    db.close()
    return rows, total


def get_bookmaker_stats_by_shift(shift_id: int):
    db = connect()
    rows = db.execute("""
        SELECT COALESCE(bookmaker,'Other'), COUNT(*), COALESCE(SUM(stake),0), COALESCE(AVG(stake),0), COALESCE(AVG(odds),0), COALESCE(SUM(profit),0)
        FROM bets WHERE shift_id=? GROUP BY COALESCE(bookmaker,'Other') ORDER BY COUNT(*) DESC
    """, (shift_id,)).fetchall()
    total = db.execute("SELECT COUNT(*) FROM bets WHERE shift_id=?", (shift_id,)).fetchone()[0]
    db.close()
    return rows, total


def get_today_stats(user_id: int):
    db = connect()
    start_day = now_dt().replace(hour=0, minute=0, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
    end_day = now_dt().replace(hour=23, minute=59, second=59, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
    row = db.execute("""
        SELECT COUNT(*),COALESCE(SUM(stake),0),COALESCE(AVG(odds),0),COALESCE(AVG(ev),0),
               COALESCE(SUM(CASE WHEN result_status='win' THEN 1 ELSE 0 END),0),
               COALESCE(SUM(CASE WHEN result_status='lose' THEN 1 ELSE 0 END),0),
               COALESCE(SUM(CASE WHEN result_status='half_win' THEN 1 ELSE 0 END),0),
               COALESCE(SUM(CASE WHEN result_status='half_lose' THEN 1 ELSE 0 END),0),
               COALESCE(SUM(CASE WHEN result_status='refund' THEN 1 ELSE 0 END),0),
               COALESCE(SUM(CASE WHEN result_status='pending' THEN 1 ELSE 0 END),0),
               COALESCE(SUM(profit),0)
        FROM bets WHERE user_id=? AND created_at>=? AND created_at<=?
    """, (user_id, start_day, end_day)).fetchone()
    db.close()
    return row


def update_bet_result(bet_id: int, result_status: str):
    db = connect()
    row = db.execute("SELECT stake,odds FROM bets WHERE id=?", (bet_id,)).fetchone()
    if not row:
        db.close()
        return False
    stake, odds = row
    payout, profit = calc_settlement(stake, odds, result_status)
    db.execute("UPDATE bets SET result_status=?, payout=?, profit=? WHERE id=?", (result_status, payout, profit, bet_id))
    db.commit()
    db.close()
    log_info(f"Bet result updated | bet_id={bet_id} | status={result_status}")
    return True


def delete_bet_by_id(bet_id: int):
    db = connect()
    row = db.execute("SELECT shift_id,stake FROM bets WHERE id=?", (bet_id,)).fetchone()
    if not row:
        db.close()
        return False, None
    shift_id, stake = row
    db.execute("DELETE FROM bets WHERE id=?", (bet_id,))
    db.execute("UPDATE shifts SET spent=spent-? WHERE id=?", (stake, shift_id))
    db.commit()
    db.close()
    log_warning(f"Bet deleted | bet_id={bet_id}")
    return True, stake


def update_bet_stake(bet_id: int, new_stake: float):
    db = connect()
    row = db.execute("SELECT shift_id, stake, odds, result_status FROM bets WHERE id=?", (bet_id,)).fetchone()
    if not row:
        db.close()
        return False, "Ставка не найдена."
    shift_id, old_stake, odds, result_status = row
    delta = new_stake - old_stake
    payout, profit = calc_settlement(new_stake, odds, result_status)
    db.execute("UPDATE bets SET stake=?, payout=?, profit=? WHERE id=?", (new_stake, payout, profit, bet_id))
    db.execute("UPDATE shifts SET spent=spent+? WHERE id=?", (delta, shift_id))
    db.commit()
    db.close()
    return True, old_stake


def get_recent_logs(limit: int = 10):
    db = connect()
    rows = db.execute("SELECT level,message,created_at FROM logs ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
    db.close()
    return rows


def get_due_reminders():
    db = connect()
    rows = db.execute("""
        SELECT id,user_id,match_name,market,match_start_at,stake,odds,bookmaker FROM bets
        WHERE reminder_sent=0 AND result_status='pending' AND match_start_at IS NOT NULL
    """).fetchall()
    db.close()
    now = now_dt()
    upper = now + timedelta(minutes=REMINDER_MINUTES)
    due = []
    for row in rows:
        bet_id, user_id, match_name, market, match_start_at, stake, odds, bookmaker = row
        try:
            dt = datetime.fromisoformat(match_start_at)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=TIMEZONE)
        except Exception:
            continue
        if now <= dt <= upper:
            due.append({"id": bet_id, "user_id": user_id, "match_name": match_name, "market": market, "match_start_at": dt, "stake": stake, "odds": odds, "bookmaker": bookmaker})
    return due


def mark_reminder_sent(bet_id: int):
    db = connect()
    db.execute("UPDATE bets SET reminder_sent=1 WHERE id=?", (bet_id,))
    db.commit()
    db.close()


# =========================
# PARSER + RISK ENGINE
# =========================
def parse_bet(text: str):
    pattern = re.compile(r"(⚽️🏒🎾.*?)(?=\n\s*⚽️🏒🎾|\Z)", re.S)
    matches = pattern.findall(text)
    if not matches:
        return None
    block = None
    for candidate in matches:
        if re.search(r"⚽️🏒🎾\s*(.+?)\n", candidate, re.S) and re.search(r"🚩\s*(.+?),\s*(\d{1,2}:\d{2})\s+(\d{2}/\d{2})", candidate) and re.search(r"❗️\s*(.+?)\s*коэф\.?\s*([\d.,]+)❗️", candidate, re.S):
            block = candidate.strip()
            break
    if not block:
        return None
    sport_line = re.search(r"⚽️🏒🎾\s*(.+?)\n", block, re.S)
    event_line = re.search(r"🚩\s*(.+?),\s*(\d{1,2}:\d{2})\s+(\d{2}/\d{2})", block)
    market_line = re.search(r"❗️\s*(.+?)\s*коэф\.?\s*([\d.,]+)❗️", block, re.S)
    ev_line = re.search(r"Математическое ожидание\s*≈\s*([\d.,]+)%", block, re.I)
    bk_line = re.search(r"Ставка сделана👉\s*([^\n(]+)", block, re.I)
    if not sport_line or not event_line or not market_line:
        return None
    full_header = sport_line.group(1).strip()
    sport, tournament = split_sport_tournament(full_header)
    match_name = event_line.group(1).strip()
    match_time = event_line.group(2).strip()
    match_date = event_line.group(3).strip()
    market = re.sub(r"\s+", " ", market_line.group(1)).strip()
    odds = float(market_line.group(2).replace(",", "."))
    ev = float(ev_line.group(1).replace(",", ".")) if ev_line else None
    bookmaker = bk_line.group(1).strip() if bk_line else ""
    match_start_at = parse_match_start(match_date, match_time)
    parsed = {
        "sport": sport,
        "tournament": tournament,
        "match_name": match_name,
        "match_time": match_time,
        "match_date": match_date,
        "match_start_at": match_start_at.isoformat(),
        "market": market,
        "odds": odds,
        "ev": ev,
        "bookmaker": bookmaker,
        "hash": hash_text(block),
        "source_text": block,
    }
    parsed["event_key"] = event_key_from(match_name, parsed["match_start_at"])
    return normalize_market(parsed)


def analyze_risk(parsed: dict):
    rows = get_pending_similar_bets(parsed)
    notes = []
    statuses = []
    for row in rows:
        existing = {
            "id": row[0], "sport": row[1], "match_name": row[2], "market": row[3], "odds": row[4], "stake": row[5], "bookmaker": row[6],
            "selection_side": row[8], "market_type": row[9], "market_side": row[10], "line_value": row[11], "event_key": row[12], "semantic_key": row[13], "market_group": row[14]
        }
        if existing["id"] is None:
            continue
        # Повтор: тот же матч + тот же тип рынка + та же сторона выбора. Для форы линии не важны — повтор жёсткий.
        same_type = existing["market_type"] == parsed.get("market_type")
        same_selection = existing["selection_side"] == parsed.get("selection_side") and parsed.get("selection_side") not in {None, "unknown"}
        if same_type and same_selection and parsed.get("market_type") in {"handicap", "moneyline"}:
            statuses.append("duplicate")
            notes.append(
                f"⚠️ <b>ВНИМАНИЕ: ПОВТОР</b>\n"
                f"Уже есть: {existing['market']}\n"
                f"🏦 {bookmaker_label(existing['bookmaker'])} | 📈 {existing['odds']} | 💸 {existing['stake']}\n"
                f"Новая: {parsed['market']}\n"
                f"Это та же сторона и тот же тип рынка. Похоже на усиление позиции."
            )
            continue
        if same_type and parsed.get("market_type") == "total" and existing.get("market_side") == parsed.get("market_side"):
            statuses.append("duplicate")
            notes.append(
                f"⚠️ <b>ВНИМАНИЕ: ПОВТОР ПО ТОТАЛУ</b>\n"
                f"Уже есть: {existing['market']} | 📈 {existing['odds']} | 💸 {existing['stake']}\n"
                f"Новая: {parsed['market']}"
            )
            continue

        # Фора: противоположные стороны => коридор / вилка / плечо без плюса
        if same_type and parsed.get("market_type") == "handicap" and existing.get("selection_side") in {"home", "away"} and parsed.get("selection_side") in {"home", "away"} and existing.get("selection_side") != parsed.get("selection_side"):
            line1 = existing.get("line_value")
            line2 = parsed.get("line_value")
            if line1 is None or line2 is None:
                continue
            cond1, val1 = handicap_bound(existing["selection_side"], float(line1))
            cond2, val2 = handicap_bound(parsed["selection_side"], float(line2))
            if cond1 and cond2:
                lower = max([v for c, v in [(cond1, val1), (cond2, val2)] if c == "gt"], default=None)
                upper = min([v for c, v in [(cond1, val1), (cond2, val2)] if c == "lt"], default=None)
                stake2, payout, total_stake, hedge_profit, hedge_roi, implied = arbitrage_metrics(existing["stake"], existing["odds"], parsed["odds"])
                if lower is not None and upper is not None and lower < upper:
                    width = round(upper - lower, 2)
                    corridor_profit = round(existing["stake"] * (existing["odds"] - 1) + stake2 * (parsed["odds"] - 1), 2)
                    outside_profit = hedge_profit
                    statuses.append("corridor")
                    notes.append(
                        f"🟣 <b>ВНИМАНИЕ: КОРИДОР</b>\n"
                        f"Уже есть: {existing['market']} | 📈 {existing['odds']} | 💸 {existing['stake']}\n"
                        f"Новая: {parsed['market']} | 📈 {parsed['odds']}\n"
                        f"Коридор по марже 1-й команды: <b>{lower} — {upper}</b>\n"
                        f"Ширина коридора: <b>{width}</b> очков\n"
                        f"Рекомендованная сумма второго плеча: <b>{stake2}</b>\n"
                        f"При попадании в коридор: примерно <b>{corridor_profit}</b>\n"
                        f"Вне коридора при балансировке: примерно <b>{outside_profit}</b> ({hedge_roi}%)"
                    )
                else:
                    # Нет коридора. Проверим вилку как покрытие противоположных плеч.
                    statuses.append("arbitrage" if implied < 1 else "opposite_no_value")
                    title = "🟢 <b>ВНИМАНИЕ: ВИЛКА</b>" if implied < 1 else "🟡 <b>Противоположное плечо найдено, но вилки нет</b>"
                    notes.append(
                        f"{title}\n"
                        f"Уже есть: {existing['market']} | 📈 {existing['odds']} | 💸 {existing['stake']}\n"
                        f"Новая: {parsed['market']} | 📈 {parsed['odds']}\n"
                        f"Сумма вероятностей: <b>{implied}</b>\n"
                        f"Рекомендованная сумма второго плеча: <b>{stake2}</b>\n"
                        f"Потенциальный результат при балансировке: <b>{hedge_profit}</b> ({hedge_roi}%)"
                    )

        # Тоталы: ТБ/ТМ => коридор или вилка/плечо
        if same_type and parsed.get("market_type") == "total" and existing.get("market_side") != parsed.get("market_side"):
            line1 = existing.get("line_value")
            line2 = parsed.get("line_value")
            if line1 is None or line2 is None:
                continue
            over_line = line1 if existing["market_side"] == "over" else line2 if parsed["market_side"] == "over" else None
            under_line = line1 if existing["market_side"] == "under" else line2 if parsed["market_side"] == "under" else None
            stake2, payout, total_stake, hedge_profit, hedge_roi, implied = arbitrage_metrics(existing["stake"], existing["odds"], parsed["odds"])
            if over_line is not None and under_line is not None and over_line < under_line:
                width = round(under_line - over_line, 2)
                corridor_profit = round(existing["stake"] * (existing["odds"] - 1) + stake2 * (parsed["odds"] - 1), 2)
                statuses.append("corridor")
                notes.append(
                    f"🟣 <b>ВНИМАНИЕ: КОРИДОР ПО ТОТАЛУ</b>\n"
                    f"Уже есть: {existing['market']} | 📈 {existing['odds']} | 💸 {existing['stake']}\n"
                    f"Новая: {parsed['market']} | 📈 {parsed['odds']}\n"
                    f"Коридор: итоговый тотал между <b>{over_line}</b> и <b>{under_line}</b>\n"
                    f"Ширина: <b>{width}</b>\n"
                    f"Рекомендованная сумма второго плеча: <b>{stake2}</b>\n"
                    f"При попадании в коридор: примерно <b>{corridor_profit}</b>"
                )
            else:
                statuses.append("arbitrage" if implied < 1 else "opposite_no_value")
                title = "🟢 <b>ВНИМАНИЕ: ВИЛКА ПО ТОТАЛУ</b>" if implied < 1 else "🟡 <b>Противоположный тотал, но вилки нет</b>"
                notes.append(
                    f"{title}\n"
                    f"Уже есть: {existing['market']} | 📈 {existing['odds']} | 💸 {existing['stake']}\n"
                    f"Новая: {parsed['market']} | 📈 {parsed['odds']}\n"
                    f"Сумма вероятностей: <b>{implied}</b>\n"
                    f"Рекомендованная сумма второго плеча: <b>{stake2}</b>\n"
                    f"Потенциальный результат при балансировке: <b>{hedge_profit}</b> ({hedge_roi}%)"
                )

    if not statuses:
        status = "new"
        notes_text = "✅ Повторов, коридоров и вилок среди pending-ставок не найдено."
    elif "corridor" in statuses and "arbitrage" in statuses:
        status = "mixed"
        notes_text = "\n\n".join(notes)
    elif "corridor" in statuses:
        status = "corridor"
        notes_text = "\n\n".join(notes)
    elif "arbitrage" in statuses:
        status = "arbitrage"
        notes_text = "\n\n".join(notes)
    elif "duplicate" in statuses:
        status = "duplicate"
        notes_text = "\n\n".join(notes)
    else:
        status = "opposite_no_value"
        notes_text = "\n\n".join(notes)
    parsed["risk_status"] = status
    parsed["risk_notes"] = notes_text
    return parsed


# =========================
# EXPORTS
# =========================
def export_bets_to_csv(user_id: int) -> str | None:
    db = connect()
    rows = db.execute("""
        SELECT id,shift_id,created_at,sport,tournament,match_name,match_date,match_time,match_start_at,market,market_group,odds,ev,bookmaker,stake,result_status,payout,profit,risk_status,risk_notes
        FROM bets WHERE user_id=? ORDER BY id DESC
    """, (user_id,)).fetchall()
    db.close()
    if not rows:
        return None
    filename = f"bets_export_{now_dt().strftime('%Y%m%d_%H%M%S')}.csv"
    with open(filename, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow(["id","shift_id","created_at","sport","tournament","match_name","match_date","match_time","match_start_at","market","market_group","odds","ev","bookmaker","stake","result_status","payout","profit","risk_status","risk_notes"])
        writer.writerows(rows)
    return filename


def export_shift_to_xlsx(user_id: int, shift_id: int) -> str | None:
    shift = get_shift_by_id(shift_id, user_id)
    if not shift:
        return None
    db = connect()
    bets = db.execute("""
        SELECT id,created_at,sport,tournament,match_name,match_date,match_time,match_start_at,market,market_group,odds,ev,bookmaker,stake,result_status,payout,profit,risk_status,risk_notes
        FROM bets WHERE shift_id=? ORDER BY id ASC
    """, (shift_id,)).fetchall()
    rejected = db.execute("SELECT created_at,match_name,market,odds,bookmaker,risk_status,risk_notes,reason FROM rejected_bets WHERE user_id=? ORDER BY id DESC", (user_id,)).fetchall()
    db.close()
    if not bets:
        return None
    filename = f"shift_{shift_id}_report_{now_dt().strftime('%Y%m%d_%H%M%S')}.xlsx"
    wb = Workbook()
    ws = wb.active
    ws.title = "Summary"
    sid, uid, started_at, ended_at, budget, spent, status = shift
    stats = get_shift_stats(shift_id)
    total_bets, total_stake, avg_odds, avg_ev, wins, loses, half_wins, half_loses, refunds, pendings, total_profit = stats
    roi = calc_roi(total_profit, total_stake)
    summary_rows = [
        ["Shift ID", sid], ["Started", started_at], ["Ended", ended_at or "active"], ["Budget", budget], ["Spent", spent], ["Bets", total_bets],
        ["Total stake", round(total_stake, 2)], ["Avg odds", round(avg_odds, 2)], ["Avg EV", round(avg_ev, 2)], ["Profit", round(total_profit, 2)], ["ROI %", roi],
        ["Win", wins], ["Lose", loses], ["Half win", half_wins], ["Half lose", half_loses], ["Refund", refunds], ["Pending", pendings]
    ]
    for r in summary_rows:
        ws.append(r)
    ws.append([])
    ws.append(["Market", "Count", "Share %", "Total stake", "Avg stake", "Avg odds", "Avg EV", "Profit", "ROI %", "Pending"])
    market_rows, total = get_market_stats_by_shift(shift_id)
    for market, count, total_stake_m, avg_stake, avg_odds_m, avg_ev_m, profit_m, pending_m in market_rows:
        ws.append([market, count, round(count / total * 100, 2) if total else 0, round(total_stake_m, 2), round(avg_stake, 2), round(avg_odds_m, 2), round(avg_ev_m, 2), round(profit_m, 2), calc_roi(profit_m, total_stake_m), pending_m])
    ws.append([])
    ws.append(["Bookmaker", "Count", "Share %", "Total stake", "Avg stake", "Avg odds", "Profit", "ROI %"])
    book_rows, total_bk = get_bookmaker_stats_by_shift(shift_id)
    for bookmaker, count, total_stake_b, avg_stake, avg_odds_b, profit_b in book_rows:
        ws.append([bookmaker_label(bookmaker), count, round(count / total_bk * 100, 2) if total_bk else 0, round(total_stake_b, 2), round(avg_stake, 2), round(avg_odds_b, 2), round(profit_b, 2), calc_roi(profit_b, total_stake_b)])

    ws2 = wb.create_sheet("Bets")
    ws2.append(["id","created_at","sport","tournament","match_name","match_date","match_time","match_start_at","market","market_group","odds","ev","bookmaker","stake","result_status","payout","profit","risk_status","risk_notes"])
    for b in bets:
        b = list(b)
        b[12] = bookmaker_label(b[12])
        ws2.append(b)
    ws3 = wb.create_sheet("Risks_Rejected")
    ws3.append(["created_at","match_name","market","odds","bookmaker","risk_status","risk_notes","reason"])
    for r in rejected:
        r = list(r)
        r[4] = bookmaker_label(r[4])
        ws3.append(r)
    wb.save(filename)
    return filename


# =========================
# KEYBOARDS
# =========================
def main_menu_kb():
    return ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="🎯 Смена"), KeyboardButton(text="📚 Ставки")],[KeyboardButton(text="📊 Статистика"), KeyboardButton(text="⚙️ Сервис")],[KeyboardButton(text="❌ Отмена")]], resize_keyboard=True, is_persistent=True)


def shift_menu_kb(active: bool):
    keyboard = [[KeyboardButton(text="🚀 Начать смену")]] if not active else [[KeyboardButton(text="📍 Текущая смена"), KeyboardButton(text="🏁 Завершить смену")]]
    keyboard.append([KeyboardButton(text="⬅️ Назад")])
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True, is_persistent=True)


def bets_menu_kb():
    return ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="➕ Добавить ставку"), KeyboardButton(text="🧾 Последняя ставка")],[KeyboardButton(text="📚 Последние 20 ставок")],[KeyboardButton(text="🏷 Отметить результат"), KeyboardButton(text="🗑 Delete last")],[KeyboardButton(text="✏️ Исправить сумму")],[KeyboardButton(text="⬅️ Назад")]], resize_keyboard=True, is_persistent=True)


def stats_menu_kb():
    return ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="📈 Статистика по смене"), KeyboardButton(text="📋 Список смен")],[KeyboardButton(text="📅 Статистика за день"), KeyboardButton(text="📌 Ближайшие матчи")],[KeyboardButton(text="📤 Export CSV all")],[KeyboardButton(text="⬅️ Назад")]], resize_keyboard=True, is_persistent=True)


def service_menu_kb():
    return ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="📋 Логи")],[KeyboardButton(text="⬅️ Назад")]], resize_keyboard=True, is_persistent=True)


def yes_no_kb():
    return ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="✅ Подтвердить"), KeyboardButton(text="❌ Отмена")]], resize_keyboard=True, is_persistent=True)


def risk_decision_kb():
    return ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="💾 Сохранить"), KeyboardButton(text="🚫 Отказаться")],[KeyboardButton(text="❌ Отмена")]], resize_keyboard=True, is_persistent=True)


def amount_retry_kb():
    return ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="🔁 Повторить ввод суммы")],[KeyboardButton(text="❌ Отмена")]], resize_keyboard=True, is_persistent=True)


def result_kb():
    return ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="🕒 В ожидании")],[KeyboardButton(text="✅ Выигрыш"), KeyboardButton(text="❌ Проигрыш")],[KeyboardButton(text="🟡 Половина выигрыша"), KeyboardButton(text="🟠 Половина проигрыша")],[KeyboardButton(text="↩️ Возврат")],[KeyboardButton(text="❌ Отмена")]], resize_keyboard=True, is_persistent=True)


def shift_list_kb(has_prev: bool, has_next: bool):
    row = []
    if has_prev:
        row.append(KeyboardButton(text="⬅️ Пред. смены"))
    if has_next:
        row.append(KeyboardButton(text="➡️ След. смены"))
    keyboard = [row] if row else []
    keyboard.append([KeyboardButton(text="⬅️ Назад")])
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True, is_persistent=True)


def selected_shift_kb():
    return ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="📈 Отчет смены"), KeyboardButton(text="📚 Ставки смены")],[KeyboardButton(text="📦 XLSX смены")],[KeyboardButton(text="⬅️ Назад")]], resize_keyboard=True, is_persistent=True)


# =========================
# FORMATTERS
# =========================
def format_shift_stats_text(shift_id: int, title: str = "📈 Статистика по смене") -> str:
    shift = get_shift_by_id(shift_id, OWNER_ID)
    if not shift:
        return "📭 Смена не найдена."
    sid, uid, started_at, ended_at, budget, spent, status = shift
    stats = get_shift_stats(shift_id)
    total_bets, total_stake, avg_odds, avg_ev, wins, loses, half_wins, half_loses, refunds, pendings, total_profit = stats
    remain = round(budget - spent, 2)
    roi = calc_roi(total_profit, total_stake)
    text = (
        f"{title}\n\n"
        f"🆔 Смена: <b>{shift_id}</b>\n"
        f"🕒 Начало: <b>{started_at} МСК</b>\n"
        f"🏁 Конец: <b>{ended_at or 'активна'}</b>\n"
        f"🎯 Ставок: <b>{total_bets}</b>\n"
        f"💸 Общая сумма: <b>{round(total_stake, 2)}</b>\n"
        f"📈 Средний КФ: <b>{round(avg_odds, 2) if total_bets else 0}</b>\n"
        f"🧠 Среднее EV: <b>{round(avg_ev, 2) if total_bets else 0}</b>\n\n"
        f"✅ Выигрыш: <b>{wins}</b>\n❌ Проигрыш: <b>{loses}</b>\n🟡 Half win: <b>{half_wins}</b>\n🟠 Half lose: <b>{half_loses}</b>\n↩️ Возврат: <b>{refunds}</b>\n🕒 Pending: <b>{pendings}</b>\n\n"
        f"💰 Бюджет: <b>{budget}</b>\n💸 Поставлено: <b>{spent}</b>\n🟢 Остаток: <b>{remain}</b>\n📊 Прибыль: <b>{round(total_profit, 2)}</b>\n📐 ROI: <b>{roi}%</b>"
    )
    market_rows, total = get_market_stats_by_shift(shift_id)
    if market_rows:
        text += "\n\n📌 <b>Маркеты</b>"
        for market, count, total_stake_m, avg_stake, avg_odds_m, avg_ev_m, profit_m, pending_m in market_rows[:10]:
            text += f"\n• <b>{market}</b>: {count} ставок, {round(count/total*100,2) if total else 0}%, ср. сумма {round(avg_stake,2)}, ср. КФ {round(avg_odds_m,2)}, ROI {calc_roi(profit_m,total_stake_m)}%"
    return text


# =========================
# COMMANDS / NAVIGATION
# =========================
@dp.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    if message.from_user.id != OWNER_ID:
        await message.answer("⛔ Этот бот доступен только владельцу.")
        return
    await state.clear()
    await message.answer("🚀 <b>Бот учёта ставок + риск-сканер запущен</b>\n\nВыбери раздел 👇", reply_markup=main_menu_kb())


@dp.message(F.text == "⬅️ Назад")
async def back_to_main(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("🏠 Главное меню", reply_markup=main_menu_kb())


@dp.message(F.text == "❌ Отмена")
async def cancel_action(message: Message, state: FSMContext):
    data = await state.get_data()
    parsed = data.get("pending_bet")
    if parsed:
        save_rejected_bet(message.from_user.id, parsed, "cancelled")
    await state.clear()
    await message.answer("❌ Действие отменено.", reply_markup=main_menu_kb())


@dp.message(F.text == "🎯 Смена")
async def open_shift_menu(message: Message, state: FSMContext):
    await state.clear()
    active = get_active_shift(message.from_user.id)
    if active:
        _, budget, spent, started_at = active
        await message.answer(f"🟢 <b>Смена активна</b>\n\n🕒 {started_at} МСК\n💰 {budget}\n💸 {spent}\n🟢 Остаток: {round(budget-spent,2)}", reply_markup=shift_menu_kb(True))
    else:
        await message.answer("🎯 <b>Раздел «Смена»</b>", reply_markup=shift_menu_kb(False))


@dp.message(F.text == "📚 Ставки")
async def open_bets_menu(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("📚 <b>Раздел «Ставки»</b>", reply_markup=bets_menu_kb())


@dp.message(F.text == "📊 Статистика")
async def open_stats_menu(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("📊 <b>Раздел «Статистика»</b>", reply_markup=stats_menu_kb())


@dp.message(F.text == "⚙️ Сервис")
async def open_service_menu(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("⚙️ <b>Раздел «Сервис»</b>", reply_markup=service_menu_kb())


# =========================
# SHIFT FLOW
# =========================
@dp.message(F.text == "🚀 Начать смену")
async def start_shift_button(message: Message, state: FSMContext):
    if has_recent_action(message.from_user.id, "start_shift"):
        return
    if get_active_shift(message.from_user.id):
        await message.answer("ℹ️ Смена уже активна.", reply_markup=shift_menu_kb(True))
        return
    await state.set_state(ShiftState.waiting_budget)
    await message.answer("💰 <b>Введи бюджет смены</b>\nПример: <code>10000</code>", reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="❌ Отмена")]], resize_keyboard=True, is_persistent=True))


@dp.message(ShiftState.waiting_budget)
async def budget_input(message: Message, state: FSMContext):
    try:
        budget = as_float(message.text.strip())
        if budget <= 0:
            raise ValueError
    except Exception:
        await message.answer("⚠️ Бюджет не распознан. Введи число, например: <code>10000</code>")
        return
    start_shift_db(message.from_user.id, now_str(), budget)
    await state.clear()
    await message.answer(f"✅ <b>Смена начата</b>\n💰 Бюджет: <b>{budget}</b>", reply_markup=shift_menu_kb(True))


@dp.message(F.text == "📍 Текущая смена")
async def current_shift_handler(message: Message):
    active = get_active_shift(message.from_user.id)
    if not active:
        await message.answer("📭 Активной смены нет.", reply_markup=shift_menu_kb(False))
        return
    shift_id, budget, spent, started_at = active
    await message.answer(format_shift_stats_text(shift_id, "📊 <b>Текущая смена</b>"), reply_markup=shift_menu_kb(True))


@dp.message(F.text == "🏁 Завершить смену")
async def end_shift_handler(message: Message, state: FSMContext):
    active = get_active_shift(message.from_user.id)
    if not active:
        await message.answer("📭 Активной смены нет.", reply_markup=shift_menu_kb(False))
        return
    shift_id, budget, spent, started_at = active
    await state.set_state(ShiftState.waiting_end_shift_confirm)
    await message.answer(f"🏁 <b>Подтвердить завершение смены?</b>\n\n💰 {budget}\n💸 {spent}\n🟢 Остаток: {round(budget-spent,2)}", reply_markup=yes_no_kb())


@dp.message(ShiftState.waiting_end_shift_confirm, F.text == "✅ Подтвердить")
async def confirm_end_shift(message: Message, state: FSMContext):
    active = get_active_shift(message.from_user.id)
    if not active:
        await state.clear()
        await message.answer("📭 Активной смены уже нет.", reply_markup=shift_menu_kb(False))
        return
    shift_id, budget, spent, started_at = active
    end_shift_db(shift_id, now_str())
    await state.clear()
    await message.answer(f"🏁 <b>Смена завершена</b>\n\n{format_shift_stats_text(shift_id, '📊 Итог смены')}", reply_markup=shift_menu_kb(False))


# =========================
# BETS / RESULT FLOW
# =========================
@dp.message(F.text == "➕ Добавить ставку")
async def add_bet_hint(message: Message):
    if not get_active_shift(message.from_user.id):
        await message.answer("⚠️ Сначала начни смену.", reply_markup=shift_menu_kb(False))
        return
    await message.answer("📥 Перешли мне сообщение со ставкой. Я проверю повтор / коридор / вилку перед сохранением.", reply_markup=bets_menu_kb())


@dp.message(F.text == "💾 Сохранить")
async def risk_save_handler(message: Message, state: FSMContext):
    current_state = await state.get_state()
    if current_state != ShiftState.waiting_risk_decision.state:
        return
    await state.set_state(ShiftState.waiting_bet_amount)
    await message.answer("💬 Теперь напиши сумму ставки.", reply_markup=amount_retry_kb())


@dp.message(F.text == "🚫 Отказаться")
async def risk_reject_handler(message: Message, state: FSMContext):
    data = await state.get_data()
    parsed = data.get("pending_bet")
    if parsed:
        save_rejected_bet(message.from_user.id, parsed, "manual_reject")
    await state.clear()
    await message.answer("🚫 Ставка отклонена и сохранена в rejected_bets.", reply_markup=bets_menu_kb())


@dp.message(F.text == "🧾 Последняя ставка")
async def last_bet_handler(message: Message):
    row = get_last_bet(message.from_user.id)
    if not row:
        await message.answer("📭 Пока нет ставок.", reply_markup=bets_menu_kb())
        return
    bet_id, sport, match_name, market, odds, stake, bookmaker, created_at, result_status = row
    await message.answer(f"🧾 <b>Последняя ставка</b>\n\n🏅 {sport}\n🏟 <b>{match_name}</b>\n📌 {market}\n📈 {odds}\n💸 {stake}\n🏦 {bookmaker_label(bookmaker)}\n🏷 {RESULT_LABELS.get(result_status,result_status)}", reply_markup=bets_menu_kb())


@dp.message(F.text == "📚 Последние 20 ставок")
async def last_20_handler(message: Message):
    rows = get_last_bets(message.from_user.id, 20)
    if not rows:
        await message.answer("📭 Пока нет ставок.", reply_markup=bets_menu_kb())
        return
    lines = []
    for i, row in enumerate(rows, 1):
        bet_id, sport, match_name, market, odds, stake, bookmaker, created_at, result_status = row
        lines.append(f"{i}. <b>{sport}</b> | {match_name}\n📌 {market}\n📈 {odds} | 💸 {stake} | 🏦 {bookmaker_label(bookmaker)}\n🏷 {RESULT_LABELS.get(result_status,result_status)}")
    await message.answer("\n\n".join(lines), reply_markup=bets_menu_kb())


@dp.message(F.text == "🗑 Delete last")
async def delete_last_handler(message: Message, state: FSMContext):
    row = get_last_bet(message.from_user.id)
    if not row:
        await message.answer("📭 Нет ставок для удаления.", reply_markup=bets_menu_kb())
        return
    bet_id, sport, match_name, market, odds, stake, bookmaker, created_at, result_status = row
    await state.update_data(delete_bet_id=bet_id)
    await state.set_state(ShiftState.waiting_delete_last_confirm)
    await message.answer(f"🗑 <b>Удалить последнюю ставку?</b>\n\n🏟 {match_name}\n📌 {market}\n💸 {stake}\n📈 {odds}\n🏦 {bookmaker_label(bookmaker)}\n🏷 {RESULT_LABELS.get(result_status,result_status)}", reply_markup=yes_no_kb())


@dp.message(ShiftState.waiting_delete_last_confirm, F.text == "✅ Подтвердить")
async def confirm_delete_last(message: Message, state: FSMContext):
    data = await state.get_data()
    bet_id = data.get("delete_bet_id")
    ok, stake = delete_bet_by_id(bet_id)
    await state.clear()
    await message.answer(f"🗑 Ставка удалена. Возвращено в расход смены: <b>{stake}</b>" if ok else "⚠️ Не удалось удалить ставку.", reply_markup=bets_menu_kb())


@dp.message(F.text == "✏️ Исправить сумму")
async def edit_stake_start(message: Message, state: FSMContext):
    rows = get_last_bets(message.from_user.id, 20)
    if not rows:
        await message.answer("📭 Нет ставок.", reply_markup=bets_menu_kb())
        return
    mapping, lines = {}, ["✏️ <b>Выбери ставку для изменения суммы</b>\n"]
    for idx, row in enumerate(rows, 1):
        bet_id, sport, match_name, market, odds, stake, bookmaker, created_at, result_status = row
        mapping[str(idx)] = bet_id
        lines.append(f"{idx}. <b>{match_name}</b>\n📌 {market}\n💸 {stake} | 📈 {odds}\n")
    await state.update_data(edit_stake_choices=mapping)
    await state.set_state(ShiftState.waiting_edit_stake_bet_number)
    await message.answer("\n".join(lines) + "\nНапиши номер ставки.", reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="❌ Отмена")]], resize_keyboard=True, is_persistent=True))


@dp.message(ShiftState.waiting_edit_stake_bet_number)
async def edit_stake_choose_number(message: Message, state: FSMContext):
    data = await state.get_data()
    mapping = data.get("edit_stake_choices", {})
    text = (message.text or "").strip()
    if text not in mapping:
        await message.answer("⚠️ Номер не найден.")
        return
    bet_id = mapping[text]
    bet = get_bet_by_id(bet_id)
    if not bet:
        await state.clear(); await message.answer("⚠️ Не удалось найти ставку.", reply_markup=bets_menu_kb()); return
    _id, sport, tournament, match_name, match_date, match_time, market, odds, ev, bookmaker, stake, created_at, result_status, payout, profit, shift_id = bet
    await state.update_data(edit_stake_bet_id=bet_id)
    await state.set_state(ShiftState.waiting_edit_stake_value)
    await message.answer(f"✏️ <b>Изменение суммы</b>\n\n🏟 {match_name}\n📌 {market}\n💸 Текущая: <b>{stake}</b>\n\nНапиши новую сумму.")


@dp.message(ShiftState.waiting_edit_stake_value)
async def edit_stake_value(message: Message, state: FSMContext):
    data = await state.get_data(); bet_id = data.get("edit_stake_bet_id")
    try:
        new_stake = as_float((message.text or "").strip())
        if new_stake <= 0: raise ValueError
    except Exception:
        await message.answer("⚠️ Сумма не распознана."); return
    ok, old_stake = update_bet_stake(bet_id, new_stake)
    await state.clear()
    await message.answer(f"✅ Сумма обновлена. Старая: <b>{old_stake}</b>, новая: <b>{new_stake}</b>" if ok else f"⚠️ {old_stake}", reply_markup=bets_menu_kb())


@dp.message(F.text == "🏷 Отметить результат")
async def mark_result_start(message: Message, state: FSMContext):
    rows = get_pending_bets(message.from_user.id, 20)
    if not rows:
        await message.answer("📭 Нет ожидающих ставок.", reply_markup=bets_menu_kb()); return
    mapping, lines = {}, ["🏷 <b>Выбери pending-ставку по номеру</b>\n"]
    for idx, row in enumerate(rows, 1):
        bet_id, sport, match_name, market, odds, stake, bookmaker, created_at, result_status = row
        mapping[str(idx)] = bet_id
        lines.append(f"{idx}. <b>{match_name}</b>\n📌 {market}\n💸 {stake} | 📈 {odds}\n")
    await state.update_data(result_choices=mapping)
    await state.set_state(ShiftState.waiting_result_bet_number)
    await message.answer("\n".join(lines) + "\nНапиши номер.", reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="❌ Отмена")]], resize_keyboard=True, is_persistent=True))


@dp.message(ShiftState.waiting_result_bet_number)
async def result_bet_number_input(message: Message, state: FSMContext):
    data = await state.get_data(); mapping = data.get("result_choices", {})
    text = (message.text or "").strip()
    if text not in mapping:
        await message.answer("⚠️ Номер не найден."); return
    bet_id = mapping[text]; bet = get_bet_by_id(bet_id)
    if not bet:
        await state.clear(); await message.answer("⚠️ Ставка не найдена.", reply_markup=bets_menu_kb()); return
    _id, sport, tournament, match_name, match_date, match_time, market, odds, ev, bookmaker, stake, created_at, result_status, payout, profit, shift_id = bet
    await state.update_data(selected_bet_id=bet_id)
    await state.set_state(ShiftState.waiting_result_status)
    await message.answer(f"🎯 <b>Выбрана ставка</b>\n\n🏟 {match_name}\n📌 {market}\n💸 {stake}\n📈 {odds}\n\nВыбери результат:", reply_markup=result_kb())


@dp.message(ShiftState.waiting_result_status, F.text.in_(list(RESULT_MAP.keys())))
async def set_result_handler(message: Message, state: FSMContext):
    data = await state.get_data(); bet_id = data.get("selected_bet_id")
    result_status = RESULT_MAP[message.text]
    ok = update_bet_result(bet_id, result_status)
    await state.clear()
    await message.answer(f"✅ Результат обновлён: <b>{RESULT_LABELS[result_status]}</b>" if ok else "⚠️ Не удалось обновить результат.", reply_markup=bets_menu_kb())


# =========================
# STATS / SHIFTS / EXPORTS
# =========================
@dp.message(F.text == "📈 Статистика по смене")
async def shift_stats_handler(message: Message):
    active = get_active_shift(message.from_user.id)
    if not active:
        await message.answer("📭 Активной смены нет.", reply_markup=stats_menu_kb()); return
    await message.answer(format_shift_stats_text(active[0]), reply_markup=stats_menu_kb())


@dp.message(F.text == "📅 Статистика за день")
async def today_stats_handler(message: Message):
    stats = get_today_stats(message.from_user.id)
    total_bets, total_stake, avg_odds, avg_ev, wins, loses, half_wins, half_loses, refunds, pendings, total_profit = stats
    await message.answer(f"📅 <b>Статистика за сегодня</b>\n\n🎯 {total_bets}\n💸 {round(total_stake,2)}\n📈 Ср.КФ {round(avg_odds,2) if total_bets else 0}\n📊 Прибыль {round(total_profit,2)}\n📐 ROI {calc_roi(total_profit,total_stake)}%\n🕒 Pending {pendings}", reply_markup=stats_menu_kb())


@dp.message(F.text == "📤 Export CSV all")
async def export_csv_all(message: Message):
    path = export_bets_to_csv(message.from_user.id)
    if not path:
        await message.answer("📭 Нет данных.", reply_markup=stats_menu_kb()); return
    await message.answer_document(FSInputFile(path), caption="📤 CSV за всё время готов.", reply_markup=stats_menu_kb())


@dp.message(F.text == "📋 Список смен")
async def shift_list_start(message: Message, state: FSMContext):
    await show_shift_page(message, state, 0)


async def show_shift_page(message: Message, state: FSMContext, page: int):
    offset = page * SHIFT_PAGE_SIZE
    rows, total = list_shifts(message.from_user.id, offset, SHIFT_PAGE_SIZE)
    if not rows:
        await message.answer("📭 Смен пока нет.", reply_markup=stats_menu_kb()); return
    mapping, lines = {}, [f"📋 <b>Смены {offset+1}–{offset+len(rows)} из {total}</b>\n"]
    for idx, row in enumerate(rows, 1):
        shift_id, started_at, ended_at, budget, spent, status = row
        stats = get_shift_stats(shift_id)
        total_bets, total_stake, avg_odds, avg_ev, wins, loses, half_wins, half_loses, refunds, pendings, total_profit = stats
        mapping[str(idx)] = shift_id
        lines.append(f"{idx}. ID <b>{shift_id}</b> | {started_at}\nСтатус: {status}, бюджет {budget}, поставлено {spent}, ставок {total_bets}, ROI {calc_roi(total_profit,total_stake)}%\n")
    await state.update_data(shift_page=page, shift_choices=mapping)
    await state.set_state(ShiftState.waiting_shift_number)
    await message.answer("\n".join(lines) + "\nНапиши номер смены.", reply_markup=shift_list_kb(page > 0, offset + len(rows) < total))


@dp.message(ShiftState.waiting_shift_number, F.text.in_({"➡️ След. смены", "⬅️ Пред. смены"}))
async def shift_page_nav(message: Message, state: FSMContext):
    data = await state.get_data(); page = data.get("shift_page", 0)
    page = page + 1 if message.text == "➡️ След. смены" else max(0, page - 1)
    await show_shift_page(message, state, page)


@dp.message(ShiftState.waiting_shift_number)
async def shift_choose_number(message: Message, state: FSMContext):
    data = await state.get_data(); mapping = data.get("shift_choices", {})
    text = (message.text or "").strip()
    if text not in mapping:
        await message.answer("⚠️ Номер смены не найден."); return
    shift_id = mapping[text]
    await state.update_data(selected_shift_id=shift_id)
    await state.set_state(None)
    await message.answer(format_shift_stats_text(shift_id, f"📊 <b>Выбрана смена {shift_id}</b>"), reply_markup=selected_shift_kb())


@dp.message(F.text == "📈 Отчет смены")
async def selected_shift_report(message: Message, state: FSMContext):
    data = await state.get_data(); shift_id = data.get("selected_shift_id")
    if not shift_id:
        await message.answer("⚠️ Сначала выбери смену через 📋 Список смен.", reply_markup=stats_menu_kb()); return
    await message.answer(format_shift_stats_text(shift_id), reply_markup=selected_shift_kb())


@dp.message(F.text == "📚 Ставки смены")
async def selected_shift_bets(message: Message, state: FSMContext):
    data = await state.get_data()
    shift_id = data.get("selected_shift_id")
    if not shift_id:
        await message.answer("⚠️ Сначала выбери смену через 📋 Список смен.", reply_markup=stats_menu_kb())
        return

    rows, total = get_bets_by_shift(shift_id, 0, BET_PAGE_SIZE)
    if not rows:
        await message.answer("📭 В этой смене нет ставок.", reply_markup=selected_shift_kb())
        return

    mapping = {}
    lines = [f"📚 <b>Ставки смены {shift_id}</b>
Показано 1–{len(rows)} из {total}
"]
    for idx, row in enumerate(rows, 1):
        bet_id, sport, match_name, market, odds, stake, bookmaker, created_at, result_status = row
        mapping[str(idx)] = bet_id
        lines.append(
            f"{idx}. <b>{match_name}</b>
"
            f"📌 {market}
"
            f"💸 {stake} | 📈 {odds} | 🏦 {bookmaker_label(bookmaker)}
"
            f"🏷 {RESULT_LABELS.get(result_status, result_status)}
"
        )

    await state.update_data(shift_bet_choices=mapping, selected_shift_id=shift_id)
    await state.set_state(ShiftState.waiting_shift_bet_number)
    await message.answer(
        "
".join(lines) + "
Напиши номер ставки, чтобы рассчитать/отметить результат.",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="❌ Отмена")]],
            resize_keyboard=True,
            is_persistent=True,
        ),
    )


@dp.message(ShiftState.waiting_shift_bet_number)
async def selected_shift_bet_number(message: Message, state: FSMContext):
    text = (message.text or "").strip()
    data = await state.get_data()
    mapping = data.get("shift_bet_choices", {})

    if text not in mapping:
        await message.answer("⚠️ Номер ставки не найден. Напиши номер из списка.")
        return

    bet_id = mapping[text]
    bet = get_bet_by_id(bet_id)
    if not bet:
        await state.clear()
        await message.answer("⚠️ Не удалось найти ставку.", reply_markup=selected_shift_kb())
        return

    (
        _id, sport, tournament, match_name, match_date, match_time, market,
        odds, ev, bookmaker, stake, created_at, result_status, payout, profit, shift_id
    ) = bet

    await state.update_data(selected_shift_bet_id=bet_id, selected_shift_id=shift_id)
    await state.set_state(ShiftState.waiting_shift_bet_result_status)
    await message.answer(
        f"🎯 <b>Выбрана ставка из смены</b>

"
        f"🏟 <b>{match_name}</b>
"
        f"📌 {market}
"
        f"💸 Сумма: <b>{stake}</b>
"
        f"📈 КФ: <b>{odds}</b>
"
        f"🏦 {bookmaker_label(bookmaker)}
"
        f"🏷 Сейчас: <b>{RESULT_LABELS.get(result_status, result_status)}</b>

"
        f"Выбери результат:",
        reply_markup=result_kb(),
    )


@dp.message(ShiftState.waiting_shift_bet_result_status, F.text.in_(list(RESULT_MAP.keys())))
async def selected_shift_bet_set_result(message: Message, state: FSMContext):
    data = await state.get_data()
    bet_id = data.get("selected_shift_bet_id")
    shift_id = data.get("selected_shift_id")
    result_status = RESULT_MAP[message.text]

    if not bet_id:
        await state.clear()
        await message.answer("⚠️ Не выбрана ставка.", reply_markup=stats_menu_kb())
        return

    ok = update_bet_result(bet_id, result_status)
    await state.update_data(selected_shift_id=shift_id)
    await state.set_state(None)

    await message.answer(
        f"✅ Результат обновлён: <b>{RESULT_LABELS[result_status]}</b>" if ok else "⚠️ Не удалось обновить результат.",
        reply_markup=selected_shift_kb(),
    )


@dp.message(F.text == "📦 XLSX смены")
async def selected_shift_xlsx(message: Message, state: FSMContext):
    data = await state.get_data()
    shift_id = data.get("selected_shift_id")
    if not shift_id:
        await message.answer("⚠️ Сначала выбери смену через 📋 Список смен.", reply_markup=stats_menu_kb())
        return
    path = export_shift_to_xlsx(message.from_user.id, shift_id)
    if not path:
        await message.answer("📭 Нет данных для экспорта.", reply_markup=selected_shift_kb())
        return
    await message.answer_document(FSInputFile(path), caption=f"📦 XLSX отчёт по смене {shift_id} готов.", reply_markup=selected_shift_kb())


@dp.message(F.text == "📌 Ближайшие матчи")
async def upcoming_matches_handler(message: Message):
    db = connect()
    rows = db.execute("SELECT match_name,market,stake,odds,bookmaker,match_start_at FROM bets WHERE user_id=? AND result_status='pending' AND match_start_at IS NOT NULL ORDER BY match_start_at ASC LIMIT 20", (message.from_user.id,)).fetchall()
    db.close()
    future = []
    for match_name, market, stake, odds, bookmaker, match_start_at in rows:
        try:
            dt = datetime.fromisoformat(match_start_at)
            if dt.tzinfo is None: dt = dt.replace(tzinfo=TIMEZONE)
        except Exception:
            continue
        if dt >= now_dt(): future.append((match_name, market, stake, odds, bookmaker, dt))
    if not future:
        await message.answer("📭 Ближайших pending матчей нет.", reply_markup=stats_menu_kb()); return
    lines = ["📌 <b>Ближайшие матчи</b>\n"]
    for i, (match_name, market, stake, odds, bookmaker, dt) in enumerate(future[:10], 1):
        lines.append(f"{i}. <b>{match_name}</b>\n📌 {market}\n💸 {stake} | 📈 {odds} | 🏦 {bookmaker_label(bookmaker)}\n🕒 {dt.strftime('%d.%m.%Y %H:%M')} МСК\n")
    await message.answer("\n".join(lines), reply_markup=stats_menu_kb())


@dp.message(F.text == "📋 Логи")
async def logs_handler(message: Message):
    rows = get_recent_logs(10)
    if not rows:
        await message.answer("📭 Логов нет.", reply_markup=service_menu_kb()); return
    await message.answer("\n".join(["📋 <b>Последние логи</b>\n"] + [f"<b>{level}</b> | {created_at}\n{text}\n" for level, text, created_at in rows]), reply_markup=service_menu_kb())


# =========================
# UNIVERSAL TEXT / FORWARDED BET FLOW
# =========================
@dp.message(F.text == "🔁 Повторить ввод суммы")
async def retry_amount_handler(message: Message, state: FSMContext):
    if await state.get_state() != ShiftState.waiting_bet_amount.state:
        await message.answer("ℹ️ Сейчас нет активного ввода суммы.", reply_markup=bets_menu_kb()); return
    await message.answer("🔁 Напиши сумму заново. Пример: <code>1500</code>", reply_markup=amount_retry_kb())


@dp.message(F.text)
async def universal_text_handler(message: Message, state: FSMContext):
    if message.from_user.id != OWNER_ID:
        return
    text = (message.text or "").strip()
    current_state = await state.get_state()
    if current_state in {ShiftState.waiting_budget.state, ShiftState.waiting_end_shift_confirm.state, ShiftState.waiting_delete_last_confirm.state, ShiftState.waiting_result_bet_number.state, ShiftState.waiting_result_status.state, ShiftState.waiting_edit_stake_bet_number.state, ShiftState.waiting_edit_stake_value.state, ShiftState.waiting_shift_number.state, ShiftState.waiting_shift_bet_number.state, ShiftState.waiting_shift_bet_result_status.state, ShiftState.waiting_risk_decision.state}:
        return
    if current_state == ShiftState.waiting_bet_amount.state:
        data = await state.get_data(); pending = data.get("pending_bet")
        if not pending:
            await state.clear(); await message.answer("⚠️ Не нашёл ожидаемую ставку.", reply_markup=bets_menu_kb()); return
        try:
            amount = as_float(text)
            if amount <= 0: raise ValueError
        except Exception:
            await message.answer("⚠️ Сумма не распознана. Введи число, например: <code>1500</code>", reply_markup=amount_retry_kb()); return
        active = get_active_shift(message.from_user.id)
        if not active:
            await state.clear(); await message.answer("📭 Активной смены нет.", reply_markup=shift_menu_kb(False)); return
        shift_id, budget, spent, started_at = active
        try:
            add_bet_db(shift_id, message.from_user.id, now_str(), pending, amount)
        except Exception as e:
            if "UNIQUE constraint failed" in str(e):
                await state.clear(); await message.answer("⚠️ Эта ставка уже была добавлена ранее.", reply_markup=bets_menu_kb()); return
            log_error(f"Bet insert failed: {e}")
            await state.clear(); await message.answer(f"❌ Ошибка записи ставки: <code>{e}</code>", reply_markup=bets_menu_kb()); return
        new_spent = round(spent + amount, 2); remain = round(budget - new_spent, 2)
        warn = f"\n\n⚠️ <b>Выход за лимит</b> на <b>{round(new_spent-budget,2)}</b>" if new_spent > budget else ""
        await state.clear()
        await message.answer(f"✅ <b>Ставка сохранена</b>\n\n💸 Сумма: <b>{amount}</b>\n📊 Поставлено: <b>{new_spent}</b> / <b>{budget}</b>\n🟢 Остаток: <b>{remain}</b>{warn}", reply_markup=bets_menu_kb())
        return
    if not is_forward_message(message):
        return
    if not get_active_shift(message.from_user.id):
        await message.answer("⚠️ Сначала начни смену.", reply_markup=shift_menu_kb(False)); return
    parsed = parse_bet(text)
    if not parsed:
        await message.answer("⚠️ <b>Формат ставки не распознан</b>\n\nЯ не смог корректно разобрать сообщение.", reply_markup=bets_menu_kb())
        log_warning("Bet parse failed")
        return
    parsed = analyze_risk(parsed)
    await state.update_data(pending_bet=parsed)
    await state.set_state(ShiftState.waiting_risk_decision)
    match_start = datetime.fromisoformat(parsed["match_start_at"]).astimezone(TIMEZONE).strftime("%d.%m.%Y %H:%M")
    await message.answer(
        f"🎯 <b>Ставка распознана</b>\n\n"
        f"🏅 Спорт: <b>{parsed['sport']}</b>\n🏆 Турнир: <b>{parsed['tournament'] or '-'}</b>\n🏟 Матч: <b>{parsed['match_name']}</b>\n"
        f"📌 Маркет: {parsed['market']}\n📊 Группа: <b>{parsed.get('market_group')}</b>\n📈 КФ: <b>{parsed['odds']}</b>\n🧠 EV: <b>{parsed['ev'] if parsed['ev'] is not None else '-'}</b>\n🏦 {bookmaker_label(parsed['bookmaker'])}\n🕒 Старт: <b>{match_start} МСК</b>\n\n"
        f"<b>Проверка рисков:</b>\n{parsed['risk_notes']}\n\n"
        f"Сохранить ставку или отказаться?",
        reply_markup=risk_decision_kb()
    )


# =========================
# REMINDERS
# =========================
async def reminder_job():
    reminders = get_due_reminders()
    for item in reminders:
        dt_text = item["match_start_at"].astimezone(TIMEZONE).strftime("%d.%m.%Y %H:%M")
        try:
            await bot.send_message(item["user_id"], f"⏰ <b>Напоминание</b>\n\nЧерез {REMINDER_MINUTES} минут матч:\n🏟 <b>{item['match_name']}</b>\n📌 {item['market']}\n💸 {item['stake']}\n📈 {item['odds']}\n🏦 {bookmaker_label(item['bookmaker'])}\n🕒 {dt_text} МСК", reply_markup=bets_menu_kb())
            mark_reminder_sent(item["id"])
            log_info(f"Reminder sent | bet_id={item['id']}")
        except Exception as e:
            log_error(f"Reminder failed | bet_id={item['id']} | error={e}")


async def main():
    print("BOT STARTED")
    init_db()
    scheduler.add_job(reminder_job, "interval", seconds=30, max_instances=1, coalesce=True)
    scheduler.start()
    log_info("Bot started")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())

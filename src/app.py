"""Event Pipeline — entry point.

Collects events from Telegram, parses them, enriches from external API,
applies rules, notifies, and stores in SQLite.
"""
import argparse
import asyncio
import re
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import aiohttp
import os

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from aiohttp import web
from telethon import TelegramClient, events


# ============================================================
# CONFIG
# ============================================================
def _env(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return v if v and v.strip() else default


API_ID = int(_env("API_ID", "0"))
API_HASH = _env("API_HASH", "")
BOT_TOKEN = _env("BOT_TOKEN", "")
TARGET_CHAT_ID = int(_env("TARGET_CHAT_ID", "0"))
EXTERNAL_API_URL = _env("EXTERNAL_API_URL", "")
EXTERNAL_API_TOKEN = _env("EXTERNAL_API_TOKEN", "")
HTTP_PORT = int(_env("HTTP_PORT", "7860"))
DB_PATH = _env("DB_PATH", "/data/events.db")
DRY_RUN = _env("DRY_RUN", "false").lower() in ("1", "true", "yes")
IS_DRY_RUN = DRY_RUN or API_ID == 0 or not BOT_TOKEN


# ============================================================
# MODELS
# ============================================================
@dataclass
class RawEvent:
    source: str
    chat_id: int
    sender: str
    text: str
    timestamp: datetime = field(default_factory=datetime.utcnow)


@dataclass
class ParsedEvent:
    raw: RawEvent
    object_id: str
    event_type: str
    value: float


@dataclass
class RuleResult:
    matched: bool
    rule_name: str
    reason: str = ""


# ============================================================
# PARSER
# ============================================================
PATTERNS = {
    "object_id": re.compile(r"#([A-Z0-9_]+)"),
    "event_type": re.compile(r"[💬💌]\s*([A-Z0-9_]+\.[A-Za-z0-9_.()]+)"),
    "value": re.compile(r"(?:Value|Цена|Price)\s*[:]?\s*([\d.,]+)", re.IGNORECASE),
}


def parse(raw: RawEvent) -> ParsedEvent | None:
    text = raw.text
    object_id = PATTERNS["object_id"].search(text)
    event_type = PATTERNS["event_type"].search(text)
    value = PATTERNS["value"].search(text)
    if not (object_id and event_type and value):
        return None
    try:
        val = float(value.group(1).replace(",", "."))
    except ValueError:
        return None
    return ParsedEvent(raw, object_id.group(1), event_type.group(1), val)


# ============================================================
# ENRICHER
# ============================================================
async def enrich(event: ParsedEvent) -> dict[str, Any]:
    if IS_DRY_RUN or not EXTERNAL_API_URL:
        return {}
    headers = {"Accept": "application/json"}
    if EXTERNAL_API_TOKEN:
        headers["Authorization"] = f"Bearer {EXTERNAL_API_TOKEN}"
    params = {"object_id": event.object_id, "event_type": event.event_type}
    try:
        timeout = aiohttp.ClientTimeout(total=10)
        async with aiohttp.ClientSession(timeout=timeout) as s:
            async with s.get(EXTERNAL_API_URL, headers=headers, params=params) as r:
                return await r.json() if r.status == 200 else {}
    except Exception as e:
        print(f"enrich error: {e}")
        return {}


# ============================================================
# RULES
# ============================================================
def apply_rules(event: ParsedEvent, data: dict) -> RuleResult:
    if event.value > 0:
        return RuleResult(True, "value > 0", f"value={event.value}")
    return RuleResult(False, "value > 0", f"value={event.value}")


# ============================================================
# STORAGE
# ============================================================
def init_db() -> None:
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT, source TEXT, sender TEXT,
            object_id TEXT, event_type TEXT, value REAL,
            data TEXT, rule_name TEXT, rule_matched INTEGER, reason TEXT
        );
    """)
    conn.commit()
    conn.close()


def save_event(event: ParsedEvent, result: RuleResult) -> None:
    import json
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        "INSERT INTO events (timestamp, source, sender, object_id, event_type, value, data, rule_name, rule_matched, reason) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (event.raw.timestamp.isoformat(), event.raw.source, event.raw.sender,
         event.object_id, event.event_type, event.value, "{}",
         result.rule_name, 1 if result.matched else 0, result.reason),
    )
    conn.commit()
    conn.close()


# ============================================================
# PIPELINE
# ============================================================
async def handle_event(raw: RawEvent, bot_client=None) -> None:
    print(f"event: {raw.sender} | {raw.text[:80]}")
    parsed = parse(raw)
    if not parsed:
        print("parse skipped")
        return
    data = await enrich(parsed)
    result = apply_rules(parsed, data)
    if result.matched and bot_client and not IS_DRY_RUN:
        try:
            await bot_client.send_message(
                TARGET_CHAT_ID,
                f"🔔 {parsed.object_id}\n{parsed.event_type}\nValue: {parsed.value}",
            )
        except Exception as e:
            print(f"notify error: {e}")
    save_event(parsed, result)


# ============================================================
# HTTP + MAIN
# ============================================================
async def health(_):
    return web.Response(text="OK")


async def run() -> None:
    init_db()
    print(f"started (dry_run={IS_DRY_RUN})")

    if IS_DRY_RUN:
        fake = RawEvent("dry", 0, "system",
                        "#OBJECT_1\nTYPE_A.PHASE1\nValue: 100.5")
        await handle_event(fake)
        return

    user = TelegramClient("user_session", API_ID, API_HASH)
    bot = TelegramClient("bot_session", API_ID, API_HASH)
    await user.start()
    await bot.start(bot_token=BOT_TOKEN)

    @user.on(events.NewMessage(incoming=True))
    async def on_msg(ev):
        if not ev.is_private:
            return
        sender = await ev.get_sender()
        raw = RawEvent("telegram", ev.chat_id,
                       getattr(sender, "username", "") or "",
                       ev.message.text or "")
        await handle_event(raw, bot)

    app = web.Application()
    app.router.add_get("/", health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", HTTP_PORT)
    await site.start()

    print(f"HTTP on :{HTTP_PORT}")
    await user.run_until_disconnected()


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        print("stopped")


if __name__ == "__main__":
    main()

"""
config.py — Shared singletons: logger, DB collections, bot client, constants.
Import karo baaki files mein — kabhi yahan se initialize mat karo dobara.
"""

import os
import logging
import logging.handlers
from motor.motor_asyncio import AsyncIOMotorClient
from pyrogram import Client
from dotenv import load_dotenv

load_dotenv()

# ─── Logging ─────────────────────────────────────────────────────────────────

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

_formatter = logging.Formatter(
    fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

logger = logging.getLogger("media_bot")
logger.setLevel(LOG_LEVEL)

_console = logging.StreamHandler()
_console.setFormatter(_formatter)
logger.addHandler(_console)

os.makedirs("logs", exist_ok=True)
_file = logging.handlers.RotatingFileHandler(
    "logs/bot.log", maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8"
)
_file.setFormatter(_formatter)
logger.addHandler(_file)

logging.getLogger("pyrogram").setLevel(logging.WARNING)
logging.getLogger("motor").setLevel(logging.WARNING)

logger.info("Logger initialised — level=%s", LOG_LEVEL)

# ─── MongoDB ──────────────────────────────────────────────────────────────────

_mongo = AsyncIOMotorClient(os.getenv("MONGO_URI"))
_db    = _mongo["media_manager_bot"]

channels_col = _db["channels"]    # { channel_id, file_count, order }
files_col    = _db["files"]       # { file_number, files:[{channel_id, message_id}] }
batches_col  = _db["batches"]     # { batch_id, file_groups:[[…],…] }
counter_col  = _db["counter"]     # { _id:"file_counter", value:N }
admins_col   = _db["admins"]      # { user_id, added_by, added_at }
dupes_col    = _db["duplicates"]  # { file_unique_id, file_number, saved_at }

logger.info("MongoDB collections bound.")

# ─── Pyrogram client ─────────────────────────────────────────────────────────

app = Client(
    "media-manager-bot",
    api_id=int(os.getenv("API_ID")),
    api_hash=os.getenv("API_HASH"),
    bot_token=os.getenv("BOT_TOKEN"),
)

# ─── Constants ────────────────────────────────────────────────────────────────

OWNER_ID      = int(os.getenv("OWNER_ID"))
REDIRECT_URL  = os.getenv("REDIRECT_URL")
POST_CHANNEL  = int(os.getenv("POST_CHANNEL"))
POSTER_URL    = os.getenv("POSTER_URL")
CHANNEL_LIMIT = 1000

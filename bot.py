import os
import uuid
import asyncio
import logging
import logging.handlers
from datetime import datetime, timezone
from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from pyrogram.errors import FloodWait, MessageDeleteForbidden, MessageIdInvalid
from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient

load_dotenv()

# ─── Logging Setup ───────────────────────────────────────────────────────────

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

log_formatter = logging.Formatter(
    fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# Root logger
logger = logging.getLogger("media_bot")
logger.setLevel(LOG_LEVEL)

# Console handler
console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)
logger.addHandler(console_handler)

# Rotating file handler (5 MB × 3 backups)
os.makedirs("logs", exist_ok=True)
file_handler = logging.handlers.RotatingFileHandler(
    "logs/bot.log", maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8"
)
file_handler.setFormatter(log_formatter)
logger.addHandler(file_handler)

# Suppress noisy third-party loggers
logging.getLogger("pyrogram").setLevel(logging.WARNING)
logging.getLogger("motor").setLevel(logging.WARNING)

logger.info("Logger initialised — level=%s", LOG_LEVEL)

# ─── Database ────────────────────────────────────────────────────────────────

mongo = AsyncIOMotorClient(os.getenv("MONGO_URI"))
db = mongo["media_manager_bot"]

channels_col = db["channels"]   # { channel_id, file_count }
files_col    = db["files"]      # { file_number, files:[{channel_id, message_id}] }
batches_col  = db["batches"]    # { batch_id, file_groups:[[{channel_id,message_id}],...] }
counter_col  = db["counter"]    # { _id:"file_counter", value:N }
admins_col   = db["admins"]     # { user_id, added_by, added_at }

logger.info("MongoDB collections bound.")

# ─── Bot ─────────────────────────────────────────────────────────────────────

app = Client(
    "media-manager-bot",
    api_id=int(os.getenv("API_ID")),
    api_hash=os.getenv("API_HASH"),
    bot_token=os.getenv("BOT_TOKEN")
)

OWNER_ID      = int(os.getenv("OWNER_ID"))
REDIRECT_URL  = os.getenv("REDIRECT_URL")
POST_CHANNEL  = int(os.getenv("POST_CHANNEL"))
POSTER_URL    = os.getenv("POSTER_URL")
CHANNEL_LIMIT = 1000

# ─── In-memory state ─────────────────────────────────────────────────────────

user_batches:    dict[int, list]           = {}
user_queues:     dict[int, asyncio.Queue]  = {}
queue_tasks:     dict[int, asyncio.Task]   = {}
pending_retries: dict[str, object]         = {}

# ─── Admin Helpers ───────────────────────────────────────────────────────────

async def is_admin(user_id: int) -> bool:
    if user_id == OWNER_ID:
        return True
    doc = await admins_col.find_one({"user_id": user_id})
    return doc is not None


async def get_all_admins() -> list[dict]:
    result = []
    async for doc in admins_col.find({}):
        result.append(doc)
    return result

# ─── DB Helpers ──────────────────────────────────────────────────────────────

async def get_next_file_number() -> int:
    data = await counter_col.find_one({"_id": "file_counter"})
    if not data:
        await counter_col.insert_one({"_id": "file_counter", "value": 1})
        logger.debug("File counter initialised at 1.")
        return 1
    new_val = data["value"] + 1
    await counter_col.update_one({"_id": "file_counter"}, {"$set": {"value": new_val}})
    logger.debug("File counter incremented → %d", new_val)
    return new_val


async def get_available_channels() -> list[dict]:
    result = []
    async for ch in channels_col.find({}):
        if ch.get("file_count", 0) < CHANNEL_LIMIT:
            result.append(ch)
    logger.debug("Available channels: %d", len(result))
    return result


async def increment_channel_count(channel_id: int):
    await channels_col.update_one(
        {"channel_id": channel_id},
        {"$inc": {"file_count": 1}},
        upsert=True
    )

# ─── Core: process one message ───────────────────────────────────────────────

async def process_single_message(message: Message):
    uid = message.from_user.id
    logger.info("[USER:%d] Processing message_id=%d", uid, message.id)

    channels = await get_available_channels()

    if not channels:
        logger.error("[USER:%d] No available channels — all full.", uid)
        raise RuntimeError("All channels are full (≥1000 files). Add a new channel first.")

    file_number = await get_next_file_number()
    logger.info("[USER:%d] Assigned file_number=%d", uid, file_number)

    ts  = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")
    ext = ""
    if message.document and message.document.file_name:
        ext = "." + message.document.file_name.rsplit(".", 1)[-1].upper()
    elif message.video:
        ext = ".MP4"
    elif message.audio:
        ext = ".MP3"
    elif message.photo:
        ext = ".JPG"
    elif message.voice:
        ext = ".OGG"
    elif message.video_note:
        ext = ".MP4"
    caption = f"[@atoz_links] {ts}{ext}"

    saved_files = []

    for ch in channels:
        channel_id = ch["channel_id"]
        logger.info("[USER:%d] Copying file_number=%d → channel=%d", uid, file_number, channel_id)
        while True:
            try:
                copied = await message.copy(chat_id=channel_id, caption=caption)
                saved_files.append({"channel_id": channel_id, "message_id": copied.id})
                await increment_channel_count(channel_id)
                logger.info(
                    "[USER:%d] Copied → channel=%d message_id=%d",
                    uid, channel_id, copied.id
                )
                break
            except FloodWait as e:
                logger.warning(
                    "[USER:%d] FloodWait %ds on channel=%d — sleeping.",
                    uid, e.value, channel_id
                )
                await asyncio.sleep(e.value)
            except Exception as e:
                logger.error(
                    "[USER:%d] Copy failed to channel=%d: %s",
                    uid, channel_id, e, exc_info=True
                )
                raise RuntimeError(f"Failed to copy to channel `{channel_id}`:\n`{e}`")

        await asyncio.sleep(5)

    await files_col.insert_one({
        "file_number": file_number,
        "files": saved_files
    })
    logger.info("[USER:%d] file_number=%d saved to DB with %d copies.", uid, file_number, len(saved_files))

    user_batches.setdefault(uid, []).append(saved_files)

    try:
        await message.delete()
    except Exception as e:
        logger.warning("[USER:%d] Could not delete original message: %s", uid, e)

    if len(user_batches[uid]) >= 10:
        logger.info("[USER:%d] Batch threshold reached — creating batch.", uid)
        await create_batch(uid)


async def create_batch(uid: int):
    batch_id   = str(uuid.uuid4())[:8]
    batch_link = f"{REDIRECT_URL}{batch_id}"

    logger.info("[USER:%d] Creating batch_id=%s (%d file groups).",
                uid, batch_id, len(user_batches[uid]))

    await batches_col.insert_one({
        "batch_id":    batch_id,
        "file_groups": user_batches[uid]
    })
    user_batches[uid] = []

    try:
        parts     = POSTER_URL.rstrip("/").split("/")
        p_chat_id = int("-100" + parts[-2])
        p_msg_id  = int(parts[-1])
        await app.copy_message(
            chat_id=POST_CHANNEL,
            from_chat_id=p_chat_id,
            message_id=p_msg_id,
            caption=batch_link
        )
        logger.info("[USER:%d] Batch posted to POST_CHANNEL — link=%s", uid, batch_link)
    except Exception as e:
        logger.error("[USER:%d] Poster post failed: %s", uid, e, exc_info=True)
        await app.send_message(uid, f"⚠️ Poster post failed:\n`{e}`\n\nBatch link: `{batch_link}`")

# ─── Queue Worker ─────────────────────────────────────────────────────────────

async def queue_worker(uid: int):
    logger.info("[USER:%d] Queue worker started.", uid)
    queue = user_queues[uid]
    try:
        while True:
            try:
                message = queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            await run_with_retry_ui(message, uid)
            queue.task_done()
    finally:
        logger.info("[USER:%d] Queue worker finished.", uid)
        queue_tasks.pop(uid, None)
        if uid in user_queues and user_queues[uid].empty():
            user_queues.pop(uid, None)


async def run_with_retry_ui(message: Message, uid: int):
    attempt = 0
    while True:
        attempt += 1
        logger.info("[USER:%d] Processing message_id=%d (attempt #%d).", uid, message.id, attempt)
        try:
            await process_single_message(message)
            return
        except Exception as e:
            logger.warning(
                "[USER:%d] Error on attempt #%d for message_id=%d: %s",
                uid, attempt, message.id, e
            )
            retry_key = f"retry_{uuid.uuid4().hex[:8]}"
            skip_key  = f"skip_{uuid.uuid4().hex[:8]}"

            pending_retries[retry_key] = message
            pending_retries[skip_key]  = message

            keyboard = InlineKeyboardMarkup([[
                InlineKeyboardButton("♻️ Retry", callback_data=retry_key),
                InlineKeyboardButton("🚫 Skip",  callback_data=skip_key)
            ]])

            await app.send_message(
                uid,
                f"❌ Error processing file:\n{e}\n\nWhat do you want to do?",
                reply_markup=keyboard
            )

            decision = await wait_for_decision(retry_key, skip_key)
            logger.info("[USER:%d] Decision for message_id=%d: %s", uid, message.id, decision)

            if decision == "retry":
                continue
            else:
                return


async def wait_for_decision(retry_key: str, skip_key: str) -> str:
    event           = asyncio.Event()
    decision_holder = {"value": "skip"}

    pending_retries[retry_key + "_event"] = (event, decision_holder, "retry")
    pending_retries[skip_key  + "_event"] = (event, decision_holder, "skip")

    await event.wait()

    for k in [retry_key, skip_key, retry_key + "_event", skip_key + "_event"]:
        pending_retries.pop(k, None)

    return decision_holder["value"]

# ─── Callback: Retry / Skip ───────────────────────────────────────────────────

@app.on_callback_query()
async def handle_callback(_, query: CallbackQuery):
    uid = query.from_user.id
    if not await is_admin(uid):
        logger.warning("[USER:%d] Unauthorized callback attempt: %s", uid, query.data)
        return await query.answer("Unauthorized.", show_alert=True)

    event_key = query.data + "_event"
    if event_key not in pending_retries:
        logger.warning("[USER:%d] Callback already handled: %s", uid, query.data)
        return await query.answer("Already handled.", show_alert=True)

    event, decision_holder, choice = pending_retries[event_key]
    decision_holder["value"] = choice
    logger.info("[USER:%d] Callback resolved — choice=%s key=%s", uid, choice, query.data)

    try:
        await query.message.delete()
    except Exception:
        pass

    label = "♻️ Retrying..." if choice == "retry" else "🚫 Skipped"
    await query.answer(label)
    event.set()

# ─── Admin Commands (Owner only) ─────────────────────────────────────────────

@app.on_message(filters.command("add_admin"))
async def add_admin(_, message: Message):
    uid = message.from_user.id
    if uid != OWNER_ID:
        logger.warning("[USER:%d] Unauthorized /add_admin attempt.", uid)
        return await message.reply_text("Only owner can add admins.")

    try:
        target_id = int(message.command[1])
    except (IndexError, ValueError):
        return await message.reply_text(
            "Usage: /add_admin <user_id>\nExample: /add_admin 123456789"
        )

    if target_id == OWNER_ID:
        return await message.reply_text("Owner is already the master admin.")

    if await admins_col.find_one({"user_id": target_id}):
        return await message.reply_text(f"User `{target_id}` is already an admin.")

    await admins_col.insert_one({
        "user_id":  target_id,
        "added_by": OWNER_ID,
        "added_at": datetime.now(timezone.utc).isoformat()
    })
    logger.info("[OWNER] Admin added: user_id=%d", target_id)
    await message.reply_text(f"✅ Admin Added:\n`{target_id}`")

    try:
        await app.send_message(target_id, "✅ You have been added as an admin.")
    except Exception as e:
        logger.warning("Could not notify new admin %d: %s", target_id, e)


@app.on_message(filters.command("remove_admin"))
async def remove_admin(_, message: Message):
    uid = message.from_user.id
    if uid != OWNER_ID:
        logger.warning("[USER:%d] Unauthorized /remove_admin attempt.", uid)
        return await message.reply_text("Only owner can remove admins.")

    try:
        target_id = int(message.command[1])
    except (IndexError, ValueError):
        return await message.reply_text(
            "Usage: /remove_admin <user_id>\nExample: /remove_admin 123456789"
        )

    if target_id == OWNER_ID:
        return await message.reply_text("Cannot remove the owner.")

    result = await admins_col.delete_one({"user_id": target_id})

    if result.deleted_count == 0:
        return await message.reply_text(f"User `{target_id}` is not an admin.")

    logger.info("[OWNER] Admin removed: user_id=%d", target_id)
    await message.reply_text(f"❌ Admin Removed:\n`{target_id}`")

    try:
        await app.send_message(target_id, "❌ Your admin access has been revoked.")
    except Exception as e:
        logger.warning("Could not notify removed admin %d: %s", target_id, e)


@app.on_message(filters.command("admins"))
async def list_admins(_, message: Message):
    uid = message.from_user.id
    if uid != OWNER_ID:
        logger.warning("[USER:%d] Unauthorized /admins attempt.", uid)
        return await message.reply_text("Only owner can view admins.")

    admin_list = await get_all_admins()
    lines = [f"👑 **Owner:** `{OWNER_ID}`\n"]

    if not admin_list:
        lines.append("No extra admins added yet.")
    else:
        lines.append(f"🛡 **Admins ({len(admin_list)}):**")
        for i, a in enumerate(admin_list, 1):
            added = a.get("added_at", "unknown")[:10]
            lines.append(f"{i}. `{a['user_id']}` — added {added}")

    await message.reply_text("\n".join(lines))

# ─── Channel Commands ─────────────────────────────────────────────────────────

@app.on_message(filters.command("add_channel"))
async def add_channel(_, message: Message):
    uid = message.from_user.id
    if not await is_admin(uid):
        return

    try:
        channel_id = int(message.command[1])
    except (IndexError, ValueError):
        return await message.reply_text("Usage:\n/add_channel -100xxxxxxxxxx")

    if await channels_col.find_one({"channel_id": channel_id}):
        return await message.reply_text("Channel already added.")

    await channels_col.insert_one({"channel_id": channel_id, "file_count": 0})
    logger.info("[USER:%d] Channel added: %d", uid, channel_id)
    await message.reply_text(f"Channel Added:\n`{channel_id}`")


@app.on_message(filters.command("remove_channel"))
async def remove_channel(_, message: Message):
    uid = message.from_user.id
    if not await is_admin(uid):
        return

    try:
        channel_id = int(message.command[1])
    except (IndexError, ValueError):
        return await message.reply_text("Usage:\n/remove_channel -100xxxxxxxxxx")

    await channels_col.delete_one({"channel_id": channel_id})
    logger.info("[USER:%d] Channel removed: %d", uid, channel_id)
    await message.reply_text("Channel Removed.")


@app.on_message(filters.command("channels"))
async def list_channels(_, message: Message):
    if not await is_admin(message.from_user.id):
        return

    channels = [ch async for ch in channels_col.find({})]

    if not channels:
        return await message.reply_text("No channels added yet.")

    lines = ["📋 **Target Channels**\n"]
    for i, ch in enumerate(channels, 1):
        cid    = ch["channel_id"]
        count  = ch.get("file_count", 0)
        status = "🔴 FULL" if count >= CHANNEL_LIMIT else f"🟢 {count}/{CHANNEL_LIMIT}"
        lines.append(f"{i}. `{cid}` — {status}")

    await message.reply_text("\n".join(lines))

# ─── Media Handler ───────────────────────────────────────────────────────────

@app.on_message(filters.media & filters.private)
async def save_media(_, message: Message):
    uid = message.from_user.id

    if not await is_admin(uid):
        logger.debug("[USER:%d] Non-admin media ignored (message_id=%d).", uid, message.id)
        return

    logger.info("[USER:%d] Media received → queued (message_id=%d).", uid, message.id)
    user_queues.setdefault(uid, asyncio.Queue())
    await user_queues[uid].put(message)

    if uid not in queue_tasks or queue_tasks[uid].done():
        queue_tasks[uid] = asyncio.get_event_loop().create_task(
            queue_worker(uid)
        )

# ─── Run ─────────────────────────────────────────────────────────────────────

from health_check import start_health_server

logger.info("Starting health-check server...")
start_health_server()

logger.info("Starting bot...")
app.run()

"""
bot.py — Main entry point.
Imports config, channel_handlers, health_check.
Handles: duplicate detection, media queue, admin/batch logic, startup.
"""

import asyncio
import uuid
from datetime import datetime, timezone

from pyrogram import filters
from pyrogram.errors import FloodWait
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery

from config import (
    app, logger,
    OWNER_ID, REDIRECT_URL, POST_CHANNEL, POSTER_URL,
    channels_col, files_col, batches_col, counter_col, admins_col, dupes_col,
)
import channel_handlers  # registers /add_channel /remove_channel /channels handlers

# ─── In-memory state ─────────────────────────────────────────────────────────

user_batches:    dict[int, list]          = {}
user_queues:     dict[int, asyncio.Queue] = {}
queue_tasks:     dict[int, asyncio.Task]  = {}
pending_retries: dict[str, object]        = {}

# ─── Admin helpers ────────────────────────────────────────────────────────────

async def is_admin(user_id: int) -> bool:
    if user_id == OWNER_ID:
        return True
    return await admins_col.find_one({"user_id": user_id}) is not None


async def get_all_admins() -> list[dict]:
    return [doc async for doc in admins_col.find({})]

# ─── DB helpers ───────────────────────────────────────────────────────────────

async def get_next_file_number() -> int:
    data = await counter_col.find_one({"_id": "file_counter"})
    if not data:
        await counter_col.insert_one({"_id": "file_counter", "value": 1})
        return 1
    new_val = data["value"] + 1
    await counter_col.update_one({"_id": "file_counter"}, {"$set": {"value": new_val}})
    return new_val

# ─── Duplicate detection ──────────────────────────────────────────────────────

def get_file_unique_id(message: Message) -> str | None:
    for attr in ("document", "video", "audio", "photo", "voice", "video_note", "sticker", "animation"):
        media = getattr(message, attr, None)
        if media:
            return getattr(media, "file_unique_id", None)
    return None


async def is_duplicate(fuid: str) -> tuple[bool, int | None]:
    doc = await dupes_col.find_one({"file_unique_id": fuid})
    if doc:
        return True, doc["file_number"]
    return False, None


async def register_file_unique_id(fuid: str, file_number: int):
    try:
        await dupes_col.insert_one({
            "file_unique_id": fuid,
            "file_number":    file_number,
            "saved_at":       datetime.now(timezone.utc).isoformat()
        })
    except Exception:
        pass  # duplicate key = already registered, safe to ignore

# ─── Core: process one message ───────────────────────────────────────────────

async def process_single_message(message: Message):
    uid = message.from_user.id
    logger.info("[USER:%d] Processing message_id=%d", uid, message.id)

    # ── Duplicate check ───────────────────────────────────────────────────────
    fuid = get_file_unique_id(message)
    if fuid:
        dupe, existing_num = await is_duplicate(fuid)
        if dupe:
            logger.warning(
                "[USER:%d] Duplicate file_unique_id=%s (already file_number=%s) — deleting.",
                uid, fuid, existing_num
            )
            try:
                await message.delete()
            except Exception:
                pass
            await app.send_message(
                uid,
                f"\u26a0\ufe0f **Duplicate File Detected!**\n\n"
                f"Yeh file pehle se save hai (file_number: `{existing_num}`).\n"
                f"Message delete kar diya gaya. \u2705"
            )
            return

    # ── Get single target channel (sequential fill) ───────────────────────────
    ch = await channel_handlers.get_target_channel()
    if not ch:
        logger.error("[USER:%d] No available channels — all full or none added.", uid)
        raise RuntimeError(
            "Koi bhi channel available nahi hai.\n"
            "Ya sab full hain (≥1000) ya koi add hi nahi kiya.\n"
            "/add_channel se naya channel add karo."
        )

    channel_id  = ch["channel_id"]
    file_number = await get_next_file_number()
    logger.info("[USER:%d] file_number=%d → channel=%d", uid, file_number, channel_id)

    # ── Build caption ──────────────────────────────────────────────────────────
    ts  = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")
    ext = ""
    if message.document and message.document.file_name:
        ext = "." + message.document.file_name.rsplit(".", 1)[-1].upper()
    elif message.video:      ext = ".MP4"
    elif message.audio:      ext = ".MP3"
    elif message.photo:      ext = ".JPG"
    elif message.voice:      ext = ".OGG"
    elif message.video_note: ext = ".MP4"
    caption = f"[@atoz_links] {ts}{ext}"

    # ── Copy to channel (with FloodWait retry) ────────────────────────────────
    try:
        await app.get_chat(channel_id)
    except Exception as e:
        logger.error("[USER:%d] Cannot resolve channel=%d: %s", uid, channel_id, e)
        raise RuntimeError(
            f"Channel `{channel_id}` resolve nahi hua.\n"
            f"Bot ko us channel mein **admin** banao.\n`{e}`"
        )

    while True:
        try:
            copied = await message.copy(chat_id=channel_id, caption=caption)
            break
        except FloodWait as e:
            logger.warning("[USER:%d] FloodWait %ds on channel=%d", uid, e.value, channel_id)
            await asyncio.sleep(e.value)
        except Exception as e:
            logger.error("[USER:%d] Copy failed to channel=%d: %s", uid, channel_id, e, exc_info=True)
            raise RuntimeError(f"Copy failed to channel `{channel_id}`:\n`{e}`")

    await channel_handlers.increment_channel_count(channel_id)
    logger.info("[USER:%d] Copied → channel=%d msg=%d", uid, channel_id, copied.id)

    # ── Save to DB ────────────────────────────────────────────────────────────
    saved_entry = [{"channel_id": channel_id, "message_id": copied.id}]
    await files_col.insert_one({"file_number": file_number, "files": saved_entry})
    logger.info("[USER:%d] file_number=%d saved to DB.", uid, file_number)

    if fuid:
        await register_file_unique_id(fuid, file_number)

    # ── Delete original ───────────────────────────────────────────────────────
    try:
        await message.delete()
    except Exception as e:
        logger.warning("[USER:%d] Could not delete original: %s", uid, e)

    # ── Batch logic ───────────────────────────────────────────────────────────
    user_batches.setdefault(uid, []).append(saved_entry)
    if len(user_batches[uid]) >= 10:
        logger.info("[USER:%d] Batch threshold reached.", uid)
        await create_batch(uid)

# ─── Batch creation ───────────────────────────────────────────────────────────

async def create_batch(uid: int):
    batch_id   = str(uuid.uuid4())[:8]
    batch_link = f"{REDIRECT_URL}{batch_id}"

    logger.info("[USER:%d] Creating batch_id=%s (%d groups).",
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
        await app.get_chat(p_chat_id)
        await app.get_chat(POST_CHANNEL)
        await app.copy_message(
            chat_id=POST_CHANNEL,
            from_chat_id=p_chat_id,
            message_id=p_msg_id,
            caption=batch_link
        )
        logger.info("[USER:%d] Batch posted — link=%s", uid, batch_link)
    except Exception as e:
        logger.error("[USER:%d] Poster post failed: %s", uid, e, exc_info=True)
        await app.send_message(
            uid,
            f"\u26a0\ufe0f Poster post failed:\n`{e}`\n\nBatch link: `{batch_link}`"
        )

# ─── Queue worker ─────────────────────────────────────────────────────────────

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
        try:
            await process_single_message(message)
            return
        except Exception as e:
            logger.warning("[USER:%d] Error attempt #%d msg=%d: %s",
                           uid, attempt, message.id, e)
            retry_key = f"retry_{uuid.uuid4().hex[:8]}"
            skip_key  = f"skip_{uuid.uuid4().hex[:8]}"
            pending_retries[retry_key] = message
            pending_retries[skip_key]  = message

            kb = InlineKeyboardMarkup([[
                InlineKeyboardButton("\u267b\ufe0f Retry", callback_data=retry_key),
                InlineKeyboardButton("\U0001f6ab Skip",   callback_data=skip_key),
            ]])
            await app.send_message(
                uid,
                f"\u274c Error processing file:\n{e}\n\nKya karna hai?",
                reply_markup=kb
            )
            decision = await _wait_for_decision(retry_key, skip_key)
            logger.info("[USER:%d] Decision msg=%d: %s", uid, message.id, decision)
            if decision == "retry":
                continue
            return


async def _wait_for_decision(retry_key: str, skip_key: str) -> str:
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
    if not await is_admin(query.from_user.id):
        return await query.answer("Unauthorized.", show_alert=True)

    event_key = query.data + "_event"
    if event_key not in pending_retries:
        return await query.answer("Already handled.", show_alert=True)

    event, decision_holder, choice = pending_retries[event_key]
    decision_holder["value"] = choice
    try:
        await query.message.delete()
    except Exception:
        pass
    await query.answer("\u267b\ufe0f Retrying..." if choice == "retry" else "\U0001f6ab Skipped")
    event.set()

# ─── /start ───────────────────────────────────────────────────────────────────

@app.on_message(filters.command("start") & filters.private)
async def cmd_start(_, message: Message):
    uid   = message.from_user.id
    admin = await is_admin(uid)

    if admin:
        text = (
            "\U0001f44b **Media Manager Bot**\n\n"
            "\U0001f4c1 Media bhejo — bot automatically save karta hai!\n\n"
            "\u2501" * 24 + "\n"
            "\U0001f6e1 **Admin Commands**\n"
            "/channels \u2014 Channels list + status\n"
            "/add_channel `-100xxx` \u2014 Channel add karo\n"
            "/remove_channel `-100xxx` \u2014 Channel remove karo\n\n"
            "\u2501" * 24 + "\n"
            "\U0001f451 **Owner Only**\n"
            "/admins \u2014 Admins list\n"
            "/add_admin `user_id` \u2014 Admin add karo\n"
            "/remove_admin `user_id` \u2014 Admin remove karo\n\n"
            "\u2501" * 24 + "\n"
            "\U0001f4cc **How it works**\n"
            "\u2022 Files ek channel mein jaati hain jab tak 1000 na ho jaaye\n"
            "\u2022 1000 hone ke baad agla channel automatically use hota hai\n"
            "\u2022 Har 10 files pe batch link ban jaata hai\n"
            "\u2022 Duplicate files automatically delete hoti hain\n"
        )
    else:
        text = (
            "\U0001f44b **Media Manager Bot**\n\n"
            "Yeh bot sirf authorized admins ke liye hai.\n"
            "Access ke liye owner se contact karo."
        )
    await message.reply_text(text)

# ─── Admin commands (Owner only) ─────────────────────────────────────────────

@app.on_message(filters.command("add_admin") & filters.private)
async def cmd_add_admin(_, message: Message):
    uid = message.from_user.id
    if uid != OWNER_ID:
        return await message.reply_text("Only owner can add admins.")
    try:
        target_id = int(message.command[1])
    except (IndexError, ValueError):
        return await message.reply_text("Usage: /add_admin `user_id`")
    if target_id == OWNER_ID:
        return await message.reply_text("Owner is already master admin.")
    if await admins_col.find_one({"user_id": target_id}):
        return await message.reply_text(f"User `{target_id}` is already an admin.")
    await admins_col.insert_one({
        "user_id":  target_id,
        "added_by": OWNER_ID,
        "added_at": datetime.now(timezone.utc).isoformat()
    })
    logger.info("[OWNER] Admin added: %d", target_id)
    await message.reply_text(f"\u2705 Admin Added:\n`{target_id}`")
    try:
        await app.send_message(target_id, "\u2705 You have been added as an admin.")
    except Exception:
        pass


@app.on_message(filters.command("remove_admin") & filters.private)
async def cmd_remove_admin(_, message: Message):
    uid = message.from_user.id
    if uid != OWNER_ID:
        return await message.reply_text("Only owner can remove admins.")
    try:
        target_id = int(message.command[1])
    except (IndexError, ValueError):
        return await message.reply_text("Usage: /remove_admin `user_id`")
    if target_id == OWNER_ID:
        return await message.reply_text("Cannot remove the owner.")
    result = await admins_col.delete_one({"user_id": target_id})
    if result.deleted_count == 0:
        return await message.reply_text(f"User `{target_id}` is not an admin.")
    logger.info("[OWNER] Admin removed: %d", target_id)
    await message.reply_text(f"\u274c Admin Removed:\n`{target_id}`")
    try:
        await app.send_message(target_id, "\u274c Your admin access has been revoked.")
    except Exception:
        pass


@app.on_message(filters.command("admins") & filters.private)
async def cmd_list_admins(_, message: Message):
    if message.from_user.id != OWNER_ID:
        return await message.reply_text("Only owner can view admins.")
    admin_list = await get_all_admins()
    lines = [f"\U0001f451 **Owner:** `{OWNER_ID}`\n"]
    if not admin_list:
        lines.append("No extra admins added yet.")
    else:
        lines.append(f"\U0001f6e1 **Admins ({len(admin_list)}):**")
        for i, a in enumerate(admin_list, 1):
            added = a.get("added_at", "unknown")[:10]
            lines.append(f"{i}. `{a['user_id']}` \u2014 added {added}")
    await message.reply_text("\n".join(lines))

# ─── Media handler ────────────────────────────────────────────────────────────

@app.on_message(filters.media & filters.private)
async def save_media(_, message: Message):
    uid = message.from_user.id
    if not await is_admin(uid):
        return
    logger.info("[USER:%d] Media queued (message_id=%d).", uid, message.id)
    user_queues.setdefault(uid, asyncio.Queue())
    await user_queues[uid].put(message)
    if uid not in queue_tasks or queue_tasks[uid].done():
        queue_tasks[uid] = asyncio.get_event_loop().create_task(queue_worker(uid))

# ─── Startup ──────────────────────────────────────────────────────────────────

from health_check import start_health_server


async def warmup():
    """Ensure indexes + warm Pyrogram peer cache for all registered channels."""
    await dupes_col.create_index("file_unique_id", unique=True, background=True)
    logger.info("MongoDB index ensured on duplicates.file_unique_id")

    logger.info("Warming up peer cache...")
    all_channels = [ch async for ch in channels_col.find({})]
    for ch in all_channels:
        cid = ch["channel_id"]
        try:
            await app.get_chat(cid)
            logger.info("  \u2713 channel=%d resolved", cid)
        except Exception as e:
            logger.warning("  \u2717 channel=%d FAILED: %s", cid, e)

    for label, cid_expr in [("POST_CHANNEL", lambda: POST_CHANNEL),
                             ("POSTER source", lambda: int("-100" + POSTER_URL.rstrip("/").split("/")[-2]))]:
        try:
            cid = cid_expr()
            await app.get_chat(cid)
            logger.info("  \u2713 %s=%d resolved", label, cid)
        except Exception as e:
            logger.warning("  \u2717 %s FAILED: %s", label, e)

    logger.info("Warmup complete.")


async def main():
    async with app:
        logger.info("Bot connected to Telegram.")
        await warmup()
        logger.info("Bot is running.")
        await asyncio.get_event_loop().create_future()  # run forever


logger.info("Starting health-check server...")
start_health_server()

logger.info("Starting bot...")
app.run(main())

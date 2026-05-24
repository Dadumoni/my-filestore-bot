"""
channel_handlers.py
───────────────────
Channel management — add/remove/list commands  +  core file-routing logic.

KEY FIX — Sequential routing (not broadcast):
  • Files jaate hain SIRF ek channel mein (jo current active channel hai).
  • Jab wo channel 1000 files se bhar jaaye TAB hi agla channel use hota hai.
  • /remove_channel ke baad active_channel_id in-memory cache turant clear
    hota hai — removed channel ko koi aur file nahi jaati.
"""

import asyncio
import logging
from datetime import datetime, timezone

from pyrogram import filters
from pyrogram.errors import FloodWait
from pyrogram.types import Message

from config import (
    app, OWNER_ID, CHANNEL_LIMIT,
    channels_col, logger,
)

# ─── In-memory active-channel cache ──────────────────────────────────────────
# Stores the channel_id that is currently being filled.
# None means "not yet resolved — fetch from DB on next use".
# Cleared immediately when /remove_channel is called.

_active_channel_id: int | None = None
_cache_lock = asyncio.Lock()   # prevents race on concurrent file sends


# ─── DB helpers ──────────────────────────────────────────────────────────────

async def _get_active_channel() -> dict | None:
    """
    Return the one channel that should receive the next file.

    Strategy — sequential fill:
      1. Try the cached _active_channel_id first (fast path).
      2. If cache is empty or that channel is now full/removed,
         scan DB for the first channel with file_count < CHANNEL_LIMIT
         ordered by insertion order (_id ascending = oldest first).
      3. Update cache.
      4. Return None if every channel is full or none exist.
    """
    global _active_channel_id

    async with _cache_lock:
        # Fast path — cache hit
        if _active_channel_id is not None:
            doc = await channels_col.find_one({"channel_id": _active_channel_id})
            if doc and doc.get("file_count", 0) < CHANNEL_LIMIT:
                return doc
            # Cache stale (channel removed or just filled up)
            logger.info(
                "Active channel %s is full or removed — finding next.",
                _active_channel_id
            )
            _active_channel_id = None

        # Slow path — find oldest non-full channel
        doc = await channels_col.find_one(
            {"file_count": {"$lt": CHANNEL_LIMIT}},
            sort=[("_id", 1)]   # oldest first = sequential fill
        )
        if doc:
            _active_channel_id = doc["channel_id"]
            logger.info("Active channel set → %d", _active_channel_id)
        return doc


async def get_target_channel() -> dict | None:
    """Public API used by bot.py process_single_message."""
    return await _get_active_channel()


async def increment_channel_count(channel_id: int):
    result = await channels_col.update_one(
        {"channel_id": channel_id},
        {"$inc": {"file_count": 1}}
    )
    # After increment, check if this channel just hit the limit
    doc = await channels_col.find_one({"channel_id": channel_id})
    if doc and doc.get("file_count", 0) >= CHANNEL_LIMIT:
        global _active_channel_id
        async with _cache_lock:
            if _active_channel_id == channel_id:
                logger.info(
                    "Channel %d reached limit %d — cache cleared, next file "
                    "will use next available channel.", channel_id, CHANNEL_LIMIT
                )
                _active_channel_id = None


# ─── /add_channel ─────────────────────────────────────────────────────────────

@app.on_message(filters.command("add_channel") & filters.private)
async def cmd_add_channel(_, message: Message):
    from config import admins_col   # local import avoids circular at module level

    uid = message.from_user.id

    # Auth check
    if uid != OWNER_ID:
        doc = await admins_col.find_one({"user_id": uid})
        if not doc:
            return

    try:
        channel_id = int(message.command[1])
    except (IndexError, ValueError):
        return await message.reply_text(
            "Usage: `/add_channel -100xxxxxxxxxx`",
            parse_mode="markdown"
        )

    if await channels_col.find_one({"channel_id": channel_id}):
        return await message.reply_text(
            f"⚠️ Channel `{channel_id}` is already added.", parse_mode="markdown"
        )

    await channels_col.insert_one({"channel_id": channel_id, "file_count": 0})
    logger.info("[USER:%d] Channel added: %d", uid, channel_id)
    await message.reply_text(
        f"✅ Channel Added:\n`{channel_id}`", parse_mode="markdown"
    )


# ─── /remove_channel ──────────────────────────────────────────────────────────

@app.on_message(filters.command("remove_channel") & filters.private)
async def cmd_remove_channel(_, message: Message):
    from config import admins_col

    uid = message.from_user.id

    if uid != OWNER_ID:
        doc = await admins_col.find_one({"user_id": uid})
        if not doc:
            return

    try:
        channel_id = int(message.command[1])
    except (IndexError, ValueError):
        return await message.reply_text(
            "Usage: `/remove_channel -100xxxxxxxxxx`", parse_mode="markdown"
        )

    result = await channels_col.delete_one({"channel_id": channel_id})

    if result.deleted_count == 0:
        return await message.reply_text(
            f"❌ Channel `{channel_id}` not found in DB.", parse_mode="markdown"
        )

    # ── CRITICAL: Clear in-memory cache immediately ───────────────────────────
    global _active_channel_id
    async with _cache_lock:
        if _active_channel_id == channel_id:
            _active_channel_id = None
            logger.info(
                "[USER:%d] Removed active channel %d — cache cleared immediately.",
                uid, channel_id
            )
        else:
            logger.info(
                "[USER:%d] Removed channel %d (was not active).",
                uid, channel_id
            )

    await message.reply_text(
        f"✅ Channel Removed:\n`{channel_id}`\n\n"
        f"Bot ab is channel mein koi file nahi bhejega.",
        parse_mode="markdown"
    )


# ─── /channels ────────────────────────────────────────────────────────────────

@app.on_message(filters.command("channels") & filters.private)
async def cmd_list_channels(_, message: Message):
    from config import admins_col

    uid = message.from_user.id

    if uid != OWNER_ID:
        doc = await admins_col.find_one({"user_id": uid})
        if not doc:
            return

    channels = [ch async for ch in channels_col.find({}, sort=[("_id", 1)])]

    if not channels:
        return await message.reply_text("No channels added yet.")

    active = _active_channel_id
    lines  = ["📋 **Target Channels** (sequential fill order)\n"]

    for i, ch in enumerate(channels, 1):
        cid    = ch["channel_id"]
        count  = ch.get("file_count", 0)
        is_act = " ◀ active" if cid == active else ""

        if count >= CHANNEL_LIMIT:
            status = "🔴 FULL"
        else:
            pct    = int(count / CHANNEL_LIMIT * 100)
            status = f"🟢 {count}/{CHANNEL_LIMIT} ({pct}%)"

        lines.append(f"{i}. `{cid}` — {status}{is_act}")

    await message.reply_text("\n".join(lines), parse_mode="markdown")

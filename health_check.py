"""
health_check.py
───────────────
1. HTTP health-check server  — Koyeb ke GET / probe ka jawab deta hai (200 OK)
2. Self-ping loop            — Har 10 min mein apne aap ko ping karta hai
                               taaki Koyeb free tier mein service sleep na ho.
"""

import os
import time
import threading
import logging
import urllib.request
import urllib.error
from http.server import BaseHTTPRequestHandler, HTTPServer
from datetime import datetime, timezone

logger = logging.getLogger("media_bot.health")

START_TIME = datetime.now(timezone.utc)

# ─── HTTP Handler ─────────────────────────────────────────────────────────────

class HealthHandler(BaseHTTPRequestHandler):
    """GET / aur GET /health → 200 JSON"""

    def log_message(self, format, *args):   # noqa: A002  — access log suppress
        logger.debug("Health probe: %s", format % args)

    def _send_json(self, body: str, status: int = 200):
        encoded = body.encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def do_GET(self):   # noqa: N802
        if self.path in ("/", "/health"):
            uptime = int((datetime.now(timezone.utc) - START_TIME).total_seconds())
            self._send_json(f'{{"status":"ok","uptime_seconds":{uptime}}}')
        else:
            self._send_json('{"status":"not_found"}', 404)


# ─── Self-Ping Loop ───────────────────────────────────────────────────────────

def _self_ping_loop(url: str, interval: int):
    """
    Daemon thread — har `interval` seconds mein `url` ko ping karta hai.
    Koyeb free tier mein idle timeout se bachne ke liye zaroori hai.
    """
    # Bot ko fully start hone ka time do
    time.sleep(30)
    logger.info("Self-ping loop started → %s (every %ds)", url, interval)

    while True:
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "self-ping/1.0"})
            with urllib.request.urlopen(req, timeout=10) as resp:
                logger.debug("Self-ping OK — status=%d", resp.status)
        except urllib.error.URLError as e:
            logger.warning("Self-ping failed: %s", e.reason)
        except Exception as e:
            logger.warning("Self-ping error: %s", e)

        time.sleep(interval)


# ─── Public API ───────────────────────────────────────────────────────────────

def start_health_server(port: int | None = None):
    """
    Health-check HTTP server + self-ping dono start karta hai.

    Env vars:
      PORT          — HTTP port (Koyeb automatically set karta hai, default 8000)
      PUBLIC_URL    — Apna Koyeb service URL (e.g. https://my-bot-abc123.koyeb.app)
                      Agar set na ho toh self-ping disabled rahega.
      PING_INTERVAL — Ping interval seconds (default 600 = 10 minutes)
    """
    port = port or int(os.getenv("PORT", 8000))

    # ── HTTP server ──────────────────────────────────────────────────────────
    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    t_server = threading.Thread(target=server.serve_forever, daemon=True)
    t_server.start()
    logger.info("Health-check server listening on port %d", port)

    # ── Self-ping ────────────────────────────────────────────────────────────
    public_url = os.getenv("PUBLIC_URL", "").rstrip("/")
    if public_url:
        interval = int(os.getenv("PING_INTERVAL", 600))   # default 10 min
        ping_url = f"{public_url}/health"
        t_ping = threading.Thread(
            target=_self_ping_loop,
            args=(ping_url, interval),
            daemon=True
        )
        t_ping.start()
    else:
        logger.warning(
            "PUBLIC_URL env var not set — self-ping disabled. "
            "Bot may sleep on Koyeb free tier!"
        )

    return server


# ── Standalone test ───────────────────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    start_health_server()
    print("Health server running. Press Ctrl+C to stop.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopped.")

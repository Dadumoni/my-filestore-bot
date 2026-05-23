"""
health_check.py
───────────────
Lightweight HTTP health-check server (runs alongside the bot).
Koyeb free tier pings GET / — this returns 200 OK so the service
stays alive and doesn't get killed for missing a health probe.

Run it in a separate thread via bot.py, or let Dockerfile CMD
start both processes with a simple shell wrapper.
"""

import os
import threading
import logging
from http.server import BaseHTTPRequestHandler, HTTPServer
from datetime import datetime, timezone

logger = logging.getLogger("media_bot.health")

START_TIME = datetime.now(timezone.utc)


class HealthHandler(BaseHTTPRequestHandler):
    """Handle GET / and GET /health — return 200 JSON."""

    # Silence default per-request access logs (noisy in prod)
    def log_message(self, format, *args):  # noqa: A002
        logger.debug("Health probe: %s", format % args)

    def _send_json(self, body: str, status: int = 200):
        encoded = body.encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def do_GET(self):  # noqa: N802
        if self.path in ("/", "/health"):
            uptime = (datetime.now(timezone.utc) - START_TIME).seconds
            self._send_json(
                f'{{"status":"ok","uptime_seconds":{uptime}}}'
            )
        else:
            self._send_json('{"status":"not_found"}', 404)


def start_health_server(port: int | None = None):
    """
    Start the health-check server in a daemon thread.
    Port is read from the PORT env var (Koyeb sets this automatically),
    falling back to 8000.
    """
    port = port or int(os.getenv("PORT", 8000))
    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    logger.info("Health-check server listening on port %d", port)
    return server


# ── Allow running standalone for local testing ────────────────────────────────
if __name__ == "__main__":
    import time
    logging.basicConfig(level=logging.DEBUG)
    start_health_server()
    print("Health server running. Press Ctrl+C to stop.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopped.")

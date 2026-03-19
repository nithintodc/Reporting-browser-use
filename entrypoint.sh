#!/bin/bash
set -e

# ─── Start Xvfb (virtual display — Chrome needs a display even in headless mode on some pages) ───
Xvfb :99 -screen 0 1920x1080x24 &
export DISPLAY=:99
echo "[entrypoint] Xvfb started on :99"

# ─── Start Chrome headless with CDP ───
# Only start Chrome if LOCAL_BROWSER_CDP_URL points to localhost (i.e., we host Chrome ourselves).
# If it points to an external service (Browserless, Multilogin), skip.
if [ -z "$LOCAL_BROWSER_CDP_URL" ] || echo "$LOCAL_BROWSER_CDP_URL" | grep -q "localhost\|127.0.0.1"; then
    google-chrome \
        --headless=new \
        --disable-gpu \
        --no-sandbox \
        --disable-dev-shm-usage \
        --remote-debugging-address=0.0.0.0 \
        --remote-debugging-port=9222 \
        --disable-background-networking \
        --disable-extensions \
        --disable-sync \
        --disable-translate \
        --no-first-run \
        --user-data-dir=/tmp/chrome-profile \
        --window-size=1920,1080 &

    # Wait for Chrome CDP to be ready (max 30s)
    echo "[entrypoint] Waiting for Chrome CDP on :9222..."
    for i in $(seq 1 30); do
        if curl -s http://localhost:9222/json/version > /dev/null 2>&1; then
            echo "[entrypoint] Chrome CDP ready ($(curl -s http://localhost:9222/json/version | python3 -c 'import sys,json; print(json.load(sys.stdin).get("Browser","?"))' 2>/dev/null))"
            break
        fi
        sleep 1
    done

    # Set CDP URL if not already set
    export LOCAL_BROWSER_CDP_URL="${LOCAL_BROWSER_CDP_URL:-http://localhost:9222}"
    echo "[entrypoint] LOCAL_BROWSER_CDP_URL=$LOCAL_BROWSER_CDP_URL"
fi

# ─── Run the app ───
echo "[entrypoint] Starting main.py..."
exec python main.py

#!/usr/bin/env bash
# Start Chrome with remote debugging and a persistent profile so automation can attach (USE_LOCAL_BROWSER=true).
# Uses the same profile as auto-start (CHROME_USER_DATA_DIR or .cursor/chrome-debug-profile).

set -e
PORT="${1:-9222}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
USER_DATA_DIR="${CHROME_USER_DATA_DIR:-$PROJECT_DIR/.cursor/chrome-debug-profile}"
mkdir -p "$USER_DATA_DIR"

if [[ "$(uname)" == "Darwin" ]]; then
  CHROME="/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
elif [[ -n "$WINDIR" ]]; then
  CHROME="C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe"
else
  CHROME="google-chrome"
fi

if [[ ! -x "$CHROME" ]] && ! command -v "$CHROME" &>/dev/null; then
  echo "Chrome not found at $CHROME. Install Chrome or set CHROME path." >&2
  exit 1
fi

echo "Starting Chrome with --remote-debugging-port=$PORT --user-data-dir=$USER_DATA_DIR"
echo "Log in to Gmail/DoorDash once in this window; then run: python main.py"
exec "$CHROME" --remote-debugging-port="$PORT" --user-data-dir="$USER_DATA_DIR"

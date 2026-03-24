#!/usr/bin/env bash
# One script to run the app: activate venv, then run the DoorDash report workflow.
# Usage:
#   ./run.sh          # Run scripted flow (main.py)
#   ./run.sh browser-use   # Run AI-driven flow (run_browser_use.py)
#   ./run.sh install  # Only create venv + install deps (no run)

set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# browser-use requires Python >= 3.11; macOS CLT python3 is often 3.9.
pick_python311_plus() {
  local cand
  for cand in python3.13 python3.12 python3.11 python3; do
    if command -v "$cand" >/dev/null 2>&1 \
      && "$cand" -c 'import sys; sys.exit(0 if sys.version_info >= (3, 11) else 1)' 2>/dev/null; then
      command -v "$cand"
      return 0
    fi
  done
  return 1
}

PY="$(pick_python311_plus)" || {
  echo "ERROR: Need Python 3.11 or newer (browser-use). Install e.g.:" >&2
  echo "  brew install python@3.12" >&2
  echo "Then ensure python3.12 or python3.11 is on your PATH." >&2
  exit 1
}

# 1. Create venv if missing, or recreate if it was made with Python < 3.11
if [[ ! -d .venv ]] || ! .venv/bin/python -c 'import sys; sys.exit(0 if sys.version_info >= (3, 11) else 1)' 2>/dev/null; then
  if [[ -d .venv ]]; then
    echo "Replacing .venv (needs Python >= 3.11; was built with an older interpreter)..."
    rm -rf .venv
  fi
  echo "Creating .venv with $PY ($("$PY" --version))..."
  "$PY" -m venv .venv
fi

VENV_PYTHON="$SCRIPT_DIR/.venv/bin/python"

# 2. Activate venv (for interactive shells / tools that expect VIRTUAL_ENV)
echo "Activating venv..."
# shellcheck source=/dev/null
source .venv/bin/activate

# 3. Install deps if needed (quick check: browser_use not installed)
# Use $VENV_PYTHON so we never rely on PATH (some environments lack `python` after activate).
if ! "$VENV_PYTHON" -c "import browser_use" 2>/dev/null; then
  echo "Installing dependencies..."
  "$VENV_PYTHON" -m pip install --upgrade pip
  "$VENV_PYTHON" -m pip install -r requirements.txt
fi

# 4. Check .env exists
if [[ ! -f .env ]]; then
  echo "Warning: .env not found. Copy .env.example to .env and set credentials."
  echo "  cp .env.example .env"
  read -p "Continue anyway? [y/N] " -n 1 -r
  echo
  [[ $REPLY =~ ^[Yy]$ ]] || exit 1
fi

# 5. Run
if [[ "${1:-}" == "install" ]]; then
  echo "Install complete. Run ./run.sh to start the app."
  exit 0
fi

if [[ "${1:-}" == "browser-use" ]]; then
  echo "Running browser-use (AI-driven)..."
  exec "$VENV_PYTHON" run_browser_use.py
fi

echo "Running main flow (scripted DoorDash report)..."
exec "$VENV_PYTHON" main.py

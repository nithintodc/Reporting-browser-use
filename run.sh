#!/usr/bin/env bash
# One script to run the app: activate venv, then run the DoorDash report workflow.
# Usage:
#   ./run.sh          # Run scripted flow (main.py)
#   ./run.sh browser-use   # Run AI-driven flow (run_browser_use.py)
#   ./run.sh install  # Only create venv + install deps (no run)

set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# 1. Create venv if missing
if [[ ! -d .venv ]]; then
  echo "Creating .venv..."
  python3 -m venv .venv
fi

# 2. Activate venv
echo "Activating venv..."
# shellcheck source=/dev/null
source .venv/bin/activate

# 3. Install deps if needed (quick check: playwright not installed)
if ! python -c "import playwright" 2>/dev/null; then
  echo "Installing dependencies..."
  pip install -r requirements.txt
  playwright install chromium
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
  exec python run_browser_use.py
fi

echo "Running main flow (scripted DoorDash report)..."
exec python main.py

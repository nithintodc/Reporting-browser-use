#!/bin/bash
# =============================================================================
# Step 4: Verification — Run this INSIDE the GCE VM
# Checks all components are working before first real run
# =============================================================================
set -e

APP_DIR="/opt/doordash-bot"
PASS=0
FAIL=0
WARN=0

check() {
    local name="$1"
    shift
    if "$@" > /dev/null 2>&1; then
        echo "  [PASS] $name"
        PASS=$((PASS + 1))
    else
        echo "  [FAIL] $name"
        FAIL=$((FAIL + 1))
    fi
}

warn_check() {
    local name="$1"
    shift
    if "$@" > /dev/null 2>&1; then
        echo "  [PASS] $name"
        PASS=$((PASS + 1))
    else
        echo "  [WARN] $name (optional)"
        WARN=$((WARN + 1))
    fi
}

echo "============================================"
echo " Deployment Verification"
echo "============================================"
echo ""

# ──── System ────
echo "System:"
check "Google Chrome installed"         command -v google-chrome
check "Python 3 installed"              command -v python3
check "Xvfb installed"                  command -v Xvfb
check "curl installed"                  command -v curl

echo ""

# ──── Services ────
echo "Services:"
check "Xvfb service running"            systemctl is-active xvfb
check "Chrome headless service running"  systemctl is-active chrome-headless
check "Chrome CDP responding"            curl -sf http://localhost:9222/json/version

echo ""

# ──── Chrome details ────
echo "Chrome CDP info:"
CHROME_VER=$(curl -sf http://localhost:9222/json/version 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin).get('Browser','unknown'))" 2>/dev/null || echo "unreachable")
echo "  Browser: $CHROME_VER"
echo "  CDP URL: http://localhost:9222"

echo ""

# ──── Application ────
echo "Application:"
check "App directory exists"             test -d "$APP_DIR"
check ".venv exists"                     test -d "$APP_DIR/.venv"
check ".env exists"                      test -f "$APP_DIR/.env"
check "main.py exists"                   test -f "$APP_DIR/main.py"
check "agents/ exists"                   test -d "$APP_DIR/agents"
check "downloads/ exists"                test -d "$APP_DIR/downloads"
check "logs/ exists"                     test -d "$APP_DIR/logs"

echo ""

# ──── .env variables ────
echo "Environment variables (.env):"
cd "$APP_DIR"
source .venv/bin/activate 2>/dev/null || true

check_env() {
    local var_name="$1"
    if grep -q "^${var_name}=" "$APP_DIR/.env" 2>/dev/null; then
        local val
        val=$(grep "^${var_name}=" "$APP_DIR/.env" | cut -d= -f2-)
        if [ -n "$val" ] && [ "$val" != "your_password_here" ] && [ "$val" != "your_browser_use_api_key" ]; then
            echo "  [PASS] $var_name is set"
            PASS=$((PASS + 1))
        else
            echo "  [FAIL] $var_name is set but looks like a placeholder"
            FAIL=$((FAIL + 1))
        fi
    else
        echo "  [FAIL] $var_name is missing from .env"
        FAIL=$((FAIL + 1))
    fi
}

check_env "DOORDASH_EMAIL"
check_env "DOORDASH_PASSWORD"
check_env "BROWSER_USE_API_KEY"
check_env "LOCAL_BROWSER_CDP_URL"
check_env "GOOGLE_SPREADSHEET_ID"

# Optional
if grep -q "^SLACK_WEBHOOK_URL=" "$APP_DIR/.env" 2>/dev/null; then
    echo "  [PASS] SLACK_WEBHOOK_URL is set"
    PASS=$((PASS + 1))
else
    echo "  [WARN] SLACK_WEBHOOK_URL not set (Slack alerts disabled)"
    WARN=$((WARN + 1))
fi

echo ""

# ──── GCP Service Account Key ────
echo "Google Sheets credentials:"
if ls "$APP_DIR"/todc-marketing-*.json &>/dev/null 2>&1; then
    SA_FILE=$(ls "$APP_DIR"/todc-marketing-*.json | head -1)
    SA_EMAIL=$(python3 -c "import json; print(json.load(open('$SA_FILE'))['client_email'])" 2>/dev/null || echo "unknown")
    echo "  [PASS] Service account key found: $(basename $SA_FILE)"
    echo "         Email: $SA_EMAIL"
    PASS=$((PASS + 1))
else
    echo "  [FAIL] No todc-marketing-*.json found"
    echo "         Copy it from your laptop:"
    echo "         gcloud compute scp ~/path/todc-marketing-*.json $(hostname):$APP_DIR/"
    FAIL=$((FAIL + 1))
fi

echo ""

# ──── Python imports ────
echo "Python dependencies:"
cd "$APP_DIR"
source .venv/bin/activate 2>/dev/null || true

check "import browser_use"     python3 -c "import browser_use"
check "import pandas"          python3 -c "import pandas"
check "import openpyxl"        python3 -c "import openpyxl"
check "import requests"        python3 -c "import requests"
check "import dotenv"          python3 -c "import dotenv"
check "import googleapiclient" python3 -c "import googleapiclient"

echo ""

# ──── Network ────
echo "Network connectivity:"
check "Can reach DoorDash"             curl -sf --max-time 10 https://merchant-portal.doordash.com -o /dev/null
check "Can reach Google Sheets API"    curl -sf --max-time 10 https://sheets.googleapis.com -o /dev/null
warn_check "Can reach Slack"           curl -sf --max-time 10 https://hooks.slack.com -o /dev/null

echo ""

# ──── Shared memory ────
echo "System resources:"
SHM_SIZE=$(df -BM /dev/shm 2>/dev/null | tail -1 | awk '{print $2}' | tr -d 'M')
if [ -n "$SHM_SIZE" ] && [ "$SHM_SIZE" -ge 1024 ]; then
    echo "  [PASS] /dev/shm: ${SHM_SIZE}MB (>= 1024MB)"
    PASS=$((PASS + 1))
else
    echo "  [WARN] /dev/shm: ${SHM_SIZE:-unknown}MB (recommend >= 2048MB)"
    echo "         Fix: sudo mount -o remount,size=2G /dev/shm"
    WARN=$((WARN + 1))
fi

MEM_TOTAL=$(free -m 2>/dev/null | awk '/^Mem:/{print $2}')
if [ -n "$MEM_TOTAL" ] && [ "$MEM_TOTAL" -ge 8000 ]; then
    echo "  [PASS] RAM: ${MEM_TOTAL}MB (>= 8GB)"
    PASS=$((PASS + 1))
else
    echo "  [WARN] RAM: ${MEM_TOTAL:-unknown}MB (recommend >= 8GB, 16GB ideal)"
    WARN=$((WARN + 1))
fi

echo ""

# ──── Summary ────
echo "============================================"
echo " Results: $PASS passed, $FAIL failed, $WARN warnings"
echo "============================================"

if [ "$FAIL" -eq 0 ]; then
    echo ""
    echo " All critical checks passed! Ready to run:"
    echo "   cd $APP_DIR && source .venv/bin/activate && python main.py"
    echo ""
else
    echo ""
    echo " Fix the $FAIL failed check(s) above before running."
    echo ""
    exit 1
fi

#!/bin/bash
# =============================================================================
# Step 2: VM Setup — Run this INSIDE the GCE VM (after SSH)
# Installs Chrome, Python, clones repo, configures services
# =============================================================================
set -euo pipefail

APP_DIR="/opt/doordash-bot"
CHROME_PROFILE_DIR="/home/chrome-profile"

echo "============================================"
echo " VM Setup for DoorDash Automation"
echo "============================================"

# ──────────────────────────────────────────────────────────────────────────────
# 1. System packages
# ──────────────────────────────────────────────────────────────────────────────
echo ""
echo ">>> Installing system packages..."
sudo apt-get update
sudo apt-get install -y --no-install-recommends \
    wget gnupg curl git python3 python3-pip python3-venv \
    fonts-liberation libasound2 libatk-bridge2.0-0 libatk1.0-0 \
    libcups2 libdbus-1-3 libdrm2 libgbm1 libnspr4 libnss3 \
    libxcomposite1 libxdamage1 libxrandr2 libxss1 libxtst6 \
    xdg-utils xvfb

echo "    System packages installed."

# ──────────────────────────────────────────────────────────────────────────────
# 2. Install Google Chrome
# ──────────────────────────────────────────────────────────────────────────────
echo ""
echo ">>> Installing Google Chrome..."
if command -v google-chrome &>/dev/null; then
    echo "    Chrome already installed: $(google-chrome --version)"
else
    wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | sudo apt-key add -
    echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" | \
        sudo tee /etc/apt/sources.list.d/google-chrome.list
    sudo apt-get update
    sudo apt-get install -y google-chrome-stable
    echo "    Installed: $(google-chrome --version)"
fi

# ──────────────────────────────────────────────────────────────────────────────
# 3. Create Chrome headless systemd service
# ──────────────────────────────────────────────────────────────────────────────
echo ""
echo ">>> Creating Chrome headless service..."

sudo mkdir -p "$CHROME_PROFILE_DIR"

sudo tee /etc/systemd/system/chrome-headless.service > /dev/null << 'CHROME_SERVICE'
[Unit]
Description=Headless Chrome with CDP (port 9222)
After=network.target

[Service]
Type=simple
Environment=DISPLAY=:99
ExecStartPre=/usr/bin/Xvfb :99 -screen 0 1920x1080x24 &
ExecStart=/usr/bin/google-chrome \
    --headless=new \
    --disable-gpu \
    --no-sandbox \
    --disable-dev-shm-usage \
    --remote-debugging-address=127.0.0.1 \
    --remote-debugging-port=9222 \
    --disable-background-networking \
    --disable-extensions \
    --disable-sync \
    --disable-translate \
    --no-first-run \
    --user-data-dir=/home/chrome-profile \
    --window-size=1920,1080
Restart=always
RestartSec=5
# Run as non-root user for security
User=nobody
Group=nogroup

[Install]
WantedBy=multi-user.target
CHROME_SERVICE

# Xvfb as a separate service (Chrome needs a display)
sudo tee /etc/systemd/system/xvfb.service > /dev/null << 'XVFB_SERVICE'
[Unit]
Description=Xvfb virtual display
Before=chrome-headless.service

[Service]
Type=simple
ExecStart=/usr/bin/Xvfb :99 -screen 0 1920x1080x24
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
XVFB_SERVICE

# Update Chrome service to depend on Xvfb and remove the ExecStartPre
sudo tee /etc/systemd/system/chrome-headless.service > /dev/null << 'CHROME_SERVICE'
[Unit]
Description=Headless Chrome with CDP (port 9222)
After=network.target xvfb.service
Requires=xvfb.service

[Service]
Type=simple
Environment=DISPLAY=:99
ExecStart=/usr/bin/google-chrome \
    --headless=new \
    --disable-gpu \
    --no-sandbox \
    --disable-dev-shm-usage \
    --remote-debugging-address=127.0.0.1 \
    --remote-debugging-port=9222 \
    --disable-background-networking \
    --disable-extensions \
    --disable-sync \
    --disable-translate \
    --no-first-run \
    --user-data-dir=/home/chrome-profile \
    --window-size=1920,1080
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
CHROME_SERVICE

sudo chown -R nobody:nogroup "$CHROME_PROFILE_DIR" || true
sudo systemctl daemon-reload
sudo systemctl enable xvfb chrome-headless
sudo systemctl start xvfb
sleep 2
sudo systemctl start chrome-headless
sleep 3

# Verify Chrome is running
if curl -s http://localhost:9222/json/version > /dev/null 2>&1; then
    echo "    Chrome CDP is running:"
    curl -s http://localhost:9222/json/version | python3 -m json.tool
else
    echo "    WARNING: Chrome CDP not responding. Check: sudo journalctl -u chrome-headless -f"
fi

# ──────────────────────────────────────────────────────────────────────────────
# 4. Clone/update the application
# ──────────────────────────────────────────────────────────────────────────────
echo ""
echo ">>> Setting up application at $APP_DIR..."

if [ -d "$APP_DIR" ]; then
    echo "    App directory exists. Pulling latest..."
    cd "$APP_DIR"
    git pull || echo "    Git pull failed — you may need to set up SSH keys or use HTTPS."
else
    echo "    Cloning repository..."
    echo "    IMPORTANT: Replace the URL below with your actual repo URL."
    echo ""
    read -p "    Enter your git repo URL (HTTPS): " REPO_URL
    sudo git clone "$REPO_URL" "$APP_DIR"
    sudo chown -R "$(whoami)" "$APP_DIR"
    cd "$APP_DIR"
fi

# ──────────────────────────────────────────────────────────────────────────────
# 5. Python virtual environment
# ──────────────────────────────────────────────────────────────────────────────
echo ""
echo ">>> Setting up Python environment..."

cd "$APP_DIR"
if [ ! -d ".venv" ]; then
    python3 -m venv .venv
fi
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
pip install requests  # ensure requests is installed (slack_agent dependency)

echo "    Python environment ready."

# ──────────────────────────────────────────────────────────────────────────────
# 6. Create .env file
# ──────────────────────────────────────────────────────────────────────────────
echo ""
echo ">>> Creating .env file..."

if [ -f "$APP_DIR/.env" ]; then
    echo "    .env already exists — skipping. Edit manually if needed: nano $APP_DIR/.env"
else
    read -p  "    DoorDash email: " DD_EMAIL
    read -sp "    DoorDash password: " DD_PASS; echo ""
    read -sp "    Browser Use API key: " BU_KEY; echo ""
    read -p  "    Google Spreadsheet ID: " GSHEET_ID
    read -p  "    Slack webhook URL (or blank to skip): " SLACK_URL
    read -p  "    Operator name (e.g. TODC): " OP_NAME

    cat > "$APP_DIR/.env" << ENVFILE
DOORDASH_EMAIL=${DD_EMAIL}
DOORDASH_PASSWORD=${DD_PASS}
BROWSER_USE_API_KEY=${BU_KEY}
LOCAL_BROWSER_CDP_URL=http://localhost:9222
GOOGLE_SPREADSHEET_ID=${GSHEET_ID}
SLACK_WEBHOOK_URL=${SLACK_URL}
MAX_CAMPAIGNS_PER_SESSION=5
OPERATOR_NAME=${OP_NAME:-TODC}
ENVFILE

    chmod 600 "$APP_DIR/.env"
    echo "    .env created (permissions: 600)."
fi

# ──────────────────────────────────────────────────────────────────────────────
# 7. Copy GCP service account key (for Google Sheets push)
# ──────────────────────────────────────────────────────────────────────────────
echo ""
echo ">>> GCP service account key for Google Sheets..."
if ls "$APP_DIR"/todc-marketing-*.json &>/dev/null 2>&1; then
    echo "    Service account key already present."
else
    echo "    You need to copy your todc-marketing-*.json to the VM."
    echo "    From your laptop, run:"
    echo "      gcloud compute scp /path/to/todc-marketing-*.json $(hostname):$APP_DIR/"
    echo "    Or paste the JSON into a file manually."
fi

# ──────────────────────────────────────────────────────────────────────────────
# 8. Increase shared memory (prevents Chrome crashes)
# ──────────────────────────────────────────────────────────────────────────────
echo ""
echo ">>> Increasing /dev/shm to 2GB..."
sudo mount -o remount,size=2G /dev/shm 2>/dev/null || true

# Make persistent across reboots
if ! grep -q "tmpfs /dev/shm" /etc/fstab; then
    echo "tmpfs /dev/shm tmpfs defaults,size=2G 0 0" | sudo tee -a /etc/fstab
fi

echo ""
echo "============================================"
echo " VM Setup Complete!"
echo ""
echo " Test the app:"
echo "   cd $APP_DIR"
echo "   source .venv/bin/activate"
echo "   python main.py"
echo ""
echo " Set up scheduled runs:"
echo "   bash deploy/03-schedule.sh"
echo "============================================"

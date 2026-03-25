# VM Setup (Manual Clone) — DoorDash Bot

This is the “do it directly on the VM” setup guide you asked for.

It avoids the previous `deploy.sh --install` failure by ensuring **Python 3.11+** is used for the virtualenv. (The `browser-use` package requires Python 3.11+; if the VM uses an older `python3`, pip will show “No matching distribution found”.)

## Target VM (from your screenshot)

- Instance: `todc-ent-applications`
- Zone: `us-west2-a`
- App directory: `/opt/doordash-bot`

## 1) SSH to the VM

```bash
gcloud compute ssh todc-ent-applications --zone=us-west2-a
```

## 2) Ensure Python 3.11+ is installed

```bash
python3 --version
python3.11 --version || true
```

If `python3.11` is missing, run:

```bash
sudo apt-get update
sudo apt-get install -y software-properties-common
sudo add-apt-repository -y ppa:deadsnakes/ppa
sudo apt-get update
sudo apt-get install -y python3.11 python3.11-venv python3.11-dev
```

Verify:

```bash
python3.11 --version
```

## 3) (Optional) Ensure Chrome CDP is running

If you already ran your VM setup scripts, you can skip this check.

```bash
systemctl is-active chrome-headless || true
curl -sf http://localhost:9222/json/version
```

Expected: a JSON payload with `Browser` and `webSocketDebuggerUrl`.

## 4) Clone the repo on the VM
```bash
# If you need the correct repo URL, grab it on your laptop:
#   git remote get-url origin

cd /opt

if [ -d /opt/doordash-bot/.git ]; then
  cd /opt/doordash-bot
  git pull
else
  # Paste your repo URL here:
  REPO_URL="PASTE_YOUR_REPO_URL_HERE"
  sudo git clone "$REPO_URL" /opt/doordash-bot
fi

sudo chown -R "$USER":"$USER" /opt/doordash-bot
cd /opt/doordash-bot
```

## 5) Create a venv using Python 3.11 and install deps

```bash
cd /opt/doordash-bot

python3.11 -m venv .venv
source .venv/bin/activate

python -m pip install --upgrade pip setuptools wheel
pip install -r requirements.txt
```

Quick sanity:

```bash
python -c "import browser_use; import pandas; import openpyxl; import requests"
```

## 6) Create `.env`

```bash
cd /opt/doordash-bot

cp .env.example .env
nano .env
```

You must set at least:
- `DOORDASH_EMAIL`
- `DOORDASH_PASSWORD`
- `BROWSER_USE_API_KEY`
- `LOCAL_BROWSER_CDP_URL=http://localhost:9222`
- `GOOGLE_SPREADSHEET_ID`

## 7) Add the GCP service account JSON for Google Sheets

Copy your file into `/opt/doordash-bot/`:

```bash
ls -la todc-marketing-*.json || true
```

If missing, from your laptop:

```bash
gcloud compute scp /path/to/todc-marketing-*.json todc-ent-applications:/opt/doordash-bot --zone=us-west2-a
```

## 8) Verify everything

```bash
cd /opt/doordash-bot
bash deploy/04-verify.sh
```

If verification passes, run the bot:

```bash
source .venv/bin/activate
python main.py
```

## Troubleshooting: `browser-use` install fails with “No matching distribution found”

1. Confirm you’re using Python 3.11:
   - `python3.11 --version`
   - and that your venv points to it:
     - `which python`
     - `python --version`
2. Delete and recreate the venv:
   ```bash
   rm -rf .venv
   python3.11 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```


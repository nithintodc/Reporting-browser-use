# GCP Deployment Guide — DoorDash Browser Automation (Option A)

> **Goal**: Move the DoorDash reporting + campaign automation from a local laptop to a GCE VM with headless Chrome, controlled via Chrome DevTools Protocol (CDP).

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  GCE VM: e2-standard-4 (4 vCPU, 16 GB RAM, Ubuntu 22.04)      │
│                                                                  │
│  ┌──────────────────────┐    CDP (localhost:9222)                │
│  │ Chrome Headless       │◄──────────────────────┐               │
│  │ (systemd service)     │                       │               │
│  │ --remote-debugging    │                       │               │
│  └──────────────────────┘                       │               │
│                                                  │               │
│  ┌──────────────────────────────────────────────┐│               │
│  │ Python App (main.py)                          │               │
│  │  ├─ browser-use Agent ──── connects via CDP ──┘               │
│  │  ├─ analysis_agent (pandas)                                   │
│  │  ├─ marketing_agent (pandas)                                  │
│  │  ├─ combined_report_agent (openpyxl)                          │
│  │  ├─ google_pusher_agent ──► Google Sheets API                 │
│  │  └─ slack_agent ──────────► Slack Webhook                     │
│  └──────────────────────────────────────────────┘                │
│                                                                  │
│  Cloud Scheduler (cron) ──► starts VM or triggers cron           │
│  Firewall: CDP port 9222 BLOCKED from external                   │
└─────────────────────────────────────────────────────────────────┘
```

---

## Cost Estimate

| Resource | Always-on | Scheduled (4 hrs/day) |
|----------|-----------|----------------------|
| GCE e2-standard-4 | $97/month | **$16/month** |
| 50 GB pd-balanced disk | $5/month | $5/month |
| Cloud Scheduler | Free | Free |
| Cloud Logging | Free (<50 GB) | Free |
| browser-use API (~20 campaigns/day) | ~$60-90/month | ~$60-90/month |
| **Total** | **~$162/month** | **~$81/month** |

With 1-year committed use: ~30% off compute. With scheduled start/stop: **~$81/month all-in**.

---

## Prerequisites

Before starting, you need:

- [ ] GCP account with billing enabled
- [ ] `gcloud` CLI installed on your laptop ([install guide](https://cloud.google.com/sdk/docs/install))
- [ ] Your existing credentials:
  - `todc-marketing-*.json` (GCP service account key for Google Sheets)
  - `BROWSER_USE_API_KEY`
  - `DOORDASH_EMAIL` and `DOORDASH_PASSWORD`
  - `SLACK_WEBHOOK_URL` (optional)
  - `GOOGLE_SPREADSHEET_ID`

---

## Step 1: GCP Project Setup (from your laptop)

### 1.1 Authenticate and set project

```bash
gcloud auth login
gcloud config set project YOUR_PROJECT_ID
```

### 1.2 Enable required APIs

```bash
gcloud services enable \
    compute.googleapis.com \
    cloudscheduler.googleapis.com \
    sheets.googleapis.com \
    drive.googleapis.com \
    secretmanager.googleapis.com \
    logging.googleapis.com
```

### 1.3 Create service account

```bash
PROJECT_ID=$(gcloud config get-value project)
SA_NAME="doordash-automation"
SA_EMAIL="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

gcloud iam service-accounts create $SA_NAME \
    --display-name="DoorDash Automation Bot"

# Grant required roles
for ROLE in roles/logging.logWriter roles/secretmanager.secretAccessor roles/compute.instanceAdmin.v1; do
    gcloud projects add-iam-policy-binding $PROJECT_ID \
        --member="serviceAccount:${SA_EMAIL}" \
        --role="$ROLE" --quiet
done
```

### 1.4 Create firewall rule (CRITICAL — blocks external access to Chrome CDP)

```bash
gcloud compute firewall-rules create deny-cdp-external \
    --direction=INGRESS \
    --action=DENY \
    --rules=tcp:9222 \
    --source-ranges=0.0.0.0/0 \
    --target-tags=doordash-bot \
    --priority=100 \
    --description="Block external access to Chrome CDP port"
```

> **Why this matters**: Chrome CDP has no authentication. Anyone who can reach port 9222 has full browser control. This rule ensures only localhost (the app on the same VM) can connect.

### 1.5 Create the GCE VM

```bash
gcloud compute instances create doordash-bot \
    --zone=us-central1-a \
    --machine-type=e2-standard-4 \
    --image-family=ubuntu-2204-lts \
    --image-project=ubuntu-os-cloud \
    --boot-disk-size=50GB \
    --boot-disk-type=pd-balanced \
    --service-account=${SA_EMAIL} \
    --scopes=https://www.googleapis.com/auth/cloud-platform \
    --tags=doordash-bot \
    --metadata=enable-oslogin=true
```

**Why e2-standard-4 (4 vCPU, 16 GB RAM)?**
| Component | RAM needed |
|-----------|-----------|
| Chrome headless (1-2 tabs) | 1-2 GB |
| browser-use Agent (page history) | 1-2 GB |
| Pandas + openpyxl (report processing) | 1-2 GB |
| Python runtime + OS | 2 GB |
| **Headroom for spikes** | 8+ GB |

Do **NOT** use e2-micro or e2-small — Chrome will OOM crash mid-workflow.

### 1.6 Copy your service account key to the VM

```bash
gcloud compute scp /path/to/todc-marketing-*.json doordash-bot:/tmp/ --zone=us-central1-a
```

**Or use the automated script:**
```bash
# From your laptop, inside the project directory:
export GCP_PROJECT_ID="your-project-id"
bash deploy/01-gcp-setup.sh
```

---

## Step 2: VM Setup (SSH into the VM)

### 2.1 SSH in

```bash
gcloud compute ssh doordash-bot --zone=us-central1-a
```

### 2.2 Install system packages

```bash
sudo apt-get update
sudo apt-get install -y --no-install-recommends \
    wget gnupg curl git python3 python3-pip python3-venv \
    fonts-liberation libasound2 libatk-bridge2.0-0 libatk1.0-0 \
    libcups2 libdbus-1-3 libdrm2 libgbm1 libnspr4 libnss3 \
    libxcomposite1 libxdamage1 libxrandr2 libxss1 libxtst6 \
    xdg-utils xvfb
```

### 2.3 Install Google Chrome

```bash
wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | sudo apt-key add -
echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" | \
    sudo tee /etc/apt/sources.list.d/google-chrome.list
sudo apt-get update
sudo apt-get install -y google-chrome-stable

# Verify
google-chrome --version
# Expected: Google Chrome 13x.x.xxxx.xx
```

> **Why Chrome and not Chromium?** DoorDash may fingerprint the browser. Chrome is identical to what real users run. Chromium has detectable differences.

### 2.4 Increase shared memory (prevents Chrome crashes)

```bash
sudo mount -o remount,size=2G /dev/shm

# Make persistent across reboots
echo "tmpfs /dev/shm tmpfs defaults,size=2G 0 0" | sudo tee -a /etc/fstab
```

### 2.5 Create Xvfb service (virtual display)

Chrome needs a display — even in headless mode some pages require it.

```bash
sudo tee /etc/systemd/system/xvfb.service << 'EOF'
[Unit]
Description=Xvfb virtual display :99

[Service]
Type=simple
ExecStart=/usr/bin/Xvfb :99 -screen 0 1920x1080x24
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable --now xvfb
```

### 2.6 Create Chrome headless service

```bash
sudo mkdir -p /home/chrome-profile

sudo tee /etc/systemd/system/chrome-headless.service << 'EOF'
[Unit]
Description=Headless Chrome with CDP on port 9222
After=xvfb.service
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
EOF

sudo systemctl daemon-reload
sudo systemctl enable --now chrome-headless
```

### 2.7 Verify Chrome CDP is running

```bash
# Wait a few seconds for Chrome to start
sleep 3

curl -s http://localhost:9222/json/version | python3 -m json.tool
```

Expected output:
```json
{
    "Browser": "Chrome/13x.x.xxxx.xx",
    "Protocol-Version": "1.3",
    "webSocketDebuggerUrl": "ws://127.0.0.1:9222/devtools/browser/..."
}
```

If this fails, check logs:
```bash
sudo journalctl -u chrome-headless -n 50
sudo journalctl -u xvfb -n 20
```

---

## Step 3: Deploy the Application

You can either clone on the VM (below) or **sync from your laptop** with `deploy.sh` after one-time VM setup (Chrome, `.env`, service account key). See [Deploy from laptop (rsync)](#deploy-from-laptop-rsync).

If you want the simplest “do everything on the VM” flow (and you hit `browser-use` install issues), follow `deploy/VM_SETUP_MANUAL.md` instead.

### 3.1 Clone the repository

```bash
sudo git clone https://github.com/YOUR_ORG/Reporting-browser-use-claude-code.git /opt/doordash-bot
sudo chown -R $(whoami) /opt/doordash-bot
cd /opt/doordash-bot
```

### 3.2 Set up Python environment

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```

### 3.3 Copy service account key

```bash
# If you used gcloud scp to /tmp earlier:
cp /tmp/todc-marketing-*.json /opt/doordash-bot/
```

### 3.4 Create .env file

```bash
cat > /opt/doordash-bot/.env << 'EOF'
# DoorDash credentials
DOORDASH_EMAIL=mcd+example@theondemandcompany.com
DOORDASH_PASSWORD=your_actual_password

# Browser Use API
BROWSER_USE_API_KEY=your_actual_api_key

# Connect to headless Chrome on this VM
LOCAL_BROWSER_CDP_URL=http://localhost:9222

# Google Sheets
GOOGLE_SPREADSHEET_ID=your_spreadsheet_id

# Slack
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/YOUR/WEBHOOK/URL

# Tuning
MAX_CAMPAIGNS_PER_SESSION=5
OPERATOR_NAME=TODC
EOF

# Restrict permissions (only owner can read)
chmod 600 /opt/doordash-bot/.env
```

> **Edit the values above** with your real credentials. Use `nano /opt/doordash-bot/.env` to edit.

---

## Step 4: Verify Everything

### 4.1 Run the verification script

```bash
cd /opt/doordash-bot
bash deploy/04-verify.sh
```

This checks:
- Chrome CDP is responding
- All Python dependencies import correctly
- `.env` has all required variables
- Service account key is present
- Network can reach DoorDash, Google, and Slack
- RAM and /dev/shm are adequate

### 4.2 Manual test run

```bash
cd /opt/doordash-bot
source .venv/bin/activate
python main.py
```

Monitor in real-time:
```bash
# In another SSH session:
tail -f /opt/doordash-bot/logs/latest.log
```

Check Slack for notifications — you should see "Phase 1 started" within 30 seconds.

---

## Step 5: Set Up Scheduled Runs

### Option A: Simple cron (VM stays on — $97/month compute)

```bash
# Run daily at 6 AM Central (12:00 UTC)
(crontab -l 2>/dev/null; echo "0 12 * * * cd /opt/doordash-bot && /opt/doordash-bot/.venv/bin/python main.py >> /opt/doordash-bot/logs/cron_\$(date +\%Y\%m\%d).log 2>&1") | crontab -

# Verify
crontab -l
```

### Option B: Auto start/stop VM (saves ~80% — $16/month compute)

**On the VM**, create a boot-trigger service:

```bash
sudo tee /etc/systemd/system/doordash-bot.service << 'EOF'
[Unit]
Description=DoorDash Bot — runs on boot, shuts down when done
After=chrome-headless.service network-online.target
Requires=chrome-headless.service

[Service]
Type=oneshot
WorkingDirectory=/opt/doordash-bot
EnvironmentFile=/opt/doordash-bot/.env
ExecStart=/bin/bash -c 'source /opt/doordash-bot/.venv/bin/activate && python main.py && sleep 60 && shutdown -h now'
StandardOutput=journal
StandardError=journal
TimeoutStartSec=7200

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable doordash-bot.service
```

**On your laptop**, create Cloud Scheduler to start the VM daily:

```bash
PROJECT_ID=$(gcloud config get-value project)
SA_EMAIL="doordash-automation@${PROJECT_ID}.iam.gserviceaccount.com"

gcloud scheduler jobs create http doordash-daily-start \
    --schedule="0 6 * * *" \
    --uri="https://compute.googleapis.com/compute/v1/projects/${PROJECT_ID}/zones/us-central1-a/instances/doordash-bot/start" \
    --http-method=POST \
    --oauth-service-account-email="${SA_EMAIL}" \
    --time-zone="America/Chicago" \
    --location=us-central1
```

**How it works:**
1. Cloud Scheduler sends HTTP POST to Compute API at 6 AM → VM starts
2. VM boots → systemd starts Chrome → starts `doordash-bot.service`
3. App runs the full pipeline (reports → analysis → campaigns)
4. App finishes → VM shuts itself down
5. You only pay for the 2-4 hours the VM was running

**Test it:**
```bash
# Manually trigger (from laptop):
gcloud compute instances start doordash-bot --zone=us-central1-a

# Watch it run (from laptop):
gcloud compute ssh doordash-bot --zone=us-central1-a -- "journalctl -u doordash-bot -f"
```

**Or use the automated script:**
```bash
# On the VM:
bash deploy/03-schedule.sh
```

---

## Step 6: Docker Alternative (Optional)

If you prefer Docker over a bare VM setup:

### 6.1 Build the image (on your laptop or the VM)

```bash
cd /path/to/Reporting-browser-use-claude-code
docker build -t doordash-bot .
```

### 6.2 Run with Docker

```bash
docker run --rm \
    --shm-size=2g \
    --env-file .env \
    -v $(pwd)/downloads:/app/downloads \
    -v $(pwd)/logs:/app/logs \
    -v $(pwd)/todc-marketing-ad02212d4f16.json:/app/todc-marketing-ad02212d4f16.json:ro \
    doordash-bot
```

The `Dockerfile` and `entrypoint.sh` handle:
- Installing Chrome
- Starting Xvfb + Chrome headless
- Waiting for CDP to be ready
- Running `main.py`

### 6.3 Push to GCR and run on the VM

```bash
# Tag and push
PROJECT_ID=$(gcloud config get-value project)
docker tag doordash-bot gcr.io/$PROJECT_ID/doordash-bot
docker push gcr.io/$PROJECT_ID/doordash-bot

# On the VM:
sudo apt-get install -y docker.io
sudo docker pull gcr.io/$PROJECT_ID/doordash-bot
sudo docker run --rm --shm-size=2g --env-file /opt/doordash-bot/.env \
    -v /opt/doordash-bot/downloads:/app/downloads \
    -v /opt/doordash-bot/logs:/app/logs \
    gcr.io/$PROJECT_ID/doordash-bot
```

---

## Monitoring & Logs

### View logs on the VM

```bash
# Latest run log
tail -f /opt/doordash-bot/logs/latest.log

# Chrome service logs
sudo journalctl -u chrome-headless -f

# App service logs (if using systemd)
sudo journalctl -u doordash-bot -f
```

### Slack notifications

Already built in. Every phase sends Slack messages:
- "Phase 1 started — Login + Reports"
- "Login successful"
- "Reports downloaded"
- "Analysis phase started"
- "[1/20] TODC-14351-$15 — done (180s)"
- "Phase 2 complete — 20 total | 18 ok | 1 failed | 1 skipped"

### Google Cloud Logging (optional)

```bash
pip install google-cloud-logging
```

Add to `main.py` after `setup_logging()`:
```python
try:
    import google.cloud.logging
    google.cloud.logging.Client().setup_logging()
except ImportError:
    pass
```

Then view in GCP Console → Logging → Logs Explorer.

---

## Troubleshooting

### Chrome crashes with OOM

```bash
# Check memory
free -h

# Increase /dev/shm
sudo mount -o remount,size=2G /dev/shm

# If persistent, upgrade VM:
# (from laptop — VM must be stopped first)
gcloud compute instances stop doordash-bot --zone=us-central1-a
gcloud compute instances set-machine-type doordash-bot \
    --machine-type=e2-standard-8 --zone=us-central1-a
gcloud compute instances start doordash-bot --zone=us-central1-a
```

### Chrome CDP not responding

```bash
sudo systemctl restart xvfb
sleep 2
sudo systemctl restart chrome-headless
sleep 3
curl -s http://localhost:9222/json/version

# If still failing, check:
sudo journalctl -u chrome-headless -n 50
```

### Login fails / CAPTCHA

If DoorDash starts showing CAPTCHAs on the cloud VM:

1. **Preserve cookies**: The `--user-data-dir=/home/chrome-profile` flag keeps session data between Chrome restarts. After one successful login, cookies persist.

2. **Use a residential proxy**: Add to Chrome flags in the systemd service:
   ```
   --proxy-server=http://YOUR_PROXY_IP:PORT
   ```

3. **Upgrade to Multilogin** (see Option B in the original guide): Anti-fingerprinting + persistent profiles significantly reduce CAPTCHA rates.

### Downloads not appearing

```bash
# Check download directory
ls -la /opt/doordash-bot/downloads/

# Chrome headless sometimes has download issues — verify CDP:
curl -s http://localhost:9222/json | python3 -m json.tool
```

### Google Sheets push fails (403)

```bash
# Get the service account email
python3 -c "import json; print(json.load(open('todc-marketing-ad02212d4f16.json'))['client_email'])"

# Make sure this email has EDITOR access to the Google Sheet
# Open the Sheet → Share → paste the email → Editor → Save
```

### VM won't shut down (scheduled mode)

```bash
# Check if the bot service is still running
sudo systemctl status doordash-bot

# Force stop
sudo systemctl stop doordash-bot

# Manual shutdown
sudo shutdown -h now
```

---

## Files Created/Modified

| File | What it does |
|------|-------------|
| `agents/doordash_agent.py` | **Modified** — `_get_browser()` now supports CDP URL from env |
| `run_browser_use.py` | **Modified** — `_get_browser()` now supports CDP URL from env |
| `.env.example` | **Updated** — added all cloud-relevant variables |
| `requirements.txt` | **Updated** — added `requests` (was missing) |
| `Dockerfile` | **New** — builds image with Chrome + Python app |
| `entrypoint.sh` | **New** — starts Xvfb, Chrome, waits for CDP, runs app |
| `.dockerignore` | **New** — excludes .env, .venv, credentials from image |
| `deploy/01-gcp-setup.sh` | **New** — automated GCP project setup (run from laptop) |
| `deploy/02-vm-setup.sh` | **New** — automated VM setup (run on VM via SSH) |
| `deploy/03-schedule.sh` | **New** — configure cron or auto start/stop |
| `deploy/04-verify.sh` | **New** — pre-flight check of all components |
| `deploy.sh` | **New** — rsync project from laptop to GCE VM (`GCP_VM_NAME`, `GCP_ZONE`, optional `--install` / `--verify`) |
| `deploy/gce-rsync-rsh.sh` | **New** — rsync `-e` helper so `gcloud compute ssh` gets `--` before remote `rsync` (fixes “command not found: INSTANCE”) |
| `git.sh` | **New** — commit and push current (or chosen) branch to `origin` |

---

## Deploy from laptop (rsync)

Use this when you develop locally and want the same tree on the VM **without** logging in to run `git pull`.

> **This project’s target VM** (Console): **`todc-ent-applications`** · zone **`us-west2-a`** · external IP shown in Compute Engine (use `gcloud compute ssh` by name; no need to type the IP). Override anytime with `GCP_VM_NAME` / `GCP_ZONE`.

### Prerequisites

- Same as [Prerequisites](#prerequisites): `gcloud` installed and authenticated.
- You can open an SSH session to the instance:  
  `gcloud compute ssh todc-ent-applications --zone=us-west2-a`
- **First time on the VM**: complete Chrome/Xvfb and Python setup (see [Step 2](#step-2-vm-setup-ssh-into-the-vm)) or run `bash deploy/02-vm-setup.sh` so `/opt/doordash-bot` exists and `.venv` + `.env` are configured.  
  `deploy.sh` creates `/opt/doordash-bot` and fixes ownership if missing, but it does **not** install Chrome or create `.env`.

### `deploy.sh` (code → GCE VM)

From the **project root** on your laptop:

| Command | What it does |
|--------|----------------|
| `./deploy.sh` | Rsync repo to the VM (default **`todc-ent-applications`** in **`us-west2-a`** → `/opt/doordash-bot`). Excludes `.git`, `.venv`, `.env`, `downloads/`, `logs/`, and `todc-marketing-*.json`. |
| `./deploy.sh --install` | After sync, runs `pip install -r requirements.txt` inside `.venv` on the VM (activates `.venv` if present). |
| `./deploy.sh --verify` | After sync, runs `deploy/04-verify.sh` on the VM. |
| `./deploy.sh --delete` | Adds `rsync --delete` so extra files under the app dir on the VM are removed. **Use sparingly**; prefer the default incremental sync so ad-hoc files on the server are kept unless you know you want a mirror. |

**Environment overrides** (optional):

| Variable | Default | Purpose |
|----------|---------|---------|
| `GCP_VM_NAME` | `todc-ent-applications` | Instance name |
| `GCP_ZONE` | `us-west2-a` | Zone |
| `GCP_REMOTE_DIR` | `/opt/doordash-bot` | Remote application path |
| `GCP_RSYNC_IAP` | unset (`0`) | Set to `1` if you use [IAP tunneling](https://cloud.google.com/iap/docs/using-tcp-forwarding) for SSH |

Example:

```bash
cd /path/to/Reporting-browser-use-claude-code
export GCP_VM_NAME=todc-ent-applications
export GCP_ZONE=us-west2-a
./deploy.sh --install
```

After deploy, run the app on the VM (SSH session):

```bash
cd /opt/doordash-bot && source .venv/bin/activate && python main.py
```

### `git.sh` (code → GitHub)

From the **project root**, commit staged changes and push to `origin` (uses your existing `origin` URL — not hard-coded).

| Command | What it does |
|--------|----------------|
| `./git.sh` | `git add -A`, prompts for commit message if needed, **push current branch** |
| `./git.sh "fix: slack notifier"` | Commit with message, push current branch |
| `./git.sh -b main "chore: release"` | Push branch `main` (ensure you have committed on that branch or are checked out to it) |
| `./git.sh --dry-run` | Show status/diff only |
| `GIT_BRANCH=main ./git.sh "msg"` | Default push target `main` via env (ensure that branch has your commits) |

For interactive first-time setup of `origin` and default branch naming, you can still use `./push_to_github.sh`.

**Typical loop**: `./git.sh "describe change"` then `./deploy.sh --install` so GitHub and the VM both match.

---

## Quick Reference

```bash
# SSH into the VM (this project’s instance)
gcloud compute ssh todc-ent-applications --zone=us-west2-a

# Start/stop VM manually
gcloud compute instances start todc-ent-applications --zone=us-west2-a
gcloud compute instances stop todc-ent-applications --zone=us-west2-a

# Run the bot manually
cd /opt/doordash-bot && source .venv/bin/activate && python main.py

# View logs
tail -f /opt/doordash-bot/logs/latest.log

# Restart Chrome
sudo systemctl restart chrome-headless

# Check Chrome CDP
curl -s http://localhost:9222/json/version

# Run verification
bash /opt/doordash-bot/deploy/04-verify.sh

# From laptop (project root): GitHub then GCE
./git.sh "your commit message"
./deploy.sh --install
```

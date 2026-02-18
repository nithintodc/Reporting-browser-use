# DoorDash Merchant Portal – Financial Report Automation

Multi-agent browser automation that logs into Gmail, extracts the DoorDash 2FA code, logs into the DoorDash merchant portal, and downloads the financial report for a given date range (e.g. Jan 1–31, 2026). Files are saved under `./downloads` and renamed to `doordash_financial_report_2026_01.csv`.

## Tech stack

- **Python 3.11+**
- **Playwright** (headed mode)
- **Async/await** end-to-end
- **python-dotenv** for credentials (no passwords in code)
- **Structured logging** to stderr

## Project structure

```
.
├── main.py                 # Orchestrator (scripted): DoorDash login + report download
├── run_browser_use.py      # Optional: AI-driven flow via browser-use (LLM controls browser)
├── push_to_github.sh       # Script to push code to GitHub
├── requirements.txt
├── .env.example            # Copy to .env and fill credentials
├── .env                    # Not committed; your credentials
├── downloads/              # Report output (created automatically)
└── agents/
    ├── __init__.py
    ├── browser_manager.py  # Playwright lifecycle, context, tabs
    ├── gmail_agent.py      # Gmail login + OTP extraction from DoorDash emails
    ├── doordash_agent.py   # Merchant login, 2FA, report creation, download
    └── report_storage_agent.py  # Verify file, rename to final CSV name
```

## Setup

1. **Clone or copy the project** and go to the project directory:
   ```bash
   cd /path/to/Reporting-browser-use
   ```

2. **Create a virtual environment** (recommended):
   ```bash
   python3.11 -m venv .venv
   source .venv/bin/activate   # Windows: .venv\Scripts\activate
   ```

3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   playwright install chromium
   ```

4. **Configure credentials** (do not commit `.env`):
   ```bash
   cp .env.example .env
   ```
   Edit `.env` and set:
   - `GMAIL_EMAIL` – Gmail address used for DoorDash 2FA
   - `GMAIL_PASSWORD` – Gmail password (or app password if 2FA is on for Gmail)
   - `DOORDASH_EMAIL` – DoorDash merchant portal email
   - `DOORDASH_PASSWORD` – DoorDash merchant portal password

## Run

### Option A: Real Chrome with a persistent profile (recommended)

Use full Chrome with a **dedicated profile** so cookies, login state, 2FA trust, and device fingerprint are kept. Log in once, then automation attaches to that session.

1. **In `.env`:**
   ```bash
   USE_LOCAL_BROWSER=true
   LOCAL_BROWSER_CDP_URL=http://localhost:9222
   DOORDASH_EMAIL=your@email.com
   DOORDASH_PASSWORD=yourpassword
   ```
   Optional: set `CHROME_USER_DATA_DIR` to a path you want (e.g. a profile you create in `chrome://profiles`). If unset, the script uses `.cursor/chrome-debug-profile` (created automatically, persistent).

2. **First time only — start Chrome with that profile and the debug port:**
   ```bash
   # Default profile (script-managed); macOS:
   /Applications/Google\ Chrome.app/Contents/MacOS/Google\ Chrome \
     --remote-debugging-port=9222 \
     --user-data-dir="$(pwd)/.cursor/chrome-debug-profile"
   ```
   Or use your own path:
   ```bash
   /Applications/Google\ Chrome.app/Contents/MacOS/Google\ Chrome \
     --remote-debugging-port=9222 \
     --user-data-dir="/path/to/your/chrome-profile"
   ```
   In that window, **log in to Gmail and DoorDash once** (and complete any 2FA). Then close Chrome when you’re done.

3. **Run the workflow:**
   ```bash
   python main.py
   ```
   If nothing is listening on 9222, the script **auto-starts Chrome** with the same profile (`CHROME_USER_DATA_DIR` or `.cursor/chrome-debug-profile`), so your logins are reused. The script attaches to that Chrome and uses the existing Gmail session for 2FA codes.

**Why this works:** One persistent `--user-data-dir` keeps cookies, login state, and 2FA trust. Automation connects via `--remote-debugging-port=9222` and reuses that session, avoiding most login and “Verify it’s you” failures.

### Option B: Fresh Playwright browser

1. In `.env`, leave `USE_LOCAL_BROWSER` unset or `false`, and set all four credentials (Gmail + DoorDash).
2. Run:
   ```bash
   python main.py
   ```

- The browser runs in **headed mode** so you can watch the flow.
- The script will:
  1. (Option A) Attach to your Chrome and open Gmail in a new tab; (Option B) start a new browser and log into Gmail.
  2. Wait for the inbox (or use existing session).
  3. Open the DoorDash merchant portal in a new tab and log in.
  4. If 2FA appears, poll Gmail (every 5s, up to 60s) for a DoorDash email and use the 6-digit OTP.
  5. Go to Reports → Create report → Financial report → set dates (01/01/2026–01/31/2026) → Create report.
  6. Wait 30 seconds for the report, then click Download.
  7. Save the file under `./downloads` and rename it to `doordash_financial_report_2026_01.csv`.

### Option C: AI-driven control (browser-use)

For more flexible, LLM-driven control (e.g. if the portal UI changes or you want the agent to adapt):

1. Install deps: `pip install -r requirements.txt` (includes `browser-use`).
2. Set in `.env`: `OPENAI_API_KEY` or `BROWSER_USE_API_KEY`, plus `DOORDASH_EMAIL` and `DOORDASH_PASSWORD`. Optional: `CHROME_USER_DATA_DIR` for a persistent profile.
3. Close all Chrome windows, then run:
   ```bash
   python run_browser_use.py
   ```
   The browser-use agent will open Chrome, log in, navigate to Reports, create the financial report for Jan 1–31 2026, and download it. Uses the same persistent profile as Option A when `CHROME_USER_DATA_DIR` is set.

## Behavior and robustness

- **Waits**: Uses Playwright `wait_for_selector` / `wait_for_load_state` (no fixed sleeps except the required 30s after creating the report).
- **Retries**: Up to 3 attempts with a 5-second delay between them.
- **Logging**: Timestamp, level, logger name, and message to stderr.
- **Security**: Credentials only in `.env`; `.env` should be in `.gitignore`.

## Requirements

- Gmail and DoorDash merchant portal accounts.
- DoorDash 2FA emails sent to the same Gmail account (script looks for a 6-digit code in the latest DoorDash email).
- **Option A (local browser):** Chrome (or Chromium) started with `--remote-debugging-port=9222`; Gmail and optionally DoorDash already logged in there.
- **Option B (fresh browser):** If Gmail has its own 2FA, use an [App Password](https://support.google.com/accounts/answer/185833) for `GMAIL_PASSWORD`.

## Push to GitHub

To push this project to [https://github.com/nithintodc/Reporting-browser-use](https://github.com/nithintodc/Reporting-browser-use):

```bash
./push_to_github.sh
```

Optional: pass a branch name, e.g. `./push_to_github.sh main`. The script will init git (if needed), add `origin`, stage files (respecting `.gitignore`), commit, and push. Your `.env` and `.venv` are not committed.

## License

Use and modify as needed for your environment.

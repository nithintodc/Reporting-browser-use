# DoorDash Merchant Portal – Report Automation

Browser-use (LLM-driven) automation for the DoorDash Merchant Portal: login, create financial and marketing reports, download them, and create a marketing campaign. Analysis agents (AnalysisAgent, MarketingAgent, combined report, Google Sheets push) run on the downloaded files unchanged.

## Tech stack

- **Python 3.11+**
- **browser-use** – AI-driven browser control (OpenAI or Browser Use Cloud)
- **Async/await** end-to-end
- **python-dotenv** for credentials (no passwords in code)
- **Structured logging** to stderr

## Project structure

```
.
├── main.py                 # Orchestrator: browser-use DoorDash flow + analysis agents
├── run_browser_use.py      # Standalone browser-use run (simpler task)
├── run.sh                  # One-step run: venv + deps + main.py (macOS/Linux)
├── run.bat                 # One-step run (Windows)
├── requirements.txt
├── .env.example            # Copy to .env and fill credentials
├── .env                    # Not committed; your credentials
├── downloads/              # Report output (created automatically)
└── agents/
    ├── doordash_agent.py   # Browser-use: login, reports, download, campaign
    ├── analysis_agent.py   # Financial report analysis
    ├── marketing_agent.py  # Marketing report analysis
    ├── combined_report_agent.py
    ├── google_pusher_agent.py
    └── report_storage_agent.py
```

## Setup

1. **Clone or copy the project** and go to the project directory.

2. **Create a virtual environment** (recommended):
   ```bash
   python3.11 -m venv .venv
   source .venv/bin/activate   # Windows: .venv\Scripts\activate
   ```

3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure credentials** (do not commit `.env`):
   ```bash
   cp .env.example .env
   ```
   Edit `.env` and set:
   - `DOORDASH_EMAIL` – DoorDash merchant portal email
   - `DOORDASH_PASSWORD` – DoorDash merchant portal password
   - **One of:** `OPENAI_API_KEY` (recommended) or `BROWSER_USE_API_KEY` – for browser-use LLM

## Run

**Quick start (all-in-one):**

```bash
./run.sh              # Activate venv, install deps if needed, run main.py
./run.sh browser-use   # Run AI-driven flow (run_browser_use.py)
./run.sh install      # Only create venv + install deps; run later with ./run.sh
```

On Windows: `run.bat`, `run.bat browser-use`, `run.bat install`.

---

### Running the main flow

1. Set in `.env`: **`OPENAI_API_KEY`** (recommended) or `BROWSER_USE_API_KEY`, plus `DOORDASH_EMAIL` and `DOORDASH_PASSWORD`.

2. (Optional) If you see "Failed to connect to browser-use API" or DNS errors, use `OPENAI_API_KEY` — OpenAI is reachable on most networks; Browser Use Cloud may be blocked.

3. **Run the workflow:**
   ```bash
   python main.py
   ```
   The browser-use agent will open a browser, log in to the DoorDash merchant portal, create financial and marketing reports, download them, and create the campaign. Downloaded files are then processed by the analysis and marketing agents; results are combined and optionally pushed to Google Sheets.


### Standalone browser-use (simpler task)

For a minimal run (login + single financial report + download):

```bash
python run_browser_use.py
```

## Behavior and robustness

- **Retries**: Up to 3 attempts with a 5-second delay between them.
- **Logging**: Timestamp, level, logger name, and message to stderr.
- **Security**: Credentials only in `.env`; `.env` should be in `.gitignore`.

## Requirements

- DoorDash merchant portal account.
- One of: `OPENAI_API_KEY` or `BROWSER_USE_API_KEY` for the browser-use LLM.

## Push to GitHub

To push this project to [https://github.com/nithintodc/Reporting-browser-use](https://github.com/nithintodc/Reporting-browser-use):

```bash
./push_to_github.sh
```

Optional: pass a branch name, e.g. `./push_to_github.sh main`. The script will init git (if needed), add `origin`, stage files (respecting `.gitignore`), commit, and push. Your `.env` and `.venv` are not committed.

## License

Use and modify as needed for your environment.

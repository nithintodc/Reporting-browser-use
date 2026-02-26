@echo off
REM One script to run the app: activate venv, then run the DoorDash report workflow.
REM Usage:
REM   run.bat          - Run scripted flow (main.py)
REM   run.bat browser-use  - Run AI-driven flow (run_browser_use.py)
REM   run.bat install  - Only create venv + install deps (no run)

cd /d "%~dp0"

if not exist .venv (
  echo Creating .venv...
  python -m venv .venv
)

echo Activating venv...
call .venv\Scripts\activate.bat

python -c "import browser_use" 2>nul || (
  echo Installing dependencies...
  pip install -r requirements.txt
)

if not exist .env (
  echo Warning: .env not found. Copy .env.example to .env and set credentials.
  echo   copy .env.example .env
  pause
)

if "%1"=="install" (
  echo Install complete. Run run.bat to start the app.
  exit /b 0
)

if "%1"=="browser-use" (
  echo Running browser-use (AI-driven)...
  python run_browser_use.py
) else (
  echo Running main flow (scripted DoorDash report)...
  python main.py
)

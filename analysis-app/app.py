"""
Streamlit Cloud entry point.
Delegates to the main app in the app/ folder so the repo structure matches
Streamlit Cloud's expectation of app.py at root.
"""
import sys
from pathlib import Path

# Add app folder to path so imports work when we load app/app.py
app_dir = Path(__file__).resolve().parent / "app"
sys.path.insert(0, str(app_dir))

# Load and run the actual Streamlit app
import importlib.util
spec = importlib.util.spec_from_file_location("app_main", app_dir / "app.py")
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

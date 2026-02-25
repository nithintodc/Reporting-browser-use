"""
GooglePusherAgent: push the final financial and marketing analysis Excel reports
into a single Google Sheets file as separate sheets.

Uses the same credential pattern as analysis-app gdrive_utils:
- GCP_SERVICE_ACCOUNT_JSON (env, JSON string), or
- GOOGLE_APPLICATION_CREDENTIALS / GCP_CREDENTIALS_PATH (path to JSON), or
- default: analysis-app/app/todc-marketing-*.json

Requires Google Sheets API scope. Creates one spreadsheet with one tab per
Excel sheet (financial sheets first, then marketing sheets).
"""

import json
import logging
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# Sheet title max length (Google Sheets limit)
SHEET_TITLE_MAX_LEN = 100
# Characters not allowed in sheet titles
SHEET_TITLE_FORBIDDEN = re.compile(r'[*?\:/\\\[\]]')


def _load_credentials():
    """Load service account credentials (same pattern as analysis-app gdrive_utils, no Streamlit)."""
    from google.oauth2 import service_account

    credentials_info = None

    # 1) Environment: JSON string
    if os.environ.get("GCP_SERVICE_ACCOUNT_JSON"):
        try:
            credentials_info = json.loads(os.environ["GCP_SERVICE_ACCOUNT_JSON"])
        except (json.JSONDecodeError, KeyError):
            pass

    # 2) File path from env
    credentials_path = None
    if credentials_info is None:
        credentials_path = os.environ.get("GCP_CREDENTIALS_PATH") or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if credentials_info is None and credentials_path:
        credentials_path = Path(credentials_path)
        if credentials_path.exists():
            return service_account.Credentials.from_service_account_file(
                str(credentials_path),
                scopes=[
                    "https://www.googleapis.com/auth/drive",
                    "https://www.googleapis.com/auth/spreadsheets",
                ],
            )
        logger.warning("GooglePusherAgent: Credentials file not found at %s", credentials_path)

    # 3) Use credentials from JSON env
    if credentials_info is not None:
        return service_account.Credentials.from_service_account_info(
            credentials_info,
            scopes=[
                "https://www.googleapis.com/auth/drive",
                "https://www.googleapis.com/auth/spreadsheets",
            ],
        )

    # 4) Project root: todc-marketing-*.json (e.g. todc-marketing-ad02212d4f16.json)
    project_root = Path(__file__).resolve().parent.parent
    for f in project_root.glob("todc-marketing-*.json"):
        if f.is_file():
            return service_account.Credentials.from_service_account_file(
                str(f),
                scopes=[
                    "https://www.googleapis.com/auth/drive",
                    "https://www.googleapis.com/auth/spreadsheets",
                ],
            )

    # 5) analysis-app/app/todc-marketing-*.json
    app_dir = project_root / "analysis-app" / "app"
    if app_dir.is_dir():
        for f in app_dir.glob("todc-marketing-*.json"):
            return service_account.Credentials.from_service_account_file(
                str(f),
                scopes=[
                    "https://www.googleapis.com/auth/drive",
                    "https://www.googleapis.com/auth/spreadsheets",
                ],
            )

    raise FileNotFoundError(
        "Google Sheets credentials not found. Set GCP_SERVICE_ACCOUNT_JSON (JSON string), "
        "GCP_CREDENTIALS_PATH or GOOGLE_APPLICATION_CREDENTIALS (path to JSON), or place "
        "todc-marketing-*.json in the project root or analysis-app/app."
    )


def _sanitize_sheet_title(title: str) -> str:
    """Return a sheet title safe for Google Sheets (length and forbidden chars)."""
    s = SHEET_TITLE_FORBIDDEN.sub(" ", title).strip() or "Sheet"
    return s[:SHEET_TITLE_MAX_LEN]


def _excel_to_sheet_data(excel_path: Path) -> Dict[str, List[List[Any]]]:
    """
    Read an Excel file and return a dict mapping sheet name -> list of rows (each row is list of cell values).
    """
    try:
        import pandas as pd
    except ImportError:
        raise RuntimeError("pandas is required for GooglePusherAgent. Install with: pip install pandas")
    try:
        import openpyxl
    except ImportError:
        raise RuntimeError("openpyxl is required. Install with: pip install openpyxl")

    excel_path = Path(excel_path)
    if not excel_path.is_file():
        return {}
    out = {}
    xl = pd.ExcelFile(excel_path)
    for sheet_name in xl.sheet_names:
        df = pd.read_excel(xl, sheet_name=sheet_name, header=None)
        # Convert to list of lists; replace NaN with empty string
        rows = df.fillna("").astype(str).values.tolist()
        out[sheet_name] = rows
    return out


def _build_combined_sheets(
    financial_xlsx: Optional[Path],
    marketing_xlsx: Optional[Path],
) -> Tuple[List[str], Dict[str, List[List[Any]]]]:
    """
    Build ordered list of sheet titles and map title -> rows.
    Financial sheets first, then marketing. Sheet names are sanitized and de-duplicated.
    """
    seen = set()
    order = []
    data: Dict[str, List[List[Any]]] = {}

    def add(name: str, rows: List[List[Any]]) -> None:
        safe = _sanitize_sheet_title(name)
        if not safe or not rows:
            return
        # De-duplicate: if same title exists, append suffix
        key = safe
        cnt = 0
        while key in seen:
            cnt += 1
            key = f"{safe[:SHEET_TITLE_MAX_LEN - 4]}_{cnt}"[:SHEET_TITLE_MAX_LEN]
        seen.add(key)
        order.append(key)
        data[key] = rows

    if financial_xlsx and financial_xlsx.is_file():
        for sheet_name, rows in _excel_to_sheet_data(financial_xlsx).items():
            add(sheet_name, rows)
    if marketing_xlsx and marketing_xlsx.is_file():
        for sheet_name, rows in _excel_to_sheet_data(marketing_xlsx).items():
            add(sheet_name, rows)

    return order, data


def push_to_sheets(
    financial_xlsx_path: Optional[Path] = None,
    marketing_xlsx_path: Optional[Path] = None,
    spreadsheet_title: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """
    Create one Google Sheets file with all sheets from the financial and marketing Excel files.

    Args:
        financial_xlsx_path: Path to financial_analysis_*.xlsx
        marketing_xlsx_path: Path to marketing_analysis_*.xlsx
        spreadsheet_title: Title for the new spreadsheet (default: "DoorDash Reports YYYY-MM-DD HH:MM")

    Returns:
        Dict with spreadsheet_id, spreadsheet_url, sheet_count; or None if no data or on error.
    """
    from datetime import datetime
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError

    order, data = _build_combined_sheets(
        Path(financial_xlsx_path) if financial_xlsx_path else None,
        Path(marketing_xlsx_path) if marketing_xlsx_path else None,
    )
    if not order or not data:
        logger.warning("GooglePusherAgent: No sheet data to push (missing or empty Excel files)")
        return None

    try:
        creds = _load_credentials()
    except FileNotFoundError as e:
        logger.warning("GooglePusherAgent: Skipping push - %s", e)
        return None

    sheets_service = build("sheets", "v4", credentials=creds)

    if not spreadsheet_title:
        spreadsheet_title = f"DoorDash Reports {datetime.now().strftime('%Y-%m-%d %H:%M')}"

    # Create spreadsheet with one sheet per tab (first sheet is default)
    sheet_properties = [{"properties": {"title": _sanitize_sheet_title(title)}} for title in order]
    body = {
        "properties": {"title": spreadsheet_title[:SHEET_TITLE_MAX_LEN]},
        "sheets": sheet_properties,
    }
    try:
        create_res = sheets_service.spreadsheets().create(body=body).execute()
    except HttpError as e:
        logger.warning("GooglePusherAgent: Failed to create spreadsheet: %s", e)
        return None

    spreadsheet_id = create_res["spreadsheetId"]
    spreadsheet_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit"

    # Write data to each sheet
    value_ranges = []
    for title in order:
        rows = data.get(title, [])
        if not rows:
            continue
        # Sheet name in range: quote if it contains spaces/special
        safe_title = _sanitize_sheet_title(title)
        range_name = f"'{safe_title}'!A1"
        value_ranges.append({"range": range_name, "values": rows})

    if value_ranges:
        try:
            sheets_service.spreadsheets().values().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={"valueInputOption": "USER_ENTERED", "data": value_ranges},
            ).execute()
        except HttpError as e:
            logger.warning("GooglePusherAgent: Failed to write sheet values: %s", e)
            # Spreadsheet was created; still return link
    logger.info("GooglePusherAgent: Pushed %s sheets to %s", len(order), spreadsheet_url)
    return {
        "spreadsheet_id": spreadsheet_id,
        "spreadsheet_url": spreadsheet_url,
        "sheet_count": len(order),
    }


def run(
    financial_xlsx_path: Optional[Path] = None,
    marketing_xlsx_path: Optional[Path] = None,
    spreadsheet_title: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """
    Push financial and marketing analysis reports to a single Google Sheets file.
    Returns dict with spreadsheet_id, spreadsheet_url, sheet_count; or None.
    """
    return push_to_sheets(
        financial_xlsx_path=financial_xlsx_path,
        marketing_xlsx_path=marketing_xlsx_path,
        spreadsheet_title=spreadsheet_title,
    )

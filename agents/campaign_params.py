"""
Load campaign parameters from the combined analysis Excel file.

Reads "Day-Slot - {storeID}" sheets to get:
- store_id from the sheet name
- Day, Slot, Min.Subtotal from each data row (columns Day, Slot, Min.Subtotal).

Provides both single-params (first row) and all combos for looping campaigns.

Also writes campaigns_executed.csv in the run directory to log each campaign setup.
"""

import csv
import logging
import re
from pathlib import Path
from typing import List, Optional

# CSV filename and columns for campaign execution log (in run_dir, e.g. downloads/email-timestamp/)
CAMPAIGNS_EXECUTED_CSV = "campaigns_executed.csv"
CAMPAIGNS_EXECUTED_COLUMNS = [
    "StoreID",
    "Campaign Name",
    "%value",
    "Min.Subtotal value",
    "Maximum discount value",
    "Status",
]

logger = logging.getLogger(__name__)

try:
    import pandas as pd
except ImportError:
    pd = None


# Day-Slot sheet name pattern: "Day-Slot - 14351" or "Financial - Day-Slot - 14351"
DAY_SLOT_SHEET_PREFIX = "Day-Slot - "
DAY_SLOT_SHEET_PATTERN = re.compile(r"Day-Slot\s*-\s*(.+)", re.IGNORECASE)


def get_campaign_params_from_combined_analysis(combined_xlsx_path: Path) -> Optional[dict]:
    """
    Read the first Day-Slot - {storeID} sheet from combined_analysis_*.xlsx.

    Returns a dict with:
        store_id: str (e.g. "14351")
        day: str (e.g. "Wednesday")
        slot: str (e.g. "Lunch")
        min_subtotal: float (e.g. 20.0)
        campaign_name: str (e.g. "14351-Lunch-Wednesday")

    Returns None if file missing, no matching sheet, or required columns/row missing.
    """
    if pd is None:
        logger.warning("campaign_params: pandas required to read combined analysis")
        return None

    path = Path(combined_xlsx_path)
    if not path.is_file() or path.suffix.lower() not in (".xlsx", ".xls"):
        logger.warning("campaign_params: combined analysis path is not a valid Excel file: %s", path)
        return None

    try:
        xl = pd.ExcelFile(path)
    except Exception as e:
        logger.warning("campaign_params: could not open Excel file %s: %s", path, e)
        return None

    # Find first sheet whose name contains "Day-Slot - " and extract store_id
    target_sheet = None
    store_id = None
    for name in xl.sheet_names:
        if "Day-Slot - " in name or DAY_SLOT_SHEET_PATTERN.search(name):
            target_sheet = name
            match = DAY_SLOT_SHEET_PATTERN.search(name)
            if match:
                store_id = match.group(1).strip()
            else:
                # "Day-Slot - 14351" -> take after "Day-Slot - "
                idx = name.find("Day-Slot - ")
                if idx >= 0:
                    store_id = name[idx + len("Day-Slot - "):].strip()
            if store_id:
                break

    if not target_sheet or not store_id:
        logger.warning("campaign_params: no 'Day-Slot - {storeID}' sheet found in %s", path.name)
        return None

    # Combined report writes title at row 1, header at row 3 (0-indexed: header=2)
    try:
        df = pd.read_excel(xl, sheet_name=target_sheet, header=2)
    except Exception as e:
        logger.warning("campaign_params: could not read sheet %s: %s", target_sheet, e)
        return None

    # Normalize column names (strip whitespace)
    df.columns = df.columns.astype(str).str.strip()

    required = ["Day", "Slot", "Min.Subtotal"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        logger.warning("campaign_params: sheet %s missing columns: %s (have: %s)", target_sheet, missing, list(df.columns))
        return None

    # First data row (skip header); drop rows where Day or Slot is NaN
    data = df.dropna(subset=["Day", "Slot"]).head(1)
    if data.empty:
        logger.warning("campaign_params: sheet %s has no data rows with Day and Slot", target_sheet)
        return None

    row = data.iloc[0]
    day = str(row["Day"]).strip()
    slot = str(row["Slot"]).strip()
    min_val = row["Min.Subtotal"]

    # Parse Min.Subtotal: may be number or string like "$20.00"
    try:
        if pd.isna(min_val):
            min_subtotal = 20.0
        elif isinstance(min_val, (int, float)):
            min_subtotal = float(min_val)
        else:
            s = str(min_val).strip().replace("$", "").replace(",", "")
            min_subtotal = float(s) if s else 20.0
    except (ValueError, TypeError):
        min_subtotal = 20.0

    if min_subtotal <= 0:
        min_subtotal = 20.0

    # Campaign name: e.g. TODC-14351-Wednesday-Lunch
    campaign_name = f"TODC-{store_id}-{day}-{slot}"

    return {
        "store_id": store_id,
        "day": day,
        "slot": slot,
        "min_subtotal": min_subtotal,
        "campaign_name": campaign_name,
    }


def get_all_campaign_combos_from_combined_analysis(combined_xlsx_path: Path) -> List[dict]:
    """
    Read all "Day-Slot - {storeID}" sheets and yield every (store_id, day, slot, min_subtotal) combo.

    Returns list of dicts:
        store_id: str
        day: str (e.g. "Monday")
        slot: str (e.g. "Breakfast")
        min_subtotal: float
        campaign_name: str (TODC-{store_id}-{Day}-{Slot})

    Sheet names may be "Day-Slot - {StoreID}" or "Financial - Day-Slot - {StoreID}". Header is at row 3 (0-indexed 2).
    """
    if pd is None:
        logger.warning("campaign_params: pandas required")
        return []

    path = Path(combined_xlsx_path)
    if not path.is_file() or path.suffix.lower() not in (".xlsx", ".xls"):
        logger.warning("campaign_params: not a valid Excel file: %s", path)
        return []

    try:
        xl = pd.ExcelFile(path)
    except Exception as e:
        logger.warning("campaign_params: could not open %s: %s", path, e)
        return []

    combos: List[dict] = []
    for sheet_name in xl.sheet_names:
        if "Day-Slot - " not in sheet_name and not DAY_SLOT_SHEET_PATTERN.search(sheet_name):
            continue
        match = DAY_SLOT_SHEET_PATTERN.search(sheet_name)
        if match:
            store_id = match.group(1).strip()
        else:
            idx = sheet_name.find("Day-Slot - ")
            store_id = (sheet_name[idx + len("Day-Slot - "):].strip() if idx >= 0 else "")
        if not store_id:
            continue

        try:
            df = pd.read_excel(xl, sheet_name=sheet_name, header=2)
        except Exception as e:
            logger.debug("campaign_params: skip sheet %s: %s", sheet_name, e)
            continue

        df.columns = df.columns.astype(str).str.strip()
        required = ["Day", "Slot", "Min.Subtotal"]
        if any(c not in df.columns for c in required):
            continue

        data = df.dropna(subset=["Day", "Slot"])
        for _, row in data.iterrows():
            day = str(row["Day"]).strip()
            slot = str(row["Slot"]).strip()
            min_val = row["Min.Subtotal"]
            try:
                if pd.isna(min_val):
                    min_subtotal = 20.0
                elif isinstance(min_val, (int, float)):
                    min_subtotal = float(min_val)
                else:
                    s = str(min_val).strip().replace("$", "").replace(",", "")
                    min_subtotal = float(s) if s else 20.0
            except (ValueError, TypeError):
                min_subtotal = 20.0
            if min_subtotal <= 0:
                min_subtotal = 20.0

            campaign_name = f"TODC-{store_id}-{day}-{slot}"
            combos.append({
                "store_id": store_id,
                "day": day,
                "slot": slot,
                "min_subtotal": min_subtotal,
                "campaign_name": campaign_name,
            })

    logger.info("campaign_params: found %s campaign combos in %s", len(combos), path.name)
    return combos


def get_campaigns_executed_path(run_dir: Path) -> Path:
    """Return the path to campaigns_executed.csv inside run_dir."""
    return Path(run_dir) / CAMPAIGNS_EXECUTED_CSV


def ensure_campaigns_executed_csv(run_dir: Path) -> Path:
    """
    Create campaigns_executed.csv in run_dir with header if it does not exist.
    Returns the path to the CSV file.
    """
    path = get_campaigns_executed_path(run_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(CAMPAIGNS_EXECUTED_COLUMNS)
        logger.info("campaign_params: created %s", path)
    return path


def log_campaign_executed(
    run_dir: Path,
    store_id: str,
    campaign_name: str,
    pct_value: int = 15,
    min_subtotal: float = 10,
    max_discount: str = "Always lowest",
    status: str = "Completed",
) -> None:
    """
    Append one row to campaigns_executed.csv in run_dir.
    Call after each campaign is executed (or with status="Failed" on error).
    """
    path = get_campaigns_executed_path(run_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    file_exists = path.exists()
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if not file_exists:
            w.writerow(CAMPAIGNS_EXECUTED_COLUMNS)
        w.writerow([store_id, campaign_name, pct_value, min_subtotal, max_discount, status])
    logger.debug("campaign_params: logged campaign %s -> %s", campaign_name, path)

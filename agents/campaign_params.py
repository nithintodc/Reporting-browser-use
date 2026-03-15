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

# slots.csv = grid: rows = slots, columns = days, cell values = tag (1..42). Day-Slot -> Tag.
# Per-store subtotal -> tags are derived from Day-Slot - {Store ID} sheets (Day, Slot, Min.Subtotal) + slots grid.


# Day names in sheets are full (Monday, Tuesday); slots.csv uses short (Mon, Tue, ...)
_DAY_TO_GRID = {
    "monday": "Mon", "tue": "Tue", "tuesday": "Tue", "wed": "Wed", "wednesday": "Wed",
    "thu": "Thur", "thurs": "Thur", "thursday": "Thur", "fri": "Fri", "friday": "Fri",
    "sat": "Sat", "saturday": "Sat", "sun": "Sun", "sunday": "Sun",
    "mon": "Mon",
}


def _day_to_grid_key(day_str: str) -> str:
    """Normalize sheet Day (e.g. Monday) to slots grid column (e.g. Mon)."""
    if not day_str:
        return ""
    k = day_str.strip().lower()
    return _DAY_TO_GRID.get(k, day_str.strip()[:3] if len(day_str) >= 3 else day_str.strip())


def _parse_tag(val) -> Optional[int]:
    """Parse a cell value to tag int; return None if empty/invalid."""
    if val is None:
        return None
    if isinstance(val, int):
        return val if val >= 0 else None
    s = str(val).strip()
    if s in ("", "nan", "None"):
        return None
    try:
        return int(float(s))
    except (ValueError, TypeError):
        return None


def load_slots_grid(slots_path: Path) -> dict:
    """
    Load slots.csv as grid: rows = slots, columns = days, values = tag numbers.

    First row: first cell empty (or slot header), rest = day names (Mon, Tue, ...).
    Remaining rows: first column = slot name (Early morning, Breakfast, ...), rest = tag ints.
    Returns dict mapping (day, slot) -> tag (int). Example: ("Mon", "Early morning") -> 1.
    Returns {} if file missing or invalid.
    """
    path = Path(slots_path)
    if not path.is_file():
        logger.warning("campaign_params: slots.csv not found at %s", path)
        return {}

    try:
        with open(path, "r", newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            rows = list(reader)
    except Exception as e:
        logger.warning("campaign_params: could not read slots.csv %s: %s", path, e)
        return {}

    if len(rows) < 2:
        return {}

    # Row 0: header. First cell empty or label, rest = day names
    header = rows[0]
    days = [str(h).strip() for h in header[1:] if str(h).strip()]
    if not days:
        logger.warning("campaign_params: slots.csv has no day columns")
        return {}

    result: dict = {}
    for row in rows[1:]:
        if not row:
            continue
        slot = str(row[0]).strip()
        if not slot:
            continue
        for j, day in enumerate(days):
            if j + 1 >= len(row):
                break
            tag = _parse_tag(row[j + 1])
            if tag is not None:
                result[(day, slot)] = tag

    return result


def load_subtotal_tags(subtotal_tags_path: Path) -> dict:
    """
    Load subtotal -> tags mapping from CSV.

    Expected columns (any case): Minimum Subtotal (or Min Subtotal), Tags (comma-separated numbers).
    Returns dict: { min_subtotal: [tag1, tag2, ...] }. Example: 15 -> [1, 5, 7, 8].
    Returns {} if file missing or invalid.
    """
    path = Path(subtotal_tags_path)
    if not path.is_file():
        logger.warning("campaign_params: subtotal_tags.csv not found at %s", path)
        return {}

    try:
        with open(path, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
    except Exception as e:
        logger.warning("campaign_params: could not read subtotal_tags.csv %s: %s", path, e)
        return {}

    if not rows:
        return {}

    # Detect columns: min subtotal and tags
    first = rows[0]
    min_col = None
    tags_col = None
    for key in first.keys():
        k = key.strip().lower().replace(" ", "").replace("_", "")
        if "minsubtotal" in k or "minimumsubtotal" in k or (k == "subtotal"):
            min_col = key
        if k == "tags":
            tags_col = key
    if not min_col or not tags_col:
        logger.warning("campaign_params: subtotal_tags.csv needs 'Minimum Subtotal' and 'Tags'. Have: %s", list(first.keys()))
        return {}

    result: dict = {}
    for row in rows:
        try:
            min_val = row.get(min_col)
            if min_val is None or str(min_val).strip() in ("", "nan"):
                continue
            min_subtotal = int(round(float(str(min_val).strip().replace("$", "").replace(",", ""))))
        except (ValueError, TypeError):
            continue
        raw = row.get(tags_col) or ""
        tags = []
        for part in str(raw).replace("，", ",").split(","):
            t = _parse_tag(part.strip())
            if t is not None and t not in tags:
                tags.append(t)
        if tags:
            result[min_subtotal] = sorted(tags)

    return result


def _load_store_id_to_name(xl) -> dict:
    """
    Read Store-wise sheet from combined analysis workbook to build store_id → store_name mapping.
    Looks for columns 'Merchant Store ID' and 'Store Name' (header at row 3, 0-indexed row 2).
    Returns empty dict if sheet/columns not found.
    """
    if pd is None:
        return {}
    mapping = {}
    for sheet_name in xl.sheet_names:
        if sheet_name.lower().replace("-", "").replace(" ", "") in ("storewise", "financialstorewise"):
            try:
                df = pd.read_excel(xl, sheet_name=sheet_name, header=2)
                df.columns = df.columns.astype(str).str.strip()
                id_col = None
                name_col = None
                for c in df.columns:
                    cl = c.lower()
                    if cl in ("merchant store id", "store id"):
                        id_col = c
                    elif cl == "store name":
                        name_col = c
                if id_col and name_col:
                    for _, row in df.dropna(subset=[id_col]).iterrows():
                        sid = str(row[id_col]).strip()
                        sname = str(row[name_col]).strip() if pd.notna(row[name_col]) else ""
                        if sid and sname:
                            mapping[sid] = sname
                    logger.info("campaign_params: loaded %d store ID→name mappings from %s", len(mapping), sheet_name)
                    break
            except Exception as e:
                logger.debug("campaign_params: could not read store names from %s: %s", sheet_name, e)
    return mapping


def get_store_ids_from_combined_analysis(combined_xlsx_path: Path) -> List[str]:
    """
    Read combined analysis workbook and return list of store IDs from Day-Slot sheet names.

    Sheet names like "Day-Slot - 14351" or "Financial - Day-Slot - 14351". Returns unique store IDs.
    """
    if pd is None:
        return []

    path = Path(combined_xlsx_path)
    if not path.is_file() or path.suffix.lower() not in (".xlsx", ".xls"):
        return []

    try:
        xl = pd.ExcelFile(path)
    except Exception:
        return []

    store_ids: List[str] = []
    for name in xl.sheet_names:
        if "Day-Slot - " not in name and not DAY_SLOT_SHEET_PATTERN.search(name):
            continue
        match = DAY_SLOT_SHEET_PATTERN.search(name)
        if match:
            sid = match.group(1).strip()
        else:
            idx = name.find("Day-Slot - ")
            sid = (name[idx + len("Day-Slot - "):].strip() if idx >= 0 else "")
        if sid and sid not in store_ids:
            store_ids.append(sid)

    return store_ids


def get_subtotal_to_tags_per_store_from_combined(
    combined_xlsx_path: Path,
    slots_grid: dict,
) -> dict:
    """
    Derive per-store subtotal -> list of tags from Day-Slot - {Store ID} sheets + slots grid.

    For each sheet: read Day, Slot, Min.Subtotal. Group by Min.Subtotal; for each (Day, Slot)
    look up tag in slots_grid (day normalized to grid key, e.g. Monday -> Mon). Build
    store_id -> { min_subtotal -> [tag, ...] }.
    slots_grid: (day_key, slot) -> tag, e.g. ("Mon", "Early morning") -> 1.
    Returns {} if pandas missing or file invalid.
    """
    if pd is None:
        logger.warning("campaign_params: pandas required to read combined analysis")
        return {}

    path = Path(combined_xlsx_path)
    if not path.is_file() or path.suffix.lower() not in (".xlsx", ".xls"):
        return {}

    try:
        xl = pd.ExcelFile(path)
    except Exception as e:
        logger.warning("campaign_params: could not open %s: %s", path, e)
        return {}

    result: dict = {}
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

        if store_id not in result:
            result[store_id] = {}

        data = df.dropna(subset=["Day", "Slot"])
        for _, row in data.iterrows():
            day = str(row["Day"]).strip()
            slot = str(row["Slot"]).strip()
            min_val = row["Min.Subtotal"]
            try:
                if pd.isna(min_val):
                    continue
                if isinstance(min_val, (int, float)):
                    min_subtotal = int(round(float(min_val)))
                else:
                    s = str(min_val).strip().replace("$", "").replace(",", "")
                    min_subtotal = int(round(float(s))) if s else 0
            except (ValueError, TypeError):
                continue
            if min_subtotal <= 0:
                continue

            day_key = _day_to_grid_key(day)
            tag = slots_grid.get((day_key, slot))
            if tag is None:
                tag = slots_grid.get((day, slot))
            if tag is not None:
                if min_subtotal not in result[store_id]:
                    result[store_id][min_subtotal] = []
                if tag not in result[store_id][min_subtotal]:
                    result[store_id][min_subtotal].append(tag)

    for store_id in result:
        for min_subtotal in result[store_id]:
            result[store_id][min_subtotal] = sorted(result[store_id][min_subtotal])

    return result


def get_campaign_combos_from_slots_and_combined(
    slots_path: Path,
    combined_xlsx_path: Path,
) -> List[dict]:
    """
    Build one campaign per (store_id, min_subtotal) by deriving subtotal->tags from data.

    Reads Day-Slot - {Store ID} sheets (Day, Slot, Min.Subtotal) and slots.csv grid (day, slot -> tag).
    For each store: groups rows by Min.Subtotal and maps (Day, Slot) to tags via the grid.
    Returns list of dicts: store_id, min_subtotal, slot_tags (list of int), campaign_name (TODC-{StoreID}-${min_subtotal}).
    """
    path = Path(slots_path)
    if not path.is_file():
        logger.warning("campaign_params: slots.csv required for slots-based combos")
        return []

    slots_grid = load_slots_grid(path)
    if not slots_grid:
        logger.warning("campaign_params: slots grid empty")
        return []

    combined_path = Path(combined_xlsx_path)
    if not combined_path.is_file() or combined_path.suffix.lower() not in (".xlsx", ".xls"):
        logger.warning("campaign_params: combined analysis path required")
        return []

    per_store = get_subtotal_to_tags_per_store_from_combined(combined_path, slots_grid)
    if not per_store:
        logger.warning("campaign_params: no subtotal->tags derived from Day-Slot sheets")
        return []

    # Load store ID → name mapping for fallback search in campaign creation
    try:
        xl = pd.ExcelFile(combined_path)
        store_id_to_name = _load_store_id_to_name(xl)
    except Exception:
        store_id_to_name = {}

    combos: List[dict] = []
    for store_id, subtotal_to_tags in per_store.items():
        for min_subtotal, tags in subtotal_to_tags.items():
            if not tags:
                continue
            campaign_name = f"TODC-{store_id}-${min_subtotal}"
            combo = {
                "store_id": store_id,
                "min_subtotal": min_subtotal,
                "slot_tags": list(tags),
                "campaign_name": campaign_name,
            }
            store_name = store_id_to_name.get(store_id, "")
            if store_name:
                combo["store_name"] = store_name
            combos.append(combo)

    logger.info(
        "campaign_params: derived %s campaign combos from Day-Slot sheets + slots grid (%s stores)",
        len(combos),
        len(per_store),
    )
    return combos


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

    # Load store ID → name mapping for fallback search in campaign creation
    store_id_to_name = _load_store_id_to_name(xl)

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
            combo = {
                "store_id": store_id,
                "day": day,
                "slot": slot,
                "min_subtotal": min_subtotal,
                "campaign_name": campaign_name,
            }
            store_name = store_id_to_name.get(store_id, "")
            if store_name:
                combo["store_name"] = store_name
            combos.append(combo)

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

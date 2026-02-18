"""
AnalysisAgent: runs after browser download. Unzips report, uses FINANCIAL_DETAILED_* only,
runs analysis-app functions that operate on that file (DD data loading, slot analysis,
summary/store tables), and writes a final Excel report to the downloads folder.
"""

import logging
import re
import sys
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple

logger = logging.getLogger(__name__)

# Date column names used by analysis-app for DoorDash files
DD_DATE_COLUMN_VARIATIONS = [
    "Timestamp local date", "Timestamp Local Date", "Timestamp Local date",
    "timestamp local date", "Date", "date", "Timestamp", "timestamp",
]

# Used for extracting and writing Excel
try:
    import pandas as pd
except ImportError:
    pd = None


def _find_financial_detailed_in_zip(zip_path: Path) -> Optional[str]:
    """Return the member name of FINANCIAL_DETAILED_*.csv in the zip, or None."""
    with zipfile.ZipFile(zip_path, "r") as z:
        for name in z.namelist():
            if "FINANCIAL_DETAILED" in name.upper() and name.upper().endswith(".CSV"):
                return name
    return None


def _extract_financial_detailed_csv(zip_path: Path, output_dir: Path) -> Optional[Path]:
    """Extract FINANCIAL_DETAILED_* CSV from zip to output_dir. Returns path to extracted CSV or None."""
    member = _find_financial_detailed_in_zip(zip_path)
    if not member:
        return None
    out_csv = output_dir / "financial_detailed_report.csv"
    with zipfile.ZipFile(zip_path, "r") as z:
        with z.open(member) as f:
            out_csv.write_bytes(f.read())
    logger.info("AnalysisAgent: Extracted %s to %s", member, out_csv.name)
    return out_csv


def _split_report_dates(
    report_start: str, report_end: str
) -> Tuple[str, str, str, str]:
    """
    Derive pre and post date ranges from report range. Uses first half as pre, second half as post.
    Dates in MM/DD/YYYY format. Returns (pre_start, pre_end, post_start, post_end).
    """
    start_dt = pd.to_datetime(report_start, format="%m/%d/%Y")
    end_dt = pd.to_datetime(report_end, format="%m/%d/%Y")
    delta = (end_dt - start_dt).days + 1
    if delta <= 1:
        # Single day: use same for pre and post (growth will be 0)
        s = start_dt.strftime("%m/%d/%Y")
        return s, s, s, s
    half = delta // 2
    mid = start_dt + pd.Timedelta(days=half - 1)
    pre_start = start_dt.strftime("%m/%d/%Y")
    pre_end = mid.strftime("%m/%d/%Y")
    post_start = (mid + pd.Timedelta(days=1)).strftime("%m/%d/%Y")
    post_end = end_dt.strftime("%m/%d/%Y")
    return pre_start, pre_end, post_start, post_end


def _find_date_column(df) -> Optional[str]:
    """Return first column name in df that matches DD date variations (case-insensitive)."""
    cols_lower = {c.strip().lower(): c for c in df.columns}
    for name in DD_DATE_COLUMN_VARIATIONS:
        if name in df.columns:
            return name
        if name.lower() in cols_lower:
            return cols_lower[name.lower()]
    return None


def _infer_report_date_range(
    zip_path: Path,
    extracted_csv: Optional[Path],
    fallback_start: str,
    fallback_end: str,
) -> Tuple[str, str]:
    """
    Infer actual report date range so filtering returns data. Uses ZIP filename
    (e.g. financial_2026-02-12_2026-02-18_...) or CSV date column min/max.
    Returns (start_date, end_date) in MM/DD/YYYY.
    """
    # 1) Try ZIP filename: financial_YYYY-MM-DD_YYYY-MM-DD_...
    name = zip_path.name
    match = re.search(r"financial_(\d{4})-(\d{2})-(\d{2})_(\d{4})-(\d{2})-(\d{2})", name, re.I)
    if match:
        y1, m1, d1 = match.group(1), match.group(2), match.group(3)
        y2, m2, d2 = match.group(4), match.group(5), match.group(6)
        start_str = f"{m1}/{d1}/{y1}"
        end_str = f"{m2}/{d2}/{y2}"
        logger.info("AnalysisAgent: Inferred report range from filename: %s to %s", start_str, end_str)
        return start_str, end_str
    # 2) Read CSV and use min/max of date column
    if extracted_csv and extracted_csv.is_file() and pd is not None:
        try:
            df = pd.read_csv(extracted_csv, nrows=100000)
            df.columns = df.columns.str.strip()
            date_col = _find_date_column(df)
            if date_col:
                ser = pd.to_datetime(df[date_col], format="%m/%d/%Y", errors="coerce")
                if ser.isna().all():
                    ser = pd.to_datetime(df[date_col], format="%Y-%m-%d", errors="coerce")
                if ser.isna().all():
                    ser = pd.to_datetime(df[date_col], errors="coerce")
                ser = ser.dropna()
                if not ser.empty:
                    start_dt = ser.min()
                    end_dt = ser.max()
                    if hasattr(start_dt, "strftime"):
                        start_str = start_dt.strftime("%m/%d/%Y")
                        end_str = end_dt.strftime("%m/%d/%Y")
                    else:
                        s = str(start_dt)[:10]
                        e = str(end_dt)[:10]
                        start_str = f"{s[5:7]}/{s[8:10]}/{s[:4]}" if len(s) >= 10 else s
                        end_str = f"{e[5:7]}/{e[8:10]}/{e[:4]}" if len(e) >= 10 else e
                    logger.info("AnalysisAgent: Inferred report range from CSV dates: %s to %s", start_str, end_str)
                    return start_str, end_str
        except Exception as e:
            logger.debug("AnalysisAgent: Could not infer range from CSV: %s", e)
    logger.info("AnalysisAgent: Using provided report range: %s to %s", fallback_start, fallback_end)
    return fallback_start, fallback_end


def _mock_streamlit() -> None:
    """Install a minimal streamlit mock so analysis-app modules can be imported without Streamlit."""
    import types
    mock = types.ModuleType("streamlit")
    mock.error = lambda *a, **k: None
    mock.warning = lambda *a, **k: None
    mock.info = lambda *a, **k: None
    mock.success = lambda *a, **k: None
    mock.session_state = {}
    mock.cache_data = lambda f: f
    mock.spinner = lambda x: types.SimpleNamespace(__enter__=lambda s: None, __exit__=lambda s, *a: None)
    sys.modules["streamlit"] = mock


def _write_analysis_excel(
    output_dir: Path,
    dd_summary1,
    dd_summary2,
    dd_table1,
    dd_table2,
    sales_pre_post_table,
    sales_yoy_table,
    payouts_pre_post_table,
    payouts_yoy_table,
    operator_name: Optional[str] = None,
) -> Path:
    """Build Excel with DoorDash summary, store-level, and slot tables. Returns path to saved file."""
    from openpyxl import Workbook
    from openpyxl.styles import Font
    from openpyxl.utils.dataframe import dataframe_to_rows

    wb = Workbook()
    wb.remove(wb.active)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    tag = (operator_name.strip() if operator_name and isinstance(operator_name, str) else None)
    filename = f"{tag}_analysis_report_{timestamp}.xlsx" if tag else f"analysis_report_{timestamp}.xlsx"
    filepath = output_dir / filename

    def add_sheet_from_df(ws, df, start_row=1):
        if df is None or (hasattr(df, "empty") and df.empty):
            return start_row
        if hasattr(df, "reset_index") and df.index.name:
            df_export = df.reset_index()
        else:
            df_export = df if hasattr(df, "columns") else pd.DataFrame(df)
        for r_idx, row in enumerate(dataframe_to_rows(df_export, index=False, header=True), start=start_row):
            for c_idx, value in enumerate(row, start=1):
                cell = ws.cell(row=r_idx, column=c_idx, value=value)
                if r_idx == start_row:
                    cell.font = Font(bold=True)
        return start_row + len(df_export) + 2

    row = 1
    # Summary
    ws_sum = wb.create_sheet("DoorDash Summary")
    if dd_summary1 is not None and not (getattr(dd_summary1, "empty", True)):
        ws_sum.cell(row=1, column=1, value="Table 1: Pre vs Post")
        ws_sum.cell(row=1, column=1).font = Font(bold=True, size=12)
        row = add_sheet_from_df(ws_sum, dd_summary1, 3)
    if dd_summary2 is not None and not (getattr(dd_summary2, "empty", True)):
        ws_sum.cell(row=row, column=1, value="Table 2: YoY")
        ws_sum.cell(row=row, column=1).font = Font(bold=True, size=12)
        row = add_sheet_from_df(ws_sum, dd_summary2, row + 2)

    # Store-level
    ws_store = wb.create_sheet("DoorDash Store-Level")
    r = 1
    if dd_table1 is not None and not (getattr(dd_table1, "empty", True)):
        ws_store.cell(row=1, column=1, value="Table 1: Pre vs Post (Store-Level)")
        ws_store.cell(row=1, column=1).font = Font(bold=True, size=12)
        r = add_sheet_from_df(ws_store, dd_table1, 3)
    if dd_table2 is not None and not (getattr(dd_table2, "empty", True)):
        ws_store.cell(row=r, column=1, value="Table 2: YoY (Store-Level)")
        ws_store.cell(row=r, column=1).font = Font(bold=True, size=12)
        add_sheet_from_df(ws_store, dd_table2, r + 2)

    # Slot-based
    ws_slot = wb.create_sheet("Slot-based Analysis")
    r = 1
    for name, tbl in [
        ("Sales - Pre vs Post", sales_pre_post_table),
        ("Sales - YoY", sales_yoy_table),
        ("Payouts - Pre vs Post", payouts_pre_post_table),
        ("Payouts - YoY", payouts_yoy_table),
    ]:
        if tbl is not None and not (getattr(tbl, "empty", True)):
            ws_slot.cell(row=r, column=1, value=name)
            ws_slot.cell(row=r, column=1).font = Font(bold=True, size=12)
            r = add_sheet_from_df(ws_slot, tbl, r + 2)

    wb.save(filepath)
    logger.info("AnalysisAgent: Wrote %s", filepath.name)
    return filepath


def run(
    zip_path: Path,
    output_dir: Path,
    report_start_date: str,
    report_end_date: str,
    excluded_dates: Optional[list] = None,
    operator_name: Optional[str] = None,
) -> Optional[Path]:
    """
    Run analysis using only FINANCIAL_DETAILED_* from the downloaded zip.
    Uses analysis-app: data loading (DD), process_data, slot_analysis, table_generation.
    Writes analysis_report_<timestamp>.xlsx to output_dir. Returns path to that file or None.
    """
    if pd is None:
        raise RuntimeError("pandas is required for AnalysisAgent. Install with: pip install pandas")
    try:
        import openpyxl  # noqa: F401
    except ImportError:
        raise RuntimeError(
            "openpyxl is required for AnalysisAgent to write Excel reports. "
            "Install with: pip install openpyxl"
        )

    zip_path = Path(zip_path)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # 1) Unzip: extract FINANCIAL_DETAILED_* CSV
    extracted_csv = _extract_financial_detailed_csv(zip_path, output_dir)
    if not extracted_csv or not extracted_csv.is_file():
        logger.warning("AnalysisAgent: No FINANCIAL_DETAILED_* in zip; skipping analysis")
        return None

    # 2) Infer actual report date range from ZIP filename or CSV so filtering returns data
    actual_start, actual_end = _infer_report_date_range(
        zip_path, extracted_csv, report_start_date, report_end_date
    )
    pre_start, pre_end, post_start, post_end = _split_report_dates(actual_start, actual_end)
    excluded_dates = excluded_dates or []

    # 3) Mock streamlit and add analysis-app to path so we can import without running Streamlit
    analysis_app_dir = Path(__file__).resolve().parent.parent / "analysis-app" / "app"
    if not analysis_app_dir.is_dir():
        logger.warning("AnalysisAgent: analysis-app/app not found at %s", analysis_app_dir)
        return None

    _mock_streamlit()
    if str(analysis_app_dir) not in sys.path:
        sys.path.insert(0, str(analysis_app_dir))

    try:
        from data_processing import load_and_aggregate_dd_data, process_data
        from table_generation import create_summary_tables, get_platform_store_tables
    except Exception as e:
        logger.warning("AnalysisAgent: Failed to import analysis-app modules: %s", e)
        return None

    # 4) Load and aggregate DD data (FINANCIAL_DETAILED schema = same as dd-data)
    try:
        dd_agg = load_and_aggregate_dd_data(
            excluded_dates=excluded_dates,
            pre_start_date=pre_start,
            pre_end_date=pre_end,
            post_start_date=post_start,
            post_end_date=post_end,
            dd_data_path=extracted_csv,
        )
    except Exception as e:
        logger.warning("AnalysisAgent: load_and_aggregate_dd_data failed: %s", e)
        dd_agg = (
            pd.DataFrame(), pd.DataFrame(), pd.DataFrame(),
            pd.DataFrame(), pd.DataFrame(), pd.DataFrame(),
            pd.DataFrame(), pd.DataFrame(), pd.DataFrame(),
            pd.DataFrame(), pd.DataFrame(), pd.DataFrame(),
        )

    (pre_24_sales, pre_24_payouts, pre_24_orders, post_24_sales, post_24_payouts, post_24_orders,
     pre_25_sales, pre_25_payouts, pre_25_orders, post_25_sales, post_25_payouts, post_25_orders) = dd_agg

    dd_sales_df, dd_payouts_df, dd_orders_df = process_data(
        pre_24_sales, pre_24_payouts, pre_24_orders,
        post_24_sales, post_24_payouts, post_24_orders,
        pre_25_sales, pre_25_payouts, pre_25_orders,
        post_25_sales, post_25_payouts, post_25_orders,
    )

    # Empty new customers (no marketing files in this workflow)
    dd_new_customers_df = pd.DataFrame(columns=[
        "Store ID", "pre_24", "post_24", "pre_25", "post_25",
        "PrevsPost", "LastYear_Pre_vs_Post", "YoY", "Growth%", "YoY%"
    ])
    if not dd_sales_df.empty and "Store ID" in dd_sales_df.columns:
        all_stores = sorted(dd_sales_df["Store ID"].astype(str).unique().tolist())
        sys.modules["streamlit"].session_state["selected_stores_DoorDash"] = all_stores

    dd_summary1, dd_summary2 = None, None
    dd_table1, dd_table2 = None, None
    if not dd_sales_df.empty:
        selected = sys.modules["streamlit"].session_state.get("selected_stores_DoorDash", [])
        if not selected and "Store ID" in dd_sales_df.columns:
            selected = sorted(dd_sales_df["Store ID"].astype(str).unique().tolist())
        dd_summary1, dd_summary2 = create_summary_tables(
            dd_sales_df, dd_payouts_df, dd_orders_df, dd_new_customers_df,
            selected or [], is_ue=False
        )
        dd_table1, dd_table2 = get_platform_store_tables(dd_sales_df, "selected_stores_DoorDash")

    # 5) Slot analysis (uses FINANCIAL_DETAILED file)
    try:
        from slot_analysis import process_slot_analysis
        sales_pre_post_table, sales_yoy_table, payouts_pre_post_table, payouts_yoy_table = process_slot_analysis(
            extracted_csv,
            pre_start_date=pre_start,
            pre_end_date=pre_end,
            post_start_date=post_start,
            post_end_date=post_end,
            excluded_dates=excluded_dates,
        )
    except Exception as e:
        logger.warning("AnalysisAgent: Slot analysis failed: %s", e)
        slot_order = ["Early morning", "Breakfast", "Lunch", "Afternoon", "Dinner", "Late night"]
        empty_slot = pd.DataFrame({
            "Slot": slot_order,
            "Pre": [0.0] * len(slot_order),
            "Post": [0.0] * len(slot_order),
            "Pre vs Post": [0.0] * len(slot_order),
            "Growth%": ["0.0%"] * len(slot_order),
        })
        sales_pre_post_table = sales_yoy_table = payouts_pre_post_table = payouts_yoy_table = empty_slot

    # 6) Write final Excel
    try:
        report_path = _write_analysis_excel(
            output_dir,
            dd_summary1, dd_summary2,
            dd_table1, dd_table2,
            sales_pre_post_table, sales_yoy_table,
            payouts_pre_post_table, payouts_yoy_table,
            operator_name=operator_name,
        )
        return report_path
    except Exception as e:
        logger.warning("AnalysisAgent: Failed to write Excel: %s", e)
        return None

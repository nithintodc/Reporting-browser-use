"""
Marketing data processing — self-contained module for loading DoorDash promotion
and sponsored listing CSVs and building the Corporate vs TODC metrics table.
Replaces the external dependency on DD-automate-app-llm/analysis-app.
"""

import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

# ── Column mappings ──────────────────────────────────────────────────────────

DD_PROMO_COLS = {
    "campaign": "Is self serve campaign",
    "orders": "Orders",
    "sales": "Sales",
    "spend": "Customer discounts from marketing | (Funded by you)",
    "new_customers": "New customers acquired",
}

DD_SPONSORED_COLS = {
    "campaign": "Is self serve campaign",
    "orders": "Orders",
    "sales": "Sales",
    "spend": "Marketing fees | (including any applicable taxes)",
}


# ── Helpers ──────────────────────────────────────────────────────────────────

def _filter_excluded_dates(df: pd.DataFrame, date_col: str, excluded_dates: list) -> pd.DataFrame:
    """Remove rows whose date falls in *excluded_dates*."""
    if not excluded_dates or date_col not in df.columns or df.empty:
        return df
    df = df.copy()
    if not pd.api.types.is_datetime64_any_dtype(df[date_col]):
        df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df = df.dropna(subset=[date_col])
    if df.empty:
        return df
    excluded_objs = []
    for d in excluded_dates:
        if isinstance(d, str):
            dt = pd.to_datetime(d, format="%m/%d/%Y", errors="coerce")
            if pd.notna(dt):
                excluded_objs.append(dt.date())
        elif hasattr(d, "date"):
            excluded_objs.append(d.date())
    if not excluded_objs:
        return df
    df["_date_only"] = df[date_col].dt.date
    df = df[~df["_date_only"].isin(excluded_objs)].drop(columns=["_date_only"])
    return df


def _find_marketing_folders(root_path) -> list:
    """Return sorted list of marketing_* directories under *root_path*."""
    root = Path(root_path) if root_path else Path(".")
    if not root.exists():
        return []
    return sorted([d for d in root.iterdir() if d.is_dir() and d.name.startswith("marketing_")])


# ── Data loaders ─────────────────────────────────────────────────────────────

def load_promotion_data(marketing_folder_path, pre_start, pre_end, post_start, post_end, excluded_dates=None):
    """Load MARKETING_PROMOTION*.csv files and split into pre/post DataFrames."""
    pre_dfs, post_dfs = [], []
    for mdir in _find_marketing_folders(marketing_folder_path):
        for f in mdir.glob("MARKETING_PROMOTION*.csv"):
            try:
                df = pd.read_csv(f)
                df.columns = df.columns.str.strip()
                if "Date" not in df.columns:
                    continue
                df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
                df = df.dropna(subset=["Date"])
                cols = DD_PROMO_COLS
                if not all(c in df.columns for c in [cols["campaign"], cols["orders"], cols["sales"], cols["spend"]]):
                    continue
                pre_df = df[
                    (df["Date"].dt.date >= pd.to_datetime(pre_start).date())
                    & (df["Date"].dt.date <= pd.to_datetime(pre_end).date())
                ]
                post_df = df[
                    (df["Date"].dt.date >= pd.to_datetime(post_start).date())
                    & (df["Date"].dt.date <= pd.to_datetime(post_end).date())
                ]
                if excluded_dates:
                    pre_df = _filter_excluded_dates(pre_df, "Date", excluded_dates)
                    post_df = _filter_excluded_dates(post_df, "Date", excluded_dates)
                pre_dfs.append(pre_df)
                post_dfs.append(post_df)
            except Exception as e:
                logger.warning("Error loading %s: %s", f.name, e)
    return (
        pd.concat(pre_dfs, ignore_index=True) if pre_dfs else pd.DataFrame(),
        pd.concat(post_dfs, ignore_index=True) if post_dfs else pd.DataFrame(),
    )


def load_sponsored_data(marketing_folder_path, pre_start, pre_end, post_start, post_end, excluded_dates=None):
    """Load MARKETING_SPONSORED_LISTING*.csv files and split into pre/post DataFrames."""
    pre_dfs, post_dfs = [], []
    for mdir in _find_marketing_folders(marketing_folder_path):
        for f in mdir.glob("MARKETING_SPONSORED_LISTING*.csv"):
            try:
                df = pd.read_csv(f)
                df.columns = df.columns.str.strip()
                if "Date" not in df.columns:
                    continue
                df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
                df = df.dropna(subset=["Date"])
                cols = DD_SPONSORED_COLS
                if not all(c in df.columns for c in [cols["campaign"], cols["orders"], cols["sales"], cols["spend"]]):
                    continue
                pre_df = df[
                    (df["Date"].dt.date >= pd.to_datetime(pre_start).date())
                    & (df["Date"].dt.date <= pd.to_datetime(pre_end).date())
                ]
                post_df = df[
                    (df["Date"].dt.date >= pd.to_datetime(post_start).date())
                    & (df["Date"].dt.date <= pd.to_datetime(post_end).date())
                ]
                if excluded_dates:
                    pre_df = _filter_excluded_dates(pre_df, "Date", excluded_dates)
                    post_df = _filter_excluded_dates(post_df, "Date", excluded_dates)
                pre_dfs.append(pre_df)
                post_dfs.append(post_df)
            except Exception as e:
                logger.warning("Error loading %s: %s", f.name, e)
    return (
        pd.concat(pre_dfs, ignore_index=True) if pre_dfs else pd.DataFrame(),
        pd.concat(post_dfs, ignore_index=True) if post_dfs else pd.DataFrame(),
    )


# ── Metrics table builder ───────────────────────────────────────────────────

def build_dd_promotions_metrics_table(pre_promo, post_promo, pre_sponsored, post_sponsored):
    """
    Build Corporate vs TODC metrics table.
    Rows: Sales, Total Spend, ROAS, New Customers, Orders, AOV, Cost per Order, CAC
    Cols: TODC Promo, TODC Ads, Corp Promo, Corp Ads
    """
    pc, sc = DD_PROMO_COLS, DD_SPONSORED_COLS

    def _agg_promo(df, is_todc):
        if df.empty:
            return {"sales": 0, "spend": 0, "orders": 0, "new_customers": 0}
        val = True if is_todc else False
        mask = (df[pc["campaign"]] == val) | (df[pc["campaign"]].astype(str).str.lower() == str(val).lower())
        d = df[mask]
        if d.empty:
            return {"sales": 0, "spend": 0, "orders": 0, "new_customers": 0}
        sales = pd.to_numeric(d[pc["sales"]], errors="coerce").fillna(0).sum()
        spend = pd.to_numeric(d[pc["spend"]], errors="coerce").fillna(0).sum()
        orders = pd.to_numeric(d[pc["orders"]], errors="coerce").fillna(0).sum()
        nc_col = pc.get("new_customers")
        new_customers = pd.to_numeric(d[nc_col], errors="coerce").fillna(0).sum() if nc_col in d.columns else 0
        return {"sales": sales, "spend": spend, "orders": orders, "new_customers": new_customers}

    def _agg_sponsored(df, is_todc):
        if df.empty:
            return {"sales": 0, "spend": 0, "orders": 0}
        val = True if is_todc else False
        mask = (df[sc["campaign"]] == val) | (df[sc["campaign"]].astype(str).str.lower() == str(val).lower())
        d = df[mask]
        if d.empty:
            return {"sales": 0, "spend": 0, "orders": 0}
        sales = pd.to_numeric(d[sc["sales"]], errors="coerce").fillna(0).sum()
        spend = pd.to_numeric(d[sc["spend"]], errors="coerce").fillna(0).sum()
        orders = pd.to_numeric(d[sc["orders"]], errors="coerce").fillna(0).sum()
        return {"sales": sales, "spend": spend, "orders": orders, "new_customers": 0}

    todc_promo = _agg_promo(post_promo, True)
    corp_promo = _agg_promo(post_promo, False)
    todc_ads = _agg_sponsored(post_sponsored, True)
    corp_ads = _agg_sponsored(post_sponsored, False)

    def _col(m):
        return [
            m["sales"],
            m["spend"],
            m["sales"] / m["spend"] if m["spend"] else 0,
            m.get("new_customers", 0),
            m["orders"],
            m["sales"] / m["orders"] if m["orders"] else 0,
            m["spend"] / m["orders"] if m["orders"] else 0,
            m["spend"] / m["new_customers"] if m.get("new_customers") else 0,
        ]

    metrics = ["Sales", "Total Spend", "ROAS", "New Customers", "Orders", "AOV", "Cost per Order", "CAC"]
    data = {
        "TODC Promo": _col(todc_promo),
        "TODC Ads": _col(todc_ads),
        "Corp Promo": _col(corp_promo),
        "Corp Ads": _col(corp_ads),
    }
    return pd.DataFrame(data, index=metrics)


# ── Public API (matches the old marketing_analysis module interface) ─────────

def create_corporate_vs_todc_table(
    excluded_dates=None,
    pre_start_date=None,
    pre_end_date=None,
    post_start_date=None,
    post_end_date=None,
    marketing_folder_path=None,
):
    """Build Corporate vs TODC metrics. Returns (promotion_table, sponsored_table, combined_table)."""
    if not marketing_folder_path or not Path(marketing_folder_path).is_dir():
        return None, None, None
    excluded_dates = excluded_dates or []
    pre_start = pre_start_date or post_start_date
    pre_end = pre_end_date or post_end_date
    post_start = post_start_date
    post_end = post_end_date
    if not post_start or not post_end:
        return None, None, None
    try:
        pre_promo, post_promo = load_promotion_data(
            marketing_folder_path, pre_start, pre_end, post_start, post_end, excluded_dates
        )
        pre_sponsored, post_sponsored = load_sponsored_data(
            marketing_folder_path, pre_start, pre_end, post_start, post_end, excluded_dates
        )
        combined_table = build_dd_promotions_metrics_table(pre_promo, post_promo, pre_sponsored, post_sponsored)
        return pd.DataFrame(), pd.DataFrame(), combined_table
    except Exception:
        return None, None, None

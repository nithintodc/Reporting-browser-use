"""Data processing for New-store-app - promotions metrics and slot analysis"""
import pandas as pd
import streamlit as st
from pathlib import Path
from utils import (
    filter_master_file_by_date_range,
    filter_excluded_dates,
    get_time_slot,
    get_day_type,
    DD_DATE_COLUMN_VARIATIONS,
)


# Column names - same as main app
DD_PROMO_COLS = {
    'campaign': 'Is self serve campaign',
    'orders': 'Orders',
    'sales': 'Sales',
    'spend': 'Customer discounts from marketing | (Funded by you)',
    'new_customers': 'New customers acquired',
}
DD_SPONSORED_COLS = {
    'campaign': 'Is self serve campaign',
    'orders': 'Orders',
    'sales': 'Sales',
    'spend': 'Marketing fees | (including any applicable taxes)',
}

# DD Financial file columns (handle case variations)
DD_FINANCIAL_COLS = {
    'subtotal': ['Subtotal'],
    'commission': ['Commission'],
    'processing_fee': ['Payment processing fee'],
    'marketing_fees': ['Marketing fees | (including any applicable taxes)'],
    'cust_discounts': [
        'Customer discounts from marketing | (Funded by you)',
        'Customer discounts from marketing | (funded by you)',  # lowercase in some files
    ],
    'net_total': ['Net total', 'Net total (for historical reference only)'],
}


def _find_col(df, candidates):
    """Find first matching column (case-insensitive fallback)."""
    for c in candidates:
        if c in df.columns:
            return c
    cols_lower = {x.lower(): x for x in df.columns}
    for c in candidates:
        if c.lower() in cols_lower:
            return cols_lower[c.lower()]
    return None


def build_core_business_metrics(file_path, start_date, end_date, excluded_dates=None):
    """
    From DD financial file: Contribution = Subtotal - Commission - Marketing fees
      - Payment processing fee - Customer discounts funded by you
    Contribution % = Contribution / Subtotal
    """
    df = filter_master_file_by_date_range(file_path, start_date, end_date, DD_DATE_COLUMN_VARIATIONS, excluded_dates)
    if df.empty:
        return None

    sub_col = _find_col(df, DD_FINANCIAL_COLS['subtotal'])
    comm_col = _find_col(df, DD_FINANCIAL_COLS['commission'])
    proc_col = _find_col(df, DD_FINANCIAL_COLS['processing_fee'])
    mkt_col = _find_col(df, DD_FINANCIAL_COLS['marketing_fees'])
    disc_col = _find_col(df, DD_FINANCIAL_COLS['cust_discounts'])
    net_col = _find_col(df, DD_FINANCIAL_COLS['net_total'])

    if not sub_col:
        return None

    # Filter to Order transactions only
    if 'Transaction type' in df.columns:
        df = df[df['Transaction type'].astype(str).str.lower() == 'order'].copy()
    if df.empty:
        return None

    for c in [sub_col, comm_col, proc_col, mkt_col, disc_col, net_col]:
        if c:
            df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)

    subtotal = df[sub_col].sum()
    commission = df[comm_col].sum() if comm_col else 0
    processing_fee = df[proc_col].sum() if proc_col else 0
    marketing_fees = df[mkt_col].sum() if mkt_col else 0
    cust_discounts = df[disc_col].sum() if disc_col else 0
    net_total = df[net_col].sum() if net_col else 0

    # Contribution = Subtotal - Commission - Marketing fees - Payment processing fee - Customer discounts
    contribution = subtotal - commission - marketing_fees - processing_fee - cust_discounts
    contribution_pct = (contribution / subtotal * 100) if subtotal else 0

    orders_col = 'DoorDash order ID' if 'DoorDash order ID' in df.columns else None
    orders = df[orders_col].nunique() if orders_col else len(df)
    aov = subtotal / orders if orders else 0
    commission_pct = (commission / subtotal * 100) if subtotal else 0
    mkt_pct = (marketing_fees / subtotal * 100) if subtotal else 0
    proc_pct = (processing_fee / subtotal * 100) if subtotal else 0

    return {
        'Total Orders': orders,
        'Gross Sales (Subtotal)': subtotal,
        'Net Payout (Net total)': net_total,
        'AOV': aov,
        'Commission %': commission_pct,
        'Marketing Cost %': mkt_pct,
        'Processing Fee %': proc_pct,
        'Contribution': contribution,
        'Contribution %': contribution_pct,
    }


def find_marketing_folders(root_path):
    """Find marketing_* directories."""
    root = Path(root_path) if root_path else Path('.')
    if not root.exists():
        return []
    return sorted([d for d in root.iterdir() if d.is_dir() and d.name.startswith('marketing_')])


def load_promotion_data(marketing_folder_path, pre_start, pre_end, post_start, post_end, excluded_dates=None):
    """Load DD promotion files for Pre and Post periods. Returns (pre_df, post_df)."""
    pre_dfs, post_dfs = [], []
    for mdir in find_marketing_folders(marketing_folder_path):
        for f in mdir.glob('MARKETING_PROMOTION*.csv'):
            try:
                df = pd.read_csv(f)
                df.columns = df.columns.str.strip()
                if 'Date' not in df.columns:
                    continue
                df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
                df = df.dropna(subset=['Date'])
                cols = DD_PROMO_COLS
                if not all(c in df.columns for c in [cols['campaign'], cols['orders'], cols['sales'], cols['spend']]):
                    continue
                pre_df = df[(df['Date'].dt.date >= pd.to_datetime(pre_start).date()) &
                            (df['Date'].dt.date <= pd.to_datetime(pre_end).date())]
                post_df = df[(df['Date'].dt.date >= pd.to_datetime(post_start).date()) &
                             (df['Date'].dt.date <= pd.to_datetime(post_end).date())]
                if excluded_dates:
                    pre_df = filter_excluded_dates(pre_df, 'Date', excluded_dates)
                    post_df = filter_excluded_dates(post_df, 'Date', excluded_dates)
                pre_dfs.append(pre_df)
                post_dfs.append(post_df)
            except Exception as e:
                st.warning(f"Error loading {f.name}: {e}")
                continue
    pre_combined = pd.concat(pre_dfs, ignore_index=True) if pre_dfs else pd.DataFrame()
    post_combined = pd.concat(post_dfs, ignore_index=True) if post_dfs else pd.DataFrame()
    return pre_combined, post_combined


def load_sponsored_data(marketing_folder_path, pre_start, pre_end, post_start, post_end, excluded_dates=None):
    """Load DD sponsored listing files for Pre and Post periods. Returns (pre_df, post_df)."""
    pre_dfs, post_dfs = [], []
    for mdir in find_marketing_folders(marketing_folder_path):
        for f in mdir.glob('MARKETING_SPONSORED_LISTING*.csv'):
            try:
                df = pd.read_csv(f)
                df.columns = df.columns.str.strip()
                if 'Date' not in df.columns:
                    continue
                df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
                df = df.dropna(subset=['Date'])
                cols = DD_SPONSORED_COLS
                if not all(c in df.columns for c in [cols['campaign'], cols['orders'], cols['sales'], cols['spend']]):
                    continue
                pre_df = df[(df['Date'].dt.date >= pd.to_datetime(pre_start).date()) &
                            (df['Date'].dt.date <= pd.to_datetime(pre_end).date())]
                post_df = df[(df['Date'].dt.date >= pd.to_datetime(post_start).date()) &
                             (df['Date'].dt.date <= pd.to_datetime(post_end).date())]
                if excluded_dates:
                    pre_df = filter_excluded_dates(pre_df, 'Date', excluded_dates)
                    post_df = filter_excluded_dates(post_df, 'Date', excluded_dates)
                pre_dfs.append(pre_df)
                post_dfs.append(post_df)
            except Exception as e:
                st.warning(f"Error loading {f.name}: {e}")
                continue
    pre_combined = pd.concat(pre_dfs, ignore_index=True) if pre_dfs else pd.DataFrame()
    post_combined = pd.concat(post_dfs, ignore_index=True) if post_dfs else pd.DataFrame()
    return pre_combined, post_combined


def build_dd_promotions_metrics_table(pre_promo, post_promo, pre_sponsored, post_sponsored):
    """
    Build DoorDash metrics table like the image:
    Rows: Sales, Total Spend, ROAS, New Customers, Orders, AOV, Cost per Order, CAC
    Cols: TODC Promo, TODC Ads, Corp Promo, Corp Ads
    """
    pc, sc = DD_PROMO_COLS, DD_SPONSORED_COLS
    slot_order = ['TODC Promo', 'TODC Ads', 'Corp Promo', 'Corp Ads']

    def _agg_promo(df, is_todc):
        if df.empty:
            return {'sales': 0, 'spend': 0, 'orders': 0, 'new_customers': 0}
        # Is self serve: True/true = TODC, False/false = Corporate
        val = True if is_todc else False
        mask = (df[pc['campaign']] == val) | (df[pc['campaign']].astype(str).str.lower() == str(val).lower())
        d = df[mask]
        if d.empty:
            return {'sales': 0, 'spend': 0, 'orders': 0, 'new_customers': 0}
        sales = pd.to_numeric(d[pc['sales']], errors='coerce').fillna(0).sum()
        spend = pd.to_numeric(d[pc['spend']], errors='coerce').fillna(0).sum()
        orders = pd.to_numeric(d[pc['orders']], errors='coerce').fillna(0).sum()
        nc_col = pc.get('new_customers')
        new_customers = pd.to_numeric(d[nc_col], errors='coerce').fillna(0).sum() if nc_col in d.columns else 0
        return {'sales': sales, 'spend': spend, 'orders': orders, 'new_customers': new_customers}

    def _agg_sponsored(df, is_todc):
        if df.empty:
            return {'sales': 0, 'spend': 0, 'orders': 0}
        val = True if is_todc else False
        mask = (df[sc['campaign']] == val) | (df[sc['campaign']].astype(str).str.lower() == str(val).lower())
        d = df[mask]
        if d.empty:
            return {'sales': 0, 'spend': 0, 'orders': 0}
        sales = pd.to_numeric(d[sc['sales']], errors='coerce').fillna(0).sum()
        spend = pd.to_numeric(d[sc['spend']], errors='coerce').fillna(0).sum()
        orders = pd.to_numeric(d[sc['orders']], errors='coerce').fillna(0).sum()
        return {'sales': sales, 'spend': spend, 'orders': orders, 'new_customers': 0}

    # Use POST period for the main metrics table (like Corporate vs TODC in main app)
    todc_promo = _agg_promo(post_promo, True)
    corp_promo = _agg_promo(post_promo, False)
    todc_ads = _agg_sponsored(post_sponsored, True)
    corp_ads = _agg_sponsored(post_sponsored, False)

    data = {
        'TODC Promo': [
            todc_promo['sales'],
            todc_promo['spend'],
            todc_promo['sales'] / todc_promo['spend'] if todc_promo['spend'] else 0,
            todc_promo['new_customers'],
            todc_promo['orders'],
            todc_promo['sales'] / todc_promo['orders'] if todc_promo['orders'] else 0,
            todc_promo['spend'] / todc_promo['orders'] if todc_promo['orders'] else 0,
            todc_promo['spend'] / todc_promo['new_customers'] if todc_promo['new_customers'] else 0,
        ],
        'TODC Ads': [
            todc_ads['sales'],
            todc_ads['spend'],
            todc_ads['sales'] / todc_ads['spend'] if todc_ads['spend'] else 0,
            todc_ads['new_customers'],
            todc_ads['orders'],
            todc_ads['sales'] / todc_ads['orders'] if todc_ads['orders'] else 0,
            todc_ads['spend'] / todc_ads['orders'] if todc_ads['orders'] else 0,
            todc_ads['spend'] / todc_ads['new_customers'] if todc_ads['new_customers'] else 0,
        ],
        'Corp Promo': [
            corp_promo['sales'],
            corp_promo['spend'],
            corp_promo['sales'] / corp_promo['spend'] if corp_promo['spend'] else 0,
            corp_promo['new_customers'],
            corp_promo['orders'],
            corp_promo['sales'] / corp_promo['orders'] if corp_promo['orders'] else 0,
            corp_promo['spend'] / corp_promo['orders'] if corp_promo['orders'] else 0,
            corp_promo['spend'] / corp_promo['new_customers'] if corp_promo['new_customers'] else 0,
        ],
        'Corp Ads': [
            corp_ads['sales'],
            corp_ads['spend'],
            corp_ads['sales'] / corp_ads['spend'] if corp_ads['spend'] else 0,
            corp_ads['new_customers'],
            corp_ads['orders'],
            corp_ads['sales'] / corp_ads['orders'] if corp_ads['orders'] else 0,
            corp_ads['spend'] / corp_ads['orders'] if corp_ads['orders'] else 0,
            corp_ads['spend'] / corp_ads['new_customers'] if corp_ads['new_customers'] else 0,
        ],
    }
    metrics = ['Sales', 'Total Spend', 'ROAS', 'New Customers', 'Orders', 'AOV', 'Cost per Order', 'CAC']
    return pd.DataFrame(data, index=metrics)


def process_dd_slot_analysis_pre_post(file_path, pre_start, pre_end, post_start, post_end, excluded_dates=None):
    """DoorDash slot analysis - Pre vs Post only (no YoY). Uses Timestamp local time."""
    slot_order = ['Early morning', 'Breakfast', 'Lunch', 'Afternoon', 'Dinner', 'Late night']
    pre_df = filter_master_file_by_date_range(file_path, pre_start, pre_end, DD_DATE_COLUMN_VARIATIONS, excluded_dates)
    post_df = filter_master_file_by_date_range(file_path, post_start, post_end, DD_DATE_COLUMN_VARIATIONS, excluded_dates)

    ref_df = post_df if not post_df.empty else pre_df
    if ref_df.empty:
        return None, None

    time_col = None
    for col in ['Timestamp local time', 'Timestamp Local Time', 'timestamp local time', 'Order received local time']:
        if col in ref_df.columns:
            time_col = col
            break
    if time_col is None:
        time_col = next((c for c in ref_df.columns if 'time' in c.lower()), None)
    if time_col is None:
        return None, None

    sales_col = 'Subtotal'
    payout_col = 'Net total' if 'Net total' in ref_df.columns else 'Net total (for historical reference only)'
    if payout_col not in ref_df.columns:
        payout_col = next((c for c in ref_df.columns if 'net' in c.lower()), None)
    if sales_col not in ref_df.columns or payout_col is None:
        return None, None

    def _agg_by_slot(df):
        if df.empty or time_col not in df.columns or sales_col not in df.columns:
            return {s: {'sales': 0, 'payouts': 0} for s in slot_order}
        d = df.copy()
        d['Slot'] = d[time_col].apply(get_time_slot)
        d = d.dropna(subset=['Slot'])
        d[sales_col] = pd.to_numeric(d[sales_col], errors='coerce').fillna(0)
        d[payout_col] = pd.to_numeric(d[payout_col], errors='coerce').fillna(0) if payout_col in d.columns else 0
        agg = d.groupby('Slot').agg({sales_col: 'sum', payout_col: 'sum'}).reset_index()
        result = {s: {'sales': 0, 'payouts': 0} for s in slot_order}
        for _, row in agg.iterrows():
            s = row['Slot']
            if s in result:
                result[s] = {'sales': row[sales_col], 'payouts': row[payout_col]}
        return result

    pre_slots = _agg_by_slot(pre_df)
    post_slots = _agg_by_slot(post_df)

    sales_data = []
    payouts_data = []
    for slot in slot_order:
        pre_s = pre_slots[slot]['sales']
        post_s = post_slots[slot]['sales']
        diff = post_s - pre_s
        growth = f"{((post_s - pre_s) / pre_s * 100):.1f}%" if pre_s else "0.0%"
        sales_data.append({'Slot': slot, 'Pre': pre_s, 'Post': post_s, 'Pre vs Post': diff, 'Growth%': growth})
        pre_p = pre_slots[slot]['payouts']
        post_p = post_slots[slot]['payouts']
        diff_p = post_p - pre_p
        growth_p = f"{((post_p - pre_p) / pre_p * 100):.1f}%" if pre_p else "0.0%"
        payouts_data.append({'Slot': slot, 'Pre': pre_p, 'Post': post_p, 'Pre vs Post': diff_p, 'Growth%': growth_p})

    return pd.DataFrame(sales_data), pd.DataFrame(payouts_data)


def process_ue_slot_analysis_pre_post(file_path, pre_start, pre_end, post_start, post_end, excluded_dates=None):
    """UberEats slot analysis - Pre vs Post only. Uses 'Order Accept Time' for slots."""
    slot_order = ['Early morning', 'Breakfast', 'Lunch', 'Afternoon', 'Dinner', 'Late night']
    time_col = 'Order Accept Time'
    date_col_variations = ['Order date', 'Order Date', 'Date', 'date']

    try:
        df = pd.read_csv(file_path, skiprows=[0], header=0)
        df.columns = df.columns.str.strip()
        if time_col not in df.columns:
            time_col_alt = next((c for c in df.columns if 'order accept' in c.lower() or 'accept time' in c.lower()), None)
            if time_col_alt:
                time_col = time_col_alt
            else:
                st.warning(f"'Order Accept Time' not found in UE file. Columns: {list(df.columns)[:15]}")
                return None, None

        date_col = df.columns[8] if len(df.columns) > 8 else 'Order date'
        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
        df = df.dropna(subset=[date_col])
        pre_start_dt = pd.to_datetime(pre_start, format='%m/%d/%Y')
        pre_end_dt = pd.to_datetime(pre_end, format='%m/%d/%Y')
        post_start_dt = pd.to_datetime(post_start, format='%m/%d/%Y')
        post_end_dt = pd.to_datetime(post_end, format='%m/%d/%Y')
        pre_df = df[(df[date_col] >= pre_start_dt) & (df[date_col] <= pre_end_dt)]
        post_df = df[(df[date_col] >= post_start_dt) & (df[date_col] <= post_end_dt)]
        if excluded_dates:
            pre_df = filter_excluded_dates(pre_df, date_col, excluded_dates)
            post_df = filter_excluded_dates(post_df, date_col, excluded_dates)

        sales_col = 'Sales (excl. tax)'
        payout_col = 'Total payout'
        if sales_col not in df.columns or payout_col not in df.columns:
            st.warning("Sales or Total payout column not found in UE file")
            return None, None

        def _agg_by_slot(d):
            if d.empty:
                return {s: {'sales': 0, 'payouts': 0} for s in slot_order}
            dd = d.copy()
            dd['Slot'] = dd[time_col].apply(get_time_slot)
            dd = dd.dropna(subset=['Slot'])
            dd[sales_col] = pd.to_numeric(dd[sales_col], errors='coerce').fillna(0)
            dd[payout_col] = pd.to_numeric(dd[payout_col], errors='coerce').fillna(0)
            agg = dd.groupby('Slot').agg({sales_col: 'sum', payout_col: 'sum'}).reset_index()
            result = {s: {'sales': 0, 'payouts': 0} for s in slot_order}
            for _, row in agg.iterrows():
                s = row['Slot']
                if s in result:
                    result[s] = {'sales': row[sales_col], 'payouts': row[payout_col]}
            return result

        pre_slots = _agg_by_slot(pre_df)
        post_slots = _agg_by_slot(post_df)

        sales_data = []
        payouts_data = []
        for slot in slot_order:
            pre_s = pre_slots[slot]['sales']
            post_s = post_slots[slot]['sales']
            diff = post_s - pre_s
            growth = f"{((post_s - pre_s) / pre_s * 100):.1f}%" if pre_s else "0.0%"
            sales_data.append({'Slot': slot, 'Pre': pre_s, 'Post': post_s, 'Pre vs Post': diff, 'Growth%': growth})
            pre_p = pre_slots[slot]['payouts']
            post_p = post_slots[slot]['payouts']
            diff_p = post_p - pre_p
            growth_p = f"{((post_p - pre_p) / pre_p * 100):.1f}%" if pre_p else "0.0%"
            payouts_data.append({'Slot': slot, 'Pre': pre_p, 'Post': post_p, 'Pre vs Post': diff_p, 'Growth%': growth_p})

        return pd.DataFrame(sales_data), pd.DataFrame(payouts_data)
    except Exception as e:
        st.error(f"Error processing UE slot analysis: {e}")
        return None, None


def _load_ue_by_date_range(file_path, start_date, end_date, excluded_dates=None):
    """Load UE file and filter by date range."""
    try:
        df = pd.read_csv(file_path, skiprows=[0], header=0)
        df.columns = df.columns.str.strip()
        date_col = df.columns[8] if len(df.columns) > 8 else None
        if not date_col:
            return pd.DataFrame()
        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
        df = df.dropna(subset=[date_col])
        start_dt = pd.to_datetime(start_date, format='%m/%d/%Y')
        end_dt = pd.to_datetime(end_date, format='%m/%d/%Y')
        df = df[(df[date_col] >= start_dt) & (df[date_col] <= end_dt)]
        if excluded_dates:
            df = filter_excluded_dates(df, date_col, excluded_dates)
        return df
    except Exception:
        return pd.DataFrame()


def _find_ue_col(df, names):
    """Find column by name variations (case/strip)."""
    for n in names:
        for c in df.columns:
            if c.strip().lower() == n.strip().lower():
                return c
    return None


def build_core_business_metrics_ue(file_path, start_date, end_date, excluded_dates=None):
    """UE core metrics: Sales (excl. tax), Total payout, Marketplace Fee, etc."""
    df = _load_ue_by_date_range(file_path, start_date, end_date, excluded_dates)
    if df.empty:
        return None
    sales_col = _find_ue_col(df, ['Sales (excl. tax)', 'Total item sales excl tax']) or 'Sales (excl. tax)'
    payout_col = _find_ue_col(df, ['Total payout', 'Total payout ', 'Total payout,']) or 'Total payout'
    mkt_col = _find_ue_col(df, ['Marketplace Fee', 'Marketplace fee']) or 'Marketplace Fee'
    if sales_col not in df.columns:
        return None
    if payout_col not in df.columns:
        payout_col = next((c for c in df.columns if 'total payout' in c.lower()), None)
    if not payout_col:
        return None
    df[sales_col] = pd.to_numeric(df[sales_col], errors='coerce').fillna(0)
    df[payout_col] = pd.to_numeric(df[payout_col], errors='coerce').fillna(0)
    mkt_pct_col = 'Marketplace fee %' if 'Marketplace fee %' in df.columns else None
    subtotal = df[sales_col].sum()
    net_total = df[payout_col].sum()
    orders = df['Order ID'].nunique() if 'Order ID' in df.columns else len(df)
    aov = subtotal / orders if orders else 0
    mkt_fee = df[mkt_col].sum() if mkt_col in df.columns else 0
    mkt_pct = (mkt_fee / subtotal * 100) if subtotal else 0
    contribution_pct = (net_total / subtotal * 100) if subtotal else 0
    return {
        'Total Orders': orders,
        'Gross Sales (Subtotal)': subtotal,
        'Net Payout (Net total)': net_total,
        'AOV': aov,
        'Commission %': mkt_pct,
        'Marketing Cost %': 0,
        'Processing Fee %': 0,
        'Contribution': net_total,
        'Contribution %': contribution_pct,
    }


def build_day_type_table(file_path, start_date, end_date, excluded_dates=None):
    """Build table by Day Type: Weekday (Mon-Fri) vs Weekend (Sat-Sun). DD file."""
    df = filter_master_file_by_date_range(file_path, start_date, end_date, DD_DATE_COLUMN_VARIATIONS, excluded_dates)
    if df.empty:
        return None
    if 'Transaction type' in df.columns:
        df = df[df['Transaction type'].astype(str).str.lower() == 'order'].copy()
    if df.empty:
        return None
    date_col = _find_col(df, ['Timestamp local date', 'Date'])
    if not date_col:
        date_col = next((c for c in df.columns if 'date' in c.lower()), None)
    if not date_col:
        return None
    df['Day Type'] = df[date_col].apply(get_day_type)
    df = df.dropna(subset=['Day Type'])
    sub_col = _find_col(df, DD_FINANCIAL_COLS['subtotal'])
    net_col = _find_col(df, DD_FINANCIAL_COLS['net_total'])
    order_col = 'DoorDash order ID' if 'DoorDash order ID' in df.columns else None
    if not sub_col:
        return None
    df[sub_col] = pd.to_numeric(df[sub_col], errors='coerce').fillna(0)
    if net_col:
        df[net_col] = pd.to_numeric(df[net_col], errors='coerce').fillna(0)
    if order_col:
        grp = df.groupby('Day Type').agg({sub_col: 'sum', order_col: 'nunique'}).reset_index()
        grp.columns = ['Day Type', 'Sales', 'Orders']
    else:
        grp = df.groupby('Day Type').agg({sub_col: 'sum'}).reset_index()
        grp.columns = ['Day Type', 'Sales']
        grp['Orders'] = len(df)
    if net_col:
        net_grp = df.groupby('Day Type')[net_col].sum().reset_index()
        net_grp.columns = ['Day Type', 'Net Payout']
        grp = grp.merge(net_grp, on='Day Type')
    grp['AOV'] = grp['Sales'] / grp['Orders'].replace(0, 1)
    cols = ['Day Type', 'Orders', 'Sales', 'AOV']
    if 'Net Payout' in grp.columns:
        cols.append('Net Payout')
    return grp[cols]


def build_day_type_table_ue(file_path, start_date, end_date, excluded_dates=None):
    """Build Day Type table for UberEats."""
    df = _load_ue_by_date_range(file_path, start_date, end_date, excluded_dates)
    if df.empty:
        return None
    date_col = df.columns[8] if len(df.columns) > 8 else 'Order date'
    df['Day Type'] = df[date_col].apply(get_day_type)
    df = df.dropna(subset=['Day Type'])
    sales_col = _find_ue_col(df, ['Sales (excl. tax)', 'Total item sales excl tax']) or 'Sales (excl. tax)'
    payout_col = _find_ue_col(df, ['Total payout', 'Total payout ']) or next((c for c in df.columns if 'total payout' in c.lower()), None)
    order_col = 'Order ID' if 'Order ID' in df.columns else None
    if sales_col not in df.columns:
        return None
    df[sales_col] = pd.to_numeric(df[sales_col], errors='coerce').fillna(0)
    if payout_col in df.columns:
        df[payout_col] = pd.to_numeric(df[payout_col], errors='coerce').fillna(0)
    if order_col:
        grp = df.groupby('Day Type').agg({sales_col: 'sum', order_col: 'nunique'}).reset_index()
        grp.columns = ['Day Type', 'Sales', 'Orders']
    else:
        grp = df.groupby('Day Type')[sales_col].sum().reset_index()
        grp.columns = ['Day Type', 'Sales']
        grp['Orders'] = len(df)
    if payout_col in df.columns:
        net_grp = df.groupby('Day Type')[payout_col].sum().reset_index()
        net_grp.columns = ['Day Type', 'Net Payout']
        grp = grp.merge(net_grp, on='Day Type')
    grp['AOV'] = grp['Sales'] / grp['Orders'].replace(0, 1)
    cols = ['Day Type', 'Orders', 'Sales', 'AOV'] + (['Net Payout'] if 'Net Payout' in grp.columns else [])
    return grp[cols]


def _dd_financial_with_dims(file_path, start_date, end_date, excluded_dates=None):
    """Load DD financial, filter to orders, add Slot and Day Type. Return df with Store name, Slot, Day Type, and numeric cols."""
    df = filter_master_file_by_date_range(file_path, start_date, end_date, DD_DATE_COLUMN_VARIATIONS, excluded_dates)
    if df.empty:
        return pd.DataFrame()
    if 'Transaction type' in df.columns:
        df = df[df['Transaction type'].astype(str).str.lower() == 'order'].copy()
    if df.empty:
        return pd.DataFrame()
    date_col = _find_col(df, ['Timestamp local date', 'Date']) or next((c for c in df.columns if 'date' in c.lower()), None)
    time_col = next((c for c in df.columns if 'local time' in c.lower() or c == 'Timestamp local time'), None)
    store_col = 'Store name' if 'Store name' in df.columns else (next((c for c in df.columns if 'store' in c.lower()), None))
    if not date_col:
        return pd.DataFrame()
    df['Day Type'] = df[date_col].apply(get_day_type)
    df = df.dropna(subset=['Day Type'])
    if time_col:
        df['Slot'] = df[time_col].apply(get_time_slot)
        df = df.dropna(subset=['Slot'])
    else:
        df['Slot'] = 'All'
    # Financial raw data uses "Merchant store ID"; fallback to "Store ID"
    store_id_col = 'Merchant store ID' if 'Merchant store ID' in df.columns else ('Store ID' if 'Store ID' in df.columns else None)
    if not store_col:
        df['Store name'] = 'All'
        df['Store'] = 'All'
    else:
        if store_id_col and store_id_col in df.columns:
            df['Store'] = df[store_id_col].astype(str) + ' - ' + df[store_col].astype(str)
        else:
            df['Store'] = df[store_col].astype(str)
    sub_col = _find_col(df, DD_FINANCIAL_COLS['subtotal'])
    net_col = _find_col(df, DD_FINANCIAL_COLS['net_total'])
    comm_col = _find_col(df, DD_FINANCIAL_COLS['commission'])
    mkt_col = _find_col(df, DD_FINANCIAL_COLS['marketing_fees'])
    proc_col = _find_col(df, DD_FINANCIAL_COLS['processing_fee'])
    disc_col = _find_col(df, DD_FINANCIAL_COLS['cust_discounts'])
    order_col = 'DoorDash order ID' if 'DoorDash order ID' in df.columns else None
    if not sub_col:
        return pd.DataFrame()
    for c in [sub_col, net_col, comm_col, mkt_col, proc_col, disc_col]:
        if c and c in df.columns:
            df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)
    df['_contribution'] = df[sub_col] - df.get(comm_col, 0).fillna(0) - df.get(mkt_col, 0).fillna(0) - df.get(proc_col, 0).fillna(0) - df.get(disc_col, 0).fillna(0)
    return df, {'sub': sub_col, 'net': net_col, 'order_col': order_col, 'store_col': 'Store'}


def build_pivot_metrics_dd(dd_path, promo_df, sponsored_df, start_date, end_date, pivot_by, excluded_dates=None):
    """
    pivot_by: list of 'Store', 'Slot', 'Days'. Build one row per group with all metrics.
    Labels: Self (was TODC), Corp for promo/ads.
    """
    if not pivot_by:
        pivot_by = ['Store']
    out = _dd_financial_with_dims(dd_path, start_date, end_date, excluded_dates)
    if isinstance(out, tuple):
        df, col_map = out
    else:
        df = out
        col_map = {}
    if df.empty:
        return None
    sub_col = col_map.get('sub', 'Subtotal')
    net_col = col_map.get('net')
    order_col = col_map.get('order_col')
    store_col = col_map.get('store_col', 'Store')

    group_cols = []
    if 'Store' in pivot_by and 'Store' in df.columns:
        group_cols.append('Store')
    if 'Slot' in pivot_by and 'Slot' in df.columns:
        group_cols.append('Slot')
    if 'Days' in pivot_by and 'Day Type' in df.columns:
        group_cols.append('Day Type')

    if not group_cols:
        agg = {sub_col: 'sum', '_contribution': 'sum'}
        if net_col:
            agg[net_col] = 'sum'
        if order_col:
            agg[order_col] = 'nunique'
        fin = df.groupby(lambda x: 0).agg(agg).reset_index(drop=True)
        if order_col:
            fin = fin.rename(columns={order_col: 'Orders'})
        else:
            fin['Orders'] = len(df)
        fin['Sales'] = fin[sub_col]
        fin['Net Payout'] = fin[net_col] if net_col in fin.columns else 0
        fin['AOV'] = fin['Sales'] / fin['Orders'].replace(0, 1)
        fin['Profitability'] = (fin['Net Payout'] / fin['Sales'] * 100).replace([float('inf'), float('-inf')], 0).fillna(0)
        for c in ['Store', 'Slot', 'Days']:
            if c in pivot_by:
                fin[c] = 'All'
        rows = fin.to_dict('records')
    else:
        agg = {sub_col: 'sum', '_contribution': 'sum'}
        if net_col:
            agg[net_col] = 'sum'
        if order_col:
            agg[order_col] = 'nunique'
        g = df.groupby(group_cols).agg(agg).reset_index()
        if order_col:
            g = g.rename(columns={order_col: 'Orders'})
        else:
            g['Orders'] = df.groupby(group_cols).size().values
        g['Sales'] = g[sub_col]
        g['Net Payout'] = g[net_col] if net_col else 0
        g['AOV'] = g['Sales'] / g['Orders'].replace(0, 1)
        g['Profitability'] = (g['Net Payout'] / g['Sales'] * 100).fillna(0)
        # Rename for display
        rename = {}
        if 'Day Type' in g.columns:
            rename['Day Type'] = 'Days'
        if rename:
            g = g.rename(columns=rename)
        rows = g.to_dict('records')

    # Marketing: by (Store, Days) only (no Slot in marketing)
    pc, sc = DD_PROMO_COLS, DD_SPONSORED_COLS
    mkt_rows = {}  # key = (store?, days?) -> metrics

    def _mkt_key(r):
        parts = []
        if 'Store' in pivot_by and 'Store name' in promo_df.columns:
            parts.append(r.get('Store name', r.get('Store', 'All')))
        if 'Days' in pivot_by and 'Day Type' in promo_df.columns:
            parts.append(r.get('Day Type', 'All'))
        return tuple(parts) if parts else (0,)

    if not promo_df.empty and 'Date' in promo_df.columns:
        promo_df = promo_df.copy()
        promo_df['Date'] = pd.to_datetime(promo_df['Date'], errors='coerce')
        promo_df = promo_df.dropna(subset=['Date'])
        start_dt = pd.to_datetime(start_date, errors='coerce')
        end_dt = pd.to_datetime(end_date, errors='coerce')
        if pd.notna(start_dt) and pd.notna(end_dt):
            promo_df = promo_df[(promo_df['Date'] >= start_dt) & (promo_df['Date'] <= end_dt)]
        promo_df['Day Type'] = promo_df['Date'].apply(get_day_type)
        store_id_mkt = 'Merchant store ID' if 'Merchant store ID' in promo_df.columns else ('Store ID' if 'Store ID' in promo_df.columns else None)
        store_name_mkt = 'Store name' if 'Store name' in promo_df.columns else None
        for _, row in promo_df.iterrows():
            key = ()
            if 'Store' in pivot_by and (store_id_mkt or store_name_mkt):
                if store_id_mkt and store_name_mkt and store_id_mkt in promo_df.columns and store_name_mkt in promo_df.columns:
                    store_val = f"{row.get(store_id_mkt, '')} - {row.get(store_name_mkt, '')}"
                else:
                    store_val = row.get(store_name_mkt, row.get(store_id_mkt, 'All'))
                key = key + (store_val,)
            if 'Days' in pivot_by:
                key = key + (row.get('Day Type', 'All'),)
            if not key:
                key = (0,)
            if key not in mkt_rows:
                mkt_rows[key] = {'self_promo_sales': 0, 'self_promo_spend': 0, 'corp_promo_sales': 0, 'corp_promo_spend': 0,
                                 'self_promo_nc': 0, 'corp_promo_nc': 0, 'self_ads_sales': 0, 'self_ads_spend': 0,
                                 'corp_ads_sales': 0, 'corp_ads_spend': 0}
            is_self = row.get(pc['campaign']) == True or str(row.get(pc['campaign'], '')).lower() == 'true'
            sales = pd.to_numeric(row.get(pc['sales'], 0), errors='coerce') or 0
            spend = pd.to_numeric(row.get(pc['spend'], 0), errors='coerce') or 0
            nc = pd.to_numeric(row.get(pc.get('new_customers', ''), 0), errors='coerce') or 0
            if is_self:
                mkt_rows[key]['self_promo_sales'] += sales
                mkt_rows[key]['self_promo_spend'] += spend
                mkt_rows[key]['self_promo_nc'] += nc
            else:
                mkt_rows[key]['corp_promo_sales'] += sales
                mkt_rows[key]['corp_promo_spend'] += spend
                mkt_rows[key]['corp_promo_nc'] += nc

    if not sponsored_df.empty and 'Date' in sponsored_df.columns:
        sponsored_df = sponsored_df.copy()
        sponsored_df['Date'] = pd.to_datetime(sponsored_df['Date'], errors='coerce')
        sponsored_df = sponsored_df.dropna(subset=['Date'])
        start_dt = pd.to_datetime(start_date, errors='coerce')
        end_dt = pd.to_datetime(end_date, errors='coerce')
        if pd.notna(start_dt) and pd.notna(end_dt):
            sponsored_df = sponsored_df[(sponsored_df['Date'] >= start_dt) & (sponsored_df['Date'] <= end_dt)]
        sponsored_df['Day Type'] = sponsored_df['Date'].apply(get_day_type)
        store_id_mkt = 'Merchant store ID' if 'Merchant store ID' in sponsored_df.columns else ('Store ID' if 'Store ID' in sponsored_df.columns else None)
        store_name_mkt = 'Store name' if 'Store name' in sponsored_df.columns else None
        for _, row in sponsored_df.iterrows():
            key = ()
            if 'Store' in pivot_by and (store_id_mkt or store_name_mkt):
                if store_id_mkt and store_name_mkt and store_id_mkt in sponsored_df.columns and store_name_mkt in sponsored_df.columns:
                    store_val = f"{row.get(store_id_mkt, '')} - {row.get(store_name_mkt, '')}"
                else:
                    store_val = row.get(store_name_mkt, row.get(store_id_mkt, 'All'))
                key = key + (store_val,)
            if 'Days' in pivot_by:
                key = key + (row.get('Day Type', 'All'),)
            if not key:
                key = (0,)
            if key not in mkt_rows:
                mkt_rows[key] = {'self_promo_sales': 0, 'self_promo_spend': 0, 'corp_promo_sales': 0, 'corp_promo_spend': 0,
                                 'self_promo_nc': 0, 'corp_promo_nc': 0, 'self_ads_sales': 0, 'self_ads_spend': 0,
                                 'corp_ads_sales': 0, 'corp_ads_spend': 0}
            is_self = row.get(sc['campaign']) == True or str(row.get(sc['campaign'], '')).lower() == 'true'
            sales = pd.to_numeric(row.get(sc['sales'], 0), errors='coerce') or 0
            spend = pd.to_numeric(row.get(sc['spend'], 0), errors='coerce') or 0
            if is_self:
                mkt_rows[key]['self_ads_sales'] += sales
                mkt_rows[key]['self_ads_spend'] += spend
            else:
                mkt_rows[key]['corp_ads_sales'] += sales
                mkt_rows[key]['corp_ads_spend'] += spend

    # Merge marketing into rows by (Store, Days)
    for r in rows:
        key = ()
        if 'Store' in pivot_by:
            key = key + (r.get('Store', 'All'),)
        if 'Days' in pivot_by:
            key = key + (r.get('Days', 'All'),)
        if not key:
            key = (0,)
        m = mkt_rows.get(key, {})
        r['Self Promo Sales'] = m.get('self_promo_sales', 0)
        r['Self Promo Spend'] = m.get('self_promo_spend', 0)
        r['Corp Promo Sales'] = m.get('corp_promo_sales', 0)
        r['Corp Promo Spend'] = m.get('corp_promo_spend', 0)
        r['Self Promo ROAS'] = (r['Self Promo Sales'] / r['Self Promo Spend']) if r.get('Self Promo Spend') else 0
        r['Corp Promo ROAS'] = (r['Corp Promo Sales'] / r['Corp Promo Spend']) if r.get('Corp Promo Spend') else 0
        r['Self Ads Sales'] = m.get('self_ads_sales', 0)
        r['Self Ads Spend'] = m.get('self_ads_spend', 0)
        r['Corp Ads Sales'] = m.get('corp_ads_sales', 0)
        r['Corp Ads Spend'] = m.get('corp_ads_spend', 0)
        r['Self Ads ROAS'] = (r['Self Ads Sales'] / r['Self Ads Spend']) if r.get('Self Ads Spend') else 0
        r['Corp Ads ROAS'] = (r['Corp Ads Sales'] / r['Corp Ads Spend']) if r.get('Corp Ads Spend') else 0
        r['New Customers (Self)'] = m.get('self_promo_nc', 0) + 0  # ads don't have NC in same way
        r['New Customers (Corp)'] = m.get('corp_promo_nc', 0)
        r['Total Promo Sales'] = r['Self Promo Sales'] + r['Corp Promo Sales']
        r['Total Ads Sales'] = r['Self Ads Sales'] + r['Corp Ads Sales']
        sales_tot = r.get('Sales') or 0
        r['Promo % of Sales'] = (r['Total Promo Sales'] / sales_tot * 100) if sales_tot else 0
        r['Ads % of Sales'] = (r['Total Ads Sales'] / sales_tot * 100) if sales_tot else 0
        r['Profitability'] = (r.get('Net Payout', 0) / sales_tot * 100) if sales_tot else 0

    # Drop internal cols and ensure all metric columns exist (0 if missing)
    all_metric_cols = ['Orders', 'Sales', 'Net Payout', 'AOV', 'Profitability',
                       'Self Promo Sales', 'Corp Promo Sales', 'Self Promo Spend', 'Corp Promo Spend',
                       'Self Promo ROAS', 'Corp Promo ROAS', 'Self Ads Sales', 'Corp Ads Sales',
                       'Self Ads Spend', 'Corp Ads Spend', 'Self Ads ROAS', 'Corp Ads ROAS',
                       'Total Promo Sales', 'Total Ads Sales', 'Promo % of Sales', 'Ads % of Sales',
                       'New Customers (Self)', 'New Customers (Corp)']
    for r in rows:
        for k in list(r.keys()):
            if k.startswith('_') or k in (sub_col, net_col):
                r.pop(k, None)
        for col in all_metric_cols:
            if col not in r or pd.isna(r.get(col)):
                r[col] = 0
    return pd.DataFrame(rows)


def build_pivot_metrics_ue(ue_path, start_date, end_date, pivot_by, excluded_dates=None):
    """UberEats pivot metrics: no Corp/Self, only financial. pivot_by: list of 'Store', 'Slot', 'Days'."""
    df = _load_ue_by_date_range(ue_path, start_date, end_date, excluded_dates)
    if df.empty:
        return None
    date_col = df.columns[8] if len(df.columns) > 8 else None
    time_col = _find_ue_col(df, ['Order Accept Time', 'Order accept time']) or next((c for c in df.columns if 'accept' in c.lower()), None)
    store_col = _find_ue_col(df, ['Store Name', 'Store name']) or next((c for c in df.columns if 'store' in c.lower()), None)
    sales_col = _find_ue_col(df, ['Sales (excl. tax)', 'Total item sales excl tax']) or next((c for c in df.columns if 'sales' in c.lower()), None)
    payout_col = _find_ue_col(df, ['Total payout', 'Total payout ']) or next((c for c in df.columns if 'total payout' in c.lower()), None)
    if not sales_col or sales_col not in df.columns:
        return None
    df[sales_col] = pd.to_numeric(df[sales_col], errors='coerce').fillna(0)
    if payout_col and payout_col in df.columns:
        df[payout_col] = pd.to_numeric(df[payout_col], errors='coerce').fillna(0)
    else:
        payout_col = None
    df['Day Type'] = df[date_col].apply(get_day_type)
    df = df.dropna(subset=['Day Type'])
    if time_col:
        df['Slot'] = df[time_col].apply(get_time_slot)
        df = df.dropna(subset=['Slot'])
    else:
        df['Slot'] = 'All'
    store_id_col = _find_ue_col(df, ['Merchant store ID', 'Store ID', 'External store ID'])
    if not store_col:
        df['Store'] = 'All'
    else:
        if store_id_col and store_id_col in df.columns:
            df['Store'] = df[store_id_col].astype(str) + ' - ' + df[store_col].astype(str)
        else:
            df['Store'] = df[store_col].astype(str)
    order_col = 'Order ID' if 'Order ID' in df.columns else None

    if not pivot_by:
        pivot_by = ['Store']
    group_cols = []
    if 'Store' in pivot_by:
        group_cols.append('Store')
    if 'Slot' in pivot_by and 'Slot' in df.columns:
        group_cols.append('Slot')
    if 'Days' in pivot_by:
        group_cols.append('Day Type')

    agg_base = {sales_col: 'sum'}
    if payout_col:
        agg_base[payout_col] = 'sum'
    if order_col:
        agg_base[order_col] = 'nunique'
    if not group_cols:
        g = df.assign(_g=0).groupby('_g').agg(agg_base).reset_index()
        if order_col:
            g = g.rename(columns={order_col: 'Orders'})
        else:
            g['Orders'] = len(df)
        g['Sales'] = g[sales_col]
        g['Net Payout'] = g[payout_col] if payout_col and payout_col in g.columns else 0
        g['AOV'] = g['Sales'] / g['Orders'].replace(0, 1)
        g['Profitability'] = (g['Net Payout'] / g['Sales'].replace(0, 1) * 100).replace([float('inf'), float('-inf')], 0).fillna(0)
        g['Days'] = 'All'
        g['Store'] = 'All'
        g['Slot'] = 'All'
    else:
        agg = {sales_col: 'sum'}
        if payout_col:
            agg[payout_col] = 'sum'
        if order_col:
            agg[order_col] = 'nunique'
        g = df.groupby(group_cols).agg(agg).reset_index()
        if order_col:
            g = g.rename(columns={order_col: 'Orders'})
        else:
            g['Orders'] = df.groupby(group_cols).size().values
        g['Sales'] = g[sales_col]
        g['Net Payout'] = g[payout_col] if payout_col and payout_col in g.columns else 0
        g['AOV'] = g['Sales'] / g['Orders'].replace(0, 1)
        g['Profitability'] = (g['Net Payout'] / g['Sales'].replace(0, 1) * 100).replace([float('inf'), float('-inf')], 0).fillna(0)
        g = g.rename(columns={'Day Type': 'Days'}) if 'Day Type' in g.columns else g
        if 'Store' not in g.columns and 'Store' in pivot_by:
            g['Store'] = 'All'
        if 'Slot' not in g.columns and 'Slot' in pivot_by:
            g['Slot'] = 'All'
    out_cols = [c for c in ['Store', 'Slot', 'Days'] if c in g.columns] + ['Orders', 'Sales', 'Net Payout', 'AOV', 'Profitability']
    return g[[c for c in out_cols if c in g.columns]]

"""
New-store-app: Pivot by Store, Slot, Days. Metrics per group (Self vs Corp for DoorDash; no Corp for UberEats).
"""
import streamlit as st
import pandas as pd
from pathlib import Path
import tempfile

from data_processing import (
    load_promotion_data,
    load_sponsored_data,
    build_pivot_metrics_dd,
    build_pivot_metrics_ue,
)

st.set_page_config(
    page_title="New Store - Promotions Analysis",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("📊 Promotions Analysis")

with st.sidebar:
    st.header("📁 File Upload")
    dd_file = st.file_uploader("DoorDash financial", type=['csv'], key='dd_upload')
    ue_file = st.file_uploader("UberEats financial", type=['csv'], key='ue_upload')
    marketing_folder_path = st.text_input("Marketing folder path", placeholder="/path/to/marketing_*", key='mkt_path')
    mkt_promo_files = st.file_uploader("MARKETING_PROMOTION*.csv", type=['csv'], accept_multiple_files=True, key='mkt_promo')
    mkt_sponsored_files = st.file_uploader("MARKETING_SPONSORED_LISTING*.csv", type=['csv'], accept_multiple_files=True, key='mkt_sponsored')

    st.header("📅 Date range")
    analysis_mode = st.radio("Mode", ["Pre vs Post", "Period"], label_visibility="collapsed")
    if analysis_mode == "Pre vs Post":
        pre_start = st.text_input("Pre Start (MM/DD/YYYY)", value="11/01/2025", key='pre_start')
        pre_end = st.text_input("Pre End", value="11/30/2025", key='pre_end')
        post_start = st.text_input("Post Start", value="12/01/2025", key='post_start')
        post_end = st.text_input("Post End", value="12/31/2025", key='post_end')
        period_start, period_end = post_start, post_end
    else:
        period_start = st.text_input("Start (MM/DD/YYYY)", value="12/01/2025", key='period_start')
        period_end = st.text_input("End", value="12/31/2025", key='period_end')
        pre_start, pre_end, post_start, post_end = period_start, period_end, period_start, period_end

    st.header("Pivot by")
    pivot_by = st.multiselect("Group metrics by", ["Store", "Slot", "Days"], default=["Store", "Days"], key='pivot_by')

    st.divider()
    run_analysis = st.button("Start analysis", type="primary", use_container_width=True)

temp_dir = Path(tempfile.gettempdir()) / "new_store_app"
temp_dir.mkdir(exist_ok=True)


def save_uploaded_file(uploaded, name):
    if uploaded is None:
        return None
    p = temp_dir / name
    p.write_bytes(uploaded.getvalue())
    return p


def get_marketing_for_period():
    pre_promo, post_promo = pd.DataFrame(), pd.DataFrame()
    pre_sponsored, post_sponsored = pd.DataFrame(), pd.DataFrame()
    pc = {'campaign': 'Is self serve campaign', 'orders': 'Orders', 'sales': 'Sales',
          'spend': 'Customer discounts from marketing | (Funded by you)', 'new_customers': 'New customers acquired'}
    sc = {'campaign': 'Is self serve campaign', 'orders': 'Orders', 'sales': 'Sales',
          'spend': 'Marketing fees | (including any applicable taxes)'}

    if mkt_promo_files or mkt_sponsored_files:
        from utils import filter_excluded_dates
        post_s = pd.to_datetime(post_start, format='%m/%d/%Y').date()
        post_e = pd.to_datetime(post_end, format='%m/%d/%Y').date()
        for f in mkt_promo_files or []:
            df = pd.read_csv(f)
            df.columns = df.columns.str.strip()
            if 'Date' not in df.columns or not all(c in df.columns for c in [pc['campaign'], pc['orders'], pc['sales'], pc['spend']]):
                continue
            df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
            df = df.dropna(subset=['Date'])
            post_promo = pd.concat([post_promo, df[(df['Date'].dt.date >= post_s) & (df['Date'].dt.date <= post_e)]], ignore_index=True)
        for f in mkt_sponsored_files or []:
            df = pd.read_csv(f)
            df.columns = df.columns.str.strip()
            if 'Date' not in df.columns or not all(c in df.columns for c in [sc['campaign'], sc['orders'], sc['sales'], sc['spend']]):
                continue
            df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
            df = df.dropna(subset=['Date'])
            post_sponsored = pd.concat([post_sponsored, df[(df['Date'].dt.date >= post_s) & (df['Date'].dt.date <= post_e)]], ignore_index=True)
    else:
        mkt_root = Path(marketing_folder_path) if marketing_folder_path else temp_dir
        _, post_promo = load_promotion_data(mkt_root, pre_start, pre_end, post_start, post_end)
        _, post_sponsored = load_sponsored_data(mkt_root, pre_start, pre_end, post_start, post_end)
    return post_promo, post_sponsored


dd_path = save_uploaded_file(dd_file, "dd_data.csv") if dd_file else None
ue_path = save_uploaded_file(ue_file, "ue_data.csv") if ue_file else None
period_s = post_start if analysis_mode == "Pre vs Post" else period_start
period_e = post_end if analysis_mode == "Pre vs Post" else period_end

has_any_file = (dd_path and dd_path.exists()) or (ue_path and ue_path.exists())
if run_analysis:
    st.session_state.analysis_run = True
if st.session_state.get('analysis_run', False) and has_any_file:
    # ---------- DoorDash ----------
    st.header("🚪 DoorDash")
    if dd_path and dd_path.exists():
        if not pivot_by:
            st.warning("Select at least one pivot: Store, Slot, or Days.")
        else:
            try:
                post_promo, post_sponsored = get_marketing_for_period()
            except Exception:
                post_promo, post_sponsored = pd.DataFrame(), pd.DataFrame()
            try:
                pdf = build_pivot_metrics_dd(dd_path, post_promo, post_sponsored, period_s, period_e, pivot_by)
            except Exception as e:
                pdf = None
                st.warning(f"Could not process DoorDash data: {e}")
            if pdf is not None and not pdf.empty:
                disp = pdf.copy()
                dollar_cols = ['Sales', 'Net Payout', 'Self Promo Sales', 'Corp Promo Sales', 'Self Promo Spend', 'Corp Promo Spend',
                               'Self Ads Sales', 'Corp Ads Sales', 'Self Ads Spend', 'Corp Ads Spend', 'Total Promo Sales', 'Total Ads Sales', 'AOV']
                for c in dollar_cols:
                    if c in disp.columns:
                        disp[c] = disp[c].apply(lambda x: f"${float(x):,.2f}" if pd.notna(x) else "$0.00")
                for c in ['Profitability', 'Promo % of Sales', 'Ads % of Sales']:
                    if c in disp.columns:
                        disp[c] = disp[c].apply(lambda x: f"{float(x):.1f}%" if pd.notna(x) else "0%")
                for c in ['Self Promo ROAS', 'Corp Promo ROAS', 'Self Ads ROAS', 'Corp Ads ROAS']:
                    if c in disp.columns:
                        disp[c] = disp[c].apply(lambda x: f"{float(x):.2f}" if pd.notna(x) else "0.00")
                for c in ['Orders', 'New Customers (Self)', 'New Customers (Corp)']:
                    if c in disp.columns:
                        disp[c] = disp[c].apply(lambda x: f"{int(x):,}" if pd.notna(x) else "0")
                st.dataframe(disp, use_container_width=True, hide_index=True)
            else:
                st.info("No DoorDash data for the selected period or pivot.")
    else:
        st.info("Upload DoorDash financial file to see DoorDash metrics.")

    st.divider()

    # ---------- UberEats ----------
    st.header("🍔 UberEats")
    if ue_path and ue_path.exists():
        if not pivot_by:
            st.warning("Select at least one pivot: Store, Slot, or Days.")
        else:
            try:
                uedf = build_pivot_metrics_ue(ue_path, period_s, period_e, pivot_by)
            except Exception as e:
                uedf = None
                st.warning(f"Could not process UberEats data: {e}")
            if uedf is not None and not uedf.empty:
                disp = uedf.copy()
                for c in ['Sales', 'Net Payout', 'AOV']:
                    if c in disp.columns:
                        disp[c] = disp[c].apply(lambda x: f"${float(x):,.2f}" if pd.notna(x) else "$0.00")
                if 'Profitability' in disp.columns:
                    disp['Profitability'] = disp['Profitability'].apply(lambda x: f"{float(x):.1f}%" if pd.notna(x) else "0%")
                if 'Orders' in disp.columns:
                    disp['Orders'] = disp['Orders'].apply(lambda x: f"{int(x):,}" if pd.notna(x) else "0")
                st.dataframe(disp, use_container_width=True, hide_index=True)
            else:
                st.info("No UberEats data for the selected period or pivot.")
    else:
        st.info("Upload UberEats financial file to see UberEats metrics.")
else:
    st.info("Upload at least one file (DoorDash or UberEats), set date range and pivot, then click **Start analysis**.")

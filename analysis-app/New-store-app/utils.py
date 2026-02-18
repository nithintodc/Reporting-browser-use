"""Utility functions for New-store-app - same column definitions as main app"""
import pandas as pd
import streamlit as st
from pathlib import Path

DD_DATE_COLUMN_VARIATIONS = ['Timestamp local date', 'Timestamp Local Date', 'Timestamp Local date',
                             'timestamp local date', 'Date', 'date', 'Timestamp', 'timestamp']


def get_time_slot(time_str):
    """
    Categorize a time string into a slot.
    - Early morning: 12:00 AM – 4:59 AM
    - Breakfast: 5:00 AM – 10:59 AM
    - Lunch: 11:00 AM – 1:59 PM
    - Afternoon: 2:00 PM – 4:59 PM
    - Dinner: 5:00 PM – 7:59 PM
    - Late night: 8:00 PM – 11:59 PM
    """
    try:
        if pd.isna(time_str) or time_str == '':
            return None
        time_obj = pd.to_datetime(time_str, errors='coerce')
        if pd.isna(time_obj):
            return None
        hour = time_obj.hour
        minute = time_obj.minute
        total_minutes = hour * 60 + minute
        if total_minutes >= 0 and total_minutes < 300:      # 12am-4:59am
            return 'Early morning'
        elif total_minutes >= 300 and total_minutes < 660:  # 5am-10:59am
            return 'Breakfast'
        elif total_minutes >= 660 and total_minutes < 840:  # 11am-1:59pm
            return 'Lunch'
        elif total_minutes >= 840 and total_minutes < 1020: # 2pm-4:59pm
            return 'Afternoon'
        elif total_minutes >= 1020 and total_minutes < 1200: # 5pm-7:59pm
            return 'Dinner'
        elif total_minutes >= 1200:                         # 8pm-11:59pm
            return 'Late night'
        return None
    except Exception:
        return None


def get_day_type(dt):
    """Monday-Friday = Weekday, Saturday-Sunday = Weekend."""
    try:
        if pd.isna(dt):
            return None
        d = pd.to_datetime(dt, errors='coerce')
        if pd.isna(d):
            return None
        # 0=Monday, 6=Sunday
        wd = d.weekday()
        return 'Weekend' if wd >= 5 else 'Weekday'
    except Exception:
        return None


def filter_excluded_dates(df, date_col, excluded_dates):
    """Filter out excluded dates from a DataFrame."""
    if not excluded_dates or date_col not in df.columns or df.empty:
        return df
    df = df.copy()
    if not pd.api.types.is_datetime64_any_dtype(df[date_col]):
        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
    df = df.dropna(subset=[date_col])
    if df.empty:
        return df
    excluded_objs = []
    for d in excluded_dates:
        if isinstance(d, str):
            dt = pd.to_datetime(d, format='%m/%d/%Y', errors='coerce')
            if pd.notna(dt):
                excluded_objs.append(dt.date())
        elif hasattr(d, 'date'):
            excluded_objs.append(d.date())
        elif isinstance(d, pd.Timestamp):
            excluded_objs.append(d.date())
    if not excluded_objs:
        return df
    df['_date_only'] = df[date_col].dt.date
    df = df[~df['_date_only'].isin(excluded_objs)].drop(columns=['_date_only'])
    return df


def find_date_column(df, preferred_names):
    """Find date column by case-insensitive matching."""
    for name in preferred_names:
        if name in df.columns:
            return name
    cols_lower = {c.lower(): c for c in df.columns}
    for name in preferred_names:
        if name.lower() in cols_lower:
            return cols_lower[name.lower()]
    return None


def filter_by_date_range(df, date_col, start_date, end_date, excluded_dates=None):
    """Filter DataFrame by date range."""
    if df.empty or date_col not in df.columns:
        return pd.DataFrame()
    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
    df = df.dropna(subset=[date_col])
    if isinstance(start_date, str):
        start_dt = pd.to_datetime(start_date, format='%m/%d/%Y')
    else:
        start_dt = pd.to_datetime(start_date)
    if isinstance(end_date, str):
        end_dt = pd.to_datetime(end_date, format='%m/%d/%Y')
    else:
        end_dt = pd.to_datetime(end_date)
    df = df[(df[date_col] >= start_dt) & (df[date_col] <= end_dt)]
    if excluded_dates:
        df = filter_excluded_dates(df, date_col, excluded_dates)
    return df


def filter_master_file_by_date_range(file_path, start_date, end_date, date_col_variations, excluded_dates=None):
    """Load CSV and filter by date range. Same logic as main app utils."""
    try:
        is_ue = 'ue' in str(file_path).lower() or 'ubereats' in str(file_path).lower()
        if is_ue:
            df = pd.read_csv(file_path, skiprows=[0], header=0)
        else:
            df = pd.read_csv(file_path)
        df.columns = df.columns.str.strip()
        if is_ue:
            if len(df.columns) <= 8:
                return pd.DataFrame()
            actual_date_col = df.columns[8]
        else:
            actual_date_col = find_date_column(df, date_col_variations)
            if actual_date_col is None:
                return pd.DataFrame()
        df[actual_date_col] = pd.to_datetime(df[actual_date_col], errors='coerce')
        df = df.dropna(subset=[actual_date_col])
        if isinstance(start_date, str):
            start_dt = pd.to_datetime(start_date, format='%m/%d/%Y')
        else:
            start_dt = pd.to_datetime(start_date)
        if isinstance(end_date, str):
            end_dt = pd.to_datetime(end_date, format='%m/%d/%Y')
        else:
            end_dt = pd.to_datetime(end_date)
        df = df[(df[actual_date_col] >= start_dt) & (df[actual_date_col] <= end_dt)]
        if excluded_dates:
            df = filter_excluded_dates(df, actual_date_col, excluded_dates)
        return df
    except Exception as e:
        st.error(f"Error loading {Path(file_path).name}: {str(e)}")
        return pd.DataFrame()

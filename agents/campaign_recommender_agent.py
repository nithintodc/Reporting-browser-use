"""
CampaignRecommenderAgent: builds the Campaign recommendations sheet from store AOV.

Rules (per store):
  - B = MROUND(AOV, 5)  (min order for new-customer offer)
  - A = 20% if B > AOV else 15%  (discount for new customers)
  - Recommendation 1: "New customers {A}% off on min order of {B} upto Always lowest"
  - C = CEILING(AOV*1.2, 5)  (min order for all-customers offer)
  - Recommendation 2: "All customers 15% off on min order of {C} upto Always lowest"
"""

import logging
import math
from pathlib import Path
from typing import Optional, Union

logger = logging.getLogger(__name__)

try:
    import pandas as pd
except ImportError:
    pd = None


MERCHANT_STORE_ID_LABEL = "Merchant Store ID"


def build_recommendations(store_aov: "pd.DataFrame") -> "pd.DataFrame":
    """
    Build campaign recommendations table from a DataFrame with at least Merchant Store ID and AOV.

    Args:
        store_aov: DataFrame with columns "Merchant Store ID" or "Merchant store ID" and "AOV" (numeric).

    Returns:
        DataFrame with Merchant Store ID, AOV, Min order (new cust) B, Discount % (new cust) A,
        Recommendation 1, Min order (all cust) C, Recommendation 2.
    """
    if pd is None or store_aov is None or store_aov.empty:
        return pd.DataFrame() if pd else None
    if "AOV" not in store_aov.columns:
        logger.warning("CampaignRecommenderAgent: No AOV column in store_aov")
        return pd.DataFrame()
    store_col = (
        MERCHANT_STORE_ID_LABEL if MERCHANT_STORE_ID_LABEL in store_aov.columns
        else "Merchant store ID" if "Merchant store ID" in store_aov.columns
        else "Store ID" if "Store ID" in store_aov.columns
        else store_aov.columns[0]
    )
    out = store_aov[[store_col, "AOV"]].copy()
    out = out.rename(columns={store_col: MERCHANT_STORE_ID_LABEL})
    aov = out["AOV"].astype(float)
    # B = MROUND(AOV, 5)
    B = (aov / 5).round() * 5
    B = B.clip(lower=5)
    # A = 20 if B > AOV else 15
    A = (20 * (B > aov) + 15 * (B <= aov)).astype(int)
    # C = CEILING(AOV*1.2, 5)
    C = aov.apply(lambda x: math.ceil((float(x) * 1.2) / 5) * 5)
    C = C.clip(lower=5)
    out["Min order (new cust) B"] = B
    out["Discount % (new cust) A"] = A
    out["Recommendation 1"] = (
        "New customers " + A.astype(str) + "% off on min order of $" + B.astype(int).astype(str) + " upto Always lowest"
    )
    out["Min order (all cust) C"] = C
    out["Recommendation 2"] = (
        "All customers 15% off on min order of $" + C.astype(int).astype(str) + " upto Always lowest"
    )
    return out[
        [
            MERCHANT_STORE_ID_LABEL,
            "AOV",
            "Min order (new cust) B",
            "Discount % (new cust) A",
            "Recommendation 1",
            "Min order (all cust) C",
            "Recommendation 2",
        ]
    ]


def run(store_aov: Union["pd.DataFrame", Path]) -> Optional["pd.DataFrame"]:
    """
    Build campaign recommendations. Accepts either a DataFrame with Merchant Store ID and AOV,
    or a path to a CSV with those columns.

    Returns:
        DataFrame with campaign recommendations, or None on error.
    """
    if pd is None:
        return None
    if isinstance(store_aov, Path):
        if not store_aov.is_file():
            logger.warning("CampaignRecommenderAgent: File not found %s", store_aov)
            return None
        store_aov = pd.read_csv(store_aov)
    return build_recommendations(store_aov)

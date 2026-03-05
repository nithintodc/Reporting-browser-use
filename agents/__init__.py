"""Multi-agent automation for DoorDash: browser-use (login, reports, download, campaign) + analysis agents.

Main flow: Login → Report creation → Report download → Analysis → Campaign creation (subtotal + tags)
for all stores and all subtotals. Entry point: run_reports_then_analysis_then_campaign.
"""

from agents.doordash_agent import (
    run_reports_then_analysis_then_campaign,
    get_task_description_reports_only,
    get_task_description_campaign_for_subtotal_combo,
)
from agents.report_storage_agent import ReportStorageAgent

__all__ = [
    "run_reports_then_analysis_then_campaign",
    "get_task_description_reports_only",
    "get_task_description_campaign_for_subtotal_combo",
    "ReportStorageAgent",
]

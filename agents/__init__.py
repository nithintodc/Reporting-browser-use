"""Multi-agent automation for DoorDash: browser-use (login, reports, download, campaign) + analysis agents."""

from agents.doordash_agent import (
    run,
    run_reports_only,
    run_campaign_only,
    run_reports_then_analysis_then_campaign,
    get_task_description,
    get_task_description_reports_only,
    get_task_description_campaign_only,
    get_task_description_campaign_already_logged_in,
    get_task_description_campaign_for_combo,
)
from agents.report_storage_agent import ReportStorageAgent

__all__ = [
    "run",
    "run_reports_only",
    "run_campaign_only",
    "run_reports_then_analysis_then_campaign",
    "get_task_description",
    "get_task_description_reports_only",
    "get_task_description_campaign_only",
    "get_task_description_campaign_already_logged_in",
    "get_task_description_campaign_for_combo",
    "ReportStorageAgent",
]

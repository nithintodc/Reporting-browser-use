"""
Slack Agent: Pushes specific terminal steps to a designated Slack channel.
Uses standard Incoming Webhooks API.
"""
import logging
import os
import requests

logger = logging.getLogger(__name__)

def push_to_slack(message: str) -> None:
    """
    Sends a message to the Slack webhook URL defined in SLACK_WEBHOOK_URL.
    If the webhook is omitted, simply skips posting.
    """
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if not webhook_url or not webhook_url.strip():
        return
    
    try:
        payload = {"text": message}
        response = requests.post(webhook_url, json=payload, timeout=10)
        
        if response.status_code not in (200, 201):
            logger.warning(f"SlackAgent: Failed to push to Slack. HTTP {response.status_code}")
    except requests.exceptions.RequestException as e:
        logger.warning(f"SlackAgent: Error while pushing to Slack: {e}")

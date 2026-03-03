"""
Slack Agent: Pushes specific terminal steps to a designated Slack channel.
Uses standard Incoming Webhooks API.
"""
import logging
import os
from pathlib import Path

import requests

logger = logging.getLogger(__name__)

# Only log "webhook not set" once per process to avoid log spam
_slack_skipped_logged = False


def _get_webhook_url() -> str | None:
    """Return SLACK_WEBHOOK_URL, loading .env from project root if not set."""
    url = os.getenv("SLACK_WEBHOOK_URL")
    if url and url.strip():
        return url.strip()
    # If not set, try loading .env from project root (parent of agents/)
    try:
        from dotenv import load_dotenv
        env_path = Path(__file__).resolve().parent.parent / ".env"
        if env_path.is_file():
            load_dotenv(env_path)
            url = os.getenv("SLACK_WEBHOOK_URL")
            if url and url.strip():
                return url.strip()
    except Exception:
        pass
    return None


def push_to_slack(message: str) -> None:
    """
    Sends a message to the Slack webhook URL defined in SLACK_WEBHOOK_URL.
    If the webhook is omitted, skips posting and logs once that alerts are disabled.
    """
    global _slack_skipped_logged
    webhook_url = _get_webhook_url()
    if not webhook_url:
        if not _slack_skipped_logged:
            logger.warning(
                "Slack: SLACK_WEBHOOK_URL not set or empty — Slack alerts disabled. "
                "Set it in .env or ensure the app is run from the project root so .env is loaded."
            )
            _slack_skipped_logged = True
        return

    try:
        payload = {"text": message}
        response = requests.post(webhook_url, json=payload, timeout=10)

        if response.status_code in (200, 201):
            logger.debug("Slack: message sent")
        else:
            logger.warning(
                "SlackAgent: Failed to push to Slack. HTTP %s — %s",
                response.status_code,
                response.text[:200] if response.text else "",
            )
    except requests.exceptions.RequestException as e:
        logger.warning("SlackAgent: Error while pushing to Slack: %s", e)

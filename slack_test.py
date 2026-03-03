#!/usr/bin/env python3
"""
Test script for Slack webhook connection.
Sends a test message to the channel configured via SLACK_WEBHOOK_URL.
"""
import os
import sys

from dotenv import load_dotenv
import requests

load_dotenv()

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")
TEST_MESSAGE = "Slack connection test from Reporting-browser-use — if you see this, the webhook works."


def test_slack_connection() -> bool:
    """Send a test message to Slack and return True if successful."""
    if not SLACK_WEBHOOK_URL or not SLACK_WEBHOOK_URL.strip():
        print("SLACK_WEBHOOK_URL is not set in .env")
        return False

    try:
        payload = {"text": TEST_MESSAGE}
        response = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=10)

        if response.status_code in (200, 201):
            # Slack returns "ok" in the body on success
            body = (response.text or "").strip().lower()
            if body == "ok":
                print("Slack connection OK: test message sent to your channel.")
                return True
            print(f"Slack responded with status {response.status_code} but unexpected body: {response.text}")
            return False

        print(f"Slack webhook failed: HTTP {response.status_code}")
        if response.text:
            print(f"Response: {response.text}")
        return False

    except requests.exceptions.Timeout:
        print("Slack connection test timed out after 10s")
        return False
    except requests.exceptions.RequestException as e:
        print(f"Slack connection error: {e}")
        return False


if __name__ == "__main__":
    ok = test_slack_connection()
    sys.exit(0 if ok else 1)

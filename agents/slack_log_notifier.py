"""
Slack log notifier: watches terminal/log output and sends Slack messages when
specific signals appear. Does not modify the agent workflow — only consumes
log records and triggers Slack alerts.
"""
import logging
import os

from agents.slack_agent import push_to_slack

# Signals we look for in log messages (substring match) -> Slack message to send.
# Each pattern is only triggered once per process (deduplicated).
_SIGNALS = [
    # (pattern in log message, lambda email -> message or static message)
    ("Login was successful", lambda email: f"I logged into {email}"),
    # New campaign system: mappings pushed to combined analysis
    ("campaign mapping(s) to sheet", lambda _: "Campaign mappings pushed to combined analysis sheet."),
    # Phase 2 started (campaigns from combined_analysis or slots)
    ("Phase 2 —", lambda _: "Phase 2: campaign creation started."),
]

_sent_signals: set[str] = set()


class SlackLogNotifierHandler(logging.Handler):
    """
    Logging handler that watches for known terminal/log signals and sends
    corresponding Slack messages. Each signal is sent at most once per run.
    """

    def __init__(self, doordash_email: str = "", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.doordash_email = (doordash_email or os.getenv("DOORDASH_EMAIL") or "").strip()

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = record.getMessage() or ""
            for pattern, message_fn in _SIGNALS:
                if pattern in msg and pattern not in _sent_signals:
                    _sent_signals.add(pattern)
                    if callable(message_fn):
                        text = message_fn(self.doordash_email)
                    else:
                        text = message_fn
                    if text:
                        push_to_slack(text)
                    break
        except Exception:
            self.handleError(record)


def install_slack_log_notifier(doordash_email: str = "") -> None:
    """
    Add a handler to the root logger so that terminal/log signals (e.g. from
    browser_use) trigger Slack messages. Call once at app startup after
    load_dotenv() and optionally pass DOORDASH_EMAIL for messages that need it.
    """
    email = (doordash_email or os.getenv("DOORDASH_EMAIL") or "").strip()
    handler = SlackLogNotifierHandler(doordash_email=email)
    handler.setLevel(logging.DEBUG)
    logging.getLogger().addHandler(handler)

"""
ReportStorageAgent: verifies downloaded file exists, renames to
doordash_financial_report_2026_01.csv, logs success.
"""

import logging
from pathlib import Path

logger = logging.getLogger(__name__)

FINAL_FILENAME = "doordash_financial_report_2026_01.csv"


class ReportStorageAgent:
    """Verifies and renames the downloaded financial report."""

    def __init__(self, download_dir: Path) -> None:
        self.download_dir = Path(download_dir)

    def process(self, downloaded_path: Path) -> Path:
        """
        Verify file exists, rename to doordash_financial_report_2026_01.csv.
        Returns path to the final file.
        """
        path = Path(downloaded_path)
        if not path.is_file():
            raise FileNotFoundError(f"Downloaded file not found: {path}")

        final_path = self.download_dir / FINAL_FILENAME
        if path.resolve() != final_path.resolve():
            path.rename(final_path)
            logger.info("ReportStorageAgent: Renamed to %s", final_path)
        else:
            logger.info("ReportStorageAgent: File already named %s", final_path)
        logger.info("ReportStorageAgent: Success — report saved as %s", final_path)
        return final_path

#!/usr/bin/env python3
"""
Open and validate the downloaded DoorDash report (ZIP or CSV).
Usage:
  python scripts/check_downloaded_file.py [path]
  python scripts/check_downloaded_file.py
If no path given, uses the most recent file in ./downloads.
"""

import csv
import sys
import zipfile
from pathlib import Path

DOWNLOADS = Path(__file__).resolve().parent.parent / "downloads"


def is_zip(path: Path) -> bool:
    with open(path, "rb") as f:
        return f.read(4) == b"PK\x03\x04"


def main() -> None:
    if len(sys.argv) > 1:
        path = Path(sys.argv[1]).resolve()
    else:
        if not DOWNLOADS.exists():
            print(f"Downloads folder not found: {DOWNLOADS}")
            sys.exit(1)
        files = sorted(DOWNLOADS.iterdir(), key=lambda p: p.stat().st_mtime, reverse=True)
        if not files:
            print(f"No files in {DOWNLOADS}")
            sys.exit(1)
        path = files[0]
        print(f"Using latest file: {path.name}\n")

    if not path.is_file():
        print(f"Not a file: {path}")
        sys.exit(1)

    print(f"File: {path}")
    print(f"Size: {path.stat().st_size:,} bytes")
    print()

    if is_zip(path):
        print("Type: ZIP archive (DoorDash often sends report as ZIP containing CSV)")
        with zipfile.ZipFile(path, "r") as z:
            names = z.namelist()
            print(f"Contents ({len(names)} item(s)): {names}")
            for name in names:
                if name.lower().endswith(".csv"):
                    print(f"\n--- First 10 lines of {name} (inside ZIP) ---")
                    with z.open(name) as f:
                        raw = f.read()
                    try:
                        text = raw.decode("utf-8")
                    except UnicodeDecodeError:
                        text = raw.decode("utf-8", errors="replace")
                    lines = text.splitlines()[:10]
                    for i, line in enumerate(lines, 1):
                        print(f"  {i}: {line[:200]}{'...' if len(line) > 200 else ''}")
                    print(f"  ... (total lines in CSV: {len(text.splitlines()):,})")
        return

    # Treat as CSV
    print("Type: CSV (plain text)")
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            sample = f.read(8192)
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            line_count = sum(1 for _ in f)
    except Exception as e:
        print(f"Could not read as text: {e}")
        print("File may be binary (e.g. ZIP saved with .csv extension). Try unzipping manually.")
        return

    print(f"Line count: {line_count:,}")
    print("\n--- First 10 lines ---")
    for i, line in enumerate(sample.splitlines()[:10], 1):
        print(f"  {i}: {line[:200]}{'...' if len(line) > 200 else ''}")

    # Try CSV dialect
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            reader = csv.reader(f)
            row0 = next(reader)
            print(f"\nCSV columns ({len(row0)}): {row0[:8]}{'...' if len(row0) > 8 else ''}")
    except Exception as e:
        print(f"\nCSV parse note: {e}")


if __name__ == "__main__":
    main()

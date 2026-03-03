#!/usr/bin/env python3
"""
C1: Source Arrival Detection

Connects to BigQuery and verifies that every expected source table exists
in the `raw` dataset.  If any table is missing it prints a mock Slack alert
and exits with code 1 so that the GitHub Action (or any orchestrator) halts
before dbt build runs.

Usage:
    python scripts/check_source_arrival.py
"""

import sys
from datetime import datetime, timezone

from google.cloud import bigquery

PROJECT_ID = "guys-nhs"
DATASET = "raw"

EXPECTED_TABLES = [
    "patients",
    "encounters",
    "encounters_schema_change_batch",
    "conditions",
    "observations",
    "medications",
    "clinical_notes",
]


def get_existing_tables(client: bigquery.Client) -> set[str]:
    """Return the set of table names present in the raw dataset."""
    tables = client.list_tables(f"{PROJECT_ID}.{DATASET}")
    return {t.table_id for t in tables}


def mock_slack_alert(missing: list[str]) -> None:
    """Print a formatted mock Slack alert."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print("MOCK SLACK ALERT")
    print("=" * 60)
    print(f"Channel : #data-pipeline-alerts")
    print(f"Time    : {now}")
    print(f"Severity: CRITICAL")
    print(f"Message : Source arrival failure — tables missing from `{PROJECT_ID}.{DATASET}`.")
    print(f"Missing : {', '.join(missing)}")
    print(f"Action  : dbt run has NOT been triggered.")


def main() -> int:
    client = bigquery.Client(project=PROJECT_ID)
    existing = get_existing_tables(client)

    missing = [t for t in EXPECTED_TABLES if t not in existing]

    if missing:
        print(f"ERROR: {len(missing)} source table(s) missing from `{PROJECT_ID}.{DATASET}`:")
        for t in missing:
            print(f"  - {t}")
        mock_slack_alert(missing)
        return 1

    print(f"OK: All {len(EXPECTED_TABLES)} expected source tables present in `{PROJECT_ID}.{DATASET}`.")
    for t in EXPECTED_TABLES:
        print(f" - {t}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""
C1: Source Freshness Detection

Connects to BigQuery and checks that key source tables contain data that
is recent enough to be considered fresh.  If any table's most recent
record is older than the configured threshold, it prints a mock Slack
alert and exits with code 1.

Usage:
    python scripts/check_source_freshness.py
"""

import sys
from datetime import datetime, timezone, timedelta

from google.cloud import bigquery

PROJECT_ID = "guys-nhs"
DATASET = "raw"

# Each entry: (table_name, timestamp_column, max_age_hours, is_epoch_millis)
FRESHNESS_CHECKS = [
    ("encounters", "START", 168, False),             # 7 days
    ("encounters_schema_change_batch", "START", 168, True),  # epoch milliseconds
    ("conditions", "START", 168, False),
    ("observations", "DATE", 168, False),
    ("medications", "START", 168, False),
]


def check_freshness(client: bigquery.Client) -> list[dict]:
    """Return a list of stale tables with details."""
    stale = []
    now = datetime.now(timezone.utc)

    for table, column, max_age_hours, is_epoch_millis in FRESHNESS_CHECKS:
        if is_epoch_millis:
            expr = f"MAX(TIMESTAMP_MILLIS(CAST(`{column}` AS INT64)))"
        else:
            expr = f"MAX(CAST(`{column}` AS TIMESTAMP))"
        query = f"""
            SELECT {expr} AS latest
            FROM `{PROJECT_ID}`.`{DATASET}`.`{table}`
        """
        try:
            result = client.query(query).result()
            row = next(iter(result))
            latest = row.latest

            if latest is None:
                stale.append({
                    "table": table,
                    "reason": "no rows or all NULLs",
                    "latest": None,
                    "threshold_hours": max_age_hours,
                })
            else:
                if latest.tzinfo is None:
                    latest = latest.replace(tzinfo=timezone.utc)
                age = now - latest
                age_hours = age.total_seconds() / 3600
                if age_hours > max_age_hours:
                    stale.append({
                        "table": table,
                        "reason": f"latest record is {age.days}d {age.seconds // 3600}h old (threshold: {max_age_hours}h)",
                        "latest": latest.isoformat(),
                        "threshold_hours": max_age_hours,
                    })
        except Exception as e:
            stale.append({
                "table": table,
                "reason": f"query failed: {e}",
                "latest": None,
                "threshold_hours": max_age_hours,
            })

    return stale


def mock_slack_alert(stale: list[dict]) -> None:
    """Print a formatted mock Slack alert for stale data."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print("MOCK SLACK ALERT")
    print("=" * 60)
    print(f"Channel : #data-pipeline-alerts")
    print(f"Time    : {now}")
    print(f"Severity: WARNING")
    print(f"Message : Stale source data detected in `{PROJECT_ID}.{DATASET}`.")
    print(f"Tables  :")
    for entry in stale:
        print(f"  - {entry['table']}: {entry['reason']}")
    print(f"Action  : Review source ingestion before trusting downstream models.")


def main() -> int:
    client = bigquery.Client(project=PROJECT_ID)

    stale = check_freshness(client)

    if stale:
        print(f"WARNING: {len(stale)} source table(s) have stale or missing data:")
        for entry in stale:
            print(f"  - {entry['table']}: {entry['reason']}")
        mock_slack_alert(stale)
        return 1

    print(f"OK: All {len(FRESHNESS_CHECKS)} checked source tables have fresh data.")
    for table, _, max_age_hours, _ in FRESHNESS_CHECKS:
        print(f"  - {table} (threshold: {max_age_hours}h)")
    return 0


if __name__ == "__main__":
    sys.exit(main())

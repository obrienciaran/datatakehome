"""Load raw CSV files into BigQuery `raw` dataset.

Usage:
    python load_to_bigquery.py [--project PROJECT_ID]

Arguments:
    --project GCP project ID (default: guys-nhs)

Examples:
    # Run with the default project
    python load_to_bigquery.py

    # Run with a specific project
    python load_to_bigquery.py --project my-gcp-project

Requirements:
    Authenticate with Google Cloud before running:
        gcloud auth application-default login
"""

import argparse
from pathlib import Path

import pandas as pd
from google.cloud import bigquery

DATA_DIR = Path(__file__).resolve().parent.parent / "data"

CSV_TO_TABLE = {
    "patients.csv": "patients",
    "encounters.csv": "encounters",
    "encounters_schema_change_batch.csv": "encounters_schema_change_batch",
    "conditions.csv": "conditions",
    "observations.csv": "observations",
    "medications.csv": "medications",
    "clinical_notes.csv": "clinical_notes",
}


def load_csvs(project_id: str) -> None:
    client = bigquery.Client(project=project_id)

    dataset_ref = bigquery.DatasetReference(project_id, "raw")
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = "europe-west2"
    client.create_dataset(dataset, exists_ok=True)

    for csv_name, table_name in CSV_TO_TABLE.items():
        csv_path = DATA_DIR / csv_name
        print(f"Loading {csv_path.name} -> raw.{table_name} ...")

        df = pd.read_csv(csv_path)

        table_id = f"{project_id}.raw.{table_name}"
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=True,
        )

        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()

        table = client.get_table(table_id)
        print(f"  {table.num_rows} rows loaded into {table_id}")

    print("\nAll tables loaded successfully.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load CSV files into BigQuery raw dataset")
    parser.add_argument("--project", default="guys-nhs", help="GCP project ID (default: guys-nhs)")
    args = parser.parse_args()
    load_csvs(args.project)

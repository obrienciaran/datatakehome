"""Extract primary disorder from ED triage notes using Gemini.

Reads clinical_notes from BigQuery, sends the free-text NOTE_TEXT to
gemini-2.5-flash-lite, and writes the results back to BigQuery as a new
table: raw.clinical_notes_enriched.
Note: this overwrites the existing table. This is just for the takehome test so acceptable.

Usage:
    python -m llm.transform [--project guys-nhs] [--rows 5]
"""

import argparse
import os
import time

import google.generativeai as genai
import pandas as pd
from google.cloud import bigquery

from llm.base import Transform

PROJECT_ID_DEFAULT = "guys-nhs"
DATASET = "raw"
SOURCE_TABLE = "clinical_notes"
DEST_TABLE = "clinical_notes_enriched"
MODEL_NAME = "gemini-2.5-flash-lite"

PROMPT_TEMPLATE = (
    "You are a clinical NLP system. Given the following ED triage note, "
    "extract the single most likely PRIMARY DISORDER the patient is presenting with. "
    "Respond with ONLY the disorder name in plain English (e.g. 'Urinary tract infection', "
    "'Acute coronary syndrome', 'Viral upper respiratory tract infection'). "
    "If you cannot determine a primary disorder, respond with 'Unknown'.\n\n"
    "Triage note:\n{note_text}"
)


class ClinicalNoteTransform(Transform):
    """Extract primary disorder from free-text ED triage notes via Gemini."""

    def __init__(self, project_id: str, num_rows: int = 5) -> None:
        self.project_id = project_id
        self.num_rows = num_rows

        api_key = os.environ.get("GEMINI_API_KEY")
        if not api_key:
            raise EnvironmentError("GEMINI_API_KEY environment variable is not set.")
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel(MODEL_NAME)
        self.bq_client = bigquery.Client(project=project_id)

    def _read_source(self) -> pd.DataFrame:
        query = (
            f"SELECT * FROM `{self.project_id}.{DATASET}.{SOURCE_TABLE}` "
            f"LIMIT {self.num_rows}"
        )
        print(f"Reading top {self.num_rows} rows from {DATASET}.{SOURCE_TABLE} ...")
        return self.bq_client.query(query).to_dataframe()

    def _extract_disorder(self, note_text: str) -> str:
        prompt = PROMPT_TEMPLATE.format(note_text=note_text)
        response = self.model.generate_content(prompt)
        return response.text.strip()

    def run(self, df: pd.DataFrame | None = None) -> pd.DataFrame:
        if df is None:
            df = self._read_source()

        disorders: list[str] = []
        for idx, row in df.iterrows():
            note = row["NOTE_TEXT"]
            print(f"[{idx}] Extracting disorder from note ({len(note)} chars) ...")
            disorder = self._extract_disorder(note)
            print(f"-> {disorder}")
            disorders.append(disorder)
            time.sleep(0.5)  # respect free-tier rate limits

        df = df.copy()
        df["PRIMARY_DISORDER"] = disorders
        return df

    def write_to_bigquery(self, df: pd.DataFrame) -> None:
        table_id = f"{self.project_id}.{DATASET}.{DEST_TABLE}"
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=True,
        )
        print(f"Writing {len(df)} rows to {table_id} ...")
        job = self.bq_client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()
        print(f"  Done. {len(df)} rows written to {table_id}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract primary disorder from clinical notes via Gemini")
    parser.add_argument("--project", default=PROJECT_ID_DEFAULT, help="GCP project ID")
    parser.add_argument("--rows", type=int, default=5, help="Number of rows to process (default: 5)")
    args = parser.parse_args()

    transform = ClinicalNoteTransform(project_id=args.project, num_rows=args.rows)
    enriched = transform.run()
    transform.write_to_bigquery(enriched)


if __name__ == "__main__":
    main()

# Data Engineering — Ciaran O'Brien - Take-Home Assessment

## Overview

### Part A Data Loading

Examine `load`. Data is loaded from the repo and uploaded to Big Query using Python

### Part B Data Assessment

Examine `dbt`. The raw data is loaded from the repo and used as a `source` for dbt. Some simple cleaning is applied in the `staging` layer. Two `mart` layer tables as requested in Part C are created.

The dbt run is contained in Docker for reproducibility and safety, and runs daily with a cron job via GitHub Actions at 06:00 each morning. A GitHub secret was created with `BigQuery Data Editor` and `BigQuery Job User` roles. Before the build, checks for stale data and freshness are run using Python and dbt. This is to satisfy C1 and C2 in `Part C`, see [here](https://github.com/obrienciaran/datatakehome/tree/main/dbt).

Table level and column level documentation has been added. Column level tests have been added.

See [`notebooks/readme.md`](notebooks/readme.md) for the written answers to `B1` and `B2`, and [`notebooks/data_quality_assessment.ipynb`](notebooks/data_quality_assessment.ipynb) for the full exploratory analysis

### Part C Transformation Pipeline

The "ED Length of Stay" and "Frequent Attenders" tables have been added as `mart` layer tables. Checks for source data and silent failures have been put in place, whereby `dbt run` cannot triggered, and a (fake) Slack alert is sent.

### Bonus

Simply, the `clinical_notes.csv` is loaded from Big Query, then `Gemini-2.5-Flash-Lite` is applied to extract the `primary disorder` from the table, and the results saved back to Big Query. I used a free tier of the Gemini API and only ran 2 rows for demonstration purposes. Here I used Abstract Base Classes to ensure code consistency, and also put placeholders for future LLM work such as augmenting or synthesising data.
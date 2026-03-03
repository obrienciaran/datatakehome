# Data Engineering — Take-Home Assessment

## Overview

### Part A Data Loading

Examine `load`. Data is loaded from the repo and uploaded to Big Query using Python

### Part B Data Assessment

Examine `dbt`. The raw data is used as a `source` for DBT. Some simple cleaning is applied in the `staging` layer. Two `mart` layer tables as requested in Part C are created.

The DBT run is contained in Docker for reproducibility and safety, and ran daily with a cron job via Github actions at 06:00 each morning. A github secret was created with `Big Query Data Editor` and `Big Query Job User` roles. Before the build, checks for stale data and freshness are ran using Python and DBT before `dbt run` can happen. This is to satisfy C1 and C2 in `Part C`, see [here](https://github.com/obrienciaran/datatakehome/tree/main/dbt)

Table level and column level documentation has been added. Column level tests have been added.

See `notebooks/data_quality_assessment.ipynb` for the details of `B1` and `B2`

### Part C Transformation Pipeline

The "ED Length of Stay" and "Frequent Attenders" tables have been added as `mart` layer tables. Checks for source data and silent failures have been put in place, whereby `dbt run` cannot triggered, and a (fake) Slack alert is sent.

### Bonus

Simply, the `clinical_notes.csv` is loaded from Big Query, then Gemini-2.5-Flash-Lite is applied to extract the `primary disorder` from the table, and the results saved back to Big Query. I used a free tier of the Gemini API and only ran 2 rows for demonstration purposes. Here I used Abstract Base Classes to ensure code consistency, and also put placeholders for future LLM work such as augmenting or synthesising data.
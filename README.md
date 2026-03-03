# Data Engineering — Ciaran O'Brien - Take-Home Assessment

## Overview

### Part A Data Loading

Examine `load`. Data is loaded from the repo and uploaded to Big Query using Python.

### Part B Data Assessment

Examine `dbt`. The raw data is used as a `source` for dbt and loaded from the repo. Some simple cleaning is applied in the `staging` layer. Two `mart` layer tables as requested in Part C are created.

The daily production run is containerised in Docker for reproducibility. A pinned image ensures the dbt version and dependencies are consistent across every run. The image is built and pushed to GitHub Container Registry (GHCR) automatically when the `Dockerfile` or `requirements.txt` changes on `main`, and is pulled at runtime each morning at 06:00 with a cronjob in a Github action. Note that the CI pipeline installs dbt directly via pip and does not use Docker. A GitHub secret was created with `BigQuery Data Editor` and `BigQuery Job User` roles.

**CI on pull requests:** Any PR touching `dbt/**` triggers two jobs. First, `dbt parse` runs as a compile check to catch syntax errors early. If that passes, `dbt build` (models + tests) runs against an isolated, ephemeral BigQuery dataset named after the PR number (e.g. `ci_pr_42`). To keep CI fast and cost-effective as the pipeline scales, the built-in dbt `source()` macro is overridden to apply `TABLESAMPLE SYSTEM (1 PERCENT)` when `target == ci` — giving a representative random sample transparently, without any changes needed in the staging models themselves. The dataset is torn down once the job completes, regardless of outcome. This ensures broken models and failing tests are caught before merging.

**Daily cron job:** A scheduled run fires at 06:00 UTC each morning. Before `dbt build` can run, two Python pre-flight scripts check that source data has arrived and is fresh, and `dbt source freshness` is also run. Only once all checks pass does `dbt build` execute. This satisfies C1 and C2 in `Part C`, see [here](https://github.com/obrienciaran/datatakehome/tree/main/dbt).

Table level and column level documentation has been added. Column level tests have been added.

See [`notebooks/readme.md`](notebooks/readme.md) for the written answers to `B1` and `B2`, and [`notebooks/data_quality_assessment.ipynb`](notebooks/data_quality_assessment.ipynb) for the full exploratory analysis

### Part C Transformation Pipeline

The "ED Length of Stay" and "Frequent Attenders" tables have been added as `mart` layer tables. Checks for source data and silent failures have been put in place, whereby `dbt run` cannot triggered, and a (fake) Slack alert is sent.

### Bonus

Simply, the `clinical_notes.csv` is loaded from Big Query, then `Gemini-2.5-Flash-Lite` is applied to extract the `primary disorder` from the table, and the results saved back to Big Query. I used a free tier of the Gemini API and only ran 2 rows for demonstration purposes. Here I used Abstract Base Classes to ensure code consistency, and also put placeholders for future LLM work such as augmenting or synthesising data.

<img width="891" height="460" alt="Screenshot 2026-03-03 at 16 17 16" src="https://github.com/user-attachments/assets/d26fca55-1aef-471e-a4f3-711080245ebf" />

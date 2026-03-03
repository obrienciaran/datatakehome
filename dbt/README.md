# dbt – NHS ED Pipeline

## Setup

Install dbt and the BigQuery adapter:

```bash
pip install dbt-bigquery
```

Install dbt package dependencies:

```bash
cd dbt
dbt deps
```

## Running

```bash
dbt run
```

To run tests:

```bash
dbt test
```

---

## C1: Source Arrival Detection

> *How would you detect when a source data file doesn't arrive, and what happens to downstream tables?*

### The Issue

In a production pipeline, upstream source tables (CSV uploads, EHR extracts, etc.) may fail to arrive due to network issues, vendor outages, or process failures. If we proceed with a `dbt run` when source data is missing, downstream staging and mart tables will either fail with SQL errors or silently produce empty or stale results.

If a source table is absent and the pipeline runs anyway:

- **Staging models** that `SELECT FROM` the missing source will fail with a "table not found" SQL error.
- **Mart models** that depend on those staging models will also fail or be skipped.
- If the source table *exists* but hasn't been refreshed (stale data), all downstream models will succeed but produce outdated results with no visible error, i.e. a silent failure.

### How We Detect It

Here we use three layers of defence:

#### 1. `on-run-start` hook: block the run if sources are missing

The macro `check_source_tables_exist` (in `macros/check_source_tables_exist.sql`) runs at the start of every `dbt run`. It queries BigQuery `INFORMATION_SCHEMA.TABLES` for all expected source tables. If any are missing, it raises a compiler error that aborts the entire dbt run before any model is built:

```
SOURCE ARRIVAL FAILURE – the following expected source tables are missing
from `guys-nhs.raw`: encounters.  The dbt run has been aborted…
```

This is configured in `dbt_project.yml`:

```yaml
on-run-start:
  - "{{ check_source_tables_exist() }}"
```

#### 2. `dbt source freshness`: detect stale data

Even if a table exists, the data inside may be stale. We configure source freshness on the encounters table in `sources.yml`, using the `START` column (encounter start timestamp)
as a proxy for when data was loaded since the raw data doesn't have a dedicated `loaded_at` or `updated_at` column:

```yaml
- name: encounters
  loaded_at_field: "CAST(START AS TIMESTAMP)"
  freshness:
    warn_after: {count: 48, period: hour}
    error_after: {count: 168, period: hour}
```

Running `dbt source freshness` will warn if the most recent encounter is older than 48 hours and error if older than 7 days. In production, this would run on a schedule before `dbt run`.

```bash
dbt source freshness
```

#### 3. Python pre-flight script: orchestrator integration with alerting

The script `scripts/check_source_freshness.py` connects to BigQuery, checks for all expected tables, and if any are missing:

- Prints a mock Slack alert with channel, timestamp, and missing table names
- Exits with code 1, which an orchestrator (Airflow, cron) can use to halt the pipeline

```bash
python scripts/check_source_freshness.py
```

---

## C2: Silent Failure Detection

> *How would you detect silent failures where the pipeline succeeds but the output data is wrong?*

### The Problem

A pipeline can complete with exit code 0 while producing incorrect results. Examples:

- A broken join drops all rows, producing an empty mart table
- A schema change renames a column, causing `NULL` values everywhere
- Timestamp parsing goes wrong, producing wildly incorrect date calculations
- A filter condition silently excludes all data

Everything looks fine from the pipeline's perspective, but these are failures for the users.

### How We Detect It

We use three complementary approaches:

#### 1. `on-run-end` row count validation

The macro `validate_row_counts` (in `macros/validate_row_counts.sql`) runs after every `dbt run`. It checks that each mart table:

- Has **more than zero rows** (catches broken joins, empty results)
- Has **no more than 10x** the row count of `stg_encounters` (catches fan-outs from bad joins — e.g. a missing or non-unique join key can multiply rows, so a mart with 500,000 rows against 10,000 encounters is a clear signal that a join is producing duplicate combinations)

```yaml
on-run-end:
  - "{{ validate_row_counts() }}"
```

#### 2. Generic column test: frequent attender flag consistency (`marts.yml`)

A column-level generic test that checks `is_frequent_attender` is correctly derived from `visits_rolling_12m` for every row. This test validates the derivation logic, and if any row has a mismatch, e.g. the flag says `false` but the count is 5, then DBT will return those rows and the test fails.

```yaml
- name: is_frequent_attender
  tests:
    - dbt_utils.expression_is_true:
        expression: "= (visits_rolling_12m >= 3)"
```

#### 3. Singular test: length of stay within bounds (`tests/assert_los_within_bounds.sql`)

Checks that the **median** length of stay falls within a clinically plausible range (> 0 hours and < 720 hours / 30 days). This catches:

- Timestamp corruption (e.g., epoch milliseconds parsed as seconds -> Length of stay of thousands of hours)
- All-null length of stay values
- Negative durations from swapped start/stop timestamps

```bash
dbt test --select assert_los_within_bounds
```

### When Each Check Runs

**Before the daily 06:00 cron job (pre-flight gates):**

These run on the GitHub Actions runner *before* dbt starts. If any fail, the pipeline halts and dbt never touches BigQuery:

```bash
python scripts/check_source_arrival.py # verifies all expected source tables exist
python scripts/check_source_freshness.py # verifies source data is recent enough
```

**During `dbt build` (built into the dbt run):**

```bash
dbt build # runs models + tests in dependency order
```

- `on-run-start`: `check_source_tables_exist()` — blocks the run if any source table is missing
- `on-run-end`: `validate_row_counts()` — warns if any mart has 0 rows or >10x the encounter count
- All generic tests (unique, not_null, accepted_values, expression_is_true) run after each model

**On pull requests (CI):**

These run in the `dbt CI` GitHub Action when a PR touches `dbt/**`:

```bash
dbt parse # validates SQL compiles (no credentials needed)
dbt build # runs models + tests against an ephemeral CI dataset
```

To keep CI fast and cost-effective, the built-in `source()` macro is overridden in `macros/ci_limit.sql` to apply `TABLESAMPLE SYSTEM (1 PERCENT)` on all raw source tables when `target == ci`. Staging models require no changes — the sampling is applied transparently.

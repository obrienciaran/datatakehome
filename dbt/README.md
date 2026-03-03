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

#### 3. Python pre-flight scripts: orchestrator integration with alerting

Two Python scripts run as gates in the GitHub Action before `dbt build`:

**`scripts/check_source_arrival.py`** — verifies all expected source tables exist in BigQuery. If any are missing, it prints a mock Slack alert and exits with code 1, halting the workflow.

```bash
python scripts/check_source_arrival.py
```

**`scripts/check_source_freshness.py`** — queries the most recent timestamp in each source table and checks it against a staleness threshold (e.g., 168 hours / 7 days). If any table's data is too old, it prints a mock Slack alert and exits with code 1.

```bash
python scripts/check_source_freshness.py
```

In the daily GitHub Action (`dbt_daily_run.yml`), these run in sequence before `dbt build`:

1. `check_source_arrival.py` — are the tables there?
2. `check_source_freshness.py` — is the data inside them recent?
3. `dbt source freshness` — dbt's native freshness check
4. `dbt build` — only runs if all of the above pass

In production, the mock Slack alerts would be replaced by real webhook calls.

---

## C2: Silent Failure Detection

> *How would you detect silent failures where the pipeline succeeds but the output data is wrong?*

### The Problem

A pipeline can complete with exit code 0 while producing incorrect results. Examples:

- A broken join drops all rows, producing an empty mart table
- A schema change renames a column, causing `NULL` values everywhere
- Timestamp parsing goes wrong, producing wildly incorrect date calculations
- A filter condition silently excludes all data

These are the hardest failures to catch because everything *looks* fine from the pipeline's perspective.

### How We Detect It

We implement three complementary approaches:

#### 1. `on-run-end` row count validation

The macro `validate_row_counts` (in `macros/validate_row_counts.sql`) runs after every `dbt run`. It checks that each mart table:

- Has **more than zero rows** (catches broken joins, empty results)
- Has a **plausible ratio** to the staging encounter count (catches fan-outs from bad joins)

```yaml
on-run-end:
  - "{{ validate_row_counts() }}"
```

#### 2. Singular test: frequent attenders exist (`tests/assert_frequent_attenders_exist.sql`)

A domain-specific assertion: the `frequent_attenders` mart must contain at least one row where `is_frequent_attender = true`. If a schema change or join bug eliminated all frequent attenders, this test catches it:

```bash
dbt test --select assert_frequent_attenders_exist
```

#### 3. Singular test: length of stay within bounds (`tests/assert_los_within_bounds.sql`)

Checks that the **median length of stay** falls within a clinically plausible range (> 0 hours and < 720 hours / 30 days). This catches:

- Timestamp corruption (e.g., epoch milliseconds parsed as seconds → LOS of thousands of hours)
- All-null LOS values
- Negative durations from swapped start/stop timestamps

```bash
dbt test --select assert_los_within_bounds
```

### Running All Checks

```bash
# Full pipeline with built-in source and output validation
dbt run          # on-run-start checks sources; on-run-end checks row counts
dbt test         # runs all generic + singular tests

# Standalone checks
dbt source freshness                          # dbt native staleness check
python scripts/check_source_arrival.py        # pre-flight: missing tables
python scripts/check_source_freshness.py      # pre-flight: stale data
```

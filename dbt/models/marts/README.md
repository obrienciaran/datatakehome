# Mart Layer

Analyst-ready tables for **Length of Stay** and **Frequent Attenders** analysis, built on the staging layer.

## Models

### `mart_length_of_stay`

Encounter-grain table with length of stay in hours, broken down by presenting condition (`reason_code` / `reason_description` from the encounter, not the conditions table). Includes `encounter_class` so analysts can filter by type (ED, inpatient, etc.).

### `mart_frequent_attenders`

Encounter-grain table with a rolling 12-month visit count per patient. Flags patients with 3+ encounters in any 365-day window as frequent attenders. Joined to patient demographics for reporting.

Uses a self-join approach (`TIMESTAMP_SUB` + `BETWEEN`) rather than a `RANGE` window frame, because BigQuery does not support `RANGE BETWEEN INTERVAL ... PRECEDING` on timestamp-typed ORDER BY keys.

## Data Quality Issues Found

Three categories of data quality issues were discovered during mart development and are handled defensively in the mart SQL.

### 1. Duplicate encounter IDs across source systems

`stg_encounters` unions the main encounters table with a schema-change batch from secondary source systems (EPIC_PROD, CERNER_LEGACY). **188 encounter IDs appear in both the main batch and the schema-change batch.** This causes uniqueness violations at the mart layer.

**Resolution:** Both mart models deduplicate encounters using `ROW_NUMBER() OVER (PARTITION BY encounter_id ORDER BY CASE source_system WHEN 'MAIN' THEN 0 ELSE 1 END)`, preferring the MAIN source system record when duplicates exist.

### 2. Duplicate patient IDs in the patients table

`stg_patients` contains **21 duplicate patient IDs** in the raw source data, causing fan-out when joining encounters to patient demographics.

**Resolution:** `mart_frequent_attenders` deduplicates patients with `ROW_NUMBER() OVER (PARTITION BY patient_id)` before joining.

### 3. Encounters where stop timestamp precedes start timestamp

**1,043 encounters** have `encounter_stop < encounter_start`, producing negative `length_of_stay_hours` values. These are likely data entry errors or clock/timezone issues in the source systems.

**Resolution:** These rows are preserved in `mart_length_of_stay` (not filtered out) so analysts can see the full picture. The `dbt_utils.expression_is_true` test for `length_of_stay_hours >= 0` is configured with `severity: warn` to flag this as a known data quality issue without blocking the pipeline.

### 4. Timestamp-formatted date strings in the patients table

The raw `BIRTHDATE` and `DEATHDATE` columns contain values like `'1936-08-27 00:00:00'` (timestamp format) rather than `'1936-08-27'` (date format). A direct `CAST(... AS DATE)` fails on these values in BigQuery.

**Resolution:** `stg_patients` was updated to use `DATE(CAST(... AS TIMESTAMP))` which handles both formats correctly.

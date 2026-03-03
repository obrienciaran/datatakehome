# Part B: Data Quality Assessment

Full exploratory analysis in [`data_quality_assessment.ipynb`](data_quality_assessment.ipynb).

---

## B1. Data Quality Issues

### patients.csv

| # | Issue | Type | Count | Blocking? | Pipeline Handling |
|---|-------|------|-------|-----------|-------------------|
| 1 | **Duplicate patient IDs** — 21 unique IDs appear more than once (22 extra rows total; one ID appears 3 times). | Integrity | 22 | **Blocking** — duplicates inflate patient counts and corrupt joins to encounters. | Deduplicate in staging using `ROW_NUMBER()` partitioned by `Id`, ordered by a deterministic tiebreaker (e.g., `BIRTHDATE`, `HEALTHCARE_EXPENSES`). Keep the first row and quarantine the rest for manual review. |
| 2 | **Future birthdates** — 23 patients have a `BIRTHDATE` after today's date. | Validity | 23 | **Blocking** — age-derived metrics (e.g., paediatric vs. adult segmentation, life expectancy) will be wrong. | Flag rows where `BIRTHDATE > CURRENT_DATE()` and null-out the invalid date so downstream calculations treat the age as unknown rather than negative. Log for source-system correction. |
| 3 | **Death date before birth date** — 27 patients have `DEATHDATE < BIRTHDATE`. | Validity | 27 | **Blocking** — mortality and length-of-life analytics will produce negative or nonsensical values. | Where `DEATHDATE < BIRTHDATE`, null-out both dates and flag the record. Without a reliable way to determine which date is correct, both must be treated as unknown. |
| 4 | **Swapped Lat / Lon** — 23 rows have Lat values in the −71 range and Lon values in the +42 range. Massachusetts Lat should be positive (~41–43 °N) and Lon should be negative (~−69 to −73 °W), so the two columns have been transposed on these rows. | Accuracy | 23 | **Non-blocking** — geographic analyses (catchment area, travel distance) would be affected, but core clinical analytics are unaffected. | Detect rows where `Lat < 0` and `Lon > 0` (for a Massachusetts dataset) and swap the two columns. Add a validation rule to catch this in future loads. |
| 5 | **Corrupted SSN values** — 23 rows contain random non-numeric characters in the SSN field (e.g., `#w#n=M>#:`, `coUNEhE+kk~!`, `pFCiRc.)D.Ca`). | Validity | 23 | **Non-blocking** — SSN is not typically used in clinical analytics, but it is a PII field that should be well-formed for record linkage. | Apply a regex validation (`\d{3}-\d{2}-\d{4}`) in staging. Null-out values that don't match and log them. |
| 6 | **Widespread nulls across optional columns** — `ZIP` (551), `MAIDEN` (858), `MARITAL` (385), `SUFFIX` (1,181), `DRIVERS` (213), `PASSPORT` (273), `PREFIX` (244). | Completeness | Various | **Non-blocking** — these are demographic/administrative fields. Most analytics don't depend on them, but high null rates in `ZIP` limit geographic analysis. | Accept nulls as valid for optional fields. Document expected null rates per column. Add monitoring to alert if null rates spike beyond baseline thresholds, which may indicate an upstream extraction issue. |

### encounters.csv

| # | Issue | Type | Count | Blocking? | Pipeline Handling |
|---|-------|------|-------|-----------|-------------------|
| 1 | **STOP before START** — 1,043 encounters have a `STOP` timestamp earlier than `START`, producing negative durations. | Validity | 1,043 | **Blocking** — length-of-stay and duration-based metrics will be negative or nonsensical. This is ~2% of rows. | Where `STOP < START`, swap the two timestamps (likely a data-entry inversion). If the resulting duration is still implausible (e.g., > 365 days), null-out both and flag for review. |
| 2 | **Orphaned patient references** — 10 encounters reference a `PATIENT` ID that does not exist in `patients.csv`. Descriptions are labelled "Orphan encounter N". | Integrity | 10 | **Blocking** — any join to the patients table will silently drop these rows, under-counting encounters. | Use a left join from encounters to patients and flag rows where the patient side is null. Quarantine orphaned encounters in a separate audit table. Investigate whether the referenced patients were deleted or never loaded. |
| 3 | **Implausible historical dates** — 1,084 encounters have `START` dates before 1920, spanning 1800–1919. Many produce durations of 100+ years. | Validity | 1,084 | **Blocking** — these will skew time-series analyses, average length-of-stay calculations, and any date-range filters. ~2% of all rows. | Apply a minimum-date threshold (e.g., `1900-01-01`). Encounters with `START < threshold` should be quarantined. The threshold should be configurable and documented. |
| 4 | **High null rate in REASONCODE / REASONDESCRIPTION** — 39,578 out of 53,356 rows (~74%) have null reason fields. | Completeness | 39,578 | **Non-blocking** — reason codes add clinical context but are not required for core encounter metrics (counts, costs, durations). The null rate is consistent, suggesting "no recorded reason" rather than data loss. | Accept nulls as valid (many encounter types like wellness visits genuinely lack a reason code). Document the expected null rate. Alert if it deviates significantly from baseline. |

---

## B2. Unifying `encounters_schema_change_batch.csv`

### Schema Differences

| Aspect | `encounters.csv` | `encounters_schema_change_batch.csv` |
|--------|-------------------|--------------------------------------|
| Encounter type column | `ENCOUNTERCLASS` | `ENCOUNTER_TYPE` (renamed) |
| Timestamp format | ISO 8601 strings (`2011-04-25T16:07:39Z`) | Epoch milliseconds (`1587340225000`) |
| Extra column | — | `SOURCE_SYSTEM` (`EPIC_PROD` / `CERNER_LEGACY`) |
| Row count | 53,356 | 200 |

### Unification Approach

1. **Column alignment** — Rename `ENCOUNTER_TYPE` to `ENCOUNTERCLASS` to match the main schema. This is a straightforward rename; the values (ambulatory, wellness, inpatient, etc.) are consistent between both files.

2. **Timestamp conversion** — Convert epoch milliseconds to UTC datetime: `pd.to_datetime(col, unit='ms', utc=True)` in Python, or `TIMESTAMP_MILLIS(col)` in BigQuery SQL. Negative epoch values (pre-1970 dates) are valid and convert correctly.

3. **Preserve the `SOURCE_SYSTEM` column** — Add `SOURCE_SYSTEM` to the unified schema and backfill the original encounters with a default value (e.g., `LEGACY` or `SYNTHEA`) so the column is always populated. This lineage is valuable for debugging and filtering.

4. **Union the datasets** — After alignment, `UNION ALL` the two tables. The overlap window spans 1939–2020 for both sources, and 158 months have records from both, so these are not sequential batches — they overlap. Deduplication on `Id` should be applied if any IDs appear in both files.

5. **Apply the same quality checks** — The batch file inherits the same validation rules (date range thresholds, duration checks, referential integrity to patients). One row in the batch has a missing `STOP` value that should be flagged.

In a production pipeline (e.g., dbt), this would be implemented as a staging model that reads both sources, applies the column rename and timestamp cast, unions the results, and feeds into the existing encounter quality checks.

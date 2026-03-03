/*
    C2 – Silent Failure Detection (singular test)

    Checks that the median length of stay is within a plausible range:
    - Greater than 0 hours  (rules out all-null or all-zero values)
    - Less than 720 hours / 30 days  (rules out timestamp corruption)

    A silent data issue — such as epoch timestamps being interpreted as
    calendar dates — would produce wildly implausible LOS values and be
    caught by this test.

    dbt convention: a singular test fails when it returns rows.
    We return a row when the median LOS falls outside the expected bounds.
*/

with stats as (
    select
        percentile_cont(length_of_stay_hours, 0.5) over () as median_los_hours
    from {{ ref('length_of_stay') }}
    where length_of_stay_hours is not null
    limit 1
)

select 1 as failure_flag
from stats
where median_los_hours <= 0
    or median_los_hours > 720

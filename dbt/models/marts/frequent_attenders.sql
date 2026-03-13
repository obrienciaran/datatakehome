-- This model is NOT unique at the patient level. Each row represents a single ED encounter,
-- so a patient with 50 attendances will have 50 rows. A patient is considered a frequent
-- attender if they have 3 or more visits within a rolling 12-month window (is_frequent_attender = true).

with encounters as (
    select * from {{ ref('stg_encounters') }}
),

patients as (
    select *
    from (
        select
            *,
            row_number() over (partition by patient_id order by patient_id) as _dedup_rn
        from {{ ref('stg_patients') }}
    )
    where _dedup_rn = 1
),

-- This approach is O(n log n) so a bit better. It uses a sliding window and counts incrementally rather than comparing every pair from table A and B
--    encounter_with_rolling as (
--    select
--        patient_id,
--        encounter_id,
--        encounter_class,
--        encounter_start,
--        count(*) over (
--            partition by patient_id
--            order by unix_seconds(encounter_start)
--            -- 31536000 is the number of seconds in 365 days
--            -- will drift due to leap years, but so will the below approach
--            range between 31536000 preceding and current row
--        ) as visits_rolling_12m
--    from encounters
--)

-- This is a self join which can get computationally expensive as it is O(n²). See the above comment for a better way.
encounter_with_rolling as (
    select
        e1.patient_id,
        e1.encounter_id,
        e1.encounter_class,
        e1.encounter_start,
        count(*) as visits_rolling_12m
    from encounters as e1
    inner join encounters as e2
        on e1.patient_id = e2.patient_id
        and e2.encounter_start between
            timestamp_sub(e1.encounter_start, interval 365 day)
            and e1.encounter_start
    group by 1, 2, 3, 4
),

joined as (
    select
        ewr.patient_id,
        p.first_name,
        p.last_name,
        p.gender,
        p.birth_date,
        ewr.encounter_id,
        ewr.encounter_class,
        ewr.encounter_start,
        ewr.visits_rolling_12m,
        ewr.visits_rolling_12m >= 3 as is_frequent_attender
    from encounter_with_rolling as ewr
    inner join patients as p
        on ewr.patient_id = p.patient_id
)

select * from joined

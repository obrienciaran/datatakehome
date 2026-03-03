-- Calculates the length of stay in hours for each encounter, derived from the difference
-- between encounter_start and encounter_stop. Only encounters with a recorded stop time are included.

with encounters as (
    select
        *,
        row_number() over (
            partition by encounter_id
            order by case source_system when 'MAIN' then 0 else 1 end
        ) as _dedup_rn
    from {{ ref('stg_encounters') }}
    where encounter_stop is not null
)

select
    encounter_id,
    patient_id,
    encounter_class,
    encounter_start,
    encounter_stop,
    timestamp_diff(encounter_stop, encounter_start, MINUTE) / 60.0 as length_of_stay_hours,
    reason_code,
    reason_description
from encounters
where _dedup_rn = 1

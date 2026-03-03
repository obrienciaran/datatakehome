with encounters as (
    select *
    from (
        select
            *,
            row_number() over (
                partition by encounter_id
                order by case source_system when 'MAIN' then 0 else 1 end
            ) as _dedup_rn
        from {{ ref('stg_encounters') }}
    )
    where _dedup_rn = 1
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

with source as (
    select * from {{ source('raw', 'conditions') }}
),

renamed as (
    select
        PATIENT                     as patient_id,
        ENCOUNTER                   as encounter_id,
        cast(CODE as STRING)        as condition_code,
        DESCRIPTION                 as condition_description,
        cast(START as DATE)         as condition_start,
        cast(STOP as DATE)          as condition_stop
    from source
)

select * from renamed

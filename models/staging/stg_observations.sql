with source as (
    select * from {{ source('raw', 'observations') }}
),

renamed as (
    select
        cast(DATE as TIMESTAMP)     as observation_date,
        PATIENT                     as patient_id,
        ENCOUNTER                   as encounter_id,
        cast(CODE as STRING)        as observation_code,
        DESCRIPTION                 as observation_description,
        cast(VALUE as STRING)       as observation_value,
        UNITS                       as observation_units,
        TYPE                        as observation_type
    from source
)

select * from renamed

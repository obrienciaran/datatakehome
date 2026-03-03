with source as (
    select * from {{ source('raw', 'clinical_notes') }}
),

renamed as (
    select
        ENCOUNTER_ID                        as encounter_id,
        PATIENT_ID                          as patient_id,
        cast(NOTE_DATETIME as TIMESTAMP)    as note_datetime,
        NOTE_TYPE                           as note_type,
        NOTE_TEXT                           as note_text
    from source
)

select * from renamed

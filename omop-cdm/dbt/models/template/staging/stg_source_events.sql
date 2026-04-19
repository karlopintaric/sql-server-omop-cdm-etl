{{
    config(
        tags=['partitioned']
    )
}}

select
    patient_source_id,
    cast(event_start_date as date) as condition_start_date,
    condition_source_code,
    condition_source_value,
    cast(visit_occurrence_id as bigint) as visit_occurrence_id
from {{ source('stage', 'source_events') }}

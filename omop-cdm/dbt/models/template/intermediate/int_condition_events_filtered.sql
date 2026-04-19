{{
    config(
        tags=['partitioned']
    )
}}

select {{ limit_rows() }}
    patient_source_id,
    condition_start_date,
    condition_source_code,
    condition_source_value,
    visit_occurrence_id
from {{ ref('stg_source_events') }}
where {{ filter_by_year('condition_start_date') }}

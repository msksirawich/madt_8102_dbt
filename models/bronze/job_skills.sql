{{
    config(
        materialized='view',
        schema='bronze'
    )
}}

-- Bronze layer: Raw job_skills data as-is from source
-- Many-to-many link for job skill requirements

select
    job_id,
    skill_id,
    importance,
    created_at,
    updated_at,
    _ingestion_time,
    dt,
from {{ source('bronze', 'job_skills') }}

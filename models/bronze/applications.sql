{{
    config(
        materialized='view',
        schema='bronze'
    )
}}

-- Bronze layer: Raw application data as-is from source
-- Job application records

select
    application_id,
    user_id,
    job_id,
    current_status,
    applied_at,
    created_at,
    updated_at,
    _ingestion_time,
    dt,
from {{ source('bronze', 'applications') }}

{{
    config(
        materialized='view',
        schema='bronze'
    )
}}

-- Bronze layer: Raw education data as-is from source
-- Education history for users

select
    education_id,
    user_id,
    degree,
    school_name,
    created_at,
    updated_at,
    _ingestion_time,
    dt,
from {{ source('bronze', 'education') }}

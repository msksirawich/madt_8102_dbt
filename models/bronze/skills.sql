{{
    config(
        materialized='view',
        schema='bronze'
    )
}}

-- Bronze layer: Raw skills data as-is from source
-- Master list of skills (Python, SQL, etc.)

select
    skill_id,
    name,
    category,
    created_at,
    updated_at,
    _ingestion_time,
    dt,
from {{ source('bronze', 'skills') }}

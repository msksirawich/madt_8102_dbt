{{
    config(
        materialized='view',
        schema='bronze'
    )
}}

-- Bronze layer: Raw user_skills data as-is from source
-- Many-to-many link between users and skills

select
    user_id,
    skill_id,
    proficiency,
    created_at,
    updated_at,
    _ingestion_time,
    dt,
from {{ source('bronze', 'user_skills') }}

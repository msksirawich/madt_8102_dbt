{{
    config(
        materialized='view',
        schema='bronze'
    )
}}

-- Bronze layer: Raw profile data as-is from source
-- Extended user details (1:1 with users)

select
    profile_id,
    user_id,
    headline,
    summary,
    current_salary,
    willing_to_relocate,
    created_at,
    updated_at,
    _ingestion_time,
    dt,
from {{ source('bronze', 'profiles') }}

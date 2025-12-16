{{
    config(
        materialized='view',
        schema='bronze'
    )
}}

-- Bronze layer: Raw user data as-is from source
-- Core identity table for user accounts

select
    user_id,
    email,
    password_hash,
    is_active,
    created_at,
    updated_at,
    _ingestion_time,
    dt,
from {{ source('bronze', 'users') }}

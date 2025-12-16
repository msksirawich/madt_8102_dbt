{{
    config(
        materialized='table',
        schema='silver'
    )
}}

-- Silver layer: Cleaned and standardized user data
-- Applies data quality rules and standardization

with source as (
    select *
    from {{ ref('users') }}
),

cleaned as (
    select
        user_id,
        lower(trim(email)) as email,
        password_hash,
        is_active,
        created_at,
        updated_at,
        current_timestamp as _ingestion_time,
        dt
    from source
    where
        user_id is not null
)

select * from cleaned

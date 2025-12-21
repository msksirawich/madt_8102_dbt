{{
    config(
        materialized='incremental',
        unique_key='profile_id',
        schema='silver'
    )
}}

-- Silver layer: Cleaned and standardized profile data
-- Applies data quality rules and standardization

with source as (
    select *
    from {{ ref('profiles') }}
    -- where CAST(dt AS DATE) = CAST('{{ var("execution_date") }}' AS DATE)
),

cleaned as (
    select
        profile_id,
        user_id,
        lower(trim(headline)) as headline,
        lower(trim(summary)) as summary,
        current_salary,
        willing_to_relocate,
        created_at,
        updated_at,
        current_timestamp as _ingestion_time,
        dt
    from source
    where
        profile_id is not null
)

select * from cleaned

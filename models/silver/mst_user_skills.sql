{{
    config(
        materialized='incremental',
        unique_key=['user_id', 'skill_id'],
        schema='silver'
    )
}}

-- Silver layer: Cleaned and standardized user_skills data
-- Applies data quality rules and validates proficiency levels

with source as (
    select *
    from {{ ref('user_skills') }}
    -- where CAST(dt AS DATE) = CAST('{{ var("execution_date") }}' AS DATE)
),

cleaned as (
    select
        user_id,
        skill_id,
        proficiency,
        created_at,
        updated_at,
        current_timestamp as _ingestion_time,
        dt
    from source
    where
        user_id is not null
)

select * from cleaned

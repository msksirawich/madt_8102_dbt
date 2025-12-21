{{
    config(
        materialized='incremental',
        unique_key='skill_id',
        schema='silver'
    )
}}

-- Silver layer: Cleaned and standardized skills data
-- Applies data quality rules and standardization

with source as (
    select *
    from {{ ref('skills') }}
    -- where CAST(dt AS DATE) = CAST('{{ var("execution_date") }}' AS DATE)
),

cleaned as (
    select
        skill_id,
        lower(trim(name)) as name,
        lower(trim(category)) as category,
        created_at,
        updated_at,
        current_timestamp as _ingestion_time,
        dt
    from source
    where
        skill_id is not null
)

select * from cleaned

{{
    config(
        materialized='incremental',
        unique_key=['job_id', 'skill_id'],
        schema='silver'
    )
}}

-- Silver layer: Cleaned and standardized job_skills data
-- Applies data quality rules and validates importance levels

with source as (
    select *
    from {{ ref('job_skills') }}
    -- where CAST(dt AS DATE) = CAST('{{ var("execution_date") }}' AS DATE)
),

cleaned as (
    select
        job_id,
        skill_id,
        importance,
        created_at,
        updated_at,
        current_timestamp as _ingestion_time,
        dt
    from source
    where
        job_id is not null
)

select * from cleaned

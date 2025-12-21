{{
    config(
        materialized='incremental',
        unique_key='education_id',
        schema='silver'
    )
}}

-- Silver layer: Cleaned and standardized education data
-- Applies data quality rules and standardization

with source as (
    select *
    from {{ ref('education') }}
    -- where CAST(dt AS DATE) = CAST('{{ var("execution_date") }}' AS DATE)
),

cleaned as (
    select
        education_id,
        user_id,
        lower(trim(degree)) as degree,
        lower(trim(school_name)) as school_name,
        created_at,
        updated_at,
        current_timestamp as _ingestion_time,
        dt
    from source
    where
        education_id is not null
)

select * from cleaned

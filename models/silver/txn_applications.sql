{{
    config(
        materialized='incremental',
        unique_key='application_id',
        schema='silver'
    )
}}

-- Silver layer: Cleaned and standardized application data
-- Applies data quality rules and standardization

with source as (
    select *
    from {{ ref('applications') }}
    where CAST(dt AS DATE) = CAST('{{ var("execution_date") }}' AS DATE)
),

cleaned as (
    select
        application_id,
        user_id,
        job_id,
        lower(trim(current_status)) as current_status,
        applied_at,
        created_at,
        updated_at,
        current_timestamp as _ingestion_time,
        dt
    from source
    where
        application_id is not null
)

select * from cleaned

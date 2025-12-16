{{
    config(
        materialized='table',
        schema='silver'
    )
}}

-- Silver layer: Cleaned and standardized work history data
-- Applies data quality rules, standardization, and computed fields

with source as (
    select *
    from {{ ref('work_history') }}
),

cleaned as (
    select
        work_id,
        user_id,
        lower(trim(company_name)) as company_name,
        lower(trim(title)) as title,
        start_date,
        end_date,
        created_at,
        updated_at,
        current_timestamp as _ingestion_time,
        dt
    from source
    where
        work_id is not null
)

select * from cleaned

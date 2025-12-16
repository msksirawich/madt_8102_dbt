{{
    config(
        materialized='table',
        schema='silver'
    )
}}

-- Silver layer: Cleaned and standardized job posting data
-- Applies data quality rules, standardization, and computed fields

with source as (
    select *
    from {{ ref('job_postings') }}
),

cleaned as (
    select
        job_id,
        company_id,
        lower(trim(title)) as title,
        description_html,
        min_salary,
        max_salary,
        lower(trim(status)) as status,
        published_at,
        created_at,
        updated_at,
        current_timestamp as _ingestion_time,
        dt
    from source
    where
        job_id is not null
)

select * from cleaned

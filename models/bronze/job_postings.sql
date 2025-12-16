{{
    config(
        materialized='view',
        schema='bronze'
    )
}}

-- Bronze layer: Raw job posting data as-is from source
-- Job catalog/listings

select
    job_id,
    company_id,
    title,
    description_html,
    min_salary,
    max_salary,
    status,
    published_at,
    created_at,
    updated_at,
    _ingestion_time,
    dt,
from {{ source('bronze', 'job_postings') }}

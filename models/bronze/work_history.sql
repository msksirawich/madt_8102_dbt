{{
    config(
        materialized='view',
        schema='bronze'
    )
}}

-- Bronze layer: Raw work history data as-is from source
-- Work experience records

select
    work_id,
    user_id,
    company_name,
    title,
    start_date,
    end_date,
    created_at,
    updated_at,
    _ingestion_time,
    dt,
from {{ source('bronze', 'work_history') }}

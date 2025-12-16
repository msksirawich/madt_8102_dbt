{{
    config(
        materialized='view',
        schema='bronze'
    )
}}

-- Bronze layer: Raw company data as-is from source
-- Company registry

select
    company_id,
    name,
    industry,
    size_range,
    created_at,
    updated_at,
    _ingestion_time,
    dt,
from {{ source('bronze', 'companies') }}

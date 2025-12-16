{{
    config(
        materialized='table',
        schema='silver'
    )
}}

-- Silver layer: Cleaned and standardized company data
-- Applies data quality rules and standardization

with source as (
    select *
    from {{ ref('companies') }}
),

cleaned as (
    select
        company_id,
        lower(trim(name)) as name,
        lower(trim(industry)) as industry,
        lower(trim(size_range)) as size_range,
        created_at,
        updated_at,
        current_timestamp as _ingestion_time,
        dt
    from source
    where
        company_id is not null
)

select * from cleaned



-- Silver layer: Cleaned and standardized customer data
-- This layer applies data quality rules and standardization

with source as (
    select * from "medallion"."main_bronze"."bronze_customers"
),

cleaned as (
    select
        customer_id,
        lower(trim(email)) as email,
        trim(first_name) as first_name,
        trim(last_name) as last_name,
        first_name || ' ' || last_name as full_name,
        created_at,
        updated_at,
        current_timestamp as dbt_loaded_at
    from source
    where
        -- Data quality filters
        customer_id is not null
        and email is not null
        and email like '%@%.%'  -- Basic email validation
        and first_name is not null
        and last_name is not null
)

select * from cleaned
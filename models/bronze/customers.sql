{{
    config(
        materialized='view',
        schema='bronze'
    )
}}

-- Bronze layer: Raw customer data as-is from source
-- This layer provides a 1:1 view of source data without transformation

select
    customer_id,
    email,
    first_name,
    last_name,
    created_at,
    updated_at
from {{ source('bronze', 'customers') }}

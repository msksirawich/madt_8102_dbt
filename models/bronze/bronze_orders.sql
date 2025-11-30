{{
    config(
        materialized='view',
        schema='bronze'
    )
}}

-- Bronze layer: Raw order data as-is from source
-- This layer provides a 1:1 view of source data without transformation

select
    order_id,
    customer_id,
    order_date,
    amount,
    status,
    created_at,
    updated_at
from {{ source('bronze', 'raw_orders') }}

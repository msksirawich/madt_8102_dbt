{{
    config(
        materialized='incremental',
        unique_key='order_id',
        schema='silver'
    )
}}

-- Silver layer: Cleaned and standardized order data
-- This layer applies data quality rules and enriches with customer information

with source as (
    select * from {{ ref('bronze_orders') }}
    {% if is_incremental() %}
    where created_at::date = CAST('{{ var("execution_date") }}' AS DATE)
    {% endif %}
),

customers as (
    select customer_id from {{ ref('silver_customers') }}
),

cleaned as (
    select
        o.order_id,
        o.customer_id,
        o.order_date,
        cast(o.order_date as date) as order_date_only,
        o.amount,
        upper(trim(o.status)) as status,
        o.created_at,
        o.updated_at,
        current_timestamp as dbt_loaded_at
    from source o
    inner join customers c
        on o.customer_id = c.customer_id
    where
        -- Data quality filters
        o.order_id is not null
        and o.customer_id is not null
        and o.order_date is not null
        and o.amount is not null
        and o.amount > 0
        and o.status is not null
)

select * from cleaned

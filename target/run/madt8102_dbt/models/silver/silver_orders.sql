
  
    

    create or replace table `madt-8102-479812`.`madt8102_silver`.`silver_orders`
      
    
    

    
    OPTIONS()
    as (
      

-- Silver layer: Cleaned and standardized order data
-- This layer applies data quality rules and enriches with customer information

with source as (
    select * from `madt-8102-479812`.`madt8102_bronze`.`orders`
    
),

customers as (
    select customer_id from `madt-8102-479812`.`madt8102_silver`.`silver_customers`
),

cleaned as (
    select
        o.order_id,
        o.customer_id,
        o.order_date,
        cast(o.order_date as date) as order_date_only,
        cast(o.amount as float64) as amount,
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
        and cast(o.amount as float64) > 0
        and o.status is not null
)

select * from cleaned
    );
  
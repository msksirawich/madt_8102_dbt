
  
    
    

    create  table
      "medallion"."main_gold"."gold_customer_metrics__dbt_tmp"
  
    as (
      

-- Gold layer: Customer-level business metrics
-- This layer aggregates data for business intelligence and reporting

with customers as (
    select * from "medallion"."main_silver"."silver_customers"
),

orders as (
    select * from "medallion"."main_silver"."silver_orders"
),

customer_orders as (
    select
        customer_id,
        count(*) as total_orders,
        count(case when status = 'DELIVERED' then 1 end) as delivered_orders,
        count(case when status = 'CANCELLED' then 1 end) as cancelled_orders,
        sum(amount) as lifetime_value,
        avg(amount) as avg_order_value,
        min(order_date) as first_order_date,
        max(order_date) as last_order_date,
        max(order_date_only) as last_order_date_only
    from orders
    group by customer_id
),

metrics as (
    select
        c.customer_id,
        c.email,
        c.full_name,
        c.created_at as customer_since,
        coalesce(co.total_orders, 0) as total_orders,
        coalesce(co.delivered_orders, 0) as delivered_orders,
        coalesce(co.cancelled_orders, 0) as cancelled_orders,
        coalesce(co.lifetime_value, 0) as lifetime_value,
        coalesce(co.avg_order_value, 0) as avg_order_value,
        co.first_order_date,
        co.last_order_date,
        co.last_order_date_only,
        case
            when co.total_orders is null then 'Never Ordered'
            when co.total_orders = 1 then 'One-Time Customer'
            when co.total_orders between 2 and 5 then 'Repeat Customer'
            when co.total_orders > 5 then 'VIP Customer'
        end as customer_segment,
        case
            when co.last_order_date_only >= current_date - interval '30 days' then 'Active'
            when co.last_order_date_only >= current_date - interval '90 days' then 'At Risk'
            when co.last_order_date_only < current_date - interval '90 days' then 'Churned'
            else 'Never Ordered'
        end as customer_status,
        current_timestamp as dbt_loaded_at
    from customers c
    left join customer_orders co
        on c.customer_id = co.customer_id
)

select * from metrics
    );
  
  
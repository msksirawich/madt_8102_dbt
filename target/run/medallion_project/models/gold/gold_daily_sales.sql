
  
    
    

    create  table
      "medallion"."main_gold"."gold_daily_sales__dbt_tmp"
  
    as (
      

-- Gold layer: Daily sales metrics
-- This layer provides time-series metrics for business reporting and analysis

with orders as (
    select * from "medallion"."main_silver"."silver_orders"
),

daily_aggregates as (
    select
        order_date_only as sales_date,
        count(*) as total_orders,
        count(distinct customer_id) as unique_customers,
        sum(amount) as total_revenue,
        avg(amount) as avg_order_value,
        min(amount) as min_order_value,
        max(amount) as max_order_value,

        -- Orders by status
        count(case when status = 'PENDING' then 1 end) as pending_orders,
        count(case when status = 'PROCESSING' then 1 end) as processing_orders,
        count(case when status = 'SHIPPED' then 1 end) as shipped_orders,
        count(case when status = 'DELIVERED' then 1 end) as delivered_orders,
        count(case when status = 'CANCELLED' then 1 end) as cancelled_orders,

        -- Revenue by status
        sum(case when status = 'DELIVERED' then amount else 0 end) as delivered_revenue,
        sum(case when status = 'CANCELLED' then amount else 0 end) as cancelled_revenue

    from orders
    group by order_date_only
),

with_moving_averages as (
    select
        *,
        -- 7-day moving average
        avg(total_revenue) over (
            order by sales_date
            rows between 6 preceding and current row
        ) as revenue_7day_ma,

        -- 30-day moving average
        avg(total_revenue) over (
            order by sales_date
            rows between 29 preceding and current row
        ) as revenue_30day_ma,

        -- Day of week metrics
        dayname(sales_date) as day_of_week,
        extract(dow from sales_date) as day_of_week_num,

        current_timestamp as dbt_loaded_at

    from daily_aggregates
)

select * from with_moving_averages
order by sales_date desc
    );
  
  
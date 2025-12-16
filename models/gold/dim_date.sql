{{
    config(
        materialized='table',
        schema='gold'
    )
}}

-- Gold layer: Date dimension table
-- Generates a complete date spine for analytics
-- Grain: One row per calendar date

with date_spine as (
    -- Generate dates from 2020-01-01 to 2030-12-31
    select
        date_day
    from
        unnest(generate_date_array('2020-01-01', '2030-12-31', interval 1 day)) as date_day
),

date_dimension as (
    select
        -- Surrogate key in YYYYMMDD format
        cast(format_date('%Y%m%d', date_day) as int64) as date_key,

        -- Date attributes
        date_day as full_date,
        extract(year from date_day) as year,
        extract(month from date_day) as month,
        extract(day from date_day) as day,
        extract(quarter from date_day) as quarter,
        extract(dayofweek from date_day) as day_of_week,  -- 1=Sunday, 7=Saturday in BigQuery
        extract(dayofyear from date_day) as day_of_year,
        extract(week from date_day) as week_of_year,

        -- Day name and month name
        format_date('%A', date_day) as day_name,
        format_date('%B', date_day) as month_name,

        -- Weekend flag
        case
            when extract(dayofweek from date_day) in (1, 7) then true  -- Sunday = 1, Saturday = 7 in BigQuery
            else false
        end as is_weekend,

        -- Holiday flag (placeholder - customize based on your region)
        case
            -- New Year's Day
            when extract(month from date_day) = 1 and extract(day from date_day) = 1 then true
            -- Christmas
            when extract(month from date_day) = 12 and extract(day from date_day) = 25 then true
            -- Add more holidays as needed
            else false
        end as is_holiday,

        -- Fiscal periods (assuming calendar year = fiscal year)
        case
            when extract(month from date_day) in (1, 2, 3) then 'Q1'
            when extract(month from date_day) in (4, 5, 6) then 'Q2'
            when extract(month from date_day) in (7, 8, 9) then 'Q3'
            when extract(month from date_day) in (10, 11, 12) then 'Q4'
        end as fiscal_quarter,

        -- First and last day of month flags
        case
            when extract(day from date_day) = 1 then true
            else false
        end as is_first_day_of_month,

        case
            when date_day = last_day(date_day, month) then true
            else false
        end as is_last_day_of_month,

        -- Audit fields
        current_timestamp as _created_at

    from date_spine
)

select * from date_dimension
order by date_key

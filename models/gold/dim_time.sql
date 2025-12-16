{{
    config(
        materialized='table',
        schema='gold'
    )
}}

-- Gold layer: Time dimension table
-- Generates time-of-day buckets for intraday analysis
-- Grain: One row per hour (24 rows) for simplified time analysis

with time_spine as (
    -- Generate time values for every hour (0-23)
    select
        hour_value
    from
        unnest(generate_array(0, 23, 1)) as hour_value
),

time_dimension as (
    select
        -- Surrogate key in HHMMSS format (top of hour)
        cast(lpad(cast(hour_value as string), 2, '0') || '0000' as int64) as time_key,

        -- Time components
        hour_value as hour,
        0 as minute,
        0 as second,

        -- Time of day classification
        case
            when hour_value between 0 and 5 then 'late_night'
            when hour_value between 6 and 11 then 'morning'
            when hour_value between 12 and 17 then 'afternoon'
            when hour_value between 18 and 23 then 'evening'
        end as time_of_day,

        -- Business hours classification
        case
            when hour_value between 9 and 16 then 'work_hours'
            when hour_value between 6 and 8
                or hour_value between 17 and 20 then 'commute_hours'
            else 'off_hours'
        end as time_segment,

        -- Peak vs off-peak (for job platforms, evening is peak)
        case
            when hour_value between 18 and 22 then true
            else false
        end as is_peak_hours,

        -- Time periods for reporting
        case
            when hour_value < 12 then 'AM'
            else 'PM'
        end as am_pm,

        -- Hour ranges for bucketing
        case
            when hour_value between 0 and 3 then '00-03'
            when hour_value between 4 and 7 then '04-07'
            when hour_value between 8 and 11 then '08-11'
            when hour_value between 12 and 15 then '12-15'
            when hour_value between 16 and 19 then '16-19'
            when hour_value between 20 and 23 then '20-23'
        end as hour_bucket,

        -- Audit fields
        current_timestamp as _created_at

    from time_spine
)

select * from time_dimension
order by time_key

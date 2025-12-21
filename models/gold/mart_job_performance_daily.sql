{{
    config(
        materialized='incremental',
        schema='gold',
        unique_key='job_day_sk'
    )
}}

-- Gold layer: Job Performance Daily Mart
-- Aggregated metrics for executive dashboard
-- Grain: One row per job per day
-- Materialization: Incremental (processes only new dates)
-- Aligned with fact_job_content_engagement and fact_application_flow schemas

with engagement_metrics as (
    select
        job_id_natural,
        date_key,

        -- View metrics (aggregated from user-level fact)
        count(*) as view_count,
        count(distinct user_id_natural) as userview_count,

        -- Duration metrics (aggregated from fact table's avg and max)
        sum(avg_time_on_page_seconds) as total_view_duration_sec,
        max(max_time_on_page_seconds) as max_view_duration_sec,
        avg(max_time_on_page_seconds) as avg_user_max_view_duration_sec,

        -- Scroll metrics
        avg(max_scroll_depth_percent) as avg_scroll_depth_percent,
        max(max_scroll_depth_percent) as max_scroll_depth_percent,

        -- Reason engine metrics (sum of counts from fact table)
        sum(bounce_no_apply_count) as bounce_count,
        sum(deep_read_no_apply_count) as deepread_noapply_count

    from {{ ref('fact_job_content_engagement') }}

    {% if is_incremental() %}
    -- Incremental load: only process recent dates (with 2-day lookback for late arrivals)
    where date_key >= (select max(datekey) - 2 from {{ this }})
    {% endif %}

    group by job_id_natural, date_key
),

application_metrics as (
    select
        job_id_natural,
        date_key,

        -- Total applications (count of distinct user applications)
        count(*) as total_applications,

        -- Funnel start metrics (sum of step start counts from fact table)
        sum(start_step1_count) as start_step1_count,
        sum(start_step2_count) as start_step2_count,
        sum(start_step3_count) as start_step3_count,

        -- Completion metrics (sum of completed applications)
        sum(case when is_completed then 1 else 0 end) as completed_apply_count,

        -- Drop-off metrics (sum of drop counts from fact table)
        sum(drop_step1_count) as drop_step1_count,
        sum(drop_step2_count) as drop_step2_count,
        sum(drop_step3_count) as drop_step3_count

    from {{ ref('fact_application_flow') }}

    {% if is_incremental() %}
    -- Incremental load: only process recent dates (with 2-day lookback for late arrivals)
    where date_key >= (select max(datekey) - 2 from {{ this }})
    {% endif %}

    group by job_id_natural, date_key
),

daily_performance as (
    select
        -- Composite surrogate key
        to_hex(
            md5(cast(coalesce(e.date_key, a.date_key) as string) || '_' ||
                cast(coalesce(e.job_id_natural, a.job_id_natural) as string))
        ) as job_day_sk,

        -- Dimension foreign keys
        coalesce(e.date_key, a.date_key) as datekey,
        coalesce(e.job_id_natural, a.job_id_natural) as job_id,

        -- Engagement metrics (from fact_job_content_engagement)
        coalesce(e.view_count, 0) as view_count,
        coalesce(e.userview_count, 0) as userview_count,
        coalesce(e.total_view_duration_sec, 0) as total_view_duration_sec,
        coalesce(e.max_view_duration_sec, 0) as max_view_duration_sec,
        coalesce(e.avg_user_max_view_duration_sec, 0.0) as avg_user_max_view_duration_sec,

        -- Scroll metrics
        coalesce(e.avg_scroll_depth_percent, 0.0) as avg_scroll_depth_percent,
        coalesce(e.max_scroll_depth_percent, 0) as max_scroll_depth_percent,

        -- Reason engine metrics
        coalesce(e.bounce_count, 0) as bounce_count,
        coalesce(e.deepread_noapply_count, 0) as deepread_noapply_count,

        -- Funnel start metrics (from fact_application_flow)
        coalesce(a.total_applications, 0) as total_applications,
        coalesce(a.start_step1_count, 0) as start_step1_count,
        coalesce(a.start_step2_count, 0) as start_step2_count,
        coalesce(a.start_step3_count, 0) as start_step3_count,
        coalesce(a.completed_apply_count, 0) as completed_apply_count,

        -- Drop-off metrics (from fact_application_flow)
        coalesce(a.drop_step1_count, 0) as drop_step1_count,
        coalesce(a.drop_step2_count, 0) as drop_step2_count,
        coalesce(a.drop_step3_count, 0) as drop_step3_count,

        -- Conversion rates (calculated metrics)
        case
            when coalesce(a.start_step1_count, 0) > 0
            then cast(coalesce(a.completed_apply_count, 0) as float64) / a.start_step1_count
            else 0.0
        end as step1_to_complete_rate,

        -- Bounce rate
        case
            when coalesce(e.view_count, 0) > 0
            then cast(coalesce(e.bounce_count, 0) as float64) / e.view_count
            else 0.0
        end as bounce_rate,

        -- Deep read rate (key KPI)
        case
            when coalesce(e.view_count, 0) > 0
            then cast(coalesce(e.deepread_noapply_count, 0) as float64) / e.view_count
            else 0.0
        end as deepread_noapply_rate,

        -- Audit fields
        current_timestamp as created_at,
        current_timestamp as updated_at

    from engagement_metrics e
    full outer join application_metrics a
        on e.job_id_natural = a.job_id_natural
        and e.date_key = a.date_key

    where coalesce(e.job_id_natural, a.job_id_natural) is not null  -- Ensure we have valid job
)

select * from daily_performance
order by datekey desc, job_id

{{
    config(
        materialized='table',
        schema='gold'
    )
}}

-- Gold layer: Job Performance Daily Mart
-- Aggregated metrics for executive dashboard
-- Grain: One row per job per day

with engagement_metrics as (
    select
        job_key,
        date_key,

        -- View metrics
        count(*) as view_count,
        count(distinct user_cookie_id) as unique_visitor_count,

        -- Duration metrics
        sum(time_on_page_seconds) as total_view_duration_sec,
        max(time_on_page_seconds) as max_view_duration_sec,
        avg(time_on_page_seconds) as avg_view_duration_sec,

        -- Engagement quality metrics
        sum(case when is_bounce then 1 else 0 end) as bounce_count,
        sum(case when engagement_quality = 'High' then 1 else 0 end) as high_engagement_count,
        sum(case when engagement_quality = 'Medium' then 1 else 0 end) as medium_engagement_count,
        sum(case when engagement_quality = 'Low' then 1 else 0 end) as low_engagement_count,

        -- Scroll metrics
        avg(max_scroll_depth_percent) as avg_scroll_depth_percent,
        max(max_scroll_depth_percent) as max_scroll_depth_percent,

        -- Click metrics
        sum(case when did_click_apply then 1 else 0 end) as apply_click_count,

        -- Key KPI: Deep read without apply
        sum(case when is_deep_read_no_apply then 1 else 0 end) as deepread_noapply_count

    from {{ ref('fact_job_content_engagement') }}
    where job_key is not null  -- Exclude sessions without valid job mapping
    group by job_key, date_key
),

application_metrics as (
    select
        job_key,
        date_key,

        -- Funnel start metrics (total applications that reached each step)
        count(*) as total_applications,
        sum(case when exit_step >= 1 then 1 else 0 end) as start_step1_count,
        sum(case when exit_step >= 2 then 1 else 0 end) as start_step2_count,
        sum(case when exit_step >= 3 then 1 else 0 end) as start_step3_count,

        -- Completion metrics
        sum(case when is_completed then 1 else 0 end) as completed_apply_count,

        -- Outcome metrics
        sum(case when received_offer then 1 else 0 end) as offer_count,
        sum(case when accepted_offer then 1 else 0 end) as hired_count,
        sum(case when was_rejected then 1 else 0 end) as rejected_count,

        -- Drop-off metrics
        sum(case when dropped_at_step1 then 1 else 0 end) as drop_step1_count,
        sum(case when dropped_at_step2 then 1 else 0 end) as drop_step2_count,

        -- Processing time metrics
        avg(days_since_application) as avg_days_to_process,
        max(days_since_application) as max_days_to_process

    from {{ ref('fact_application_flow') }}
    where job_key is not null  -- Exclude applications without valid job mapping
    group by job_key, date_key
),

daily_performance as (
    select
        -- Composite surrogate key
        to_hex(
            md5(cast(coalesce(e.date_key, a.date_key) as string) || '_' ||
                cast(coalesce(e.job_key, a.job_key) as string))
        ) as job_day_sk,

        -- Dimension foreign keys
        coalesce(e.date_key, a.date_key) as datekey,
        coalesce(e.job_key, a.job_key) as job_key,

        -- Engagement metrics (from fact_job_content_engagement)
        coalesce(e.view_count, 0) as view_count,
        coalesce(e.unique_visitor_count, 0) as unique_visitor_count,
        coalesce(e.total_view_duration_sec, 0) as total_view_duration_sec,
        coalesce(e.max_view_duration_sec, 0) as max_view_duration_sec,
        coalesce(e.avg_view_duration_sec, 0.0) as avg_view_duration_sec,

        -- Engagement quality distribution
        coalesce(e.high_engagement_count, 0) as high_engagement_count,
        coalesce(e.medium_engagement_count, 0) as medium_engagement_count,
        coalesce(e.low_engagement_count, 0) as low_engagement_count,

        -- Scroll metrics
        coalesce(e.avg_scroll_depth_percent, 0.0) as avg_scroll_depth_percent,
        coalesce(e.max_scroll_depth_percent, 0) as max_scroll_depth_percent,

        -- Reason engine metrics
        coalesce(e.bounce_count, 0) as bounce_count,
        coalesce(e.deepread_noapply_count, 0) as deepread_noapply_count,
        coalesce(e.apply_click_count, 0) as apply_click_count,

        -- Funnel start metrics (from fact_application_flow)
        coalesce(a.total_applications, 0) as total_applications,
        coalesce(a.start_step1_count, 0) as start_step1_count,
        coalesce(a.start_step2_count, 0) as start_step2_count,
        coalesce(a.start_step3_count, 0) as start_step3_count,
        coalesce(a.completed_apply_count, 0) as completed_apply_count,

        -- Drop-off metrics (calculated)
        coalesce(a.drop_step1_count, 0) as drop_step1_count,
        coalesce(a.drop_step2_count, 0) as drop_step2_count,
        (coalesce(a.start_step3_count, 0) - coalesce(a.completed_apply_count, 0)) as drop_step3_count,

        -- Outcome metrics
        coalesce(a.offer_count, 0) as offer_count,
        coalesce(a.hired_count, 0) as hired_count,
        coalesce(a.rejected_count, 0) as rejected_count,

        -- Processing time metrics
        coalesce(a.avg_days_to_process, 0.0) as avg_days_to_process,
        coalesce(a.max_days_to_process, 0) as max_days_to_process,

        -- Conversion rates (calculated metrics)
        case
            when coalesce(e.view_count, 0) > 0
            then cast(coalesce(e.apply_click_count, 0) as float64) / e.view_count
            else 0.0
        end as view_to_click_rate,

        case
            when coalesce(e.apply_click_count, 0) > 0
            then cast(coalesce(a.completed_apply_count, 0) as float64) / e.apply_click_count
            else 0.0
        end as click_to_complete_rate,

        case
            when coalesce(a.completed_apply_count, 0) > 0
            then cast(coalesce(a.offer_count, 0) as float64) / a.completed_apply_count
            else 0.0
        end as complete_to_offer_rate,

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
        on e.job_key = a.job_key
        and e.date_key = a.date_key

    where coalesce(e.job_key, a.job_key) is not null  -- Ensure we have valid job
)

select * from daily_performance
order by datekey desc, job_key

{{
    config(
        materialized='incremental',
        schema='gold',
        unique_key=['user_id_natural', 'job_id_natural', 'date_key']
    )
}}

-- Gold layer: Job Content Engagement Fact Table (Reason Engine)
-- Engagement metrics from streaming events
-- Grain: One row per user + job + date (dimensional grain)
-- Updated to match data model specification

with streaming_events as (
    select
        event_id,
        user_id,
        job_id,
        event_timestamp,
        event_type,
        duration_sec,
        scroll_depth_percent,
        _ingestion_time
    from {{ ref('txn_streaming_job_activity') }}

    {% if is_incremental() %}
    -- Incremental load: only process new events
    where _ingestion_time > (select max(_ingestion_time) from {{ this }})
    {% endif %}
),

-- Direct aggregation to user + job + date grain (no session level)
engagement_metrics as (
    select
        -- Grain: user + job + date
        user_id,
        job_id,
        cast(event_timestamp as date) as event_date,

        -- Aggregate VIEW metrics
        sum(case when event_type = 'VIEW' then 1 else 0 end) as view_count,

        -- Calculate average and max duration from VIEW events
        avg(case when event_type = 'VIEW' then duration_sec else null end) as avg_time_on_page_seconds,
        max(case when event_type = 'VIEW' then duration_sec else null end) as max_time_on_page_seconds,

        -- Calculate max scroll depth from SCROLL events
        max(case when event_type = 'SCROLL' then scroll_depth_percent else null end) as max_scroll_depth_percent,

        -- Check if user applied for this job on this date
        max(case when event_type = 'APPLY_BUTTON_CLICK' then 1 else 0 end) as did_apply,

        -- Count bounce VIEW events: duration < 30s (will check for apply later)
        sum(case
            when event_type = 'VIEW' and coalesce(duration_sec, 0) < 30 then 1
            else 0
        end) as bounce_view_count,

        -- Count deep read VIEW events: duration > 240s AND scroll > 70% (will check for apply later)
        sum(case
            when event_type = 'VIEW'
                 and coalesce(duration_sec, 0) > 240 then 1
            else 0
        end) as deep_view_count,

        -- Check if max scroll > 70% for deep read
        max(case
            when event_type = 'SCROLL' and scroll_depth_percent > 70 then 1
            else 0
        end) as has_deep_scroll

    from streaming_events
    group by user_id, job_id, cast(event_timestamp as date)
),

-- Calculate final metrics with apply check
final_metrics as (
    select
        user_id,
        job_id,
        event_date,
        view_count,
        avg_time_on_page_seconds,
        max_time_on_page_seconds,
        max_scroll_depth_percent,

        -- Bounce: short views AND no apply
        case when did_apply = 0 then bounce_view_count else 0 end as bounce_no_apply_count,

        -- Deep read: long views with deep scroll AND no apply
        case
            when did_apply = 0 and has_deep_scroll = 1 then deep_view_count
            else 0
        end as deep_read_no_apply_count

    from engagement_metrics
),

-- Join with dimensions
fact_engagement as (
    select
        -- Primary key (composite surrogate key: user + job + date)
        {{ dbt_utils.generate_surrogate_key([
            'du.user_key',
            'dj.job_key',
            'dd.date_key'
        ]) }} as engagement_id,

        -- Dimension foreign keys (grain)
        du.user_id_natural,  -- Nullable - only populated for authenticated users
        dj.job_id_natural,
        dd.date_key,

        -- Engagement metrics (matching data model spec)
        cast(fm.view_count as int64) as view_count,
        cast(coalesce(fm.avg_time_on_page_seconds, 0) as int64) as avg_time_on_page_seconds,
        cast(coalesce(fm.max_time_on_page_seconds, 0) as int64) as max_time_on_page_seconds,
        -- cast(coalesce(fm.max_scroll_depth_percent, 0) as int64) as max_scroll_depth_percent,
        fm.max_scroll_depth_percent,

        -- Bounce and deep read counts (user+job+date level)
        cast(fm.bounce_no_apply_count as int64) as bounce_no_apply_count,
        cast(fm.deep_read_no_apply_count as int64) as deep_read_no_apply_count,

        current_timestamp as _ingestion_time

    from final_metrics fm
    left join {{ ref('dim_job') }} dj
        on fm.job_id = dj.job_id_natural
        and dj.is_current = true
    left join {{ ref('dim_user') }} du
        on fm.user_id = du.user_id_natural
        and du.is_current = true
    left join {{ ref('dim_date') }} dd
        on cast(format_date('%Y%m%d', fm.event_date) as int64) = dd.date_key
)

select * from fact_engagement

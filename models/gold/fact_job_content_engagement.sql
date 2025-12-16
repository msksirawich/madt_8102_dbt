{{
    config(
        materialized='incremental',
        schema='gold',
        unique_key='engagement_id'
    )
}}

-- Gold layer: Job Content Engagement Fact Table
-- Session-level engagement metrics from streaming events
-- Grain: One row per session (session_id)

with streaming_events as (
    select
        event_id,
        session_id,
        user_cookie_id,
        job_id,
        event_timestamp,
        event_type,
        event_properties,
        _ingestion_time
    from {{ ref('streaming_job_activity') }}

    {% if is_incremental() %}
    -- Incremental load: only process new events
    where _ingestion_time > (select max(_ingestion_time) from {{ this }})
    {% endif %}
),

-- Step 1: Sessionize events and calculate engagement metrics
session_metrics as (
    select
        session_id,
        cast(job_id as integer) as job_id,
        user_cookie_id,  -- Keep as string (it's a hash, not a user_id)
        cast(min(event_timestamp) as date) as event_date,

        -- Duration: Time from first to last event in session (in seconds)
        date_diff('second', min(event_timestamp), max(event_timestamp)) as time_on_page_seconds,

        -- Max scroll depth from SCROLL events
        max(
            case
                when event_type = 'SCROLL'
                then cast(json_extract_string(event_properties, '$.scroll_depth_percent') as integer)
                else 0
            end
        ) as max_scroll_depth_percent,

        -- Did user click apply button?
        max(
            case
                when event_type = 'CLICK'
                     and json_extract_string(event_properties, '$.element_id') = 'apply_btn'
                then 1
                else 0
            end
        ) as did_click_apply,

        -- Count of different event types
        sum(case when event_type = 'PAGE_VIEW' then 1 else 0 end) as page_view_count,
        sum(case when event_type = 'SCROLL' then 1 else 0 end) as scroll_count,
        sum(case when event_type = 'CLICK' then 1 else 0 end) as click_count,
        sum(case when event_type = 'HEARTBEAT' then 1 else 0 end) as heartbeat_count,

        -- First and last event timestamps
        min(event_timestamp) as session_start,
        max(event_timestamp) as session_end,

        -- Ingestion time for incremental processing
        max(_ingestion_time) as _ingestion_time

    from streaming_events
    group by session_id, job_id, user_cookie_id
),

-- Step 2: Join with dimensions
fact_engagement as (
    select
        -- Primary key
        sm.session_id as engagement_id,

        -- Dimension foreign keys
        dj.job_key,
        cast(null as varchar) as user_key,  -- Cookie ID doesn't map to user_id
        dd.date_key,

        -- Time dimension (optional - based on session start hour)
        cast(strftime(sm.session_start, '%H') || '0000' as integer) as time_key,

        -- User identification (cookie-based, not authenticated user)
        sm.user_cookie_id,

        -- Engagement metrics
        sm.time_on_page_seconds,
        coalesce(sm.max_scroll_depth_percent, 0) as max_scroll_depth_percent,

        -- Bounce logic: session < 10 seconds
        case
            when sm.time_on_page_seconds < 10 then true
            else false
        end as is_bounce,

        -- Apply click flag
        case when sm.did_click_apply = 1 then true else false end as did_click_apply,

        -- Event counts
        sm.page_view_count,
        sm.scroll_count,
        sm.click_count,
        sm.heartbeat_count,

        -- Session timestamps
        sm.session_start,
        sm.session_end,
        sm.event_date,

        -- Engagement quality classification
        case
            when sm.time_on_page_seconds >= 120 and sm.max_scroll_depth_percent >= 70 then 'High'
            when sm.time_on_page_seconds >= 30 and sm.max_scroll_depth_percent >= 30 then 'Medium'
            else 'Low'
        end as engagement_quality,

        -- Deep read without apply (key KPI)
        case
            when sm.max_scroll_depth_percent > 70 and sm.did_click_apply = 0 then true
            else false
        end as is_deep_read_no_apply,

        -- Audit fields
        sm._ingestion_time,
        current_timestamp as _created_at

    from session_metrics sm
    left join {{ ref('dim_job') }} dj
        on sm.job_id = dj.job_id_natural
        and dj.is_current = true
    left join {{ ref('dim_date') }} dd
        on cast(strftime(sm.event_date, '%Y%m%d') as integer) = dd.date_key
)

select * from fact_engagement

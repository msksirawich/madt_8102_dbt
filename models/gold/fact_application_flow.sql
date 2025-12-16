{{
    config(
        materialized='incremental',
        schema='gold',
        unique_key='flow_id'
    )
}}

-- Gold layer: Application Flow Fact Table (Accumulating Snapshot)
-- Tracks application funnel progression and outcomes
-- Grain: One row per application

with applications_base as (
    select
        application_id,
        user_id,
        job_id,
        current_status,
        applied_at,
        created_at,
        updated_at,
        _ingestion_time
    from {{ ref('txn_applications') }}

    {% if is_incremental() %}
    -- Incremental load: only process new or updated applications
    where _ingestion_time > (select max(_ingestion_time) from {{ this }})
    {% endif %}
),

-- Optional: Join with streaming events to track funnel steps
application_with_steps as (
    select
        a.application_id,
        a.user_id,
        a.job_id,
        a.current_status,
        a.applied_at,
        a.created_at,
        a.updated_at,
        a._ingestion_time,

        -- Calculate exit step based on status
        case
            when a.current_status in ('applied', 'rejected', 'offer', 'accepted') then 3  -- Completed application
            when a.current_status = 'in_progress' then 2  -- Started but not submitted
            else 1  -- Viewed but not started
        end as exit_step,

        -- Completion flag
        case when a.current_status = 'applied' then true else false end as is_completed,

        -- Success flags
        case when a.current_status = 'offer' then true else false end as received_offer,
        case when a.current_status = 'accepted' then true else false end as accepted_offer,
        case when a.current_status = 'rejected' then true else false end as was_rejected

    from applications_base a
),

-- Join with dimensions
fact_application as (
    select
        -- Primary key
        cast(a.user_id as string) || '_' ||
        cast(a.job_id as string) || '_' ||
        cast(a.application_id as string) as flow_id,

        -- Dimension foreign keys
        dj.job_key,
        du.user_key,
        dd.date_key,

        -- Time dimension (optional - based on application hour)
        cast(format_timestamp('%H', cast(a.applied_at as timestamp)) || '0000' as int64) as time_key,

        -- Application identifiers
        a.application_id,

        -- Funnel metrics
        a.exit_step,
        a.is_completed,

        -- Status tracking
        a.current_status,
        a.received_offer,
        a.accepted_offer,
        a.was_rejected,

        -- Drop-off analysis
        case when a.exit_step < 2 then true else false end as dropped_at_step1,
        case when a.exit_step < 3 then true else false end as dropped_at_step2,

        -- Time metrics
        a.applied_at,
        cast(a.applied_at as date) as applied_date,
        extract(hour from cast(a.applied_at as timestamp)) as applied_hour,
        extract(dayofweek from cast(a.applied_at as timestamp)) as applied_day_of_week,

        -- Processing time (if applicable)
        timestamp_diff(cast(a.updated_at as timestamp), cast(a.applied_at as timestamp), day) as days_since_application,

        -- Application funnel classification
        case
            when a.current_status = 'accepted' then 'Hired'
            when a.current_status = 'offer' then 'Offer Extended'
            when a.current_status = 'rejected' then 'Rejected'
            when a.current_status = 'applied' then 'Under Review'
            when a.current_status = 'in_progress' then 'Incomplete'
            else 'Other'
        end as application_stage,

        -- Audit fields
        a.created_at as application_created_at,
        a.updated_at as application_updated_at,
        a._ingestion_time,
        current_timestamp as _created_at

    from application_with_steps a
    left join {{ ref('dim_job') }} dj
        on a.job_id = dj.job_id_natural
        and dj.is_current = true
    left join {{ ref('dim_user') }} du
        on a.user_id = du.user_id_natural
        and du.is_current = true
    left join {{ ref('dim_date') }} dd
        on cast(format_date('%Y%m%d', cast(a.applied_at as date)) as int64) = dd.date_key
)

select * from fact_application

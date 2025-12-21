{{
    config(
        materialized='incremental',
        schema='gold',
        unique_key=['user_id_natural', 'job_id_natural', 'date_key']
    )
}}

-- Gold layer: Application Flow Fact Table (Funnel Engine)
-- Tracks application funnel progression from streaming events
-- Grain: One row per user + job + date
-- Data source: txn_streaming_job_activity (streaming events)

with streaming_events as (
    select
        user_id,
        job_id,
        event_timestamp,
        event_type,
        -- Form interaction details
        current_step,
        step_status,
        -- Submission details
        total_steps_completed,
        _ingestion_time
    from {{ ref('txn_streaming_job_activity') }}
    where event_type in ('APPLY_BUTTON_CLICK', 'APPLICATION_FORM_INTERACTION', 'APPLICATION_SUBMISSION')

    {% if is_incremental() %}
    -- Incremental load: only process new events
    and _ingestion_time > (select max(_ingestion_time) from {{ this }})
    {% endif %}
),

-- Aggregate to user + job + date grain
application_flow_metrics as (
    select
        user_id,
        job_id,
        cast(event_timestamp as date) as application_date,

        -- Check if application was completed (has submission event)
        max(case when event_type = 'APPLICATION_SUBMISSION' then 1 else 0 end) as is_completed,

        -- Get max step reached from submission or form interactions
        coalesce(
            max(case when event_type = 'APPLICATION_SUBMISSION' then total_steps_completed end),
            max(case when event_type = 'APPLICATION_FORM_INTERACTION' and step_status = 'complete' then current_step end),
            0
        ) as max_step_reached,

        -- Count step starts (step_status = 'start')
        sum(case when current_step = 1 and step_status = 'start' then 1 else 0 end) as start_step1_count,
        sum(case when current_step = 2 and step_status = 'start' then 1 else 0 end) as start_step2_count,
        sum(case when current_step = 3 and step_status = 'start' then 1 else 0 end) as start_step3_count,

        -- Track if any step was completed
        max(case when current_step = 1 and step_status = 'complete' then 1 else 0 end) as completed_step1,
        max(case when current_step = 2 and step_status = 'complete' then 1 else 0 end) as completed_step2,
        max(case when current_step = 3 and step_status = 'complete' then 1 else 0 end) as completed_step3

    from streaming_events
    group by user_id, job_id, cast(event_timestamp as date)
),

-- Calculate drop-off counts
application_flow_final as (
    select
        user_id,
        job_id,
        application_date,
        max_step_reached,
        is_completed,

        -- Step start counts
        start_step1_count,
        start_step2_count,
        start_step3_count,

        -- Calculate drop-offs (count of steps where user started but didn't progress)
        -- Drop at step 1: started step 1 but didn't complete it
        case
            when start_step1_count > 0 and completed_step1 = 0 then start_step1_count
            else 0
        end as drop_step1_count,

        -- Drop at step 2: started step 2 but didn't complete it
        case
            when start_step2_count > 0 and completed_step2 = 0 then start_step2_count
            else 0
        end as drop_step2_count,

        -- Drop at step 3: started step 3 but didn't complete application
        case
            when start_step3_count > 0 and is_completed = 0 then start_step3_count
            else 0
        end as drop_step3_count

    from application_flow_metrics
),

-- Join with dimensions
fact_application as (
    select
        -- Primary key (composite surrogate key: user + job + date)
        {{ dbt_utils.generate_surrogate_key([
            'du.user_key',
            'dj.job_key',
            'dd.date_key'
        ]) }} as flow_id,

        -- Dimension foreign keys
        dj.job_id_natural,
        du.user_id_natural,
        dd.date_key,

        -- Funnel metrics (matching data model spec)
        cast(af.max_step_reached as int64) as max_step_reached,
        cast(af.is_completed as bool) as is_completed,

        -- Step measurements (matching data model spec)
        cast(af.start_step1_count as int64) as start_step1_count,
        cast(af.start_step2_count as int64) as start_step2_count,
        cast(af.start_step3_count as int64) as start_step3_count,
        cast(af.drop_step1_count as int64) as drop_step1_count,
        cast(af.drop_step2_count as int64) as drop_step2_count,
        cast(af.drop_step3_count as int64) as drop_step3_count,

        current_timestamp as _ingestion_time

    from application_flow_final af
    left join {{ ref('dim_job') }} dj
        on af.job_id = dj.job_id_natural
        and dj.is_current = true
    left join {{ ref('dim_user') }} du
        on af.user_id = du.user_id_natural
        and du.is_current = true
    left join {{ ref('dim_date') }} dd
        on cast(format_date('%Y%m%d', af.application_date) as int64) = dd.date_key
)

select * from fact_application

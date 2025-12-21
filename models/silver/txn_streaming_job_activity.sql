{{
    config(
        materialized='incremental',
        unique_key='event_id',
        schema='silver'
    )
}}

-- Silver layer: Cleaned and standardized streaming event data
-- Applies data quality rules, standardization, and extracts JSON properties
-- Updated to match new event taxonomy: VIEW, SCROLL, APPLY_BUTTON_CLICK, APPLICATION_FORM_INTERACTION, APPLICATION_SUBMISSION

with source as (
    select *
    from {{ ref('streaming_job_activity') }}
    where CAST(event_timestamp AS DATE) = CAST('{{ var("execution_date") }}' AS DATE)
),

cleaned as (
    select
        event_id,
        session_id,
        user_cookie_id,
        user_id,
        job_id,
        event_timestamp,
        upper(trim(event_type)) as event_type,
        event_properties,

        -- Extract common properties from JSON based on event_type
        -- VIEW properties
        case when upper(trim(event_type)) = 'VIEW'
            then cast(JSON_EXTRACT_SCALAR(event_properties, '$.load_time_ms') as int64)
            else null
        end as load_time_ms,

        case when upper(trim(event_type)) = 'VIEW'
            then cast(JSON_EXTRACT_SCALAR(event_properties, '$.duration_sec') as int64)
            else null
        end as duration_sec,

        case when upper(trim(event_type)) = 'VIEW'
            then JSON_EXTRACT_SCALAR(event_properties, '$.referrer')
            else null
        end as referrer,

        -- SCROLL properties
        case when upper(trim(event_type)) = 'SCROLL'
            then cast(JSON_EXTRACT_SCALAR(event_properties, '$.scroll_depth_percent') as int64)
            else null
        end as scroll_depth_percent,

        case when upper(trim(event_type)) = 'SCROLL'
            then cast(JSON_EXTRACT_SCALAR(event_properties, '$.max_scroll_px') as int64)
            else null
        end as max_scroll_px,

        -- APPLY_BUTTON_CLICK properties
        case when upper(trim(event_type)) = 'APPLY_BUTTON_CLICK'
            then JSON_EXTRACT_SCALAR(event_properties, '$.button_location')
            else null
        end as button_location,

        case when upper(trim(event_type)) = 'APPLY_BUTTON_CLICK'
            then cast(JSON_EXTRACT_SCALAR(event_properties, '$.is_authenticated') as bool)
            else null
        end as is_authenticated,

        case when upper(trim(event_type)) = 'APPLY_BUTTON_CLICK'
            then JSON_EXTRACT_SCALAR(event_properties, '$.application_session_id')
            else null
        end as apply_session_id,

        -- APPLICATION_FORM_INTERACTION properties
        case when upper(trim(event_type)) = 'APPLICATION_FORM_INTERACTION'
            then JSON_EXTRACT_SCALAR(event_properties, '$.application_session_id')
            else null
        end as form_session_id,

        case when upper(trim(event_type)) = 'APPLICATION_FORM_INTERACTION'
            then cast(JSON_EXTRACT_SCALAR(event_properties, '$.current_step') as int64)
            else null
        end as current_step,

        case when upper(trim(event_type)) = 'APPLICATION_FORM_INTERACTION'
            then JSON_EXTRACT_SCALAR(event_properties, '$.step_status')
            else null
        end as step_status,

        -- APPLICATION_SUBMISSION properties
        case when upper(trim(event_type)) = 'APPLICATION_SUBMISSION'
            then JSON_EXTRACT_SCALAR(event_properties, '$.application_session_id')
            else null
        end as submission_session_id,

        case when upper(trim(event_type)) = 'APPLICATION_SUBMISSION'
            then JSON_EXTRACT_SCALAR(event_properties, '$.application_id')
            else null
        end as application_id,

        case when upper(trim(event_type)) = 'APPLICATION_SUBMISSION'
            then cast(JSON_EXTRACT_SCALAR(event_properties, '$.is_complete') as bool)
            else null
        end as is_complete,

        case when upper(trim(event_type)) = 'APPLICATION_SUBMISSION'
            then cast(JSON_EXTRACT_SCALAR(event_properties, '$.total_steps_completed') as int64)
            else null
        end as total_steps_completed,

        case when upper(trim(event_type)) = 'APPLICATION_SUBMISSION'
            then cast(JSON_EXTRACT_SCALAR(event_properties, '$.total_form_time_sec') as int64)
            else null
        end as total_form_time_sec,

        current_timestamp as _ingestion_time,
        dt
    from source
    where
        -- Data quality filters
        event_id is not null
        and session_id is not null
        and event_timestamp is not null
        and event_type is not null
        and event_type in ('VIEW', 'SCROLL', 'APPLY_BUTTON_CLICK', 'APPLICATION_FORM_INTERACTION', 'APPLICATION_SUBMISSION')
)

select * from cleaned

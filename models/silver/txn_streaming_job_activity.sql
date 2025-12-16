{{
    config(
        materialized='table',
        schema='silver'
    )
}}

-- Silver layer: Cleaned and standardized streaming event data
-- Applies data quality rules, standardization, and extracts JSON properties

with source as (
    select *
    from {{ ref('streaming_job_activity') }}
),

cleaned as (
    select
        event_id,
        session_id,
        user_cookie_id,
        job_id,
        event_timestamp,
        upper(trim(event_type)) as event_type,
        event_properties,

        -- Extract common properties from JSON based on event_type
        -- PAGE_VIEW properties
        case when upper(trim(event_type)) = 'PAGE_VIEW'
            then cast(JSON_EXTRACT_SCALAR(event_properties, '$.load_time_ms') as int64)
            else null
        end as load_time_ms,

        -- SCROLL properties
        case when upper(trim(event_type)) = 'SCROLL'
            then cast(JSON_EXTRACT_SCALAR(event_properties, '$.scroll_depth_percent') as int64)
            else null
        end as scroll_depth_percent,

        case when upper(trim(event_type)) = 'SCROLL'
            then cast(JSON_EXTRACT_SCALAR(event_properties, '$.max_scroll_px') as int64)
            else null
        end as max_scroll_px,

        -- HEARTBEAT properties
        case when upper(trim(event_type)) = 'HEARTBEAT'
            then cast(JSON_EXTRACT_SCALAR(event_properties, '$.time_since_load_sec') as int64)
            else null
        end as time_since_load_sec,

        case when upper(trim(event_type)) = 'HEARTBEAT'
            then cast(JSON_EXTRACT_SCALAR(event_properties, '$.is_active') as bool)
            else null
        end as is_active,

        -- CLICK properties
        case when upper(trim(event_type)) = 'CLICK'
            then JSON_EXTRACT_SCALAR(event_properties, '$.element_id')
            else null
        end as element_id,

        case when upper(trim(event_type)) = 'CLICK'
            then JSON_EXTRACT_SCALAR(event_properties, '$.button_text')
            else null
        end as button_text,

        current_timestamp as _ingestion_time,
        dt
    from source
    where
        -- Data quality filters
        event_id is not null
        and session_id is not null
        and event_timestamp is not null
        and event_type is not null
        and event_type in ('PAGE_VIEW', 'SCROLL', 'CLICK', 'HEARTBEAT')
)

select * from cleaned

{{
    config(
        materialized='view',
        schema='bronze'
    )
}}

-- Bronze layer: Raw streaming event data as-is from source
-- Raw immutable event stream from frontend applications

select
    event_id,
    session_id,
    user_cookie_id,
    job_id,
    event_timestamp,
    event_type,
    event_properties,
    _ingestion_time,
    dt,
from {{ source('bronze', 'streaming_job_activity') }}

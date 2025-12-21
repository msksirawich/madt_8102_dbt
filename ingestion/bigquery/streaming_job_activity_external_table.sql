-- BigQuery External Table DDL for Streaming Job Activity
-- This table reads from GCS with Hive-style partitioning (dt=YYYY-MM-DD)

CREATE OR REPLACE EXTERNAL TABLE `madt-8102-479812.madt8102_bronze.streaming_job_activity`
(
  event_id STRING,
  session_id STRING,
  user_cookie_id STRING,
  user_id STRING,
  job_id STRING,
  event_timestamp TIMESTAMP,
  event_type STRING,
  event_properties STRING,
  _ingestion_time TIMESTAMP
)
WITH PARTITION COLUMNS (
  dt DATE  -- Hive partition column
)
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://madt8102_bronze/streaming_job_activity/*'],  -- Pattern: dt=YYYY-MM-DD/*.parquet
  hive_partition_uri_prefix = 'gs://madt8102_bronze/streaming_job_activity',
  require_hive_partition_filter = false
);

-- Notes:
-- 1. The 'dt' column is automatically populated from the Hive partition path
-- 2. Data location: gs://madt8102_bronze/streaming_job_activity/dt=YYYY-MM-DD/
-- 3. require_hive_partition_filter = false allows queries without partition filter
--    Set to true to enforce partition filtering for cost optimization
-- 4. _ingestion_time is automatically added during ingestion
-- 5. event_properties is stored as JSON STRING - use JSON functions to parse

-- Example queries:

-- Query all data
-- SELECT * FROM `madt-8102-479812.madt8102_bronze.streaming_job_activity`;

-- Query specific partition (recommended for large datasets)
-- SELECT * FROM `madt-8102-479812.madt8102_bronze.streaming_job_activity`
-- WHERE dt = '2024-12-01';

-- Query with date range
-- SELECT * FROM `madt-8102-479812.madt8102_bronze.streaming_job_activity`
-- WHERE dt BETWEEN '2024-12-01' AND '2024-12-07';

-- Parse JSON properties
-- SELECT
--   event_id,
--   event_type,
--   JSON_EXTRACT_SCALAR(event_properties, '$.scroll_depth_percent') as scroll_depth,
--   dt
-- FROM `madt-8102-479812.madt8102_bronze.streaming_job_activity`
-- WHERE dt = '2024-12-01' AND event_type = 'SCROLL';

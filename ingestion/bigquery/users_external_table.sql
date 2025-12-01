-- BigQuery External Table DDL for Users
-- This table reads from GCS with Hive-style partitioning (dt=YYYY-MM-DD)

CREATE OR REPLACE EXTERNAL TABLE `your-project-id.your_dataset.users_external`
(
  id INT64,
  name STRING,
  email STRING,
  status STRING,
  created_at DATE,
  updated_at DATE
)
WITH PARTITION COLUMNS (
  dt DATE  -- Hive partition column
)
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://madt8102_bronze/users/dt=*/*'],  -- Pattern: dt=YYYY-MM-DD/*.parquet
  hive_partition_uri_prefix = 'gs://madt8102_bronze/users',
  require_hive_partition_filter = false
);

-- Notes:
-- 1. Replace 'your-project-id' and 'your_dataset' with your actual values
-- 2. The 'dt' column is automatically populated from the Hive partition path
-- 3. Data location: gs://madt8102_bronze/users/dt=YYYY-MM-DD/
-- 4. require_hive_partition_filter = false allows queries without partition filter
--    Set to true to enforce partition filtering for cost optimization

-- Example queries:

-- Query all data
-- SELECT * FROM `your-project-id.your_dataset.users_external`;

-- Query specific partition (recommended for large datasets)
-- SELECT * FROM `your-project-id.your_dataset.users_external`
-- WHERE dt = '2024-12-01';

-- Query date range
-- SELECT * FROM `your-project-id.your_dataset.users_external`
-- WHERE dt BETWEEN '2024-12-01' AND '2024-12-03';

-- Count records by partition
-- SELECT dt, COUNT(*) as record_count
-- FROM `your-project-id.your_dataset.users_external`
-- GROUP BY dt
-- ORDER BY dt;

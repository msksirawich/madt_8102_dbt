-- BigQuery External Table DDL for Orders
-- This table reads from GCS with Hive-style partitioning (dt=YYYY-MM-DD)

CREATE OR REPLACE EXTERNAL TABLE `madt-8102-479812.madt8102_bronze.orders`
(
  order_id STRING,
  customer_id STRING,
  order_date STRING,
  quantity STRING,
  amount STRING,
  status STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
)
WITH PARTITION COLUMNS (
  dt DATE  -- Hive partition column
)
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://madt8102_bronze/orders/*'],  -- Pattern: dt=YYYY-MM-DD/*.parquet
  hive_partition_uri_prefix = 'gs://madt8102_bronze/orders',
  require_hive_partition_filter = false
);

-- Notes:
-- 1. Replace 'your-project-id' and 'your_dataset' with your actual values
-- 2. The 'dt' column is automatically populated from the Hive partition path
-- 3. Data location: gs://madt8102_bronze/orders/dt=YYYY-MM-DD/
-- 4. require_hive_partition_filter = false allows queries without partition filter
--    Set to true to enforce partition filtering for cost optimization

-- Example queries:

-- Query all data
-- SELECT * FROM `your-project-id.your_dataset.orders_external`;

-- Query specific partition (recommended for large datasets)
-- SELECT * FROM `your-project-id.your_dataset.orders_external`
-- WHERE dt = '2024-12-01';

-- Query with JOIN
-- SELECT
--   o.order_id,
--   o.product_name,
--   o.amount,
--   u.name as customer_name,
--   u.email
-- FROM `your-project-id.your_dataset.orders_external` o
-- JOIN `your-project-id.your_dataset.users_external` u
--   ON o.user_id = u.id
-- WHERE o.dt = '2024-12-01';

-- Aggregate by partition
-- SELECT
--   dt,
--   COUNT(*) as order_count,
--   SUM(amount) as total_revenue
-- FROM `your-project-id.your_dataset.orders_external`
-- GROUP BY dt
-- ORDER BY dt;

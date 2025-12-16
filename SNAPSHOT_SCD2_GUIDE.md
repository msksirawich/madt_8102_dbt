# SCD Type 2 Implementation with dbt Snapshots

## Overview

This project uses **dbt snapshots** for implementing Slowly Changing Dimension Type 2 (SCD2) logic for `dim_user` and `dim_job` dimensions. This is the recommended best practice for SCD2 in dbt.

---

## Why Use dbt Snapshots?

### ❌ Without Snapshots (Manual SCD2)
- Complex logic to detect changes
- Manual tracking of valid_from/valid_to timestamps
- Risk of data inconsistencies
- Difficult to maintain

### ✅ With dbt Snapshots
- Built-in SCD2 logic
- Automatic timestamp management (`dbt_valid_from`, `dbt_valid_to`)
- Reliable change detection
- Easy to maintain and audit

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                  BRONZE LAYER (Source)                  │
│  users, profiles, education, work_history, job_postings │
└────────────────────┬────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────┐
│                  SILVER LAYER (Cleaned)                 │
│   mst_users, mst_profiles, mst_education, etc.          │
└────────────────────┬────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────┐
│               SNAPSHOTS (SCD2 Tracking)                 │
│   snap_user_dimension, snap_job_dimension               │
│   • Tracks historical changes                           │
│   • Adds dbt_valid_from, dbt_valid_to, dbt_updated_at  │
└────────────────────┬────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────┐
│           GOLD LAYER (Analytical Dimensions)            │
│   dim_user, dim_job (reference snapshots)               │
│   • Adds surrogate keys                                 │
│   • Adds business logic                                 │
└─────────────────────────────────────────────────────────┘
```

---

## Snapshot Configuration

### 1. `snap_user_dimension`

**Location**: `snapshots/snap_user_dimension.sql`

**Configuration**:
```yaml
strategy: check
unique_key: user_id
check_cols:
  - headline
  - highest_degree
  - years_experience
  - current_position
  - current_salary
```

**Behavior**:
- Monitors the specified `check_cols` for changes
- When any tracked column changes, a new version is created:
  - Old record: `dbt_valid_to` set to current timestamp
  - New record: `dbt_valid_from` set to current timestamp, `dbt_valid_to` = NULL

**Example Change Detection**:
```sql
-- Original record (Day 1)
user_id: 123
headline: "Software Engineer"
years_experience: 5
dbt_valid_from: 2025-01-01
dbt_valid_to: NULL  -- Current

-- After promotion (Day 100)
-- Old record is updated:
user_id: 123
headline: "Software Engineer"
years_experience: 5
dbt_valid_from: 2025-01-01
dbt_valid_to: 2025-04-10  -- Closed

-- New record is created:
user_id: 123
headline: "Senior Software Engineer"  -- CHANGED
years_experience: 5
dbt_valid_from: 2025-04-10
dbt_valid_to: NULL  -- Current
```

---

### 2. `snap_job_dimension`

**Location**: `snapshots/snap_job_dimension.sql`

**Configuration**:
```yaml
strategy: check
unique_key: job_id
check_cols:
  - title
  - description_html
  - min_salary
  - max_salary
  - status
  - company_id
```

**Behavior**:
- Tracks job posting changes (salary updates, description edits, status changes)
- Useful for analyzing how job requirements evolved over time

---

## How to Run Snapshots

### Initial Snapshot (First Run)
```bash
# Create snapshot tables and populate with current state
dbt snapshot

# This creates:
# - gold.snap_user_dimension
# - gold.snap_job_dimension
```

### Daily/Scheduled Runs
```bash
# Run snapshots to capture new changes
dbt snapshot

# Snapshots should run BEFORE dimension models
dbt run --models gold
```

### Recommended Schedule
```bash
# Daily snapshot at midnight
0 0 * * * dbt snapshot --profiles-dir ~/.dbt

# Then run gold layer
5 0 * * * dbt run --models gold --profiles-dir ~/.dbt
```

---

## Dimension Models (Gold Layer)

The gold layer dimension models (`dim_user`, `dim_job`) reference the snapshots and add:
1. **Surrogate keys** using `dbt_utils.generate_surrogate_key()`
2. **Business logic** and derived attributes
3. **User-friendly column names**

### `dim_user.sql`
```sql
select
    {{ dbt_utils.generate_surrogate_key(['user_id', 'dbt_valid_from']) }} as user_key,
    user_id as user_id_natural,
    dbt_valid_from as valid_from,
    dbt_valid_to as valid_to,
    case when dbt_valid_to is null then true else false end as is_current,
    ...
from {{ ref('snap_user_dimension') }}
```

### `dim_job.sql`
```sql
select
    {{ dbt_utils.generate_surrogate_key(['job_id', 'dbt_valid_from']) }} as job_key,
    job_id as job_id_natural,
    dbt_valid_from as valid_from,
    dbt_valid_to as valid_to,
    case when dbt_valid_to is null then true else false end as is_current,
    ...
from {{ ref('snap_job_dimension') }}
```

---

## Testing Snapshots

### 1. Verify Snapshot Creation
```sql
-- Check if snapshots exist
select count(*) from gold.snap_user_dimension;
select count(*) from gold.snap_job_dimension;
```

### 2. View Historical Changes
```sql
-- See all versions of a user
select
    user_id,
    headline,
    years_experience,
    dbt_valid_from,
    dbt_valid_to,
    case when dbt_valid_to is null then 'CURRENT' else 'EXPIRED' end as status
from gold.snap_user_dimension
where user_id = 123
order by dbt_valid_from;
```

### 3. Verify Current Records Only
```sql
-- Get only current versions
select *
from gold.dim_user
where is_current = true;
```

### 4. Simulate a Change
```sql
-- Update source data (in silver layer)
update silver.mst_profiles
set headline = 'Senior Data Engineer'
where user_id = 123;

-- Run snapshot
-- dbt snapshot

-- Verify new version created
select * from gold.snap_user_dimension
where user_id = 123
order by dbt_valid_from desc;
```

---

## Best Practices

### ✅ DO:
1. **Run snapshots before dimension models**
   - Snapshots must run first to capture changes
   - Then run `dbt run --models gold`

2. **Use check strategy for business attributes**
   - Monitor columns that represent business changes
   - Ignore audit columns like `updated_at`

3. **Test snapshot logic**
   - Validate that changes are captured correctly
   - Check that `dbt_valid_to` is set properly

4. **Document tracked columns**
   - Clearly specify what changes trigger new versions

### ❌ DON'T:
1. **Don't track too many columns**
   - Only track meaningful business changes
   - Avoid tracking timestamps or metadata

2. **Don't delete snapshot tables**
   - Snapshots maintain history
   - Deleting = losing historical data

3. **Don't run snapshots too frequently**
   - Daily or hourly is sufficient
   - Real-time SCD2 is complex and usually unnecessary

---

## Dependencies

### Required dbt Packages

Add to `packages.yml`:
```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: 1.1.1
```

Install:
```bash
dbt deps
```

---

## Querying Historical Data

### Example: User's career progression
```sql
with user_history as (
    select
        user_id_natural,
        current_role,
        years_experience,
        career_level,
        valid_from,
        valid_to,
        is_current
    from gold.dim_user
    where user_id_natural = 123
    order by valid_from
)

select
    current_role,
    years_experience,
    career_level,
    date(valid_from) as promotion_date,
    date_diff(
        coalesce(date(valid_to), current_date()),
        date(valid_from),
        day
    ) as days_in_role
from user_history;
```

### Example: Job salary changes over time
```sql
select
    job_id_natural,
    job_title,
    avg_salary,
    salary_tier,
    valid_from,
    valid_to,
    is_current
from gold.dim_job
where job_id_natural = 456
order by valid_from;
```

---

## Troubleshooting

### Issue: Snapshot not detecting changes
**Solution**: Verify that the changed column is in `check_cols`

### Issue: Too many versions created
**Solution**: Remove volatile columns from `check_cols`

### Issue: Missing historical data
**Solution**: Check that snapshots ran before source data was updated

### Issue: dbt_valid_to not set
**Solution**: Ensure `dbt snapshot` runs regularly to close expired records

---

## Monitoring

### Check Snapshot Freshness
```sql
select
    'snap_user_dimension' as snapshot_name,
    max(dbt_updated_at) as last_updated,
    count(*) as total_records,
    sum(case when dbt_valid_to is null then 1 else 0 end) as current_records
from gold.snap_user_dimension

union all

select
    'snap_job_dimension' as snapshot_name,
    max(dbt_updated_at) as last_updated,
    count(*) as total_records,
    sum(case when dbt_valid_to is null then 1 else 0 end) as current_records
from gold.snap_job_dimension;
```

---

## Summary

| Aspect | Implementation |
|--------|----------------|
| **SCD2 Logic** | dbt snapshots (automated) |
| **Change Detection** | `check` strategy on business columns |
| **Surrogate Keys** | Generated in dim models using `dbt_utils` |
| **Refresh Schedule** | Daily via `dbt snapshot` |
| **Storage** | gold schema (BigQuery) |
| **Dependencies** | dbt_utils package |

This approach provides robust, maintainable SCD2 tracking while following dbt best practices.

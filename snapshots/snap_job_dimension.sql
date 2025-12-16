{% snapshot snap_job_dimension %}

{{
    config(
        target_schema='gold',
        target_database=target.database,
        unique_key='job_id',
        strategy='check',
        check_cols=['title', 'description_html', 'min_salary', 'max_salary', 'status', 'company_id'],
        invalidate_hard_deletes=True
    )
}}

-- Snapshot source: Job dimension combining job postings and company information
-- This snapshot captures historical changes to job postings over time
-- Strategy: Check specific columns for changes to create SCD Type 2 records

with job_postings_base as (
    select *
    from {{ ref('mst_job_postings') }}
),

companies_cleaned as (
    select *
    from {{ ref('mst_companies') }}
),

job_snapshot_source as (
    select
        -- Natural key (unique_key for snapshot)
        jp.job_id,

        -- Job attributes
        jp.title,
        jp.description_html,
        jp.min_salary,
        jp.max_salary,
        jp.status,
        jp.published_at,

        -- Salary range calculations
        case
            when jp.min_salary is not null and jp.max_salary is not null
            then (cast(jp.min_salary as decimal) + cast(jp.max_salary as decimal)) / 2
            else null
        end as avg_salary,

        case
            when jp.max_salary is not null and jp.min_salary is not null
            then cast(jp.max_salary as decimal) - cast(jp.min_salary as decimal)
            else null
        end as salary_range_width,

        -- Description word count (stripping HTML tags)
        array_length(
            split(
                regexp_replace(
                    regexp_replace(jp.description_html, '<[^>]*>', ' '),  -- Remove HTML tags
                    '\s+', ' '  -- Normalize whitespace
                ),
                ' '
            )
        ) as description_length_words,

        -- Company attributes (store company_id for joins)
        jp.company_id,
        c.name as company_name,
        c.industry as company_industry,
        c.size_range as company_size,

        -- Job classification
        case
            when lower(jp.title) like '%senior%' or lower(jp.title) like '%lead%' or lower(jp.title) like '%principal%'
            then 'Senior'
            when lower(jp.title) like '%junior%' or lower(jp.title) like '%entry%' or lower(jp.title) like '%associate%'
            then 'Junior'
            else 'Mid'
        end as seniority_level,

        -- Salary tier classification
        case
            when jp.min_salary is not null and jp.max_salary is not null and (cast(jp.min_salary as decimal) + cast(jp.max_salary as decimal)) / 2 >= 150000 then 'High'
            when jp.min_salary is not null and jp.max_salary is not null and (cast(jp.min_salary as decimal) + cast(jp.max_salary as decimal)) / 2 >= 80000 then 'Medium'
            when jp.min_salary is not null and jp.max_salary is not null and (cast(jp.min_salary as decimal) + cast(jp.max_salary as decimal)) / 2 >= 40000 then 'Low'
            else 'Unknown'
        end as salary_tier,

        -- Description quality flags
        case
            when array_length(split(regexp_replace(jp.description_html, '<[^>]*>', ' '), ' ')) >= 300
            then 'Detailed'
            when array_length(split(regexp_replace(jp.description_html, '<[^>]*>', ' '), ' ')) >= 100
            then 'Moderate'
            else 'Brief'
        end as description_quality,

        -- Source timestamps
        jp.created_at as job_created_at,
        jp.updated_at as job_updated_at

    from job_postings_base jp
    inner join companies_cleaned c on jp.company_id = c.company_id
)

select * from job_snapshot_source

{% endsnapshot %}

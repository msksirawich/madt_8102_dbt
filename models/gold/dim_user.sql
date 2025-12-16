{{
    config(
        materialized='table',
        schema='gold'
    )
}}

-- Gold layer: User dimension (SCD Type 2)
-- References snapshot for historical tracking
-- Grain: One row per user per version (tracks historical changes)

with user_snapshot as (
    select * from {{ ref('snap_user_dimension') }}
),

user_dimension as (
    select
        -- Surrogate key (dbt_scd_id from snapshot)
        {{ dbt_utils.generate_surrogate_key(['user_id', 'dbt_valid_from']) }} as user_key,

        -- Natural key
        user_id as user_id_natural,

        -- User identity
        email,
        is_active,

        -- Profile attributes
        headline as current_role,
        summary,
        current_salary,
        willing_to_relocate,

        -- Education attributes
        highest_degree,
        highest_degree_school,

        -- Work experience attributes
        years_experience,
        total_positions,
        current_position,
        current_company,

        -- Derived attributes
        career_level,
        education_rank,

        -- SCD Type 2 tracking fields (from snapshot)
        dbt_valid_from as valid_from,
        dbt_valid_to as valid_to,
        case when dbt_valid_to is null then true else false end as is_current,

        -- Audit fields
        user_created_at,
        user_updated_at,
        current_timestamp as _created_at

    from user_snapshot
)

select * from user_dimension
order by user_id_natural, valid_from

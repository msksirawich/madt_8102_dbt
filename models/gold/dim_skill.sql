{{
    config(
        materialized='table',
        schema='gold'
    )
}}

-- Gold layer: Skill dimension (Type 1 SCD)
-- Master list of skills for job matching and analytics
-- Grain: One row per skill

with skills_base as (
    select *
    from {{ ref('skills') }}
),

skill_dimension as (
    select
        -- Surrogate key
        row_number() over (order by skill_id) as skill_key,

        -- Natural key
        skill_id as skill_id_natural,

        -- Skill attributes
        lower(trim(name)) as skill_name,
        category as skill_category,

        -- Category classification
        case
            when lower(category) in ('tech', 'technical', 'programming') then 'Technical'
            when lower(category) in ('soft skill', 'soft', 'interpersonal') then 'Soft Skill'
            when lower(category) in ('language', 'languages') then 'Language'
            else 'Other'
        end as category_group,

        -- Skill popularity (to be updated via incremental loads)
        -- This would typically be calculated from fact tables
        0 as demand_count,  -- Count of jobs requiring this skill
        0 as supply_count,  -- Count of users having this skill

        -- Audit fields
        created_at as skill_created_at,
        updated_at as skill_updated_at,
        current_timestamp as _created_at

    from skills_base
)

select * from skill_dimension
order by skill_key

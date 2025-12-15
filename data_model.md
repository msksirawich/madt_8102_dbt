project job_conversion_platform {
  database_type: 'bigquery'
  note: 'full data warehouse architecture (bronze -> silver -> gold) for job conversion dashboard & gnn ai.'
}

// ==========================================================
// zone 1: oltp & streaming sources (operational system)
// ==========================================================

tablegroup "oltp_identity_profile" {
  users
  profiles
  education
  work_history
  user_skills
}

tablegroup "oltp_job_market" {
  companies
  job_postings
  skills
  job_skills
}

tablegroup "oltp_transactions" {
  applications
}

tablegroup "raw_event_streams" {
  streaming_job_activity
}

// --- oltp tables ---

table users [note: 'oltp: core identity table. source for dim_user.'] {
  user_id integer [primary key, increment, note: 'primary key']
  email varchar [unique]
  password_hash varchar
  is_active boolean [note: 'status flag']
  created_at timestamp
  updated_at timestamp
  _ingestion_time timestamp [note: 'when this record landed in the data lake/warehouse']
}

table profiles [note: 'oltp: extended user details. source for dim_user features.'] {
  profile_id integer [primary key]
  user_id integer [ref: - users.user_id, note: '1:1 relationship with users']
  headline varchar [note: 'short bio']
  summary text [note: 'full bio']
  current_salary decimal [note: 'used for salary matching features']
  willing_to_relocate boolean [note: 'critical gnn feature']
  created_at timestamp
  updated_at timestamp
  _ingestion_time timestamp [note: 'when this record landed in the data lake/warehouse']
}

table education [note: 'oltp: education history. source for dim_user.highest_degree.'] {
  education_id integer [primary key]
  user_id integer [ref: > users.user_id]
  degree varchar [note: 'bs, ms, phd']
  school_name varchar
  created_at timestamp
  updated_at timestamp
  _ingestion_time timestamp [note: 'when this record landed in the data lake/warehouse']
}

table work_history [note: 'oltp: work experience. source for dim_user.years_experience.'] {
  work_id integer [primary key]
  user_id integer [ref: > users.user_id]
  company_name varchar
  title varchar
  start_date date
  end_date date
  created_at timestamp
  updated_at timestamp
  _ingestion_time timestamp [note: 'when this record landed in the data lake/warehouse']
}

table companies [note: 'oltp: company registry. source for dim_job.company_name.'] {
  company_id integer [primary key]
  name varchar
  industry varchar [note: 'used for industry matching']
  size_range varchar
  created_at timestamp
  updated_at timestamp
  _ingestion_time timestamp [note: 'when this record landed in the data lake/warehouse']
}

table job_postings [note: 'oltp: job catalog. source for dim_job.'] {
  job_id integer [primary key]
  company_id integer [ref: > companies.company_id]
  title varchar
  description_html text [note: 'html content used to calc word count']
  min_salary decimal
  max_salary decimal
  status varchar [note: 'open, closed, draft']
  published_at timestamp
  created_at timestamp
  updated_at timestamp
  _ingestion_time timestamp [note: 'when this record landed in the data lake/warehouse']
}

table skills [note: 'oltp: master list of skills (python, sql). source for dim_skill.'] {
  skill_id integer [primary key]
  name varchar
  category varchar [note: 'tech, soft skill, language']
  created_at timestamp
  updated_at timestamp
  _ingestion_time timestamp [note: 'when this record landed in the data lake/warehouse']
}

table job_skills [note: 'oltp: many-to-many link for job requirements.'] {
  job_id integer [ref: > job_postings.job_id]
  skill_id integer [ref: > skills.skill_id]
  importance integer [note: '1-5 scale. source for fact_job_skill_demand.']
  created_at timestamp
  updated_at timestamp
  _ingestion_time timestamp [note: 'when this record landed in the data lake/warehouse']
  indexes { (job_id, skill_id) [pk] }
}

table user_skills [note: 'oltp: many-to-many link for user competencies.'] {
  user_id integer [ref: > users.user_id]
  skill_id integer [ref: > skills.skill_id]
  proficiency integer [note: '1-5 scale. source for fact_user_skill_competency.']
  created_at timestamp
  updated_at timestamp
  _ingestion_time timestamp [note: 'when this record landed in the data lake/warehouse']
  indexes { (user_id, skill_id) [pk] }
}

table applications [note: 'oltp: application state. source for fact_application_flow.'] {
  application_id integer [primary key]
  user_id integer [ref: > users.user_id]
  job_id integer [ref: > job_postings.job_id]
  current_status varchar [note: 'applied, rejected, offer']
  applied_at timestamp
  created_at timestamp
  updated_at timestamp
  _ingestion_time timestamp [note: 'when this record landed in the data lake/warehouse']
}

// raw streams (json payloads)

table streaming_job_activity [note: 'Bronze Layer: Raw immutable event stream from frontend applications. One row per interaction event.'] {
  
  // Primary Identifiers
  event_id string [primary key, note: 'UUID for the specific event']
  session_id string [note: 'Critical for sessionizing data to calculate duration and bounce rates']
  user_cookie_id string [note: 'Hashed identifier for the user/browser']
  job_id string [note: 'The specific Job Listing ID being interacted with']
  
  // Temporal Data
  event_timestamp timestamp [note: 'UTC timestamp of when the event occurred']

  // Event Classification
  // We use a single table with an event_type discriminator rather than separate tables
  event_type string [note: "Categorical: 'PAGE_VIEW', 'SCROLL', 'CLICK', 'HEARTBEAT'"]
  
  // The Flexible Payload (The "What")
  // Using a JSON/Variant type allows us to store attributes specific to the event_type
  // without creating a sparse table with null columns.
  event_properties json [note: '''
    Dynamic payload based on event_type. Examples:
    - PAGE_VIEW: { "load_time_ms": 200 }
    - SCROLL: { "scroll_depth_percent": 75, "max_scroll_px": 1200 }
    - HEARTBEAT: { "time_since_load_sec": 30, "is_active": true }
    - CLICK: { "element_id": "apply_btn", "button_text": "Apply Now" }
  ''']

  // Metadata
  _ingestion_time timestamp [note: 'When this record landed in the data lake/warehouse']
}


// ==========================================================
// zone 2: data warehouse (silver layer - dimensions & facts)
// ==========================================================

tablegroup "dw_dimensions" {
  dim_date
  dim_time
  dim_user
  dim_job
  dim_skill
}

tablegroup "dw_facts" {
  fact_application_flow
  fact_job_content_engagement
  fact_job_skill_demand
  fact_user_skill_competency
}

table dim_date [note: 'dimension: standard calendar.'] {
  date_key integer [primary key, note: 'yyyymmdd']
  full_date date
  is_weekend boolean
  is_holiday boolean
}

table dim_time [note: 'dimension: time of day buckets.'] {
  time_key integer [primary key, note: 'hhmmss']
  time_segment varchar [note: 'morning, work hours, evening']
}

table dim_user [note: 'dimension: user attributes.'] {
  user_key integer [primary key, note: 'surrogate key']
  user_id_natural integer [note: 'source: users.user_id']
  current_role varchar [note: 'source: profiles.headline']
  years_experience integer [note: 'calc from work_history']
  highest_degree varchar [note: 'calc from education']
  valid_from timestamp [note: 'scd type 2 start']
  valid_to timestamp [note: 'scd type 2 end']
  is_current boolean [note: 'current active record flag']
}

table dim_job [note: 'dimension: job attributes.'] {
  job_key integer [primary key, note: 'surrogate key']
  job_id_natural integer [note: 'source: job_postings.job_id']
  job_title varchar
  company_name varchar
  company_industry varchar
  description_length_words integer [note: 'calc from description_html word count']
  valid_from timestamp
  valid_to timestamp
  is_current boolean
}

table dim_skill [note: 'dimension: skill catalog.'] {
  skill_key integer [primary key, note: 'surrogate key']
  skill_name varchar
  category varchar
}

// --- facts ---

table fact_job_content_engagement [note: 'fact: "reason engine". session-based engagement metrics.'] {
  engagement_id varchar [primary key, note: 'session id']
  job_key integer [ref: > dim_job.job_key]
  user_key integer [ref: > dim_user.user_key]
  date_key integer [ref: > dim_date.date_key]
  time_on_page_seconds integer [note: 'derived from streaming_events_view.duration_ms']
  max_scroll_depth_percent integer [note: 'derived from streaming_events_view.max_scroll_percent']
  is_bounce boolean [note: 'logic: duration < 10s']
  did_click_apply boolean [note: 'logic: true if apply click in session']
}

table fact_application_flow [note: 'fact: "funnel engine". stitches intent (stream) with reality (oltp).'] {
  flow_id varchar [primary key]
  job_key integer [ref: > dim_job.job_key]
  user_key integer [ref: > dim_user.user_key]
  date_key integer [ref: > dim_date.date_key]
  exit_step integer [note: 'last completed step (1, 2, or 3)']
  is_completed boolean [note: 'true if status = applied']
}

table fact_job_skill_demand [note: 'fact: gnn graph edge. job->skill.'] {
  job_key integer [ref: > dim_job.job_key]
  skill_key integer [ref: > dim_skill.skill_key]
  importance_weight integer [note: '1-5 importance']
}

table fact_user_skill_competency [note: 'fact: gnn graph edge. user->skill.'] {
  user_key integer [ref: > dim_user.user_key]
  skill_key integer [ref: > dim_skill.skill_key]
  proficiency_level integer [note: '1-5 proficiency']
}


// ==========================================================
// zone 3: data mart (gold layer)
// ==========================================================

tablegroup "dw_mart" {
  mart_job_performance_daily
}

table mart_job_performance_daily [note: 'mart: aggregated specifically for executive dashboard. grain: job + day.'] {
  job_day_sk integer [primary key, note: 'hash of date + jobid']

  // dimensions (foreign keys)
  datekey integer [ref: > dim_date.date_key, note: 'matches csv datekey']
  job_id integer [ref: > dim_job.job_key, note: 'matches csv job_id']

  // engagement metrics
  view_count integer [note: 'count of fact_engagement rows']
  userview_count integer [note: 'count of distinct user keys']
  total_view_duration_sec integer [note: 'sum of time_on_page_seconds']
  max_view_duration_sec integer [note: 'max time by single user']
  avg_user_max_view_duration_sec float [note: 'avg of user max times']

  // funnel start metrics
  start_step1_count integer [note: 'count of step 1 starts']
  start_step2_count integer [note: 'count of step 2 starts']
  start_step3_count integer [note: 'count of step 3 starts']
  completed_apply_count integer [note: 'count of completed apps']

  // drop-off metrics (calculated)
  drop_step1_count integer [note: 'step 1 - step 2']
  drop_step2_count integer [note: 'step 2 - step 3']
  drop_step3_count integer [note: 'step 3 - completed']

  // reason engine metrics
  bounce_count integer [note: 'sessions < 10s']
  deepread_noapply_count integer [note: 'kpi: scroll > 70% and no apply']

  // audit
  created_at timestamp
  updated_at timestamp
}

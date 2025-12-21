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
  user_id varchar [primary key, note: 'primary key - format: USR000001 (USR + 6 digits)']
  email varchar [unique]
  password_hash varchar
  is_active boolean [note: 'status flag']
  created_at timestamp
  updated_at timestamp
  _ingestion_time timestamp [note: 'when this record landed in the data lake/warehouse']
}

table profiles [note: 'oltp: extended user details. source for dim_user features.'] {
  profile_id varchar [primary key, note: 'primary key - format: PRF000001 (PRF + 6 digits)']
  user_id varchar [ref: - users.user_id, note: '1:1 relationship with users']
  headline varchar [note: 'short bio']
  summary text [note: 'full bio']
  current_salary decimal [note: 'used for salary matching features']
  willing_to_relocate boolean [note: 'critical gnn feature']
  created_at timestamp
  updated_at timestamp
  _ingestion_time timestamp [note: 'when this record landed in the data lake/warehouse']
}

table education [note: 'oltp: education history. source for dim_user.highest_degree.'] {
  education_id varchar [primary key, note: 'primary key - format: EDU000001 (EDU + 6 digits)']
  user_id varchar [ref: > users.user_id]
  degree varchar [note: 'bs, ms, phd']
  school_name varchar
  created_at timestamp
  updated_at timestamp
  _ingestion_time timestamp [note: 'when this record landed in the data lake/warehouse']
}

table work_history [note: 'oltp: work experience. source for dim_user.years_experience.'] {
  work_id varchar [primary key, note: 'primary key - format: WRK000001 (WRK + 6 digits)']
  user_id varchar [ref: > users.user_id]
  company_name varchar
  title varchar
  start_date date
  end_date date
  created_at timestamp
  updated_at timestamp
  _ingestion_time timestamp [note: 'when this record landed in the data lake/warehouse']
}

table companies [note: 'oltp: company registry. source for dim_job.company_name.'] {
  company_id varchar [primary key, note: 'primary key - format: COM000001 (COM + 6 digits)']
  name varchar
  industry varchar [note: 'used for industry matching']
  size_range varchar
  created_at timestamp
  updated_at timestamp
  _ingestion_time timestamp [note: 'when this record landed in the data lake/warehouse']
}

table job_postings [note: 'oltp: job catalog. source for dim_job.'] {
  job_id varchar [primary key, note: 'primary key - format: JOB000001 (JOB + 6 digits)']
  company_id varchar [ref: > companies.company_id]
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
  skill_id varchar [primary key, note: 'primary key - format: SKL000001 (SKL + 6 digits)']
  name varchar
  category varchar [note: 'tech, soft skill, language']
  created_at timestamp
  updated_at timestamp
  _ingestion_time timestamp [note: 'when this record landed in the data lake/warehouse']
}

table job_skills [note: 'oltp: many-to-many link for job requirements.'] {
  job_id varchar [ref: > job_postings.job_id, note: 'composite pk - format: JOB000001']
  skill_id varchar [ref: > skills.skill_id, note: 'composite pk - format: SKL000001']
  importance integer [note: '1-5 scale. source for fact_job_skill_demand.']
  created_at timestamp
  updated_at timestamp
  _ingestion_time timestamp [note: 'when this record landed in the data lake/warehouse']
  indexes { (job_id, skill_id) [pk] }
}

table user_skills [note: 'oltp: many-to-many link for user competencies.'] {
  user_id varchar [ref: > users.user_id, note: 'composite pk - format: USR000001']
  skill_id varchar [ref: > skills.skill_id, note: 'composite pk - format: SKL000001']
  proficiency integer [note: '1-5 scale. source for fact_user_skill_competency.']
  created_at timestamp
  updated_at timestamp
  _ingestion_time timestamp [note: 'when this record landed in the data lake/warehouse']
  indexes { (user_id, skill_id) [pk] }
}

table applications [note: 'oltp: application state. source for fact_application_flow.'] {
  application_id varchar [primary key, note: 'primary key - format: APP000001 (APP + 6 digits)']
  user_id varchar [ref: > users.user_id]
  job_id varchar [ref: > job_postings.job_id]
  current_status varchar [note: 'applied, rejected, offer']
  applied_at timestamp
  created_at timestamp
  updated_at timestamp
  _ingestion_time timestamp [note: 'when this record landed in the data lake/warehouse']
}

// raw streams (json payloads)

table streaming_job_activity [note: 'Bronze Layer: Raw immutable event stream from frontend applications. Unified stream for both content engagement and application funnel tracking.'] {

  // Primary Identifiers
  event_id string [primary key, note: 'UUID for the specific event']
  session_id string [note: 'Browser session ID - used for content engagement sessionization']
  user_cookie_id string [note: 'Hashed identifier for the user/browser (anonymous tracking)']
  user_id string [note: 'Authenticated user ID - nullable for anonymous browsing, required for application events']
  job_id string [note: 'The specific Job Listing ID being interacted with']

  // Temporal Data
  event_timestamp timestamp [note: 'UTC timestamp of when the event occurred']

  // Event Classification
  // Single table with event_type discriminator for both engagement and application funnel events
  event_type string [note: '''
    Event Taxonomy:

    Content Engagement Events:
    - VIEW: Job posting page loaded
    - SCROLL: User scrolled on job detail page

    Application Funnel Events:
    - APPLY_BUTTON_CLICK: User clicks "Apply Now" button (funnel entry point)
    - APPLICATION_FORM_INTERACTION: Step-level tracking (start/complete/abandon)
    - APPLICATION_SUBMISSION: Final application submit (funnel completion)
  ''']

  // The Flexible Payload (The "What")
  // JSON schema varies by event_type to avoid sparse columns
  event_properties json [note: '''
    Dynamic payload based on event_type. Schema examples:

    Content Engagement:
    - VIEW: { "load_time_ms": 200, "duration_sec": 30, "referrer": "google" }
    - SCROLL: { "scroll_depth_percent": 75, "max_scroll_px": 1200 }

    Application Funnel:
    - APPLY_BUTTON_CLICK: {
        "button_location": "job_detail_header",
        "is_authenticated": true,
        "application_session_id": "sess-uuid-123"
      }

    - APPLICATION_FORM_INTERACTION: {
        "application_session_id": "sess-uuid-123",
        "current_step": 1,
        "step_status": "start"
      }
      Note: step_status values: ["start", "complete", "abandon"]
      Note: current_step values: [1, 2, 3]

    - APPLICATION_SUBMISSION: {
        "application_session_id": "sess-uuid-123",
        "application_id": "APP000001",
        "is_complete": true,
        "total_steps_completed": 3,
        "total_form_time_sec": 120
      }

    Key Design Notes:
    - application_session_id: Unique per application attempt (one browser session may have multiple application attempts)
    - application_id: Links to OLTP applications table, only present in APPLICATION_SUBMISSION
    - step_status tracks granular user behavior: start (landed), complete (progressed), abandon (detected timeout/exit)
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

  // measurement
  view_count integer [note: 'sum of view']
  avg_time_on_page_seconds integer
  max_time_on_page_seconds integer [note: 'derived from streaming_events_view.duration_ms']
  max_scroll_depth_percent integer [note: 'derived from streaming_events_view.max_scroll_percent']
  bounce_no_apply_count boolean [note: 'logic: duration < 30s and no apply button click']
  deep_read_no_apply_count integer [note: 'logic: duration > 240s and max_scroll_depth_percent > 70% and no apply button click']
}


table fact_application_flow [note: 'fact: "funnel engine". stitches intent (stream) with reality (oltp).'] {
  flow_id varchar [primary key]
  job_key integer [ref: > dim_job.job_key]
  user_key integer [ref: > dim_user.user_key]
  date_key integer [ref: > dim_date.date_key]

  // measurement
  max_step_reached integer [note: 'last completed step (1, 2, or 3)']
  is_completed boolean [note: 'true if status = applied']

  // step measurement
  start_step1_count integer
  start_step2_count integer
  start_step3_count integer
  drop_step1_count integer
  drop_step2_count integer
  drop_step3_count integer
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

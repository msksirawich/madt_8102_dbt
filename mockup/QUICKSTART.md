# Quick Start Guide

## Generate Mock OLTP Data in 3 Steps

### 1️⃣ Install Dependencies

```bash
cd mockup/
pip install -r requirements.txt
```

### 2️⃣ Generate All Tables

```bash
python mockup_generator.py --all
```

This generates 10 OLTP tables in the correct dependency order:
- ✅ users (1,000 records)
- ✅ companies (200 records)
- ✅ skills (100 records)
- ✅ profiles (~900 records)
- ✅ education (~1,500 records)
- ✅ work_history (~2,500 records)
- ✅ job_postings (500 records)
- ✅ job_skills (~3,000 records)
- ✅ user_skills (~8,000 records)
- ✅ applications (~3,000 records)

**Total: ~20,000 records in ~30 seconds**

### 3️⃣ Verify Output

```bash
ls -lh output/
```

Expected files:
```
output/
├── users.csv
├── companies.csv
├── skills.csv
├── profiles.csv
├── education.csv
├── work_history.csv
├── job_postings.csv
├── job_skills.csv
├── user_skills.csv
└── applications.csv
```

## What's Generated?

### Core Tables

**users.csv** - 1,000 users
- user_id, email, password_hash, is_active, created_at, updated_at
- 95% active users
- Realistic email addresses

**companies.csv** - 200 companies
- company_id, name, industry, size_range, created_at, updated_at
- Various industries (Technology, Finance, Healthcare, etc.)
- Realistic company sizes

**skills.csv** - 100 skills
- skill_id, name, category, created_at, updated_at
- Tech skills (Python, Java, AWS, etc.)
- Soft skills (Communication, Leadership, etc.)
- Languages (English, Spanish, etc.)

### User Details

**profiles.csv** - ~900 profiles (90% of users)
- profile_id, user_id, headline, summary, current_salary, willing_to_relocate
- Realistic job titles
- Salary range: $40K-$250K
- 30% willing to relocate

**education.csv** - ~1,500 records (1-3 per user)
- education_id, user_id, degree, school_name
- Degrees: BS, MS, PhD, MBA
- Top universities

**work_history.csv** - ~2,500 records (0-5 per user)
- work_id, user_id, company_name, title, start_date, end_date
- Chronological work experience
- 20% current jobs (end_date = null)

### Job Market

**job_postings.csv** - 500 jobs
- job_id, company_id, title, description_html, min_salary, max_salary, status
- 70% open, 20% closed, 10% draft
- Various seniority levels
- Salary ranges

**job_skills.csv** - ~3,000 records (3-10 per job)
- job_id, skill_id, importance (1-5)
- Required skills per job
- Importance weights

### User-Job Interactions

**user_skills.csv** - ~8,000 records (5-15 per user)
- user_id, skill_id, proficiency (1-5)
- User competencies
- Proficiency levels

**applications.csv** - ~3,000 records (2-10 per active user)
- application_id, user_id, job_id, current_status, applied_at
- Application statuses: applied, screening, interview, offer, rejected, withdrawn
- Realistic conversion funnel
- Only active users apply to open jobs

## Common Commands

### Generate Specific Tables

```bash
# Generate only core tables
python mockup_generator.py --tables users,companies,skills

# Generate user-related tables
python mockup_generator.py --tables users,profiles,education,work_history

# Generate single table
python mockup_generator.py --config config/users_config.yaml
```

### Customize Output

```bash
# Save to different directory
python mockup_generator.py --all --output data/mockup/

# Use different config directory
python mockup_generator.py --all --config-dir my_configs/
```

### Regenerate Data

```bash
# Remove old data
rm -rf output/

# Generate fresh dataset
python mockup_generator.py --all
```

## Customization

### Change Record Counts

Edit `num_records` in config files:

```yaml
# config/users_config.yaml
num_records: 5000  # Change from 1000 to 5000
```

### Adjust Distributions

Edit `weights` for realistic distributions:

```yaml
# config/education_config.yaml
degree:
  type: faker
  provider: random_element
  elements:
    - BS
    - MS
    - PhD
    - MBA
  weights: [50, 30, 10, 10]  # More BS degrees
```

### Modify Date Ranges

```yaml
# config/users_config.yaml
created_at:
  type: faker
  provider: date_time_between
  start_date: "-5y"  # 5 years instead of 3
  end_date: "now"
```

## Next Steps

### 1. Validate Generated Data

```bash
# Check record counts
wc -l output/*.csv

# Preview data
head output/users.csv
head output/job_postings.csv
```

### 2. Load to Database

```bash
# PostgreSQL example
psql -d your_db -c "COPY users FROM '/path/to/output/users.csv' CSV HEADER;"

# BigQuery example (via ingestion pipeline)
cp output/*.csv ../ingestion/data/
cd ../ingestion/
python main.py --config config/csv_users_pipeline_config.yaml --execution-date 2024-12-01
```

### 3. Explore Data

```python
import pandas as pd

# Load data
users = pd.read_csv('output/users.csv')
jobs = pd.read_csv('output/job_postings.csv')
applications = pd.read_csv('output/applications.csv')

# Check distributions
print(users['is_active'].value_counts())
print(jobs['status'].value_counts())
print(applications['current_status'].value_counts())

# Validate relationships
user_ids = users['user_id'].tolist()
profile_user_ids = profiles['user_id'].tolist()
print(f"All profile user_ids in users: {all(uid in user_ids for uid in profile_user_ids)}")
```

## Troubleshooting

### "Cannot generate foreign key: table data not found"

**Solution**: Generate parent tables first
```bash
python mockup_generator.py --tables users  # Parent first
python mockup_generator.py --tables profiles  # Child second
```

Or use `--all` to generate in correct order.

### "ModuleNotFoundError: No module named 'faker'"

**Solution**: Install dependencies
```bash
pip install -r requirements.txt
```

### Generated data has incorrect relationships

**Solution**: Delete output and regenerate all tables in order
```bash
rm -rf output/
python mockup_generator.py --all
```

## Tips

1. **Start with small numbers** (100 records) to test quickly
2. **Use --all** to ensure correct dependency order
3. **Version control configs** for reproducibility
4. **Check logs** for detailed generation info
5. **Validate foreign keys** before loading to database

## Performance

| Records | Time |
|---------|------|
| 1K users + all tables | ~5 sec |
| 10K users + all tables | ~30 sec |
| 100K users + all tables | ~5 min |

---

**Ready to go!** See `README.md` for detailed documentation.

# OLTP Data Mockup Generator

A configuration-driven mockup data generator for OLTP tables using Faker. Generates realistic test data based on YAML configuration files with support for foreign keys, constraints, and complex relationships.

## Features

✅ **Configuration-Driven**: Define data generation rules in YAML files
✅ **Realistic Data**: Uses Faker library for realistic names, emails, dates, etc.
✅ **Foreign Key Support**: Automatic handling of table relationships
✅ **Constraint Handling**: Unique constraints, value ranges, distributions
✅ **Dependency Management**: Generates tables in correct order
✅ **Multiple Output Formats**: CSV and Parquet support
✅ **Reproducible**: Seeded random generation for consistent results

## Project Structure

```
mockup/
├── config/
│   ├── users_config.yaml              # Core user identity
│   ├── profiles_config.yaml           # User profiles (1:1)
│   ├── education_config.yaml          # User education history
│   ├── work_history_config.yaml       # Work experience
│   ├── companies_config.yaml          # Company registry
│   ├── job_postings_config.yaml       # Job catalog
│   ├── skills_config.yaml             # Skills master list
│   ├── job_skills_config.yaml         # Job skill requirements
│   ├── user_skills_config.yaml        # User skill competencies
│   └── applications_config.yaml       # Job applications
├── output/
│   └── (generated CSV files)
├── mockup_generator.py                # Main generator script
├── requirements.txt
└── README.md
```

## Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Generate Data

**Generate all tables** (in dependency order):
```bash
python mockup_generator.py --all
```

**Generate specific tables**:
```bash
python mockup_generator.py --tables users,companies,skills
```

**Generate single table**:
```bash
python mockup_generator.py --config config/users_config.yaml
```

### 3. Check Output

Generated data will be in the `output/` directory:
```bash
ls -lh output/
# users.csv, companies.csv, skills.csv, etc.
```

## Configuration Format

Each table has a YAML configuration file defining generation rules.

### Example: users_config.yaml

```yaml
table_name: users
description: "Core user identity table"

num_records: 1000

output:
  file_format: csv
  file_name: users.csv

columns:
  user_id:
    type: sequence
    start: 1
    description: "Primary key, auto-increment"

  email:
    type: faker
    provider: email
    unique: true
    description: "User email address (unique)"

  password_hash:
    type: faker
    provider: sha256
    description: "Hashed password"

  is_active:
    type: faker
    provider: boolean
    probability: 0.95
    description: "User active status (95% active)"

  created_at:
    type: faker
    provider: date_time_between
    start_date: "-3y"
    end_date: "now"
    description: "Account creation timestamp"

  updated_at:
    type: derived
    source: created_at
    offset_days:
      min: 0
      max: 365
    description: "Last update timestamp"

dependencies: []
```

## Column Types

### 1. **Sequence** (Auto-increment)
```yaml
user_id:
  type: sequence
  start: 1
```

### 2. **Faker** (Faker providers)
```yaml
email:
  type: faker
  provider: email
  unique: true  # Optional

name:
  type: faker
  provider: company

salary:
  type: faker
  provider: random_int
  min: 50000
  max: 200000

is_active:
  type: faker
  provider: boolean
  probability: 0.9

description:
  type: faker
  provider: text
  max_nb_chars: 500

created_at:
  type: faker
  provider: date_time_between
  start_date: "-3y"
  end_date: "now"
```

### 3. **Random Element** (Weighted choices)
```yaml
degree:
  type: faker
  provider: random_element
  elements:
    - BS
    - MS
    - PhD
    - MBA
  weights: [40, 30, 20, 10]  # Optional weights
```

### 4. **Foreign Key** (Reference other tables)
```yaml
user_id:
  type: foreign_key
  reference_table: users
  reference_column: user_id
  unique: true  # Optional: for 1:1 relationships
```

### 5. **Derived** (Based on other columns)
```yaml
# Derived from date column
updated_at:
  type: derived
  source: created_at
  offset_days:
    min: 0
    max: 30

# Derived from numeric column
max_salary:
  type: derived
  source: min_salary
  offset:
    min: 20000
    max: 50000
```

### 6. **Predefined List** (Fixed catalog)
```yaml
name:
  type: predefined_list
  values:
    - "Python"
    - "Java"
    - "JavaScript"
  unique: true
```

### 7. **Conditional** (Based on conditions)
```yaml
published_at:
  type: conditional
  condition: status
  if_equals:
    open:
      type: faker
      provider: date_time_between
      start_date: "-6m"
      end_date: "now"
    draft: null
```

### 8. **Same As** (Copy from another column)
```yaml
created_at:
  type: same_as
  source: applied_at
```

## Generation Order (Dependency Chain)

Tables are generated in this order to respect foreign key dependencies:

1. **users** (no dependencies)
2. **companies** (no dependencies)
3. **skills** (no dependencies)
4. **profiles** (depends on users)
5. **education** (depends on users)
6. **work_history** (depends on users)
7. **job_postings** (depends on companies)
8. **job_skills** (depends on job_postings, skills)
9. **user_skills** (depends on users, skills)
10. **applications** (depends on users, job_postings)

## Data Volume

Default configuration generates:

| Table | Records | Relationships |
|-------|---------|---------------|
| users | 1,000 | - |
| companies | 200 | - |
| skills | 100 | - |
| profiles | ~900 | 90% of users |
| education | ~1,500 | 1-3 per user |
| work_history | ~2,500 | 0-5 per user |
| job_postings | 500 | Per company |
| job_skills | ~3,000 | 3-10 per job |
| user_skills | ~8,000 | 5-15 per user |
| applications | ~3,000 | 2-10 per user |

**Total**: ~20,000 records

## Customization

### Adjust Record Counts

Edit the `num_records` in each config file:

```yaml
# users_config.yaml
num_records: 5000  # Change from 1000 to 5000
```

### Adjust Distributions

Modify weights in `random_element`:

```yaml
status:
  type: faker
  provider: random_element
  elements:
    - "open"
    - "closed"
    - "draft"
  weights: [80, 15, 5]  # 80% open, 15% closed, 5% draft
```

### Adjust Date Ranges

Change date parameters:

```yaml
created_at:
  type: faker
  provider: date_time_between
  start_date: "-5y"  # 5 years ago
  end_date: "now"
```

### Add Custom Faker Providers

The script supports all Faker providers. See [Faker documentation](https://faker.readthedocs.io/) for available providers.

## Advanced Features

### 1. Unique Constraints

Ensure uniqueness for a column:

```yaml
email:
  type: faker
  provider: email
  unique: true
```

### 2. Conditional Generation

Generate different values based on conditions:

```yaml
end_date:
  type: conditional
  condition: is_current_job
  if_true: null
  if_false:
    type: faker
    provider: date_between
    start_date: start_date
    end_date: "now"
  probability_current: 0.2
```

### 3. Records Per Parent

For many-to-one relationships:

```yaml
# education_config.yaml
records_per_user:
  min: 1
  max: 3
  avg: 1.5
```

### 4. Ratio-Based Generation

Generate records as percentage of parent:

```yaml
# profiles_config.yaml
num_records_ratio: 0.9  # 90% of users have profiles
```

## Output Formats

### CSV (Default)
```yaml
output:
  file_format: csv
  file_name: users.csv
```

### Parquet
```yaml
output:
  file_format: parquet
  file_name: users.parquet
```

## Usage Examples

### Generate Fresh Dataset

```bash
# Remove old data
rm -rf output/

# Generate all tables
python mockup_generator.py --all
```

### Generate Only Core Tables

```bash
python mockup_generator.py --tables users,companies,skills
```

### Generate with Custom Output Directory

```bash
python mockup_generator.py --all --output data/mockup/
```

### Generate Single Table for Testing

```bash
python mockup_generator.py --config config/users_config.yaml
```

## Data Quality

The generator ensures:

✅ **Referential Integrity**: Foreign keys reference existing records
✅ **Unique Constraints**: No duplicates for unique columns
✅ **Data Types**: Correct types (int, string, date, boolean)
✅ **Value Ranges**: Min/max constraints enforced
✅ **Distributions**: Weighted random for realistic data
✅ **Chronological Order**: Dates follow logical order (created_at < updated_at)
✅ **Business Rules**: Custom logic (e.g., 95% users active)

## Integration with Ingestion Pipeline

Generated CSV files can be used with the ingestion pipeline:

```bash
# Generate mockup data
cd mockup/
python mockup_generator.py --all

# Copy to ingestion data folder
cp output/*.csv ../ingestion/data/

# Run ingestion
cd ../ingestion/
python main.py --config config/csv_users_pipeline_config.yaml --execution-date 2024-12-01
```

## Troubleshooting

### Foreign Key Errors

**Problem**: "Cannot generate foreign key: table data not found"

**Solution**: Generate parent table first
```bash
python mockup_generator.py --tables users
python mockup_generator.py --tables profiles
```

### Not Enough Unique Values

**Problem**: "Not enough unique values in reference table"

**Solution**: Increase parent table records or reduce child table records

### Missing Faker Provider

**Problem**: "Unknown Faker provider"

**Solution**: Check [Faker docs](https://faker.readthedocs.io/) for correct provider name

## Extension

### Add New Table

1. Create YAML config in `config/`
2. Define columns and relationships
3. Add to generation order in `mockup_generator.py`
4. Generate

### Add Custom Logic

Extend `MockDataGenerator` class:

```python
def post_process_custom(self, df: pd.DataFrame) -> pd.DataFrame:
    # Custom processing logic
    return df
```

## Best Practices

1. **Start Small**: Test with 100 records before generating thousands
2. **Check Dependencies**: Ensure parent tables exist before children
3. **Use Weights**: Realistic distributions improve data quality
4. **Version Control**: Keep configs in git for reproducibility
5. **Validate Output**: Check CSVs before loading to database

## Performance

- **1,000 users**: ~2 seconds
- **10,000 users + all tables**: ~30 seconds
- **100,000 users + all tables**: ~5 minutes

## License

MIT License

## Support

For issues or questions:
1. Check config YAML syntax
2. Verify dependencies are installed
3. Review logs for detailed error messages
4. Check that parent tables exist for foreign keys

---

**Ready to generate realistic test data!** 🚀

# GNN Data for Job Recommendation System

This directory contains mock data for training a Graph Neural Network (GNN) based job recommendation system.

## Graph Structure

The GNN uses a **heterogeneous bipartite graph** with three node types and multiple edge types:

### Node Types
1. **Users** (`dim_user.csv`) - 50 users with varied roles, experience, and education
2. **Jobs** (`dim_job.csv`) - 50 job postings across different industries and companies
3. **Skills** (`dim_skill.csv`) - 50 skills (technical, soft skills, languages)

### Edge Types (Relationships)

#### 1. User → Skill Edges
**File**: `fact_user_skill_competency.csv`
- **Description**: Skills possessed by users
- **Weight**: `proficiency_level` (1-5 scale)
- **Use in GNN**: User node features, skill-based similarity

#### 2. Job → Skill Edges
**File**: `fact_job_skill_demand.csv`
- **Description**: Skills required by jobs
- **Weight**: `importance_weight` (1-5 scale)
- **Use in GNN**: Job node features, requirement matching

#### 3. User → Job Edges
**File**: `fact_unified_activity.csv`
- **Description**: User interactions with jobs (impressions, clicks, views, applications)
- **Weight**: `interaction_weight` (0.1-1.0)
  - impression: 0.1-0.2
  - click: 0.3-0.5
  - view: 0.6-0.8
  - apply: 1.0
- **Use in GNN**: Training labels, collaborative filtering signals

## Supporting Tables

### Temporal Dimensions
- `dim_date.csv` - Date dimension for temporal features
- `dim_time.csv` - Time-of-day buckets for behavior patterns

### Engagement Metrics
- `fact_job_content_engagement.csv` - Detailed session metrics (scroll depth, time on page, bounce rate)
- `fact_application_flow.csv` - Application funnel tracking (conversion rates, drop-off points)

## Data Statistics

- **Users**: 50
- **Jobs**: 50
- **Skills**: 50
- **User-Skill edges**: ~250 (avg 5 skills per user)
- **Job-Skill edges**: ~300 (avg 6 skills per job)
- **User-Job interactions**: 180 activities
  - Impressions: ~50
  - Clicks: ~50
  - Views: ~33
  - Applications: 7 (conversion rate: ~3.9%)

## GNN Model Input Format

### Node Features
```python
# User nodes
user_features = {
    'years_experience': continuous,
    'highest_degree': categorical,
    'skill_embedding': aggregated from user_skill edges
}

# Job nodes
job_features = {
    'company_industry': categorical,
    'description_length_words': continuous,
    'skill_embedding': aggregated from job_skill edges
}

# Skill nodes
skill_features = {
    'category': categorical (tech, soft_skill, language)
}
```

### Edge Features
```python
# User-Skill edges
{
    'proficiency_level': continuous (1-5)
}

# Job-Skill edges
{
    'importance_weight': continuous (1-5)
}

# User-Job edges (interaction)
{
    'interaction_weight': continuous (0-1),
    'activity_type': categorical,
    'temporal_features': [hour_of_day, day_of_week]
}
```

## Recommended GNN Architectures

1. **GraphSAGE** - For inductive learning (new users/jobs)
2. **GAT (Graph Attention Networks)** - For interpretable skill matching
3. **LightGCN** - For collaborative filtering with implicit feedback
4. **Heterogeneous GNN (HetGNN)** - For multi-type nodes and edges

## Training Task

**Link Prediction**: Predict probability of user applying to a job

**Positive samples**: user-job pairs with `activity_type='apply'` (7 samples)
**Negative samples**: unobserved user-job pairs (random sampling)

**Train/Val/Test split recommendation**:
- Train: 70% of interactions (before 2024-12-12)
- Validation: 15% of interactions (2024-12-12 to 2024-12-13)
- Test: 15% of interactions (2024-12-14)

## Usage Example

```python
import pandas as pd
import torch
from torch_geometric.data import HeteroData

# Load data
users = pd.read_csv('dim_user.csv')
jobs = pd.read_csv('dim_job.csv')
skills = pd.read_csv('dim_skill.csv')
user_skills = pd.read_csv('fact_user_skill_competency.csv')
job_skills = pd.read_csv('fact_job_skill_demand.csv')
interactions = pd.read_csv('fact_unified_activity.csv')

# Build heterogeneous graph
data = HeteroData()

# Add node features
data['user'].x = ... # user features
data['job'].x = ... # job features
data['skill'].x = ... # skill features

# Add edges
data['user', 'has_skill', 'skill'].edge_index = ...
data['job', 'requires_skill', 'skill'].edge_index = ...
data['user', 'interacts', 'job'].edge_index = ...
data['user', 'interacts', 'job'].edge_attr = ... # interaction weights

# Train GNN model
model = HeteroGNN(...)
```

## Next Steps

1. **Feature Engineering**:
   - Aggregate skill embeddings for users/jobs
   - Add temporal decay for old interactions
   - Calculate skill gap features (job skills - user skills)

2. **Graph Construction**:
   - Add user-user similarity edges (collaborative filtering)
   - Add job-job similarity edges (content-based)
   - Add skill-skill co-occurrence edges

3. **Model Training**:
   - Implement negative sampling strategy
   - Add ranking loss (BPR, InfoNCE)
   - Implement cold-start handling for new users/jobs

4. **Evaluation Metrics**:
   - Hit Rate@K
   - NDCG@K
   - MRR (Mean Reciprocal Rank)
   - Coverage and Diversity

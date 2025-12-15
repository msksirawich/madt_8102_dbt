# GNN Data Directory - Complete Index

## 📂 Directory Structure

```
gnn/data/
├── README.md                           # Original data documentation
├── GNN_READY_README.md                 # ⭐ START HERE - GNN-ready data guide
├── INDEX.md                            # This file - complete index
│
├── graph_metadata.json                 # Graph schema and statistics
├── gnn_loader.py                       # Python data loader (PyTorch/DGL)
│
├── [UPSTREAM TABLES - Raw ETL Output]
│   ├── dim_user.csv                    # 50 users with profiles
│   ├── dim_job.csv                     # 50 job postings
│   ├── dim_skill.csv                   # 50 skills catalog
│   ├── dim_date.csv                    # Date dimension
│   ├── dim_time.csv                    # Time dimension
│   ├── fact_user_skill_competency.csv  # User skill proficiency
│   ├── fact_job_skill_demand.csv       # Job skill requirements
│   ├── fact_unified_activity.csv       # User-job interactions
│   ├── fact_job_content_engagement.csv # Engagement metrics
│   └── fact_application_flow.csv       # Application funnel
│
└── [GNN-READY TABLES - Processed]
    ├── nodes_user.csv                  # User nodes (50×11 features)
    ├── nodes_job.csv                   # Job nodes (50×10 features)
    ├── nodes_skill.csv                 # Skill nodes (50×7 features)
    ├── edges_user_skill.csv            # User→Skill edges (250)
    ├── edges_job_skill.csv             # Job→Skill edges (300)
    └── edges_user_job.csv              # User→Job edges (180) ⭐ LABELS
```

## 🎯 Quick Navigation

### For Data Scientists / ML Engineers
👉 **Start here**: `GNN_READY_README.md`
- Complete guide to GNN-ready data
- Quick start examples (PyTorch Geometric & DGL)
- Feature schemas and preprocessing details
- Training strategies and recommended models

### For Data Engineers / DBT Developers
👉 **Start here**: `README.md`
- Original data model documentation
- ETL pipeline details
- Graph structure explanation
- Raw table schemas

### For Python Developers
👉 **Start here**: `gnn_loader.py`
- Ready-to-use data loader
- Supports PyTorch Geometric and DGL
- Train/val/test split utilities
- Run `python gnn_loader.py` to test

### For Architects / Researchers
👉 **Start here**: `graph_metadata.json`
- Complete graph schema
- Node and edge type definitions
- Statistics and metrics
- Recommended GNN architectures

## 📊 Data Flow

```
┌─────────────────────────────────────────────────────────────┐
│  OLTP + Streaming Sources                                   │
│  (users, jobs, skills, applications, events)                │
└─────────────────────────┬───────────────────────────────────┘
                          │ ETL Pipeline
                          ▼
┌─────────────────────────────────────────────────────────────┐
│  UPSTREAM TABLES (Bronze/Silver Layer)                      │
│  ├── dim_user.csv                                           │
│  ├── dim_job.csv                                            │
│  ├── dim_skill.csv                                          │
│  ├── fact_user_skill_competency.csv                         │
│  ├── fact_job_skill_demand.csv                              │
│  └── fact_unified_activity.csv                              │
└─────────────────────────┬───────────────────────────────────┘
                          │ GNN Preprocessing
                          │ - Feature engineering
                          │ - Normalization
                          │ - One-hot encoding
                          ▼
┌─────────────────────────────────────────────────────────────┐
│  GNN-READY TABLES (Gold Layer)                              │
│  ├── nodes_user.csv       (50 nodes × 11 features)          │
│  ├── nodes_job.csv        (50 nodes × 10 features)          │
│  ├── nodes_skill.csv      (50 nodes × 7 features)           │
│  ├── edges_user_skill.csv (250 edges)                       │
│  ├── edges_job_skill.csv  (300 edges)                       │
│  └── edges_user_job.csv   (180 edges + labels)              │
└─────────────────────────┬───────────────────────────────────┘
                          │ GNN Training
                          ▼
┌─────────────────────────────────────────────────────────────┐
│  GNN Models                                                  │
│  ├── GraphSAGE (cold start)                                 │
│  ├── GAT (interpretability)                                 │
│  └── HeteroGNN (best accuracy)                              │
└─────────────────────────────────────────────────────────────┘
```

## 📋 File Purpose Matrix

| File | Type | Purpose | Used By |
|------|------|---------|---------|
| `dim_*.csv` | Raw Data | Data warehouse dimensions | DBT, Analytics |
| `fact_*.csv` | Raw Data | Data warehouse facts | DBT, Analytics |
| `nodes_*.csv` | GNN Data | Node features (preprocessed) | GNN Training |
| `edges_*.csv` | GNN Data | Graph edges (COO format) | GNN Training |
| `graph_metadata.json` | Metadata | Schema & statistics | All users |
| `gnn_loader.py` | Tool | Python data loader | Data Scientists |
| `README.md` | Docs | Original data guide | Data Engineers |
| `GNN_READY_README.md` | Docs | GNN training guide | ML Engineers |
| `INDEX.md` | Docs | This index | Everyone |

## 🔄 Data Lineage

### Upstream Tables → GNN Nodes

```
dim_user.csv
  ├─ Extract features: years_experience, highest_degree, current_role
  ├─ Normalize: years_experience → [0, 1]
  ├─ One-hot encode: degree, role
  ├─ Aggregate: num_skills, avg_proficiency from fact_user_skill_competency
  └─ Output: nodes_user.csv

dim_job.csv
  ├─ Extract features: company_industry, seniority, description_length
  ├─ Normalize: description_length → [0, 1]
  ├─ One-hot encode: industry, seniority
  ├─ Aggregate: num_required_skills, avg_importance from fact_job_skill_demand
  └─ Output: nodes_job.csv

dim_skill.csv
  ├─ Extract features: skill_name, category
  ├─ One-hot encode: category (tech, soft_skill, language)
  ├─ Aggregate: popularity_user, popularity_job, avg_proficiency, avg_importance
  └─ Output: nodes_skill.csv
```

### Upstream Tables → GNN Edges

```
fact_user_skill_competency.csv
  ├─ Map: (user_key, skill_key) → (src_node_id, dst_node_id)
  ├─ Normalize: proficiency_level → [0, 1]
  └─ Output: edges_user_skill.csv

fact_job_skill_demand.csv
  ├─ Map: (job_key, skill_key) → (src_node_id, dst_node_id)
  ├─ Normalize: importance_weight → [0, 1]
  └─ Output: edges_job_skill.csv

fact_unified_activity.csv
  ├─ Map: (user_key, job_key) → (src_node_id, dst_node_id)
  ├─ Weight: impression=0.1, click=0.4, view=0.7, apply=1.0
  ├─ Label: is_apply = 1 if activity_type='apply', else 0
  └─ Output: edges_user_job.csv ⭐ TRAINING LABELS
```

## 🎯 Use Cases

### 1. Train a GNN Model
**Files needed**: `nodes_*.csv`, `edges_*.csv`, `gnn_loader.py`
```python
from gnn_loader import load_pyg_hetero_graph
data = load_pyg_hetero_graph('gnn/data')
```

### 2. Analyze Raw Data
**Files needed**: `dim_*.csv`, `fact_*.csv`
```python
import pandas as pd
users = pd.read_csv('dim_user.csv')
interactions = pd.read_csv('fact_unified_activity.csv')
```

### 3. Understand Graph Structure
**Files needed**: `graph_metadata.json`
```python
import json
with open('graph_metadata.json') as f:
    metadata = json.load(f)
    print(metadata['graph_info'])
```

### 4. Build Custom Loader
**Files needed**: `graph_metadata.json`, `nodes_*.csv`, `edges_*.csv`
```python
import pandas as pd
# Read metadata for schema
# Load nodes and edges manually
# Build custom graph structure
```

## 📈 Key Statistics

| Metric | Value |
|--------|-------|
| **Total Nodes** | 150 |
| **Total Edges** | 730 |
| **Node Types** | 3 (user, job, skill) |
| **Edge Types** | 3 (user-skill, job-skill, user-job) |
| **Training Labels** | 180 (7 positive, 173 negative) |
| **Conversion Rate** | 3.9% |
| **Feature Dimensions** | User: 11, Job: 10, Skill: 7 |

## 🚀 Getting Started

1. **For GNN Training**: Read `GNN_READY_README.md`
2. **Run Test Loader**: `python gnn_loader.py`
3. **Check Metadata**: `cat graph_metadata.json | jq`
4. **Train Your Model**: See examples in `GNN_READY_README.md`

## 📞 Support

- **Schema Questions**: See `graph_metadata.json`
- **Training Questions**: See `GNN_READY_README.md`
- **Data Questions**: See `README.md`
- **Code Questions**: See `gnn_loader.py`

---

**Last Updated**: 2024-12-14
**Version**: 1.0
**Status**: Production Ready ✅

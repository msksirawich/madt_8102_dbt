# GNN-Ready Data for Job Recommendation

This directory contains **production-ready** data for Graph Neural Network (GNN) training. All features are preprocessed, normalized, and encoded for direct consumption by GNN frameworks.

## 📁 File Structure

### Node Tables (Processed Features)
- **`nodes_user.csv`** - User nodes with 11 features (50 nodes)
- **`nodes_job.csv`** - Job nodes with 10 features (50 nodes)
- **`nodes_skill.csv`** - Skill nodes with 7 features (50 nodes)

### Edge Tables (COO Format)
- **`edges_user_skill.csv`** - User→Skill edges with proficiency weights (250 edges)
- **`edges_job_skill.csv`** - Job→Skill edges with importance weights (300 edges)
- **`edges_user_job.csv`** - User→Job interaction edges with labels (180 edges)

### Metadata & Tools
- **`graph_metadata.json`** - Complete graph schema and statistics
- **`gnn_loader.py`** - Python loader for PyTorch Geometric and DGL
- **`README.md`** - Original data documentation
- **`GNN_READY_README.md`** - This file

## 🚀 Quick Start

### Option 1: PyTorch Geometric

```python
from gnn_loader import load_pyg_hetero_graph

# Load the graph
data = load_pyg_hetero_graph('gnn/data')

# Inspect the graph
print(f"Node types: {data.node_types}")
print(f"Edge types: {data.edge_types}")

# Access node features
user_features = data['user'].x  # Shape: [50, 11]
job_features = data['job'].x    # Shape: [50, 10]

# Access edges
user_job_edges = data['user', 'interacts', 'job'].edge_index
labels = data['user', 'interacts', 'job'].edge_label  # Binary: apply or not
```

### Option 2: DGL

```python
from gnn_loader import load_dgl_hetero_graph

# Load the graph
g = load_dgl_hetero_graph('gnn/data')

# Inspect the graph
print(f"Node types: {g.ntypes}")
print(f"Edge types: {g.etypes}")

# Access node features
user_features = g.nodes['user'].data['feat']  # Shape: [50, 11]

# Access edge features and labels
edge_weights = g.edges['interacts'].data['feat']
edge_labels = g.edges['interacts'].data['label']
```

### Option 3: Manual Loading

```python
import pandas as pd
import numpy as np

# Load node features
users = pd.read_csv('nodes_user.csv')
jobs = pd.read_csv('nodes_job.csv')
skills = pd.read_csv('nodes_skill.csv')

# Load edges
user_job_edges = pd.read_csv('edges_user_job.csv')

# Extract edge list (COO format)
src_nodes = user_job_edges['src_node_id'].values
dst_nodes = user_job_edges['dst_node_id'].values
labels = user_job_edges['is_apply'].values  # 1 = application, 0 = no application
```

## 📊 Graph Statistics

| Metric | Value |
|--------|-------|
| **Total Nodes** | 150 (50 users + 50 jobs + 50 skills) |
| **Total Edges** | 730 |
| **User→Skill Edges** | 250 |
| **Job→Skill Edges** | 300 |
| **User→Job Edges** | 180 |
| **Positive Labels** | 7 applications (3.9% conversion) |
| **Negative Labels** | 173 non-applications |

## 🎯 Prediction Task

**Task Type**: Link Prediction (Binary Classification)

**Goal**: Predict whether a user will **apply** to a job given:
- User features (experience, education, skills)
- Job features (industry, seniority, required skills)
- Skill graph structure (user skills ↔ job requirements)
- Historical interaction patterns

**Labels**:
- `is_apply = 1`: User applied to the job ✅
- `is_apply = 0`: User viewed but didn't apply ❌

## 📐 Feature Schemas

### User Node Features (11 features)

| Feature | Type | Range | Description |
|---------|------|-------|-------------|
| `years_experience_norm` | continuous | [0, 1] | Normalized years of experience |
| `degree_bs` | binary | {0, 1} | Has Bachelor's degree |
| `degree_ms` | binary | {0, 1} | Has Master's degree |
| `degree_phd` | binary | {0, 1} | Has PhD |
| `degree_mba` | binary | {0, 1} | Has MBA |
| `role_data` | binary | {0, 1} | Data-focused role |
| `role_engineer` | binary | {0, 1} | Engineering role |
| `role_manager` | binary | {0, 1} | Management role |
| `role_other` | binary | {0, 1} | Other role |
| `num_skills` | integer | [0, 10] | Number of skills possessed |
| `avg_proficiency` | continuous | [0, 5] | Average skill proficiency |

### Job Node Features (10 features)

| Feature | Type | Range | Description |
|---------|------|-------|-------------|
| `industry_technology` | binary | {0, 1} | Technology industry |
| `industry_finance` | binary | {0, 1} | Finance industry |
| `industry_healthcare` | binary | {0, 1} | Healthcare industry |
| `industry_other` | binary | {0, 1} | Other industry |
| `description_length_norm` | continuous | [0, 1] | Normalized job description length |
| `num_required_skills` | integer | [0, 10] | Number of required skills |
| `avg_skill_importance` | continuous | [0, 5] | Average skill importance |
| `seniority_junior` | binary | {0, 1} | Junior level |
| `seniority_mid` | binary | {0, 1} | Mid level |
| `seniority_senior` | binary | {0, 1} | Senior level |

### Skill Node Features (7 features)

| Feature | Type | Range | Description |
|---------|------|-------|-------------|
| `category_tech` | binary | {0, 1} | Technical skill |
| `category_soft_skill` | binary | {0, 1} | Soft skill |
| `category_language` | binary | {0, 1} | Language skill |
| `popularity_user` | integer | [0, 50] | Number of users with this skill |
| `popularity_job` | integer | [0, 50] | Number of jobs requiring this skill |
| `avg_proficiency` | continuous | [0, 5] | Average user proficiency |
| `avg_importance` | continuous | [0, 5] | Average job importance |

### Edge Features

**User→Skill Edges**:
- `proficiency_level`: {1, 2, 3, 4, 5}
- `proficiency_norm`: [0, 1] (normalized)

**Job→Skill Edges**:
- `importance_weight`: {1, 2, 3, 4, 5}
- `importance_norm`: [0, 1] (normalized)

**User→Job Edges**:
- `interaction_weight`: [0.1, 1.0] (impression=0.1, click=0.4, view=0.7, apply=1.0)
- `is_apply`: {0, 1} **← TARGET LABEL**
- `activity_type`: {impression, click, view, apply}
- `engagement_score`: [0, 1]
- `date_key`: YYYYMMDD format
- `time_key`: HHMMSS format

## 🔬 Recommended GNN Architectures

### 1. GraphSAGE (Recommended for Cold Start)
```python
from torch_geometric.nn import SAGEConv

class GraphSAGEModel(torch.nn.Module):
    def __init__(self, in_channels, hidden_channels):
        super().__init__()
        self.conv1 = SAGEConv(in_channels, hidden_channels)
        self.conv2 = SAGEConv(hidden_channels, hidden_channels)

    def forward(self, x, edge_index):
        x = self.conv1(x, edge_index).relu()
        x = self.conv2(x, edge_index)
        return x
```

**Pros**: Inductive learning, handles new users/jobs well
**Use Case**: Production systems with frequent new users

### 2. GAT - Graph Attention Network (Recommended for Interpretability)
```python
from torch_geometric.nn import GATConv

class GATModel(torch.nn.Module):
    def __init__(self, in_channels, hidden_channels, heads=4):
        super().__init__()
        self.conv1 = GATConv(in_channels, hidden_channels, heads=heads)
        self.conv2 = GATConv(hidden_channels * heads, hidden_channels, heads=1)

    def forward(self, x, edge_index):
        x = self.conv1(x, edge_index).relu()
        x = self.conv2(x, edge_index)
        return x
```

**Pros**: Attention weights show skill importance
**Use Case**: When you need to explain recommendations

### 3. Heterogeneous GNN (Best for This Graph)
```python
from torch_geometric.nn import HeteroConv, SAGEConv

class HeteroGNN(torch.nn.Module):
    def __init__(self, hidden_channels):
        super().__init__()
        self.conv1 = HeteroConv({
            ('user', 'has_skill', 'skill'): SAGEConv((-1, -1), hidden_channels),
            ('job', 'requires_skill', 'skill'): SAGEConv((-1, -1), hidden_channels),
            ('user', 'interacts', 'job'): SAGEConv((-1, -1), hidden_channels),
        })
```

**Pros**: Leverages different node/edge types
**Use Case**: Maximum accuracy for this heterogeneous graph

## 📈 Training Strategy

### Train/Val/Test Split

```python
from gnn_loader import get_train_val_test_split

split = get_train_val_test_split('gnn/data',
                                  train_ratio=0.7,
                                  val_ratio=0.15,
                                  seed=42)

# Temporal split based on date_key
train_edges = split['train']  # 70% earliest interactions
val_edges = split['val']      # 15% middle
test_edges = split['test']    # 15% most recent
```

### Negative Sampling

Since only 3.9% of interactions are applications, use negative sampling:

```python
# Ratio: 1 positive : 5 negatives (balanced training)
negative_sampling_ratio = 5

# Generate negative samples: random user-job pairs without edges
```

### Loss Function

```python
import torch.nn.functional as F

# Binary Cross-Entropy for imbalanced data
pos_weight = torch.tensor([negative_sampling_ratio])
criterion = torch.nn.BCEWithLogitsLoss(pos_weight=pos_weight)
```

### Evaluation Metrics

```python
# Ranking metrics
- Hit Rate @ K (HR@10): Did true job appear in top-10?
- NDCG @ K: Normalized Discounted Cumulative Gain
- MRR: Mean Reciprocal Rank

# Classification metrics
- AUC-ROC: Area under ROC curve
- Precision/Recall @ K
```

## 🔧 Data Preprocessing Applied

All preprocessing has been completed. No additional steps needed:

✅ **Normalization**: Years of experience, description length → [0, 1]
✅ **One-hot encoding**: Degree, role, industry, seniority, skill category
✅ **Edge weight normalization**: Proficiency and importance → [0, 1]
✅ **Interaction weight mapping**: impression(0.1), click(0.4), view(0.7), apply(1.0)
✅ **Node ID indexing**: Sequential IDs starting from 0

## 📦 Dependencies

```bash
# PyTorch Geometric
pip install torch torch-geometric

# Or DGL
pip install torch dgl

# Data processing
pip install pandas numpy
```

## 🎓 Example Training Script

```python
import torch
from gnn_loader import load_pyg_hetero_graph, get_train_val_test_split

# Load data
data = load_pyg_hetero_graph('gnn/data')
split = get_train_val_test_split('gnn/data')

# Define model
model = HeteroGNN(hidden_channels=64)
optimizer = torch.optim.Adam(model.parameters(), lr=0.001)

# Training loop
for epoch in range(100):
    model.train()
    optimizer.zero_grad()

    # Forward pass
    out = model(data.x_dict, data.edge_index_dict)

    # Compute loss on training edges
    loss = criterion(out, labels)

    loss.backward()
    optimizer.step()

    # Validation
    if epoch % 10 == 0:
        model.eval()
        with torch.no_grad():
            val_out = model(data.x_dict, data.edge_index_dict)
            val_metrics = evaluate(val_out, val_labels)
        print(f'Epoch {epoch}, Loss: {loss:.4f}, Val HR@10: {val_metrics["hr@10"]:.4f}')
```

## 📚 Additional Resources

- **Graph Metadata**: See `graph_metadata.json` for complete schema
- **Original Data**: See `README.md` for raw data documentation
- **PyTorch Geometric Docs**: https://pytorch-geometric.readthedocs.io/
- **DGL Docs**: https://docs.dgl.ai/

## 🤝 Support

For issues or questions:
1. Check `graph_metadata.json` for detailed schema
2. Run `python gnn_loader.py` to test data loading
3. Verify data integrity with provided statistics

---

**Ready to train!** 🚀 All data is preprocessed and ready for GNN frameworks.

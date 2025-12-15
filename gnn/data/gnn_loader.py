"""
GNN Data Loader for Job Recommendation Graph

This module provides utilities to load the job recommendation graph
into PyTorch Geometric or DGL format for GNN training.

Usage:
    # PyTorch Geometric
    from gnn_loader import load_pyg_hetero_graph
    data = load_pyg_hetero_graph('gnn/data')

    # DGL
    from gnn_loader import load_dgl_hetero_graph
    graph = load_dgl_hetero_graph('gnn/data')
"""

import os
import pandas as pd
import numpy as np
import json
from typing import Dict, Tuple, Optional


class GraphDataLoader:
    """Base class for loading graph data from CSV files"""

    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        self.metadata = self._load_metadata()

    def _load_metadata(self) -> Dict:
        """Load graph metadata from JSON file"""
        metadata_path = os.path.join(self.data_dir, 'graph_metadata.json')
        with open(metadata_path, 'r') as f:
            return json.load(f)

    def load_node_features(self, node_type: str) -> Tuple[np.ndarray, int]:
        """
        Load node features for a specific node type

        Args:
            node_type: Type of node ('user', 'job', 'skill')

        Returns:
            Tuple of (features array, num_nodes)
        """
        node_info = self.metadata['node_types'][node_type]
        file_path = os.path.join(self.data_dir, node_info['file'])
        df = pd.read_csv(file_path)

        feature_cols = node_info['feature_columns']
        features = df[feature_cols].values.astype(np.float32)
        num_nodes = node_info['count']

        return features, num_nodes

    def load_edges(self, edge_type: str) -> Tuple[np.ndarray, np.ndarray, Optional[np.ndarray]]:
        """
        Load edges for a specific edge type

        Args:
            edge_type: Type of edge ('user_skill', 'job_skill', 'user_job')

        Returns:
            Tuple of (source nodes, target nodes, edge features)
        """
        edge_info = self.metadata['edge_types'][edge_type]
        file_path = os.path.join(self.data_dir, edge_info['file'])
        df = pd.read_csv(file_path)

        src = df[edge_info['source_column']].values
        dst = df[edge_info['target_column']].values

        # Load edge features if available
        edge_features = None
        if 'edge_features' in edge_info and edge_info['edge_features']:
            edge_features = df[edge_info['edge_features']].values.astype(np.float32)

        return src, dst, edge_features


def load_pyg_hetero_graph(data_dir: str = '.'):
    """
    Load heterogeneous graph in PyTorch Geometric format

    Args:
        data_dir: Directory containing the graph data files

    Returns:
        HeteroData object for PyTorch Geometric
    """
    try:
        import torch
        from torch_geometric.data import HeteroData
    except ImportError:
        raise ImportError(
            "PyTorch Geometric is not installed. "
            "Install it with: pip install torch-geometric"
        )

    loader = GraphDataLoader(data_dir)
    data = HeteroData()

    # Load node features
    print("Loading node features...")
    for node_type in ['user', 'job', 'skill']:
        features, num_nodes = loader.load_node_features(node_type)
        data[node_type].x = torch.tensor(features, dtype=torch.float)
        data[node_type].num_nodes = num_nodes
        print(f"  {node_type}: {num_nodes} nodes, {features.shape[1]} features")

    # Load edges
    print("\nLoading edges...")

    # User -> Skill edges
    src, dst, edge_feat = loader.load_edges('user_skill')
    data['user', 'has_skill', 'skill'].edge_index = torch.tensor(
        np.vstack([src, dst]), dtype=torch.long
    )
    data['user', 'has_skill', 'skill'].edge_attr = torch.tensor(
        edge_feat, dtype=torch.float
    )
    # Reverse edges for undirected graph
    data['skill', 'rev_has_skill', 'user'].edge_index = torch.tensor(
        np.vstack([dst, src]), dtype=torch.long
    )
    data['skill', 'rev_has_skill', 'user'].edge_attr = torch.tensor(
        edge_feat, dtype=torch.float
    )
    print(f"  user -> skill: {len(src)} edges")

    # Job -> Skill edges
    src, dst, edge_feat = loader.load_edges('job_skill')
    data['job', 'requires_skill', 'skill'].edge_index = torch.tensor(
        np.vstack([src, dst]), dtype=torch.long
    )
    data['job', 'requires_skill', 'skill'].edge_attr = torch.tensor(
        edge_feat, dtype=torch.float
    )
    # Reverse edges
    data['skill', 'rev_requires_skill', 'job'].edge_index = torch.tensor(
        np.vstack([dst, src]), dtype=torch.long
    )
    data['skill', 'rev_requires_skill', 'job'].edge_attr = torch.tensor(
        edge_feat, dtype=torch.float
    )
    print(f"  job -> skill: {len(src)} edges")

    # User -> Job edges (main prediction target)
    src, dst, edge_feat = loader.load_edges('user_job')
    data['user', 'interacts', 'job'].edge_index = torch.tensor(
        np.vstack([src, dst]), dtype=torch.long
    )
    data['user', 'interacts', 'job'].edge_attr = torch.tensor(
        edge_feat, dtype=torch.float
    )
    # Get labels (is_apply column)
    edge_df = pd.read_csv(os.path.join(data_dir, 'edges_user_job.csv'))
    data['user', 'interacts', 'job'].edge_label = torch.tensor(
        edge_df['is_apply'].values, dtype=torch.long
    )
    print(f"  user -> job: {len(src)} edges")

    print("\nGraph loaded successfully!")
    print(f"Total nodes: {sum(data[nt].num_nodes for nt in ['user', 'job', 'skill'])}")
    print(f"Total edges: {sum(data[et].edge_index.shape[1] for et in data.edge_types)}")

    return data


def load_dgl_hetero_graph(data_dir: str = '.'):
    """
    Load heterogeneous graph in DGL format

    Args:
        data_dir: Directory containing the graph data files

    Returns:
        DGLHeteroGraph object
    """
    try:
        import dgl
        import torch
    except ImportError:
        raise ImportError(
            "DGL is not installed. "
            "Install it with: pip install dgl"
        )

    loader = GraphDataLoader(data_dir)

    # Load edges
    print("Loading edges...")
    graph_data = {}

    # User -> Skill edges (bidirectional)
    src, dst, _ = loader.load_edges('user_skill')
    graph_data[('user', 'has_skill', 'skill')] = (src, dst)
    graph_data[('skill', 'rev_has_skill', 'user')] = (dst, src)
    print(f"  user <-> skill: {len(src)} edges")

    # Job -> Skill edges (bidirectional)
    src, dst, _ = loader.load_edges('job_skill')
    graph_data[('job', 'requires_skill', 'skill')] = (src, dst)
    graph_data[('skill', 'rev_requires_skill', 'job')] = (dst, src)
    print(f"  job <-> skill: {len(src)} edges")

    # User -> Job edges (directed)
    src, dst, _ = loader.load_edges('user_job')
    graph_data[('user', 'interacts', 'job')] = (src, dst)
    print(f"  user -> job: {len(src)} edges")

    # Create heterogeneous graph
    g = dgl.heterograph(graph_data)

    # Add node features
    print("\nLoading node features...")
    for node_type in ['user', 'job', 'skill']:
        features, num_nodes = loader.load_node_features(node_type)
        g.nodes[node_type].data['feat'] = torch.tensor(features, dtype=torch.float)
        print(f"  {node_type}: {num_nodes} nodes, {features.shape[1]} features")

    # Add edge features
    print("\nLoading edge features...")

    # User-Skill edge features
    _, _, edge_feat = loader.load_edges('user_skill')
    g.edges['has_skill'].data['feat'] = torch.tensor(edge_feat, dtype=torch.float)
    g.edges['rev_has_skill'].data['feat'] = torch.tensor(edge_feat, dtype=torch.float)

    # Job-Skill edge features
    _, _, edge_feat = loader.load_edges('job_skill')
    g.edges['requires_skill'].data['feat'] = torch.tensor(edge_feat, dtype=torch.float)
    g.edges['rev_requires_skill'].data['feat'] = torch.tensor(edge_feat, dtype=torch.float)

    # User-Job edge features and labels
    _, _, edge_feat = loader.load_edges('user_job')
    g.edges['interacts'].data['feat'] = torch.tensor(edge_feat, dtype=torch.float)

    edge_df = pd.read_csv(os.path.join(data_dir, 'edges_user_job.csv'))
    g.edges['interacts'].data['label'] = torch.tensor(
        edge_df['is_apply'].values, dtype=torch.long
    )

    print("\nGraph loaded successfully!")
    print(f"Total nodes: {g.num_nodes()}")
    print(f"Total edges: {g.num_edges()}")

    return g


def get_train_val_test_split(data_dir: str = '.',
                             train_ratio: float = 0.7,
                             val_ratio: float = 0.15,
                             seed: int = 42):
    """
    Create train/val/test split for edge prediction task

    Args:
        data_dir: Directory containing the graph data files
        train_ratio: Ratio of edges for training
        val_ratio: Ratio of edges for validation
        seed: Random seed for reproducibility

    Returns:
        Dictionary with train/val/test edge indices
    """
    np.random.seed(seed)

    # Load user-job edges
    edge_df = pd.read_csv(os.path.join(data_dir, 'edges_user_job.csv'))

    # Sort by date to create temporal split
    edge_df = edge_df.sort_values('date_key')

    num_edges = len(edge_df)
    train_end = int(num_edges * train_ratio)
    val_end = int(num_edges * (train_ratio + val_ratio))

    train_idx = np.arange(0, train_end)
    val_idx = np.arange(train_end, val_end)
    test_idx = np.arange(val_end, num_edges)

    return {
        'train': train_idx,
        'val': val_idx,
        'test': test_idx,
        'train_pos': edge_df.iloc[train_idx][edge_df['is_apply'] == 1].index.values,
        'val_pos': edge_df.iloc[val_idx][edge_df['is_apply'] == 1].index.values,
        'test_pos': edge_df.iloc[test_idx][edge_df['is_apply'] == 1].index.values
    }


if __name__ == '__main__':
    print("=" * 60)
    print("Job Recommendation Graph Data Loader")
    print("=" * 60)

    # Example usage
    data_dir = '.'

    print("\n--- Loading PyTorch Geometric Graph ---")
    try:
        pyg_data = load_pyg_hetero_graph(data_dir)
        print("\nPyTorch Geometric graph loaded successfully!")
        print(f"Node types: {pyg_data.node_types}")
        print(f"Edge types: {pyg_data.edge_types}")
    except ImportError as e:
        print(f"Skipping PyTorch Geometric: {e}")

    print("\n" + "=" * 60)
    print("\n--- Loading DGL Graph ---")
    try:
        dgl_graph = load_dgl_hetero_graph(data_dir)
        print("\nDGL graph loaded successfully!")
        print(f"Node types: {dgl_graph.ntypes}")
        print(f"Edge types: {dgl_graph.etypes}")
    except ImportError as e:
        print(f"Skipping DGL: {e}")

    print("\n" + "=" * 60)
    print("\n--- Creating Train/Val/Test Split ---")
    split = get_train_val_test_split(data_dir)
    print(f"Train edges: {len(split['train'])} (positive: {len(split['train_pos'])})")
    print(f"Val edges: {len(split['val'])} (positive: {len(split['val_pos'])})")
    print(f"Test edges: {len(split['test'])} (positive: {len(split['test_pos'])})")

    print("\n" + "=" * 60)
    print("Done!")

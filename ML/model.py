
import numpy as np
import torch.nn as nn
from torch.utils.data import Dataset, DataLoader
import torch
from utils import (
    iter_playlists_json
)



class ReIterablePlaylistCorpus:
    """Allow Word2Vec to read streaming playlists multiple epochs."""
    def __init__(self, folder_path, max_files=None):
        self.folder_path = folder_path
        self.max_files = max_files

    def __iter__(self):
        for pl in iter_playlists_json(self.folder_path, self.max_files):
            yield [t['track_uri'] for t in sorted(pl['tracks'], key=lambda x: x['pos'])]




# ----------------------------
# Two-Tower Dataset + Model
# ----------------------------

class TwoTowerDataset(Dataset):
    def __init__(self, df, neg_samples=3):
        self.df = df[df['label'] == 1].reset_index(drop=True)
        self.neg_samples = neg_samples

        # Ép sang int64 ngay từ đầu
        self.pids = self.df['pid_idx'].astype('int64').values
        self.pos = self.df['track_idx'].astype('int64').values

        # All tracks cũng chuyển về int64
        self.all_tracks = df['track_idx'].astype('int64').unique()

    def __len__(self):
        return len(self.df)

    def __getitem__(self, idx):
        pid = int(self.pids[idx])
        pos = int(self.pos[idx])

        # neg samples cũng phải int64
        negs = np.random.choice(self.all_tracks, self.neg_samples).astype('int64')

        # Trả về torch.long
        return (
            torch.tensor(pid, dtype=torch.long),
            torch.tensor(pos, dtype=torch.long),
            torch.tensor(negs, dtype=torch.long)
        )



class TwoTowerModel_v2(nn.Module):
    def __init__(self, num_playlists, num_tracks, emb_dim=64):
        super().__init__()
        # +1 cho unseen index
        self.play_emb = nn.Embedding(num_playlists + 1, emb_dim)
        self.track_emb = nn.Embedding(num_tracks + 1, emb_dim)
        
        self.play_mlp = nn.Sequential(
            nn.Linear(emb_dim, emb_dim),
            nn.ReLU()
        )
        self.track_mlp = nn.Sequential(
            nn.Linear(emb_dim, emb_dim),
            nn.ReLU()
        )

    def forward(self, p_idx, t_idx):
        # map unseen index sang cuối cùng
        max_pid_idx = self.play_emb.num_embeddings - 1
        max_tid_idx = self.track_emb.num_embeddings - 1

        p_idx_safe = torch.where(p_idx < max_pid_idx, p_idx, torch.tensor(max_pid_idx, device=p_idx.device))
        t_idx_safe = torch.where(t_idx < max_tid_idx, t_idx, torch.tensor(max_tid_idx, device=t_idx.device))

        # embedding
        p_emb = self.play_emb(p_idx_safe)
        t_emb = self.track_emb(t_idx_safe)

        # forward MLP + dot-product
        return (self.play_mlp(p_emb) * self.track_mlp(t_emb)).sum(dim=1)

    @torch.no_grad()
    def encode_users(self, p_idx):
        # mapping unseen sang cuối cùng
        max_pid_idx = self.play_emb.num_embeddings - 1
        p_idx_safe = torch.where(p_idx < max_pid_idx, p_idx, torch.tensor(max_pid_idx, device=p_idx.device))
        return self.play_mlp(self.play_emb(p_idx_safe))

    @torch.no_grad()
    def encode_items(self, t_idx):
        # mapping unseen sang cuối cùng
        max_tid_idx = self.track_emb.num_embeddings - 1
        t_idx_safe = torch.where(t_idx < max_tid_idx, t_idx, torch.tensor(max_tid_idx, device=t_idx.device))
        return self.track_mlp(self.track_emb(t_idx_safe))
    
class TwoTowerModel(nn.Module): 
    def __init__(self, num_playlists, num_tracks, emb_dim=64): 
        super().__init__() 
        self.play_emb = nn.Embedding(num_playlists, emb_dim) 
        self.track_emb = nn.Embedding(num_tracks, emb_dim) 
        self.play_mlp = nn.Sequential(nn.Linear(emb_dim, emb_dim), nn.ReLU()) 
        self.track_mlp = nn.Sequential(nn.Linear(emb_dim, emb_dim), nn.ReLU()) 
    def forward(self, p_idx, t_idx): 
        p = self.play_emb(p_idx) 
        t = self.track_emb(t_idx) 
        return (self.play_mlp(p)*self.track_mlp(t)).sum(dim=1)



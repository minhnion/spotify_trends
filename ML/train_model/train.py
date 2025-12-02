
import numpy as np
import pandas as pd
from gensim.models import Word2Vec
from torch.utils.data import Dataset, DataLoader
import torch
import lightgbm as lgb
from sklearn.preprocessing import LabelEncoder
from tqdm import tqdm
import os
import random
import pickle


from utils import (
    precision_at_k,
    recall_at_k,
    dcg_at_k,
    ndcg_at_k,
    average_precision,
    iter_playlists_json,
    compute_global_popularity,
    compute_features_batch_enhanced,
    track2vec_sim_list,
    track2vec_candidates,
    load_json_to_df,
    rank_with_models,
    load_json_to_df_streaming,
    rank_with_models_v2,
    val_sample_to_df
    
)

from model import TwoTowerDataset, TwoTowerModel_v2, ReIterablePlaylistCorpus


def safe_transform(le, values):
    known = set(le.classes_)
    return np.array([le.transform([v])[0] if v in known else -1 for v in values])

def train_track2vec_streaming(folder_path, vector_size=64, window=5, epochs=5, max_files=None, save_path=None):
    """
    Train Track2Vec model (Word2Vec) từ playlist corpus và lưu model nếu save_path được cung cấp.
    
    Args:
        folder_path (str): folder chứa playlist JSON.
        vector_size (int): embedding dimension.
        window (int): Word2Vec window size.
        epochs (int): số epoch train.
        max_files (int): giới hạn số file JSON dùng.
        save_path (str): đường dẫn để lưu model (Word2Vec). Nếu None, không lưu.
    
    Returns:
        Word2Vec model đã train.
    """
    corpus = ReIterablePlaylistCorpus(folder_path, max_files)
    model = Word2Vec(vector_size=vector_size, window=window, min_count=1, workers=4)
    model.build_vocab(corpus)
    model.train(corpus, total_examples=model.corpus_count, epochs=epochs)
    
    if save_path:
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        model.save(save_path)
        print(f"Track2Vec model saved to {save_path}")
    
    return model

@torch.no_grad()
def fast_eval(model, df_val, uid_le, tid_le, ks=10, device="cuda"):

    # --- caching encode users/items ---
    unique_u = torch.tensor(uid_le.transform(df_val["pid"].unique()), device=device)
    unique_i = torch.tensor(tid_le.transform(df_val["track_uri"].unique()), device=device)

    user_embs = model.encode_users(unique_u)         # shape [U, D]
    item_embs = model.encode_items(unique_i)         # shape [I, D]

    # mapping id → index
    u2idx = {u: idx for idx, u in enumerate(unique_u.cpu().numpy())}
    i2idx = {i: idx for idx, i in enumerate(unique_i.cpu().numpy())}

    # nhóm theo user → list item
    grouped = {}
    for row in df_val.itertuples():
        uid = uid_le.transform([row.pid])[0]
        iid = tid_le.transform([row.track_uri])[0]
        if uid not in grouped:
            grouped[uid] = []
        grouped[uid].append(iid)

    results = {k: 0 for k in ks}
    total = len(grouped)

    # FAST Vectorized dot-product
    item_matrix = item_embs.T  # [D, I]

    for uid, items in grouped.items():
        u_idx = u2idx[uid]
        uvec = user_embs[u_idx]          # [D]

        scores = (uvec @ item_matrix).cpu().numpy()   # FAST

        top_items = scores.argsort()[::-1][:max(ks)]

        for k in ks:
            # check nếu item thật có trong top-k
            if any(i2idx[i] in top_items[:k] for i in items):
                results[k] += 1

    for k in ks:
        results[k] /= total

    return results


def train_twotower_with_validation_gpu(df_train, df_val, device='cuda',
                                       emb_dim=64, lr=1e-3, batch_size=1024,
                                       epochs=5, ckpt_dir="checkpoints", ks=[10]):
    import os
    os.makedirs(ckpt_dir, exist_ok=True)

 # ==== 1. Fast categorical encoding ====
    df_train['pid_idx'] = df_train['pid'].astype('category').cat.codes
    df_train['track_idx'] = df_train['track_uri'].astype('category').cat.codes

    pid_categories = df_train['pid'].astype('category').cat.categories
    track_categories = df_train['track_uri'].astype('category').cat.categories

    # ==== 2. Fast safe-transform val ====
    pid_map = {v: i for i, v in enumerate(pid_categories)}
    tid_map = {v: i for i, v in enumerate(track_categories)}
    
    pid2idx = {v: i for i, v in enumerate(pid_categories)}     
    track2idx = {v: i for i, v in enumerate(track_categories)}
    os.makedirs("checkpoints", exist_ok=True)
    with open("checkpoints/mappings.pkl", "wb") as f:
        pickle.dump({"pid2idx": pid2idx, "track2idx": track2idx}, f)
    print("Mappings saved to checkpoints/mappings.pkl")

    df_val['pid_idx'] = df_val['pid'].map(pid_map).fillna(-1).astype(int)
    df_val['track_idx'] = df_val['track_uri'].map(tid_map).fillna(-1).astype(int)

    df_val = df_val[(df_val['pid_idx'] != -1) & (df_val['track_idx'] != -1)]

    # ==== 3. Prepare other metadata ====
    num_playlists = df_train['pid_idx'].nunique()
    num_tracks = df_train['track_idx'].nunique()

    val_pid_group = (
        df_val.groupby('pid')['track_uri']
        .agg(list)
        .to_dict()
    )

    unique_val_pids = df_val['pid'].unique()
    all_track_idx = torch.arange(num_tracks)
    
    ds = TwoTowerDataset(df_train)
    num_workers = 0 if os.name == "nt" else 4
    dl = DataLoader(ds, batch_size=batch_size, shuffle=True,
                num_workers=num_workers, pin_memory=True)

    model = TwoTowerModel_v2(num_playlists, num_tracks, emb_dim).to(device)
    opt = torch.optim.Adam(model.parameters(), lr=lr)

    best_ndcg = 0
    ckpt_path = os.path.join(ckpt_dir, "best_twotower.pth")


    # ==== Training ====
    for ep in range(epochs):
        print(f"\n===== Epoch {ep+1}/{epochs} =====")

        model.train()
        total_loss = 0
        train_pbar = tqdm(dl, desc=f"Train E{ep+1}", leave=False)

        for pid, pos, negs in train_pbar:
            pid, pos, negs = pid.to(device), pos.to(device), negs.to(device)

            opt.zero_grad()
            pos_score = model(pid, pos)
            neg_score = model(pid.repeat_interleave(negs.shape[1]),
                              negs.view(-1)).view(pid.shape[0], -1).mean(1)

            loss = -torch.log(torch.sigmoid(pos_score - neg_score)).mean()
            loss.backward()
            opt.step()

            total_loss += loss.item()
            train_pbar.set_postfix(loss=loss.item())

        avg_loss = total_loss / len(dl)

        # ==== EVAL VECTORIZED ====
        model.eval()

        # map pid -> idx
        pid_map = {p: i for i, p in enumerate(df_train['pid'].astype('category').cat.categories)}
        track_map = {i: t for i, t in enumerate(df_train['track_uri'].astype('category').cat.categories)}

        # all tracks tensor long
        all_track_idx = torch.arange(num_tracks, dtype=torch.long).to(device)
        # map pid -> idx
        val_pid_idx = torch.tensor([pid_map[p] for p in unique_val_pids], dtype=torch.long).to(device)

        # expand batch cho tất cả tracks
        pid_expand = val_pid_idx.unsqueeze(1).repeat(1, num_tracks).view(-1)
        track_expand = all_track_idx.repeat(len(val_pid_idx))
        batch_size = 1024
        all_scores = []

        for i in range(0, len(pid_expand), batch_size):
            pid_batch = pid_expand[i:i+batch_size]
            track_batch = track_expand[i:i+batch_size]
            with torch.no_grad():
                all_scores.append(model(pid_batch, track_batch).cpu())

        scores = torch.cat(all_scores)
        scores = scores.view(len(val_pid_idx), num_tracks)

        # lấy topk index
        topk_idx = torch.topk(scores, k=max(ks), dim=1).indices.cpu().numpy()  # [num_val_pids, topk]

        # map topk idx về track_uri
        topk_preds = np.array([[track_map[i] for i in row] for row in topk_idx], dtype=object)

        # đảm bảo luôn là 2D
        if topk_preds.ndim == 1:
            topk_preds = topk_preds.reshape(1, -1)

        # lấy truth list
        truth_list = [val_pid_group.get(p, []) for p in unique_val_pids]

        # tính NDCG cho từng k
        ndcg_scores = []
        for k in ks:
            preds_k = topk_preds[:, :k]
            ndcg_k = np.mean([ndcg_at_k(truth, pred, k) 
                            for truth, pred in zip(truth_list, preds_k)])
            ndcg_scores.append(ndcg_k)

        print(f"Epoch {ep+1}: Loss={avg_loss:.4f} NDCG={ndcg_scores}")

        if np.mean(ndcg_scores) > best_ndcg:
            best_ndcg = np.mean(ndcg_scores)
            torch.save({
                "model_state_dict": model.state_dict(),
                "optimizer_state_dict": opt.state_dict(),
                "best_ndcg": best_ndcg
            }, ckpt_path)
            print(f">> Saved best model NDCG={best_ndcg:.4f}")

    return model,pid2idx, track2idx

# ----------------------------
# Full pipeline example
# ----------------------------
def train(
    train_path="dataset_splits/train.json",
    val_path="dataset_splits/val.json",
    test_path="dataset_splits/test.json",
    config=None,
    device="cuda",
    folder_path="D:/Downloads/spotify_million_playlist_dataset/data",
    max_files= 10
):
    """
    print("Loading datasets...")
    df_train = load_json_to_df_streaming(train_path, max_playlists=800)
    df_val   = load_json_to_df_streaming(val_path, max_playlists=100)
    """

    print("Computing global popularity...")
    global_pop = compute_global_popularity(folder_path, max_files=max_files)

    print("Training Track2Vec...")
    t2v_model = train_track2vec_streaming(folder_path,
                                          vector_size=64, window=5,
                                          epochs=5, max_files=max_files, save_path="checkpoints/track2vec.model")

    if config is None:
        config = {
            "tt_emb_dim": 64,
            "tt_lr": 5e-4,
            "tt_batch_size": 256,
            "tt_epochs": 10,
            "lgb_num_leaves": 63,
            "lgb_lr": 0.1,
            "lgb_estimators": 200,
            "alpha": 0.1
        }

    # ---- Build candidate features ----
    print("Building candidate features...")
    """
    playlists = df_train.groupby("pid")["track_uri"].apply(list).to_dict()
    pid_list = list(playlists.keys())
    """
    playlists = list(iter_playlists_json(folder_path, max_files=max_files))
    random.shuffle(playlists)
    playlists = playlists[:100000]

    val_sample = playlists[:10000]
    
    df_val = val_sample_to_df(val_sample)
    
    rows = []

    for i in range(0, 1200, 50):
            batch = playlists[i:i+50]
            rows.extend(
                compute_features_batch_enhanced(
                    batch, global_pop, t2v_model,
                    top_seed=5, cand_t2v_topk=200,
                    max_cands=500, pop_topk=300
                )
            )
    df = pd.DataFrame(rows)

        

    # ---- Train Two-Tower ----
    print("Training TwoTower...")
    tt_model, pid2idx, track2idx, = train_twotower_with_validation_gpu(
        df_train=df[["pid","track_uri","label"]].rename(columns={"pid":"pid","track_uri":"track_uri"}),
        df_val=df_val,
        device=device,
        emb_dim=config["tt_emb_dim"],
        lr=config["tt_lr"],
        batch_size=config["tt_batch_size"],
        epochs=config["tt_epochs"]
    )

    # ---- Train LightGBM ----
    print("Training LightGBM...")
    lgb_train = lgb.Dataset(
        df[["cand_rank","popularity","t2v_score","seed_overlap","playlist_len"]],
        label=df["label"]
    )

    lgb_params = {
        "num_leaves": config["lgb_num_leaves"],
        "learning_rate": config["lgb_lr"],
        "objective": "binary",
        "feature_fraction": 0.8,
        "bagging_fraction": 0.8,
        "min_data_in_leaf": 20,
        "verbose": -1
    }

    lgb_model = lgb.train(lgb_params, lgb_train, num_boost_round=config["lgb_estimators"])
    ckpt_dir = "checkpoints_lgb"
    os.makedirs(ckpt_dir, exist_ok=True)

    lgb_model_path = os.path.join(ckpt_dir, "lgb_model.txt")

    # Lưu model
    lgb_model.save_model(lgb_model_path)

    print(f"LightGBM model saved to {lgb_model_path}")

    # ---- Evaluate ----
    print("Evaluating model...")
    ndcgs = []

    val_groups = df_val.groupby("pid", group_keys=False).apply(lambda g: [{"track_uri": t, "pos": i} for i, t in enumerate(g["track_uri"])]
).to_dict()

    for pid, truth in tqdm(val_groups.items()):
        preds, truth_used = rank_with_models_v2(
            {"pid": pid, "tracks": val_groups[pid]},
            global_pop, t2v_model,
            lgb_model, tt_model,
            pid2idx, track2idx,
            num_playlists=len(pid2idx), 
            num_tracks=len(track2idx),
            alpha=config["alpha"]
        )
        ndcgs.append(ndcg_at_k(truth_used, preds, 100))

    print("Final val NDCG@100 =", np.mean(ndcgs))

    return t2v_model, lgb_model, tt_model, global_pop, config



# ----------------------------
# Example usage
# ----------------------------
if __name__ == "__main__":
    train()

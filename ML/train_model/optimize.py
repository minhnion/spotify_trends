import os
import json
import random
from collections import Counter
from tqdm import tqdm
import numpy as np
import pandas as pd
from scipy.sparse import csr_matrix
from sklearn.model_selection import train_test_split
import lightgbm as lgb
import torch
import torch.nn as nn
from torch.utils.data import Dataset, DataLoader
from gensim.models import Word2Vec

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
    track2vec_candidates
    
)

from model import TwoTowerDataset, TwoTowerModel, ReIterablePlaylistCorpus





def train_track2vec_streaming(folder_path, vector_size=64, window=5, epochs=5, max_files=None):
    corpus = ReIterablePlaylistCorpus(folder_path, max_files)
    model = Word2Vec(vector_size=vector_size, window=window, min_count=1, workers=4)
    model.build_vocab(corpus)
    model.train(corpus, total_examples=model.corpus_count, epochs=epochs)
    return model



def train_twotower(df, device='cuda', emb_dim=64, lr=1e-3, batch_size=1024, epochs=5):
    from sklearn.preprocessing import LabelEncoder

    pid_le = LabelEncoder()
    tid_le = LabelEncoder()

    df['pid_idx'] = pid_le.fit_transform(df['pid'])
    df['track_idx'] = tid_le.fit_transform(df['track_uri'])

    ds = TwoTowerDataset(df)
    dl = DataLoader(ds, batch_size=batch_size, shuffle=True, num_workers=2)

    num_playlists = df['pid_idx'].nunique()
    num_tracks = df['track_idx'].nunique()

    model = TwoTowerModel(num_playlists, num_tracks, emb_dim).to(device)
    opt = torch.optim.Adam(model.parameters(), lr=lr)

    for ep in range(epochs):
        total_loss = 0
        for pid, pos, negs in dl:
            pid, pos, negs = pid.to(device), pos.to(device), negs.to(device)
            opt.zero_grad()

            pos_score = model(pid, pos)
            neg_score = model(
                pid.repeat_interleave(negs.shape[1]),
                negs.view(-1)
            ).view(pid.shape[0], -1).mean(1)

            loss = -torch.log(torch.sigmoid(pos_score - neg_score)).mean()
            loss.backward()
            opt.step()
            total_loss += loss.item()

        print(f"[TwoTower] Epoch {ep+1}/{epochs} Loss={total_loss:.4f}")

    return model, pid_le, tid_le


# ----------------------------
# Rank With Models
# ----------------------------

def rank_with_models(pl,
                     global_pop,
                     t2v_model,
                     lgb_model,
                     tt_model,
                     pid_le,
                     tid_le,
                     alpha=0.3,
                     max_cands=500):

    tracks_sorted = sorted(pl['tracks'], key=lambda x: x['pos'])
    seed_tracks = [t['track_uri'] for t in tracks_sorted][:5]

    # Track2vec candidates
    t2v_counter = Counter()
    for s in seed_tracks:
        sims = track2vec_sim_list(t2v_model, s, topk=200)
        for trk, score in sims:
            t2v_counter[trk] += score

    t2v_cands = [t for t, _ in t2v_counter.most_common(max_cands)]
    popular_top = [t for t,_ in global_pop.most_common(300)]

    # merge with popularity
    seen = set()
    cands = []
    for t in t2v_cands:
        if t not in seen:
            seen.add(t)
            cands.append(t)
            if len(cands) == max_cands: break
    if len(cands) < max_cands:
        for t in popular_top:
            if t not in seen:
                seen.add(t)
                cands.append(t)
                if len(cands) == max_cands: break

    # truth
    truth = set(t['track_uri'] for t in pl['tracks'])

    # Build DataFrame for LGB features
    rows = []
    for rank, tid in enumerate(cands):
        rows.append({
            'pid': pl['pid'],
            'track_uri': tid,
            'cand_rank': rank,
            'popularity': global_pop.get(tid, 0),
            't2v_score': t2v_counter.get(tid, 0.0),
            'seed_overlap': 1 if tid in seed_tracks else 0,
            'playlist_len': len(tracks_sorted)
        })

    df_c = pd.DataFrame(rows)

    # LGB predict
    lgb_pred = lgb_model.predict(df_c[['cand_rank','popularity','t2v_score','seed_overlap','playlist_len']])

    # Two Tower predict
    df_c['pid_idx'] = pid_le.transform(df_c['pid'])
    df_c['track_idx'] = tid_le.transform(df_c['track_uri'])
    tt_scores = tt_model(
        torch.tensor(df_c['pid_idx'].values).cuda(),
        torch.tensor(df_c['track_idx'].values).cuda()
    ).detach().cpu().numpy()

    final_score = (1 - alpha) * lgb_pred + alpha * tt_scores
    df_c['final_score'] = final_score

    df_c = df_c.sort_values('final_score', ascending=False)

    preds = df_c['track_uri'].tolist()
    return preds, truth


# ----------------------------
# Random Search Main Function
# ----------------------------

def random_search_streaming(folder_path, csv_path, n_trials=5, max_files=None, device='cuda'):
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)

    if not os.path.exists(csv_path):
        pd.DataFrame().to_csv(csv_path, index=False)

    best_score = -1
    best_cfg = None

    print("Computing global popularity...")
    global_pop = compute_global_popularity(folder_path, max_files=max_files)

    print("Training Track2Vec model...")
    t2v_model = train_track2vec_streaming(folder_path, vector_size=64, window=5, epochs=5, max_files=max_files)

    search_space = {
        'tt_emb_dim':[64,128],
        'tt_lr':[1e-4,5e-4],
        'tt_batch_size':[128,256],
        'tt_epochs':[5,10],
        'lgb_num_leaves':[31,63],
        'lgb_lr':[0.05,0.1],
        'lgb_estimators':[100,200],
        'alpha':[0.1,0.3,0.5, 0.7]
    }

    playlists = list(iter_playlists_json(folder_path, max_files=max_files))
    random.shuffle(playlists)
    playlists = playlists[:2000]

    val_sample = playlists[:300]

    for trial in range(n_trials):
        print(f"\n========== TRIAL {trial+1}/{n_trials} ==========")

        cfg = {k: random.choice(v) for k, v in search_space.items()}
        print("Config:", cfg)

        # Step 1: Build candidate feature data
        print("Building candidate features...")
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

        # Step 2: Train Two Tower
        print("Training TwoTower...")
        tt_model, pid_le, tid_le = train_twotower(
            df,
            device=device,
            emb_dim=cfg['tt_emb_dim'],
            lr=cfg['tt_lr'],
            batch_size=cfg['tt_batch_size'],
            epochs=cfg['tt_epochs']
        )

        # Step 3: Train LightGBM
        print("Training LightGBM...")
        lgb_train = lgb.Dataset(
            df[['cand_rank','popularity','t2v_score','seed_overlap','playlist_len']],
            label=df['label']
        )

        lgb_params = {
            'num_leaves': cfg['lgb_num_leaves'],
            'learning_rate': cfg['lgb_lr'],
            'objective': 'binary',
            'min_data_in_leaf': 20,
            'feature_fraction': 0.8,
            'bagging_fraction': 0.8,
            'verbose': -1
        }

        lgb_model = lgb.train(
            lgb_params,
            lgb_train,
            num_boost_round=cfg['lgb_estimators']
        )

        # Step 4: Evaluate
        print("Evaluating model...")
        ndcgs = []

        for pl in tqdm(val_sample):
            preds, truth = rank_with_models(
                pl, global_pop, t2v_model,
                lgb_model, tt_model,
                pid_le, tid_le,
                alpha=cfg['alpha']
            )
            ndcgs.append(ndcg_at_k(truth, preds, 100))

        score = np.mean(ndcgs)
        print("Trial score =", score)

        # Save to CSV
        row_out = cfg.copy()
        row_out['score'] = score
        pd.DataFrame([row_out]).to_csv(csv_path, mode='a', header=False, index=False)

        if score > best_score:
            best_score = score
            best_cfg = cfg

    print("\n========== BEST ==========")
    print("Best score:", best_score)
    print("Best config:", best_cfg)
    return best_cfg


if __name__=="__main__":
    folder_path = r"D:\Downloads\spotify_million_playlist_dataset\data"
    csv_path = "ML/random_search_results.csv"
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    best_cfg = random_search_streaming(folder_path, csv_path, n_trials=30, max_files=20, device='cuda')

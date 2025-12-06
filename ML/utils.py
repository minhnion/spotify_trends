import os
import json
from collections import Counter
import numpy as np
import torch
import pandas as pd
import tqdm
import ijson




def precision_at_k(truth, preds, k):
    preds_k = preds[:k]
    return len(set(preds_k) & set(truth)) / k

def recall_at_k(truth, preds, k):
    return len(set(preds[:k]) & set(truth)) / len(truth) if len(truth) > 0 else 0.0

def dcg_at_k(rel_list, k):
    rel_list = np.asarray(rel_list)[:k]
    if rel_list.size == 0: return 0.0
    discounts = np.log2(np.arange(2, rel_list.size + 2))
    return np.sum(rel_list / discounts)

def ndcg_at_k(truth, preds, k):
    rel = [1 if p in truth else 0 for p in preds[:k]]
    idcg = dcg_at_k(sorted(rel, reverse=True), k)
    return dcg_at_k(rel, k) / idcg if idcg > 0 else 0.0

def average_precision(truth, preds, k=None):
    if k is None: k = len(preds)
    num_hits, score = 0.0, 0.0
    for i, p in enumerate(preds[:k]):
        if p in truth:
            num_hits += 1.0
            score += num_hits / (i+1)
    return score / max(1.0, len(truth))


# ----------------------------
# Data streaming loader
# ----------------------------

def iter_playlists_json(folder_path, max_files=None):
    files = sorted([f for f in os.listdir(folder_path) if f.endswith(".json")])
    if max_files: files = files[:max_files]
    for file in files:
        with open(os.path.join(folder_path, file), "r", encoding="utf8") as f:
            data = json.load(f)
            for pl in data["playlists"]:
                yield pl


# ----------------------------
# Candidate Generator
# ----------------------------

def compute_global_popularity(folder_path, max_files=None):
    counter = Counter()
    for pl in iter_playlists_json(folder_path, max_files=max_files):
        for t in pl['tracks']:
            counter[t['track_uri']] += 1
    return counter

track2vec_cache = {}

def track2vec_sim_list(model, track, topk=200):
    if track in track2vec_cache:
        return track2vec_cache[track]
    try:
        sims = model.wv.most_similar(track, topn=topk)
    except KeyError:
        sims = []
    track2vec_cache[track] = sims
    return sims

def compute_features_batch_enhanced(
    playlists_batch, global_pop_counter, t2v_model,
    top_seed=5, cand_t2v_topk=200, max_cands=500, pop_topk=300):

    rows = []
    popular_top = [t for t,_ in global_pop_counter.most_common(pop_topk)]

    for pl in playlists_batch:
        pid = pl['pid']
        tracks_sorted = sorted(pl['tracks'], key=lambda x: x['pos'])
        seed_tracks = [t['track_uri'] for t in tracks_sorted][:top_seed]
        playlist_len = len(tracks_sorted)

        # t2v similarity aggregation
        t2v_counter = Counter()
        for s in seed_tracks:
            sims = track2vec_sim_list(t2v_model, s, topk=cand_t2v_topk)
            for trk, score in sims:
                t2v_counter[trk] += score

        t2v_cands = [t for t,_ in t2v_counter.most_common(max_cands)]

        candidates = []
        seen = set()

        for t in t2v_cands:
            if t not in seen:
                seen.add(t)
                candidates.append(t)
                if len(candidates) == max_cands: break

        if len(candidates) < max_cands:
            for t in popular_top:
                if t not in seen:
                    seen.add(t)
                    candidates.append(t)
                    if len(candidates) == max_cands: break

        truth = set(t['track_uri'] for t in pl['tracks'])

        cand_t2v_score = {}
        for t in candidates:
            ssum = 0.0
            for s in seed_tracks:
                sims = track2vec_cache.get(s)
                if sims:
                    for trk, score in sims:
                        if trk == t:
                            ssum += score
                            break
            cand_t2v_score[t] = ssum

        for rank, tid in enumerate(candidates):
            rows.append({
                'pid': pid,
                'track_uri': tid,
                'cand_rank': rank,
                'popularity': global_pop_counter.get(tid, 0),
                't2v_score': cand_t2v_score.get(tid, 0.0),
                'seed_overlap': 1 if tid in seed_tracks else 0,
                'playlist_len': playlist_len,
                'label': 1 if tid in truth else 0
            })

    return rows


def track2vec_candidates(model, seed_tracks, topk=500):
    counter = Counter()
    for t in seed_tracks:
        sims = track2vec_sim_list(model, t, topk=topk)
        for trk, score in sims:
            counter[trk] += score
    return [t for t, _ in counter.most_common(topk)]



def save_checkpoint(model, optimizer, epoch, best_loss, path):
    torch.save({
        'model_state_dict': model.state_dict(),
        'optimizer_state_dict': optimizer.state_dict(),
        'epoch': epoch,
        'best_loss': best_loss
    }, path)

def load_checkpoint(model, optimizer, path, device='cuda'):
    ckpt = torch.load(path, map_location=device)
    model.load_state_dict(ckpt['model_state_dict'])
    optimizer.load_state_dict(ckpt['optimizer_state_dict'])
    return ckpt['epoch'], ckpt['best_loss']




def safe_transform(le, values):
    """Transform values using LabelEncoder, unseen values -> -1."""
    classes_set = set(le.classes_)
    transformed = []
    for v in values:
        if v in classes_set:
            transformed.append(le.transform([v])[0])
        else:
            transformed.append(-1)
    return np.array(transformed)

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

def load_json_to_df(path):
    """
    Chuyển playlist JSON thành dataframe pid / track_uri
    """

    rows = []
    with open(path, "r", encoding="utf8") as f:
        data = json.load(f)  # data là list

    for pl in data:
        pid = pl["pid"]
        for t in pl["tracks"]:
            rows.append({
                "pid": pid,
                "track_uri": t["track_uri"],
                # bạn có thể thêm các feature khác nếu muốn:
                "artist_name": t.get("artist_name"),
                "album_uri": t.get("album_uri"),
                "track_name": t.get("track_name"),
            })
    return pd.DataFrame(rows)

def load_json_to_df_streaming(path, max_playlists=None):
    pids = []
    track_uris = []
    poss = []

    with open(path, "r", encoding="utf-8") as f:
        # file dạng LIST
        parser = ijson.items(f, "item")
        count = 0
        
        for pl in tqdm(parser, desc=f"Streaming {path}"):
            pid = pl["pid"]

            for idx, t in enumerate(pl.get("tracks", [])):
                pids.append(pid)
                track_uris.append(t["track_uri"])
                poss.append(idx)

            count += 1
            if max_playlists and count >= max_playlists:
                break

    return pd.DataFrame({
        "pid": pids,
        "track_uri": track_uris,
        "pos": poss
    })
    
def rank_with_models_v2(
    pl,
    global_pop,
    t2v_model,
    lgb_model,
    tt_model,
    pid2idx,
    track2idx,
    num_playlists=None,
    num_tracks=None,
    alpha=0.3,
    max_cands=500
):
    """
    Rank tracks for a playlist `pl` using:
    - Track2Vec similarities
    - LightGBM features
    - Two-Tower model embeddings

    Arguments:
        pl: dict, playlist info, must contain 'pid' and 'tracks' (list of dict with 'track_uri')
        global_pop: Counter or dict of global popularity of tracks
        t2v_model: trained Word2Vec model
        lgb_model: trained LightGBM model
        tt_model: trained TwoTowerModel
        pid2idx: dict, map playlist id -> embedding index
        track2idx: dict, map track id -> embedding index
        num_playlists: int, number of playlists in TT embedding
        num_tracks: int, number of tracks in TT embedding
        alpha: float, weight for TT model
        max_cands: int, max candidates to rank
    Returns:
        preds: list of track_uri sorted by final score
        truth: set of ground-truth track_uri
    """

    # --- seed tracks ---
    tracks_sorted = sorted(pl['tracks'], key=lambda x: x['pos'])
    seed_tracks = [t['track_uri'] for t in tracks_sorted][:5]

    # --- Track2Vec candidates ---
    t2v_counter = Counter()
    for s in seed_tracks:
        sims = track2vec_sim_list(t2v_model, s, topk=200)
        for trk, score in sims:
            t2v_counter[trk] += score

    t2v_cands = [t for t, _ in t2v_counter.most_common(max_cands)]
    popular_top = [t for t,_ in global_pop.most_common(300)]

    # --- merge candidates ---
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

    # --- ground truth ---
    truth = set(t['track_uri'] for t in pl['tracks'])

    # --- build feature DataFrame for LGB ---
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

    # --- LGB prediction ---
    lgb_pred = lgb_model.predict(df_c[['cand_rank','popularity','t2v_score','seed_overlap','playlist_len']])

    # --- Two-Tower prediction ---
    # map pid/track to embedding index, unseen -> last index
    assert num_playlists is not None and num_tracks is not None, "num_playlists and num_tracks must be provided"

    pid_idx = torch.tensor([pid2idx.get(pl['pid'], num_playlists)] * len(df_c), dtype=torch.long).cuda()
    track_idx = torch.tensor([track2idx.get(tid, num_tracks) for tid in df_c['track_uri']], dtype=torch.long).cuda()

    # forward pass
    tt_scores = tt_model(pid_idx, track_idx).detach().squeeze()

    # --- combine scores ---
    final_score = (1 - alpha) * lgb_pred + alpha * tt_scores.cpu().numpy()
    df_c['final_score'] = final_score

    # --- sort and return ---
    df_c = df_c.sort_values('final_score', ascending=False)
    preds = df_c['track_uri'].tolist()

    return preds, truth

def val_sample_to_df(val_sample):
    pids = []
    track_uris = []
    poss = []

    for pl in tqdm.tqdm(val_sample, desc="Building df_val from val_sample"):
        pid = pl["pid"]
        for idx, t in enumerate(pl.get("tracks", [])):
            pids.append(pid)
            track_uris.append(t["track_uri"])
            poss.append(idx)

    df_val = pd.DataFrame({
        "pid": pids,
        "track_uri": track_uris,
        "pos": poss
    })

    return df_val
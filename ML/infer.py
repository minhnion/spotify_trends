import torch
import numpy as np
import pickle
from utils import (
    compute_global_popularity,
    rank_with_models_v2
)
import lightgbm as lgb
from model import TwoTowerModel_v2
from gensim.models import Word2Vec

# -----------------------------
# Load pretrained TwoTower
# -----------------------------
def load_twotower_model(ckpt_path, num_playlists, num_tracks, emb_dim=64, device="cuda"):
    model = TwoTowerModel_v2(num_playlists, num_tracks, emb_dim).to(device)
    checkpoint = torch.load(ckpt_path, map_location=device, weights_only=False)
    model.load_state_dict(checkpoint["model_state_dict"])
    model.eval()
    print(f"TwoTower model loaded from {ckpt_path} with best NDCG={checkpoint.get('best_ndcg', 'N/A')}")
    return model

# -----------------------------
# Load pretrained LightGBM
# -----------------------------
def load_lgb_model(lgb_path):
    model = lgb.Booster(model_file=lgb_path)
    print(f"LightGBM model loaded from {lgb_path}")
    return model

# -----------------------------
# Load mappings pid2idx / track2idx
# -----------------------------
def load_mappings(mapping_path):
    with open(mapping_path, "rb") as f:
        mappings = pickle.load(f)
    print(f"Mappings loaded from {mapping_path}")
    return mappings["pid2idx"], mappings["track2idx"]

# -----------------------------
# Load pretrained Track2Vec
# -----------------------------
def load_track2vec_model(path):
    model = Word2Vec.load(path)
    print(f"Track2Vec model loaded from {path}")
    return model

# -----------------------------
# Inference single playlist
# -----------------------------
@torch.no_grad()
def infer_playlist(
    playlist,       # dict: {"pid": str, "tracks": list of track_uri}
    global_pop,
    t2v_model,
    lgb_model,
    tt_model,
    pid2idx,
    track2idx,
    alpha=0.1,
    device="cuda",
    top_k=500
):
    tt_model.eval()

    preds, _ = rank_with_models_v2(
        playlist,
        global_pop,
        t2v_model,
        lgb_model,
        tt_model,
        pid2idx,
        track2idx,
        num_playlists=len(pid2idx),
        num_tracks=len(track2idx),
        alpha=alpha
    )
    return preds[:top_k]

# -----------------------------
# Batch inference
# -----------------------------
def infer_playlists(
    playlist_list,
    global_pop,
    t2v_model,
    lgb_model,
    tt_model,
    pid2idx,
    track2idx,
    alpha=0.1,
    top_k=500,
    device="cuda"
):
    results = {}
    for pl in playlist_list:
        pid = pl["pid"]
        preds = infer_playlist(pl, global_pop, t2v_model, lgb_model, tt_model,
                               pid2idx, track2idx, alpha=alpha, device=device, top_k=top_k)
        results[pid] = preds
    return results

# -----------------------------
# Example usage
# -----------------------------
if __name__ == "__main__":
    device = "cuda"

    # -----------------------------
    # Paths pretrained models
    # -----------------------------
    twotower_ckpt = "checkpoints/best_twotower.pth"
    lgb_model_path = "checkpoints_lgb/lgb_model.txt"
    mappings_path = "checkpoints/mappings.pkl"
    t2v_model_path = "checkpoints/track2vec.model"

    # -----------------------------
    # Load models & mappings
    # -----------------------------
    pid2idx, track2idx = load_mappings(mappings_path)
    tt_model = load_twotower_model(twotower_ckpt, num_playlists=len(pid2idx),
                                   num_tracks=len(track2idx), emb_dim=64, device=device)
    lgb_model = load_lgb_model(lgb_model_path)
    t2v_model = load_track2vec_model(t2v_model_path)

    # -----------------------------
    # Compute / load global popularity
    # -----------------------------
    folder_path = "D:/Downloads/spotify_million_playlist_dataset/data"
    global_pop = compute_global_popularity(folder_path, max_files=10)

    # -----------------------------
    # Example inference for 1 playlist
    # -----------------------------
    test_playlist = {"pid": "new_playlist_001", "tracks": ["spotify:track:1", "spotify:track:2"]}
    test_playlist_correct = {
    "pid": "new_playlist_001",
    "tracks": [{"track_uri": t, "pos": i} for i, t in enumerate(test_playlist["tracks"])]
}
    top500_tracks = infer_playlist(test_playlist_correct, global_pop, t2v_model, lgb_model, tt_model,
                                   pid2idx, track2idx, alpha=0.1, top_k=500)
    print(f"Top 10 tracks for {test_playlist['pid']}:", top500_tracks[:10])

    # -----------------------------
    # Batch inference example
    # -----------------------------
    from utils import iter_playlists_json
    playlist_list = list(iter_playlists_json(folder_path, max_files=5))
    recommended = infer_playlists(playlist_list, global_pop, t2v_model, lgb_model, tt_model,
                                  pid2idx, track2idx, alpha=0.1, top_k=500)
    first_pid = playlist_list[0]["pid"]
    print(f"Top 5 recommendations for {first_pid}:", recommended[first_pid][:5])

import os
import json
import random
import pickle
from tqdm import tqdm

# ============================================================
# 1. LOAD MPD JSON
# ============================================================

def load_mpd_jsons(folder_path, max_files=None):
    files = sorted([
        os.path.join(folder_path, f)
        for f in os.listdir(folder_path)
        if f.endswith(".json")
    ])

    if max_files:
        files = files[:max_files]

    playlists = []
    print(f"Loading {len(files)} JSON files...")

    for fp in tqdm(files):
        with open(fp, "r", encoding="utf-8") as f:
            data = json.load(f)
            playlists.extend(data["playlists"])

    print(f"Total playlists loaded: {len(playlists)}")
    return playlists


# ============================================================
# 2. SPLIT PLAYLISTS (70/15/15)
# ============================================================

def split_playlists(playlists, train_ratio=0.7, val_ratio=0.15, seed=42):
    """
    Split by playlist (shuffle toàn bộ playlist)
    Không split theo track.
    """
    rng = random.Random(seed)
    rng.shuffle(playlists)

    n = len(playlists)
    n_train = int(n * train_ratio)
    n_val = int(n * val_ratio)

    train_p = playlists[:n_train]
    val_p   = playlists[n_train:n_train+n_val]
    test_p  = playlists[n_train+n_val:]

    print(f"Train: {len(train_p)} | Val: {len(val_p)} | Test: {len(test_p)}")
    return train_p, val_p, test_p


# ============================================================
# 3. CREATE EVAL PLAYLISTS (seed + ground truth)
# ============================================================

def make_eval_playlists(playlists, seed_ratio=0.3, min_tracks=5):
    """
    Tạo danh sách:
    {
        pid,
        seed_tracks: ['uri1', 'uri2', ...],
        gt_tracks:   ['uriX', ...]
    }
    """
    eval_data = []

    for p in playlists:
        tracks = p["tracks"]
        if len(tracks) < min_tracks:
            continue

        # tách seed và ground-truth theo seed_ratio
        k = max(1, int(len(tracks) * seed_ratio))

        seed_uris = [t["track_uri"] for t in tracks[:k]]
        gt_uris   = [t["track_uri"] for t in tracks[k:]]

        eval_data.append({
            "pid": p["pid"],
            "seed_tracks": seed_uris,
            "gt_tracks": gt_uris
        })

    return eval_data


# ============================================================
# 4. SAVE SPLITS
# ============================================================

# ============================================================
# 4. SAVE SPLITS (JSON version)
# ============================================================

def save_json(obj, path):
    """Save Python object to JSON with UTF-8 + pretty formatting."""
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

    size_mb = os.path.getsize(path) / (1024 * 1024)
    print(f"Saved: {path} ({size_mb:.2f} MB)")


def save_splits(train_p, val_p, test_p, out_dir="dataset_splits_json"):
    os.makedirs(out_dir, exist_ok=True)

    save_json(train_p, f"{out_dir}/train.json")
    save_json(val_p,   f"{out_dir}/val.json")
    save_json(test_p,  f"{out_dir}/test.json")

    print("Saved train/val/test raw playlists (JSON).")


def save_eval_splits(val_eval, test_eval, out_dir="dataset_splits_json"):
    save_json(val_eval, f"{out_dir}/val_eval.json")
    save_json(test_eval, f"{out_dir}/test_eval.json")
    print("Saved val/test eval playlists (JSON).")


# ============================================================
# 5. MAIN
# ============================================================

def main():
    MPD_FOLDER = r"D:\Downloads\spotify_million_playlist_dataset\data"
    OUTPUT_DIR = "dataset_splits"

    MAX_FILES = 10
    TRAIN_RATIO = 0.7
    VAL_RATIO = 0.15
    SEED_RATIO = 0.3

    print("\n==== LOADING MPD ====")
    playlists = load_mpd_jsons(MPD_FOLDER, max_files=MAX_FILES)

    print("\n==== SPLITTING PLAYLISTS (70/15/15) ====")
    train_p, val_p, test_p = split_playlists(
        playlists,
        train_ratio=TRAIN_RATIO,
        val_ratio=VAL_RATIO
    )

    print("\n==== BUILDING EVAL SETS ====")
    val_eval  = make_eval_playlists(val_p,  seed_ratio=SEED_RATIO)
    test_eval = make_eval_playlists(test_p, seed_ratio=SEED_RATIO)

    print(f"Val eval: {len(val_eval)} playlists")
    print(f"Test eval: {len(test_eval)} playlists")

    print("\n==== SAVING ====")
    save_splits(train_p, val_p, test_p, out_dir=OUTPUT_DIR)
    save_eval_splits(val_eval, test_eval, out_dir=OUTPUT_DIR)

    print("\n Dataset split completed successfully!")


if __name__ == "__main__":
    main()

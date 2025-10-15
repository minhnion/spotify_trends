from fastapi import FastAPI
from pydantic import BaseModel
from typing import List

app = FastAPI(title="MPD Recommendation - Example")

class SeedPlaylist(BaseModel):
    name: str = None
    seed_tracks: List[str] = []

@app.get("/")
def root():
    return {"status": "ok", "message": "MPD pipeline example service"}

@app.post("/recommend")
def recommend(seed: SeedPlaylist):
    # NOTE: this is a placeholder implementation.
    # In a real system, you'd query candidate DB + call ranking model.
    seeds = seed.seed_tracks or []
    # naive: return seed tracks repeated or a static list
    recommendations = []
    for i in range(500):
        # dummy URIs
        recommendations.append(f"spotify:track:dummy_{i}")
    return {"num_seed": len(seeds), "recommendations": recommendations}

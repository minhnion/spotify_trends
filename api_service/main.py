import os
import json
import redis
from fastapi import FastAPI, HTTPException

app = FastAPI(
    title="Spotify Recommendation API",
    description="API để lấy gợi ý bài hát cho playlist.",
    version="1.0.0"
)

redis_host = os.getenv("REDIS_HOST", "localhost")
redis_client = redis.Redis(host=redis_host, port=6379, db=0, decode_responses=True)

@app.on_event("startup")
def startup_event():
    """Kiểm tra kết nối Redis khi khởi động."""
    try:
        redis_client.ping()
        print("Successfully connected to Redis.")
    except redis.exceptions.ConnectionError as e:
        print(f"Error connecting to Redis: {e}")

@app.get("/recommendations/{playlist_id}")
def get_recommendations(playlist_id: int):
    """
    Lấy về danh sách 10 track_uri được gợi ý cho một playlist_id cụ thể.
    """
    key = f"recommendations:{playlist_id}"
    
    # Truy vấn dữ liệu từ Redis
    recommendations_json = redis_client.get(key)
    
    if recommendations_json is None:
        raise HTTPException(
            status_code=404, 
            detail=f"Playlist with ID {playlist_id} not found or no recommendations available."
        )
        
    # Dữ liệu trong Redis là một chuỗi JSON, cần parse nó lại
    recommendations = json.loads(recommendations_json)
    
    return {"playlist_id": playlist_id, "recommended_tracks": recommendations}

@app.get("/")
def read_root():
    return {"message": "Welcome to the Spotify Recommendation API"}
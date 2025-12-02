import os
import redis
from fastapi import FastAPI, HTTPException

app = FastAPI(
    title="Spotify Recommendation API",
    description="Debugging Mode",
    version="1.0.0"
)

# Lấy cấu hình
redis_host = os.getenv("REDIS_HOST", "localhost")
redis_port = int(os.getenv("REDIS_PORT", 6379))

print(f"🔌 CONFIG: Connecting to Redis at {redis_host}:{redis_port}...")

# Kết nối Redis
try:
    redis_client = redis.Redis(host=redis_host, port=redis_port, db=0, decode_responses=True)
    redis_client.ping()
    print("✅ CONNECTION SUCCESS: Connected to Redis!")
    
    # [DEBUG]
    keys = redis_client.keys("playlist:*")
    print(f"👀 DEBUG SCAN: Found {len(keys)} keys starting with 'playlist:'.")
    if keys:
        print(f"👉 Sample keys: {keys[:5]}")
    else:
        print("⚠️ WARNING: Redis is EMPTY or keys have wrong prefix!")
        
except Exception as e:
    print(f"❌ CONNECTION FAILED: {e}")

@app.get("/recommendations/{playlist_id}")
def get_recommendations(playlist_id: int):
    key = f"playlist:{playlist_id}"
    
    print(f"🔍 SEARCHING: Looking for key '{key}'...")
    
    data_string = redis_client.get(key)
    
    if data_string is None:
        print(f"❌ NOT FOUND: Key '{key}' does not exist.")
        raise HTTPException(
            status_code=404, 
            detail=f"Playlist with ID {playlist_id} not found."
        )
        
    print(f"✅ FOUND: Data found for '{key}'")
    recommendations_list = data_string.split(",")
    
    return {
        "playlist_id": playlist_id, 
        "recommended_tracks": recommendations_list
    }

# --- ĐOẠN QUAN TRỌNG ĐỂ TEST PASS ---
@app.get("/")
def read_root():
    return {"message": "Spotify Lakehouse API is Running!"}
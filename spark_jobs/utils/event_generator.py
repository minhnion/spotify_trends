import random
from datetime import datetime
from .config import NEW_HIT_SONGS, EXISTING_PID_RANGE, PROB_NEW_PLAYLIST, PROB_NEW_HIT_SONG

def generate_event(new_pid_counter):
    """
    Tạo ra một sự kiện streaming ngẫu nhiên dựa trên các kịch bản.
    File này chứa toàn bộ logic giả lập.
    """
    # Kịch bản 1: Tạo playlist mới
    if random.random() < PROB_NEW_PLAYLIST:
        event_type = "create_playlist"
        pid = new_pid_counter
        track_uri = random.choice(NEW_HIT_SONGS) if random.random() < 0.5 else None
        
        print(f"*** Generating NEW PLAYLIST event: pid={pid} ***")
        
        return {
            "event_type": event_type,
            "pid": pid,
            "track_uri": track_uri,
            "playlist_name": f"New Awesome Playlist {pid}",
            "timestamp": datetime.now().isoformat()
        }, new_pid_counter + 1

    # Kịch bản 2: Thêm một track vào playlist đã có
    event_type = "add_track"
    pid = random.randint(*EXISTING_PID_RANGE)
    
    if random.random() < PROB_NEW_HIT_SONG:
        track_uri = random.choice(NEW_HIT_SONGS)
        print(f"--- Generating NEW HIT event: track={track_uri} ---")
    else:
        track_uri = f"spotify:track:old_track_{random.randint(0, 2000000)}"

    return {
        "event_type": event_type,
        "pid": pid,
        "track_uri": track_uri,
        "timestamp": datetime.now().isoformat()
    }, new_pid_counter
# File: kafka_producer.py (Chạy trên Windows)
import json
import time
import random
from kafka import KafkaProducer
from datetime import datetime

# Kết nối tới localhost:9092 (Vì chạy từ Windows)
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

TOPIC = "playlist_events"

print("🚀 Bắt đầu bắn dữ liệu vào Kafka...")

try:
    while True:
        # Giả lập dữ liệu sự kiện
        event = {
            "event_type": random.choice(["play", "skip", "like"]),
            "pid": random.randint(1, 100),
            "track_uri": f"spotify:track:{random.randint(1000, 9999)}",
            "playlist_name": f"My Playlist {random.randint(1, 10)}",
            "timestamp": datetime.now().isoformat()
        }
        
        producer.send(TOPIC, value=event)
        print(f"Sent: {event}")
        time.sleep(2) # Cứ 2 giây bắn 1 tin
except KeyboardInterrupt:
    print("Dừng Producer.")
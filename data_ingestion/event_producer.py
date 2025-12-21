import sys
from pathlib import Path
import json
import time
import random
from kafka import KafkaProducer

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from spark_jobs.utils.event_generator import generate_event
from spark_jobs.utils.config import NEW_PID_START



def create_producer():
    """Tạo một Kafka Producer."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("Kafka Producer connected successfully.")
        return producer
    except Exception as e:
        print(f"Error connecting to Kafka: {e}")
        time.sleep(5) # Đợi 5 giây rồi thử lại
        return create_producer()

def run_event_producer():
    producer = create_producer()
    topic_name = "playlist_events"
    new_pid_counter = NEW_PID_START

    try:
        while True:
            event, new_pid_counter = generate_event(new_pid_counter)

            # BẮT BUỘC
            event["event_ts"] = time.strftime(
                "%Y-%m-%dT%H:%M:%S", time.gmtime()
            )

            producer.send(
                topic_name,
                key=str(event["pid"]).encode(),
                value=event
            )

            # Flush nhẹ để tránh lag window
            if random.random() < 0.1:
                producer.flush()

            time.sleep(random.uniform(0.5, 1.5))

    except KeyboardInterrupt:
        print(" Producer stopped")

    finally:
        producer.flush()
        producer.close()

if __name__ == "__main__":
    run_event_producer()
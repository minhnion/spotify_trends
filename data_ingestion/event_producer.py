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
    """
    Hàm chính chạy producer.
    Nhiệm vụ: Lấy sự kiện từ generator và gửi vào Kafka.
    """
    producer = create_producer()
    if not producer:
        return

    topic_name = 'playlist_events'
    new_pid_counter = NEW_PID_START
    
    while True:
        try:
            event, new_pid_counter = generate_event(new_pid_counter)
            
            print(f"Sending event: {event}")
            producer.send(topic_name, key=str(event['pid']).encode('utf-8'), value=event)
            
            time.sleep(random.uniform(1, 3))
        
        except KeyboardInterrupt:
            print("\nProducer stopped.")
            break
        except Exception as e:
            print(f"An error occurred: {e}")
            time.sleep(5)
    
    producer.flush()
    producer.close()

if __name__ == "__main__":
    run_event_producer()
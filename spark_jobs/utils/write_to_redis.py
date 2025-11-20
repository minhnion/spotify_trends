import redis
import json

def write_to_redis(partition):
    r = redis.Redis(host='localhost', port=6379, db=0)

    with r.pipeline() as pipe:
        for row in partition:
            pid = row['pid']
            # Chuyển danh sách track_uri thành một chuỗi JSON
            recommendations_json = json.dumps(row['recommendations'])
            pipe.set(f"recommendations:{pid}", recommendations_json)
        pipe.execute()
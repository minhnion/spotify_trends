import redis
import json
import os

def write_to_redis(partition):
    """
    Write recommendations to Redis.
    Auto-detect environment (local vs K8s) based on KUBERNETES_SERVICE_HOST.
    """
    
    if os.getenv("KUBERNETES_SERVICE_HOST"):
        redis_host = 'redis-service'
        redis_port = 6379
        print(f"🚀 K8s mode - Connecting to Redis at {redis_host}:{redis_port}")
    else:
        redis_host = 'localhost'
        redis_port = 6379
        print(f"💻 Local mode - Connecting to Redis at {redis_host}:{redis_port}")
    
    try:
        r = redis.Redis(
            host=redis_host, 
            port=redis_port, 
            db=0,
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=5
        )
        
        r.ping()
        
        with r.pipeline() as pipe:
            count = 0
            for row in partition:
                pid = row['pid']
                recommendations_json = json.dumps(row['recommendations'])
                pipe.set(f"recommendations:{pid}", recommendations_json)
                count += 1
                
                if count % 1000 == 0:
                    pipe.execute()
                    print(f"✓ Wrote {count} recommendations to Redis")
            
            if count % 1000 != 0:
                pipe.execute()
            
            print(f"✓ Partition complete: {count} recommendations written to Redis")
            
    except redis.ConnectionError as e:
        print(f"❌ Redis connection error: {e}")
        print(f"   Tried to connect to: {redis_host}:{redis_port}")
        raise
    except Exception as e:
        print(f"❌ Error writing to Redis: {e}")
        raise
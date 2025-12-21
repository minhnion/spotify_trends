Access into container kafka-broker

```bash
docker exec -it kafka-broker /bin/bash
```

In container run:

```bash
kafka-topics --create \
  --topic playlist_events \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 1
```
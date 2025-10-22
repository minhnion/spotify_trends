# 🎵 Spotify MPD Data Pipeline Architecture

```text
[Spotify MPD JSON files]  
      │
      ├── (1) Data Ingestion  →  MinIO / AWS S3 (Object Storage for raw JSON)
      │        └─> Dùng Spark hoặc Dask đọc dữ liệu lớn, song song hóa
      │
      ├── (2) Batch ETL (Apache Spark / PySpark)
      │        ├─ Làm sạch, chuẩn hóa, parse metadata
      │        ├─ Lưu kết quả dạng Parquet / Delta Lake (curated zone)
      │        └─ Đồng bộ sang Feature Store (Feast / Delta table)
      │
      │              ▼
      │        [Feature Store (Delta / Parquet / Feast)]
      │              │
      │              ├─> Model Training (Spark MLlib / PyTorch / LightGBM)
      │              │       • Candidate Generation: popularity / ALS / Word2Vec  
      │              │       • Ranking Model: LightGBM / XGBoost / TFRS  
      │              │       • Evaluation: Precision@K / MAP / Recall@K  
      │              │
      │              └─> Save model artifacts → MLflow + MinIO (model registry)
      │
      ├── (3) Streaming Ingest Simulation
      │        ├─ Kafka topics (playlists, user_events)
      │        └─> Spark Structured Streaming / Flink
      │               ├─ Transformations, aggregations
      │               ├─ Write results to:
      │               │      • NoSQL store (Cassandra / MongoDB / Elasticsearch)
      │               │      • Vector store (Milvus / Qdrant / Redis-Vector)
      │               └─ Emit metrics → Prometheus + Grafana dashboard
      │
      └── (4) Serving Layer
               ├─ Model Serving: TorchServe / TF-Serving (containerized)
               ├─ API Gateway: FastAPI / Flask (deployed on Kubernetes)
               ├─ Query flow:
               │       [Client request] 
               │           ↓
               │       FastAPI → query candidate list from NoSQL / Vector store
               │           ↓
               │       Ranking service (LightGBM / NN model)
               │           ↓
               │       Return top-500 tracks (personalized recommendations)
               │
               └─ Logging & Monitoring:
                       • Logs: Loki / ELK  
                       • Metrics: Prometheus + Grafana  
                       • Model tracking: MLflow

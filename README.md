# 🎵 Spotify Million Playlist Dataset – Big Data Project

This project implements a **full-scale Big Data pipeline** to build a music recommendation system using the **Spotify Million Playlist Dataset**.  
It demonstrates a modern **data engineering lifecycle** from ingestion → processing → serving, deployed in a **cloud-native Kubernetes environment**.

![Python](https://img.shields.io/badge/Python-3.8%2B-blue)
![Spark](https://img.shields.io/badge/Apache%20Spark-3.5-orange)
![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.7-red)
![Kubernetes](https://img.shields.io/badge/Kubernetes-Minikube-blueviolet)

---

## 🏗️ Architecture Overview

The system follows a modern **Lambda / Kappa hybrid pattern** with a **Lakehouse architecture**, supporting both **batch** and **stream processing**, and is designed to be fully cloud-native.

### Core Components

- **Ingestion**
  - Kafka (Streaming)
  - Python Scripts (Batch)
- **Storage**
  - MinIO (S3-compatible Data Lake)
  - Apache Iceberg / Parquet
- **Processing**
  - Apache Spark (ETL, Structured Streaming, MLlib)
- **Serving**
  - Redis (Low-latency cache)
  - FastAPI (REST API)
- **Orchestration**
  - Apache Airflow
- **Deployment**
  - Docker Compose (Local)
  - Kubernetes (Production)

---

## 📂 Project Structure

| Directory | Description |
|---------|-------------|
| `/api_service` | FastAPI backend for serving recommendations via REST API |
| `/dags` | Apache Airflow DAGs for orchestration |
| `/data_ingestion` | Batch ingestion & data simulation scripts |
| `/kubernetes` | Kubernetes manifests (infra & applications) |
| `/notebooks` | Jupyter notebooks for EDA & prototyping |
| `/spark_jobs` | Spark ETL, training & streaming jobs |
| `docker-compose.yml` | Local development environment |

---

## 🚀 Getting Started

### Prerequisites

- **Docker Desktop** (Docker Compose enabled)
- **Python 3.8+**
- **Minikube & kubectl** (for Kubernetes deployment)

Optional (recommended):

```bash
pip install -r requirements.txt
```

---

## I. Quick Start (Local – Docker Compose)

Recommended for exploring the system without setting up a full cluster.

### 1. Configure Environment

```bash
cp .env.example .env
# Edit .env if needed (default: minioadmin / minioadmin)
```

### 2. Start Services

```bash
docker compose up -d
```

### 3. Access Interfaces

| Service | URL |
|-------|-----|
| MinIO Console | http://localhost:9001 |
| Airflow UI | http://localhost:8080 |
| Jupyter Lab | http://localhost:8888 |
| API Swagger | http://localhost:8000/docs |

Retrieve Jupyter token:

```bash
docker compose logs jupyter
```

### 4. Initial Data Ingestion

```bash
python data_ingestion/batch_ingest.py
```

Verify upload in bucket: **`spotify-raw-data`**

---

## II. Working with Spark Jobs

### 1. Install Required JARs

```bash
chmod +x spark_jobs/install_jars.sh
./spark_jobs/install_jars.sh
```

### 2. Build Spark Image (Optional)

```bash
docker build -t spark-jobs:latest -f spark_jobs/Dockerfile .
```

---

## III. Deploy to Kubernetes (Minikube)

### 1. Start Minikube

```bash
minikube start --cpus 4 --memory 6144
```

### 2. Use Minikube Docker Daemon

```bash
eval $(minikube -p minikube docker-env)
```

```bash
docker build -t spotify-api:latest ./api_service
docker build -t spark-jobs:latest -f spark_jobs/Dockerfile .
```

### 3. Apply Kubernetes Manifests

```bash
kubectl apply -f kubernetes/core-infra/00-namespace.yaml
kubectl apply -f kubernetes/core-infra/minio.yaml
kubectl apply -f kubernetes/core-infra/redis.yaml
kubectl apply -f kubernetes/apps/
```

### 4. Access Services

```bash
kubectl port-forward -n spotify service/minio-service 9000:9000 9001:9001
```

### 5. Seed Data

```bash
python data_ingestion/batch_ingest.py
```

---

## 🔄 Recommended Workflow

### Local Development Flow

1. docker compose up -d  
2. Ingest raw JSON data  
3. Run Spark ETL (JSON → Parquet / Iceberg)  
4. Train ALS recommendation model  
5. Precompute Top-K recommendations into Redis  
6. Query API: `/recommendations/{playlist_id}`  

### Kubernetes Flow

1. Deploy core infrastructure  
2. Build & push images  
3. Submit Spark jobs  
4. Monitor:
```bash
kubectl get pods -n spotify
```

---

## 🛠️ Troubleshooting

- **MinIO bucket missing** → Check `batch_ingest.py` & `.env`
- **Spark socket reset** → Increase memory (≥ 6GB)
- **Jupyter token**:
```bash
docker compose logs jupyter
```
- **Pending pods**:
```bash
kubectl describe pod <pod-name> -n spotify
```

---

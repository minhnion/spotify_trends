# Spotify Million Playlist Dataset - Big Data Project

This project implements a full-scale big data pipeline to build a music recommendation system based on the Spotify Million Playlist Dataset.

## Architecture Overview

The architecture follows a modern Lambda/Kappa pattern, leveraging a data lakehouse, batch/stream processing, and is fully deployed on Kubernetes.

- **Ingestion:** Kafka
- **Storage:** MinIO + Apache Iceberg
- **Processing:** Apache Spark (ETL, Streaming, ML)
- **Serving:** Redis + FastAPI
- **Orchestration:** Airflow / Dagster
- **Deployment:** Kubernetes (K8s)

## Project Structure

- `/api_service`: FastAPI backend for serving recommendations.
- `/dags`: Orchestration pipelines (Airflow/Dagster).
- `/data_ingestion`: Scripts for initial data loading.
- `/kubernetes`: All K8s manifests for deployment.
- `/notebooks`: Jupyter notebooks for EDA and experimentation.
- `/spark_jobs`: Core logic for all Spark applications.

## How to Get Started

1.  **Setup Environment:** (Instructions to be added)
2.  **Run Initial Ingestion:** (Instructions to be added)
3.  **Deploy to Kubernetes:** (Instructions to be added)

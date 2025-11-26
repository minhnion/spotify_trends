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

Socket reset / Python worker crashes (show() / collect errors)
----------------------------------------------------------
If you get a connection reset error originating from PythonRunner (java.net.SocketException: Connection reset) while calling show() or when collecting results, this means the Python worker process crashed, was killed, or the driver-worker socket was reset. Common causes and quick fixes:

- Worker crash due to incompatible Python or environment mismatches:
	- Set `PYSPARK_PYTHON` and `PYSPARK_DRIVER_PYTHON` to the same Python used to run your script (we set this inside `data_ingestion/ingest.py`).

- Incompatible Arrow / pyarrow version or stability issues:
	- Try disabling Arrow (set `spark.sql.execution.arrow.pyspark.enabled=false`). The example ingestion script disables Arrow by default to reduce these issues.

- Out-of-memory / large collect to driver:
	- Avoid calling `.show()` on large DataFrames or use `.show(5)` / `.limit(n)` / `.take(n)` to reduce data volume.
	- Increase `spark.driver.maxResultSize` (we set `1g` in `data_ingestion/ingest.py`) and/or increase executor memory.

- Network / firewall issues on Windows or local machine:
	- Make sure local firewall/antivirus doesn't block ephemeral ports used by Spark's Python workers.
	- Increase `spark.network.timeout` and `spark.executor.heartbeatInterval` if workers disconnect due to heartbeat timeouts.

Debugging tips:
	- Run a very small sample (sample fraction 0.01 or fewer rows) and call `.show()` to confirm the code path works.
	- Inspect logs printed to console for any Python tracebacks from worker processes. Worker crashes often print Python exceptions in the console above the Java socket error.
	- If you need, I can add a small executor-side debug job to print environment variables or Python version used by workers so you can confirm environment parity.

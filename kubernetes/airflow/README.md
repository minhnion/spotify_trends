# Airflow on Kubernetes (Production-Ready Setup)

This directory contains the Kubernetes manifests to deploy Apache Airflow with a **LocalExecutor** architecture backed by **PostgreSQL**. This setup is more robust than the default SQLite/SequentialExecutor and allows for parallel task execution.

## 🏗 Architecture

*   **Database**: PostgreSQL 13 (Persistent storage via PVC).
*   **Executor**: LocalExecutor (allows parallelism).
*   **Airflow Pod**: Runs 2 containers in parallel:
    1.  `webserver`: The UI.
    2.  `scheduler`: Schedules and triggers tasks.
*   **Init Container**: Automatically runs DB migrations and creates the default Admin user on startup.
*   **RBAC**: Configured with permissions to spawn Spark jobs (manage Pods, Services, ConfigMaps).

## 🚀 Deployment Guide

### 1. Prerequisites
Ensure your Minikube cluster is running and the namespace exists:
```bash
minikube start
kubectl create namespace spotify
```

### 2. Build the Docker Image
You must build the custom Airflow image (which includes Spark binaries and dependencies) directly inside the Minikube environment:

```bash
# Point Docker CLI to Minikube
eval $(minikube -p minikube docker-env)

# Build image from the project root
docker build -t airflow-k8s:latest -f Dockerfile.airflow.k8s .
```

### 3. Deploy PostgreSQL
Deploy the database first so it's ready for Airflow to connect.

```bash
kubectl apply -f kubernetes/airflow/postgres.yaml
```

### 4. Deploy Airflow
Apply the Airflow deployment. This will create the service account, roles, PVCs, and the Airflow pod.

```bash
kubectl apply -f kubernetes/airflow/airflow.yaml
```

*Note: The first time this runs, the `init-container` will take about 30-60 seconds to initialize the database schema.*

### 5. Access the UI

To access the Airflow Web UI:

**Method A: Minikube Service**
```bash
minikube service airflow-service -n spotify
```

**Method B: NodePort (Manual)**
The service is exposed on NodePort `30000`.
URL: `http://localhost:30000` (or `http://<minikube-ip>:30000`)

**Login Credentials:**
*   **Username**: `admin`
*   **Password**: `admin`
*(Created automatically by the init container)*

## 📦 Managing DAGs

Since the `/opt/airflow` directory is mounted to a Persistent Volume (PVC) to preserve logs and DB data, the DAGs built into the Docker image are hidden by the mount.

To update or add DAGs, you must copy them into the running pod:

```bash
# 1. Get the Pod Name
POD_NAME=$(kubectl get pods -n spotify -l app=airflow -o jsonpath="{.items[0].metadata.name}")

# 2. Copy DAGs and Scripts
kubectl cp dags/spotify_etl_k8s_dag.py spotify/$POD_NAME:/opt/airflow/dags/ -c airflow-scheduler
kubectl cp scripts/train_model_on_k8s.sh spotify/$POD_NAME:/opt/scripts/ -c airflow-scheduler
kubectl cp scripts/precompute_recomendations_on_k8s.sh spotify/$POD_NAME:/opt/scripts/ -c airflow-scheduler
```

## 🛠 Troubleshooting

**Check Logs:**
```bash
# Webserver logs
kubectl logs -n spotify -l app=airflow -c airflow-webserver

# Scheduler logs
kubectl logs -n spotify -l app=airflow -c airflow-scheduler

# Init (DB Migration) logs
kubectl logs -n spotify -l app=airflow -c airflow-init
```

**Restart Airflow:**
```bash
kubectl delete pod -n spotify -l app=airflow
# Kubernetes will automatically recreate it
```

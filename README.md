# Spotify MPD BigData Pipeline - Starter Repo

This repository is a starter scaffold for the **Spotify Million Playlist Dataset (MPD)** Big Data pipeline.
It contains minimal example code and manifests to help you start implementing the ingestion, ETL, model training,
streaming and serving components described in the project design.

## Contents
- `etl/` : PySpark example jobs for ingest and transform.
- `serving/` : FastAPI microservice for candidate + ranking (example).
- `models/` : placeholder for trained models / MLflow artifacts.
- `infra/` : docker-compose and minimal K8s manifests.
- `notebooks/` : starter Jupyter notebook template.
- `scripts/` : helper scripts.
- `requirements.txt` : Python requirements for local development.
- `Dockerfile` : example for building the FastAPI service.

## How to use
1. Unzip the project:
   ```
   unzip spotify_mpd_pipeline.zip
   cd spotify_mpd_pipeline
   ```
2. Create a Python virtualenv and install dependencies:
   ```
   python -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```
3. Run the FastAPI example (serving):
   ```
   uvicorn serving.app:app --reload --host 0.0.0.0 --port 8080
   ```
4. Run PySpark example (adjust SPARK_HOME or use spark-submit):
   ```
   spark-submit etl/ingest_example.py
   ```
5. To push to GitHub:
   ```
   git init
   git add .
   git commit -m "Initial scaffold for MPD pipeline"
   gh repo create your-username/spotify-mpd-pipeline --public --source=. --remote=origin
   git push -u origin main
   ```
   *(If you don't have `gh` CLI, create a new repo on GitHub.com and follow instructions.)*

## Notes
- This scaffold is intentionally minimal and for learning & development purposes.
- Replace placeholders and add credentials (do **not** commit secrets).
- See the project design doc for detailed architecture and recommended tools.


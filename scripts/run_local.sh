#!/usr/bin/env bash
echo "Starting local dev steps..."
echo "- start minio, kafka, postgres via docker-compose"
docker-compose -f infra/docker-compose.yml up -d
echo "You can now run the FastAPI server in another terminal:"
echo "uvicorn serving.app:app --reload --host 0.0.0.0 --port 8080"

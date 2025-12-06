### Ingestion data json to Minio

#### 1. Run on local
```bash
python data_ingestion/batch_ingest.py
```
The access http://localhost:9001 -> in bucket spotify-raw-data will see raw data with files format '.json'

#### 2. Run on k8s:
A terminal:
```bash
kubectl port-forward --namespace spotify service/minio-service 9000:9000 9001:9001
```

 Another terminal:
```bash
python data_ingestion/batch_ingest.py
```
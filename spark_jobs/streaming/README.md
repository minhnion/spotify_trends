### Spark Job streaming

#### 1. Run on local 
running with 3 different terminals
``` bash
python data_ingestion/event_producer.py
```

``` bash
python spark_jobs/streaming/run_streaming.py
```
``` bash
python spark_jobs/streaming/streaming_to_db.py
```
Wait 15 minutes before the data is written to the database.


Out put will be in bucket spotify-processed-data with files format '.parquet'

#### 2. Run on k8s

``` bash
kubectl create namespace kafka
helm repo add strimzi https://strimzi.io/charts/
helm install strimzi strimzi/strimzi-kafka-operator -n kafka
```
``` bash
kubectl apply -f kubernetes/core-infra/kafka.yaml
kubectl apply -f kubernetes/core-infra/spark-sa.yml
docker build -f Dockerfile.producer -t spotify-producer .
kubectl apply -f kubernetes/jobs/kafka-producer-deployment.yaml 
docker build -f Dockerfile.spark -t spotify-spark .
```
``` bash
scripts/run_streaming_on_k8s.sh
scripts/streaming_to_db_k8s.sh
scripts/run_compact_on_k8s.sh
```

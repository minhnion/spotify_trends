### Spark Job ETL to process raw data 

#### 1. Run on local 
``` bash
chmod +x scripts/run_etl_job.sh
```

``` bash
./scripts/run_etl_job.sh
```
#### 2. Run on K8S 
``` bash
chmod +x scripts/run_etl_on_k8s.sh
```

``` bash
./scripts/run_etl_on_k8s.sh
```

Out put will be in bucket spotify-processed-data with files format '.parquet'
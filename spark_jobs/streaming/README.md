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
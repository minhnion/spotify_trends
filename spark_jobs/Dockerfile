FROM apache/spark-py:latest

USER root
COPY spark_jobs/requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt

USER spark
COPY . /opt/spark/work-dir/
WORKDIR /opt/spark/work-dir
FROM tabulario/spark-iceberg:latest
USER root
COPY jars/ /opt/spark/jars/
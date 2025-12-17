FROM tabulario/spark-iceberg:3.5.3
USER root
COPY jars/ /opt/spark/jars/
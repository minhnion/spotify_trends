FROM apache/spark-py:latest

USER root

COPY spark_jobs/requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt


RUN mkdir -p /opt/spark/.ivy2 && \
    chown -R 185:185 /opt/spark/.ivy2 && \
    chmod -R 777 /opt/spark/.ivy2


RUN mkdir -p /opt/spark/work-dir && \
    chown -R 185:185 /opt/spark/work-dir

COPY --chown=185 . /opt/spark/work-dir/

WORKDIR /opt/spark/work-dir

USER 185
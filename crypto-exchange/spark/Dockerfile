FROM apache/spark:3.5.0

USER root

RUN pip install --no-cache-dir prometheus-client==0.20.0

COPY spark_streaming.py /opt/spark-apps/

CMD ["spark-submit", \
     "--master", "spark://spark-master:7077", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0", \
     "--conf", "spark.executor.memory=1g", \
     "--conf", "spark.driver.memory=1g", \
     "/opt/spark-apps/spark_streaming.py"]

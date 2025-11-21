FROM apache/spark:3.5.1

# cài pip packages
USER root
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
RUN mkdir -p /home/spark/.ivy2/cache && chown -R spark:spark /home/spark/.ivy2

USER spark
COPY app /app
WORKDIR /app

# spark-submit với Kafka connector
CMD ["/opt/spark/bin/spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1", "spark_streaming.py"]

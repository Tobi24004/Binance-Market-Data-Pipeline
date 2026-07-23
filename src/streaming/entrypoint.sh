#!/bin/bash
# spark-submit is the only supported way to run spark_stream.py:
# - matches the exact Spark version of the cluster (same base image)
# - --py-files ships dependencies.zip (src/common/) to the executors
# - --packages pulls the Kafka + Postgres JDBC connectors Spark needs
set -euo pipefail

exec spark-submit \
  --master "${SPARK_MASTER_URL:-spark://spark-master:7077}" \
  --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3" \
  --py-files /app/dependencies.zip \
  /app/src/streaming/spark_stream.py

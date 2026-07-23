FROM bitnamilegacy/spark:3.5.1

USER root
WORKDIR /app

# zip is not present in the bitnami/spark base image; needed to build
# dependencies.zip from src/common/ below.
RUN install_packages zip

COPY src/streaming/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY src/common ./src/common
COPY src/streaming ./src/streaming

# Spark executors cannot see src/common/ unless it is shipped as a zip via
# --py-files (see entrypoint.sh) - plain PYTHONPATH only covers the driver.
RUN cd /app/src && zip -r /app/dependencies.zip common -x '*/__pycache__/*'

ENV PYTHONPATH=/app/src
RUN chmod +x /app/src/streaming/entrypoint.sh

ENTRYPOINT ["/app/src/streaming/entrypoint.sh"]

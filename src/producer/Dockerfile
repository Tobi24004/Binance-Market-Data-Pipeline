FROM python:3.11-slim

WORKDIR /app

COPY src/producer/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY src/common ./src/common
COPY src/producer ./src/producer

ENV PYTHONPATH=/app/src
ENV PYTHONUNBUFFERED=1

CMD ["python", "-m", "producer.main"]

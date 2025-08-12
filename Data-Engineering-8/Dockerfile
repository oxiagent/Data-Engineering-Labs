FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer.py .


ENV KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
    KAFKA_TOPIC=tweets \
    INPUT_PATH=/data/amazon_reviews.csv \
    RATE_PER_SEC=12

CMD ["python", "producer.py"]

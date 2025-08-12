FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY consumer.py .

# каталог для вихідних CSV
RUN mkdir -p /data/output
ENV OUTPUT_DIR=/data/output
ENV BOOTSTRAP_SERVERS=kafka:9092
ENV TOPIC=tweets
ENV GROUP_ID=tweets-consumer

CMD ["python", "consumer.py"]

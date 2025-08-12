import os, csv, json, time, uuid
from datetime import datetime, timezone
from kafka import KafkaProducer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC      = os.getenv("KAFKA_TOPIC", "tweets")
INPUT_PATH = os.getenv("INPUT_PATH", "/data/amazon_reviews.csv")
RATE       = float(os.getenv("RATE_PER_SEC", "12"))  
def now_iso():
    return datetime.now(timezone.utc).isoformat()

def main():
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=50,
        acks="all",
    )

    sent = 0
    period = 1.0 / RATE

    with open(INPUT_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            msg = {
                "id": str(uuid.uuid4()),                 
                "text": (row.get("review_body") or "")[:240], 
                "user_id": int(row.get("customer_id") or 0),
                "product_id": row.get("product_id"),
                "rating": int(row.get("star_rating") or 0),
                "created_at": now_iso(),                
            }
            producer.send(TOPIC, msg)
            sent += 1
            time.sleep(period)

    producer.flush()
    print(f"Done. Sent {sent} messages to topic '{TOPIC}'.")

if __name__ == "__main__":
    main()

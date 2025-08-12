import os
import json
import csv
from datetime import datetime
from kafka import KafkaConsumer

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("TOPIC", "tweets")
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "/data/output")
GROUP_ID = os.getenv("GROUP_ID", "tweets-consumer")

os.makedirs(OUTPUT_DIR, exist_ok=True)

def safe_deserializer(m: bytes):
    """Return dict for valid JSON, or None for empty/invalid payloads."""
    if not m:
        return None
    try:
        return json.loads(m.decode("utf-8"))
    except Exception:
        
        return None

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    group_id=GROUP_ID,
    auto_offset_reset="latest",          # стартуємо з кінця, щоб оминути старий “сміттєвий” payload
    enable_auto_commit=True,
    value_deserializer=safe_deserializer
)

print(f"Listening to topic '{TOPIC}' on {BOOTSTRAP_SERVERS}...")


files_cache: dict[str, tuple[object, csv.writer]] = {}

try:
    for message in consumer:
        payload = message.value
        if not payload:
            continue  

        author_id = payload.get("user_id")          
        created_at_str = payload.get("created_at")  
        text = (payload.get("text") or "").replace("\n", " ").strip()

        if not (author_id and created_at_str and text):
            continue  

        
        created_at = datetime.fromisoformat(created_at_str.replace("Z", "+00:00"))
        filename_time = created_at.strftime("%d_%m_%Y_%H_%M")
        filename = f"tweets_{filename_time}.csv"
        filepath = os.path.join(OUTPUT_DIR, filename)

        if filename not in files_cache:
            f = open(filepath, mode="a", newline="", encoding="utf-8")
            w = csv.writer(f)
            
            if f.tell() == 0:
                w.writerow(["author_id", "created_at", "text"])
            files_cache[filename] = (f, w)

        f, w = files_cache[filename]
        w.writerow([author_id, created_at_str, text])
        f.flush()

except KeyboardInterrupt:
    print("\n Stopping consumer...")
finally:
    for f, _ in files_cache.values():
        try:
            f.close()
        except Exception:
            pass

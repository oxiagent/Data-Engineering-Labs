
set -euo pipefail

CSV_PATH="$HOME/Documents/Data-Engineering-8/amazon_reviews.csv"

docker run --rm -it \
  --name tweets_producer \
  --network data-engineering-8_kafka_net \
  -e KAFKA_BOOTSTRAP_SERVERS="kafka:9092" \
  -e KAFKA_TOPIC="tweets" \
  -e INPUT_PATH="/data/amazon_reviews.csv" \
  -e RATE_PER_SEC="12" \
  -v "$CSV_PATH":/data/amazon_reviews.csv:ro \
  tweets-producer:latest

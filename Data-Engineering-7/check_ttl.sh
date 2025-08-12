
set -euo pipefail

BASE="http://localhost:30081"
PERIOD="2005-09"
LIMIT="5"

echo "== Warm up: =="
curl -sS "$BASE/stats/top-products?period=$PERIOD&limit=$LIMIT" >/dev/null

echo "== Cache keys in Redis =="

KEY=$(docker exec -i redis_lab redis-cli --scan --pattern 'cache:*' | head -n 1 || true)

if [[ -z "${KEY:-}" ]]; then
  echo "Keys with pattern 'cache:*' are not found. Any key will be tested"
  KEY=$(docker exec -i redis_lab redis-cli --scan | head -n 1 || true)
fi

if [[ -z "${KEY:-}" ]]; then
  echo "Redis has no keys."
  exit 1
fi

echo "Key: $KEY is selected"

echo "== Read TTL key =="
TTL1=$(docker exec -i redis_lab redis-cli TTL "$KEY")
echo "TTL зараз: $TTL1 сек"

sleep 3

TTL2=$(docker exec -i redis_lab redis-cli TTL "$KEY")
echo "TTL in 3 sec: $TTL2 sec"

if [[ "$TTL1" -gt "$TTL2" ]]; then
  echo "TTL is decreasing. Cache works with expecting TTL."
else
  echo "TTL didn't decrease. "
fi


set -euo pipefail

echo "== Redis before cleaning =="
BEFORE=$(docker exec -i redis_lab redis-cli DBSIZE)
echo "Key quantity: $BEFORE"

echo "== FLUSHALL =="
docker exec -i redis_lab redis-cli FLUSHALL

echo "== Redis after cleaning =="
AFTER=$(docker exec -i redis_lab redis-cli DBSIZE)
echo "Key quantity: $AFTER"

if [[ "$AFTER" -eq 0 ]]; then
  echo "Cache is cleaned."
else
  echo "Redis has keys. They could be created after tests."
fi

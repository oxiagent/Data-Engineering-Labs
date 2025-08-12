

BASE=http://localhost:30081
PROD=0140291784
PROD_R=0897934490
STAR=5
CUST=34291823
PERIOD=2005-09
LIMIT=5

echo "===== HEALTH CHECK ====="
curl -sS $BASE/health || true
echo -e "\nDocs: $BASE/docs\n"

echo "===== FIRST RUN (warm up cache) ====="
time curl -sS "$BASE/reviews/product/$PROD" | head -c 300; echo
time curl -sS "$BASE/reviews/product/$PROD_R/rating/$STAR" | head -c 300; echo
time curl -sS "$BASE/reviews/customer/$CUST" | head -c 300; echo
time curl -sS "$BASE/stats/top-products?period=$PERIOD&limit=$LIMIT" | head -c 300; echo

echo "===== SECOND RUN (should be cached) ====="
time curl -sS "$BASE/reviews/product/$PROD" -o /dev/null
time curl -sS "$BASE/reviews/product/$PROD_R/rating/$STAR" -o /dev/null
time curl -sS "$BASE/reviews/customer/$CUST" -o /dev/null
time curl -sS "$BASE/stats/top-products?period=$PERIOD&limit=$LIMIT" -o /dev/null

echo "===== REDIS KEYS ====="
docker exec -it redis_lab redis-cli KEYS '*'

# bench.py
import time, statistics, httpx, os

BASE = os.getenv("API", "http://localhost:8000")
ROUTES = [
    "/campaign/1/performance",
    "/advertiser/1/spending",
    "/user/254839/engagements",
]

def run_round(label, n=50):
    times = []
    with httpx.Client(timeout=10) as client:
        for _ in range(n):
            t0 = time.perf_counter()
            r = client.get(BASE + route)
            dt = (time.perf_counter() - t0) * 1000
            times.append(dt)
    print(f"{label}: n={n} p50={statistics.median(times):.2f} ms  avg={statistics.mean(times):.2f} ms  p95={statistics.quantiles(times, n=20)[18]:.2f} ms")

for route in ROUTES:
    print(f"\n== {route} ==")
    # Cold cache
    os.system("docker exec -it de5_redis redis-cli FLUSHALL > /dev/null 2>&1")
    run_round("cold")
    # Warm cache
    run_round("warm")

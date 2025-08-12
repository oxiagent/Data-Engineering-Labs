import os
import json
import hashlib
from datetime import date, timedelta
from typing import Optional, List

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
import redis

# --- Config from env ---
CASS_CONTACT_POINTS = os.getenv("CASS_CONTACT_POINTS", "cassandra_lab").split(",")
CASS_PORT = int(os.getenv("CASS_PORT", "9042"))
CASS_KEYSPACE = os.getenv("CASS_KEYSPACE", "amazon")
REDIS_HOST = os.getenv("REDIS_HOST", "redis_lab")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
CACHE_TTL = int(os.getenv("CACHE_TTL_SECONDS", "300"))

# --- Connect Cassandra ---
cluster = Cluster(CASS_CONTACT_POINTS, port=CASS_PORT)
session = cluster.connect()
session.execute(
    f"CREATE KEYSPACE IF NOT EXISTS {CASS_KEYSPACE} "
    "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};"
)
session.set_keyspace(CASS_KEYSPACE)

# --- Connect Redis ---
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

app = FastAPI(title="Amazon Reviews Analytics API (Cassandra + Redis)")

# --- Helpers: caching ---
def cache_get_or_set(key: str, producer):
    data = r.get(key)
    if data is not None:
        return json.loads(data)
    val = producer()
    r.setex(key, CACHE_TTL, json.dumps(val, default=str))
    return val

def cache_key(prefix: str, **kwargs) -> str:
    payload = json.dumps(kwargs, sort_keys=True, default=str)
    h = hashlib.sha1(payload.encode()).hexdigest()
    return f"{prefix}:{h}"

# --- Helpers: Cassandra date conversion ---
try:
    from cassandra.util import Date as CassandraDate
except Exception:
    CassandraDate = None

def _to_py_date(v):
    """Normalize Cassandra 'Date' or integer day-offset to datetime.date."""
    if CassandraDate is not None and isinstance(v, CassandraDate):
        return v.date()  # returns datetime.date
    if isinstance(v, int):
        return date(1970, 1, 1) + timedelta(days=v)
    return v  # assume already datetime.date or None

def _row_to_review_dict(row):
    d = row._asdict()
    d["review_date"] = _to_py_date(d.get("review_date"))
    return d

# --- Models ---
class Review(BaseModel):
    product_id: Optional[str] = None
    review_id: str
    customer_id: int
    star_rating: int
    verified_purchase: int
    review_headline: Optional[str] = None
    review_body: Optional[str] = None
    review_date: date

class TopItem(BaseModel):
    product_id: str
    review_count: int

class TopCustomerCount(BaseModel):
    customer_id: int
    count: int

# --- Endpoints ---

# 1) All reviews for product_id (optional date range)
@app.get("/reviews/product/{product_id}", response_model=List[Review])
def reviews_by_product(
    product_id: str,
    from_date: Optional[date] = Query(None),
    to_date: Optional[date] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
):
    key = cache_key(
        "reviews_by_product",
        product_id=product_id,
        from_date=from_date,
        to_date=to_date,
        limit=limit,
    )

    def run():
        if from_date and to_date:
            stmt = session.prepare(
                "SELECT product_id, review_id, customer_id, star_rating, verified_purchase, review_headline, review_body, review_date "
                "FROM reviews_by_product WHERE product_id=? AND review_date >= ? AND review_date <= ? "
                "LIMIT ?"
            )
            rows = session.execute(stmt, (product_id, from_date, to_date, limit))
        elif from_date:
            stmt = session.prepare(
                "SELECT product_id, review_id, customer_id, star_rating, verified_purchase, review_headline, review_body, review_date "
                "FROM reviews_by_product WHERE product_id=? AND review_date >= ? "
                "LIMIT ?"
            )
            rows = session.execute(stmt, (product_id, from_date, limit))
        elif to_date:
            stmt = session.prepare(
                "SELECT product_id, review_id, customer_id, star_rating, verified_purchase, review_headline, review_body, review_date "
                "FROM reviews_by_product WHERE product_id=? AND review_date <= ? "
                "LIMIT ?"
            )
            rows = session.execute(stmt, (product_id, to_date, limit))
        else:
            stmt = session.prepare(
                "SELECT product_id, review_id, customer_id, star_rating, verified_purchase, review_headline, review_body, review_date "
                "FROM reviews_by_product WHERE product_id=? LIMIT ?"
            )
            rows = session.execute(stmt, (product_id, limit))
        return [Review(**_row_to_review_dict(row)).model_dump() for row in rows]

    return cache_get_or_set(key, run)

# 2) All reviews for product_id with star_rating
@app.get("/reviews/product/{product_id}/rating/{star_rating}", response_model=List[Review])
def reviews_by_product_and_rating(
    product_id: str,
    star_rating: int,
    limit: int = Query(100, ge=1, le=1000),
):
    key = cache_key(
        "reviews_by_product_and_rating",
        product_id=product_id,
        star_rating=star_rating,
        limit=limit,
    )

    def run():
        stmt = session.prepare(
            "SELECT product_id, review_id, customer_id, star_rating, verified_purchase, review_headline, review_body, review_date "
            "FROM reviews_by_product_and_rating WHERE product_id=? AND star_rating=? LIMIT ?"
        )
        rows = session.execute(stmt, (product_id, star_rating, limit))
        return [Review(**_row_to_review_dict(row)).model_dump() for row in rows]

    return cache_get_or_set(key, run)

# 3) All reviews for customer_id
@app.get("/reviews/customer/{customer_id}", response_model=List[Review])
def reviews_by_customer(
    customer_id: int,
    limit: int = Query(100, ge=1, le=1000),
):
    key = cache_key("reviews_by_customer", customer_id=customer_id, limit=limit)

    def run():
        stmt = session.prepare(
            "SELECT customer_id, review_id, product_id, star_rating, verified_purchase, review_headline, review_body, review_date "
            "FROM reviews_by_customer WHERE customer_id=? LIMIT ?"
        )
        rows = session.execute(stmt, (customer_id, limit))
        out = []
        for row in rows:
            d = _row_to_review_dict(row)
            out.append(
                Review(
                    product_id=d.get("product_id"),
                    review_id=d["review_id"],
                    customer_id=d["customer_id"],
                    star_rating=d["star_rating"],
                    verified_purchase=d["verified_purchase"],
                    review_headline=d.get("review_headline"),
                    review_body=d.get("review_body"),
                    review_date=d["review_date"],
                ).model_dump()
            )
        return out

    return cache_get_or_set(key, run)

# 4) N most reviewed items for a given period
@app.get("/top/items", response_model=List[TopItem])
def top_items(period: str = Query(..., pattern=r"^\d{4}-\d{2}$"), limit: int = Query(10, ge=1, le=1000)):
    key = cache_key("top_items", period=period, limit=limit)

    def run():
        stmt = session.prepare(
            "SELECT product_id, review_count FROM top_products_by_period WHERE period_ym=? LIMIT ?"
        )
        rows = session.execute(stmt, (period, limit))
        return [TopItem(product_id=row.product_id, review_count=row.review_count).model_dump() for row in rows]

    return cache_get_or_set(key, run)

# 5) N most productive customers (verified purchases) for a period
@app.get("/top/customers/verified", response_model=List[TopCustomerCount])
def top_customers_verified(period: str = Query(..., pattern=r"^\d{4}-\d{2}$"), limit: int = Query(10, ge=1, le=1000)):
    key = cache_key("top_customers_verified", period=period, limit=limit)

    def run():
        stmt = session.prepare(
            "SELECT customer_id, verified_review_count FROM top_customers_verified_by_period WHERE period_ym=? LIMIT ?"
        )
        rows = session.execute(stmt, (period, limit))
        return [TopCustomerCount(customer_id=row.customer_id, count=row.verified_review_count).model_dump() for row in rows]

    return cache_get_or_set(key, run)

# 6) N most productive “haters” (1-2 stars) for a period
@app.get("/top/customers/haters", response_model=List[TopCustomerCount])
def top_haters(period: str = Query(..., pattern=r"^\d{4}-\d{2}$"), limit: int = Query(10, ge=1, le=1000)):
    key = cache_key("top_haters", period=period, limit=limit)

    def run():
        stmt = session.prepare(
            "SELECT customer_id, low_rating_count FROM top_haters_by_period WHERE period_ym=? LIMIT ?"
        )
        rows = session.execute(stmt, (period, limit))
        return [TopCustomerCount(customer_id=row.customer_id, count=row.low_rating_count).model_dump() for row in rows]

    return cache_get_or_set(key, run)

# 7) N most productive “backers” (4-5 stars) for a period
@app.get("/top/customers/backers", response_model=List[TopCustomerCount])
def top_backers(period: str = Query(..., pattern=r"^\d{4}-\d{2}$"), limit: int = Query(10, ge=1, le=1000)):
    key = cache_key("top_backers", period=period, limit=limit)

    def run():
        stmt = session.prepare(
            "SELECT customer_id, high_rating_count FROM top_backers_by_period WHERE period_ym=? LIMIT ?"
        )
        rows = session.execute(stmt, (period, limit))
        return [TopCustomerCount(customer_id=row.customer_id, count=row.high_rating_count).model_dump() for row in rows]

    return cache_get_or_set(key, run)

@app.get("/health")
def health():
    return {"status": "ok"}

# api/app/main.py
from fastapi import FastAPI, Response
from pydantic import BaseModel
from sqlalchemy import text
from .db import get_engine

import os
import json
import redis

app = FastAPI(title="Real-Time Ad API with Redis Cache")

# --- Ініціалізація клієнтів ---
r = redis.Redis(
    host=os.getenv("REDIS_HOST", "redis"),
    port=int(os.getenv("REDIS_PORT", "6379")),
    decode_responses=True,
)
engine = get_engine()

# ---------- Health ----------
@app.get("/health")
def health():
    # Redis
    try:
        redis_ok = r.ping()
    except Exception:
        redis_ok = False

    # MySQL
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        mysql_ok = True
    except Exception:
        mysql_ok = False

    return {"redis": redis_ok, "mysql": mysql_ok}


# =======================
#  /campaign/{id}/performance
#  TTL = 30s
# =======================

class CampaignPerformance(BaseModel):
    campaign_id: int
    clicks: int
    impressions: int
    ctr: float
    ad_spend: float

TTL_CAMPAIGN_PERF = int(os.getenv("TTL_CAMPAIGN_PERF", "30"))

def _cache_key_campaign_perf(campaign_id: int) -> str:
    return f"campaign:{campaign_id}:perf"

def _fetch_campaign_perf_from_db(campaign_id: int) -> dict | None:
    # Твій SQL з фільтром по campaign_id
    sql = text("""
        SELECT
              fe.campaign_id,
              COUNT(fe.event_id) AS total_impressions,
              COALESCE(SUM(fe.was_clicked), 0) AS total_clicks,
              COALESCE(SUM(fe.was_clicked) / NULLIF(COUNT(fe.event_id), 0), 0) AS ctr,
              COALESCE(SUM(fe.ad_cost), 0) AS ad_spend
        FROM analytics_db.fact_event AS fe
        WHERE fe.campaign_id = :campaign_id
        GROUP BY fe.campaign_id
    """)
    with engine.connect() as conn:
        row = conn.execute(sql, {"campaign_id": campaign_id}).mappings().first()

    if not row:
        return None

    return {
        "campaign_id": int(row["campaign_id"]),
        "clicks": int(row["total_clicks"] or 0),
        "impressions": int(row["total_impressions"] or 0),
        "ctr": float(row["ctr"] or 0),
        "ad_spend": round(float(row["ad_spend"] or 0), 2),
    }

@app.get("/campaign/{campaign_id}/performance", response_model=CampaignPerformance)
def get_campaign_performance(campaign_id: int, response: Response):
    key = _cache_key_campaign_perf(campaign_id)

    cached = r.get(key)
    if cached:
        response.headers["X-Cache"] = "HIT"
        return json.loads(cached)

    data = _fetch_campaign_perf_from_db(campaign_id)
    if data is None:
        response.headers["X-Cache"] = "MISS"
        return CampaignPerformance(
            campaign_id=campaign_id, clicks=0, impressions=0, ctr=0.0, ad_spend=0.0
        )

    r.set(key, json.dumps(data), ex=TTL_CAMPAIGN_PERF)
    response.headers["X-Cache"] = "MISS"
    return data


# =======================
#  /advertiser/{id}/spending
#  TTL = 300s (5 хв)
# =======================

class AdvertiserSpending(BaseModel):
    advertiser_id: int
    total_spend: float

TTL_ADVERTISER_SPEND = int(os.getenv("TTL_ADVERTISER_SPEND", "300"))

def _cache_key_adv_spend(advertiser_id: int) -> str:
    return f"advertiser:{advertiser_id}:spend"

def _fetch_advertiser_spend_from_db(advertiser_id: int) -> dict | None:
    # Твій SQL з фільтром по advertiser_id
    sql = text("""
        SELECT
           cam.advertiser_id,
           COALESCE(SUM(fe.ad_cost), 0) AS ad_spend
        FROM analytics_db.dim_campaign AS cam
        LEFT JOIN analytics_db.fact_event AS fe
          ON cam.campaign_id = fe.campaign_id
        WHERE cam.advertiser_id = :advertiser_id
        GROUP BY cam.advertiser_id
    """)
    with engine.connect() as conn:
        row = conn.execute(sql, {"advertiser_id": advertiser_id}).mappings().first()

    if not row:
        return None

    return {
        "advertiser_id": int(row["advertiser_id"]),
        "total_spend": round(float(row["ad_spend"] or 0), 2),
    }

@app.get("/advertiser/{advertiser_id}/spending", response_model=AdvertiserSpending)
def get_advertiser_spending(advertiser_id: int, response: Response):
    key = _cache_key_adv_spend(advertiser_id)

    cached = r.get(key)
    if cached:
        response.headers["X-Cache"] = "HIT"
        return json.loads(cached)

    data = _fetch_advertiser_spend_from_db(advertiser_id)
    if data is None:
        response.headers["X-Cache"] = "MISS"
        return AdvertiserSpending(advertiser_id=advertiser_id, total_spend=0.0)

    r.set(key, json.dumps(data), ex=TTL_ADVERTISER_SPEND)
    response.headers["X-Cache"] = "MISS"
    return data


# =======================
#  /user/{id}/engagements
#  TTL = 120s (можеш змінити у .env)
# =======================

class UserEngagements(BaseModel):
    user_id: int
    ads_engaged: list[int]
    count: int

TTL_USER_ENG = int(os.getenv("TTL_USER_ENG", "120"))

def _cache_key_user_eng(user_id: int) -> str:
    return f"user:{user_id}:eng"

def _fetch_user_engagements_from_db(user_id: int) -> dict | None:
    # Твій запит — повернемо списком оголошення (ad_slot_id), з якими юзер взаємодіяв
    sql = text("""
        SELECT DISTINCT
           fe.ad_slot_id
        FROM analytics_db.fact_event AS fe
        WHERE fe.user_id = :user_id
          AND fe.was_clicked = 1
        ORDER BY fe.ad_slot_id
    """)
    with engine.connect() as conn:
        rows = conn.execute(sql, {"user_id": user_id}).fetchall()

    ads = [int(r[0]) for r in rows] if rows else []
    return {
        "user_id": user_id,
        "ads_engaged": ads,
        "count": len(ads),
    }

@app.get("/user/{user_id}/engagements", response_model=UserEngagements)
def get_user_engagements(user_id: int, response: Response):
    key = _cache_key_user_eng(user_id)

    cached = r.get(key)
    if cached:
        response.headers["X-Cache"] = "HIT"
        return json.loads(cached)

    data = _fetch_user_engagements_from_db(user_id)

    r.set(key, json.dumps(data), ex=TTL_USER_ENG)
    response.headers["X-Cache"] = "MISS"
    return data

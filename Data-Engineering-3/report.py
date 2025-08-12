import json
import pandas as pd
from pymongo import MongoClient
from datetime import datetime, timedelta
import os

# Підключення до MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["ad_analytics"]
collection = db["user_engagement"]

# Створити папку output
os.makedirs("output", exist_ok=True)

# 1. User Ad Interaction History
def query_user_interaction(user_id):
    result = collection.find_one({"user_id": user_id})
    if result:
        with open("output/query1_user_history.json", "w") as f:
            json.dump(result, f, default=str, indent=2)
    else:
        print(f"⚠️ No user with user_id = {user_id}")

# 2. Last 5 Sessions with Click Info
def query_last_5_sessions(user_id):
    result = collection.find_one({"user_id": user_id})
    if result and "sessions" in result:
        sessions = sorted(result["sessions"], key=lambda x: x["start_time"], reverse=True)[:5]
        for session in sessions:
            session["clicks"] = sum(1 for i in session["ad_interactions"] if i.get("was_clicked") is True)
        with open("output/query2_last_sessions.json", "w") as f:
            json.dump(sessions, f, default=str, indent=2)
    else:
        print(f"⚠️ No sessions found for user_id = {user_id}")

# 3. Clicks Per Hour per Campaign (only 24h/0h segm)
def query_clicks_per_hour(advertiser_name):
    advertiser_name = advertiser_name.strip()

    pipeline = [
        {"$unwind": "$sessions"},
        {"$unwind": "$sessions.ad_interactions"},
        {"$match": {
            "sessions.ad_interactions.was_clicked": True,
            "sessions.ad_interactions.campaign.advertiser_name": advertiser_name,
            "$expr": { 
                "$eq": [ 
                    { "$hour": "$sessions.ad_interactions.click_timestamp" }, 
                    0 
                ] 
            }
        }},
        {"$group": {
            "_id": {
                "hour": { "$hour": "$sessions.ad_interactions.click_timestamp" },
                "campaign": "$sessions.ad_interactions.campaign.campaign_name"
            },
            "clicks": { "$sum": 1 }
        }},
        {"$sort": { "_id.hour": 1 }}
    ]

    result = list(collection.aggregate(pipeline))

    if result:
        df = pd.DataFrame(result)
        df.to_csv("output/query3_clicks_per_hour.csv", index=False)
        print("Results saved to query3_clicks_per_hour.csv")
    else:
        print(f"⚠️ No click data found for advertiser = {advertiser_name} at hour 0")


# 4. Ad Fatigue Detection
def query_ad_fatigue(min_views=4): # the dataset was cut (in the lab 1) and it impacts results. Max could be only 4
    pipeline = [
        {"$unwind": "$sessions"},
        {"$unwind": "$sessions.ad_interactions"},
        {"$match": {"sessions.ad_interactions.was_clicked": False}},
        {"$group": {
            "_id": {
                "user_id": "$user_id",
                "ad_id": "$sessions.ad_interactions.ad_slot_id"
            },
            "count": {"$sum": 1}
        }},
        {"$match": {"count": {"$gte": min_views}}},
        {"$group": {
            "_id": "$_id.user_id",
            "ad_fatigue_ads": {"$push": "$_id.ad_id"}
        }}
    ]
    result = list(collection.aggregate(pipeline))
    if result:
        df = pd.DataFrame(result)
        df.to_csv("output/query4_ad_fatigue.csv", index=False)
    else:
        print(" No ad fatigue cases detected.")

# 5. Top 3 Engaged Categories
def query_top_categories(user_id):
    pipeline = [
        {"$match": {"user_id": user_id}},
        {"$unwind": "$sessions"},
        {"$unwind": "$sessions.ad_interactions"},
        {"$match": {"sessions.ad_interactions.was_clicked": True}},
        {"$project": {
            "interest": {
                "$trim": { "input": "$sessions.ad_interactions.campaign.targeting.interest" }
            }
        }},
        {"$group": {
            "_id": "$interest",
            "clicks": {"$sum": 1}
        }},
        {"$sort": {"clicks": -1}},
        {"$limit": 3}
    ]
    result = list(collection.aggregate(pipeline))
    with open("output/query5_top_categories.json", "w") as f:
        json.dump(result, f, indent=2)


# 🏁 Запуск усіх запитів
query_user_interaction("309169")
query_last_5_sessions("309169")
query_clicks_per_hour("Advertiser_18")
query_ad_fatigue()
query_top_categories("309169")

print("Усі запити виконано. Результати збережено у папку 'output/'")

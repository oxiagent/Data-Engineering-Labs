import pandas as pd
import pymysql
from pymongo import MongoClient
from datetime import datetime
import warnings

warnings.filterwarnings("ignore", category=FutureWarning)

# 1. Параметри MySQL
mysql_config = {
    'host': '127.0.0.1',  
    'user': 'analyst',
    'password': 'password',
    'database': 'analytics_db',
    'port': 3306
}

# 2. Підключення до MongoDB
mongo_client = MongoClient("mongodb://localhost:27017/")
mongo_db = mongo_client["ad_analytics"]
mongo_collection = mongo_db["user_engagement"]

# 3. Зчитування таблиць
conn = pymysql.connect(**mysql_config)

fact_event = pd.read_sql("SELECT * FROM fact_event", conn)
dim_user = pd.read_sql("SELECT * FROM dim_user", conn)
dim_campaign = pd.read_sql("SELECT * FROM dim_campaign", conn)
dim_targeting = pd.read_sql("SELECT * FROM dim_targeting", conn)
dim_advertiser = pd.read_sql("SELECT * FROM dim_advertiser", conn)

conn.close()

# 4. Приведення дат
fact_event["timestamp"] = pd.to_datetime(fact_event["timestamp"])
fact_event["click_timestamp"] = pd.to_datetime(fact_event["click_timestamp"], errors="coerce")
dim_campaign["start_date"] = pd.to_datetime(dim_campaign["start_date"], errors="coerce")
dim_campaign["end_date"] = pd.to_datetime(dim_campaign["end_date"], errors="coerce")

# 5. JOIN

df = fact_event.merge(dim_user, on="user_id", how="left") \
               .merge(dim_campaign, on="campaign_id", how="left", suffixes=("", "_campaign")) \
               .merge(dim_targeting, on="campaign_id", how="left") \
               .merge(dim_advertiser, on="advertiser_id", how="left", suffixes=("", "_advertiser"))

# 6. Створення допоміжних полів
df["campaign_name"] = df["name"]
df["advertiser_name"] = df["name_advertiser"]
df["was_clicked"] = df["click_timestamp"].notnull()

# 7. Побудова документів
documents = []

for user_id, user_df in df.groupby("user_id"):
    user_info = user_df.iloc[0]
    sessions = []

    for (date, device), session_df in user_df.groupby([
        user_df['timestamp'].dt.date, user_df['device']
    ]):
        ad_interactions = []

        for _, row in session_df.iterrows():
            ad_interactions.append({
                "event_id": row.get("event_id"),
                "timestamp": row.get("timestamp"),
                "ad_slot_id": row.get("ad_slot_id"),
                "campaign": {
                    "campaign_id": row.get("campaign_id"),
                    "campaign_name": row.get("campaign_name"),
                    "advertiser_name": row.get("advertiser_name"),
                    "start_date": row.get("start_date"),
                    "end_date": row.get("end_date"),
                    "budget": float(row.get("budget")) if pd.notnull(row.get("budget")) else None,
                    "remaining_budget": float(row.get("remaining_budget")) if pd.notnull(row.get("remaining_budget")) else None,
                    "targeting": {
                        "criteria": row.get("criteria"),
                        "interest": row.get("interest"),
                        "country": row.get("country")
                    }
                },
                "bid_amount": float(row.get("bid_amount")) if pd.notnull(row.get("bid_amount")) else None,
                "ad_cost": float(row.get("ad_cost")) if pd.notnull(row.get("ad_cost")) else None,
                "ad_revenue": float(row.get("ad_revenue")) if pd.notnull(row.get("ad_revenue")) else None,
                "was_clicked": row.get("was_clicked", False),
                "click_timestamp": row.get("click_timestamp") if pd.notnull(row.get("click_timestamp")) else None
            })

        session_doc = {
            "session_id": f"{user_id}_{device}_{date}",
            "device": device,
            "start_time": min(session_df['timestamp']),
            "ad_interactions": ad_interactions
        }

        sessions.append(session_doc)

    interests_raw = user_info.get("interests", "")
    interests = interests_raw.split(",") if pd.notnull(interests_raw) else []

    user_document = {
        "user_id": user_id,
        "age": int(user_info.get("age")) if pd.notnull(user_info.get("age")) else None,
        "gender": user_info.get("gender", ""),
        "location": user_info.get("location", ""),
        "interests": interests,
        "sessions": sessions
    }

    documents.append(user_document)

# 8. Завантаження в MongoDB
if documents:
    mongo_collection.insert_many(documents)
    print(f"Inserted {len(documents)} documents into MongoDB.")
else:
    print("No documents to insert.")

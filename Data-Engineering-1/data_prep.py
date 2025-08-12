import pandas as pd
from sqlalchemy import create_engine, text

# Підключення до MySQL через PyMySQL
engine = create_engine(
    "mysql+pymysql://root:root_password@127.0.0.1:3306/analytics_db"
)

paths = {
    "stg_ad_events": "SourceDatasets/ad_events_new.csv",
    "stg_campaigns": "SourceDatasets/campaigns.csv",
    "stg_users":     "SourceDatasets/users.csv",
}

with engine.begin() as conn:
    for table_name, csv_path in paths.items():
        print(f"Loading `{table_name}` from `{csv_path}`…")
        df = pd.read_csv(csv_path)
        df.to_sql(table_name, conn, if_exists="replace", index=False)
        cnt = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
        print(f" → {cnt:,} rows loaded into `{table_name}`\n")

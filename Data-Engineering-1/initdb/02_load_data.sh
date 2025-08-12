#!/bin/bash
set -e

# 1) Виключаємо «жорсткі» режими, щоб zero‑date перетворювався на NULL
mysql -uroot -proot -e "
  SET GLOBAL sql_mode = REPLACE(
    REPLACE(
      REPLACE(@@GLOBAL.sql_mode, 'NO_ZERO_DATE', ''),
    'NO_ZERO_IN_DATE', ''),
  'ERROR_FOR_DIVISION_BY_ZERO', '');
"

# Підключальний рядок із LOCAL INFILE
MYSQL="mysql --local-infile=1 -uroot -proot analytics_db -e"

echo "🛠️  Applying schema (01_schema.sql)..."
mysql -uroot -proot analytics_db < /docker-entrypoint-initdb.d/01_schema.sql

echo "🛠️  Creating staging tables..."
$MYSQL "
  DROP TABLE IF EXISTS stg_ad_events;
  CREATE TABLE stg_ad_events (
    EventID                   VARCHAR(100),
    AdvertiserName            VARCHAR(255),
    CampaignName              VARCHAR(255),
    CampaignStartDate         DATE,
    CampaignEndDate           DATE,
    CampaignTargetingCriteria VARCHAR(255),
    CampaignTargetingInterest VARCHAR(255),
    CampaignTargetingCountry  VARCHAR(100),
    AdSlotSize                VARCHAR(50),
    UserID                    VARCHAR(100),
    Device                    VARCHAR(50),
    Location                  VARCHAR(100),
    Timestamp                 DATETIME,
    BidAmount                 DECIMAL(10,4),
    AdCost                    DECIMAL(10,4),
    WasClicked                VARCHAR(10),
    ClickTimestamp            DATETIME,
    AdRevenue                 DECIMAL(10,4),
    Budget                    DECIMAL(12,2),
    RemainingBudget           DECIMAL(12,2)
  );

  DROP TABLE IF EXISTS stg_campaigns;
  CREATE TABLE stg_campaigns (
    CampaignID        VARCHAR(100),
    AdvertiserName    VARCHAR(255),
    CampaignName      VARCHAR(255),
    CampaignStartDate DATE,
    CampaignEndDate   DATE,
    TargetingCriteria VARCHAR(255),
    AdSlotSize        VARCHAR(50),
    Budget            DECIMAL(12,2),
    RemainingBudget   DECIMAL(12,2)
  );

  DROP TABLE IF EXISTS stg_users;
  CREATE TABLE stg_users (
    UserID     VARCHAR(100),
    Age        INT,
    Gender     VARCHAR(50),
    Location   VARCHAR(100),
    Interests  TEXT,
    SignupDate DATE
  );
"

echo "📥  Loading CSVs into staging..."
$MYSQL "LOAD DATA LOCAL INFILE '/docker-entrypoint-initdb.d/SourceDatasets/ad_events_new.csv'
INTO TABLE stg_ad_events
FIELDS TERMINATED BY ',' ENCLOSED BY '\"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;"

$MYSQL "LOAD DATA LOCAL INFILE '/docker-entrypoint-initdb.d/SourceDatasets/campaigns.csv'
INTO TABLE stg_campaigns
FIELDS TERMINATED BY ',' ENCLOSED BY '\"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;"

$MYSQL "LOAD DATA LOCAL INFILE '/docker-entrypoint-initdb.d/SourceDatasets/users.csv'
INTO TABLE stg_users
FIELDS TERMINATED BY ',' ENCLOSED BY '\"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;"

echo "🔄  Populating dimension tables..."
$MYSQL "
  INSERT IGNORE INTO dim_advertiser(name)
    SELECT DISTINCT AdvertiserName FROM stg_campaigns;

  INSERT IGNORE INTO dim_campaign(advertiser_id, name, start_date, end_date, budget, remaining_budget)
    SELECT da.advertiser_id, sc.CampaignName, sc.CampaignStartDate, sc.CampaignEndDate, sc.Budget, sc.RemainingBudget
    FROM stg_campaigns sc
    JOIN dim_advertiser da ON da.name = sc.AdvertiserName;

  INSERT IGNORE INTO dim_user(user_id, device, location)
    SELECT DISTINCT UserID, Device, Location FROM stg_ad_events;

  INSERT IGNORE INTO dim_ad_slot(size)
    SELECT DISTINCT AdSlotSize FROM stg_ad_events;

  INSERT IGNORE INTO dim_targeting(campaign_id, criteria, interest, country)
    SELECT DISTINCT
      dc.campaign_id,
      ae.CampaignTargetingCriteria,
      ae.CampaignTargetingInterest,
      ae.CampaignTargetingCountry
    FROM stg_ad_events ae
    JOIN dim_campaign dc ON dc.name = ae.CampaignName;
"

echo "🚀  Inserting into fact_event..."
$MYSQL "
  INSERT INTO fact_event(
    event_id,
    campaign_id,
    targeting_id,
    ad_slot_id,
    user_id,
    timestamp,
    bid_amount,
    ad_cost,
    was_clicked,
    click_timestamp,
    ad_revenue
  )
  SELECT
    ae.EventID,
    dc.campaign_id,
    dt.targeting_id,
    das.ad_slot_id,
    ae.UserID,
    ae.Timestamp,
    ae.BidAmount,
    ae.AdCost,
    CASE
      WHEN ae.WasClicked IN ('1','TRUE','true') THEN 1
      ELSE 0
    END AS was_clicked,
    NULLIF(ae.ClickTimestamp,'0000-00-00 00:00:00') AS click_timestamp,
    ae.AdRevenue
  FROM stg_ad_events ae
  JOIN dim_campaign dc ON dc.name = ae.CampaignName
  JOIN dim_targeting dt
    ON dt.campaign_id = dc.campaign_id
   AND dt.criteria     = ae.CampaignTargetingCriteria
   AND dt.interest     = ae.CampaignTargetingInterest
   AND dt.country      = ae.CampaignTargetingCountry
  JOIN dim_ad_slot das ON das.size = ae.AdSlotSize;
"

echo "✅  All data loaded successfully."

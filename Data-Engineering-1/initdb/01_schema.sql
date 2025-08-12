-- 01_schema.sql
-- Створення бази та всіх таблиць (довідники + факти)

CREATE DATABASE IF NOT EXISTS analytics_db
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;
USE analytics_db;

-- Dim Advertiser
CREATE TABLE IF NOT EXISTS dim_advertiser (
  advertiser_id   INT AUTO_INCREMENT PRIMARY KEY,
  name            VARCHAR(255) NOT NULL UNIQUE
);

-- Dim Campaign
CREATE TABLE IF NOT EXISTS dim_campaign (
  campaign_id       INT AUTO_INCREMENT PRIMARY KEY,
  advertiser_id     INT NOT NULL,
  name              VARCHAR(255) NOT NULL,
  start_date        DATE,
  end_date          DATE,
  budget            DECIMAL(12,2),
  remaining_budget  DECIMAL(12,2),
  FOREIGN KEY (advertiser_id) REFERENCES dim_advertiser(advertiser_id)
);

-- Dim User
CREATE TABLE IF NOT EXISTS dim_user (
  user_id    VARCHAR(100) PRIMARY KEY,
  device     VARCHAR(50),
  location   VARCHAR(100)
);

-- Dim Ad Slot
CREATE TABLE IF NOT EXISTS dim_ad_slot (
  ad_slot_id  INT AUTO_INCREMENT PRIMARY KEY,
  size        VARCHAR(50) UNIQUE
);

-- Dim Targeting
CREATE TABLE IF NOT EXISTS dim_targeting (
  targeting_id INT AUTO_INCREMENT PRIMARY KEY,
  campaign_id  INT NOT NULL,
  criteria     VARCHAR(255),
  interest     VARCHAR(255),
  country      VARCHAR(100),
  FOREIGN KEY (campaign_id) REFERENCES dim_campaign(campaign_id)
);

-- Fact Event
CREATE TABLE IF NOT EXISTS fact_event (
  event_id        VARCHAR(100) PRIMARY KEY,
  campaign_id     INT NOT NULL,
  targeting_id    INT NOT NULL,
  ad_slot_id      INT NOT NULL,
  user_id         VARCHAR(100) NOT NULL,
  timestamp       DATETIME NOT NULL,
  bid_amount      DECIMAL(10,4),
  ad_cost         DECIMAL(10,4),
  was_clicked     BOOLEAN,
  click_timestamp DATETIME,
  ad_revenue      DECIMAL(10,4),
  FOREIGN KEY (campaign_id)  REFERENCES dim_campaign(campaign_id),
  FOREIGN KEY (targeting_id) REFERENCES dim_targeting(targeting_id),
  FOREIGN KEY (ad_slot_id)   REFERENCES dim_ad_slot(ad_slot_id),
  FOREIGN KEY (user_id)      REFERENCES dim_user(user_id)
);

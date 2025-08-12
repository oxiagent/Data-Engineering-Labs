
CREATE KEYSPACE IF NOT EXISTS amazon
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE amazon;

-- 1. Return all reviews for specified product_id
CREATE TABLE IF NOT EXISTS reviews_by_product (
  product_id text,
  review_date date,
  review_id text,
  customer_id bigint,
  star_rating int,
  verified_purchase int,
  review_headline text,
  review_body text,
  PRIMARY KEY ((product_id), review_date, review_id)
) WITH CLUSTERING ORDER BY (review_date DESC, review_id ASC);

-- 2. Return all reviews for specified product_id with given star_rating.
CREATE TABLE IF NOT EXISTS reviews_by_product_and_rating (
  product_id text,
  star_rating int,
  review_date date,
  review_id text,
  customer_id bigint,
  verified_purchase int,
  review_headline text,
  review_body text,
  PRIMARY KEY ((product_id, star_rating), review_date, review_id)
) WITH CLUSTERING ORDER BY (review_date DESC, review_id ASC);

-- 3. Return all reviews for specified customer_id.
CREATE TABLE IF NOT EXISTS reviews_by_customer (
  customer_id bigint,
  review_date date,
  review_id text,
  product_id text,
  star_rating int,
  verified_purchase int,
  review_headline text,
  review_body text,
  PRIMARY KEY ((customer_id), review_date, review_id)
) WITH CLUSTERING ORDER BY (review_date DESC, review_id ASC);

-- 4. Return N most reviewed items (by # of reviews) for a given period of time.
CREATE TABLE IF NOT EXISTS top_products_by_period (
  period_ym text,            -- 'YYYY-MM'
  review_count int,
  product_id text,
  PRIMARY KEY ((period_ym), review_count, product_id)
) WITH CLUSTERING ORDER BY (review_count DESC, product_id ASC);

-- 5. Return N most productive customers (by # of reviews written for verified purchases) for a given period.
CREATE TABLE IF NOT EXISTS top_customers_verified_by_period (
  period_ym text,
  verified_review_count int,
  customer_id bigint,
  PRIMARY KEY ((period_ym), verified_review_count, customer_id)
) WITH CLUSTERING ORDER BY (verified_review_count DESC, customer_id ASC);

-- 6. Return N most productive “haters” (by # of 1- or 2-star reviews) for a given period.
CREATE TABLE IF NOT EXISTS top_haters_by_period (
  period_ym text,
  low_rating_count int,
  customer_id bigint,
  PRIMARY KEY ((period_ym), low_rating_count, customer_id)
) WITH CLUSTERING ORDER BY (low_rating_count DESC, customer_id ASC);

-- 7. Return N most productive “backers” (by # of 4- or 5-star reviews) for a given period.
CREATE TABLE IF NOT EXISTS top_backers_by_period (
  period_ym text,
  high_rating_count int,
  customer_id bigint,
  PRIMARY KEY ((period_ym), high_rating_count, customer_id)
) WITH CLUSTERING ORDER BY (high_rating_count DESC, customer_id ASC);
